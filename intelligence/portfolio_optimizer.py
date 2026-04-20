"""
Portfolio optimizer for multi-pub demand_ids.

The LL floor-write endpoint is PUT /v1/demands/{id} — **demand-global**.
27% of demand_ids (67/253) are wired into multiple publishers, so a floor
change there moves them all at once. The per-tuple `optimizer.py` won't
apply those proposals because each one only reasons about a single pub.

This module reasons about the demand as a portfolio: for each multi-pub
demand, jointly estimate expected weekly net revenue across ALL pubs
running it, find the single floor F* that maximizes the portfolio sum,
and emit a proposal with `multi_pub_acknowledged=True` + a per-pub
breakdown so a human can sanity-check before applying.

Why not just apply per-pub optima independently?
-------------------------------------------------
You can't. The endpoint writes one floor per demand. If pub A wants $0.50
and pub B wants $1.00, setting $1.00 strips pub A's cheap wins; setting
$0.50 accepts pub B's low-value inventory. The only honest thing to do
is solve for the F that maximizes the sum and accept the trade-off.

Safety gates (stricter than per-pub optimizer)
----------------------------------------------
- Every pub in the portfolio must be currently live (BIDS>0, last 7d).
- No individual pub allowed to lose more than PER_PUB_LOSS_CAP_PCT of
  its own pre-change revenue, even if the portfolio is up. Prevents
  "majority pub loves it, minority pub gets destroyed" outcomes.
- Joint bootstrap CI lower bound must be positive.
- Holdout: if ANY pub in the portfolio has the tuple in holdout 'control',
  the whole proposal is skipped — we don't blow the holdout to service
  the non-control pubs. (Rare; but enforced.)
- Quarantine: same rule.
- Cooldown: any ledger entry on this demand_id within 24h blocks.

All outputs land in data/proposals.json alongside the per-tuple proposals
produced by optimizer.py. The proposer handles both kinds identically.
"""
from __future__ import annotations

import argparse
import gzip
import json
import random
import time
import uuid
from collections import defaultdict
from dataclasses import asdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

from core import floor_ledger, ll_mgmt, margin
from core.ll_mgmt import PROTECTED_FLOOR_MINIMUMS
from intelligence import holdout, price_response, quarantine
from intelligence.optimizer import (
    Proposal, DEFAULT_MARGIN_PCT, MIN_ABSOLUTE_WEEKLY_LIFT,
    MIN_RELATIVE_LIFT, COOLDOWN_HOURS, _new_proposal_id, _now_iso,
)


def _protected_minimum(demand_name: str) -> float | None:
    """Return the contractual floor minimum for a demand, or None.

    Mirrors the write-path clamp in core.ll_mgmt.set_demand_floor(). Applied
    here at proposal-generation time so the optimizer doesn't waste cycles
    evaluating candidates it can't actually ship, and so the proposal UI
    doesn't show misleading sub-contract figures."""
    name_lower = (demand_name or "").lower()
    for tokens, min_floor in PROTECTED_FLOOR_MINIMUMS:
        if any(tok in name_lower for tok in tokens):
            return min_floor
    return None

DATA_DIR = Path(__file__).parent.parent / "data"
HOURLY_PATH = DATA_DIR / "hourly_pub_demand.json.gz"
PORTFOLIO_DEBUG_PATH = DATA_DIR / "portfolio_proposals_debug.json"

BOOTSTRAP_ITERATIONS = 200
PER_PUB_LOSS_CAP_PCT = 0.20          # no individual pub loses >20% of its own rev
MIN_PORTFOLIO_WEEKLY_REV = 100.0     # don't bother with tiny demands
MIN_PUB_SAMPLES = 20                 # hours of data per pub for a real opinion
FLOOR_SEARCH_GRID_QS = [0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 0.95]


def _load_hourly() -> list[dict]:
    with gzip.open(HOURLY_PATH, "rt") as f:
        return json.load(f)


def _percentile(values: list[float], p: float) -> float:
    if not values:
        return 0.0
    s = sorted(values)
    k = (len(s) - 1) * p
    lo, hi = int(k), min(int(k) + 1, len(s) - 1)
    return s[lo] + (s[hi] - s[lo]) * (k - lo)


def _multi_pub_demands(rows: list[dict], lookback_days: int = 7) -> dict[int, set[int]]:
    """Demands live on ≥2 publishers in last `lookback_days`."""
    cutoff = (datetime.now(timezone.utc).date() - timedelta(days=lookback_days)).isoformat()
    by_demand: dict[int, set[int]] = defaultdict(set)
    for r in rows:
        if str(r.get("DATE", "")) < cutoff:
            continue
        if float(r.get("BIDS", 0) or 0) <= 0:
            continue
        did = int(r.get("DEMAND_ID", 0))
        pid = int(r.get("PUBLISHER_ID", 0))
        if did == 0 or pid == 0:
            continue
        by_demand[did].add(pid)
    return {did: pids for did, pids in by_demand.items() if len(pids) >= 2}


def _per_pub_samples(rows: list[dict], demand_id: int, pub_ids: set[int],
                     lookback_days: int) -> dict[int, list[tuple[float, float]]]:
    """For each pub, return list of (clear_ecpm, revenue) samples across
    hours where wins>0 and rev>0 in the last `lookback_days`."""
    cutoff = (datetime.now(timezone.utc).date() - timedelta(days=lookback_days)).isoformat()
    out: dict[int, list[tuple[float, float]]] = defaultdict(list)
    names: dict[int, str] = {}
    for r in rows:
        if str(r.get("DATE", "")) < cutoff:
            continue
        if int(r.get("DEMAND_ID", 0)) != demand_id:
            continue
        pid = int(r.get("PUBLISHER_ID", 0))
        if pid not in pub_ids:
            continue
        wins = float(r.get("WINS", 0) or 0)
        rev = float(r.get("GROSS_REVENUE", 0) or 0)
        if wins <= 0 or rev <= 0:
            continue
        names[pid] = r.get("PUBLISHER_NAME", "") or names.get(pid, "")
        out[pid].append((rev / wins * 1000.0, rev))
    return out, names


def _predict_rev(samples: list[tuple[float, float]], floor: float, lookback_days: int) -> float:
    """Expected weekly rev at floor — impression-truncation approx, scaled to 7d."""
    if not samples:
        return 0.0
    cleared = sum(rev for ecpm, rev in samples if ecpm >= floor)
    return cleared * (7.0 / lookback_days)


def _candidate_grid(all_samples: dict[int, list[tuple[float, float]]]) -> list[float]:
    """Union of per-pub quantile grids, deduped."""
    all_ecpms: list[float] = []
    for pub_samples in all_samples.values():
        all_ecpms.extend(e for e, _ in pub_samples)
    if not all_ecpms:
        return []
    all_ecpms.sort()
    grid = {0.0}
    for q in FLOOR_SEARCH_GRID_QS:
        grid.add(round(all_ecpms[min(int(q * len(all_ecpms)), len(all_ecpms) - 1)], 2))
    return sorted(grid)


def _in_cooldown(demand_id: int) -> bool:
    for r in floor_ledger.read_all():
        if r["demand_id"] != demand_id or not r["applied"] or r["dry_run"]:
            continue
        if r.get("actor") == "dayparting":
            continue   # dayparting runs hourly; owns its own cooldown
        last = datetime.fromisoformat(r["ts_utc"].replace("Z", "+00:00"))
        if (datetime.now(timezone.utc) - last) < timedelta(hours=COOLDOWN_HOURS):
            return True
    return False


def _any_pub_gated(demand_id: int, pub_ids: set[int]) -> str | None:
    """Return a human reason if ANY pub in the portfolio is in holdout
    control or quarantine. We refuse the whole proposal rather than blow
    the experimental design."""
    for pid in pub_ids:
        if holdout.is_tuple_held_out(pid, demand_id):
            # distinguish inactive from control
            group = holdout.assign_tuple(pid, demand_id)
            if group == "inactive":
                continue  # inactive pubs don't block — they just don't contribute
            return f"pub {pid} in holdout:{group}"
        if quarantine.is_in_quarantine(pid, demand_id):
            return f"pub {pid} in quarantine"
    return None


def _current_live_floor(demand_id: int) -> float | None:
    try:
        return ll_mgmt._get(f"/v1/demands/{demand_id}").get("minBidFloor")
    except Exception:
        return None


def generate(*, lookback_days: int = 14, dry_margin_fetch: bool = False) -> list[Proposal]:
    """Build one aggregated proposal per multi-pub demand that clears gates."""
    rows = _load_hourly()
    multi = _multi_pub_demands(rows, lookback_days=7)
    margins = {} if dry_margin_fetch else _safe_margin_fetch()

    proposals: list[Proposal] = []
    debug_rows: list[dict] = []
    for did, pub_ids in multi.items():
        # Cooldown first — cheapest gate
        if _in_cooldown(did):
            debug_rows.append({"demand_id": did, "verdict": "cooldown"})
            continue
        gate_reason = _any_pub_gated(did, pub_ids)
        if gate_reason:
            debug_rows.append({"demand_id": did, "verdict": "gated", "detail": gate_reason})
            continue

        samples_by_pub, names = _per_pub_samples(rows, did, pub_ids, lookback_days)
        # Pubs with too little signal still count (fixed-revenue baseline),
        # but won't meaningfully shift the argmax.
        total_rev = sum(sum(rev for _, rev in s) for s in samples_by_pub.values()) * (7.0 / lookback_days)
        if total_rev < MIN_PORTFOLIO_WEEKLY_REV:
            debug_rows.append({"demand_id": did, "verdict": "portfolio_rev_too_small",
                               "weekly_rev": round(total_rev, 2)})
            continue

        # Current live floor on the demand (what's actually set right now)
        current_floor = _current_live_floor(did)

        # Search grid (union across pubs)
        grid = _candidate_grid(samples_by_pub)
        if not grid:
            continue

        # Clamp grid to contract minimums (e.g. 9 Dots must be >= $1.70).
        # Without this the optimizer happily proposes sub-contract floors that
        # the write-path clamp in ll_mgmt will catch and rewrite — wasteful
        # and misleading in the proposal UI.
        demand_name_for_filter = _demand_name(rows, did)
        min_floor = _protected_minimum(demand_name_for_filter)
        if min_floor is not None:
            grid = [F for F in grid if F >= min_floor]
            if not grid:
                # Degenerate case: the grid lives entirely below the contract
                # floor. Inject the minimum so the optimizer at least has one
                # valid candidate.
                grid = [min_floor]
            debug_rows.append({"demand_id": did, "verdict": "contract_floor_clamp",
                               "min_floor": min_floor, "remaining_grid": grid})

        # Evaluate each candidate floor
        current_point = _portfolio_revenue(samples_by_pub, float(current_floor or 0), lookback_days)
        per_candidate = []
        for F in grid:
            point = _portfolio_revenue(samples_by_pub, F, lookback_days)
            per_candidate.append((F, point))

        # Choose argmax, BUT enforce per-pub loss cap vs current
        best = None
        for F, point in per_candidate:
            # per-pub check
            ok = True
            for pid, s in samples_by_pub.items():
                cur_pub = _predict_rev(s, float(current_floor or 0), lookback_days)
                new_pub = _predict_rev(s, F, lookback_days)
                if cur_pub > 5 and new_pub < cur_pub * (1 - PER_PUB_LOSS_CAP_PCT):
                    ok = False
                    break
            if not ok:
                continue
            if best is None or point > best[1]:
                best = (F, point)
        if best is None:
            debug_rows.append({"demand_id": did, "verdict": "no_candidate_within_loss_cap"})
            continue
        F_star, rev_star = best
        gross_lift = rev_star - current_point

        if gross_lift < MIN_ABSOLUTE_WEEKLY_LIFT:
            debug_rows.append({"demand_id": did, "verdict": "lift_under_min",
                               "gross_lift": round(gross_lift, 2)})
            continue
        if current_point > 0 and gross_lift / current_point < MIN_RELATIVE_LIFT:
            debug_rows.append({"demand_id": did, "verdict": "relative_lift_too_small",
                               "rel": round(gross_lift / current_point, 4)})
            continue

        # Bootstrap CI on the portfolio-level gross_lift
        ci_low, ci_high = _bootstrap_ci(samples_by_pub, float(current_floor or 0),
                                         F_star, lookback_days)
        if ci_low < 0:
            debug_rows.append({"demand_id": did, "verdict": "CI_crosses_zero",
                               "gross_lift": round(gross_lift, 2),
                               "ci_low": round(ci_low, 2)})
            continue

        # Margin: use the revenue-weighted average across pubs
        margin_num, margin_den = 0.0, 0.0
        for pid, s in samples_by_pub.items():
            pub_rev = sum(rev for _, rev in s)
            m_pct = margins.get(pid, {}).get("margin_pct", DEFAULT_MARGIN_PCT)
            margin_num += pub_rev * m_pct
            margin_den += pub_rev
        margin_pct = (margin_num / margin_den) if margin_den > 0 else DEFAULT_MARGIN_PCT
        net_lift = gross_lift * (margin_pct / 100.0)

        # Demand/pub names
        demand_name = _demand_name(rows, did)
        # Per-pub breakdown for reviewer sanity
        breakdown = []
        for pid, s in samples_by_pub.items():
            cur_p = _predict_rev(s, float(current_floor or 0), lookback_days)
            new_p = _predict_rev(s, F_star, lookback_days)
            breakdown.append({
                "pub_id": pid, "pub_name": names.get(pid, ""),
                "cur_weekly_rev": round(cur_p, 2),
                "new_weekly_rev": round(new_p, 2),
                "delta": round(new_p - cur_p, 2),
            })
        breakdown.sort(key=lambda b: -abs(b["delta"]))

        p = Proposal(
            id=_new_proposal_id(),
            ts_utc=_now_iso(),
            publisher_id=0,                           # sentinel: portfolio-level
            publisher_name=f"[portfolio: {len(pub_ids)} pubs]",
            demand_id=did,
            demand_name=demand_name,
            current_floor=current_floor,
            proposed_floor=round(float(F_star), 4),
            expected_weekly_net_lift=round(net_lift, 2),
            expected_weekly_gross_lift=round(gross_lift, 2),
            ci_low_net=round(ci_low * margin_pct / 100.0, 2),
            ci_high_net=round(ci_high * margin_pct / 100.0, 2),
            margin_pct=round(margin_pct, 2),
            current_weekly_rev=round(current_point, 2),
            proposed_weekly_rev=round(rev_star, 2),
            confidence="high" if ci_low > gross_lift * 0.5 and all(
                len(s) >= MIN_PUB_SAMPLES for s in samples_by_pub.values()
            ) else "medium",
            reason=(f"portfolio across {len(pub_ids)} pubs; "
                    f"F* chosen from {len(grid)}-point grid, "
                    f"per-pub loss cap {int(PER_PUB_LOSS_CAP_PCT*100)}% held"),
            demand_runs_on_n_pubs=len(pub_ids),
            multi_pub_acknowledged=True,      # ← this is the key bit
        )
        # Attach breakdown to proposal (extra field; Proposal is a dataclass
        # but the proposer serializes to dict via asdict()+setdefault so we
        # store breakdown in a sibling debug file keyed by proposal id.)
        proposals.append(p)
        debug_rows.append({
            "demand_id": did, "verdict": "proposed",
            "proposal_id": p.id, "breakdown": breakdown,
        })

    PORTFOLIO_DEBUG_PATH.write_text(json.dumps({
        "generated_utc": _now_iso(),
        "multi_pub_demand_count": len(multi),
        "rows": debug_rows,
    }, indent=2))
    return proposals


def _portfolio_revenue(samples_by_pub, floor: float, lookback_days: int) -> float:
    return sum(_predict_rev(s, floor, lookback_days) for s in samples_by_pub.values())


def _bootstrap_ci(samples_by_pub, cur_floor, new_floor, lookback_days) -> tuple[float, float]:
    boot = []
    for _ in range(BOOTSTRAP_ITERATIONS):
        resampled = {
            pid: [s[random.randrange(len(s))] for _ in range(len(s))]
            for pid, s in samples_by_pub.items() if s
        }
        cur = _portfolio_revenue(resampled, cur_floor, lookback_days)
        new = _portfolio_revenue(resampled, new_floor, lookback_days)
        boot.append(new - cur)
    boot.sort()
    return boot[int(BOOTSTRAP_ITERATIONS * 0.1)], boot[int(BOOTSTRAP_ITERATIONS * 0.9)]


def _demand_name(rows: list[dict], demand_id: int) -> str:
    for r in rows:
        if int(r.get("DEMAND_ID", 0)) == demand_id:
            return r.get("DEMAND_NAME", "") or ""
    return ""


def _safe_margin_fetch() -> dict[int, dict]:
    try:
        return margin.get_publisher_margins(lookback_days=30)
    except Exception as e:
        print(f"[portfolio] margin fetch failed: {e} — default {DEFAULT_MARGIN_PCT}%")
        return {}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--lookback", type=int, default=14)
    ap.add_argument("--no-margin", action="store_true")
    args = ap.parse_args()
    props = generate(lookback_days=args.lookback, dry_margin_fetch=args.no_margin)
    print(f"generated {len(props)} portfolio proposals")
    for p in props[:20]:
        print(f"  demand={p.demand_id:<5} ({p.demand_name[:32]:<34}) "
              f"{p.current_floor} → {p.proposed_floor}  "
              f"E[net/wk]=+${p.expected_weekly_net_lift:.0f}  "
              f"CI[${p.ci_low_net:+.0f}…${p.ci_high_net:+.0f}]  "
              f"pubs={p.demand_runs_on_n_pubs}  [{p.confidence}]")


if __name__ == "__main__":
    main()
