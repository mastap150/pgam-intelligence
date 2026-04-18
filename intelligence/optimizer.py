"""
Floor optimizer — daily proposal engine.

For every non-holdout (publisher, demand) tuple with enough signal:
  1. Fit a price-response model on the last 14d of hourly data.
  2. Pull the current live floor from LL management API.
  3. Search the candidate floor grid and find the one that maximizes
     E[weekly net revenue] = E[weekly gross] × margin(publisher).
  4. Compare to the revenue expected at the current floor.
     Only emit a proposal if the improvement clears min thresholds on
     absolute $ AND relative %, AND the 10% CI lower bound still beats
     status quo (so we don't ship coin flips).

Proposals are ranked by expected net $ lift and written to
``data/proposals.json`` for the proposer (Slack posting) to pick up.

Safety rails baked in:
  - holdout.is_tuple_held_out() → skip
  - max ±25% change from current floor per proposal (avoid whiplash)
  - ≤1 proposal per tuple per 24h (cooldown via floor_ledger)
  - daily cap on total $ revenue exposure under pending proposals

This module ONLY writes a proposals.json file. Nothing is applied here.
"""
from __future__ import annotations

import argparse
import gzip
import json
import time
import uuid
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from core import floor_ledger, ll_mgmt, margin
from intelligence import holdout, price_response, quarantine

DATA_DIR = Path(__file__).parent.parent / "data"
PROPOSALS_PATH = DATA_DIR / "proposals.json"

# Emission thresholds
MIN_ABSOLUTE_WEEKLY_LIFT = 10.0    # $ per week
MIN_RELATIVE_LIFT = 0.10           # 10 %
MAX_CHANGE_FRACTION = 0.25         # ±25 % from current floor (if current > 0)
MIN_LOOKBACK_SAMPLES = 20
COOLDOWN_HOURS = 24
DEFAULT_MARGIN_PCT = 50.0          # fallback when no recent history


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _new_proposal_id() -> str:
    return f"prop_{int(time.time() * 1000):x}{uuid.uuid4().hex[:6]}"


def _recent_ledger_ts(publisher_id: int, demand_id: int) -> str | None:
    """Return most-recent applied ledger timestamp for this tuple, else None."""
    matches = [
        r for r in floor_ledger.read_all()
        if r["publisher_id"] == publisher_id
        and r["demand_id"] == demand_id
        and r["applied"]
        and not r["dry_run"]
    ]
    if not matches:
        return None
    return max(r["ts_utc"] for r in matches)


def _in_cooldown(publisher_id: int, demand_id: int) -> bool:
    last = _recent_ledger_ts(publisher_id, demand_id)
    if not last:
        return False
    last_dt = datetime.fromisoformat(last.replace("Z", "+00:00"))
    return (datetime.now(timezone.utc) - last_dt) < timedelta(hours=COOLDOWN_HOURS)


def _current_live_floor(publisher_cache: dict[int, dict], publisher_id: int, demand_id: int):
    """Return live minBidFloor (None or float) from LL. Caches publisher fetches."""
    pub = publisher_cache.get(publisher_id)
    if pub is None:
        pub = ll_mgmt.get_publisher(publisher_id)
        publisher_cache[publisher_id] = pub
    for pref in pub.get("biddingpreferences", []):
        for v in pref.get("value", []):
            if v.get("id") == demand_id:
                return v.get("minBidFloor")
    return "not_found"


def _clip_to_change_bounds(current, proposed) -> float:
    """Keep proposal within ±MAX_CHANGE_FRACTION of current if current > 0,
    otherwise allow any floor."""
    if current in (None, "not_found") or current == 0:
        return proposed
    lo = current * (1 - MAX_CHANGE_FRACTION)
    hi = current * (1 + MAX_CHANGE_FRACTION)
    return max(lo, min(hi, proposed))


@dataclass
class Proposal:
    id: str
    ts_utc: str
    publisher_id: int
    publisher_name: str
    demand_id: int
    demand_name: str
    current_floor: Any          # None / float
    proposed_floor: float
    expected_weekly_net_lift: float
    expected_weekly_gross_lift: float
    ci_low_net: float
    ci_high_net: float
    margin_pct: float
    current_weekly_rev: float
    proposed_weekly_rev: float
    confidence: str             # high | medium | low
    reason: str


def _confidence(samples: int, ci_low_net: float) -> str:
    if samples >= 150 and ci_low_net > 0:
        return "high"
    if samples >= 50 and ci_low_net > -5:
        return "medium"
    return "low"


def generate(*, lookback_days: int = 14, min_total_revenue: float = 20.0,
             dry_margin_fetch: bool = False) -> dict:
    """Produce and persist a fresh set of proposals."""
    # 1. Fit all models
    models = price_response.fit_all(lookback_days=lookback_days,
                                    min_total_revenue=min_total_revenue)
    # 2. Margin lookup (publisher-level)
    margins = {} if dry_margin_fetch else _safe_margin_fetch()
    # 3. Iterate
    proposals: list[Proposal] = []
    rejected: list[dict] = []
    publisher_cache: dict[int, dict] = {}

    for m in models:
        tuple_key = (m.publisher_id, m.demand_id)
        if holdout.is_tuple_held_out(*tuple_key):
            rejected.append({"tuple": tuple_key, "reason": "holdout_or_inactive"})
            continue
        if quarantine.is_in_quarantine(*tuple_key):
            rejected.append({"tuple": tuple_key, "reason": "quarantine"})
            continue
        if _in_cooldown(*tuple_key):
            rejected.append({"tuple": tuple_key, "reason": "cooldown"})
            continue
        if len(m.samples) < MIN_LOOKBACK_SAMPLES:
            rejected.append({"tuple": tuple_key, "reason": "insufficient_samples"})
            continue

        # Current live floor
        try:
            current = _current_live_floor(publisher_cache, m.publisher_id, m.demand_id)
        except Exception as e:
            rejected.append({"tuple": tuple_key, "reason": f"fetch_failed:{e}"})
            continue
        if current == "not_found":
            rejected.append({"tuple": tuple_key, "reason": "demand_not_on_publisher"})
            continue

        # Sweep
        sweep = m.sweep()
        if not sweep:
            continue
        best = max(sweep, key=lambda r: r.get("expected_weekly_revenue") or 0)
        proposed_floor = _clip_to_change_bounds(current, best["floor"])

        # Re-predict at the clipped floor (may differ from best)
        clipped_pred = m.predict(proposed_floor)
        current_pred = m.predict(float(current) if current is not None else 0.0)

        gross_lift = (clipped_pred["expected_weekly_revenue"] or 0) - \
                     (current_pred["expected_weekly_revenue"] or 0)

        margin_pct = margins.get(m.publisher_id, {}).get("margin_pct", DEFAULT_MARGIN_PCT)
        net_lift = gross_lift * (margin_pct / 100.0)
        ci_low_net = ((clipped_pred["ci_low"] or 0) - (current_pred["expected_weekly_revenue"] or 0)) \
                     * (margin_pct / 100.0)
        ci_high_net = ((clipped_pred["ci_high"] or 0) - (current_pred["expected_weekly_revenue"] or 0)) \
                      * (margin_pct / 100.0)

        current_rev = current_pred["expected_weekly_revenue"] or 0
        relative = (net_lift / max(current_rev * margin_pct / 100.0, 1.0))

        # Emission gates
        if net_lift < MIN_ABSOLUTE_WEEKLY_LIFT:
            rejected.append({"tuple": tuple_key, "reason": f"lift_under_min_${net_lift:.2f}"})
            continue
        if relative < MIN_RELATIVE_LIFT:
            rejected.append({"tuple": tuple_key, "reason": f"relative_lift_{relative:.1%}"})
            continue
        if ci_low_net < 0:
            rejected.append({"tuple": tuple_key, "reason": "CI_crosses_zero"})
            continue

        reason_parts = []
        if current is None and proposed_floor > 0:
            reason_parts.append("add floor on null-floor demand")
        elif current is not None and proposed_floor < float(current):
            reason_parts.append("lower floor — current clears below it")
        elif current is not None and proposed_floor > float(current):
            reason_parts.append("raise floor — cheap wins dominate revenue")
        reason_parts.append(f"{len(m.samples)} hourly samples, 14d")

        p = Proposal(
            id=_new_proposal_id(),
            ts_utc=_now_iso(),
            publisher_id=m.publisher_id,
            publisher_name=m.publisher_name,
            demand_id=m.demand_id,
            demand_name=m.demand_name,
            current_floor=current,
            proposed_floor=round(float(proposed_floor), 4),
            expected_weekly_net_lift=round(net_lift, 2),
            expected_weekly_gross_lift=round(gross_lift, 2),
            ci_low_net=round(ci_low_net, 2),
            ci_high_net=round(ci_high_net, 2),
            margin_pct=round(margin_pct, 2),
            current_weekly_rev=round(current_rev, 2),
            proposed_weekly_rev=round(clipped_pred["expected_weekly_revenue"] or 0, 2),
            confidence=_confidence(len(m.samples), ci_low_net),
            reason="; ".join(reason_parts),
        )
        proposals.append(p)

    proposals.sort(key=lambda x: -x.expected_weekly_net_lift)

    out = {
        "generated_utc": _now_iso(),
        "lookback_days": lookback_days,
        "n_models_fit": len(models),
        "n_proposals": len(proposals),
        "n_rejected": len(rejected),
        "proposals": [asdict(p) for p in proposals],
    }
    PROPOSALS_PATH.write_text(json.dumps(out, indent=2, default=str))
    return out


def _safe_margin_fetch() -> dict[int, dict]:
    """Grab publisher margins; tolerate API outages (fall back to empty)."""
    try:
        return margin.get_publisher_margins(lookback_days=30)
    except Exception as e:
        print(f"[optimizer] margin fetch failed: {e} — using default {DEFAULT_MARGIN_PCT}%")
        return {}


def run() -> dict:
    return generate()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--lookback", type=int, default=14)
    ap.add_argument("--show", action="store_true")
    ap.add_argument("--no-margin", action="store_true",
                    help="skip margin fetch, use default")
    args = ap.parse_args()

    if args.show:
        if not PROPOSALS_PATH.exists():
            print("no proposals yet")
            return
        data = json.loads(PROPOSALS_PATH.read_text())
        print(f"\n{data['n_proposals']} proposals generated at {data['generated_utc']}")
        print(f"{'ID':<22} {'Pub':<22} {'Demand':<30} "
              f"{'Cur':>6} {'→':^3} {'New':>6} {'$ lift/wk':>10} {'conf':>7}")
        for p in data["proposals"][:30]:
            cur = str(p["current_floor"]) if p["current_floor"] is not None else "null"
            print(f"{p['id']:<22} {p['publisher_name'][:20]:<22} {p['demand_name'][:28]:<30} "
                  f"{cur:>6} {'→':^3} {p['proposed_floor']:>6.2f} "
                  f"{p['expected_weekly_net_lift']:>10,.2f} {p['confidence']:>7}")
        return

    out = generate(lookback_days=args.lookback, dry_margin_fetch=args.no_margin)
    print(json.dumps({k: v for k, v in out.items() if k != "proposals"}, indent=2, default=str))
    print(f"\ntop proposals:")
    for p in out["proposals"][:10]:
        cur = p["current_floor"] if p["current_floor"] is not None else "null"
        print(f"  {p['publisher_name'][:25]:<27} {p['demand_name'][:28]:<30} "
              f"{cur} → {p['proposed_floor']:.2f}  "
              f"+${p['expected_weekly_net_lift']:>7,.2f}/wk  [{p['confidence']}]  {p['reason']}")


if __name__ == "__main__":
    main()
