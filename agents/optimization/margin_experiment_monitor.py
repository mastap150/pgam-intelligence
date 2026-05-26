"""
agents/optimization/margin_experiment_monitor.py

A/B watchdog for publisher-level margin experiments. Different from
intervention_journal (which watches demand floor changes) because:

  - Margin changes are at PUBLISHER level, not DEMAND level
  - The signal that matters is NET CONTRIBUTION to PGAM
    (gross × realized_margin), not raw gross revenue
  - DSPs may take longer to adapt to margin changes than floor changes
    (payout-side, not bid-side)

How margin experiments are ledgered
-----------------------------------
Margin changes reuse the floor_ledger schema by:
  - actor              = "margin_experiment_<pubname>_<date>"
  - publisher_id       = the actual pub id (not 0)
  - demand_id          = 0
  - demand_name        = "[margin-experiment]"
  - old_floor          = pre-change margin as fraction (e.g. 0.15)
  - new_floor          = post-change margin as fraction (e.g. 0.18)

This lets us reuse floor_ledger.read_all() / record() without adding a
new audit log file. The monitor identifies margin entries by the actor
prefix.

Verdict logic
-------------
Age <48h:                 skip (too young, DSPs still adapting)
Age 48h-7d:               provisional — revert only if net_drop > 15%
Age 7d+:                  full verdict — revert if net_drop > 8%

NET contribution per day (pre vs post) is the canonical signal:
  net_per_day = (gross_per_day) × (realized_margin)

Note we use REALIZED margin not contracted — because that's what
actually flows to PGAM.

Safety posture
--------------
- Max 1 revert per run (margin changes are big — be cautious)
- All evaluations + reverts ledgered with linkage to the original entry
- Idempotent: an already-evaluated entry is detected and skipped
- Slacks winners + losers per cycle if anything changed
"""
from __future__ import annotations

import gzip
import json
from collections import defaultdict
from datetime import datetime, timedelta, timezone, date
from pathlib import Path

from core import floor_ledger, ll_mgmt, slack

DATA_DIR = Path(__file__).parent.parent.parent / "data"
HOURLY_PATH = DATA_DIR / "hourly_pub_demand.json.gz"

# Observation windows
MIN_OBSERVATION_HOURS = 48        # don't evaluate younger than this
EVAL_PROVISIONAL_HOURS = 168      # 7d — switch from provisional to final verdict at this age
LOOKBACK_DAYS = 30                # how far back we scan ledger for entries

# Pre-baseline window (used to compute "what would have happened")
PRE_BASELINE_DAYS = 14            # 14d before the change

# Verdict thresholds (NET contribution per day)
PROVISIONAL_LOSER_RATIO = 0.85    # at 48h-7d age: revert if net drops >15%
FINAL_LOSER_RATIO = 0.92          # at 7d+ age: revert if net drops >8%
WINNER_RATIO = 1.10               # net up 10%+ = clear winner (log, keep)

MAX_REVERTS_PER_RUN = 1           # cautious — these are big knobs
ACTOR_PREFIX = "margin_experiment"


def _is_margin_experiment(entry: dict) -> bool:
    """True if a ledger entry is a margin experiment we should watch."""
    actor = (entry.get("actor", "") or "").lower()
    return actor.startswith(ACTOR_PREFIX) and "_revert" not in actor and "_eval" not in actor


def _already_evaluated(ledger_id: str, all_rows: list[dict]) -> bool:
    """Has this margin entry been evaluated (logged or reverted) in a prior run?

    Primary check: structured linkage fields (evaluated_from / reverted_from),
    now persisted by floor_ledger.record(). Fallback: scan the reason text for
    the ledger_id, which covers historical entries written before the linkage
    fields existed.
    """
    if not ledger_id:
        return False
    for r in all_rows:
        if r.get("evaluated_from") == ledger_id:
            return True
        if r.get("reverted_from") == ledger_id:
            return True
        # Fallback for pre-linkage entries: our eval/revert entries embed the
        # source ledger_id in the reason text ("Evaluated margin <id>" / "Auto-revert margin <id>")
        actor = (r.get("actor", "") or "")
        if actor.startswith(ACTOR_PREFIX) and ("_eval" in actor or "_revert" in actor):
            if ledger_id in (r.get("reason", "") or ""):
                return True
    return False


def _per_pub_metrics(hourly: list[dict], pub_id: int,
                    start: datetime, end: datetime) -> dict:
    """Compute gross, payout, imps, bids, wins for a pub in a time window."""
    g = 0.0
    p = 0.0
    imp = 0
    bids = 0
    wins = 0
    seen_days = set()
    for r in hourly:
        if int(r.get("PUBLISHER_ID", 0) or 0) != pub_id:
            continue
        d = str(r.get("DATE", ""))
        h = int(r.get("HOUR", 0) or 0)
        if not d:
            continue
        try:
            row_dt = datetime(int(d[:4]), int(d[5:7]), int(d[8:10]),
                              h, tzinfo=timezone.utc)
        except (ValueError, TypeError):
            continue
        if start <= row_dt <= end:
            g += float(r.get("GROSS_REVENUE", 0) or 0)
            p += float(r.get("PUB_PAYOUT", 0) or 0)
            imp += int(r.get("IMPRESSIONS", 0) or 0)
            bids += int(r.get("BIDS", 0) or 0)
            wins += int(r.get("WINS", 0) or 0)
            seen_days.add(d)
    n_days = max(1, len(seen_days))
    realized_mgn = ((g - p) / g) if g else 0.0
    net = g - p
    return {
        "gross": g,
        "payout": p,
        "net": net,
        "imps": imp,
        "bids": bids,
        "wins": wins,
        "n_days": n_days,
        "gross_per_day": g / n_days,
        "net_per_day": net / n_days,
        "imp_per_day": imp / n_days,
        "realized_margin": realized_mgn,
        "win_rate": (wins / bids) if bids else 0,
    }


def evaluate_entry(entry: dict, hourly: list[dict],
                   all_ledger: list[dict]) -> dict | None:
    """Return a decision dict for a margin entry, or None if not evaluable."""
    now = datetime.now(timezone.utc)
    ts_iso = entry["ts_utc"]
    ts_dt = datetime.fromisoformat(ts_iso.replace("Z", "+00:00"))
    age_hours = (now - ts_dt).total_seconds() / 3600

    if age_hours < MIN_OBSERVATION_HOURS:
        return None  # too young

    if _already_evaluated(entry.get("id"), all_ledger):
        return None

    pub_id = entry.get("publisher_id")
    if not pub_id:
        return None

    old_margin = entry.get("old_floor")  # margin stored in old_floor slot
    new_margin = entry.get("new_floor")
    if old_margin is None or new_margin is None:
        return None

    # Pre-window: PRE_BASELINE_DAYS before the change
    pre_start = ts_dt - timedelta(days=PRE_BASELINE_DAYS)
    pre_end = ts_dt
    pre = _per_pub_metrics(hourly, pub_id, pre_start, pre_end)

    # Post-window: change → now
    post_start = ts_dt
    post_end = now
    post = _per_pub_metrics(hourly, pub_id, post_start, post_end)

    if pre["n_days"] < 3 or post["n_days"] < 2:
        return None  # not enough data either side

    # NET contribution per day is the canonical signal
    pre_net = pre["net_per_day"]
    post_net = post["net_per_day"]
    if pre_net < 0.50:  # tiny pub, skip
        return None
    net_ratio = post_net / pre_net

    # Volume-rate change (impressions/day) — secondary signal
    vol_ratio = (post["imp_per_day"] / pre["imp_per_day"]) if pre["imp_per_day"] else 1.0

    # Verdict — escalates with age
    if age_hours < EVAL_PROVISIONAL_HOURS:
        threshold = PROVISIONAL_LOSER_RATIO  # 0.85 → -15% triggers revert
        eval_stage = "provisional_48h"
    else:
        threshold = FINAL_LOSER_RATIO        # 0.92 → -8% triggers revert
        eval_stage = "final_7d"

    if net_ratio < threshold:
        verdict = "loser"
    elif net_ratio > WINNER_RATIO:
        verdict = "winner"
    else:
        verdict = "neutral"

    return {
        "ledger_id": entry.get("id"),
        "pub_id": pub_id,
        "pub_name": entry.get("publisher_name", ""),
        "actor": entry.get("actor", ""),
        "old_margin_pct": round(float(old_margin) * 100, 1),
        "new_margin_pct": round(float(new_margin) * 100, 1),
        "ts_utc": ts_iso,
        "age_hours": round(age_hours, 1),
        "eval_stage": eval_stage,
        "pre_gross_per_day": round(pre["gross_per_day"], 2),
        "post_gross_per_day": round(post["gross_per_day"], 2),
        "pre_net_per_day": round(pre_net, 2),
        "post_net_per_day": round(post_net, 2),
        "pre_realized_mgn": round(pre["realized_margin"] * 100, 1),
        "post_realized_mgn": round(post["realized_margin"] * 100, 1),
        "net_ratio": round(net_ratio, 3),
        "vol_ratio": round(vol_ratio, 3),
        "verdict": verdict,
    }


def _record_evaluation(d: dict) -> None:
    """Log this evaluation so we don't re-check the same entry."""
    floor_ledger.record(
        publisher_id=d["pub_id"], publisher_name="[margin-eval]",
        demand_id=0, demand_name=d["pub_name"],
        old_floor=None, new_floor=None,
        actor=f"{ACTOR_PREFIX}_eval",
        reason=(f"Evaluated margin {d['ledger_id']} ({d['eval_stage']}): "
                f"verdict={d['verdict']} "
                f"({d['old_margin_pct']}%→{d['new_margin_pct']}%, "
                f"net/d ${d['pre_net_per_day']}→${d['post_net_per_day']}, "
                f"ratio={d['net_ratio']:.2f}, vol_ratio={d['vol_ratio']:.2f})"),
        dry_run=False, applied=True,
        evaluated_from=d["ledger_id"],
    )


def _execute_revert(d: dict) -> bool:
    """Revert publisher margin to its pre-change value."""
    pid = d["pub_id"]
    target_margin = d["old_margin_pct"] / 100.0
    try:
        p = ll_mgmt._get(f"/v1/publishers/{pid}")
        before = p.get("margin")
        p["margin"] = target_margin
        ll_mgmt._put(f"/v1/publishers/{pid}", p)
        after_p = ll_mgmt._get(f"/v1/publishers/{pid}")
        after = after_p.get("margin")
        if after is None or abs(float(after) - target_margin) > 0.001:
            print(f"[{ACTOR_PREFIX}] REVERT PUT didn't stick on pub {pid}: "
                  f"expected {target_margin}, got {after}")
            return False
        floor_ledger.record(
            publisher_id=pid, publisher_name=d["pub_name"],
            demand_id=0, demand_name="[margin-experiment-revert]",
            old_floor=float(before) if before is not None else None,
            new_floor=target_margin,
            actor=f"{ACTOR_PREFIX}_revert",
            reason=(f"Auto-revert margin {d['ledger_id']}: "
                    f"verdict=loser ({d['eval_stage']}), "
                    f"net/d ${d['pre_net_per_day']}→${d['post_net_per_day']} "
                    f"(ratio {d['net_ratio']:.2f}). "
                    f"Restored {d['new_margin_pct']}%→{d['old_margin_pct']}%."),
            dry_run=False, applied=True,
            reverted_from=d["ledger_id"],
        )
        return True
    except Exception as e:
        print(f"[{ACTOR_PREFIX}] revert FAILED on pub {pid}: {e}")
        return False


def run() -> dict:
    """Scheduler entry."""
    now = datetime.now(timezone.utc)
    if not HOURLY_PATH.exists():
        return {"skipped": True, "reason": "no hourly data"}

    with gzip.open(HOURLY_PATH, "rt") as f:
        hourly = json.load(f)
    all_ledger = floor_ledger.read_all()

    cutoff = (now - timedelta(days=LOOKBACK_DAYS)).isoformat()
    candidates = [r for r in all_ledger
                  if r.get("ts_utc", "") >= cutoff
                  and r.get("applied") and not r.get("dry_run")
                  and _is_margin_experiment(r)]

    print(f"[{ACTOR_PREFIX}] {len(candidates)} margin-experiment ledger entries "
          f"in last {LOOKBACK_DAYS}d")

    decisions = []
    reverted = []
    winners = []
    neutrals = []
    for entry in candidates:
        d = evaluate_entry(entry, hourly, all_ledger)
        if d is None:
            continue
        decisions.append(d)
        _record_evaluation(d)
        if d["verdict"] == "winner":
            winners.append(d)
        elif d["verdict"] == "loser":
            if len(reverted) >= MAX_REVERTS_PER_RUN:
                continue
            if _execute_revert(d):
                reverted.append(d)
        else:
            neutrals.append(d)

    # Slack summary
    if decisions:
        parts = [f":balance_scale: *Margin experiment monitor — {now.strftime('%Y-%m-%d %H:%M UTC')}*"]
        if reverted:
            parts.append(f"\n*🔻 Reverted {len(reverted)} loser(s):*")
            for d in reverted:
                parts.append(f"  • pub `{d['pub_id']}` {d['pub_name'][:35]}: "
                             f"{d['old_margin_pct']}%↔{d['new_margin_pct']}% "
                             f"(net/d ${d['pre_net_per_day']}→${d['post_net_per_day']}, "
                             f"ratio {d['net_ratio']:.2f})")
        if winners:
            parts.append(f"\n*🟢 Winners ({len(winners)}) — keeping:*")
            for d in winners:
                parts.append(f"  • pub `{d['pub_id']}` {d['pub_name'][:35]}: "
                             f"{d['old_margin_pct']}%→{d['new_margin_pct']}% "
                             f"(net/d ${d['pre_net_per_day']}→${d['post_net_per_day']}, "
                             f"ratio {d['net_ratio']:.2f})")
        if neutrals:
            parts.append(f"\n*⚪ Neutral ({len(neutrals)}) — keeping:*")
            for d in neutrals[:3]:
                parts.append(f"  • pub `{d['pub_id']}` {d['pub_name'][:35]}: "
                             f"ratio {d['net_ratio']:.2f}, vol_ratio {d['vol_ratio']:.2f}")
        try:
            slack.send_text("\n".join(parts))
        except Exception as e:
            print(f"[{ACTOR_PREFIX}] Slack post failed: {e}")

    return {
        "ran_at": now.isoformat(),
        "candidates_examined": len(candidates),
        "evaluated": len(decisions),
        "winners": len(winners),
        "neutral": len(neutrals),
        "reverted": len(reverted),
    }


if __name__ == "__main__":
    print(json.dumps(run(), indent=2, default=str))
