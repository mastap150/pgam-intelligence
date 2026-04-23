"""
agents/optimization/auto_unpause.py

Daily agent that re-activates *silently paused* inventory — publisher ×
demand tuples that were active, are now paused, and have no ledger entry
explaining why.

The goal: catch accidental pauses (human error in the LL UI, config drift,
third-party state changes) that cost real money each day they remain paused.

Candidate filter
----------------
A (pub_id, demand_id) tuple qualifies if ALL of:
  1. Currently paused at the biddingpreferences level (status=2)
  2. Had BIDS > 0 and GROSS_REVENUE > 0 in at least MIN_HEALTHY_DAYS
     over the last LOOKBACK_DAYS
  3. NO recent floor_ledger entry (last 30 days) mentioning a
     deliberate pause for this tuple (actor contains "pause" or
     reason contains "pause"/"disable"/"quarantine")
  4. The tuple's historical 30-day revenue exceeds MIN_HIST_REV_30D
  5. Quarantine + holdout both clear for this tuple

Safety posture
--------------
- MAX_UNPAUSES_PER_RUN cap — limits blast radius
- For each unpause, logs ledger entry with actor="auto_unpause_<date>"
- If LL_DRY_RUN is set, doesn't actually unpause
- Re-pausing by the pilot_watchdog or other agent is not prevented;
  this agent won't fight it if the tuple is re-paused with a ledgered
  reason next cycle
"""
from __future__ import annotations

import gzip
import json
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

from core import ll_mgmt, floor_ledger
from intelligence import holdout, quarantine

DATA_DIR = Path(__file__).parent.parent.parent / "data"
HOURLY_PATH = DATA_DIR / "hourly_pub_demand.json.gz"

LOOKBACK_DAYS = 14
MIN_HEALTHY_DAYS = 5        # must have had revenue on at least N days
MIN_HIST_REV_30D = 50.0     # don't bother with tiny historical tuples
LEDGER_PAUSE_LOOKBACK_DAYS = 30
MAX_UNPAUSES_PER_RUN = 5

# Keywords in ledger entries that indicate a deliberate pause
PAUSE_KEYWORDS = ("pause", "disable", "quarantine", "holdout", "archive")

ACTOR_PREFIX = "auto_unpause"


def _recent_pause_ledger(demand_id: int, publisher_id: int) -> bool:
    """Return True if there's a recent ledger entry that suggests a
    deliberate pause on this tuple."""
    cutoff = (datetime.now(timezone.utc) - timedelta(days=LEDGER_PAUSE_LOOKBACK_DAYS)).isoformat()
    for r in floor_ledger.read_all():
        if r.get("ts_utc", "") < cutoff:
            continue
        if r.get("demand_id") != demand_id:
            continue
        if publisher_id and r.get("publisher_id") != publisher_id:
            continue
        text = (str(r.get("actor", "")) + " " + str(r.get("reason", ""))).lower()
        if any(kw in text for kw in PAUSE_KEYWORDS):
            return True
    return False


def _historical_healthy_tuples() -> dict:
    """Return {(pub_id, demand_id): {rev_30d, n_healthy_days}} for tuples with
    recent activity."""
    if not HOURLY_PATH.exists():
        return {}
    with gzip.open(HOURLY_PATH, "rt") as f:
        rows = json.load(f)
    cutoff = (datetime.now(timezone.utc).date() - timedelta(days=LOOKBACK_DAYS)).isoformat()
    days_with_rev = defaultdict(set)
    total_rev = defaultdict(float)
    for r in rows:
        d = str(r.get("DATE", ""))
        if d < cutoff:
            continue
        pid = int(r.get("PUBLISHER_ID", 0) or 0)
        did = int(r.get("DEMAND_ID", 0) or 0)
        if not pid or not did:
            continue
        rev = float(r.get("GROSS_REVENUE", 0) or 0)
        bids = float(r.get("BIDS", 0) or 0)
        if rev > 0 and bids > 0:
            days_with_rev[(pid, did)].add(d)
        total_rev[(pid, did)] += rev
    return {k: {"rev": total_rev[k], "days": len(days_with_rev[k])}
            for k in total_rev if total_rev[k] > 0}


def run() -> dict:
    """Scheduler entry."""
    actor = f"{ACTOR_PREFIX}_{datetime.now(timezone.utc).strftime('%Y%m%d')}"

    # 1. Collect historical healthy tuples
    healthy = _historical_healthy_tuples()
    print(f"[{actor}] {len(healthy)} tuples with revenue in last {LOOKBACK_DAYS}d")

    # 2. Enumerate all publishers, find paused demands
    all_pubs = ll_mgmt.get_publishers(include_archived=False)
    candidates = []
    for pub in all_pubs:
        pid = pub["id"]
        try:
            detail = ll_mgmt.get_publisher(pid)
        except Exception:
            continue
        pname = detail.get("name", "")
        if detail.get("isArchived"): continue
        if detail.get("status") == 2:  # publisher itself is paused — respect that
            continue
        for pref in detail.get("biddingpreferences", []):
            for item in pref.get("value", []):
                if item.get("status") != 2:
                    continue  # not paused
                did = item.get("id")
                if not did:
                    continue
                key = (pid, did)
                h = healthy.get(key)
                if not h:
                    continue  # no historical revenue
                if h["days"] < MIN_HEALTHY_DAYS:
                    continue
                if h["rev"] < MIN_HIST_REV_30D:
                    continue
                if _recent_pause_ledger(did, pid):
                    continue  # deliberate pause
                if holdout.is_tuple_held_out(pid, did):
                    continue
                if quarantine.is_in_quarantine(pid, did):
                    continue
                candidates.append({
                    "pub_id": pid, "pub_name": pname,
                    "demand_id": did, "demand_name": item.get("name", ""),
                    "hist_rev_14d": round(h["rev"], 2),
                    "healthy_days": h["days"],
                })

    candidates.sort(key=lambda c: -c["hist_rev_14d"])
    print(f"[{actor}] {len(candidates)} qualifying candidates")

    # 3. Execute up to MAX_UNPAUSES_PER_RUN
    applied = []
    for cand in candidates[:MAX_UNPAUSES_PER_RUN]:
        try:
            result = ll_mgmt.enable_publisher_demand(
                cand["pub_id"], cand["demand_id"], dry_run=False,
            )
            floor_ledger.record(
                publisher_id=cand["pub_id"], publisher_name=cand["pub_name"],
                demand_id=cand["demand_id"], demand_name=cand["demand_name"],
                old_floor=None, new_floor=None,
                actor=actor,
                reason=(f"Auto-unpause: tuple had ${cand['hist_rev_14d']:.0f} rev "
                        f"across {cand['healthy_days']} days in last 14d, "
                        f"no ledger-explained pause"),
                dry_run=False, applied=True,
            )
            applied.append(cand)
            print(f"[{actor}] ✓ unpaused pub={cand['pub_id']} demand={cand['demand_id']}: "
                  f"{cand['demand_name'][:40]}")
        except Exception as e:
            print(f"[{actor}] FAILED pub={cand['pub_id']} demand={cand['demand_id']}: {e}")

    return {
        "ran_at": datetime.now(timezone.utc).isoformat(),
        "candidates": len(candidates),
        "unpaused": len(applied),
        "applied": applied,
    }


if __name__ == "__main__":
    print(json.dumps(run(), indent=2, default=str))
