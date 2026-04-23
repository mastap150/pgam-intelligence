"""
agents/optimization/auto_revert_harmful.py

Every 4 hours, scans recent floor-ledger writes and reverts any that
correlate with a meaningful per-demand revenue drop post-change. Designed
to prevent the kind of silent bleeding we saw on 2026-04-18 when the
portfolio optimizer dropped 9 Dots floors to $0 and nobody noticed for
24+ hours.

Algorithm
---------
For each applied floor change in the last REVERT_WINDOW_HOURS:
  1. Measure pre-change 48h revenue on that demand (all pubs)
  2. Measure post-change revenue from the change-time to now
  3. Normalize to the same time window for fair comparison
  4. If post is DROP_THRESHOLD_PCT below pre AND pre was >= MIN_PRE_REV,
     revert to the pre-change floor value
  5. Skip if the write actor itself is an auto-revert (no double-reverts)
  6. Skip contract-protected demands (the clamp already protects them)
  7. Skip if we already reverted this specific ledger entry in past runs

Safety posture
--------------
- Maximum MAX_REVERTS_PER_RUN per cycle
- Only considers writes that actually stuck (verify live == ledger new_floor)
- Reverts via set_demand_floor (inherits clamp, ledger, verify)
- New ledger entry with actor="auto_revert_harmful_<date>" and
  reverted_from=<original_ledger_id> so we can't loop on ourselves
"""
from __future__ import annotations

import gzip
import json
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

from core import floor_ledger, ll_mgmt
from core.ll_mgmt import PROTECTED_FLOOR_MINIMUMS

DATA_DIR = Path(__file__).parent.parent.parent / "data"
HOURLY_PATH = DATA_DIR / "hourly_pub_demand.json.gz"

REVERT_WINDOW_HOURS = 48        # only reconsider writes in this window
MEASURE_HOURS_PRE = 48          # rev sample size pre-change
DROP_THRESHOLD_PCT = 0.20       # >20% drop = revert
MIN_PRE_REV = 30.0              # don't bother with tiny-revenue tuples
MIN_POST_HOURS = 6              # need at least this many hours of post-data
MAX_REVERTS_PER_RUN = 3

ACTOR_PREFIX = "auto_revert_harmful"
SELF_ACTOR_TOKENS = ("auto_revert", "contract_floor_sentry", "9dots_contract_restore")


def _is_protected_demand(name: str) -> bool:
    """Check if a demand name matches any protected contract token."""
    name_lower = (name or "").lower()
    for tokens, _ in PROTECTED_FLOOR_MINIMUMS:
        if any(t in name_lower for t in tokens):
            return True
    return False


def _already_reverted(ledger_entry: dict, all_rows: list[dict]) -> bool:
    """Check if we've already auto-reverted this specific ledger entry."""
    target_id = ledger_entry.get("id")
    for r in all_rows:
        if r.get("actor", "").startswith(ACTOR_PREFIX) and r.get("reverted_from") == target_id:
            return True
    return False


def _revenue_for_demand(hourly: list[dict], demand_id: int,
                       start_iso: str, end_iso: str) -> tuple[float, int]:
    """Sum revenue for a demand across [start, end] UTC. Returns (revenue, hour_count)."""
    from datetime import datetime as _dt
    start_dt = _dt.fromisoformat(start_iso.replace("Z", "+00:00"))
    end_dt = _dt.fromisoformat(end_iso.replace("Z", "+00:00"))
    rev = 0.0
    hours_seen: set[tuple[str, int]] = set()
    for r in hourly:
        if int(r.get("DEMAND_ID", 0) or 0) != demand_id:
            continue
        d = str(r.get("DATE", ""))
        h = int(r.get("HOUR", 0) or 0)
        if not d:
            continue
        row_dt = _dt(year=int(d[:4]), month=int(d[5:7]), day=int(d[8:10]),
                     hour=h, tzinfo=timezone.utc)
        if start_dt <= row_dt <= end_dt:
            rev += float(r.get("GROSS_REVENUE", 0) or 0)
            hours_seen.add((d, h))
    return rev, len(hours_seen)


def run() -> dict:
    """Scheduler entry."""
    now = datetime.now(timezone.utc)
    actor = f"{ACTOR_PREFIX}_{now.strftime('%Y%m%d')}"

    if not HOURLY_PATH.exists():
        return {"skipped": True, "reason": "hourly data missing"}

    with gzip.open(HOURLY_PATH, "rt") as f:
        hourly = json.load(f)

    all_ledger = floor_ledger.read_all()

    # Recent applied writes (excluding our own)
    window_cutoff = (now - timedelta(hours=REVERT_WINDOW_HOURS)).isoformat()
    candidates = []
    for r in all_ledger:
        if r.get("ts_utc", "") < window_cutoff:
            continue
        if not r.get("applied") or r.get("dry_run"):
            continue
        actor_lc = (r.get("actor", "") or "").lower()
        if any(tok in actor_lc for tok in SELF_ACTOR_TOKENS):
            continue
        # Only consider writes that actually changed the floor
        if r.get("old_floor") == r.get("new_floor"):
            continue
        candidates.append(r)

    print(f"[{actor}] {len(candidates)} floor-write candidates in last {REVERT_WINDOW_HOURS}h")

    reverts = []
    for c in candidates:
        if len(reverts) >= MAX_REVERTS_PER_RUN:
            break
        did = c.get("demand_id")
        if not did:
            continue
        if _is_protected_demand(c.get("demand_name", "")):
            continue  # contract clamp already protects these
        if _already_reverted(c, all_ledger):
            continue

        # Verify the write actually stuck (we don't want to revert a ghost)
        try:
            live = ll_mgmt._get(f"/v1/demands/{did}").get("minBidFloor")
        except Exception:
            continue
        new_floor = c.get("new_floor")
        if new_floor is not None and live is not None:
            if abs(float(live) - float(new_floor)) > 0.01:
                continue  # write didn't stick, nothing to revert
        elif not (new_floor is None and live is None):
            continue

        # Measure pre/post revenue
        ts_iso = c["ts_utc"]
        ts_dt = datetime.fromisoformat(ts_iso.replace("Z", "+00:00"))
        pre_start = (ts_dt - timedelta(hours=MEASURE_HOURS_PRE)).isoformat()
        pre_end = ts_iso
        post_start = ts_iso
        post_end = now.isoformat()
        pre_rev, pre_hrs = _revenue_for_demand(hourly, did, pre_start, pre_end)
        post_rev, post_hrs = _revenue_for_demand(hourly, did, post_start, post_end)

        if pre_hrs == 0 or post_hrs == 0:
            continue
        # Normalize to hourly rate for fair comparison
        pre_rate = pre_rev / pre_hrs
        post_rate = post_rev / post_hrs
        pre_extrap = pre_rate * MEASURE_HOURS_PRE  # extrapolate to window

        if pre_extrap < MIN_PRE_REV:
            continue
        if post_hrs < MIN_POST_HOURS:
            continue

        ratio = post_rate / pre_rate if pre_rate > 0 else 1.0
        if ratio >= (1 - DROP_THRESHOLD_PCT):
            continue  # not a meaningful drop

        # Revert
        target_floor = c.get("old_floor")
        print(f"[{actor}] REVERTING: demand={did} ({c.get('demand_name','')[:40]})")
        print(f"  pre: ${pre_rev:.2f} over {pre_hrs}h  (rate ${pre_rate:.2f}/h)")
        print(f"  post: ${post_rev:.2f} over {post_hrs}h  (rate ${post_rate:.2f}/h)")
        print(f"  ratio: {ratio:.2f} -> REVERT {live} -> {target_floor}")

        try:
            result = ll_mgmt.set_demand_floor(
                did, target_floor,
                verify=True,
                allow_multi_pub=True,
                _publishers_running_it=10,
            )
            ledger_entry = floor_ledger.record(
                publisher_id=0, publisher_name="[auto-revert]",
                demand_id=did, demand_name=c.get("demand_name", ""),
                old_floor=live, new_floor=target_floor,
                actor=actor,
                reason=(f"Auto-revert of ledger {c.get('id')} by {c.get('actor')}. "
                        f"pre=${pre_rate:.2f}/h, post=${post_rate:.2f}/h, ratio={ratio:.2f}"),
                dry_run=False, applied=True,
            )
            # Annotate with reverted_from so we don't re-revert
            # (we can't edit the prior record but we can add the linking key here)
            ledger_entry["reverted_from"] = c.get("id")
            reverts.append({"original_id": c.get("id"),
                            "demand_id": did,
                            "pre_rate": round(pre_rate, 2),
                            "post_rate": round(post_rate, 2),
                            "ratio": round(ratio, 3),
                            "result": result})
        except Exception as e:
            print(f"[{actor}] revert FAILED for demand {did}: {e}")

    return {
        "ran_at": now.isoformat(),
        "candidates_examined": len(candidates),
        "reverts": len(reverts),
        "details": reverts,
    }


if __name__ == "__main__":
    print(json.dumps(run(), indent=2, default=str))
