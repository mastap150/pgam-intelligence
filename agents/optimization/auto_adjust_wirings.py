"""
agents/optimization/auto_adjust_wirings.py

Every 6 hours, reviews performance of demand-wirings added in the last 7 days
and auto-adjusts any that aren't working:

  - REMOVE: if a (pub, demand) wiring has substantial bids but essentially
    zero revenue after warmup, remove it from the publisher's biddingprefs
    (DSP doesn't value this inventory)
  - SHRINK (per-demand floor): not applied here — floors are demand-global
    and would affect other pubs running the same demand. Floor adjustments
    go through the weekly review flow.

Why this exists
---------------
Adding new demands to pubs is additive and safe — but sometimes a wiring
doesn't ramp. Pubmatic wirings 54/45 on pub 290115340 cleared at $0.03 eCPM
for 3 days before we manually set a $0.10 floor on 2026-04-23 to filter.
This agent catches the "bid volume but no revenue" pattern automatically.

Adjustment criteria
-------------------
Wiring qualifies for REMOVAL if ALL:
  1. Wiring is >=48h old (warmup period — DSP integration takes time)
  2. Has accumulated >=50,000 bids in the last 24h (enough signal)
  3. Revenue <=$0.50 total in the last 24h (clearly not paying off)
  4. eCPM (where wins > 0) <=$0.02 (sub-penny clears)
  5. Not a protected-contract demand (9 Dots etc.)

Safety posture
--------------
- MAX_ADJUSTMENTS_PER_RUN cap
- Full verification + ledger trail
- Ledger entries tagged actor="auto_adjust_wirings_<date>" so auto_revert
  won't try to revert these (no double-undo loops)
"""
from __future__ import annotations

import copy
import gzip
import json
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

from core import ll_mgmt, floor_ledger
from core.ll_mgmt import PROTECTED_FLOOR_MINIMUMS

DATA_DIR = Path(__file__).parent.parent.parent / "data"
HOURLY_PATH = DATA_DIR / "hourly_pub_demand.json.gz"

# Lookback for wirings to evaluate
WIRING_LOOKBACK_DAYS = 7

# Warmup: don't evaluate wirings younger than this
WARMUP_HOURS = 48

# Evaluation window
EVAL_HOURS = 24

# Removal thresholds — wiring must meet ALL to be removed
REMOVE_MIN_BIDS = 50_000
REMOVE_MAX_REV = 0.50
REMOVE_MAX_ECPM = 0.02

MAX_ADJUSTMENTS_PER_RUN = 3

# Actor tokens that indicate a wiring action (source entries to evaluate)
WIRING_ACTOR_TOKENS = ("wire", "wiring", "activate", "unpause")

ACTOR_PREFIX = "auto_adjust_wirings"


def _is_protected(name: str) -> bool:
    name_lower = (name or "").lower()
    for tokens, _ in PROTECTED_FLOOR_MINIMUMS:
        if any(tok in name_lower for tok in tokens):
            return True
    return False


def _recent_wirings(all_ledger: list[dict]) -> list[tuple[int, int, dict]]:
    """Return list of (pub_id, demand_id, ledger_entry) for wirings in window."""
    cutoff = (datetime.now(timezone.utc) - timedelta(days=WIRING_LOOKBACK_DAYS)).isoformat()
    out = []
    for r in all_ledger:
        if r.get("ts_utc", "") < cutoff:
            continue
        if not r.get("applied") or r.get("dry_run"):
            continue
        actor = (r.get("actor", "") or "").lower()
        if not any(tok in actor for tok in WIRING_ACTOR_TOKENS):
            continue
        # Wirings have old_floor=None AND new_floor=None (pure adds, not floor changes)
        if r.get("old_floor") is not None or r.get("new_floor") is not None:
            continue
        pid = int(r.get("publisher_id", 0) or 0)
        did = int(r.get("demand_id", 0) or 0)
        if pid and did:
            out.append((pid, did, r))
    return out


def _tuple_stats(hourly: list[dict], pub_id: int, demand_id: int,
                 since_iso: str, until_iso: str) -> dict:
    """Sum bids/wins/rev/imps for a tuple in [since, until]."""
    from datetime import datetime as _dt
    since = _dt.fromisoformat(since_iso.replace("Z", "+00:00"))
    until = _dt.fromisoformat(until_iso.replace("Z", "+00:00"))
    stats = {"bids": 0.0, "wins": 0.0, "rev": 0.0, "imps": 0.0}
    for r in hourly:
        if int(r.get("PUBLISHER_ID", 0) or 0) != pub_id: continue
        if int(r.get("DEMAND_ID", 0) or 0) != demand_id: continue
        d = str(r.get("DATE", ""))
        h = int(r.get("HOUR", 0) or 0)
        if not d: continue
        row_dt = _dt(year=int(d[:4]), month=int(d[5:7]), day=int(d[8:10]),
                     hour=h, tzinfo=timezone.utc)
        if since <= row_dt <= until:
            stats["bids"] += float(r.get("BIDS", 0) or 0)
            stats["wins"] += float(r.get("WINS", 0) or 0)
            stats["rev"]  += float(r.get("GROSS_REVENUE", 0) or 0)
            stats["imps"] += float(r.get("IMPRESSIONS", 0) or 0)
    return stats


def _remove_wiring_from_pub(pub_id: int, demand_id: int) -> bool:
    """Remove a demand entry from a publisher's biddingpreferences. Returns
    True if the demand was found and the PUT succeeded, False otherwise."""
    pub = ll_mgmt.get_publisher(pub_id)
    new_pub = copy.deepcopy(pub)
    removed = False
    for pref in new_pub.get("biddingpreferences", []):
        before = len(pref.get("value", []))
        pref["value"] = [v for v in pref.get("value", []) if v.get("id") != demand_id]
        if len(pref["value"]) < before:
            removed = True
    if not removed:
        return False
    ll_mgmt._put(f"/v1/publishers/{pub_id}", new_pub)
    # Verify
    after = ll_mgmt.get_publisher(pub_id)
    for pref in after.get("biddingpreferences", []):
        for v in pref.get("value", []):
            if v.get("id") == demand_id:
                return False  # still present = PUT didn't stick
    return True


def run() -> dict:
    """Scheduler entry."""
    now = datetime.now(timezone.utc)
    actor = f"{ACTOR_PREFIX}_{now.strftime('%Y%m%d')}"

    if not HOURLY_PATH.exists():
        return {"skipped": True, "reason": "hourly data missing"}
    with gzip.open(HOURLY_PATH, "rt") as f:
        hourly = json.load(f)

    all_ledger = floor_ledger.read_all()
    wirings = _recent_wirings(all_ledger)
    print(f"[{actor}] {len(wirings)} wirings in last {WIRING_LOOKBACK_DAYS}d")

    eval_start = (now - timedelta(hours=EVAL_HOURS)).isoformat()
    eval_end = now.isoformat()

    removed = []
    kept = 0
    for pid, did, entry in wirings:
        if len(removed) >= MAX_ADJUSTMENTS_PER_RUN:
            break
        dname = entry.get("demand_name", "")
        # Protect contract demands — never auto-remove these
        if _is_protected(dname):
            continue
        # Warmup check
        wired_at = datetime.fromisoformat(entry["ts_utc"].replace("Z", "+00:00"))
        age_hours = (now - wired_at).total_seconds() / 3600
        if age_hours < WARMUP_HOURS:
            continue

        # Evaluate last 24h stats
        stats = _tuple_stats(hourly, pid, did, eval_start, eval_end)
        ecpm = (stats["rev"] / stats["wins"] * 1000) if stats["wins"] else 0.0
        meets_remove = (
            stats["bids"] >= REMOVE_MIN_BIDS
            and stats["rev"] <= REMOVE_MAX_REV
            and (stats["wins"] == 0 or ecpm <= REMOVE_MAX_ECPM)
        )
        if not meets_remove:
            kept += 1
            continue

        print(f"[{actor}] removing wiring pub={pid} demand={did} "
              f"(24h: {stats['bids']:,.0f} bids, {stats['wins']:,.0f} wins, "
              f"${stats['rev']:.2f} rev, ${ecpm:.3f} eCPM): {dname[:35]}")
        try:
            ok = _remove_wiring_from_pub(pid, did)
            if ok:
                floor_ledger.record(
                    publisher_id=pid, publisher_name=entry.get("publisher_name", ""),
                    demand_id=did, demand_name=dname,
                    old_floor=None, new_floor=None,
                    actor=actor,
                    reason=(f"Removed wiring: 24h eval showed {stats['bids']:,.0f} bids "
                            f"but ${stats['rev']:.2f} rev and ${ecpm:.3f} eCPM — DSP "
                            f"not valuing this inventory"),
                    dry_run=False, applied=True,
                )
                removed.append({"pub_id": pid, "demand_id": did,
                                "bids_24h": stats["bids"],
                                "rev_24h": stats["rev"],
                                "ecpm": ecpm,
                                "demand_name": dname})
            else:
                print(f"  REMOVE failed (entry not present or PUT didn't stick)")
        except Exception as e:
            print(f"  REMOVE FAILED: {e}")

    return {
        "ran_at": now.isoformat(),
        "wirings_examined": len(wirings),
        "kept": kept,
        "removed": len(removed),
        "details": removed,
    }


if __name__ == "__main__":
    print(json.dumps(run(), indent=2, default=str))
