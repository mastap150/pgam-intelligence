"""
agents/optimization/auto_wire_gaps.py

Daily autonomous activator for high-confidence demand-gap wirings.

Reads data/demand_gaps.json (produced by intelligence.demand_gap agent on
Monday mornings, fresh weekly) and automatically wires (pub, demand) pairs
where the estimated 30-day lift clears a threshold AND the demand's peer
performance is healthy.

Why this is safe to auto-execute
--------------------------------
This is a purely *additive* operation. We're adding a demand to a
publisher's biddingpreferences list — we never modify existing entries.
Zero risk to existing demands. The new entry starts with the demand's
default configuration (cloned from a working peer publisher), inherits
floor=None (lets LL's ML tune naturally), and gets the same safety
guards as every other code path (write-path clamp, ledger, verify).

If the wiring is a dud (cheap clears, no revenue), the worst case is a
few pennies of wasted bid capacity — we'd catch it in the weekly review
and set a per-demand floor to filter it (as happened with Pubmatic wirings
54/45 on 290115340).

Thresholds
----------
- est_lift_30d >= MIN_LIFT_30D: model must project meaningful revenue
- peer_median_win_rate >= MIN_PEER_WIN_RATE: demand must be healthy on peers
- Daily cap MAX_WIRINGS_PER_RUN to limit blast radius

Safety posture
--------------
- Only WIRE NEW (skip if demand already in pub's biddingpreferences)
- Clones config from the first peer pub that already runs the demand
- Strips per-pub stat fields (qpsYesterday, qpsPreviousHour)
- status=1 (enabled) on the new entry
- PUT full publisher, GET back, verify new entry is present
- Ledger entry with actor="auto_wire_gaps_<date>" for each wiring
- Respects LL_DRY_RUN (inherited from ll_mgmt._put)
"""
from __future__ import annotations

import copy
import json
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

from core import ll_mgmt, floor_ledger

DATA_DIR = Path(__file__).parent.parent.parent / "data"
GAPS_PATH = DATA_DIR / "demand_gaps.json"

# Thresholds
MIN_LIFT_30D = 500.0          # $ — don't bother below this
MIN_PEER_WIN_RATE = 0.02      # 2% — skip demands that lose on peer pubs too
MAX_WIRINGS_PER_RUN = 5       # daily cap on new activations

STRIP_FIELDS = {"qpsYesterday", "qpsPreviousHour"}
ACTOR_PREFIX = "auto_wire_gaps"


def _find_template_item(all_pubs_list: list[dict], demand_id: int,
                        exclude_pid: int) -> dict | None:
    """Return the first biddingpref item for this demand from any pub != exclude."""
    for pub_summary in all_pubs_list:
        pid = pub_summary.get("id")
        if pid == exclude_pid:
            continue
        try:
            detail = ll_mgmt.get_publisher(pid)
        except Exception:
            continue
        for pref in detail.get("biddingpreferences", []):
            for item in pref.get("value", []):
                if item.get("id") == demand_id:
                    return copy.deepcopy(item)
    return None


def _qualifying_gaps(gaps_data: dict) -> list[dict]:
    """Filter gaps on threshold criteria, sort by est_lift descending."""
    gaps = gaps_data.get("gaps", [])
    ok = [g for g in gaps
          if float(g.get("est_lift_30d", 0) or 0) >= MIN_LIFT_30D
          and float(g.get("peer_median_win_rate", 0) or 0) >= MIN_PEER_WIN_RATE]
    ok.sort(key=lambda g: -float(g.get("est_lift_30d", 0) or 0))
    return ok


def run() -> dict:
    """Scheduler entry: read gaps, wire top MAX_WIRINGS_PER_RUN qualifying ones."""
    actor = f"{ACTOR_PREFIX}_{datetime.now(timezone.utc).strftime('%Y%m%d')}"

    if not GAPS_PATH.exists():
        return {"skipped": True, "reason": "demand_gaps.json missing (demand_gap agent runs Mondays)"}

    gaps_data = json.loads(GAPS_PATH.read_text())
    qualifying = _qualifying_gaps(gaps_data)
    print(f"[{actor}] {len(gaps_data.get('gaps', []))} total gaps, "
          f"{len(qualifying)} qualifying "
          f"(lift>=${MIN_LIFT_30D}, peer_wr>={MIN_PEER_WIN_RATE})")

    if not qualifying:
        return {"ran_at": datetime.now(timezone.utc).isoformat(), "wired": 0,
                "reason": "no qualifying gaps"}

    all_pubs = ll_mgmt.get_publishers(include_archived=False)

    results = []
    # Group by target pub so we do one PUT per pub
    by_target = defaultdict(list)
    for g in qualifying[:MAX_WIRINGS_PER_RUN * 3]:  # look deeper, filter after dedup
        by_target[int(g["publisher_id"])].append(g)

    wired_count = 0
    for pid in by_target:
        if wired_count >= MAX_WIRINGS_PER_RUN:
            break
        try:
            target = ll_mgmt.get_publisher(pid)
        except Exception as e:
            print(f"[{actor}] pub {pid} GET failed: {e}")
            continue

        if len(target.get("biddingpreferences", [])) != 1:
            print(f"[{actor}] pub {pid} has unexpected bidpref structure — skipping")
            continue

        existing_ids = {v.get("id") for pref in target["biddingpreferences"]
                        for v in pref.get("value", [])}

        new_target = copy.deepcopy(target)
        added_here = []
        for g in by_target[pid]:
            if wired_count >= MAX_WIRINGS_PER_RUN:
                break
            did = int(g["demand_id"])
            if did in existing_ids:
                continue  # already wired
            template = _find_template_item(all_pubs, did, pid)
            if template is None:
                print(f"[{actor}] demand {did}: no template — skip")
                continue
            for f in STRIP_FIELDS:
                if f in template:
                    template[f] = 0
            template["status"] = 1
            new_target["biddingpreferences"][0]["value"].append(template)
            added_here.append((did, template.get("name", ""), g.get("est_lift_30d", 0)))
            wired_count += 1

        if not added_here:
            continue

        print(f"[{actor}] pub {pid} ({target.get('name')}): PUT with "
              f"{len(added_here)} new demands")
        try:
            ll_mgmt._put(f"/v1/publishers/{pid}", new_target)
        except Exception as e:
            print(f"[{actor}] PUT FAILED for pub {pid}: {e}")
            continue

        # verify & ledger
        try:
            after = ll_mgmt.get_publisher(pid)
            after_ids = {v.get("id") for pref in after.get("biddingpreferences", [])
                         for v in pref.get("value", [])}
        except Exception as e:
            print(f"[{actor}] verify GET failed for pub {pid}: {e}")
            continue

        for did, dname, lift in added_here:
            if did in after_ids:
                floor_ledger.record(
                    publisher_id=pid, publisher_name=after.get("name", ""),
                    demand_id=did, demand_name=dname,
                    old_floor=None, new_floor=None,
                    actor=actor,
                    reason=(f"Auto-wire from demand_gap report: est_lift_30d=${lift:.0f}, "
                            f"peer_wr>={MIN_PEER_WIN_RATE}"),
                    dry_run=False, applied=True,
                )
                results.append({"pub_id": pid, "demand_id": did,
                                "name": dname, "est_lift_30d": lift,
                                "verified": True})
                print(f"[{actor}]   ✓ demand {did} verified live")
            else:
                results.append({"pub_id": pid, "demand_id": did,
                                "name": dname, "verified": False})
                print(f"[{actor}]   ✗ demand {did} not present after PUT")

    return {
        "ran_at": datetime.now(timezone.utc).isoformat(),
        "qualifying_gaps": len(qualifying),
        "wired": len([r for r in results if r.get("verified")]),
        "results": results,
    }


if __name__ == "__main__":
    print(json.dumps(run(), indent=2, default=str))
