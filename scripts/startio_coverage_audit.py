"""
scripts/startio_coverage_audit.py

Find Start.IO revenue opportunities we are NOT capturing today.

Three classes of gap:
  A. Publishers running Start.IO demand entries that are PAUSED (status=2)
     → unpausing typically restores the revenue line immediately.
  B. Publishers with Start.IO entries assigned but minBidFloor=null AND zero
     reported activity (didn't show up in startio_floor_optimizer because
     no signal — but they're attached and could be activated blindly at a
     conservative default floor).
  C. Publishers similar to the 6 we activated (same supplier_id, similar
     ad units / categories) that have NO Start.IO demand entries assigned at
     all → biggest gap, but requires manual demand-assignment via LL UI.

Run:
    python3.13 scripts/startio_coverage_audit.py
"""
import json
import os
import sys
from collections import defaultdict
from datetime import date, timedelta

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)
from dotenv import load_dotenv
load_dotenv(dotenv_path=os.path.join(_ROOT, ".env"), override=True)

import core.ll_mgmt as llm
import core.ll_report as llr

ALREADY_ACTIVATED_PUBS = {290115375, 290115285, 290115284, 290115330, 290115374, 290115343}
OUTPUT_PATH = os.path.join(_ROOT, "logs", "startio_coverage_audit_apr17.json")


def _sf(v):
    try: return float(v)
    except (TypeError, ValueError): return 0.0


def main():
    print("Pulling all LL publishers + Start.IO demand catalogue …")
    pubs = llm.get_publishers(include_archived=False)
    print(f"  {len(pubs)} active/paused publishers")

    all_demands = llm.get_demands(include_archived=False)
    startio_demands = [
        d for d in all_demands
        if "start" in str(d.get("name", "")).lower()
    ]
    startio_demand_ids = {int(d["id"]): d.get("name", "") for d in startio_demands}
    print(f"  {len(startio_demand_ids)} Start.IO demand definitions in LL\n")

    # Pull patched revenue data once for cross-reference
    rev_rows = llr.report(
        ["PUBLISHER_ID", "DEMAND_ID", "DEMAND_NAME"],
        ["BIDS", "WINS", "IMPRESSIONS", "GROSS_REVENUE"],
        "2026-01-01", (date.today() - timedelta(days=1)).strftime("%Y-%m-%d"),
    )
    rev_lookup: dict[tuple[int, int], dict] = {}
    for r in rev_rows:
        try:
            pid = int(r.get("PUBLISHER_ID") or 0)
            did = int(r.get("DEMAND_ID") or 0)
        except (TypeError, ValueError):
            continue
        if pid and did:
            rev_lookup[(pid, did)] = r

    # ---- Per-publisher analysis -------------------------------------------
    paused_entries: list[dict] = []         # Class A
    null_floor_no_signal: list[dict] = []   # Class B
    no_startio_assigned: list[dict] = []    # Class C — publishers with NO Start.IO demands
    pubs_processed = 0

    for pub in pubs:
        pid = int(pub.get("id") or 0)
        if not pid:
            continue
        pubs_processed += 1
        pname = pub.get("name", f"pub-{pid}")

        # Walk biddingpreferences
        startio_assigned = []
        for pref in pub.get("biddingpreferences", []):
            for v in pref.get("value", []):
                try:
                    did = int(v.get("id") or 0)
                except (TypeError, ValueError):
                    continue
                if did in startio_demand_ids:
                    startio_assigned.append((did, v))

        if not startio_assigned:
            no_startio_assigned.append({"publisher_id": pid, "publisher_name": pname})
            continue

        # Has Start.IO — look at status / floor / signal
        for did, v in startio_assigned:
            dname = startio_demand_ids[did]
            status = v.get("status")
            floor = v.get("minBidFloor")
            r = rev_lookup.get((pid, did), {})
            rev = _sf(r.get("GROSS_REVENUE"))
            imps = _sf(r.get("IMPRESSIONS"))
            bids = _sf(r.get("BIDS"))

            entry = {
                "publisher_id": pid, "publisher_name": pname,
                "demand_id": did, "demand_name": dname,
                "status": status, "minBidFloor": floor,
                "rev_alltime": round(rev, 2),
                "imps_alltime": int(imps),
                "bids_alltime": int(bids),
            }

            # Class A: paused
            if status == 2:
                paused_entries.append(entry)
                continue

            # Class B: active but null floor and no signal
            if floor is None and rev < 0.50 and pid not in ALREADY_ACTIVATED_PUBS:
                null_floor_no_signal.append(entry)

    print(f"Audited {pubs_processed} publishers.\n")

    # ---- Report -----------------------------------------------------------
    print("=" * 100)
    print("  CLASS A — PAUSED Start.IO entries (unpause = instant revenue)")
    print("=" * 100)
    paused_entries.sort(key=lambda e: -e["rev_alltime"])
    if not paused_entries:
        print("  (none — all assigned Start.IO demands are active)")
    else:
        for e in paused_entries[:30]:
            print(f"  {e['publisher_name'][:40]:<40}  {e['demand_name'][:38]:<38}  "
                  f"id={e['demand_id']:<5}  rev_alltime=${e['rev_alltime']:.0f}  "
                  f"imps={e['imps_alltime']:,}")
        print(f"\n  TOTAL paused: {len(paused_entries)} entries, "
              f"${sum(e['rev_alltime'] for e in paused_entries):,.0f} all-time rev")

    print("\n" + "=" * 100)
    print("  CLASS B — Active, null floor, no signal (other-publisher candidates for blind activation)")
    print("=" * 100)
    null_floor_no_signal.sort(key=lambda e: e["publisher_name"])
    if not null_floor_no_signal:
        print("  (none)")
    else:
        # Group by publisher for cleaner readout
        by_pub: dict[int, list[dict]] = defaultdict(list)
        for e in null_floor_no_signal:
            by_pub[e["publisher_id"]].append(e)
        for pid, entries in sorted(by_pub.items()):
            pname = entries[0]["publisher_name"]
            print(f"  {pname[:50]:<50}  pub_id={pid}  ({len(entries)} Start.IO demands w/ no floor)")
        print(f"\n  TOTAL: {len(by_pub)} publishers, {len(null_floor_no_signal)} demand entries")

    print("\n" + "=" * 100)
    print(f"  CLASS C — Publishers with NO Start.IO demands assigned at all  ({len(no_startio_assigned)} publishers)")
    print("  (manual research needed — would need LL UI access to add Start.IO to these)")
    print("=" * 100)
    print(f"  Sample (first 20):")
    for e in no_startio_assigned[:20]:
        print(f"    {e['publisher_name'][:60]:<60}  pub_id={e['publisher_id']}")
    if len(no_startio_assigned) > 20:
        print(f"    … and {len(no_startio_assigned) - 20} more")

    # Persist
    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    with open(OUTPUT_PATH, "w") as f:
        json.dump({
            "date": date.today().isoformat(),
            "publishers_audited": pubs_processed,
            "class_a_paused": paused_entries,
            "class_b_null_no_signal": null_floor_no_signal,
            "class_c_no_startio_count": len(no_startio_assigned),
            "class_c_sample": no_startio_assigned[:50],
        }, f, indent=2)
    print(f"\n  full output → {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
