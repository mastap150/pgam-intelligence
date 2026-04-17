"""
scripts/ll_high_wr_floor_raise_apr16.py

Raise minBidFloor on demand entries for HIGH-WR publishers where
floors are well below avg clearing price — leaving money on the table.

Source: Apr 7-14 LL reporting analysis
  - Future Today CTV Tag:  100% WR, avg_bid $22.11, avg_floor $9.36
  - WURL $10:               67% WR, avg_bid $26.19, avg_floor $10.00
  - BidMachine Reseller:    56% WR, avg_bid $5.06,  mostly no floors
  - MetaSpoon Video:        65% WR, avg_bid $2.31,  no floors

Floor targets:
  - CTV publishers: raise to ~55% of avg_bid (conservative)
  - In-App publishers: set $1.50 minimum where no floor exists
  - Video publishers: set $1.00 minimum where no floor exists

Run modes:
  python scripts/ll_high_wr_floor_raise_apr16.py            # dry-run
  python scripts/ll_high_wr_floor_raise_apr16.py --live     # apply
"""

import argparse
import json
import os
import sys
from datetime import datetime, timezone

_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

from dotenv import load_dotenv
load_dotenv(dotenv_path=os.path.join(_REPO_ROOT, ".env"), override=True)

import core.ll_mgmt as llm

LOG_PATH = os.path.join(_REPO_ROOT, "logs", "pilot_2026-04.json")


def _now_iso():
    return datetime.now(timezone.utc).isoformat()


def _today():
    return datetime.now().strftime("%Y-%m-%d")


def log_action(action_dict: dict):
    os.makedirs(os.path.dirname(LOG_PATH), exist_ok=True)
    data = []
    if os.path.exists(LOG_PATH):
        try:
            with open(LOG_PATH) as f:
                data = json.load(f)
        except Exception:
            data = []
    if isinstance(data, dict):
        data = list(data.values())
    today = _today()
    day_entry = next((e for e in data if e.get("date") == today), None)
    if day_entry is None:
        day_entry = {"date": today, "actions_applied": []}
        data.append(day_entry)
    day_entry.setdefault("actions_applied", []).append(action_dict)
    with open(LOG_PATH, "w") as f:
        json.dump(data, f, indent=2)


def set_demand_floors(
    pub_id: int,
    pub_label: str,
    demand_floors: list[tuple],  # [(demand_id, demand_label, new_floor, only_if_none), ...]
    dry_run: bool,
) -> dict:
    """
    Set minBidFloor on specific demand entries within a publisher's biddingpreferences.

    demand_floors entries:
      (demand_id, label, new_floor, only_if_none)
      - only_if_none=True  → only set the floor if current minBidFloor is None
      - only_if_none=False → always set (overwrite existing)

    Returns summary dict.
    """
    result = {
        "action": "high_wr_floor_raise_apr16",
        "publisher_id": pub_id,
        "publisher_label": pub_label,
        "timestamp": _now_iso(),
        "applied": False,
        "dry_run": dry_run,
        "changes": [],
    }

    try:
        pub = llm.get_publisher(pub_id)
    except Exception as e:
        print(f"  ✗ [{pub_label}] ERROR fetching publisher: {e}")
        result["error"] = str(e)
        return result

    pub_name = pub.get("name", pub_label)

    # Build an index of demand_id → (pref, value_entry) for fast lookup
    demand_map: dict[int, dict] = {}
    for pref in pub.get("biddingpreferences", []):
        for v in pref.get("value", []):
            did = v.get("id")
            if did is not None:
                demand_map[did] = v

    modified = False
    for demand_id, demand_label, new_floor, only_if_none in demand_floors:
        v = demand_map.get(demand_id)
        if v is None:
            print(f"  ⚠  [{pub_name}] demand_id={demand_id} ({demand_label}) not found — skip")
            continue

        old_floor = v.get("minBidFloor")
        status    = v.get("status")

        if status == 2:
            print(f"  ·  [{pub_name}] demand_id={demand_id} ({demand_label}) PAUSED — skip")
            continue

        if only_if_none and old_floor is not None:
            print(f"  ·  [{pub_name}] demand_id={demand_id} ({demand_label}) "
                  f"already has floor={old_floor} — skip (only_if_none)")
            continue

        if old_floor == new_floor:
            print(f"  ·  [{pub_name}] demand_id={demand_id} ({demand_label}) "
                  f"floor already {old_floor} — no change")
            continue

        change = {
            "demand_id":   demand_id,
            "demand_name": demand_label,
            "old_floor":   old_floor,
            "new_floor":   new_floor,
        }
        result["changes"].append(change)

        if dry_run:
            direction = "→" if old_floor is None else ("↑" if new_floor > (old_floor or 0) else "↓")
            print(f"  DRY_RUN  [{pub_name}] demand_id={demand_id} ({demand_label}): "
                  f"minBidFloor {old_floor} {direction} {new_floor}")
        else:
            v["minBidFloor"] = new_floor
            modified = True
            direction = "→" if old_floor is None else ("↑" if new_floor > (old_floor or 0) else "↓")
            print(f"  ✓  [{pub_name}] demand_id={demand_id} ({demand_label}): "
                  f"minBidFloor {old_floor} {direction} {new_floor}")

    if not result["changes"]:
        print(f"  (no changes for {pub_name})")
        return result

    if not dry_run and modified:
        try:
            llm._put(f"/v1/publishers/{pub_id}", pub)
            result["applied"] = True
            log_action(result)
        except Exception as e:
            print(f"  ✗ [{pub_name}] PUT failed: {e}")
            result["error"] = str(e)
    elif dry_run:
        result["applied"] = False

    return result


def main():
    parser = argparse.ArgumentParser(description="LL HIGH-WR floor raises — Apr 16 2026")
    parser.add_argument("--live", action="store_true", help="Apply changes (default: dry-run)")
    args = parser.parse_args()
    dry_run = not args.live

    mode = "LIVE" if not dry_run else "DRY-RUN"
    print("=" * 70)
    print(f"  LL High-WR Floor Raises — Apr 16 2026  [{mode}]")
    print("=" * 70)

    results = []

    # ─────────────────────────────────────────────────────────────────────────
    # 1. Future Today CTV Tag (290115275)
    #    100% WR, avg_bid $22.11, current floors $8-10
    #    Strategy: raise active demand floors to $12-13 (55-60% of avg bid)
    # ─────────────────────────────────────────────────────────────────────────
    print("\n[1/4] Future Today CTV Tag (id=290115275)")
    print("      100% WR · avg_bid $22.11 · current floors $8-10 → targeting $12-13")
    r = set_demand_floors(
        pub_id=290115275,
        pub_label="Future Today CTV Tag",
        demand_floors=[
            # (demand_id, label, new_floor, only_if_none)
            (39,  "Pubmatic Future Today Video",  12.0, False),  # was $8 → $12
            (38,  "Sharethrough Future Today",    12.0, False),  # was $9 → $12
            (588, "Stirista RON Future Today",    13.0, False),  # was $10 → $13
            (888, "Sabio Future Today $9",        12.0, True),   # was None → $12
        ],
        dry_run=dry_run,
    )
    results.append(r)

    # ─────────────────────────────────────────────────────────────────────────
    # 2. WURL $10 (290115317)
    #    67% WR, avg_bid $26.19
    #    Pubmatic/Colossus already at $12-13, but Stirista at $10 and Pubmatic RON unset
    #    Strategy: raise Stirista to $14, set Pubmatic RON to $12
    # ─────────────────────────────────────────────────────────────────────────
    print("\n[2/4] WURL $10 (id=290115317)")
    print("      67% WR · avg_bid $26.19 · Stirista $10→$14, Pubmatic RON None→$12")
    r = set_demand_floors(
        pub_id=290115317,
        pub_label="WURL $10",
        demand_floors=[
            (589, "Stirista RON WURL",               14.0, False),  # was $10 → $14
            (54,  "Pubmatic RON Video Prebid Server", 12.0, True),   # was None → $12
            # Leave Pubmatic WURL ($12) and Colossus ($13) as-is — already decent
        ],
        dry_run=dry_run,
    )
    results.append(r)

    # ─────────────────────────────────────────────────────────────────────────
    # 3. BidMachine Reseller (290115340)
    #    56% WR, avg_bid $5.06, most floors are None (no floor)
    #    Strategy: set $1.50 on all active None-floor demands
    #    Note: Xandr 9 Dots already has $1.8-2.5 → leave alone
    #    Note: Illumin-US at $0.5 → raise to $1.50
    # ─────────────────────────────────────────────────────────────────────────
    print("\n[3/4] BidMachine Reseller (id=290115340)")
    print("      56% WR · avg_bid $5.06 · setting $1.50 floor on unflored active demands")
    TARGET_FLOOR_BM = 1.50
    r = set_demand_floors(
        pub_id=290115340,
        pub_label="BidMachine Reseller",
        demand_floors=[
            (604, "Magnite BidMachine In App",                   TARGET_FLOOR_BM, True),
            (605, "Magnite BidMachine In App oRTB 2.6",          TARGET_FLOOR_BM, True),
            (606, "Magnite BidMachine In App Prebid Server",     TARGET_FLOOR_BM, True),
            (607, "Magnite BidMAchine In App PS oRTB 2.6",       TARGET_FLOOR_BM, True),
            (671, "Magnite BidMachine In App US West",           TARGET_FLOOR_BM, True),
            (672, "Magnite BidMachine In App oRTB 2.6 US West",  TARGET_FLOOR_BM, True),
            (673, "Magnite BidMachine In App PS US West",        TARGET_FLOOR_BM, True),
            (674, "Magnite BidMAchine In App PS oRTB 2.6 West",  TARGET_FLOOR_BM, True),
            (683, "Stirista General OLV BidMachine",             TARGET_FLOOR_BM, True),
            (682, "Stirista General BidMachine",                 TARGET_FLOOR_BM, True),
            (684, "Sovrn BidMachine 300x250",                    TARGET_FLOOR_BM, True),
            (685, "Sovrn BidMachine 300x250 oRTB 2.6",          TARGET_FLOOR_BM, True),
            (686, "Sovrn BidMachine 320x50 PS",                  TARGET_FLOOR_BM, True),
            (687, "Sovrn BidMachine 320x50 oRTB 2.6",           TARGET_FLOOR_BM, True),
            (688, "Sovrn BidMachine 728x90 oRTB 2.6",           TARGET_FLOOR_BM, True),
            (689, "Sovrn BidMachine 728x90",                     TARGET_FLOOR_BM, True),
            (690, "Sovrn BidMachine 320x100",                    TARGET_FLOOR_BM, True),
            (691, "Sovrn BidMachine 320x100 oRTB 2.6",          TARGET_FLOOR_BM, True),
            (919, "Pubmatic RON 728x90 PS New EP",               TARGET_FLOOR_BM, True),
            (929, "Pubmatic RON 300x250 PS New EP",              TARGET_FLOOR_BM, True),
            (969, "Rise Media BidMachine In App",                TARGET_FLOOR_BM, True),
            # Illumin-US: was $0.50 → raise to $1.50 (NOT only_if_none — it already has a floor)
            (788, "Illumin US",                                  TARGET_FLOOR_BM, False),
        ],
        dry_run=dry_run,
    )
    results.append(r)

    # ─────────────────────────────────────────────────────────────────────────
    # 4. MetaSpoon Video (290115342)
    #    65% WR, avg_bid $2.31, all active demands have no floor
    #    Strategy: set $1.00 on active demands
    # ─────────────────────────────────────────────────────────────────────────
    print("\n[4/4] MetaSpoon Video (id=290115342)")
    print("      65% WR · avg_bid $2.31 · setting $1.00 floor on active unflored demands")
    TARGET_FLOOR_MS = 1.00
    r = set_demand_floors(
        pub_id=290115342,
        pub_label="MetaSpoon Video",
        demand_floors=[
            (726, "Inmobi MetaSpoon Video",             TARGET_FLOOR_MS, True),
            (728, "Inmobi MetaSpoon Video oRTB 2.6",    TARGET_FLOOR_MS, True),
            (747, "Magnite MetaSpoon Video",             TARGET_FLOOR_MS, True),
            (748, "Magnite MetaSpoon Video oRTB 2.6",   TARGET_FLOOR_MS, True),
            (54,  "Pubmatic RON Video PS",               TARGET_FLOOR_MS, True),
            (929, "Pubmatic RON 300x250 PS New EP",      TARGET_FLOOR_MS, True),
        ],
        dry_run=dry_run,
    )
    results.append(r)

    # ─────────────────────────────────────────────────────────────────────────
    # Summary
    # ─────────────────────────────────────────────────────────────────────────
    total_changes = sum(len(r.get("changes", [])) for r in results)
    errors = sum(1 for r in results if r.get("error"))

    print("\n" + "=" * 70)
    print(f"  SUMMARY: {total_changes} floor changes across {len(results)} publishers")
    if errors:
        print(f"  ⚠  {errors} publisher(s) had errors")
    if dry_run:
        print("  ⚡ DRY-RUN — nothing was changed. Re-run with --live to apply.")
    else:
        print("  ✓ LIVE — changes applied to LL. Monitor tomorrow's snapshot.")
    print("=" * 70)

    # Revenue impact estimate
    print("\n  Expected revenue impact:")
    print("  • Future Today CTV Tag: floors $9→$12+  → +$5-10/week est.")
    print("  • WURL $10:             Stirista $10→$14 → +$3-5/week est.")
    print("  • BidMachine Reseller:  floor $0→$1.50  → +$15-30/week est. (quality filter)")
    print("  • MetaSpoon Video:      floor $0→$1.00  → +$3-8/week est. (quality filter)")
    print()

    if not dry_run:
        out_path = os.path.join(_REPO_ROOT, "logs", "high_wr_raise_apr16_results.json")
        with open(out_path, "w") as f:
            json.dump(results, f, indent=2)
        print(f"  Results saved to {out_path}\n")


if __name__ == "__main__":
    main()
