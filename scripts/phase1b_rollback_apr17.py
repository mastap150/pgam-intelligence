"""
Partial rollback of phase1b_executor_apr17 misfires.

Reverts clean misfires identified in post-mortem:
- Pub 290115332 WL Interstitial: Magnite BidMachine + Pubmatic RON floors → null, Illumin US 1.5 → 0.5
- Pub 290115334 BidMachine EU: Magnite BidMachine EU → null
Leaves net-positive publishers (290115378, 290115373, 290115327) alone.

Usage:
  python -m scripts.phase1b_rollback_apr17 --dry-run
  python -m scripts.phase1b_rollback_apr17 --apply
"""
import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

from core import ll_mgmt as llm

LOG_PATH = Path(__file__).parent.parent / "logs" / "phase1b_rollback_apr17.json"


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


# (demand_id, label, revert_floor_value)  — None means clear the floor
ROLLBACKS = [
    {
        "publisher_id": 290115332,
        "publisher_label": "BidMachine - In App Interstitial (WL)",
        "reason": "Aggressive $1-3 floors set with no eCPM baseline; Pubmatic softness today",
        "demand_changes": [
            (604, "Magnite BidMachine In App", None),
            (605, "Magnite BidMachine In App oRTB 2.6", None),
            (606, "Magnite BidMachine In App PS", None),
            (607, "Magnite BidMachine In App PS oRTB 2.6", None),
            (671, "Magnite BidMachine In App US West", None),
            (672, "Magnite BidMachine In App oRTB 2.6 US West", None),
            (673, "Magnite BidMachine In App PS US West", None),
            (674, "Magnite BidMachine In App PS oRTB 2.6 West", None),
            (45, "Pubmatic RON 320x50 PS", None),
            (46, "Pubmatic RON 320x100 PS", None),
            (51, "Pubmatic RON 300x250 PS", None),
            (52, "Pubmatic RON 728x90 PS", None),
            (788, "Illumin US", 0.5),  # revert 1.5 → 0.5
        ],
    },
    {
        "publisher_id": 290115334,
        "publisher_label": "Copy - BidMachine - In App Interstitial (Europe)",
        "reason": "$1.00 EU floors likely above EU clearing",
        "demand_changes": [
            (665, "Magnite BidMachine In App EU", None),
            (666, "Magnite BidMachine In App oRTB 2.6 EU", None),
            (667, "Magnite BidMachine In App PS EU", None),
            (668, "Magnite BidMachine In App PS oRTB 2.6 EU", None),
        ],
    },
]


def log_action(result: dict):
    prior = []
    if LOG_PATH.exists():
        prior = json.loads(LOG_PATH.read_text())
    prior.append(result)
    LOG_PATH.write_text(json.dumps(prior, indent=2, default=str))


def rollback_publisher(pub_id, pub_label, reason, demand_changes, dry_run):
    result = {
        "action": "phase1b_rollback_apr17",
        "reason": reason,
        "publisher_id": pub_id,
        "publisher_label": pub_label,
        "timestamp": _now_iso(),
        "applied": False,
        "dry_run": dry_run,
        "changes": [],
        "skipped": [],
    }

    try:
        pub = llm.get_publisher(pub_id)
    except Exception as e:
        result["error"] = str(e)
        print(f"  ✗ [{pub_label}] fetch error: {e}")
        return result

    demand_map = {}
    for pref in pub.get("biddingpreferences", []):
        for v in pref.get("value", []):
            did = v.get("id")
            if did is not None:
                demand_map[did] = v

    modified = False
    for demand_id, demand_label, revert_to in demand_changes:
        v = demand_map.get(demand_id)
        if v is None:
            result["skipped"].append({"demand_id": demand_id, "reason": "not_found"})
            continue

        old_floor = v.get("minBidFloor")
        if old_floor == revert_to:
            result["skipped"].append({"demand_id": demand_id, "reason": f"already_{revert_to}"})
            continue

        change = {
            "demand_id": demand_id,
            "demand_name": demand_label,
            "old_floor": old_floor,
            "new_floor": revert_to,
        }
        result["changes"].append(change)
        tag = "DRY_RUN" if dry_run else "✓"
        print(f"  {tag}  [{pub_label[:28]:<28}] {demand_label[:38]:<38} id={demand_id:<5} "
              f"{old_floor} → {revert_to}")

        if not dry_run:
            v["minBidFloor"] = revert_to
            modified = True

    if not result["changes"]:
        return result

    if not dry_run and modified:
        try:
            llm._put(f"/v1/publishers/{pub_id}", pub)
            result["applied"] = True
            log_action(result)
        except Exception as e:
            result["error"] = str(e)
            print(f"  ✗ PUT failed: {e}")

    return result


def main():
    ap = argparse.ArgumentParser()
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--dry-run", action="store_true")
    g.add_argument("--apply", action="store_true")
    args = ap.parse_args()

    dry_run = args.dry_run

    print(f"\n{'=' * 80}\nphase1b_rollback_apr17 — {'DRY RUN' if dry_run else 'APPLY'}\n{'=' * 80}")

    summary = []
    for spec in ROLLBACKS:
        print(f"\n▶ Pub {spec['publisher_id']} — {spec['publisher_label']}")
        print(f"  reason: {spec['reason']}")
        r = rollback_publisher(
            spec["publisher_id"], spec["publisher_label"],
            spec["reason"], spec["demand_changes"], dry_run,
        )
        summary.append(r)

    total_changes = sum(len(r["changes"]) for r in summary)
    print(f"\n{'=' * 80}\nDone. {total_changes} changes across {len(summary)} publishers.")


if __name__ == "__main__":
    main()
