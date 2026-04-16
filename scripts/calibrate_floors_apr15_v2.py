"""
scripts/calibrate_floors_apr15_v2.py

Bulk floor calibration — Apr 15, 2026 (v2).

Approach: demand_id-based iteration — does NOT rely on demand name matching.
For each publisher, fetches the full publisher object, walks biddingpreferences,
and sets floors wherever entry["id"] is in a known demand_id set.

Publishers / targets
--------------------
Smaato - Magnite (id=290115372):
  Pubmatic RON seats  {54, 954, 45, 918, 919, 929, 51, 52, 50, 47, 49} → $0.35
  Magnite seats       {876, 877, 878, 879, 920, 921, 922, 923}          → $0.75

Illumin Display & Video (id=290115268):
  Magnite Display     {41, 440, 442, 443, 678}                           → $0.50
  Sovrn               {11, 12, 13, 14, 15, 16, 17, 18, 19, 798}         → $0.30

Illumin In App (id=290115270):
  Magnite In App      {7, 42, 441, 444, 445, 677, 679}                   → $0.75
  Pubmatic RON        {918, 919, 929}                                     → $0.30
"""

import os
import sys

_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

from dotenv import load_dotenv
load_dotenv(dotenv_path=os.path.join(_REPO_ROOT, '.env'))

import core.ll_mgmt as ll_mgmt
import core.slack as slack

# ---------------------------------------------------------------------------
# Mode flag — set False to go live
# ---------------------------------------------------------------------------
DRY_RUN = False

# ---------------------------------------------------------------------------
# Publisher / DSP target definitions
# ---------------------------------------------------------------------------

PUBLISHERS = [
    {
        "id":   290115372,
        "name": "Smaato - Magnite",
        "tiers": [
            {
                "label":      "Pubmatic RON",
                "demand_ids": {54, 954, 45, 918, 919, 929, 51, 52, 50, 47, 49},
                "floor":      0.35,
            },
            {
                "label":      "Magnite",
                "demand_ids": {876, 877, 878, 879, 920, 921, 922, 923},
                "floor":      0.75,
            },
        ],
    },
    {
        "id":   290115268,
        "name": "Illumin Display & Video",
        "tiers": [
            {
                "label":      "Magnite Display",
                "demand_ids": {41, 440, 442, 443, 678},
                "floor":      0.50,
            },
            {
                "label":      "Sovrn",
                "demand_ids": {11, 12, 13, 14, 15, 16, 17, 18, 19, 798},
                "floor":      0.30,
            },
        ],
    },
    {
        "id":   290115270,
        "name": "Illumin In App",
        "tiers": [
            {
                "label":      "Magnite In App",
                "demand_ids": {7, 42, 441, 444, 445, 677, 679},
                "floor":      0.75,
            },
            {
                "label":      "Pubmatic RON",
                "demand_ids": {918, 919, 929},
                "floor":      0.30,
            },
        ],
    },
]


# ---------------------------------------------------------------------------
# Core helpers
# ---------------------------------------------------------------------------

def _get_current_floor(item: dict) -> float | None:
    """Return the active floor from a biddingpreferences demand item, or None."""
    for tf in item.get("typeFields", []):
        if tf.get("type") == 3 and tf.get("setOnRule"):
            try:
                return float(tf.get("value", 0) or 0)
            except (TypeError, ValueError):
                return None
    return None


def _set_floor_on_item(item: dict, new_floor: float) -> float | None:
    """
    Mutate a demand item in-place, setting typeField type=3 value=new_floor.
    Returns the old floor value (or None if previously unset).
    """
    old_floor = _get_current_floor(item)
    type_fields = item.setdefault("typeFields", [])
    for tf in type_fields:
        if tf.get("type") == 3:
            tf["value"]     = new_floor
            tf["setOnRule"] = True
            return old_floor
    # Not found — append
    type_fields.append({"type": 3, "value": new_floor, "setOnRule": True})
    return old_floor


def calibrate_publisher(pub_cfg: dict) -> dict:
    """
    Calibrate all tier floors for a single publisher.

    Returns a summary dict:
      {
        "publisher_id": int,
        "publisher_name": str,
        "tiers": [
          {
            "label": str,
            "floor": float,
            "updated": int,          # seats actually changed
            "already_correct": int,  # seats already at target floor
            "not_found": int,        # demand_ids not in biddingpreferences
            "details": [...]
          }, ...
        ],
        "total_updated": int,
      }
    """
    pub_id   = pub_cfg["id"]
    pub_name = pub_cfg["name"]

    print(f"\n--- {pub_name} (id={pub_id}) ---")
    publisher = ll_mgmt.get_publisher(pub_id)

    # Build a flat lookup: demand_id → item (mutable reference into publisher dict)
    demand_map: dict[int, dict] = {}
    for pref in publisher.get("biddingpreferences", []):
        for item in pref.get("value", []):
            did = item.get("id")
            if did is not None:
                demand_map[did] = item

    tier_summaries = []
    total_updated  = 0

    for tier in pub_cfg["tiers"]:
        label      = tier["label"]
        target_ids = tier["demand_ids"]
        new_floor  = tier["floor"]

        updated         = 0
        already_correct = 0
        not_found_ids   = []
        details         = []

        for did in sorted(target_ids):
            if did not in demand_map:
                not_found_ids.append(did)
                details.append({"demand_id": did, "status": "NOT_IN_BIDDINGPREFS"})
                continue

            item      = demand_map[did]
            old_floor = _get_current_floor(item)

            if old_floor is not None and abs(old_floor - new_floor) < 0.001:
                already_correct += 1
                details.append({
                    "demand_id": did,
                    "status":    "ALREADY_CORRECT",
                    "floor":     old_floor,
                })
                print(f"  [skip] demand_id={did}  floor already ${old_floor:.2f}")
                continue

            # Apply (or preview)
            if DRY_RUN:
                print(
                    f"  [DRY_RUN] demand_id={did}  "
                    f"floor {old_floor}→${new_floor:.2f}  ({label})"
                )
                details.append({
                    "demand_id": did,
                    "status":    "DRY_RUN",
                    "old_floor": old_floor,
                    "new_floor": new_floor,
                })
            else:
                actual_old = _set_floor_on_item(item, new_floor)
                print(
                    f"  [SET]     demand_id={did}  "
                    f"floor {actual_old}→${new_floor:.2f}  ({label})"
                )
                details.append({
                    "demand_id": did,
                    "status":    "UPDATED",
                    "old_floor": actual_old,
                    "new_floor": new_floor,
                })
                updated += 1

        tier_summaries.append({
            "label":           label,
            "floor":           new_floor,
            "updated":         updated,
            "already_correct": already_correct,
            "not_found":       len(not_found_ids),
            "not_found_ids":   not_found_ids,
            "details":         details,
        })
        total_updated += updated

    # Single PUT per publisher (only on live run)
    if not DRY_RUN and total_updated > 0:
        print(f"  --> PUT /v1/publishers/{pub_id}  ({total_updated} seats updated)")
        ll_mgmt._put(f"/v1/publishers/{pub_id}", publisher)
    elif DRY_RUN:
        print(f"  [DRY_RUN] Would PUT /v1/publishers/{pub_id}")

    return {
        "publisher_id":   pub_id,
        "publisher_name": pub_name,
        "tiers":          tier_summaries,
        "total_updated":  total_updated,
    }


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    mode = "DRY RUN" if DRY_RUN else "LIVE"
    print(f"\n=== Bulk Floor Calibration Apr-15 v2 [{mode}] ===")

    results      = []
    grand_total  = 0

    for pub_cfg in PUBLISHERS:
        summary = calibrate_publisher(pub_cfg)
        results.append(summary)
        grand_total += summary["total_updated"] if not DRY_RUN else sum(
            len([d for d in t["details"] if d["status"] == "DRY_RUN"])
            for t in summary["tiers"]
        )

    # ------------------------------------------------------------------
    # Print human-readable summary
    # ------------------------------------------------------------------
    print(f"\n{'='*60}")
    print(f"SUMMARY [{mode}]")
    print(f"{'='*60}")

    for summary in results:
        pub_name = summary["publisher_name"]
        print(f"\n{pub_name}:")
        for tier in summary["tiers"]:
            label    = tier["label"]
            floor    = tier["floor"]
            updated  = tier["updated"]
            correct  = tier["already_correct"]
            missing  = tier["not_found"]
            dr_count = len([d for d in tier["details"] if d["status"] == "DRY_RUN"])

            if DRY_RUN:
                print(
                    f"  {label} (${floor:.2f}): "
                    f"{dr_count} would update, "
                    f"{correct} already correct, "
                    f"{missing} not in biddingprefs"
                )
            else:
                print(
                    f"  {label} (${floor:.2f}): "
                    f"{updated} updated, "
                    f"{correct} already correct, "
                    f"{missing} not in biddingprefs"
                )

            if tier["not_found_ids"]:
                print(f"    (missing ids: {tier['not_found_ids']})")

    print(f"\nGrand total seats {'to update' if DRY_RUN else 'updated'}: {grand_total}")

    # ------------------------------------------------------------------
    # Slack summary (send even on dry-run so we can verify channel config)
    # ------------------------------------------------------------------
    # Build per-publisher tier lines for the Slack message
    pub_lines = []
    for summary in results:
        tier_parts = []
        for tier in summary["tiers"]:
            cnt = len([d for d in tier["details"] if d["status"] == "DRY_RUN"]) if DRY_RUN else tier["updated"]
            tier_parts.append(f"{tier['label']} seats (${tier['floor']:.2f})")
        pub_lines.append(f"{summary['publisher_name']}: {', '.join(tier_parts)}")

    dry_prefix = "[DRY RUN] " if DRY_RUN else ""
    slack_text = (
        f":white_check_mark: *{dry_prefix}Bulk Floor Calibration Apr-15 Applied*\n"
        + "\n".join(pub_lines) + "\n"
        f"Total: {grand_total} seats {'to update (dry run)' if DRY_RUN else 'updated'}\n"
        f":warning: Manual calibration based on 7d avg bid analysis"
    )

    blocks = [{"type": "section", "text": {"type": "mrkdwn", "text": slack_text}}]

    print(f"\nSending Slack summary...")
    resp = slack.send_blocks(blocks, text=f"{dry_prefix}Bulk Floor Calibration Apr-15")
    if resp is not None:
        print(f"Slack response: HTTP {resp.status_code}")
    else:
        print("Slack: no webhook configured (skipped)")

    print(f"\n=== Done [{mode}] ===\n")
    return results


if __name__ == "__main__":
    main()
