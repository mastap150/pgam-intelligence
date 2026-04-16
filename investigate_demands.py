#!/usr/bin/env /usr/local/bin/python3.13
"""
investigate_demands.py

Fetches biddingpreferences for three publishers and reports every demand
seat (id, name, status, and type=3 floor value if present).
"""

import sys
import os

# Ensure repo root is on the path
sys.path.insert(0, "/Users/priyeshpatel/Desktop/pgam-intelligence")

from dotenv import load_dotenv
load_dotenv(dotenv_path="/Users/priyeshpatel/Desktop/pgam-intelligence/.env")

import core.ll_mgmt as ll

PUBLISHERS_TO_CHECK = [
    "Smaato - Magnite ",
    "Illumin Display & Video",
    "Illumin In App",
]


def get_demand_name_map():
    """Build {id: name} map from all demands (including archived)."""
    demands = ll.get_demands(include_archived=True)
    return {d["id"]: d.get("name", f"<id={d['id']}>") for d in demands}


def inspect_publisher(pub_name: str, demand_id_to_name: dict):
    print(f"\n{'='*70}")
    print(f"Publisher: {pub_name!r}")
    print(f"{'='*70}")

    pub = ll.get_publisher_by_name(pub_name)
    if not pub:
        print(f"  ERROR: Publisher not found by name search!")
        return

    pub_id = pub["id"]
    print(f"  Found: id={pub_id}  name={pub.get('name')!r}  status={pub.get('status')}")

    # Fetch the full publisher object to get biddingpreferences
    full = ll.get_publisher(pub_id)
    bprefs = full.get("biddingpreferences", [])
    print(f"\n  biddingpreferences count: {len(bprefs)}")

    if not bprefs:
        print("  (no biddingpreferences entries)")
        return

    for pref_idx, pref in enumerate(bprefs):
        pref_type = pref.get("type")
        rule_type = pref.get("ruleType")
        values    = pref.get("value", [])

        print(f"\n  --- biddingpreferences[{pref_idx}] ---")
        print(f"      type={pref_type}  ruleType={rule_type}  entries={len(values)}")

        # type=3 floor value (if present at pref level, not value level)
        if pref_type == 3:
            # Sometimes the floor is a direct numeric value rather than a list
            raw_val = pref.get("value")
            if isinstance(raw_val, (int, float)):
                print(f"      floor value (type=3): {raw_val}")
                continue

        # Iterate demand entries within value[]
        for item in values:
            d_id     = item.get("id")
            d_status = item.get("status")
            d_name   = demand_id_to_name.get(d_id, f"<NOT FOUND in demands list>")

            # Look for a floor nested inside the item (type=3 typeField)
            floor_val = None
            for tf in item.get("typeFields", []) or []:
                if tf.get("type") == 3:
                    floor_val = tf.get("value")

            floor_str = f"  floor(type=3)={floor_val}" if floor_val is not None else ""
            print(f"      demand_id={d_id:<6}  status={d_status}  name={d_name!r}{floor_str}")

    # Also check for a top-level type=3 in biddingpreferences that is NOT a list
    # (some publishers store floor as a separate single-value pref)
    for pref_idx, pref in enumerate(bprefs):
        if pref.get("type") == 3 and not isinstance(pref.get("value"), list):
            print(f"\n  [type=3 floor pref at index {pref_idx}]: value={pref.get('value')}")


def main():
    print("Loading all demands for name lookup …")
    demand_id_to_name = get_demand_name_map()
    print(f"Total demands loaded: {len(demand_id_to_name)}")

    for pub_name in PUBLISHERS_TO_CHECK:
        inspect_publisher(pub_name, demand_id_to_name)

    print(f"\n{'='*70}")
    print("Done.")


if __name__ == "__main__":
    main()
