"""
scripts/ll_floor_fix_apr16.py

Comprehensive LL publisher floor corrections based on Apr 7-14 performance analysis.

Two categories of changes:
  1. FLOOR-TOO-HIGH  — publisher avg_bid << floor → lower floor to recover volume
  2. HIGH-WR GAP     — high win rate + floor << avg_bid → raise floor for more revenue

Floor is stored at the adunit level (bidFloor) on LL publishers.
Some publishers also have demand-level minBidFloor in biddingpreferences.

Run modes:
  python scripts/ll_floor_fix_apr16.py          # dry-run (default)
  python scripts/ll_floor_fix_apr16.py --live   # apply changes

  python scripts/ll_floor_fix_apr16.py --inspect   # show all floor data, no changes
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

# ─────────────────────────────────────────────────────────────────────────────
# Change plan — derived from Apr 7-14 LL reporting analysis
# Format: (publisher_id, publisher_label, new_floor, reason)
# ─────────────────────────────────────────────────────────────────────────────

# Category 1: FLOOR-TOO-HIGH — floors are ABOVE avg bid, crushing volume
# Target = avg_bid × 0.50 (conservative, well below clearing price)
LOWER_FLOORS = [
    # Corrupted/extreme floors — must fix immediately
    (290115375, "Start.IO Video Magnite",    0.10, "Corrupted floor (was $54 quadrillion) — reset to minimum"),
    (290115374, "Start.IO Display Magnite",  0.10, "Corrupted floor (was $7.7M) — reset to minimum"),
    # floor > avg_bid — lowering to ~50% of avg bid
    (290115377, "AppStock",                  0.22, "Floor $6.40 >> avg_bid $0.44 — lowering to $0.22 (50% avg)"),
    (290115354, "BidMachine WL APAC",        0.10, "Floor $1.31 >> avg_bid $0.18 — lowering to MIN_FLOOR $0.10"),
    (290115326, "BidMachine Europe",         0.10, "Floor $1.22 >> avg_bid $0.19 — lowering to MIN_FLOOR $0.10"),
    (290115379, "BlueSeaX EU",               0.30, "Floor $1.97 >> avg_bid $0.60 — lowering to $0.30 (50% avg)"),
    (290115319, "BidMachine In App Display", 0.35, "Floor $1.86 >> avg_bid $0.70 — lowering to $0.35 (50% avg)"),
    (290115313, "Algorix Display",           0.55, "Floor $1.81 >> avg_bid $1.12 — lowering to $0.55 (49% avg)"),
    (290115318, "BidMachine (WL)",           0.20, "Floor $0.75 >> avg_bid $0.38 — lowering to $0.20 (53% avg)"),
    (290115270, "Illumin In App",            0.35, "Floor $1.35 >> avg_bid $0.65 — lowering to $0.35 (54% avg)"),
]

# Category 2: HIGH-WR GAP — high win rate means floor is below market; raise to capture more value
# Target = avg_bid × 0.60-0.65 (well below clearing price, but capturing more)
RAISE_FLOORS = [
    (290115275, "Future Today CTV Tag",  13.00, "100% WR, avg_bid $22.11, current floor $9.36 → $13.00 (59% avg)"),
    (290115317, "WURL $10",             16.00, "67% WR, avg_bid $26.19, current floor $10.00 → $16.00 (61% avg)"),
    (290115308, "LifeVista $4.50",       7.50, "76% WR, avg_bid $18.95, current floor $4.50 → $7.50 (40% avg)"),
    (290115340, "BidMachine Reseller",   2.00, "56% WR, avg_bid $5.06, current floor $1.23 → $2.00 (40% avg)"),
    (290115342, "MetaSpoon Video",       1.20, "65% WR, avg_bid $2.31, current floor $0.76 → $1.20 (52% avg)"),
]

ALL_CHANGES = [("LOWER", *c) for c in LOWER_FLOORS] + [("RAISE", *c) for c in RAISE_FLOORS]

LOG_PATH = os.path.join(_REPO_ROOT, "logs", "pilot_2026-04.json")


def _now_iso():
    return datetime.now(timezone.utc).isoformat()


def _today():
    return datetime.now().strftime("%Y-%m-%d")


def log_action(action_dict: dict):
    """Append to the pilot log."""
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


def inspect_publisher(pub_id: int, label: str):
    """Show floor information for a publisher without making changes."""
    print(f"\n{'─'*60}")
    print(f"  {label}  (id={pub_id})")
    print(f"{'─'*60}")

    try:
        pub = llm.get_publisher(pub_id)
    except Exception as e:
        print(f"  ERROR fetching publisher: {e}")
        return

    print(f"  Name:     {pub.get('name', '(unknown)')}")
    print(f"  Status:   {pub.get('status')}")
    print(f"  Supplier: {pub.get('supplier') or pub.get('supplierId', '?')}")

    # Adunit floor
    try:
        adunits = llm.get_adunits(pub_id)
        if adunits:
            for au in adunits:
                print(f"  Adunit id={au.get('id')}  bidFloor={au.get('bidFloor')}  name={au.get('name','')}")
        else:
            print("  Adunits: (none)")
    except Exception as e:
        print(f"  Adunits fetch error: {e}")

    # Demand floors in biddingpreferences
    prefs = pub.get("biddingpreferences", [])
    if prefs:
        print(f"  BiddingPreferences ({len(prefs)} rule(s)):")
        for pref in prefs:
            for v in pref.get("value", []):
                demand_id   = v.get("id")
                demand_name = v.get("name", "?")
                min_bf      = v.get("minBidFloor")
                status      = v.get("status")
                # Also check typeFields type=3
                tf_floor = None
                tfs = v.get("typeFields", [])
                if isinstance(tfs, list):
                    for tf in tfs:
                        if isinstance(tf, dict) and tf.get("type") == 3 and tf.get("setOnRule"):
                            tf_floor = tf.get("value")
                print(f"    demand_id={demand_id:<6} name={demand_name:<30} "
                      f"minBidFloor={min_bf!r:<15} typeField_floor={tf_floor!r}  status={status}")
    else:
        print("  BiddingPreferences: (none)")


def apply_adunit_floor(pub_id: int, label: str, new_floor: float, reason: str, dry_run: bool) -> dict:
    """
    Set bidFloor on all adunits for this publisher.
    Also reset any insane minBidFloor values on demand entries.
    """
    result = {
        "action": "floor_fix_apr16",
        "publisher_id": pub_id,
        "publisher_label": label,
        "new_floor": new_floor,
        "reason": reason,
        "timestamp": _now_iso(),
        "applied": False,
        "dry_run": dry_run,
        "adunits_updated": [],
        "demand_floors_reset": [],
    }

    try:
        pub = llm.get_publisher(pub_id)
    except Exception as e:
        print(f"  ✗ [{label}] ERROR fetching publisher: {e}")
        result["error"] = str(e)
        return result

    pub_name = pub.get("name", label)

    # ── 1. Check adunits ──────────────────────────────────────────────────────
    try:
        adunits = llm.get_adunits(pub_id)
    except Exception as e:
        adunits = []
        print(f"  ⚠ [{pub_name}] Could not fetch adunits: {e}")

    for au in adunits:
        au_id     = au.get("id")
        old_floor = au.get("bidFloor")
        if dry_run:
            print(f"  DRY_RUN  [{pub_name}] adunit {au_id}: bidFloor {old_floor} → {new_floor}  ({reason})")
        else:
            try:
                llm.update_floor(au_id, new_floor, dry_run=False)
                print(f"  ✓ [{pub_name}] adunit {au_id}: bidFloor {old_floor} → {new_floor}")
            except Exception as e:
                print(f"  ✗ [{pub_name}] adunit {au_id} update failed: {e}")
        result["adunits_updated"].append({"adunit_id": au_id, "old_floor": old_floor, "new_floor": new_floor})

    # ── 2. Reset corrupted / excessive minBidFloor on demand entries ──────────
    # Threshold: if minBidFloor > 100× new_floor we treat it as corrupted/excessive
    CORRUPTION_THRESHOLD = max(new_floor * 100, 50.0)

    pub_modified = False
    for pref in pub.get("biddingpreferences", []):
        for v in pref.get("value", []):
            mf = v.get("minBidFloor")
            if mf is not None:
                try:
                    mf_val = float(mf)
                except (TypeError, ValueError):
                    continue
                if mf_val > CORRUPTION_THRESHOLD:
                    demand_id   = v.get("id")
                    demand_name = v.get("name", "?")
                    if dry_run:
                        print(f"  DRY_RUN  [{pub_name}] demand '{demand_name}' (id={demand_id}): "
                              f"minBidFloor {mf_val:.4g} → {new_floor} (corrupted/excessive reset)")
                    else:
                        v["minBidFloor"] = new_floor
                        pub_modified = True
                        print(f"  ✓ [{pub_name}] demand '{demand_name}' (id={demand_id}): "
                              f"minBidFloor {mf_val:.4g} → {new_floor}")
                    result["demand_floors_reset"].append({
                        "demand_id": demand_id,
                        "demand_name": demand_name,
                        "old_minBidFloor": mf_val,
                        "new_minBidFloor": new_floor,
                    })

    if pub_modified and not dry_run:
        try:
            llm._put(f"/v1/publishers/{pub_id}", pub)
        except Exception as e:
            print(f"  ✗ [{pub_name}] PUT publisher failed: {e}")
            result["error"] = str(e)
            return result

    if not dry_run and (result["adunits_updated"] or result["demand_floors_reset"]):
        result["applied"] = True
        log_action(result)

    if dry_run:
        result["applied"] = False

    return result


def main():
    parser = argparse.ArgumentParser(description="LL publisher floor corrections Apr 16 2026")
    parser.add_argument("--live",    action="store_true", help="Apply changes (default: dry-run)")
    parser.add_argument("--inspect", action="store_true", help="Show floor data only, no changes")
    parser.add_argument("--only",    type=str, default=None,
                        help="Comma-separated publisher IDs to process (default: all)")
    args = parser.parse_args()

    dry_run = not args.live
    only_ids = set(int(x) for x in args.only.split(",")) if args.only else None

    if args.inspect:
        print("=" * 60)
        print("  INSPECT MODE — no changes will be made")
        print("=" * 60)
        all_pubs = [(pub_id, label) for _, pub_id, label, _, _ in ALL_CHANGES]
        seen = set()
        for pub_id, label in all_pubs:
            if pub_id not in seen:
                if only_ids is None or pub_id in only_ids:
                    inspect_publisher(pub_id, label)
                seen.add(pub_id)
        return

    mode = "LIVE" if not dry_run else "DRY-RUN"
    print("=" * 70)
    print(f"  LL Publisher Floor Fix — Apr 16 2026  [{mode}]")
    print("=" * 70)
    print(f"  Changes planned: {len(ALL_CHANGES)}")
    print(f"    LOWER (floor too high):  {len(LOWER_FLOORS)}")
    print(f"    RAISE (high-WR gap):     {len(RAISE_FLOORS)}")
    print()

    results = []
    ok = 0
    errors = 0

    for direction, pub_id, label, new_floor, reason in ALL_CHANGES:
        if only_ids is not None and pub_id not in only_ids:
            continue

        tag = "↓" if direction == "LOWER" else "↑"
        print(f"\n[{tag} {direction}]  {label}  (id={pub_id})  → floor=${new_floor:.2f}")
        print(f"  Reason: {reason}")

        r = apply_adunit_floor(pub_id, label, new_floor, reason, dry_run)
        results.append(r)

        if r.get("error"):
            errors += 1
        else:
            ok += 1

    print("\n" + "=" * 70)
    print(f"  SUMMARY: {ok} publishers processed, {errors} errors")
    if dry_run:
        print("  ⚡ DRY-RUN — nothing was changed. Re-run with --live to apply.")
    else:
        print("  ✓ LIVE — changes applied. Monitor pilot_snapshot tomorrow morning.")
    print("=" * 70)

    if not dry_run and results:
        out_path = os.path.join(_REPO_ROOT, "logs", f"floor_fix_apr16_results.json")
        with open(out_path, "w") as f:
            json.dump(results, f, indent=2)
        print(f"\n  Results saved to {out_path}")


if __name__ == "__main__":
    main()
