"""
scripts/min_floor_sweep.py

Enforce a minimum floor across all active placements.

Any placement with price < MIN_FLOOR gets bumped to MIN_FLOOR. Captures
the case where low-value bids ($0.01-$0.04) fill slots that could earn
more with a minimum $0.05 floor forcing better demand.

Usage:
    python3 -m scripts.min_floor_sweep               # dry-run
    python3 -m scripts.min_floor_sweep --apply       # execute
    python3 -m scripts.min_floor_sweep --rollback    # revert from log

Rollback uses logs/min_floor_sweep_log.json to restore per-placement
before-floors.
"""
from __future__ import annotations
import os, sys, json
from datetime import datetime, timezone
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from dotenv import load_dotenv; load_dotenv(override=True)
import core.tb_mgmt as tbm

MIN_FLOOR         = 0.05
MIN_IMPS_FILTER   = 500        # only touch placements with real activity
MAX_APPLIES_RUN   = 200

LOG_FILE = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                        "logs", "min_floor_sweep_log.json")


def run(apply: bool = False, rollback: bool = False) -> None:
    mode = "ROLLBACK" if rollback else ("APPLY" if apply else "DRY")
    print(f"\n{'='*70}\n  Min Floor Sweep  [{mode}]  (min=${MIN_FLOOR})\n{'='*70}")

    if rollback:
        if not os.path.exists(LOG_FILE):
            print("  no log to roll back"); return
        with open(LOG_FILE) as f: prior = json.load(f)
        latest = {a["placement_id"]: a["before_floor"]
                  for a in prior if a.get("applied")}
        print(f"  reverting {len(latest)} placements...")
        for pid, floor in latest.items():
            try: tbm.set_floor(pid, price=floor, dry_run=False)
            except Exception as e: print(f"  ✗ {pid}: {e}")
        return

    placements = tbm.list_all_placements_via_report(days=14, min_impressions=MIN_IMPS_FILTER)
    targets = [p for p in placements
               if float(p.get("price") or 0.0) > 0
               and float(p.get("price") or 0.0) < MIN_FLOOR]
    targets.sort(key=lambda p: -(p.get("_imps_window", 0)))
    print(f"  {len(targets)} placements below ${MIN_FLOOR} (≥{MIN_IMPS_FILTER} imps)")
    for p in targets[:15]:
        print(f"    [{p['placement_id']}] {p.get('title','')[:40]:<40}  "
              f"${p.get('price'):.2f} → ${MIN_FLOOR}  imps={p.get('_imps_window'):,}")
    if len(targets) > 15: print(f"    +{len(targets)-15} more")

    actions = []
    applied = 0
    for p in targets[:MAX_APPLIES_RUN]:
        pid   = p["placement_id"]
        before= float(p.get("price") or 0.0)
        if apply:
            try:
                tbm.set_floor(pid, price=MIN_FLOOR, dry_run=False)
                actions.append({"placement_id": pid, "title": p.get("title"),
                                "before_floor": before, "new_floor": MIN_FLOOR,
                                "applied": True,
                                "timestamp": datetime.now(timezone.utc).isoformat()})
                applied += 1
            except Exception as e:
                print(f"    ✗ {pid}: {e}")
        else:
            actions.append({"placement_id": pid, "before_floor": before,
                            "new_floor": MIN_FLOOR, "applied": False,
                            "dry_run": True})

    if apply:
        prior = []
        if os.path.exists(LOG_FILE):
            with open(LOG_FILE) as f:
                try: prior = json.load(f)
                except Exception: prior = []
        prior.extend(actions)
        with open(LOG_FILE, "w") as f: json.dump(prior, f, indent=2)

    print(f"\n  {'Applied' if apply else 'Would apply'}: {applied or len(targets)}")

    try:
        from core.slack import post_message
        tag = "🟢 LIVE" if apply else "🔍 DRY"
        post_message(f"💰 *Min Floor Sweep* {tag} — {applied or len(targets)} placements "
                     f"{'raised' if apply else 'would be raised'} to ${MIN_FLOOR}")
    except Exception: pass


if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply",    action="store_true")
    ap.add_argument("--rollback", action="store_true")
    args = ap.parse_args()
    run(apply=args.apply, rollback=args.rollback)
