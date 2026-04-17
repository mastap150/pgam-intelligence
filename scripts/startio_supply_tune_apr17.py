"""
Supply-side tuning for the 6 Start.IO supply publishers (supplier_id=7).

Three changes bundled per publisher in a single PUT:
  A. lurlEnabled          False → True       (all 6)  — DSPs learn win prices
  B. margin               0.15  → 0.10       (EU only: 290115330, 290115343)
  C. status               2     → 1          (paused pubs: 284, 285, 330, 343)
     and deliveryStatus   6     → 1          (companion of C)

Run:
    python3.13 scripts/startio_supply_tune_apr17.py            # dry-run
    python3.13 scripts/startio_supply_tune_apr17.py --live     # apply
"""
import argparse
import os
import sys

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)
from dotenv import load_dotenv
load_dotenv(dotenv_path=os.path.join(_ROOT, ".env"), override=True)

import core.ll_mgmt as llm

STARTIO_PUBS = [290115284, 290115285, 290115330, 290115343, 290115374, 290115375]
EU_PUBS = {290115330, 290115343}           # 15% → 10% margin
PAUSED_PUBS = {290115284, 290115285, 290115330, 290115343}  # status 2 → 1


def tune(pub_id: int, dry: bool):
    pub = llm.get_publisher(pub_id)
    name = pub.get("name", f"pub-{pub_id}")
    changes = []

    # A — lurlEnabled
    if pub.get("lurlEnabled") is not True:
        changes.append(("lurlEnabled", pub.get("lurlEnabled"), True))
        pub["lurlEnabled"] = True

    # B — margin normalization on EU
    if pub_id in EU_PUBS:
        cur = float(pub.get("margin") or 0)
        if abs(cur - 0.10) > 1e-6:
            changes.append(("margin", cur, 0.10))
            pub["margin"] = 0.10

    # C — unpause
    if pub_id in PAUSED_PUBS:
        if pub.get("status") != 1:
            changes.append(("status", pub.get("status"), 1))
            pub["status"] = 1
        if pub.get("deliveryStatus") != 1:
            changes.append(("deliveryStatus", pub.get("deliveryStatus"), 1))
            pub["deliveryStatus"] = 1

    if not changes:
        print(f"  = pub={pub_id}  {name[:36]:<36}  no-op")
        return

    chg_str = ", ".join(f"{k}:{a}→{b}" for k, a, b in changes)
    if dry:
        print(f"  ~ pub={pub_id}  {name[:36]:<36}  DRY  {chg_str}")
        return

    try:
        llm._put(f"/v1/publishers/{pub_id}", pub)
        print(f"  ✓ pub={pub_id}  {name[:36]:<36}  {chg_str}")
    except Exception as e:
        print(f"  ✗ pub={pub_id}  {name[:36]:<36}  ERROR: {e}")


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--live", action="store_true")
    args = p.parse_args()
    dry = not args.live
    mode = "LIVE" if not dry else "DRY-RUN"
    print(f"=== Start.IO supply-side tune — {mode} ===\n")
    for pid in STARTIO_PUBS:
        tune(pid, dry)
    print(f"\n{mode} complete.")


if __name__ == "__main__":
    main()
