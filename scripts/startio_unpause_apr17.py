"""
Unpause the 4 paused Start.IO Unruly EU demand entries identified by
startio_coverage_audit.py.

Run:
    python3.13 scripts/startio_unpause_apr17.py             # dry-run
    python3.13 scripts/startio_unpause_apr17.py --live      # apply
"""
import argparse
import os
import sys
from datetime import datetime, timezone

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)
from dotenv import load_dotenv
load_dotenv(dotenv_path=os.path.join(_ROOT, ".env"), override=True)

import core.ll_mgmt as llm

# (publisher_id, demand_id, label) — from startio_coverage_audit_apr17.json class_a_paused
PAUSED_ENTRIES = [
    (290115330, 663, "Start.IO EU / Unruly Start.IO EU"),
    (290115330, 664, "Start.IO EU / Unruly Start.IO oRTB 2.6 EU"),
    (290115343, 663, "Start.IO EU Interstitial / Unruly Start.IO EU"),
    (290115343, 664, "Start.IO EU Interstitial / Unruly Start.IO oRTB 2.6 EU"),
]


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--live", action="store_true")
    args = p.parse_args()
    dry = not args.live

    mode = "LIVE" if not dry else "DRY-RUN"
    print(f"=== Start.IO unpause — {mode} ===\n")

    for pid, did, label in PAUSED_ENTRIES:
        try:
            llm.enable_publisher_demand(pid, did, dry_run=dry)
            print(f"  ✓ pub={pid} demand={did}  {label}")
        except Exception as e:
            print(f"  ✗ pub={pid} demand={did}  {label}  ERROR: {e}")

    print(f"\n{mode} complete.")


if __name__ == "__main__":
    main()
