"""
Fleet-wide lurlEnabled rollout for the other 6 pilot suppliers (not Start.IO).

Target suppliers: PubNative=28, AppStock=33, BidMachine=24, Illumin=2,
                  Algorix=22, Smaato=30.

For each eligible publisher:
  • must be in TARGET_SUPPLIERS
  • must have status=1 (active) — paused pubs have no signal
  • must currently have lurlEnabled=False
flip lurlEnabled to True via PUT /v1/publishers/{id} and register a
pilot_watchdog watch (kind="lurl_enable") so the daily 09:30 ET scheduler
auto-reverts if rev drops >15 % AND eCPM doesn't lift >10 %.

Run:
    python3.13 scripts/lurl_fleet_enable_apr17.py           # dry-run
    python3.13 scripts/lurl_fleet_enable_apr17.py --live    # apply
"""
import argparse
import os
import sys
import json
from datetime import datetime, timezone

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)
from dotenv import load_dotenv
load_dotenv(dotenv_path=os.path.join(_ROOT, ".env"), override=True)

import core.ll_mgmt as llm
import scripts.pilot_watchdog as watchdog

TARGET_SUPPLIERS = {28, 33, 24, 2, 22, 30}
LOG_PATH = os.path.join(_ROOT, "logs", "lurl_fleet_enable_apr17.json")


def _supplier_id(p: dict):
    return p.get("supplier") or p.get("supplier_id") or p.get("supplierId")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--live", action="store_true")
    args = ap.parse_args()
    dry = not args.live
    mode = "LIVE" if not dry else "DRY-RUN"
    print(f"=== Fleet lurlEnabled rollout — {mode} ===\n")

    pubs = llm.get_publishers(include_archived=False)
    eligible = [
        p for p in pubs
        if _supplier_id(p) in TARGET_SUPPLIERS
        and p.get("status") == 1
        and p.get("lurlEnabled") is not True
    ]
    skipped_paused = [
        p for p in pubs
        if _supplier_id(p) in TARGET_SUPPLIERS
        and p.get("status") != 1
        and p.get("lurlEnabled") is not True
    ]

    print(f"Eligible (active, lurl off):   {len(eligible)} pubs")
    print(f"Skipped (paused, lurl off):    {len(skipped_paused)} pubs  — no traffic signal, not touching")
    print()

    results = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "mode":      mode,
        "applied":   [],
        "errors":    [],
        "skipped_paused": [
            {"id": p.get("id"), "name": p.get("name"), "supplier": _supplier_id(p)}
            for p in skipped_paused
        ],
    }

    for p in eligible:
        pid = int(p["id"])
        name = p.get("name", f"pub-{pid}")
        sup = _supplier_id(p)
        if dry:
            print(f"  ~ DRY  pub={pid:>10}  sup={sup:>2}  {name[:50]:<50}  lurlEnabled:False→True")
            results["applied"].append({
                "publisher_id": pid, "publisher_name": name, "supplier": sup, "dry_run": True,
            })
            continue

        # LIVE — fetch full publisher, flip, PUT, then register watch
        try:
            full = llm.get_publisher(pid)
            prior = bool(full.get("lurlEnabled", False))
            full["lurlEnabled"] = True
            llm._put(f"/v1/publishers/{pid}", full)
            watch_id = watchdog.register_lurl_watch(
                publisher_id=pid,
                publisher_name=name,
                prior_lurl=prior,
                new_lurl=True,
            )
            print(f"  ✓ pub={pid:>10}  sup={sup:>2}  {name[:50]:<50}  "
                  f"lurlEnabled:{prior}→True  watch={watch_id}")
            results["applied"].append({
                "publisher_id": pid, "publisher_name": name, "supplier": sup,
                "prior_lurl": prior, "watch_id": watch_id, "dry_run": False,
            })
        except Exception as e:
            print(f"  ✗ pub={pid:>10}  sup={sup:>2}  {name[:50]:<50}  ERROR: {e}")
            results["errors"].append({
                "publisher_id": pid, "publisher_name": name, "supplier": sup, "error": str(e),
            })

    os.makedirs(os.path.dirname(LOG_PATH), exist_ok=True)
    with open(LOG_PATH, "w") as f:
        json.dump(results, f, indent=2)

    print(f"\n{mode} complete.  applied={len(results['applied'])}  errors={len(results['errors'])}")
    print(f"  log → {LOG_PATH}")


if __name__ == "__main__":
    main()
