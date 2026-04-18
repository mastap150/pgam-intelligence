"""
scripts/optimal_price_sweep.py

One-shot: enable TB's dynamic floor optimizer (is_optimal_price) on every
TB placement where it's currently off.

Why
---
is_optimal_price lets TB's own yield ML tune the floor in real time per
placement. Having it OFF leaves free revenue on the table — TB's engine
outperforms static floors on mixed-demand placements. Zero downside if
we need to roll back: just set is_optimal_price=False.

Usage
-----
    python3 -m scripts.optimal_price_sweep              # dry-run summary
    python3 -m scripts.optimal_price_sweep --apply      # flip all off→on
    python3 -m scripts.optimal_price_sweep --rollback   # flip everything back off

Audit
-----
Writes logs/optimal_price_sweep_log.json with every change + timestamp.
"""

from __future__ import annotations

import json
import os
import sys
from datetime import datetime, timezone

_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

from dotenv import load_dotenv
load_dotenv(override=True)

import core.tb_mgmt as tbm

LOG_FILE = os.path.join(_REPO_ROOT, "logs", "optimal_price_sweep_log.json")
os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)


def append_log(entries: list[dict]) -> None:
    prior = []
    if os.path.exists(LOG_FILE):
        with open(LOG_FILE) as f:
            try: prior = json.load(f)
            except Exception: prior = []
    prior.extend(entries)
    with open(LOG_FILE, "w") as f:
        json.dump(prior, f, indent=2)


def run(apply: bool = False, rollback: bool = False) -> None:
    mode = "ROLLBACK" if rollback else ("APPLY" if apply else "DRY_RUN")
    print(f"\n{'='*72}\n  Optimal-Price Sweep  [{mode}]\n{'='*72}")

    # Account-wide scope via report endpoint (list_placement is user-scoped)
    placements = tbm.list_all_placements_via_report(days=14, min_impressions=100)
    print(f"  {len(placements)} placements account-wide (≥100 imps / 14d)")

    if rollback:
        targets = [p for p in placements if p.get("is_optimal_price")]
        print(f"  {len(targets)} with optimal_price=True → flip OFF")
    else:
        targets = [p for p in placements if not p.get("is_optimal_price")]
        print(f"  {len(targets)} with optimal_price=False → flip ON")

    if not targets:
        print("  nothing to do.")
        return

    # Preview — show top candidates by impression volume
    preview = sorted(targets, key=lambda x: -(x.get("_imps_window", 0)))
    print(f"\n  Preview (top 15 by volume):")
    for p in preview[:15]:
        print(f"    [{p['placement_id']}] {p.get('title','')[:42]:<42} "
              f"type={p.get('type','?'):<7} floor=${p.get('price',0):.2f} "
              f"imps={p.get('_imps_window',0):>10,} eCPM=${p.get('_ecpm_window',0):.2f}")
    if len(targets) > 15:
        print(f"    ... +{len(targets) - 15} more")

    if not (apply or rollback):
        # Slack brief — richer summary for "blocked" visibility
        try:
            from core.slack import post_message
            total_imps = sum(p.get("_imps_window", 0) for p in targets)
            lines = [
                f"🔍 *Optimal-Price Sweep* — DRY-RUN (awaiting edit_placement_* endpoint)",
                f"Would flip `is_optimal_price=True` on *{len(targets)}* placements",
                f"Total impressions in 14d window: {total_imps:,}",
                "\n*Top 10 by volume:*",
            ]
            for p in preview[:10]:
                lines.append(
                    f"  • [{p['placement_id']}] {p.get('title','')[:36]}  "
                    f"imps={p.get('_imps_window',0):,}  "
                    f"eCPM=${p.get('_ecpm_window',0):.2f}"
                )
            post_message("\n".join(lines))
        except Exception:
            pass
        print("\n  (dry-run — pass --apply to execute, --rollback to revert)")
        return

    new_state = False if rollback else True
    entries: list[dict] = []
    ok = fail = 0
    for p in targets:
        pid = p["placement_id"]
        try:
            tbm.set_floor(pid, is_optimal_price=new_state, dry_run=False)
            entries.append({
                "placement_id":     pid,
                "title":            p.get("title"),
                "type":             p.get("type"),
                "inventory_id":     p.get("inventory_id"),
                "new_optimal":      new_state,
                "mode":             mode,
                "timestamp":        datetime.now(timezone.utc).isoformat(),
                "applied":          True,
            })
            ok += 1
        except Exception as e:
            entries.append({
                "placement_id": pid,
                "mode":         mode,
                "timestamp":    datetime.now(timezone.utc).isoformat(),
                "applied":      False,
                "error":        str(e),
            })
            fail += 1
            print(f"    ✗ [{pid}] {e}")

    append_log(entries)
    print(f"\n  ✓ {ok} flipped  |  ✗ {fail} failed")
    print(f"  Log → {LOG_FILE}")

    # Slack
    try:
        from core.slack import post_message
        icon = "🟢" if new_state else "🔄"
        post_message(
            f"{icon} *Optimal-Price Sweep* ({mode}): "
            f"flipped {ok} placements to is_optimal_price={new_state}"
            + (f"  ({fail} failed)" if fail else "")
        )
    except Exception:
        pass


if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply",    action="store_true", help="flip off→on")
    ap.add_argument("--rollback", action="store_true", help="flip on→off")
    args = ap.parse_args()
    if args.apply and args.rollback:
        sys.exit("cannot pass both --apply and --rollback")
    run(apply=args.apply, rollback=args.rollback)
