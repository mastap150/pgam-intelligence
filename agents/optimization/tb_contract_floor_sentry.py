"""
agents/optimization/tb_contract_floor_sentry.py

Hourly defense-in-depth scan: walks every placement protected by
`tb_mgmt.PROTECTED_FLOOR_MINIMUMS`, verifies the live floor is at or
above contract, and restores any violation.

Why this is needed even with the write-path clamp
--------------------------------------------------
The clamp blocks any code-path violation. But it can't see:
  - UI edits made by humans through the TB admin panel
  - Direct API writes that bypass `tb_mgmt.set_floor` (e.g. anyone
    POSTing to `edit_placement_*` directly)
  - Server-side resets / migrations
  - The PROTECTED_FLOOR_MINIMUMS list growing stale

This agent is the catch-all. Hourly walk → restore violators → ledger
every restore for accountability.

Operation
---------
1. For each (entity_id, min_price) in PROTECTED_FLOOR_MINIMUMS:
   - If entity is a placement_id: GET that placement, check price
   - If entity is an inventory_id: GET all placements under that inv,
     check each placement's price
2. If live price < contract_min: tbm.set_floor(price=contract_min,
   actor="contract_sentry", reason="restored from $X to contract floor")
3. Log every check (whether restored or not — for audit)
4. Slack alert if ≥1 violation found

Safety
------
- The set_floor call goes through the same write-path clamp, so even
  here we're double-protected.
- No-op when PROTECTED_FLOOR_MINIMUMS is empty (current state on TB).
- Uses verify=True so any restore that doesn't actually land surfaces
  loud as a verify failure in the ledger.
"""

from __future__ import annotations

import os
import sys
from datetime import datetime, timezone

_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

from dotenv import load_dotenv
load_dotenv(override=True)
import core.tb_mgmt as tbm
from core import tb_ledger


def _placements_under_inventory(inventory_id: int) -> list[dict]:
    """Best-effort fetch of every placement under an inventory."""
    try:
        return tbm.list_placements(inventory_id=inventory_id) or []
    except Exception as e:
        print(f"  ✗ list_placements({inventory_id}): {e}")
        return []


def run(dry_run: bool = False) -> dict:
    print(f"\n{'='*72}\n  TB Contract Floor Sentry  "
          f"{'[DRY]' if dry_run else '[LIVE]'}\n{'='*72}")

    if not tbm.PROTECTED_FLOOR_MINIMUMS:
        msg = ("  no PROTECTED_FLOOR_MINIMUMS configured — sentry is "
               "no-op until contract mapping is enumerated.")
        print(msg)
        return {"checked": 0, "violations": 0, "restored": 0, "no_op": True}

    checked = 0
    violations: list[dict] = []
    restored: list[dict] = []

    for entity_id, contract_min in tbm.PROTECTED_FLOOR_MINIMUMS.items():
        contract_min = float(contract_min)
        # Decide whether this is a placement or inventory:
        # try as placement first; if it 404s, treat as inventory.
        targets: list[dict] = []
        try:
            p = tbm.get_placement(entity_id)
            if p and p.get("placement_id"):
                targets = [p]
        except Exception:
            pass
        if not targets:
            # inventory_id path
            targets = _placements_under_inventory(entity_id)

        for p in targets:
            checked += 1
            pid = p["placement_id"]
            live = float(p.get("price") or 0)
            if live + 1e-6 >= contract_min:
                continue
            # VIOLATION
            v = {
                "placement_id": pid,
                "title":        p.get("title"),
                "inventory_id": p.get("inventory_id"),
                "live_price":   live,
                "contract_min": contract_min,
                "shortfall":    round(contract_min - live, 4),
            }
            violations.append(v)
            print(f"  🚨 VIOLATION  pid={pid} {p.get('title','?')[:32]}  "
                  f"live=${live:.4f} < contract=${contract_min:.4f}")
            if dry_run:
                continue
            try:
                tbm.set_floor(
                    pid, price=contract_min, dry_run=False,
                    actor="contract_sentry",
                    reason=f"restored from ${live:.4f} to contract ${contract_min:.4f}",
                    verify=True,
                )
                restored.append(v)
            except Exception as e:
                print(f"  ✗ restore failed pid={pid}: {e}")

    print(f"\n  checked={checked}  violations={len(violations)}  "
          f"restored={len(restored)}")

    # Slack
    if violations:
        try:
            from core.slack import post_message
            tag = "🚨 CONTRACT FLOOR VIOLATIONS" if not dry_run else "🔍 CONTRACT FLOOR DRY"
            lines = [f"{tag} — {len(violations)} placement(s) below contract"]
            for v in violations[:8]:
                lines.append(f"  • [{v['placement_id']}] {v.get('title','?')[:30]}  "
                             f"${v['live_price']:.2f} → ${v['contract_min']:.2f}")
            if not dry_run:
                lines.append(f"\n✅ Restored {len(restored)}")
            post_message("\n".join(lines))
        except Exception:
            pass

    return {
        "checked":    checked,
        "violations": violations,
        "restored":   restored,
        "timestamp":  datetime.now(timezone.utc).isoformat(),
    }


if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()
    run(dry_run=args.dry_run)
