"""
agents/optimization/contract_floor_sentry.py

Daily defense-in-depth scan: enumerate every demand whose name matches a
contract-protected token (e.g. "9 Dots" → min $1.70), fetch its live floor
from LL, and if any have slipped below the contract minimum, restore them.

Why this exists
---------------
The write-path clamp in ``core.ll_mgmt.set_demand_floor()`` (PR #7) catches
API writes that try to drop below a contract floor. But it doesn't catch:
  - Manual UI edits in the Limelight dashboard (bypasses our code entirely)
  - A demand archived + recreated with a fresh $0 floor
  - Bugs elsewhere in our stack that somehow bypass the clamp
  - Third-party (LL-side) config changes

This scanner runs daily and closes those gaps. Anything below contract gets
restored to the minimum and logged with ``actor="contract_floor_sentry"``.

Safety posture
--------------
- Raises only (never lowers) — strictly a floor-of-the-floor enforcement
- Uses ``set_demand_floor()`` so it gets verified + ledgered
- If LL_DRY_RUN=true, logs what would change without writing
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from core import ll_mgmt, floor_ledger
from core.ll_mgmt import PROTECTED_FLOOR_MINIMUMS


ACTOR = "contract_floor_sentry"


def _matches_token(name: str, tokens: tuple[str, ...]) -> bool:
    name_lower = (name or "").lower()
    return any(tok in name_lower for tok in tokens)


def scan_and_enforce() -> dict:
    """Walk all demands, find any below their contract minimum, restore."""
    print(f"[{ACTOR}] scanning {len(PROTECTED_FLOOR_MINIMUMS)} protected contract(s)")
    all_demands = ll_mgmt.get_demands(include_archived=False)
    print(f"[{ACTOR}] fetched {len(all_demands)} active demands")

    violations = []
    restored = []
    for d in all_demands:
        name = d.get("name") or ""
        floor = d.get("minBidFloor")
        did = d.get("id")
        for tokens, min_floor in PROTECTED_FLOOR_MINIMUMS:
            if not _matches_token(name, tokens):
                continue
            try:
                live_val = float(floor) if floor is not None else None
            except (TypeError, ValueError):
                live_val = None
            if live_val is None or live_val < min_floor:
                violations.append({
                    "demand_id": did, "demand_name": name,
                    "live_floor": live_val, "min_floor": min_floor,
                })
                try:
                    result = ll_mgmt.set_demand_floor(
                        did, min_floor,
                        verify=True,
                        allow_multi_pub=True,
                        _publishers_running_it=10,
                    )
                    floor_ledger.record(
                        publisher_id=0, publisher_name="[contract-floor-sentry]",
                        demand_id=did, demand_name=name,
                        old_floor=live_val, new_floor=min_floor,
                        actor=ACTOR,
                        reason=(f"Daily sentry scan: live floor "
                                f"{live_val} below contract minimum {min_floor} — restored"),
                        dry_run=False, applied=True,
                    )
                    restored.append({"demand_id": did, "min_floor": min_floor,
                                     "was": live_val, "result": result})
                    print(f"[{ACTOR}] restored demand {did} to ${min_floor} (was {live_val}): {name[:50]}")
                except Exception as e:
                    print(f"[{ACTOR}] FAILED to restore demand {did}: {e}")
            break  # only match first contract token

    return {
        "scanned": len(all_demands),
        "violations": violations,
        "restored": restored,
        "ran_at": datetime.now(timezone.utc).isoformat(),
    }


def run() -> dict:
    """Scheduler entry."""
    return scan_and_enforce()


if __name__ == "__main__":
    result = run()
    print(json.dumps(result, indent=2))
