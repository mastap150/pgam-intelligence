"""
agents/optimization/contract_floor_sentry.py

Hourly defense-in-depth scan: enumerate every demand whose name matches a
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

This scanner runs hourly and closes those gaps. Anything below contract gets
restored to the minimum and logged with ``actor="contract_floor_sentry"``.

Visibility
----------
On every restoration, a Slack message is posted (deduped to one per demand
per UTC day for the normal P3 case). If the same demand has been restored
≥REPEAT_OFFENDER_THRESHOLD times in the trailing 7 days, the message is
escalated to P1 and bypasses dedup — that pattern means something upstream
keeps dropping the floor and the sentry alone is masking the real bug.

Safety posture
--------------
- Raises only (never lowers) — strictly a floor-of-the-floor enforcement
- Uses ``set_demand_floor()`` so it gets verified + ledgered
- If LL_DRY_RUN=true, logs what would change without writing
"""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

from core import floor_ledger, ll_mgmt, slack
from core.ll_mgmt import PROTECTED_FLOOR_MINIMUMS


ACTOR = "contract_floor_sentry"
REPEAT_OFFENDER_LOOKBACK_DAYS = 7
REPEAT_OFFENDER_THRESHOLD = 2


def _matches_token(name: str, tokens: tuple[str, ...]) -> bool:
    name_lower = (name or "").lower()
    return any(tok in name_lower for tok in tokens)


def _recent_restorations(demand_id: int, lookback_days: int) -> int:
    """Count prior contract_floor_sentry restorations for this demand in window."""
    cutoff = datetime.now(timezone.utc) - timedelta(days=lookback_days)
    cutoff_iso = cutoff.isoformat().replace("+00:00", "Z")
    count = 0
    for row in floor_ledger.read_all():
        if row.get("demand_id") != int(demand_id):
            continue
        if row.get("actor") != ACTOR:
            continue
        if row.get("dry_run") or not row.get("applied"):
            continue
        if row.get("ts_utc", "") >= cutoff_iso:
            count += 1
    return count


def _slack_dedup_key(demand_id: int) -> str:
    """One P3 Slack post per demand per UTC day. P1 escalations bypass this."""
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    return f"contract_floor_sentry:{demand_id}:{today}"


def _post_violation(restoration: dict, prior_restorations: int) -> None:
    """Post to Slack — P3 by default, P1 if repeat offender."""
    demand_id = restoration["demand_id"]
    is_repeat = prior_restorations >= REPEAT_OFFENDER_THRESHOLD

    if is_repeat:
        # Always page on repeat offenders, no dedup — this is the upstream-bug signal.
        header = (
            f":rotating_light: *Contract floor REPEAT OFFENDER* — "
            f"restored {prior_restorations + 1}× in last "
            f"{REPEAT_OFFENDER_LOOKBACK_DAYS}d. Something upstream keeps dropping this; "
            f"check write-path / UI edits / archived-and-recreated demands."
        )
    else:
        if slack.already_sent_today(_slack_dedup_key(demand_id)):
            return
        header = ":warning: Contract floor breach restored."

    msg = (
        f"{header}\n"
        f"• demand `{demand_id}` ({(restoration.get('demand_name') or '')[:60]})\n"
        f"• live floor `{restoration['was']}` → restored to `${restoration['min_floor']}`\n"
        f"• actor `{ACTOR}`"
    )
    slack.send_text(msg)
    if not is_repeat:
        slack.mark_sent(_slack_dedup_key(demand_id))


def scan_and_enforce() -> dict:
    """Walk all demands, find any below their contract minimum, restore."""
    print(f"[{ACTOR}] scanning {len(PROTECTED_FLOOR_MINIMUMS)} protected contract(s)")
    all_demands = ll_mgmt.get_demands(include_archived=False)
    print(f"[{ACTOR}] fetched {len(all_demands)} active demands")

    violations = []
    restored = []
    repeat_offenders = []
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
                        reason=(f"Sentry scan: live floor "
                                f"{live_val} below contract minimum {min_floor} — restored"),
                        dry_run=False, applied=True,
                    )
                    restoration = {
                        "demand_id": did, "demand_name": name,
                        "min_floor": min_floor, "was": live_val,
                        "result": result,
                    }
                    restored.append(restoration)

                    # Subtract 1: the ledger entry above is included in read_all().
                    prior = max(0, _recent_restorations(did, REPEAT_OFFENDER_LOOKBACK_DAYS) - 1)
                    if prior >= REPEAT_OFFENDER_THRESHOLD:
                        repeat_offenders.append({**restoration, "prior_restorations": prior})

                    try:
                        _post_violation(restoration, prior)
                    except Exception as e:
                        print(f"[{ACTOR}] Slack post failed: {e}")

                    print(
                        f"[{ACTOR}] restored demand {did} to ${min_floor} "
                        f"(was {live_val}, prior_restorations_7d={prior}): {name[:50]}"
                    )
                except Exception as e:
                    print(f"[{ACTOR}] FAILED to restore demand {did}: {e}")
                    try:
                        slack.send_text(
                            f":rotating_light: contract_floor_sentry FAILED to restore "
                            f"demand `{did}` ({name[:60]}): `{e}` — manual intervention required."
                        )
                    except Exception:
                        pass
            break  # only match first contract token

    return {
        "scanned": len(all_demands),
        "violations": violations,
        "restored": restored,
        "repeat_offenders": repeat_offenders,
        "ran_at": datetime.now(timezone.utc).isoformat(),
    }


def run() -> dict:
    """Scheduler entry."""
    return scan_and_enforce()


if __name__ == "__main__":
    result = run()
    print(json.dumps(result, indent=2))
