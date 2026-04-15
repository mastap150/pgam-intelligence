"""
scripts/pilot_actions.py

Controlled execution module for the PGAM pilot program.

Only publishers whose supplier_id is in PILOT_SUPPLIER_IDS may be touched.
All other publishers are hard-blocked.  Every real (non-dry-run) action is:
  • Posted to Slack via core.slack.send_blocks
  • Appended to logs/pilot_YYYY-MM.json so pilot_snapshot.py can surface it

Safety constants
----------------
PILOT_SUPPLIER_IDS   — only suppliers 28 (PubNative) and 33 (AppStock)
MAX_FLOOR_CHANGE_PCT — floor may not move more than 25 % in one step
MIN_FLOOR            — floor may never be set below $0.10
"""

import json
import os
import sys
from datetime import datetime, timezone

# Allow running directly from the repo root or from the scripts/ dir
_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

import core.ll_mgmt as ll_mgmt
import core.slack    as slack

# ---------------------------------------------------------------------------
# Safety constants
# ---------------------------------------------------------------------------

PILOT_SUPPLIER_IDS   = {28, 33}   # PubNative=28, AppStock=33
MAX_FLOOR_CHANGE_PCT = 25.0        # Never change a floor by more than 25 % in one step
MIN_FLOOR            = 0.10        # Never set a floor below $0.10

# ---------------------------------------------------------------------------
# Log path
# ---------------------------------------------------------------------------

LOG_DIR = os.path.join(os.path.dirname(__file__), "..", "logs")


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _log_path() -> str:
    """Return the path to this month's pilot log file."""
    month = datetime.now().strftime("%Y-%m")
    return os.path.normpath(os.path.join(LOG_DIR, f"pilot_{month}.json"))


def _now_et_str() -> str:
    """Return a human-readable timestamp in ET (approximated as UTC-4/UTC-5)."""
    # Use local time for display — the host should be configured to ET.
    # Fall back gracefully if tzdata is unavailable.
    try:
        from zoneinfo import ZoneInfo
        now = datetime.now(ZoneInfo("America/New_York"))
        return now.strftime("%Y-%m-%d %H:%M ET")
    except Exception:
        return datetime.now().strftime("%Y-%m-%d %H:%M")


def _now_iso() -> str:
    """Return the current UTC time as an ISO-8601 string."""
    return datetime.now(timezone.utc).isoformat()


def _today_str() -> str:
    """Return today's date as YYYY-MM-DD."""
    return datetime.now().strftime("%Y-%m-%d")


def _get_current_floor(publisher: dict, demand_id: int) -> float | None:
    """
    Walk publisher.biddingpreferences to find the typeField with type=3
    (bid floor override) for the given demand_id.

    Returns the current floor value if setOnRule=True, else None.
    """
    for pref in publisher.get("biddingpreferences", []):
        for item in pref.get("value", []):
            if item.get("id") != demand_id:
                continue
            for tf in item.get("typeFields", []):
                if tf.get("type") == 3 and tf.get("setOnRule"):
                    try:
                        return float(tf.get("value"))
                    except (TypeError, ValueError):
                        return None
    return None


def _set_floor_in_publisher(publisher: dict, demand_id: int, new_floor: float) -> bool:
    """
    Mutate the publisher dict in-place, setting typeField type=3 for the
    given demand_id.  Creates the typeField entry if it does not exist.

    Returns True if the demand was found and updated, False otherwise.
    """
    for pref in publisher.get("biddingpreferences", []):
        for item in pref.get("value", []):
            if item.get("id") != demand_id:
                continue
            type_fields = item.setdefault("typeFields", [])
            for tf in type_fields:
                if tf.get("type") == 3:
                    tf["value"]      = new_floor
                    tf["setOnRule"]  = True
                    return True
            # typeField type=3 does not yet exist — append it
            type_fields.append({"type": 3, "value": new_floor, "setOnRule": True})
            return True
    return False


def _floor_change_pct(old_floor: float, new_floor: float) -> float:
    """Return the absolute percentage change from old_floor to new_floor."""
    if old_floor == 0.0:
        return 0.0
    return abs((new_floor - old_floor) / old_floor) * 100.0


def _supplier_id(publisher: dict) -> int | None:
    """Return the supplier ID of a publisher dict, or None if absent."""
    return publisher.get("supplier") or publisher.get("supplier_id") or publisher.get("supplierId")


# ---------------------------------------------------------------------------
# Slack helpers
# ---------------------------------------------------------------------------

def _floor_change_blocks(
    publisher: dict,
    demand_name: str,
    old_floor: float,
    new_floor: float,
    reason: str,
    mode: str,
) -> list[dict]:
    """Build Slack Block Kit blocks for a floor-change confirmation."""
    direction = "−" if new_floor < old_floor else "+"
    change_pct = abs((new_floor - old_floor) / old_floor * 100) if old_floor else 0.0
    sup_id     = _supplier_id(publisher) or "?"
    pub_name   = publisher.get("name", "?")

    text = (
        f":white_check_mark: *Pilot Action Applied*\n"
        f"Publisher: {pub_name} (supplier {sup_id})\n"
        f"Demand: {demand_name}\n"
        f"Change: Floor ${old_floor:.2f} → ${new_floor:.2f}  "
        f"({direction}{change_pct:.0f}%)\n"
        f"Reason: {reason}\n"
        f"Time: {_now_et_str()}\n"
        f"Mode: {mode}"
    )

    return [{"type": "section", "text": {"type": "mrkdwn", "text": text}}]


def _seat_toggle_blocks(
    publisher: dict,
    demand_name: str,
    action: str,
    reason: str,
    mode: str,
) -> list[dict]:
    """Build Slack Block Kit blocks for an enable/disable confirmation."""
    icon    = ":white_check_mark:" if action == "enable" else ":no_entry_sign:"
    sup_id  = _supplier_id(publisher) or "?"
    pub_name = publisher.get("name", "?")
    verb    = "Enabled" if action == "enable" else "Disabled"

    text = (
        f"{icon} *Pilot Action Applied*\n"
        f"Publisher: {pub_name} (supplier {sup_id})\n"
        f"Demand: {demand_name}\n"
        f"Change: Seat {verb}\n"
        f"Reason: {reason}\n"
        f"Time: {_now_et_str()}\n"
        f"Mode: {mode}"
    )

    return [{"type": "section", "text": {"type": "mrkdwn", "text": text}}]


# ---------------------------------------------------------------------------
# Public functions
# ---------------------------------------------------------------------------

def apply_floor_change(
    publisher_name: str,
    demand_name: str,
    new_floor: float,
    reason: str,
    dry_run: bool = True,
) -> dict:
    """
    Change the floor for a specific demand partner on a pilot publisher.

    Floor changes on oRTB publishers like PubNative/AppStock are applied by
    updating the publisher's biddingpreferences — specifically the typeField
    where type=3 (bid floor) within the demand's entry.  If typeField type=3
    doesn't exist or has no value set, we add/update it.

    Safety checks
    -------------
    - Publisher must have supplier_id in PILOT_SUPPLIER_IDS
    - new_floor must be >= MIN_FLOOR
    - Change must be <= MAX_FLOOR_CHANGE_PCT from current value

    Returns dict with: publisher_id, publisher_name, demand_id, demand_name,
    old_floor, new_floor, applied (bool), reason, timestamp
    """
    timestamp = _now_iso()

    # ------------------------------------------------------------------
    # Resolve publisher
    # ------------------------------------------------------------------
    publisher = ll_mgmt.get_publisher_by_name(publisher_name)
    if publisher is None:
        raise ValueError(f"Publisher not found: {publisher_name!r}")

    pub_id      = publisher["id"]
    pub_display = publisher.get("name", publisher_name)
    sup_id      = _supplier_id(publisher)

    if sup_id not in PILOT_SUPPLIER_IDS:
        raise PermissionError(
            f"Publisher '{pub_display}' has supplier_id={sup_id} which is NOT in "
            f"PILOT_SUPPLIER_IDS={PILOT_SUPPLIER_IDS}.  Action blocked."
        )

    # ------------------------------------------------------------------
    # Resolve demand
    # ------------------------------------------------------------------
    demand = ll_mgmt.get_demand_by_name(demand_name)
    if demand is None:
        raise ValueError(f"Demand not found: {demand_name!r}")

    demand_id      = demand["id"]
    demand_display = demand.get("name", demand_name)

    # ------------------------------------------------------------------
    # Fetch full publisher object (needed for PUT later)
    # ------------------------------------------------------------------
    full_publisher = ll_mgmt.get_publisher(pub_id)

    # ------------------------------------------------------------------
    # Determine old floor
    # ------------------------------------------------------------------
    old_floor_raw = _get_current_floor(full_publisher, demand_id)
    old_floor     = old_floor_raw if old_floor_raw is not None else 0.0

    # ------------------------------------------------------------------
    # Safety checks
    # ------------------------------------------------------------------
    if new_floor < MIN_FLOOR:
        raise ValueError(
            f"new_floor=${new_floor:.4f} is below MIN_FLOOR=${MIN_FLOOR:.2f}"
        )

    change_pct = _floor_change_pct(old_floor, new_floor)
    if old_floor > 0.0 and change_pct > MAX_FLOOR_CHANGE_PCT:
        raise ValueError(
            f"Floor change of {change_pct:.1f}% exceeds MAX_FLOOR_CHANGE_PCT="
            f"{MAX_FLOOR_CHANGE_PCT}%.  "
            f"Current floor: ${old_floor:.4f}, requested: ${new_floor:.4f}"
        )

    # ------------------------------------------------------------------
    # Build result skeleton
    # ------------------------------------------------------------------
    result = {
        "action":         "floor_change",
        "publisher_id":   pub_id,
        "publisher_name": pub_display,
        "demand_id":      demand_id,
        "demand_name":    demand_display,
        "old_floor":      old_floor,
        "new_floor":      new_floor,
        "applied":        False,
        "dry_run":        dry_run,
        "reason":         reason,
        "timestamp":      timestamp,
    }

    # ------------------------------------------------------------------
    # Dry-run path
    # ------------------------------------------------------------------
    if dry_run:
        print(
            f"[pilot_actions] DRY_RUN apply_floor_change  "
            f"publisher={pub_display!r}  demand={demand_display!r}  "
            f"floor={old_floor}→{new_floor}  change={change_pct:.1f}%"
        )
        return result

    # ------------------------------------------------------------------
    # Live path — mutate publisher in-memory, PUT, Slack, log
    # ------------------------------------------------------------------
    found = _set_floor_in_publisher(full_publisher, demand_id, new_floor)
    if not found:
        raise ValueError(
            f"demand_id={demand_id} ({demand_display!r}) not found in "
            f"biddingpreferences for publisher_id={pub_id} ({pub_display!r})"
        )

    ll_mgmt._put(f"/v1/publishers/{pub_id}", full_publisher)

    print(
        f"[pilot_actions] apply_floor_change  "
        f"publisher={pub_display!r}  demand={demand_display!r}  "
        f"floor={old_floor}→{new_floor}  change={change_pct:.1f}%"
    )

    result["applied"] = True

    # Slack confirmation
    blocks = _floor_change_blocks(
        publisher=full_publisher,
        demand_name=demand_display,
        old_floor=old_floor,
        new_floor=new_floor,
        reason=reason,
        mode="LIVE",
    )
    slack.send_blocks(blocks, text=f"Pilot floor change: {pub_display} / {demand_display}")

    # Persist to log
    log_action(result)

    return result


def remove_floor_change(
    publisher_name: str,
    demand_name: str,
    reason: str,
    dry_run: bool = True,
) -> dict:
    """
    Remove (unset) the floor for a specific demand partner on a pilot publisher.

    Sets typeField type=3 setOnRule=False so the floor is no longer active.
    Used by the watchdog to auto-revert a floor change.

    Safety: publisher must be in PILOT_SUPPLIER_IDS.
    """
    timestamp = _now_iso()

    publisher = ll_mgmt.get_publisher_by_name(publisher_name)
    if publisher is None:
        raise ValueError(f"Publisher not found: {publisher_name!r}")

    pub_id      = publisher["id"]
    pub_display = publisher.get("name", publisher_name)
    sup_id      = _supplier_id(publisher)

    if sup_id not in PILOT_SUPPLIER_IDS:
        raise PermissionError(
            f"Publisher '{pub_display}' supplier_id={sup_id} not in PILOT_SUPPLIER_IDS."
        )

    demand = ll_mgmt.get_demand_by_name(demand_name)
    if demand is None:
        raise ValueError(f"Demand not found: {demand_name!r}")

    demand_id      = demand["id"]
    demand_display = demand.get("name", demand_name)

    full_publisher = ll_mgmt.get_publisher(pub_id)
    old_floor_raw  = _get_current_floor(full_publisher, demand_id)
    old_floor      = old_floor_raw if old_floor_raw is not None else 0.0

    result = {
        "action":         "floor_remove",
        "publisher_id":   pub_id,
        "publisher_name": pub_display,
        "demand_id":      demand_id,
        "demand_name":    demand_display,
        "old_floor":      old_floor,
        "new_floor":      None,
        "applied":        False,
        "dry_run":        dry_run,
        "reason":         reason,
        "timestamp":      timestamp,
    }

    if dry_run:
        print(
            f"[pilot_actions] DRY_RUN remove_floor_change  "
            f"publisher={pub_display!r}  demand={demand_display!r}  "
            f"floor={old_floor}→unset"
        )
        return result

    # Live: set setOnRule=False on typeField type=3
    for pref in full_publisher.get("biddingpreferences", []):
        for item in pref.get("value", []):
            if item.get("id") != demand_id:
                continue
            for tf in item.get("typeFields", []):
                if tf.get("type") == 3:
                    tf["setOnRule"] = False
                    tf["value"]     = 0.0

    ll_mgmt._put(f"/v1/publishers/{pub_id}", full_publisher)

    print(
        f"[pilot_actions] remove_floor_change  "
        f"publisher={pub_display!r}  demand={demand_display!r}  "
        f"floor={old_floor}→unset"
    )

    result["applied"] = True

    blocks = [{"type": "section", "text": {"type": "mrkdwn", "text": (
        f":rewind: *Pilot Floor Removed (Auto-Revert)*\n"
        f"Publisher: {pub_display} (supplier {sup_id})\n"
        f"Demand: {demand_display}\n"
        f"Change: Floor ${old_floor:.2f} → unset\n"
        f"Reason: {reason}\n"
        f"Time: {_now_et_str()}"
    )}}]
    slack.send_blocks(blocks, text=f"Pilot floor removed: {pub_display} / {demand_display}")

    log_action(result)
    return result


def disable_demand_seat(
    publisher_name: str,
    demand_name: str,
    reason: str,
    dry_run: bool = True,
) -> dict:
    """
    Disable a demand seat on a pilot publisher.

    Safety: publisher must be in PILOT_SUPPLIER_IDS.

    Returns dict with action details.
    """
    return _toggle_demand_seat(
        publisher_name=publisher_name,
        demand_name=demand_name,
        reason=reason,
        dry_run=dry_run,
        action="disable",
    )


def enable_demand_seat(
    publisher_name: str,
    demand_name: str,
    reason: str,
    dry_run: bool = True,
) -> dict:
    """
    Enable a demand seat on a pilot publisher.

    Safety: publisher must be in PILOT_SUPPLIER_IDS.

    Returns dict with action details.
    """
    return _toggle_demand_seat(
        publisher_name=publisher_name,
        demand_name=demand_name,
        reason=reason,
        dry_run=dry_run,
        action="enable",
    )


def _toggle_demand_seat(
    publisher_name: str,
    demand_name: str,
    reason: str,
    dry_run: bool,
    action: str,  # "enable" | "disable"
) -> dict:
    """Internal implementation shared by enable_demand_seat / disable_demand_seat."""
    timestamp  = _now_iso()
    new_status = 1 if action == "enable" else 2

    # Resolve publisher
    publisher = ll_mgmt.get_publisher_by_name(publisher_name)
    if publisher is None:
        raise ValueError(f"Publisher not found: {publisher_name!r}")

    pub_id      = publisher["id"]
    pub_display = publisher.get("name", publisher_name)
    sup_id      = _supplier_id(publisher)

    if sup_id not in PILOT_SUPPLIER_IDS:
        raise PermissionError(
            f"Publisher '{pub_display}' has supplier_id={sup_id} which is NOT in "
            f"PILOT_SUPPLIER_IDS={PILOT_SUPPLIER_IDS}.  Action blocked."
        )

    # Resolve demand
    demand = ll_mgmt.get_demand_by_name(demand_name)
    if demand is None:
        raise ValueError(f"Demand not found: {demand_name!r}")

    demand_id      = demand["id"]
    demand_display = demand.get("name", demand_name)

    result = {
        "action":         action,
        "publisher_id":   pub_id,
        "publisher_name": pub_display,
        "demand_id":      demand_id,
        "demand_name":    demand_display,
        "new_status":     new_status,
        "applied":        False,
        "dry_run":        dry_run,
        "reason":         reason,
        "timestamp":      timestamp,
    }

    if dry_run:
        print(
            f"[pilot_actions] DRY_RUN {action}_demand_seat  "
            f"publisher={pub_display!r}  demand={demand_display!r}"
        )
        return result

    # Live: delegate to ll_mgmt
    ll_mgmt._set_publisher_demand_status(pub_id, demand_id, new_status, dry_run=False)

    print(
        f"[pilot_actions] {action}_demand_seat  "
        f"publisher={pub_display!r}  demand={demand_display!r}"
    )

    result["applied"] = True

    # Slack confirmation
    full_publisher = ll_mgmt.get_publisher(pub_id)
    blocks = _seat_toggle_blocks(
        publisher=full_publisher,
        demand_name=demand_display,
        action=action,
        reason=reason,
        mode="LIVE",
    )
    slack.send_blocks(
        blocks,
        text=f"Pilot seat {action}: {pub_display} / {demand_display}",
    )

    # Persist to log
    log_action(result)

    return result


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

def log_action(action_dict: dict):
    """
    Append an action to logs/pilot_YYYY-MM.json.

    The log file is a list of daily snapshot entries (written by pilot_snapshot.py).
    We find the entry for today (by 'date' key) and add to its 'actions_applied' list,
    or append a standalone actions-only entry if no snapshot has run yet today.
    """
    os.makedirs(LOG_DIR, exist_ok=True)
    path  = _log_path()
    today = _today_str()

    # Load existing log (list format) or start fresh
    if os.path.exists(path):
        try:
            with open(path, "r") as f:
                data = json.load(f)
        except (json.JSONDecodeError, OSError):
            data = []
    else:
        data = []

    # Normalise: if old dict format exists, migrate to list
    if isinstance(data, dict):
        data = list(data.values())

    # Find today's entry or create one
    day_entry = next((e for e in data if e.get("date") == today), None)
    if day_entry is None:
        day_entry = {"date": today, "actions_applied": []}
        data.append(day_entry)

    day_entry.setdefault("actions_applied", []).append(action_dict)

    with open(path, "w") as f:
        json.dump(data, f, indent=2)

    print(f"[pilot_actions] logged action to {path}")


def get_todays_actions() -> list[dict]:
    """Return all actions logged today."""
    path  = _log_path()
    today = _today_str()

    if not os.path.exists(path):
        return []

    try:
        with open(path, "r") as f:
            data = json.load(f)
    except (json.JSONDecodeError, OSError):
        return []

    if isinstance(data, dict):
        return data.get(today, {}).get("actions_applied", [])

    day_entry = next((e for e in data if e.get("date") == today), None)
    return day_entry.get("actions_applied", []) if day_entry else []


# ---------------------------------------------------------------------------
# __main__ — dry-run demo
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    print("=== pilot_actions.py — dry-run demo ===\n")

    result = apply_floor_change(
        publisher_name="AppStock",
        demand_name="Pubmatic",
        new_floor=0.21,
        reason="test dry run",
        dry_run=True,
    )

    print("\nResult:")
    print(json.dumps(result, indent=2))
