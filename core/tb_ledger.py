"""
core/tb_ledger.py
~~~~~~~~~~~~~~~~~

Unified append-only audit log for every revenue-affecting TB write.

Mirrors the role of `core/floor_ledger.py` for LL. Every agent (and
manual operator) that writes via `tbm.set_floor`, `tbm.edit_inventory`,
or any other state-mutating call MUST also call `tb_ledger.record(...)`
with the pre-state and post-state.

Why
---
1. **Accountability** — when revenue moves, we can attribute cause.
2. **Reversion** — `revenue_guardian` and `tb_contract_floor_sentry`
   need a single source of truth to grade and roll back.
3. **Ghost-write detection** — pairs with `verify=True` in tb_mgmt.
   If `verify_ok=False` after a write, the ledger surfaces it.
4. **Coordination** — agents read the ledger to enforce freeze
   windows (don't re-touch a placement modified within FREEZE_DAYS).

Schema
------
Each entry is a JSON dict appended to `logs/tb_ledger.jsonl`:

  {
    "ts":           "2026-04-26T20:30:00+00:00",
    "actor":        "tb_floor_nudge",        # agent module name
    "action":       "set_floor",             # set_floor | edit_inventory | create_placement | ...
    "entity_type":  "placement",             # placement | inventory | dsp_endpoint
    "entity_id":    1067,
    "reason":       "+10% nudge, eCPM $1.27 vs floor $0.13",
    "before":       {"price": 0.13, "is_optimal_price": true},
    "after":        {"price": 0.14, "is_optimal_price": true},
    "applied":      true,                    # did the write reach the API
    "verify_ok":    true,                    # did read-after-write match
    "dry_run":      false,
    "run_id":       "tb_floor_nudge:2026-04-26T20:30",
  }

The file is JSONL (one entry per line) so concurrent appenders don't
clobber each other. Read with `iter_entries()` not `json.load`.

Public API
----------
- record(...)               — append a new entry
- iter_entries(since=None)  — generator over entries (oldest first)
- recent_changes(...)       — entries within window, optionally filtered
- placements_in_freeze(days)→ set[int] — placement IDs touched in last N days
- last_change(entity_id)    — most recent entry for an entity (or None)
"""

from __future__ import annotations

import json
import os
import sys
from datetime import datetime, timezone, timedelta
from typing import Iterator, Iterable

_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
LEDGER_PATH = os.path.join(_REPO_ROOT, "logs", "tb_ledger.jsonl")
os.makedirs(os.path.dirname(LEDGER_PATH), exist_ok=True)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


# ─── Write ───────────────────────────────────────────────────────────────────

def record(
    actor: str,
    action: str,
    entity_type: str,
    entity_id: int | str,
    reason: str = "",
    before: dict | None = None,
    after: dict | None = None,
    applied: bool = True,
    verify_ok: bool | None = None,
    dry_run: bool = False,
    run_id: str | None = None,
    extra: dict | None = None,
) -> dict:
    """
    Append a single audit entry. Returns the entry dict (also persisted).

    Idempotent on append-failure: returns the entry even if file write fails
    (so callers don't lose the data). File errors print a stderr warning but
    don't raise — the actual TB write already happened, ledger loss is
    tolerable but should be alerted on.
    """
    entry = {
        "ts":          _now_iso(),
        "actor":       actor,
        "action":      action,
        "entity_type": entity_type,
        "entity_id":   entity_id,
        "reason":      reason,
        "before":      before or {},
        "after":       after or {},
        "applied":     applied,
        "verify_ok":   verify_ok,
        "dry_run":     dry_run,
        "run_id":      run_id or f"{actor}:{_now_iso()[:16]}",
    }
    if extra:
        entry["extra"] = extra
    try:
        with open(LEDGER_PATH, "a") as f:
            f.write(json.dumps(entry, default=str) + "\n")
    except Exception as e:
        print(f"[tb_ledger] WARN: failed to persist entry — {e}", file=sys.stderr)
    return entry


# ─── Read ────────────────────────────────────────────────────────────────────

def iter_entries(since: datetime | None = None) -> Iterator[dict]:
    """Yield every ledger entry, optionally filtered to ts >= since."""
    if not os.path.exists(LEDGER_PATH):
        return
    with open(LEDGER_PATH) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                entry = json.loads(line)
            except json.JSONDecodeError:
                continue
            if since is not None:
                try:
                    ts = datetime.fromisoformat(entry["ts"].replace("Z", "+00:00"))
                except Exception:
                    continue
                if ts < since:
                    continue
            yield entry


def recent_changes(
    hours: int | None = None,
    days: int | None = None,
    actor: str | None = None,
    action: str | None = None,
    entity_type: str | None = None,
    entity_id: int | str | None = None,
    applied_only: bool = True,
) -> list[dict]:
    """Return entries within the time window matching all provided filters."""
    if hours is not None:
        since = datetime.now(timezone.utc) - timedelta(hours=hours)
    elif days is not None:
        since = datetime.now(timezone.utc) - timedelta(days=days)
    else:
        since = None

    out = []
    for e in iter_entries(since=since):
        if actor and e.get("actor") != actor:           continue
        if action and e.get("action") != action:        continue
        if entity_type and e.get("entity_type") != entity_type: continue
        if entity_id is not None and e.get("entity_id") != entity_id: continue
        if applied_only and not e.get("applied"):       continue
        if e.get("dry_run"):                            continue
        out.append(e)
    return out


def placements_in_freeze(days: int = 3) -> set[int]:
    """Return placement IDs touched within the last `days` (any actor)."""
    return {
        int(e["entity_id"])
        for e in recent_changes(days=days, entity_type="placement")
        if isinstance(e.get("entity_id"), (int, str)) and str(e["entity_id"]).isdigit()
    }


def last_change(entity_type: str, entity_id: int | str) -> dict | None:
    """Most recent applied entry for a specific entity."""
    last = None
    for e in iter_entries():
        if e.get("entity_type") == entity_type and e.get("entity_id") == entity_id and e.get("applied"):
            last = e
    return last


# ─── Stats / health ──────────────────────────────────────────────────────────

def summary(days: int = 1) -> dict:
    """Quick stats for change_outcome_digest etc."""
    entries = recent_changes(days=days)
    by_actor: dict[str, int] = {}
    by_action: dict[str, int] = {}
    verify_failures = 0
    for e in entries:
        by_actor[e["actor"]] = by_actor.get(e["actor"], 0) + 1
        by_action[e["action"]] = by_action.get(e["action"], 0) + 1
        if e.get("verify_ok") is False:
            verify_failures += 1
    return {
        "window_days":     days,
        "total_changes":   len(entries),
        "by_actor":        by_actor,
        "by_action":       by_action,
        "verify_failures": verify_failures,
    }


if __name__ == "__main__":
    import pprint
    pprint.pprint(summary(days=7))
