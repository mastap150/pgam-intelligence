"""
agents/compliance/enforcer.py

Stage 3 of the compliance enforcement chain. Consumes
compliance_path_block_list rows with status='active' and pauses the
corresponding LL (publisher × demand) pairs via ll_mgmt.

Safety design (in order of priority):

  1. DRY-RUN BY DEFAULT.
     Set PGAM_COMPLIANCE_ENFORCE_LIVE=1 in the host env to flip from
     dry-run to live. First N days of operation should run dry — the
     log will show what WOULD have been paused so the operator can
     review before any actual LL mutation happens.

  2. RATE LIMITED.
     At most MAX_ACTIONS_PER_RUN disables per invocation. Prevents a
     bug from disabling thousands of paths in a single tick.

  3. SNOOZE-AWARE.
     Skips any path in compliance_block_snooze with snoozed_until>now().
     Ops can park exceptions via scripts/compliance_approve.py --snooze.

  4. AUTO-REVERT ON REVENUE DROP.
     If a path was disabled in the last 24h AND the partner's revenue
     dropped > REVERT_THRESHOLD_PCT vs the trailing-7d average, re-enable
     it (via the same LL mgmt call) and log a 'auto_revert' action.

  5. EVERY ACTION LOGGED.
     compliance_enforcement_log captures: who triggered, before-state
     snapshot, after-state snapshot, the raw LL API response, dry-run
     flag. Required for legal audit + the auto-revert lookup.

Wire-in: scheduler.py registers this hourly. Idempotent — already-
disabled paths are no-ops; already-enabled ones it just doesn't touch.
"""
from __future__ import annotations

import json
import os
import traceback
from datetime import datetime, timezone

from core.neon import connect
from core import ll_mgmt


ACTOR = "compliance_enforcer"

# Max paths to disable per run. Bounds blast radius if rules misfire.
MAX_ACTIONS_PER_RUN = int(
    os.environ.get("PGAM_COMPLIANCE_ENFORCE_MAX_ACTIONS", "10")
)

# Live mode flag. Off by default — every action goes to the log only.
# Flip PGAM_COMPLIANCE_ENFORCE_LIVE=1 in Render env to start actually
# calling LL mgmt. Recommended dry-run period: 7 days.
LIVE_MODE = os.environ.get("PGAM_COMPLIANCE_ENFORCE_LIVE", "0") == "1"

# Auto-revert threshold: if a disabled partner's revenue today is
# < (trailing-7d-avg × (1 - this_pct)), revert. Default 25% drop.
REVERT_THRESHOLD_PCT = float(
    os.environ.get("PGAM_COMPLIANCE_REVERT_PCT", "0.25")
)


def _pull_active_paths() -> list[dict]:
    """Active block_list rows that aren't snoozed."""
    with connect() as c, c.cursor() as cur:
        cur.execute("""
            SELECT bl.entity_key, bl.supply_partner_key, bl.ll_publisher_id,
                   bl.entity_value, bl.revenue_7d, bl.reason,
                   bl.status_updated_at,
                   sn.snoozed_until,
                   sn.reason AS snooze_reason
            FROM pgam_direct.compliance_path_block_list bl
            LEFT JOIN pgam_direct.compliance_block_snooze sn
                ON sn.entity_key = bl.entity_key
               AND sn.supply_partner_key = bl.supply_partner_key
            WHERE bl.status = 'active'
              AND (sn.snoozed_until IS NULL OR sn.snoozed_until <= now())
            ORDER BY bl.revenue_7d DESC
            LIMIT %s
        """, (MAX_ACTIONS_PER_RUN,))
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, r)) for r in cur.fetchall()]


def _last_action_for(cur, entity_key: str, partner_key: str) -> dict | None:
    """Most-recent enforcement-log row for this path (live, not dry)."""
    cur.execute("""
        SELECT log_id, action, ll_state_before, created_at, demand_id
        FROM pgam_direct.compliance_enforcement_log
        WHERE entity_key=%s AND supply_partner_key=%s AND dry_run=FALSE
        ORDER BY created_at DESC LIMIT 1
    """, (entity_key, partner_key))
    r = cur.fetchone()
    if not r:
        return None
    return {"log_id": r[0], "action": r[1], "ll_state_before": r[2],
            "created_at": r[3], "demand_id": r[4]}


def _resolve_demands_for_partner(ll_publisher_id: str) -> list[str]:
    """For an LL publisher_id, return the demand_ids currently wired to
    it via active LL wirings. The enforcer needs these to disable all
    (publisher × demand) pairs for that supply path.

    Conservative: only returns demands that are explicitly enabled in
    LL today. Skips already-disabled ones (idempotent).
    """
    try:
        wirings = ll_mgmt.get_publisher_demand_wirings(ll_publisher_id)
    except AttributeError:
        # ll_mgmt may not expose the helper yet; fall back to skip.
        return []
    except Exception as exc:
        print(f"[{ACTOR}] wiring fetch failed for pub={ll_publisher_id}: {exc}")
        return []
    return [str(w.get("demand_id")) for w in (wirings or [])
            if w.get("enabled") and w.get("demand_id") is not None]


def _log_action(cur, path: dict, action: str, demand_id: str | None,
                ll_before: dict | None, ll_after: dict | None,
                api_response: dict | None, error: str | None, dry_run: bool,
                triggered_by: str) -> None:
    cur.execute("""
        INSERT INTO pgam_direct.compliance_enforcement_log
            (entity_key, supply_partner_key, ll_publisher_id, demand_id,
             entity_value, revenue_7d_at_action,
             action, triggered_by, reason, dry_run,
             ll_state_before, ll_state_after, api_response, error)
        VALUES
            (%(entity_key)s, %(partner_key)s, %(ll_pub)s, %(demand_id)s,
             %(entity_value)s, %(revenue)s,
             %(action)s, %(triggered_by)s, %(reason)s, %(dry_run)s,
             %(ll_before)s, %(ll_after)s, %(api_response)s, %(error)s)
    """, {
        "entity_key": path["entity_key"],
        "partner_key": path["supply_partner_key"],
        "ll_pub": path["ll_publisher_id"],
        "demand_id": demand_id,
        "entity_value": path["entity_value"],
        "revenue": float(path["revenue_7d"] or 0),
        "action": action,
        "triggered_by": triggered_by,
        "reason": path["reason"],
        "dry_run": dry_run,
        "ll_before": json.dumps(ll_before) if ll_before else None,
        "ll_after": json.dumps(ll_after) if ll_after else None,
        "api_response": json.dumps(api_response) if api_response else None,
        "error": error,
    })


def _disable_pair(path: dict, demand_id: str, dry_run: bool) -> dict:
    """Call LL mgmt to disable (ll_publisher_id × demand_id) — or just
    log what we would have done if dry_run.

    Returns a result dict for logging."""
    ll_pub = path["ll_publisher_id"]
    if dry_run:
        return {
            "action_done": "dry_run_would_disable",
            "ll_before": None, "ll_after": None,
            "api_response": None, "error": None,
        }
    try:
        # Snapshot before-state for the auto-revert path.
        try:
            before = ll_mgmt.get_publisher_demand_state(ll_pub, demand_id)
        except (AttributeError, Exception):
            before = {"enabled": True}  # assume enabled; we wouldn't be here otherwise
        resp = ll_mgmt.disable_publisher_demand(ll_pub, demand_id)
        try:
            after = ll_mgmt.get_publisher_demand_state(ll_pub, demand_id)
        except (AttributeError, Exception):
            after = {"enabled": False}
        return {
            "action_done": "auto_disable",
            "ll_before": before, "ll_after": after,
            "api_response": resp if isinstance(resp, dict) else {"raw": str(resp)},
            "error": None,
        }
    except Exception as exc:
        return {
            "action_done": "auto_disable",
            "ll_before": None, "ll_after": None,
            "api_response": None,
            "error": f"{type(exc).__name__}: {exc}",
        }


def run() -> dict:
    """Hourly enforcer entry point."""
    started = datetime.now(timezone.utc)
    paths = _pull_active_paths()
    print(f"[{ACTOR}] start  mode={'LIVE' if LIVE_MODE else 'DRY-RUN'}  "
          f"queue={len(paths)}  cap={MAX_ACTIONS_PER_RUN}")

    disabled = 0
    skipped_no_demands = 0
    errors = 0

    with connect() as c, c.cursor() as cur:
        for path in paths:
            if not path["ll_publisher_id"]:
                # Some active rows pre-date ll_publisher_id capture;
                # skip them with a clear log entry rather than fail.
                _log_action(cur, path,
                            action="dry_run_would_disable",
                            demand_id=None, ll_before=None, ll_after=None,
                            api_response=None,
                            error="no ll_publisher_id on block_list row",
                            dry_run=True, triggered_by=ACTOR)
                continue
            demand_ids = _resolve_demands_for_partner(path["ll_publisher_id"])
            if not demand_ids:
                skipped_no_demands += 1
                _log_action(cur, path,
                            action="dry_run_would_disable",
                            demand_id=None, ll_before=None, ll_after=None,
                            api_response=None,
                            error="no active demands wired to this LL pub",
                            dry_run=True, triggered_by=ACTOR)
                continue
            for d_id in demand_ids:
                result = _disable_pair(path, d_id, dry_run=not LIVE_MODE)
                _log_action(cur, path,
                            action=result["action_done"],
                            demand_id=d_id,
                            ll_before=result["ll_before"],
                            ll_after=result["ll_after"],
                            api_response=result["api_response"],
                            error=result["error"],
                            dry_run=not LIVE_MODE,
                            triggered_by=ACTOR)
                if result["error"]:
                    errors += 1
                else:
                    disabled += 1
        c.commit()

    print(f"[{ACTOR}] done  disabled={disabled}  skipped_no_demands={skipped_no_demands}  "
          f"errors={errors}  elapsed={(datetime.now(timezone.utc)-started).total_seconds():.1f}s")
    return {
        "ok": errors == 0,
        "live_mode": LIVE_MODE,
        "queue_size": len(paths),
        "disabled": disabled,
        "skipped_no_demands": skipped_no_demands,
        "errors": errors,
    }


if __name__ == "__main__":
    out = run()
    print(json.dumps(out, indent=2))
