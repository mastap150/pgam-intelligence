"""
agents/compliance/reactivation_monitor.py

Watches blocked / paused supply and surfaces inventory that has fixed
itself. Bridges the gap between "auditor noticed the path is healthy
again" and "operator gets a Slack signal + a one-command path to
re-enable the LL demand".

What the daily auditor already does:
  • Crawls publisher ads.txt + app-ads.txt every morning
  • For each row in compliance_path_block_list with status='active',
    re-evaluates the same compliance rules used at block time
  • Flips status='active' → 'released' when audit shows healthy
  • Records flagged_count so we know how many days a path has been
    out of compliance

What this module adds:
  1. Recomputes recommended_action for every block_list row each tick:
       reactivate / monitor / keep_blocked / whitelist_aging /
       fixed_pre_review
  2. Captures current_compliance_state — JSONB snapshot of what's
     present vs missing on the publisher RIGHT NOW (publisher might
     have *partially* fixed: e.g. added Smaato line but still missing
     PGAM seat). Operator can see the partial-fix state in the digest
     instead of having to dig manually.
  3. Writes audit-trail rows to compliance_enforcement_log so the
     reactivation history is queryable alongside the block history.
  4. Surfaces reactivation candidates in a dedicated Slack digest
     section (rendered by reporters/slack_digest.py).

Wire-in: scheduler.py runs this hourly at :37, AFTER the daily audit
has had a chance to refresh block_list state, and AFTER the enforcer
at :47 — so any state change here lands before tomorrow's runs.

Idempotent. Every run re-evaluates every row from scratch; the
last_recheck_at column gives ops "when did we last verify this".
"""
from __future__ import annotations

import json
from datetime import datetime, timezone

from core.neon import connect


ACTOR = "reactivation_monitor"

# How long a path must have been "released" before recommended_action
# graduates from 'monitor' to 'reactivate'. Bumps the bar for the
# operator: don't re-enable a path that just-now flipped healthy if it
# might revert by tomorrow's audit.
MONITOR_WINDOW_HOURS = 24

# After this many days of being status='pending_review' or 'active'
# without a fix, recommend whitelisting (publisher unlikely to ever
# carry our line; better to acknowledge + move on).
WHITELIST_AGING_DAYS = 30


_PULL_BLOCK_LIST_SQL = """
SELECT bl.entity_key, bl.kind, bl.entity_value, bl.audit_host,
       bl.supply_partner_key, bl.ll_publisher_id, bl.supply_partner_pgam_seat,
       bl.reason, bl.status, bl.status_updated_at, bl.first_flagged_at,
       bl.flagged_count, bl.revenue_7d, bl.last_recheck_at,
       sp.supply_partner_line_present,
       sp.pgam_line_present_for_path,
       sp.sellers_json_partner_declared,
       sp.observed_pgam_seats,
       sp.observed_partner_seats,
       sp.audited_at AS sp_audited_at
FROM pgam_direct.compliance_path_block_list bl
LEFT JOIN pgam_direct.compliance_entity_supply_path_audit sp
       ON sp.entity_key = bl.entity_key
      AND sp.as_of = (SELECT MAX(as_of)
                      FROM pgam_direct.compliance_entity_supply_path_audit
                      WHERE entity_key = bl.entity_key)
WHERE bl.status IN ('pending_review', 'active', 'released')
ORDER BY bl.revenue_7d DESC;
"""

_UPDATE_ROW_SQL = """
UPDATE pgam_direct.compliance_path_block_list
SET last_recheck_at         = now(),
    recommended_action      = %(action)s,
    current_compliance_state = %(state)s
WHERE entity_key = %(entity_key)s
  AND supply_partner_key = %(partner_key)s;
"""

_LOG_RELEASE_SQL = """
INSERT INTO pgam_direct.compliance_enforcement_log
    (entity_key, supply_partner_key, ll_publisher_id, entity_value,
     revenue_7d_at_action, action, triggered_by, reason, dry_run,
     ll_state_before, ll_state_after)
VALUES
    (%(entity_key)s, %(partner_key)s, %(ll_pub)s, %(entity_value)s,
     %(revenue)s, %(action)s, %(triggered_by)s, %(reason)s, FALSE,
     %(state_before)s, %(state_after)s);
"""


def _classify(row: dict) -> tuple[str, dict]:
    """Decide recommended_action + build a compliance-state snapshot.

    Returns ('reactivate' | 'monitor' | 'keep_blocked' |
             'whitelist_aging' | 'fixed_pre_review', state_dict)
    """
    # Path-aware health: every layer must be present for the path
    # to be considered healthy. A partial fix doesn't reactivate.
    layer_a = bool(row.get("supply_partner_line_present"))     # partner's line
    layer_b = bool(row.get("pgam_line_present_for_path"))      # our pgamssp seat
    layer_c = bool(row.get("sellers_json_partner_declared"))   # partner sellers.json
    sp_audited = row.get("sp_audited_at")
    healthy = layer_a and layer_b and layer_c

    state = {
        "layer_a_partner_line":        layer_a,
        "layer_b_pgam_seat_for_path":  layer_b,
        "layer_c_partner_sellers_json": layer_c,
        "healthy":                     healthy,
        "expected_pgam_seat":          row.get("supply_partner_pgam_seat"),
        "observed_pgam_seats":         row.get("observed_pgam_seats") or [],
        "observed_partner_seats":      row.get("observed_partner_seats") or [],
        "sp_audited_at":               sp_audited.isoformat() if sp_audited else None,
    }

    status = row.get("status")
    if healthy:
        # Path is fixed. Recommendation depends on WHY it was queued:
        if status == "pending_review":
            # Auditor flagged it but operator never approved enforcement.
            # No LL mutation ever happened — safe to close as released.
            return "fixed_pre_review", state
        if status == "released":
            # Auditor already flipped active → released. Check how long
            # it's been stable.
            updated = row.get("status_updated_at")
            if updated is None:
                return "monitor", state
            if updated.tzinfo is None:
                updated = updated.replace(tzinfo=timezone.utc)
            hrs = (datetime.now(timezone.utc) - updated).total_seconds() / 3600.0
            return ("reactivate" if hrs >= MONITOR_WINDOW_HOURS
                    else "monitor"), state
        if status == "active":
            # Audit says healthy but block_list still active. Auditor
            # will flip to released on its next pass — recommend monitor
            # for now (it'll graduate to reactivate after WINDOW).
            return "monitor", state
        return "monitor", state

    # Not healthy. Keep blocked, but check for aging.
    first = row.get("first_flagged_at")
    if first is None:
        return "keep_blocked", state
    if first.tzinfo is None:
        first = first.replace(tzinfo=timezone.utc)
    age_days = (datetime.now(timezone.utc) - first).total_seconds() / 86400.0
    if age_days >= WHITELIST_AGING_DAYS:
        return "whitelist_aging", state
    return "keep_blocked", state


def run() -> dict:
    """Hourly entrypoint. Returns a summary dict."""
    started = datetime.now(timezone.utc)
    print(f"[{ACTOR}] start  monitor_window_h={MONITOR_WINDOW_HOURS}  "
          f"whitelist_aging_d={WHITELIST_AGING_DAYS}")

    counts = {"reactivate": 0, "monitor": 0, "keep_blocked": 0,
              "whitelist_aging": 0, "fixed_pre_review": 0,
              "rows_examined": 0}
    new_reactivation_candidates = []

    with connect() as c, c.cursor() as cur:
        cur.execute(_PULL_BLOCK_LIST_SQL)
        cols = [d[0] for d in cur.description]
        rows = [dict(zip(cols, r)) for r in cur.fetchall()]
        for row in rows:
            counts["rows_examined"] += 1
            action, state = _classify(row)
            counts[action] = counts.get(action, 0) + 1

            cur.execute(_UPDATE_ROW_SQL, {
                "entity_key": row["entity_key"],
                "partner_key": row["supply_partner_key"],
                "action": action,
                "state":  json.dumps(state, default=str),
            })

            # When a row first becomes 'reactivate' (i.e. previous
            # recommended_action wasn't 'reactivate'), log it to the
            # enforcement audit trail + queue for Slack.
            if action == "reactivate":
                # Log only if this is a state change. We detect change
                # by checking if the row currently has reactivate set
                # AND we just wrote it (cheaper: log every reactivate
                # ONCE per status_updated_at — the enforcement_log's
                # unique-action-per-window dedup handles repeats).
                cur.execute(_LOG_RELEASE_SQL, {
                    "entity_key": row["entity_key"],
                    "partner_key": row["supply_partner_key"],
                    "ll_pub":      row["ll_publisher_id"],
                    "entity_value": row["entity_value"],
                    "revenue":     float(row["revenue_7d"] or 0),
                    "action":      "auto_release",
                    "triggered_by": ACTOR,
                    "reason":      ("path now healthy on publisher's ads.txt; "
                                    "eligible for reactivation"),
                    "state_before": json.dumps(
                        {"recommended_action": "blocked", "status": row["status"]}),
                    "state_after":  json.dumps(
                        {"recommended_action": "reactivate",
                         "status": row["status"], "audit_state": state}),
                })
                new_reactivation_candidates.append(row)
            elif action == "fixed_pre_review":
                # Pending-review row that fixed itself before we ever
                # enforced. Flip to released so it doesn't keep showing
                # up in the daily review queue.
                cur.execute("""
                    UPDATE pgam_direct.compliance_path_block_list
                    SET status='released', status_updated_at=now(),
                        status_updated_by=%s
                    WHERE entity_key=%s AND supply_partner_key=%s
                      AND status='pending_review'
                """, (ACTOR, row["entity_key"], row["supply_partner_key"]))
        c.commit()

    elapsed = (datetime.now(timezone.utc) - started).total_seconds()
    print(f"[{ACTOR}] done  examined={counts['rows_examined']}  "
          f"reactivate={counts['reactivate']}  monitor={counts['monitor']}  "
          f"keep_blocked={counts['keep_blocked']}  "
          f"whitelist_aging={counts['whitelist_aging']}  "
          f"fixed_pre_review={counts['fixed_pre_review']}  "
          f"elapsed={elapsed:.1f}s")
    return {"ok": True, **counts,
            "new_reactivation_candidates": len(new_reactivation_candidates)}


if __name__ == "__main__":
    print(json.dumps(run(), indent=2, default=str))
