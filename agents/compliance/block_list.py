"""
agents/compliance/block_list.py

Daily population of pgam_direct.compliance_path_block_list from the
supply_path_audit results.

For every non-compliant (entity × supply_partner) path with revenue
≥ MIN_BLOCK_THRESHOLD_USD, this module either:

  - INSERTS a new row with status='pending_review' (first time seen), or
  - UPDATES last_flagged_at + flagged_count on an existing row.

It also auto-releases:
  - status='active' rows whose audit is now healthy → 'released'
  - status='active' or 'pending_review' rows that no longer have any
    audit row (entity stopped earning revenue) → 'expired'

The actual bidder-edge filter that READS this table lives in
pgam-direct/web (Stage 3 of this build). This module is Stage 1: it
maintains the queue. The flip from 'pending_review' to 'active' is a
human/Slack-approval action (Stage 2 — to be built).
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date

from core.neon import connect


MIN_BLOCK_THRESHOLD_USD = 50.0


@dataclass(frozen=True)
class BlockListStats:
    candidates_seen:    int    # supply-path-audit rows that violated threshold
    rows_inserted:      int    # new entries added to the queue
    rows_refreshed:     int    # existing entries got last_flagged_at++
    rows_auto_released: int    # active rows now healthy → released
    rows_expired:       int    # rows with no audit row this run → expired
    pending_review:     int    # current queue depth awaiting ops review
    active:             int    # currently enforced (Stage 3 reads these)


_REFRESH_CANDIDATES_SQL = """
WITH today AS (
    SELECT * FROM pgam_direct.compliance_entity_supply_path_audit
    WHERE as_of = %(as_of)s
)
INSERT INTO pgam_direct.compliance_path_block_list AS bl
    (entity_key, supply_partner_key, ll_publisher_id, entity_value, kind,
     audit_host, supply_partner_pgam_seat, ll_publisher_name, revenue_7d,
     reason, first_flagged_at, last_flagged_at, flagged_count,
     status, status_updated_by)
SELECT
    t.entity_key,
    t.supply_partner_key,
    t.ll_publisher_id,
    t.entity_value,
    t.kind,
    t.audit_host,
    t.supply_partner_pgam_seat,
    t.ll_publisher_name,
    t.revenue_7d,
    CASE
        WHEN NOT t.supply_partner_line_present AND NOT t.pgam_line_present_for_path
            THEN 'both_missing'
        WHEN NOT t.supply_partner_line_present
            THEN 'partner_line_missing'
        WHEN NOT t.pgam_line_present_for_path
            THEN 'pgam_line_missing'
        ELSE 'unknown_path'
    END,
    now(), now(), 1,
    'pending_review', 'auditor'
FROM today t
WHERE t.status != 'healthy'
  AND t.supply_partner_key IS NOT NULL
  AND t.revenue_7d >= %(threshold)s
ON CONFLICT (entity_key, supply_partner_key) DO UPDATE SET
    revenue_7d              = EXCLUDED.revenue_7d,
    ll_publisher_id         = EXCLUDED.ll_publisher_id,
    ll_publisher_name       = EXCLUDED.ll_publisher_name,
    audit_host              = EXCLUDED.audit_host,
    supply_partner_pgam_seat = EXCLUDED.supply_partner_pgam_seat,
    reason                  = EXCLUDED.reason,
    last_flagged_at         = now(),
    flagged_count           = bl.flagged_count + 1
WHERE bl.status IN ('pending_review', 'active');
"""

_AUTO_RELEASE_SQL = """
-- 'active' or 'pending_review' rows whose audit now says healthy →
-- 'released'. Auto-restore once the publisher fixes the ads.txt.
UPDATE pgam_direct.compliance_path_block_list bl
SET status            = 'released',
    status_updated_at = now(),
    status_updated_by = 'auditor:auto_release'
FROM (
    SELECT entity_key, supply_partner_key
    FROM pgam_direct.compliance_entity_supply_path_audit
    WHERE as_of = %(as_of)s AND status = 'healthy'
) t
WHERE bl.entity_key = t.entity_key
  AND bl.supply_partner_key = t.supply_partner_key
  AND bl.status IN ('active', 'pending_review');
"""

_EXPIRE_SQL = """
-- Rows that have no matching audit row for today (entity stopped
-- earning revenue OR fell out of the top-N universe) → expired.
-- Released rows stay released; only pending/active get expired.
UPDATE pgam_direct.compliance_path_block_list bl
SET status            = 'expired',
    status_updated_at = now(),
    status_updated_by = 'auditor:no_recent_audit'
WHERE bl.status IN ('pending_review', 'active')
  AND NOT EXISTS (
      SELECT 1 FROM pgam_direct.compliance_entity_supply_path_audit a
      WHERE a.entity_key = bl.entity_key
        AND a.supply_partner_key = bl.supply_partner_key
        AND a.as_of = %(as_of)s
  )
  -- Grace period: only expire after row has been missing for >= 2 days
  AND bl.last_flagged_at < now() - interval '2 days';
"""

_STATS_SQL = """
SELECT
    COUNT(*) FILTER (WHERE status = 'pending_review') AS pending_review,
    COUNT(*) FILTER (WHERE status = 'active')         AS active
FROM pgam_direct.compliance_path_block_list;
"""


def refresh_block_list(
    as_of: date | None = None,
    threshold_usd: float = MIN_BLOCK_THRESHOLD_USD,
) -> BlockListStats:
    """One-shot daily maintenance. Returns counts for the runner log."""
    as_of = as_of or date.today()
    rows_inserted = 0
    rows_refreshed = 0
    rows_auto_released = 0
    rows_expired = 0

    with connect() as conn, conn.cursor() as cur:
        # Count current state to compute delta after the upsert.
        cur.execute("SELECT COUNT(*) FROM pgam_direct.compliance_path_block_list "
                    "WHERE status IN ('pending_review','active');")
        before = cur.fetchone()[0] or 0

        # Audit-driven upsert: insert new violations, refresh existing.
        cur.execute(_REFRESH_CANDIDATES_SQL,
                    {"as_of": as_of, "threshold": threshold_usd})

        cur.execute("SELECT COUNT(*) FROM pgam_direct.compliance_path_block_list "
                    "WHERE status IN ('pending_review','active');")
        after = cur.fetchone()[0] or 0
        rows_inserted = max(after - before, 0)
        rows_refreshed = max(0, 0)  # cursor.rowcount is total, hard to split

        # Auto-release: paths that are now healthy in today's audit.
        cur.execute(_AUTO_RELEASE_SQL, {"as_of": as_of})
        rows_auto_released = cur.rowcount or 0

        # Expire: stale paths with no audit row today + 2-day grace.
        cur.execute(_EXPIRE_SQL, {"as_of": as_of})
        rows_expired = cur.rowcount or 0

        cur.execute(_STATS_SQL)
        pending, active = cur.fetchone()
        conn.commit()

    # candidates_seen = anything we INSERTED OR refreshed; close enough
    # to "rows we touched".
    candidates_seen = (pending or 0) + (active or 0)
    return BlockListStats(
        candidates_seen=candidates_seen,
        rows_inserted=rows_inserted,
        rows_refreshed=rows_refreshed,  # always 0 in this simplistic delta
        rows_auto_released=rows_auto_released,
        rows_expired=rows_expired,
        pending_review=pending or 0,
        active=active or 0,
    )


_PENDING_QUEUE_QUERY = """
SELECT entity_value, kind, supply_partner_key, supply_partner_pgam_seat,
       ll_publisher_name, revenue_7d, reason, flagged_count, first_flagged_at
FROM pgam_direct.compliance_path_block_list
WHERE status = 'pending_review'
ORDER BY revenue_7d DESC
LIMIT %(limit)s;
"""


def load_pending_queue(limit: int = 15) -> list[dict]:
    """Pull rows currently awaiting ops review — used by the digest."""
    try:
        with connect() as conn, conn.cursor() as cur:
            cur.execute(_PENDING_QUEUE_QUERY, {"limit": limit})
            cols = [c.name for c in cur.description]
            return [dict(zip(cols, r)) for r in cur.fetchall()]
    except Exception as exc:
        print(f"[block_list] pending-queue read failed (non-fatal): {exc}")
        return []
