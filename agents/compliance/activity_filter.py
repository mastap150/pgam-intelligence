"""
agents/compliance/activity_filter.py

Mark compliance_publishers as is_active_recent based on LL revenue
activity in the trailing window. Without this gate, Phase 1's universal-
DIRECT check fires findings on every sellers.json entry — including
stale partners that never went live or rolled off long ago. Result:
critical alerts get drowned by hygiene noise.

This gate restricts Phase 1 to "partners live on LL right now showing
activity" — exactly the operational scope.

Algorithm:
  1. RESET: every row in compliance_publishers → is_active_recent=false,
     revenue_recent_7d=0, impressions_recent_7d=0, activity_checked_at=now()
  2. REFRESH: for each row with ll_publisher_id set (Phase 2 bridge),
     LEFT-aggregate ll_daily_partner_revenue trailing N days and update
     is_active_recent = (revenue > 0).

Unbridged entries stay inactive — there's no way to determine activity
for them without an LL publisher_id. The bridge step (Phase 2) is
responsible for closing that gap.

Lookback defaults to 7 days. Override via env if needed.
"""
from __future__ import annotations

from dataclasses import dataclass

from core.neon import connect


DEFAULT_LOOKBACK_DAYS = 7


_RESET_SQL = """
UPDATE pgam_direct.compliance_publishers
SET is_active_recent      = FALSE,
    revenue_recent_7d     = 0,
    impressions_recent_7d = 0,
    activity_checked_at   = now()
WHERE is_active = TRUE;
"""

_REFRESH_SQL = """
UPDATE pgam_direct.compliance_publishers cp
SET revenue_recent_7d     = agg.revenue,
    impressions_recent_7d = agg.impressions,
    is_active_recent      = (agg.revenue > 0),
    activity_checked_at   = now()
FROM (
    SELECT publisher_id,
           COALESCE(SUM(gross_revenue), 0)::numeric AS revenue,
           COALESCE(SUM(impressions), 0)::bigint    AS impressions
    FROM pgam_direct.ll_daily_partner_revenue
    WHERE report_date >= (current_date - %(lookback)s::int)
    GROUP BY publisher_id
) agg
WHERE cp.ll_publisher_id IS NOT NULL
  AND cp.ll_publisher_id = agg.publisher_id
  AND cp.is_active = TRUE;
"""

_STATS_SQL = """
SELECT
    COUNT(*)                              AS total,
    COUNT(*) FILTER (WHERE is_active_recent)        AS active,
    COUNT(*) FILTER (WHERE ll_publisher_id IS NULL) AS unbridged,
    COALESCE(SUM(revenue_recent_7d), 0)::numeric AS revenue_7d_total
FROM pgam_direct.compliance_publishers
WHERE is_active = TRUE;
"""


@dataclass(frozen=True)
class ActivityStats:
    total: int
    active: int
    inactive: int
    unbridged: int
    total_revenue_7d: float


def refresh_partner_activity(
    lookback_days: int = DEFAULT_LOOKBACK_DAYS,
) -> ActivityStats:
    """Atomically reset + refresh is_active_recent on compliance_publishers."""
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute(_RESET_SQL)
            cur.execute(_REFRESH_SQL, {"lookback": lookback_days})
            cur.execute(_STATS_SQL)
            row = cur.fetchone()
        conn.commit()
    total = int(row[0] or 0)
    active = int(row[1] or 0)
    unbridged = int(row[2] or 0)
    revenue = float(row[3] or 0)
    return ActivityStats(
        total=total,
        active=active,
        inactive=total - active,
        unbridged=unbridged,
        total_revenue_7d=revenue,
    )


def load_active_publisher_keys() -> set[str]:
    """Return the set of compliance_publishers.publisher_key with current activity."""
    sql = """
    SELECT publisher_key
    FROM pgam_direct.compliance_publishers
    WHERE is_active = TRUE AND is_active_recent = TRUE
    """
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            return {r[0] for r in cur.fetchall()}
