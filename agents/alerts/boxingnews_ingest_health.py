"""
agents/alerts/boxingnews_ingest_health.py
──────────────────────────────────────────────────────────────────────────────
Daily Slack alert if either boxingnews ingest lane has stopped producing.

WHY THIS EXISTS
───────────────
The trending + breaking ingest lanes wrote to WordPress for ~30 days
before anyone noticed they were silently failing with ENOTFOUND
admin.boxingnews.com (host decommissioned post-Sanity migration; nobody
updated WP_API_URL on Vercel). The catch block flattened the cause into
a generic "fetch failed" so the lanes looked like they were running
fine — Vercel cron logs showed `200 OK` because the route's outer
try/catch always returned 200.

This agent closes the gap. It runs once a day (08:30 ET) and checks
the boxingnews articles table for the prior 24h:

  - `breaking-news` tagged articles → breaking lane health
  - `trending-now`  tagged articles → trending lane health

The breaking lane runs every 15 min (96 ticks/day). If it produced
zero articles in 24h, something is wrong — publisher RSS has 9 feeds
and at least one usually files in any 24h window. Same for trending
(12 ticks/day across the same feed set).

Self-dedups so a multi-day outage doesn't spam Slack — uses the same
`already_sent_today` mechanism as the other alert agents.

THRESHOLDS
──────────
Set deliberately conservative — false-positive once a year is fine,
silent 30-day outage is not:
  - breaking: alert if 0 articles in last 24h
  - trending: alert if 0 articles in last 24h
"""

from __future__ import annotations

import os

from core.boxingnews_db import connect as connect_boxingnews
from core.slack import send_text, already_sent_today, mark_sent

ALERT_KEY = "boxingnews_ingest_health"


def run() -> None:
    """Daily check. Slacks if either lane shipped 0 articles in the
    prior 24h."""
    if already_sent_today(ALERT_KEY):
        print("[ingest_health] already sent today, skipping")
        return

    counts = _pull_24h_lane_counts()
    print(f"[ingest_health] last 24h: breaking={counts['breaking']}  trending={counts['trending']}  other={counts['other']}")

    failures: list[str] = []
    if counts["breaking"] == 0:
        failures.append(
            "*breaking lane (every 15 min)* — 0 articles in last 24h. "
            "Likely: Vercel cron paused, RSS feeds all unreachable, or "
            "publish step throwing. Check /api/ingest-breaking?key=$SYNC_SECRET&dry=1."
        )
    if counts["trending"] == 0:
        failures.append(
            "*trending lane (every 2h)* — 0 articles in last 24h. "
            "Likely: Vercel cron paused, Anthropic key issue, or DB insert "
            "blocked. Check /api/ingest-trending?key=$SYNC_SECRET&dry=1."
        )

    if not failures:
        # Healthy — no alert. Don't dedup the success path so a
        # newly-broken lane in the future fires immediately on the
        # next daily run.
        print("[ingest_health] healthy — no alert sent")
        return

    msg = (
        ":rotating_light: *BoxingNews ingest health alert*\n\n"
        + "\n\n".join(failures)
        + f"\n\nLast 24h totals — breaking: {counts['breaking']}, "
        + f"trending: {counts['trending']}, other pipelines (Sanity/extract/programmatic): {counts['other']}."
    )

    try:
        send_text(msg)
        mark_sent(ALERT_KEY)
        print("[ingest_health] alert posted to Slack")
    except Exception as exc:
        print(f"[ingest_health] Slack post failed: {exc}")


def _pull_24h_lane_counts() -> dict:
    """Count articles in the last 24h, segmented by ingest lane via the
    provenance tags written by /api/ingest-breaking and
    /api/ingest-trending (PR 2026-06-09)."""
    sql = """
        SELECT
            COUNT(*) FILTER (WHERE 'breaking-news' = ANY(tag_names)) AS breaking,
            COUNT(*) FILTER (WHERE 'trending-now'  = ANY(tag_names)) AS trending,
            COUNT(*) FILTER (WHERE NOT (
                'breaking-news' = ANY(tag_names) OR 'trending-now' = ANY(tag_names)
            ))                                                       AS other
        FROM articles
        WHERE published_at >= NOW() - INTERVAL '24 hours'
    """
    with connect_boxingnews() as conn, conn.cursor() as cur:
        cur.execute(sql)
        row = cur.fetchone()
        return {
            "breaking": int(row[0] or 0),
            "trending": int(row[1] or 0),
            "other":    int(row[2] or 0),
        }


if __name__ == "__main__":
    run()
