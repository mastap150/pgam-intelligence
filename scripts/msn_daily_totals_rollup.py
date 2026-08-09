"""
scripts/msn_daily_totals_rollup.py

Populate pgam_direct.msn_daily_totals from pgam_direct.msn_article_snapshots.

Why this exists: MSN Partner Hub exposes a real "daily aggregate"
endpoint but we've never been able to discover its URL under our auth
(see agents/etl/msn_endpoint_sniffer.py — that hunt is on hold). The
`msn_daily_totals` table has been empty as a result — every consumer
that expects impressions / CTR / saves / dislikes gets nothing.

Realtime snapshots give us READ counts per doc per 15-min tick, which
is enough to reconstruct daily read totals synthetically. We LOSE the
impression / save / dislike / CTR fields — those columns get 0 with
this rollup. That's not full parity with MSN's aggregate feed but it's
enough to unblock:

  - daily read totals per partner + per day
  - week-over-week trend analysis (already done via weekly_review, but
    now available at daily granularity)
  - reconciliation vs monthly earning report (which lands ~1 week late)

Method:
  1. For each (partner_id, doc_id, day) tuple in snapshots, take MAX
     read_count. That represents that doc's peak reads on that day —
     equivalent to how weekly_review.reads_total counts.
  2. SUM across docs per (partner_id, day) → row's read_count.
  3. UPSERT into msn_daily_totals with content_type=1 (article).

Idempotent: re-running for the same day overwrites the row (peak-reads
is deterministic given the snapshot set). Safe to run daily or in
backfill batches.

Usage:
    python3 scripts/msn_daily_totals_rollup.py                # yesterday
    python3 scripts/msn_daily_totals_rollup.py --days 30      # last 30d
    python3 scripts/msn_daily_totals_rollup.py --from 2026-07-01 --to 2026-08-09
    python3 scripts/msn_daily_totals_rollup.py --dry-run
"""

from __future__ import annotations

import argparse
import os
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

# Support both `python -m scripts.msn_daily_totals_rollup` (CI-friendly)
# and `python3 scripts/msn_daily_totals_rollup.py` (dev-friendly). Same
# pattern as other one-shot scripts in this repo.
_REPO_ROOT = Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from core.neon import connect  # noqa: E402

PARTNER_ID_DEFAULT = 'AA1lKiff'  # BoxingNews

# Rollup SQL — bucket each doc to the day it FIRST appeared in the
# realtime feed. A single article's snapshots span its full 24h window,
# so a naive "snapshot on this day → this day's count" formula double-
# counts every article that straddles midnight UTC. First-seen bucketing
# matches how msn_weekly_review.reads_total is computed (see
# agents/insights/boxingnews_weekly_review.py) so day-sums add up to
# week-sums cleanly for reconciliation.
#
# CRUCIAL: MIN(snapshot_at) must run over the FULL snapshot history for
# a doc — bounding it to a window around the target day would wrongly
# claim an old article (first seen months ago, still surfacing in the
# realtime feed) is "new today" just because the window happens to
# start there. peak_reads similarly needs the full history to catch
# late-window reads that MSN accrued after midnight UTC.
_ROLLUP_SQL = """
WITH doc_lives AS (
  SELECT partner_id,
         doc_id,
         MIN(snapshot_at)::date AS first_seen_date,
         MAX(read_count)        AS peak_reads
  FROM pgam_direct.msn_article_snapshots
  WHERE partner_id = %(partner_id)s
  GROUP BY partner_id, doc_id
)
SELECT partner_id,
       COALESCE(SUM(peak_reads), 0)::int AS read_count,
       COUNT(*)::int                     AS articles_indexed
FROM doc_lives
WHERE first_seen_date = %(day_start)s::date
GROUP BY partner_id;
"""

_UPSERT_SQL = """
INSERT INTO pgam_direct.msn_daily_totals
  (partner_id, report_date, content_type, impression_count, read_count,
   save_count, favourite_count, forward_count, unique_user_count,
   video_unique_user_count, video_start_count, monetizable_view,
   consumed_seconds, dislike_count, comments_count, ctr_click_count,
   updated_at)
VALUES
  (%(partner_id)s, %(report_date)s, 1,
   0, %(read_count)s,
   0, 0, 0, 0,
   0, 0, 0,
   0, 0, 0, 0,
   now())
ON CONFLICT (partner_id, report_date, content_type)
DO UPDATE SET
  read_count = EXCLUDED.read_count,
  updated_at = now();
"""


def rollup_day(partner_id: str, day: date, dry_run: bool = False) -> dict:
    """Compute + upsert the rollup for one (partner_id, day). Returns
    a small dict for logging: {'day', 'read_count', 'articles'}."""
    day_start = datetime.combine(day, datetime.min.time(), tzinfo=timezone.utc)
    day_end = day_start + timedelta(days=1)
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute(_ROLLUP_SQL, {
                'partner_id': partner_id,
                'day_start':  day_start,
                'day_end':    day_end,
            })
            row = cur.fetchone()
            if not row:
                # No snapshots for the day at all — write a zero row so
                # the "no data" case is distinct from "day not run yet".
                read_count = 0
                articles = 0
            else:
                _partner_id, read_count, articles = row
            if not dry_run:
                cur.execute(_UPSERT_SQL, {
                    'partner_id':  partner_id,
                    'report_date': day,
                    'read_count':  read_count,
                })
        if not dry_run:
            conn.commit()
    return {'day': day.isoformat(), 'read_count': read_count, 'articles': articles}


def _cli() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument('--partner-id', default=PARTNER_ID_DEFAULT)
    ap.add_argument('--days', type=int, default=None,
                    help='Number of days back from today to roll up (default: yesterday only)')
    ap.add_argument('--from', dest='date_from', default=None,
                    help='Start date (YYYY-MM-DD, inclusive). Overrides --days.')
    ap.add_argument('--to', dest='date_to', default=None,
                    help='End date (YYYY-MM-DD, inclusive). Defaults to yesterday.')
    ap.add_argument('--dry-run', action='store_true')
    args = ap.parse_args()

    # Compute the date range. --from/--to takes precedence over --days.
    yesterday = date.today() - timedelta(days=1)
    if args.date_from:
        d0 = date.fromisoformat(args.date_from)
        d1 = date.fromisoformat(args.date_to) if args.date_to else yesterday
    elif args.days:
        d1 = yesterday
        d0 = d1 - timedelta(days=max(0, args.days - 1))
    else:
        d0 = d1 = yesterday

    if d0 > d1:
        print(f'error: from date {d0} is after to date {d1}', file=sys.stderr)
        return 1

    print(f'[rollup] partner={args.partner_id} range={d0}..{d1} dry_run={args.dry_run}')
    total_reads = 0
    day_count = 0
    d = d0
    while d <= d1:
        try:
            result = rollup_day(args.partner_id, d, dry_run=args.dry_run)
            total_reads += result['read_count']
            day_count += 1
            print(f"  {result['day']}: reads={result['read_count']:>7d}  articles={result['articles']:>4d}")
        except Exception as exc:
            print(f'  {d}: FAILED — {exc}', file=sys.stderr)
        d += timedelta(days=1)

    print(f'[rollup] done: {day_count} days, {total_reads:,} total reads')
    return 0


if __name__ == '__main__':
    sys.exit(_cli())
