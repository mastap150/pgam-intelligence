"""
agents/etl/msn_insights_etl.py

MSN Partner Hub insights puller for BoxingNews (partner AA1lKiff).

Lands two streams into the shared Neon DB (same project the admin
dashboard at admin.pgammedia.com reads from):

1. Per-article snapshots — every 15 min, paginated across the rolling
   24h "realtime" window. One row per (doc_id, snapshot_at) in
   pgam_direct.msn_article_snapshots. We never see MSN's true
   per-article-lifetime totals, so we reconstruct them ourselves by
   taking the MAX(read_count) per doc_id across all snapshots while the
   article was in the 24h window.

2. Daily aggregate totals — per (date, content_type) into
   pgam_direct.msn_daily_totals. content_type=4 confirmed = video;
   content_type=1 strongly suspected = article. The aggregate endpoint
   path isn't confirmed from the user's DevTools trace, so the client
   tries a few candidates and the puller no-ops gracefully if all 404.

Why this exists
---------------
MSN pays BoxingNews $4 CPM on article page views. There's a 50–100×
variance between the worst- and best-performing articles in any 24h
window, and Partner Hub's UI only shows a 24h rolling snapshot — so
without our own time-series we have no way to learn what makes the
10K-view articles different from the 100-view ones. This ETL is the
data foundation for the headline A/B work in Phase 2 of the MSN growth
plan (see memory: boxingnews_msn_syndication.md).

Operating modes
---------------
- Default `run()` does a fresh pull and writes to Neon.
- `run(dry_run=True)` pulls but skips Neon writes — prints a summary
  to stdout. Use this for the first interactive Playwright login.
- `python -m agents.etl.msn_insights_etl --once` runs once (same as
  the scheduler call) and exits.
- `python -m agents.etl.msn_insights_etl --dry-run` runs once without
  Neon writes.

Hosting
-------
Playwright + Chromium is too heavy for Render's free Python tier. The
scheduler.py wires this agent in defensively so it no-ops when
Playwright isn't importable. The canonical deploy target is GitHub
Actions (see .github/workflows/msn-insights.yml) running every 15 min
with the persisted browser profile checked into an Actions cache.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
import traceback
from datetime import datetime, timezone
from typing import Any, Optional

try:
    from core.msn_partner_hub import (
        DEFAULT_PARTNER_ID,
        PartnerHubClient,
        PartnerHubError,
    )
except ImportError as exc:  # pragma: no cover - import-time only
    PartnerHubClient = None  # type: ignore
    PartnerHubError = RuntimeError  # type: ignore
    _IMPORT_ERROR = exc
    DEFAULT_PARTNER_ID = "AA1lKiff"
else:
    _IMPORT_ERROR = None

from core.neon import connect


# ---------------------------------------------------------------------------
# Schema bootstrap — embedded so first run on a fresh DB just works.
# Mirrors migrations/2026_05_16_msn_insights.sql; keep them in sync.
# ---------------------------------------------------------------------------

_SCHEMA_SQL = """
CREATE SCHEMA IF NOT EXISTS pgam_direct;

CREATE TABLE IF NOT EXISTS pgam_direct.msn_article_snapshots (
    id              BIGSERIAL   PRIMARY KEY,
    partner_id      TEXT        NOT NULL,
    doc_id          TEXT        NOT NULL,
    snapshot_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    msn_title       TEXT        NOT NULL,
    title_status    INTEGER,
    read_count      INTEGER     NOT NULL,
    rank_in_window  INTEGER     NOT NULL,
    record_count    INTEGER
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_msn_snapshots_doc_time
    ON pgam_direct.msn_article_snapshots (doc_id, snapshot_at);
CREATE INDEX IF NOT EXISTS idx_msn_snapshots_partner_time
    ON pgam_direct.msn_article_snapshots (partner_id, snapshot_at DESC);
CREATE INDEX IF NOT EXISTS idx_msn_snapshots_time
    ON pgam_direct.msn_article_snapshots (snapshot_at DESC);

CREATE TABLE IF NOT EXISTS pgam_direct.msn_article_meta (
    doc_id            TEXT        PRIMARY KEY,
    partner_id        TEXT        NOT NULL,
    msn_url           TEXT,
    canonical_url     TEXT,
    thumbnail_url     TEXT,
    msn_title_first   TEXT,
    canonical_title   TEXT,
    first_seen_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_resolved_at  TIMESTAMPTZ,
    resolve_attempts  INTEGER     NOT NULL DEFAULT 0,
    resolve_status    TEXT        NOT NULL DEFAULT 'pending',
    resolve_error     TEXT
);

CREATE INDEX IF NOT EXISTS idx_msn_meta_canonical
    ON pgam_direct.msn_article_meta (canonical_url);
CREATE INDEX IF NOT EXISTS idx_msn_meta_status_seen
    ON pgam_direct.msn_article_meta (resolve_status, first_seen_at);

CREATE TABLE IF NOT EXISTS pgam_direct.msn_daily_totals (
    partner_id                TEXT        NOT NULL,
    report_date               DATE        NOT NULL,
    content_type              INTEGER     NOT NULL,
    impression_count          INTEGER     NOT NULL DEFAULT 0,
    read_count                INTEGER     NOT NULL DEFAULT 0,
    save_count                INTEGER     NOT NULL DEFAULT 0,
    favourite_count           INTEGER     NOT NULL DEFAULT 0,
    forward_count             INTEGER     NOT NULL DEFAULT 0,
    unique_user_count         INTEGER     NOT NULL DEFAULT 0,
    video_unique_user_count   INTEGER     NOT NULL DEFAULT 0,
    video_start_count         INTEGER     NOT NULL DEFAULT 0,
    video_viewed_25_count     INTEGER     NOT NULL DEFAULT 0,
    video_viewed_50_count     INTEGER     NOT NULL DEFAULT 0,
    video_viewed_75_count     INTEGER     NOT NULL DEFAULT 0,
    video_viewed_100_count    INTEGER     NOT NULL DEFAULT 0,
    monetizable_view          INTEGER     NOT NULL DEFAULT 0,
    consumed_seconds          INTEGER     NOT NULL DEFAULT 0,
    dislike_count             INTEGER     NOT NULL DEFAULT 0,
    comments_count            INTEGER     NOT NULL DEFAULT 0,
    ctr_click_count           INTEGER     NOT NULL DEFAULT 0,
    updated_at                TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (partner_id, report_date, content_type)
);

CREATE INDEX IF NOT EXISTS idx_msn_daily_date
    ON pgam_direct.msn_daily_totals (report_date DESC);

CREATE TABLE IF NOT EXISTS pgam_direct.msn_pull_runs (
    id                    BIGSERIAL   PRIMARY KEY,
    started_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at           TIMESTAMPTZ,
    partner_id            TEXT        NOT NULL,
    realtime_rows_seen    INTEGER,
    realtime_pages        INTEGER,
    aggregate_rows_seen   INTEGER,
    ok                    BOOLEAN     NOT NULL DEFAULT FALSE,
    error_message         TEXT
);

CREATE INDEX IF NOT EXISTS idx_msn_pull_runs_started
    ON pgam_direct.msn_pull_runs (started_at DESC);

-- Per-15-minute total-PV buckets from the bucketed /realtime endpoint.
-- One row per (partner_id, bucket_at). The current bucket re-emits with
-- a growing count until the 15-min window closes, then it stabilizes —
-- so we UPSERT and let the latest pull win. `consumed_seconds_*` are
-- video-only and not surfaced by this endpoint, hence absent.
CREATE TABLE IF NOT EXISTS pgam_direct.msn_traffic_buckets (
    partner_id    TEXT        NOT NULL,
    bucket_at     TIMESTAMPTZ NOT NULL,
    read_count    INTEGER     NOT NULL,
    last_seen_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (partner_id, bucket_at)
);

CREATE INDEX IF NOT EXISTS idx_msn_traffic_buckets_partner_time
    ON pgam_direct.msn_traffic_buckets (partner_id, bucket_at DESC);

CREATE OR REPLACE VIEW pgam_direct.msn_article_peak AS
SELECT
    s.partner_id,
    s.doc_id,
    MAX(s.msn_title)              AS latest_msn_title,
    MAX(s.read_count)             AS peak_read_count,
    ROUND(MAX(s.read_count) * 0.004::numeric, 2) AS est_revenue_usd,
    MIN(s.snapshot_at)            AS first_seen_at,
    MAX(s.snapshot_at)            AS last_seen_at,
    COUNT(*)                      AS snapshot_count
FROM pgam_direct.msn_article_snapshots s
WHERE s.snapshot_at >= now() - interval '30 days'
GROUP BY s.partner_id, s.doc_id;
"""


def _ensure_schema() -> None:
    """Idempotent — safe to call on every run. Cheap on subsequent runs
    because all statements are IF NOT EXISTS / CREATE OR REPLACE."""
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute(_SCHEMA_SQL)
        conn.commit()


# ---------------------------------------------------------------------------
# Snapshot writer
# ---------------------------------------------------------------------------

_SNAPSHOT_INSERT_SQL = """
INSERT INTO pgam_direct.msn_article_snapshots
  (partner_id, doc_id, snapshot_at, msn_title, title_status,
   read_count, rank_in_window, record_count)
VALUES
  (%(partner_id)s, %(doc_id)s, %(snapshot_at)s, %(msn_title)s, %(title_status)s,
   %(read_count)s, %(rank_in_window)s, %(record_count)s)
ON CONFLICT (doc_id, snapshot_at) DO NOTHING;
"""

_META_UPSERT_SQL = """
INSERT INTO pgam_direct.msn_article_meta
  (doc_id, partner_id, msn_title_first, first_seen_at, resolve_status)
VALUES
  (%(doc_id)s, %(partner_id)s, %(msn_title_first)s, now(), 'pending')
ON CONFLICT (doc_id) DO NOTHING;
"""

_BUCKET_UPSERT_SQL = """
INSERT INTO pgam_direct.msn_traffic_buckets
  (partner_id, bucket_at, read_count, last_seen_at)
VALUES
  (%(partner_id)s, %(bucket_at)s, %(read_count)s, now())
ON CONFLICT (partner_id, bucket_at) DO UPDATE SET
  read_count   = EXCLUDED.read_count,
  last_seen_at = now();
"""


def _write_traffic_buckets(
    *,
    partner_id: str,
    records: list[dict[str, Any]],
) -> int:
    """UPSERT 15-min bucket rows. Returns count written."""
    if not records:
        return 0
    rows: list[dict[str, Any]] = []
    for rec in records:
        bucket = rec.get("date")
        read = rec.get("readCount")
        if not bucket or read is None:
            continue
        rows.append({
            "partner_id":  partner_id,
            "bucket_at":   bucket,
            "read_count":  int(read),
        })
    if not rows:
        return 0
    with connect() as conn:
        with conn.cursor() as cur:
            cur.executemany(_BUCKET_UPSERT_SQL, rows)
        conn.commit()
    return len(rows)


def _write_snapshots(
    *,
    partner_id: str,
    records: list[dict[str, Any]],
    record_count: int,
    snapshot_at: datetime,
) -> int:
    """Insert one row per record into msn_article_snapshots and seed
    msn_article_meta. Conflict-safe (a 15-min cron racing itself is fine).

    Returns the number of snapshot rows actually inserted (post-dedup)."""
    if not records:
        return 0
    snapshot_rows: list[dict[str, Any]] = []
    meta_rows: list[dict[str, Any]] = []
    for rank, rec in enumerate(records, start=1):
        doc_id = (rec.get("docID") or "").strip()
        title = (rec.get("title") or "").strip()
        if not doc_id or not title:
            continue
        snapshot_rows.append({
            "partner_id":     partner_id,
            "doc_id":         doc_id,
            "snapshot_at":    snapshot_at,
            "msn_title":      title,
            "title_status":   rec.get("titleStatus"),
            "read_count":     int(rec.get("readCount") or 0),
            "rank_in_window": rank,
            "record_count":   record_count,
        })
        meta_rows.append({
            "partner_id":      partner_id,
            "doc_id":          doc_id,
            "msn_title_first": title,
        })

    inserted = 0
    with connect() as conn:
        with conn.cursor() as cur:
            for row in snapshot_rows:
                cur.execute(_SNAPSHOT_INSERT_SQL, row)
                inserted += cur.rowcount or 0
            cur.executemany(_META_UPSERT_SQL, meta_rows)
        conn.commit()
    return inserted


# ---------------------------------------------------------------------------
# Daily aggregate writer
# ---------------------------------------------------------------------------

_DAILY_UPSERT_SQL = """
INSERT INTO pgam_direct.msn_daily_totals
  (partner_id, report_date, content_type,
   impression_count, read_count, save_count, favourite_count, forward_count,
   unique_user_count, video_unique_user_count, video_start_count,
   video_viewed_25_count, video_viewed_50_count, video_viewed_75_count,
   video_viewed_100_count, monetizable_view, consumed_seconds,
   dislike_count, comments_count, ctr_click_count, updated_at)
VALUES
  (%(partner_id)s, %(report_date)s, %(content_type)s,
   %(impression_count)s, %(read_count)s, %(save_count)s, %(favourite_count)s, %(forward_count)s,
   %(unique_user_count)s, %(video_unique_user_count)s, %(video_start_count)s,
   %(video_viewed_25_count)s, %(video_viewed_50_count)s, %(video_viewed_75_count)s,
   %(video_viewed_100_count)s, %(monetizable_view)s, %(consumed_seconds)s,
   %(dislike_count)s, %(comments_count)s, %(ctr_click_count)s, now())
ON CONFLICT (partner_id, report_date, content_type) DO UPDATE SET
  impression_count       = EXCLUDED.impression_count,
  read_count             = EXCLUDED.read_count,
  save_count             = EXCLUDED.save_count,
  favourite_count        = EXCLUDED.favourite_count,
  forward_count          = EXCLUDED.forward_count,
  unique_user_count      = EXCLUDED.unique_user_count,
  video_unique_user_count = EXCLUDED.video_unique_user_count,
  video_start_count      = EXCLUDED.video_start_count,
  video_viewed_25_count  = EXCLUDED.video_viewed_25_count,
  video_viewed_50_count  = EXCLUDED.video_viewed_50_count,
  video_viewed_75_count  = EXCLUDED.video_viewed_75_count,
  video_viewed_100_count = EXCLUDED.video_viewed_100_count,
  monetizable_view       = EXCLUDED.monetizable_view,
  consumed_seconds       = EXCLUDED.consumed_seconds,
  dislike_count          = EXCLUDED.dislike_count,
  comments_count         = EXCLUDED.comments_count,
  ctr_click_count        = EXCLUDED.ctr_click_count,
  updated_at             = now();
"""


def _write_daily(
    *,
    partner_id: str,
    rows: list[dict[str, Any]],
) -> int:
    """UPSERT aggregate rows. Returns count written."""
    if not rows:
        return 0
    normalised = [_normalise_daily_row(partner_id, r) for r in rows]
    normalised = [r for r in normalised if r is not None]
    if not normalised:
        return 0
    with connect() as conn:
        with conn.cursor() as cur:
            cur.executemany(_DAILY_UPSERT_SQL, normalised)
        conn.commit()
    return len(normalised)


def _normalise_daily_row(partner_id: str, row: dict[str, Any]) -> Optional[dict[str, Any]]:
    """Map an MSN aggregate row (camelCase) onto our snake_case columns,
    coercing missing fields to 0. Returns None if `date` is missing."""
    report_date = row.get("date")
    content_type = row.get("contentType")
    if not report_date or content_type is None:
        return None
    return {
        "partner_id":               partner_id,
        "report_date":              report_date,
        "content_type":             int(content_type),
        "impression_count":         int(row.get("impressionCount") or 0),
        "read_count":               int(row.get("readCount") or 0),
        "save_count":               int(row.get("saveCount") or 0),
        "favourite_count":          int(row.get("favouriteCount") or 0),
        "forward_count":            int(row.get("forwardCount") or 0),
        "unique_user_count":        int(row.get("uniqueUserCount") or 0),
        "video_unique_user_count":  int(row.get("videoUniqueUserCount") or 0),
        "video_start_count":        int(row.get("videoStartCount") or 0),
        "video_viewed_25_count":    int(row.get("videoViewed25Count") or 0),
        "video_viewed_50_count":    int(row.get("videoViewed50Count") or 0),
        "video_viewed_75_count":    int(row.get("videoViewed75Count") or 0),
        "video_viewed_100_count":   int(row.get("videoViewed100Count") or 0),
        "monetizable_view":         int(row.get("monetizableView") or 0),
        "consumed_seconds":         int(row.get("consumedSeconds") or 0),
        "dislike_count":             int(row.get("dislikeCount") or 0),
        "comments_count":            int(row.get("commentsCount") or 0),
        "ctr_click_count":           int(row.get("ctrClickCount") or 0),
    }


# ---------------------------------------------------------------------------
# Run-log writer
# ---------------------------------------------------------------------------

_RUN_INSERT_SQL = """
INSERT INTO pgam_direct.msn_pull_runs
  (started_at, finished_at, partner_id, realtime_rows_seen, realtime_pages,
   aggregate_rows_seen, ok, error_message)
VALUES
  (%(started_at)s, now(), %(partner_id)s, %(realtime_rows_seen)s,
   %(realtime_pages)s, %(aggregate_rows_seen)s, %(ok)s, %(error_message)s)
RETURNING id;
"""


def _log_run(payload: dict[str, Any]) -> Optional[int]:
    try:
        with connect() as conn:
            with conn.cursor() as cur:
                cur.execute(_RUN_INSERT_SQL, payload)
                row = cur.fetchone()
            conn.commit()
        return int(row[0]) if row else None
    except Exception as exc:
        # Run log is nice-to-have, never block on it.
        print(f"[msn_insights_etl] WARNING: could not log run row: {exc}")
        return None


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def run(
    *,
    partner_id: str = DEFAULT_PARTNER_ID,
    dry_run: bool = False,
    window_hours: int = 24,
    aggregate_window_days: int = 30,
) -> dict[str, Any]:
    """Pull MSN Partner Hub insights once and persist to Neon.

    Returns a status dict the scheduler can log:
        {
            "ok": bool,
            "realtime_rows": int,        # how many docs we saw
            "realtime_inserted": int,    # how many new snapshot rows landed
            "record_count": int,         # total articles in MSN's window
            "aggregate_rows": int,       # how many daily rows we wrote
            "elapsed_seconds": float,
            "error": str | None,
        }
    """
    started_at = datetime.now(tz=timezone.utc)
    t0 = time.perf_counter()

    if _IMPORT_ERROR is not None or PartnerHubClient is None:
        msg = (
            "playwright not importable — skipping MSN pull. "
            "Install with: pip install playwright && playwright install chromium. "
            f"Original error: {_IMPORT_ERROR}"
        )
        print(f"[msn_insights_etl] {msg}")
        return {
            "ok": False,
            "skipped": True,
            "error": msg,
            "elapsed_seconds": 0.0,
        }

    realtime_rows = 0
    realtime_inserted = 0
    record_count = 0
    aggregate_rows = 0
    bucket_rows = 0
    pages = 0
    err: Optional[str] = None

    try:
        if not dry_run:
            _ensure_schema()

        client = PartnerHubClient(partner_id=partner_id)
        with client.session():
            # --- realtime ---
            records, record_count = client.fetch_realtime_all(window_hours=window_hours)
            realtime_rows = len(records)
            pages = (realtime_rows + 19) // 20

            snapshot_at = datetime.now(tz=timezone.utc)
            print(
                f"[msn_insights_etl] realtime: {realtime_rows} unique docs "
                f"(recordCount reports {record_count}) over {pages} page(s)"
            )

            if records and not dry_run:
                realtime_inserted = _write_snapshots(
                    partner_id=partner_id,
                    records=records,
                    record_count=record_count,
                    snapshot_at=snapshot_at,
                )
                print(f"[msn_insights_etl] realtime: {realtime_inserted} new snapshot rows persisted")
            elif dry_run:
                _print_dry_run_summary(records)

            # --- 15-min traffic buckets (Overview tab timeline) ---
            try:
                buckets_payload = client.fetch_realtime_buckets(window_hours=window_hours)
                bucket_records = buckets_payload.get("recordList") or []
                bucket_total = sum(int(b.get("readCount") or 0) for b in bucket_records)
                est_24h = round(bucket_total * 0.004, 2)
                print(
                    f"[msn_insights_etl] buckets: {len(bucket_records)} 15-min slots, "
                    f"sum readCount = {bucket_total} (est 24h revenue ${est_24h})"
                )
                if bucket_records and not dry_run:
                    bucket_rows = _write_traffic_buckets(
                        partner_id=partner_id,
                        records=bucket_records,
                    )
                    print(f"[msn_insights_etl] buckets: {bucket_rows} rows upserted")
            except PartnerHubError as exc:
                # Buckets are nice-to-have; realtime articles is the
                # load-bearing data. Don't fail the run on a bucket miss.
                print(f"[msn_insights_etl] buckets skipped: {exc}")

            # --- aggregate (best-effort) ---
            try:
                payload = client.fetch_aggregate(window_days=aggregate_window_days)
                daily_rows = payload.get("recordList") or []
                print(f"[msn_insights_etl] aggregate: {len(daily_rows)} day(s) returned")
                if daily_rows and not dry_run:
                    aggregate_rows = _write_daily(partner_id=partner_id, rows=daily_rows)
                    print(f"[msn_insights_etl] aggregate: {aggregate_rows} day-rows upserted")
                elif dry_run and daily_rows:
                    print("[msn_insights_etl] (dry-run) example daily row:")
                    print(json.dumps(daily_rows[0], indent=2, default=str)[:400])
            except PartnerHubError as exc:
                # Aggregate endpoint path isn't confirmed yet — don't fail the run.
                print(f"[msn_insights_etl] aggregate skipped: {exc}")

    except Exception as exc:  # noqa: BLE001 -- we want every failure mode logged
        err = f"{type(exc).__name__}: {exc}"
        print(f"[msn_insights_etl] ✗ {err}")
        traceback.print_exc()

    elapsed = time.perf_counter() - t0
    ok = err is None

    if not dry_run:
        _log_run({
            "started_at":          started_at,
            "partner_id":          partner_id,
            "realtime_rows_seen":  realtime_rows,
            "realtime_pages":      pages,
            "aggregate_rows_seen": aggregate_rows,
            "ok":                  ok,
            "error_message":       err,
        })

    return {
        "ok":                ok,
        "realtime_rows":     realtime_rows,
        "realtime_inserted": realtime_inserted,
        "record_count":      record_count,
        "aggregate_rows":    aggregate_rows,
        "bucket_rows":       bucket_rows,
        "elapsed_seconds":   round(elapsed, 2),
        "error":             err,
    }


def _print_dry_run_summary(records: list[dict[str, Any]]) -> None:
    """Human-readable preview of what would be written. Sorted by read
    count desc since that's the most useful axis for a sanity check."""
    if not records:
        print("[msn_insights_etl] (dry-run) no records returned")
        return
    sorted_records = sorted(records, key=lambda r: r.get("readCount") or 0, reverse=True)
    top = sorted_records[:10]
    total_reads = sum(r.get("readCount") or 0 for r in sorted_records)
    est_revenue = total_reads * 0.004  # $4 CPM
    print(
        f"[msn_insights_etl] (dry-run) {len(sorted_records)} records, "
        f"total readCount = {total_reads:,}, est 24h revenue = ${est_revenue:,.2f}"
    )
    print("[msn_insights_etl] (dry-run) top 10 by readCount:")
    for i, r in enumerate(top, 1):
        title = (r.get("title") or "").strip()
        print(f"  {i:>2}. {r.get('readCount'):>5}  {r.get('docID')}  {title[:90]}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Pull MSN Partner Hub insights")
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Pull but skip Neon writes; print a human-readable summary.",
    )
    parser.add_argument(
        "--partner-id", default=DEFAULT_PARTNER_ID,
        help=f"MSN partner ID (default: {DEFAULT_PARTNER_ID} = BoxingNews)",
    )
    parser.add_argument(
        "--window-hours", type=int, default=24,
        help="Realtime window in hours (default 24 — MSN's max for this endpoint).",
    )
    parser.add_argument(
        "--aggregate-days", type=int, default=30,
        help="Daily aggregate trailing window (default 30).",
    )
    args = parser.parse_args()
    result = run(
        partner_id=args.partner_id,
        dry_run=args.dry_run,
        window_hours=args.window_hours,
        aggregate_window_days=args.aggregate_days,
    )
    print(f"[msn_insights_etl] result: {json.dumps(result, indent=2, default=str)}")
    sys.exit(0 if result.get("ok") else 1)
