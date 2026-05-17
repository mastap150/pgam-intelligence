-- migrations/2026_05_16_msn_insights.sql
--
-- BoxingNews MSN Partner Hub insights. Schema lives alongside the
-- existing pgam_direct.* tables that the admin.pgammedia.com app reads
-- from. Prefixed with `msn_` to keep them grouped.
--
-- The puller (agents.etl.msn_insights_etl) is idempotent and ensures
-- these tables on every run via CREATE TABLE IF NOT EXISTS, so applying
-- this file manually is optional — useful only for a clean bootstrap
-- or to inspect the schema without running the agent.

CREATE SCHEMA IF NOT EXISTS pgam_direct;

-- ---------------------------------------------------------------------
-- Per-snapshot per-article time series.
-- We pull every 15 min, paginated across the rolling 24h window. Steady
-- state is ~123 articles × 4 snapshots/hr × 24 hr = ~12K rows/day, or
-- ~1M rows / 90 days. No partitioning needed at that volume.
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS pgam_direct.msn_article_snapshots (
    id              BIGSERIAL   PRIMARY KEY,
    partner_id      TEXT        NOT NULL,
    doc_id          TEXT        NOT NULL,                -- e.g. 'AA23khOk' -> public URL .../ar-{doc_id}
    snapshot_at     TIMESTAMPTZ NOT NULL DEFAULT now(),  -- when our cron pulled
    msn_title       TEXT        NOT NULL,                -- editable MSN-displayed headline
    title_status    INTEGER,                              -- titleStatus from MSN payload
    read_count      INTEGER     NOT NULL,                -- PVs in MSN's rolling 24h window
    rank_in_window  INTEGER     NOT NULL,                -- 1-based rank by view at snapshot time
    record_count    INTEGER                               -- total articles in same 24h window
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_msn_snapshots_doc_time
    ON pgam_direct.msn_article_snapshots (doc_id, snapshot_at);
CREATE INDEX IF NOT EXISTS idx_msn_snapshots_partner_time
    ON pgam_direct.msn_article_snapshots (partner_id, snapshot_at DESC);
CREATE INDEX IF NOT EXISTS idx_msn_snapshots_time
    ON pgam_direct.msn_article_snapshots (snapshot_at DESC);

-- ---------------------------------------------------------------------
-- One row per docID. Lazily backfilled by agents.enrichment.msn_doc_resolver
-- which hits the public MSN URL once per docID, parses <link rel=canonical>
-- to find the source boxingnews.com URL, and grabs OG title/image.
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS pgam_direct.msn_article_meta (
    doc_id            TEXT        PRIMARY KEY,
    partner_id        TEXT        NOT NULL,
    msn_url           TEXT,                                   -- public MSN URL we tried to fetch
    canonical_url     TEXT,                                   -- boxingnews.com source URL from <link rel=canonical>
    thumbnail_url     TEXT,                                   -- og:image
    msn_title_first   TEXT,                                   -- first MSN-displayed headline observed
    canonical_title   TEXT,                                   -- og:title from boxingnews
    first_seen_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_resolved_at  TIMESTAMPTZ,                            -- last successful resolve
    resolve_attempts  INTEGER     NOT NULL DEFAULT 0,
    resolve_status    TEXT        NOT NULL DEFAULT 'pending', -- pending | ok | failed | gone
    resolve_error     TEXT
);

CREATE INDEX IF NOT EXISTS idx_msn_meta_canonical
    ON pgam_direct.msn_article_meta (canonical_url);
CREATE INDEX IF NOT EXISTS idx_msn_meta_status_seen
    ON pgam_direct.msn_article_meta (resolve_status, first_seen_at);

-- ---------------------------------------------------------------------
-- MSN daily aggregate. One row per (partner, date, content_type).
-- content_type=4 confirmed = video. Other values to be confirmed
-- (1 = article suspected). All counts default 0 so partial fields
-- from MSN don't blow up the insert.
-- ---------------------------------------------------------------------
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

-- ---------------------------------------------------------------------
-- Run log. Lets us see at a glance when the puller last ran, how long
-- it took, whether anything errored. Used by the dashboard's "data
-- freshness" indicator and by anomaly checks.
-- ---------------------------------------------------------------------
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

-- ---------------------------------------------------------------------
-- 15-min traffic buckets — discovered 2026-05-16. Partner Hub's Overview
-- tab calls the SAME /realtime endpoint but with `$orderBy` stripped and
-- `date=-1` set; the response flips from per-article rows to per-15-min
-- total-PV rows. recordCount ≈ 95 (24h × 4 slots, minus the partial
-- bucket the call lands inside). Authoritative source for total 24h PVs
-- and therefore current MSN revenue estimate.
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS pgam_direct.msn_traffic_buckets (
    partner_id    TEXT        NOT NULL,
    bucket_at     TIMESTAMPTZ NOT NULL,
    read_count    INTEGER     NOT NULL,
    last_seen_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (partner_id, bucket_at)
);

CREATE INDEX IF NOT EXISTS idx_msn_traffic_buckets_partner_time
    ON pgam_direct.msn_traffic_buckets (partner_id, bucket_at DESC);

-- ---------------------------------------------------------------------
-- Convenience view: per-doc peak readCount over a recent window.
-- The dashboard's "top performers" table reads from this.
-- We take MAX(read_count) over snapshots because readCount is the
-- rolling 24h total at snapshot time; the max approximates the peak
-- 24h-window total the article reached. Estimated revenue uses
-- the $4 CPM rate locked in 2026-05-16 with Priyesh.
-- ---------------------------------------------------------------------
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
