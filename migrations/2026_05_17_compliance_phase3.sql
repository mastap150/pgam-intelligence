-- migrations/2026_05_17_compliance_phase3.sql
--
-- Supply Compliance Phase 3 — downstream sellers.json audit + per-publisher
-- compliance score time-series.
--
-- Adds:
--   * compliance_downstream_sellersjson_fetches — append-only crawl log of
--     every downstream SSP sellers.json fetch (Rubicon, PubMatic, ...).
--     Mirrors compliance_adstxt_fetches in shape.
--   * compliance_publisher_scores_daily — per-publisher 0..100 compliance
--     score snapshotted nightly. Dashboard reads this for trend lines.
--
-- Idempotent — runner ensures schema on every invocation.

CREATE SCHEMA IF NOT EXISTS pgam_direct;

-- ---------------------------------------------------------------------
-- Downstream sellers.json crawl log. Verifies our seat exists in each
-- SSP's published sellers.json — catches the "SSP dropped you and you
-- didn't notice" failure mode that ads.txt alone can't see.
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS pgam_direct.compliance_downstream_sellersjson_fetches (
    fetch_id        BIGSERIAL   PRIMARY KEY,
    ssp_key         TEXT        NOT NULL,
    url             TEXT        NOT NULL,
    fetched_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    http_status     INTEGER,
    body_sha256     TEXT,
    seller_count    INTEGER,
    pgam_seat_found BOOLEAN,
    error           TEXT
);

CREATE INDEX IF NOT EXISTS idx_compliance_downstream_ssp_time
    ON pgam_direct.compliance_downstream_sellersjson_fetches (ssp_key, fetched_at DESC);

-- ---------------------------------------------------------------------
-- Per-publisher daily compliance score.
--
-- compliance_score = max(0, 100 − 25*critical − 10*high − 3*medium − 1*info)
-- counts include open findings only (status = 'open'); resolved &
-- suppressed don't drag the score.
--
-- Sentinel publisher_keys starting with '_ssp:' are SSP-level findings
-- (downstream sellers.json audits) and are excluded from publisher
-- scoring — they show up as their own scorecard later.
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS pgam_direct.compliance_publisher_scores_daily (
    publisher_key    TEXT        NOT NULL,
    as_of            DATE        NOT NULL,
    compliance_score NUMERIC(5, 2) NOT NULL,
    open_critical    INTEGER     NOT NULL DEFAULT 0,
    open_high        INTEGER     NOT NULL DEFAULT 0,
    open_medium      INTEGER     NOT NULL DEFAULT 0,
    open_info        INTEGER     NOT NULL DEFAULT 0,
    computed_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (publisher_key, as_of)
);

CREATE INDEX IF NOT EXISTS idx_compliance_scores_as_of
    ON pgam_direct.compliance_publisher_scores_daily (as_of DESC);

CREATE INDEX IF NOT EXISTS idx_compliance_scores_low
    ON pgam_direct.compliance_publisher_scores_daily (as_of DESC, compliance_score ASC)
    WHERE compliance_score < 75;
