-- migrations/2026_05_17_compliance.sql
--
-- Supply Compliance & Quality Intelligence agent (Phase 1).
--
-- Tables back the daily ads.txt / sellers.json / schain audit run by
-- agents/compliance/runner.py. Same pattern as the MSN tables: idempotent
-- CREATE TABLE IF NOT EXISTS so the agent ensures schema on every run,
-- and applying this file by hand is optional (useful for a clean bootstrap
-- or to inspect the schema without running the agent).
--
-- All tables prefixed `compliance_` and live in pgam_direct so the
-- admin.pgammedia.com app can read them alongside the existing tables.

CREATE SCHEMA IF NOT EXISTS pgam_direct;

-- ---------------------------------------------------------------------
-- Universe: one row per publisher we are responsible for monitoring.
-- Populated from PGAM's sellers.json (sellers.pgamssp.com). seller_id
-- is the account_id that must appear in the partner's ads.txt against
-- pgamssp.com as the PGAM DIRECT line.
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS pgam_direct.compliance_publishers (
    publisher_key   TEXT        PRIMARY KEY,           -- normalized: domain (lowercased)
    kind            TEXT        NOT NULL,              -- 'domain' | 'app'  (Phase 1 = 'domain' only)
    domain          TEXT        NOT NULL,              -- bare hostname, no scheme
    seller_id       TEXT        NOT NULL,              -- account_id from PGAM sellers.json
    seller_type     TEXT        NOT NULL,              -- PUBLISHER | INTERMEDIARY | BOTH
    seller_name     TEXT,
    source          TEXT        NOT NULL DEFAULT 'pgam_sellers_json',
    first_seen_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_seen_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    is_active       BOOLEAN     NOT NULL DEFAULT TRUE
);

CREATE INDEX IF NOT EXISTS idx_compliance_publishers_active
    ON pgam_direct.compliance_publishers (is_active, last_seen_at DESC);

-- ---------------------------------------------------------------------
-- Raw ads.txt / app-ads.txt fetches. Append-only. Snapshot of every
-- crawl so we can diff & answer "when did this file change".
-- body is kept null on non-200 to save space; sha lets us cheaply
-- detect change-of-content runs.
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS pgam_direct.compliance_adstxt_fetches (
    fetch_id        BIGSERIAL   PRIMARY KEY,
    publisher_key   TEXT        NOT NULL,
    variant         TEXT        NOT NULL,              -- 'ads.txt' | 'app-ads.txt'
    fetched_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    http_status     INTEGER,
    body_sha256     TEXT,
    line_count      INTEGER,
    error           TEXT
);

CREATE INDEX IF NOT EXISTS idx_compliance_adstxt_fetches_pub_time
    ON pgam_direct.compliance_adstxt_fetches (publisher_key, variant, fetched_at DESC);

-- ---------------------------------------------------------------------
-- Findings: the operational table. Dashboard reads from here. Upserted
-- by (publisher_key, check_id, fingerprint) — a recurring issue refreshes
-- last_observed_at rather than creating a new row, which gives us "this
-- has been broken for N days" for free.
--
-- status transitions:
--   open      → resolved   (set by runner when next scan passes)
--   open      → suppressed (manual ack via dashboard)
--   resolved  → open       (regression — runner re-opens with new last_observed_at)
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS pgam_direct.compliance_findings (
    finding_id          BIGSERIAL   PRIMARY KEY,
    publisher_key       TEXT        NOT NULL,
    category            TEXT        NOT NULL,           -- 'adstxt' | 'sellersjson' | 'schain' | 'monetization'
    check_id            TEXT        NOT NULL,           -- stable: 'adstxt.universal_direct_missing' etc.
    severity            TEXT        NOT NULL,           -- 'critical' | 'high' | 'medium' | 'info'
    status              TEXT        NOT NULL DEFAULT 'open',
    fingerprint         TEXT        NOT NULL,           -- dedup key inside (publisher_key, check_id)
    first_observed_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_observed_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    resolved_at         TIMESTAMPTZ,
    detail              JSONB       NOT NULL DEFAULT '{}'::jsonb,
    UNIQUE (publisher_key, check_id, fingerprint)
);

CREATE INDEX IF NOT EXISTS idx_compliance_findings_open
    ON pgam_direct.compliance_findings (severity, last_observed_at DESC)
    WHERE status = 'open';

CREATE INDEX IF NOT EXISTS idx_compliance_findings_pub
    ON pgam_direct.compliance_findings (publisher_key, status, severity);

-- ---------------------------------------------------------------------
-- Run log. One row per full compliance_runner invocation. Used for
-- freshness checks ("when did compliance last run") and rate-of-findings
-- trend lines on the dashboard.
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS pgam_direct.compliance_runs (
    run_id              BIGSERIAL   PRIMARY KEY,
    started_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at         TIMESTAMPTZ,
    publishers_scanned  INTEGER,
    adstxt_fetched      INTEGER,
    findings_opened     INTEGER,
    findings_resolved   INTEGER,
    ok                  BOOLEAN     NOT NULL DEFAULT FALSE,
    error               TEXT
);

CREATE INDEX IF NOT EXISTS idx_compliance_runs_started
    ON pgam_direct.compliance_runs (started_at DESC);
