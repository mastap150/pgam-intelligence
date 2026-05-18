-- migrations/2026_05_17_compliance_phase2.sql
--
-- Supply Compliance Phase 2 — conditional reseller-line validation.
--
-- Adds:
--   * LL publisher_id ↔ sellers.json domain bridge columns on compliance_publishers
--   * compliance_observed_monetization: rolling (publisher_key × ssp_key) revenue
--     activity table, refreshed per run from ll_daily_partner_revenue. The
--     conditional reseller validator reads this to decide which RESELLER lines
--     a given publisher's ads.txt is required to contain.
--
-- Idempotent — runner ensures schema on every invocation.

CREATE SCHEMA IF NOT EXISTS pgam_direct;

-- ---------------------------------------------------------------------
-- LL bridge columns. Filled by agents/compliance/ll_bridge.py which
-- pulls distinct (publisher_id, publisher_name) tuples out of
-- pgam_direct.ll_daily_partner_revenue and tries to match each one back
-- to a domain row in compliance_publishers (by name → sellers.json
-- domain). A row may stay unbridged forever — that's fine, we simply
-- skip those publishers in the conditional reseller validator and log.
-- ---------------------------------------------------------------------
ALTER TABLE pgam_direct.compliance_publishers
    ADD COLUMN IF NOT EXISTS ll_publisher_id    TEXT,
    ADD COLUMN IF NOT EXISTS ll_publisher_name  TEXT,
    ADD COLUMN IF NOT EXISTS ll_match_method    TEXT,        -- 'exact_name' | 'token_overlap' | 'domain_substring'
    ADD COLUMN IF NOT EXISTS ll_match_score     NUMERIC(4,3),
    ADD COLUMN IF NOT EXISTS ll_matched_at      TIMESTAMPTZ;

CREATE INDEX IF NOT EXISTS idx_compliance_publishers_ll_id
    ON pgam_direct.compliance_publishers (ll_publisher_id)
    WHERE ll_publisher_id IS NOT NULL;

-- ---------------------------------------------------------------------
-- Observed monetization: which SSPs are actively buying each
-- publisher's inventory through PGAM. Truncated and rebuilt every run
-- from the trailing N-day window in ll_daily_partner_revenue. A
-- (publisher_key, ssp_key) row exists IFF that SSP earned > 0 USD via
-- that publisher in the last N days; absence == "not currently
-- monetizing" == reseller line not required.
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS pgam_direct.compliance_observed_monetization (
    publisher_key     TEXT        NOT NULL,
    ssp_key           TEXT        NOT NULL,        -- 'rubicon' | 'pubmatic' | ...
    ssp_domain        TEXT        NOT NULL,        -- 'rubiconproject.com'
    lookback_days     INT         NOT NULL,
    revenue_usd       NUMERIC(14, 4) NOT NULL DEFAULT 0,
    impressions       BIGINT      NOT NULL DEFAULT 0,
    demand_count      INT         NOT NULL DEFAULT 0,
    demand_names      TEXT[]      NOT NULL DEFAULT '{}',
    first_observed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_observed_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (publisher_key, ssp_key)
);

CREATE INDEX IF NOT EXISTS idx_compliance_observed_pub
    ON pgam_direct.compliance_observed_monetization (publisher_key);

CREATE INDEX IF NOT EXISTS idx_compliance_observed_ssp
    ON pgam_direct.compliance_observed_monetization (ssp_key);
