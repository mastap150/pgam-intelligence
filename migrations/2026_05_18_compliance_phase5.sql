-- migrations/2026_05_18_compliance_phase5.sql
--
-- Supply Compliance Phase 5 — per-app + per-domain compliance.
--
-- Shifts the unit of analysis from "PGAM sellers.json publisher" (Phase
-- 1-4) to "LL supply entity" = a unique app bundle or domain in LL
-- stats. Universe is built from pgam_direct.ll_daily_publisher_*_demand
-- which the existing ll_4dim_etl already populates hourly.
--
-- The Phase 5 entity audit:
--   - Top N entities by trailing 7d gross_revenue (default 200)
--   - For each: fetch ads.txt (domain) or app-ads.txt (bundle via
--     app_metadata.dev_domain)
--   - Validate the universal PGAM DIRECT line using the publisher's
--     seller_id from PGAM's sellers.json
--   - Validate the conditional reseller-line set computed from the
--     SSPs observed monetizing THIS entity (not just THIS partner)
--
-- Findings reuse pgam_direct.compliance_findings with publisher_key:
--   - dom:<domain>   for domain entities
--   - app:<bundle>   for bundle entities

CREATE SCHEMA IF NOT EXISTS pgam_direct;

-- ---------------------------------------------------------------------
-- Per-run universe snapshot. Truncated and rebuilt every Phase 5 run.
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS pgam_direct.compliance_supply_entities (
    entity_key          TEXT        PRIMARY KEY,
    kind                TEXT        NOT NULL,           -- 'domain' | 'app'
    entity_value        TEXT        NOT NULL,           -- the domain or bundle
    ll_publisher_id     TEXT        NOT NULL,
    ll_publisher_name   TEXT,
    audit_host          TEXT,                            -- hostname where ads.txt lives
    audit_variant       TEXT        NOT NULL,           -- 'ads.txt' | 'app-ads.txt'
    revenue_7d          NUMERIC(14, 4) NOT NULL DEFAULT 0,
    impressions_7d      BIGINT      NOT NULL DEFAULT 0,
    active_ssps         TEXT[]      NOT NULL DEFAULT '{}',
    unclassified_demand_count INT   NOT NULL DEFAULT 0,
    expected_seller_id  TEXT,                            -- from PGAM sellers.json via partner mapping
    first_seen_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_seen_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_compliance_supply_entities_pub
    ON pgam_direct.compliance_supply_entities (ll_publisher_id);

CREATE INDEX IF NOT EXISTS idx_compliance_supply_entities_kind_rev
    ON pgam_direct.compliance_supply_entities (kind, revenue_7d DESC);

-- ---------------------------------------------------------------------
-- Extend app_metadata with developer URL fields so the entity audit can
-- resolve bundle → app-ads.txt host. agents/enrichment/app_dev_url_resolver
-- (follow-up) will lazily backfill these on top of the existing
-- app_name_enrichment pass.
-- ---------------------------------------------------------------------
ALTER TABLE pgam_direct.app_metadata
    ADD COLUMN IF NOT EXISTS dev_domain         TEXT,
    ADD COLUMN IF NOT EXISTS dev_url_resolved_at TIMESTAMPTZ;

CREATE INDEX IF NOT EXISTS idx_app_metadata_dev_domain
    ON pgam_direct.app_metadata (dev_domain)
    WHERE dev_domain IS NOT NULL;
