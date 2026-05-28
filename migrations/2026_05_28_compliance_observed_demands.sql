-- migrations/2026_05_28_compliance_observed_demands.sql
--
-- Per-demand-name history: tracks every LL DEMAND_PARTNER name ever
-- observed earning revenue, when we first saw it, and whether
-- ssp_registry.classify_demand_name() can map it to a known SSP.
--
-- Two operational signals this unlocks:
--   1. "New demand observed" — a demand_name that never appeared in
--      prior runs. Could be a legitimate new integration we should
--      know about, OR a typo in a campaign config, OR a renamed
--      partner. Either way: surface it before it accumulates revenue
--      under our radar.
--   2. "Demand unmapped to SSP" — a demand_name that
--      classify_demand_name() can't map. Either ssp_registry needs a
--      new pattern, OR this is genuinely a new SSP that needs an
--      ads.txt-line entry added to the registry. Material revenue here
--      = compliance gap (we can't audit the per-entity reseller line
--      for a demand we don't know belongs to which SSP).

CREATE SCHEMA IF NOT EXISTS pgam_direct;

CREATE TABLE IF NOT EXISTS pgam_direct.compliance_observed_demands (
    demand_name        TEXT        PRIMARY KEY,
    first_seen_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_seen_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    revenue_7d_latest  NUMERIC(14, 4) NOT NULL DEFAULT 0,
    ssp_key            TEXT,                            -- mapped to ssp_registry (NULL = unmapped)
    seen_count         INTEGER     NOT NULL DEFAULT 1
);

CREATE INDEX IF NOT EXISTS idx_compliance_observed_demands_first_seen
    ON pgam_direct.compliance_observed_demands (first_seen_at DESC);

CREATE INDEX IF NOT EXISTS idx_compliance_observed_demands_unmapped
    ON pgam_direct.compliance_observed_demands (last_seen_at DESC)
    WHERE ssp_key IS NULL;
