-- migrations/2026_05_29_compliance_entity_ssp_audit.sql
--
-- Per-(entity × SSP) compliance audit matrix. One row per
-- (revenue-generating entity × SSP actively monetizing it) per audit
-- run. Distinct from compliance_findings: findings only exist when a
-- check fails. THIS table carries the full audit record including
-- HEALTHY rows so the operational dashboard can answer "what % of $X
-- of revenue is compliant" without joining many tables.
--
-- The three explicit flags map 1:1 to the operator's mental model:
--   - pgam_direct_present  : `pgamssp.com, <PGAM-owned-id>, DIRECT` in
--                            publisher's ads.txt
--   - ssp_line_present     : `<ssp>.com, <correct_id>, RESELLER` line
--   - sellers_json_match   : SSP's PGAM-side seat declared in our
--                            sellers.json (validates the downstream
--                            direction of the supply path)
--
-- Status derivation (in audit_matrix._classify):
--   critical : PGAM_Direct OR sellers.json missing
--   warning  : reseller line mismatch only
--   healthy  : all three green

CREATE SCHEMA IF NOT EXISTS pgam_direct;

CREATE TABLE IF NOT EXISTS pgam_direct.compliance_entity_ssp_audit (
    as_of                       DATE        NOT NULL,
    entity_key                  TEXT        NOT NULL,  -- 'dom:foo.com' | 'app:com.bar.baz'
    kind                        TEXT        NOT NULL,  -- 'domain' | 'app'
    entity_value                TEXT        NOT NULL,
    audit_host                  TEXT,
    ll_publisher_name           TEXT,
    revenue_7d                  NUMERIC(14, 4) NOT NULL,

    ssp_key                     TEXT        NOT NULL,
    ssp_partner_name            TEXT        NOT NULL,
    ads_txt_url                 TEXT,

    pgam_direct_present         BOOLEAN     NOT NULL,
    pgam_seller_id_in_adstxt    TEXT,
    pgam_seller_id_expected     TEXT,

    ssp_line_present            BOOLEAN     NOT NULL,
    ssp_seller_id_in_adstxt     TEXT[]      NOT NULL DEFAULT '{}',
    ssp_seller_id_expected      TEXT,

    sellers_json_match          BOOLEAN     NOT NULL,

    status                      TEXT        NOT NULL CHECK (status IN ('critical', 'warning', 'healthy')),
    issues                      TEXT[]      NOT NULL DEFAULT '{}',
    recommended_action          TEXT        NOT NULL DEFAULT '',

    audited_at                  TIMESTAMPTZ NOT NULL DEFAULT now(),

    PRIMARY KEY (entity_key, ssp_key, as_of)
);

-- "Show me yesterday's critical rows ordered by revenue" — the dashboard
-- pull path. Most queries hit `as_of = current_date` so this is the hot
-- index.
CREATE INDEX IF NOT EXISTS idx_compliance_entity_ssp_audit_status_rev
    ON pgam_direct.compliance_entity_ssp_audit (as_of, status, revenue_7d DESC);

-- Per-SSP rollup: "for sharethrough, list every entity × its status".
CREATE INDEX IF NOT EXISTS idx_compliance_entity_ssp_audit_ssp
    ON pgam_direct.compliance_entity_ssp_audit (as_of, ssp_key, revenue_7d DESC);

-- Per-entity rollup: drives the per-publisher score detail page.
CREATE INDEX IF NOT EXISTS idx_compliance_entity_ssp_audit_entity
    ON pgam_direct.compliance_entity_ssp_audit (entity_key, as_of DESC);

-- ── Daily summary materialization ────────────────────────────────────
--
-- One row per as_of with aggregate KPIs so the digest doesn't need to
-- re-aggregate 500+ rows on every read.

CREATE TABLE IF NOT EXISTS pgam_direct.compliance_audit_summary_daily (
    as_of                       DATE        PRIMARY KEY,
    total_rows                  INTEGER     NOT NULL,
    domains_audited             INTEGER     NOT NULL,
    apps_audited                INTEGER     NOT NULL,
    ssps_audited                INTEGER     NOT NULL,
    revenue_audited_usd         NUMERIC(14, 2) NOT NULL,
    revenue_compliant_usd       NUMERIC(14, 2) NOT NULL,
    revenue_non_compliant_usd   NUMERIC(14, 2) NOT NULL,
    compliance_pct              NUMERIC(5, 2)  NOT NULL,
    critical_rows               INTEGER     NOT NULL,
    warning_rows                INTEGER     NOT NULL,
    healthy_rows                INTEGER     NOT NULL,
    below_threshold_rows        INTEGER     NOT NULL,
    computed_at                 TIMESTAMPTZ NOT NULL DEFAULT now()
);
