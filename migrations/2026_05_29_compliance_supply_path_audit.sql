-- migrations/2026_05_29_compliance_supply_path_audit.sql
--
-- Per-entity SUPPLY-PATH audit table — the compliance counterpart to
-- the per-(entity × demand_SSP) demand-side audit in
-- compliance_entity_ssp_audit.
--
-- One row per entity per as_of representing the primary supply path
-- bringing its inventory to us. Path is one of:
--   pgam_direct  : entity is in our sellers.json as PUBLISHER; PGAM
--                  is its direct supply partner
--   via_partner  : entity flows through an INTERMEDIARY supply
--                  partner (Smaato, BidMachine, Start.IO, etc.) that
--                  IS bridged to our sellers.json
--   unknown      : LL supply partner not yet bridged in
--                  compliance_ll_partner_bridge
--
-- Expected ads.txt lines differ by path:
--   pgam_direct → `pgamssp.com, <entity-specific seat>, DIRECT`
--   via_partner → `<partner_domain>, *, DIRECT` AND
--                 `pgamssp.com, <partner's PGAM seat>, RESELLER`

CREATE SCHEMA IF NOT EXISTS pgam_direct;

CREATE TABLE IF NOT EXISTS pgam_direct.compliance_entity_supply_path_audit (
    as_of                          DATE        NOT NULL,
    entity_key                     TEXT        NOT NULL,
    kind                           TEXT        NOT NULL,  -- 'domain' | 'app'
    entity_value                   TEXT        NOT NULL,
    audit_host                     TEXT,
    revenue_7d                     NUMERIC(14, 4) NOT NULL,

    path_kind                      TEXT        NOT NULL CHECK (path_kind IN ('pgam_direct', 'via_partner', 'unknown')),
    ll_publisher_id                TEXT,
    ll_publisher_name              TEXT,

    -- Resolved supply partner (NULL for pgam_direct's own publisher,
    -- though we store pgamssp.com here for cleanliness)
    supply_partner_key             TEXT,
    supply_partner_domain          TEXT,
    supply_partner_pgam_seat       TEXT,

    -- The compliance flags
    supply_partner_line_present    BOOLEAN     NOT NULL,
    pgam_line_present_for_path     BOOLEAN     NOT NULL,
    sellers_json_partner_declared  BOOLEAN     NOT NULL,

    -- Diagnostics for action lines
    expected_pgam_line             TEXT,
    observed_pgam_seats            TEXT[]      NOT NULL DEFAULT '{}',
    observed_partner_seats         TEXT[]      NOT NULL DEFAULT '{}',

    status                         TEXT        NOT NULL CHECK (status IN ('critical', 'warning', 'healthy')),
    issues                         TEXT[]      NOT NULL DEFAULT '{}',
    recommended_action             TEXT        NOT NULL DEFAULT '',

    audited_at                     TIMESTAMPTZ NOT NULL DEFAULT now(),

    PRIMARY KEY (entity_key, as_of)
);

CREATE INDEX IF NOT EXISTS idx_compliance_supply_path_status_rev
    ON pgam_direct.compliance_entity_supply_path_audit (as_of, status, revenue_7d DESC);

-- Per-supply-partner rollup: "for Smaato, list every entity it
-- monetizes + status".
CREATE INDEX IF NOT EXISTS idx_compliance_supply_path_partner
    ON pgam_direct.compliance_entity_supply_path_audit (as_of, supply_partner_key, revenue_7d DESC);

-- Per-path-kind rollup: "show me all unknowns" or "all via_partner".
CREATE INDEX IF NOT EXISTS idx_compliance_supply_path_kind
    ON pgam_direct.compliance_entity_supply_path_audit (as_of, path_kind);
