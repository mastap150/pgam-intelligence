-- migrations/2026_06_02_supply_path_schain_column.sql
--
-- Layer 5 of the (entity × supply_partner) compliance model: emitted
-- schain validation per pair. Adds the column up-front so the
-- validator can populate it; today the value is NULL for every row
-- because the source view compliance_schain_emissions_24h doesn't
-- exist yet. It gets created by a separate ClickHouse → Postgres
-- rollup in pgam-direct/web that's outside this repo.
--
-- Semantics when the source data lands:
--   TRUE   = at least one emission observed in last 24h for the
--            (publisher_id, supply_partner) pair AND no schain.* finding
--   FALSE  = emissions seen but with incomplete=true or hops>2 (i.e.
--            the actual bid traffic carries a broken schain object)
--   NULL   = no emission data yet for this pair (either ClickHouse view
--            not built, or no bid traffic in the trailing 24h)

ALTER TABLE pgam_direct.compliance_entity_supply_path_audit
    ADD COLUMN IF NOT EXISTS schain_emitted_ok BOOLEAN;

ALTER TABLE pgam_direct.compliance_entity_supply_path_audit
    ADD COLUMN IF NOT EXISTS schain_emissions_24h BIGINT;

ALTER TABLE pgam_direct.compliance_entity_supply_path_audit
    ADD COLUMN IF NOT EXISTS schain_incomplete_rate NUMERIC(6, 4);

ALTER TABLE pgam_direct.compliance_entity_supply_path_audit
    ADD COLUMN IF NOT EXISTS schain_hop_violation_rate NUMERIC(6, 4);
