-- migrations/2026_05_28_compliance_ll_bridge_many_to_one.sql
--
-- Many-to-one LL publisher → sellers.json bridge.
--
-- Background
-- ----------
-- compliance_publishers.ll_publisher_id is single-valued, which means a
-- sellers.json entry can only carry ONE LL publisher mapping. In practice
-- the same upstream supply partner spawns many LL publishers under
-- variant names — "Start.IO Display without Node", "Start.IO - Video
-- All Demand", "Start.IO Display All Demand" — and all four genuinely
-- bridge to the same start.io sellers.json entry. The single-valued
-- column makes the last write win and silently drops the others, which
-- is why the first Phase 6 production run showed 3,439 entities flowing
-- through partners that ARE bridged but appeared unbridged because the
-- specific LL publisher_id wasn't the most-recent overwrite.
--
-- Fix: a dedicated bridge table keyed on ll_publisher_id (PK), with a
-- many-to-one reference to compliance_publishers.publisher_key.
-- ll_bridge UPSERTs into this table per match. Phase 5 + Phase 6 read
-- their bridge maps from here.
--
-- The legacy compliance_publishers.ll_publisher_id column is kept for
-- backward compatibility (some places still read it); it carries the
-- highest-score match per row.

CREATE SCHEMA IF NOT EXISTS pgam_direct;

CREATE TABLE IF NOT EXISTS pgam_direct.compliance_ll_partner_bridge (
    ll_publisher_id    TEXT        PRIMARY KEY,        -- LL's internal publisher_id
    publisher_key      TEXT        NOT NULL,           -- compliance_publishers.publisher_key
    seller_type        TEXT        NOT NULL,           -- denormalized from compliance_publishers for fast filter
    seller_id          TEXT,                            -- denormalized seller_id (handy for display)
    ll_publisher_name  TEXT,                            -- snapshot of LL's name at bridge time
    bridge_method      TEXT,                            -- 'exact_name' | 'name_substring' | 'domain_substring' | 'token_overlap'
    bridge_score       NUMERIC(4, 3),
    bridged_at         TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_compliance_ll_partner_bridge_pub
    ON pgam_direct.compliance_ll_partner_bridge (publisher_key);

CREATE INDEX IF NOT EXISTS idx_compliance_ll_partner_bridge_type
    ON pgam_direct.compliance_ll_partner_bridge (seller_type);
