-- migrations/2026_06_11_publisher_chain_audit.sql
--
-- Adds the missing Layer-C-direction-2 check: does the supply partner
-- declare the app publisher in THEIR sellers.json?
--
-- Background (operator clarification 2026-06-11): the existing
-- `sellers_json_partner_declared` column on
-- compliance_entity_supply_path_audit was hard-coded TRUE whenever we
-- had a bridge entry. It claimed to validate the chain but actually
-- only confirmed we knew about the partner. The CORRECT validation
-- against ads.txt/sellers.json transparency standards is:
--
--   App publisher  → declared as PUBLISHER (or BOTH) in
--                    the exchange/partner's sellers.json
--   Exchange       → declared as INTERMEDIARY in our sellers.json
--   PGAM           → buyer
--
-- Without the publisher's direct seller record in the partner's
-- sellers.json, the chain is incomplete — buyers can't verify the
-- complete chain of custody from the publisher up.
--
-- This migration adds three columns:
--
--   publisher_declared_in_partner_sj  BOOLEAN
--     The corrected check. True iff the supply partner's sellers.json
--     contains a row with domain == this publisher's audit_host AND
--     seller_type IN ('PUBLISHER','BOTH'). NULL when the partner's
--     sellers.json hasn't been fetched yet or partner has no
--     sellers.json URL.
--
--   partner_sellers_json_seller_id    TEXT
--     The seller_id the partner assigned to this publisher (for
--     diagnostics / outreach asks)
--
--   partner_sellers_json_seller_type  TEXT
--     What the partner declared the publisher as. Catches the
--     "listed but wrong seller_type" case where a publisher is in
--     the file but as INTERMEDIARY when they should be PUBLISHER.
--
-- The existing `sellers_json_partner_declared` column stays for
-- backwards compatibility but is now redundant / always True.

ALTER TABLE pgam_direct.compliance_entity_supply_path_audit
    ADD COLUMN IF NOT EXISTS publisher_declared_in_partner_sj BOOLEAN,
    ADD COLUMN IF NOT EXISTS partner_sellers_json_seller_id   TEXT,
    ADD COLUMN IF NOT EXISTS partner_sellers_json_seller_type TEXT;

CREATE INDEX IF NOT EXISTS idx_compliance_supply_path_publisher_declared
    ON pgam_direct.compliance_entity_supply_path_audit
       (as_of, publisher_declared_in_partner_sj)
    WHERE publisher_declared_in_partner_sj = FALSE;

-- Cache table for each supply partner's parsed sellers.json index.
-- Refreshed daily by publisher_chain_audit. Keyed on
-- (partner_key, publisher_domain) so a single lookup answers "is
-- publisher X declared by partner Y, and as what type?".
CREATE TABLE IF NOT EXISTS pgam_direct.compliance_partner_sellers_index (
    partner_key       TEXT        NOT NULL,
    publisher_domain  TEXT        NOT NULL,
    seller_id         TEXT,
    seller_type       TEXT        NOT NULL,
    seller_name       TEXT,
    snapshot_date     DATE        NOT NULL,
    PRIMARY KEY (partner_key, publisher_domain, seller_id)
);

CREATE INDEX IF NOT EXISTS idx_compliance_partner_sellers_index_domain
    ON pgam_direct.compliance_partner_sellers_index
       (publisher_domain, partner_key);
