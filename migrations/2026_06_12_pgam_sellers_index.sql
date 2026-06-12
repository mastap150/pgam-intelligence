-- migrations/2026_06_12_pgam_sellers_index.sql
--
-- Layer C direction-1 — does PGAM's own sellers.json correctly declare
-- every upstream supply partner as INTERMEDIARY with the seat we use on
-- publisher app-ads.txt pgamssp.com lines?
--
-- Mirror of compliance_partner_sellers_index, but for OUR file
-- (pgamssp.com/sellers.json) instead of theirs. Refreshed daily by
-- agents/compliance/pgam_sellers_validator.py. Sibling table — same
-- shape on purpose so any future tooling that reads both gets a
-- uniform interface.
--
-- Why this matters: PubMatic and other buyers verify the full schain
-- by reading PGAM's sellers.json. If our file doesn't declare
-- BidMachine as INTERMEDIARY with the right seat, every BidMachine
-- path fails their transparency check — independent of how clean the
-- publisher-side ads.txt is.

CREATE TABLE IF NOT EXISTS pgam_direct.compliance_pgam_sellers_index (
    snapshot_date   DATE        NOT NULL,
    seller_id       TEXT        NOT NULL,
    seller_type     TEXT        NOT NULL,
    seller_name     TEXT,
    domain          TEXT        NOT NULL,
    is_confidential BOOLEAN,
    PRIMARY KEY (snapshot_date, seller_id, domain)
);

CREATE INDEX IF NOT EXISTS idx_compliance_pgam_sellers_index_domain
    ON pgam_direct.compliance_pgam_sellers_index
       (domain, snapshot_date);

-- Findings table — one row per (partner, snapshot_date) — keeps the
-- read-only finding history queryable beyond Slack scrollback.
CREATE TABLE IF NOT EXISTS pgam_direct.compliance_pgam_sellers_findings (
    snapshot_date   DATE        NOT NULL,
    partner_key     TEXT        NOT NULL,
    expected_seat   TEXT,        -- compliance_ll_partner_bridge.seller_id
    declared_seat   TEXT,        -- seller_id observed in PGAM sellers.json
    declared_type   TEXT,        -- INTERMEDIARY / BOTH / PUBLISHER / null
    status          TEXT        NOT NULL,  -- 'ok' | 'missing' | 'wrong_type' | 'wrong_seat'
    evidence        JSONB,
    PRIMARY KEY (snapshot_date, partner_key)
);
