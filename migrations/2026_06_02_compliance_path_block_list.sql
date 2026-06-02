-- migrations/2026_06_02_compliance_path_block_list.sql
--
-- Per-(entity × supply_partner) block-list queue.
--
-- LL's mgmt API gives us publisher-level on/off and per-(publisher ×
-- demand) wiring toggles, but NOT per-(publisher × specific bundle/
-- domain) granularity. So we can't enforce "block the Smaato → foxsports
-- path while leaving Smaato → other-publisher paths intact" via LL.
--
-- The enforcement layer lives at PGAM's own bidder edge in
-- pgam-direct/web: on each incoming bid request, look up
-- (LL publisher_id, bundle/domain) here; if the row exists AND
-- status='active', return no-bid. The bidder-edge filter is Stage 3 of
-- this build; this table is the contract between Stage 1 (auditor
-- populates) and Stage 3 (bidder reads).
--
-- Workflow:
--   1. Daily auditor inserts/upserts rows from supply_path_audit where
--      status != 'healthy' AND revenue_7d >= materiality threshold.
--      Default status: 'pending_review'.
--   2. Ops reviews queue (via digest + admin UI), flips qualifying rows
--      to status='active'.
--   3. pgam-direct/web bidder edge consults this table per-request.
--   4. Auto-release: every audit run, rows with status='active' where
--      the latest audit shows path is now healthy get status='released'
--      and the bidder edge stops blocking.

CREATE SCHEMA IF NOT EXISTS pgam_direct;

CREATE TABLE IF NOT EXISTS pgam_direct.compliance_path_block_list (
    -- Identity (composite key)
    entity_key            TEXT        NOT NULL,    -- 'dom:foo.com' | 'app:com.bar.baz'
    supply_partner_key    TEXT        NOT NULL,    -- 'smaato.com' (compliance_publishers.publisher_key)
    ll_publisher_id       TEXT,                    -- LL publisher_id for the bidder filter
    -- ↑ kept as TEXT for join compatibility; bidder edge needs this
    --   to match incoming bid requests at runtime.

    -- Context (denormalized from audit row at insert time, refreshed daily)
    entity_value          TEXT        NOT NULL,
    kind                  TEXT        NOT NULL CHECK (kind IN ('domain', 'app')),
    audit_host            TEXT,
    supply_partner_pgam_seat TEXT,
    ll_publisher_name     TEXT,
    revenue_7d            NUMERIC(14, 4) NOT NULL DEFAULT 0,

    -- Why this row exists
    reason                TEXT        NOT NULL,    -- e.g. 'pgam_line_missing', 'partner_line_missing', 'both'
    first_flagged_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_flagged_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    flagged_count         INTEGER     NOT NULL DEFAULT 1,

    -- State machine
    status                TEXT        NOT NULL DEFAULT 'pending_review'
        CHECK (status IN (
            'pending_review',   -- Auditor flagged, ops hasn't reviewed yet
            'active',           -- Approved for enforcement; bidder edge blocks
            'released',         -- Path now healthy; bidder edge stops blocking
            'whitelisted',      -- Ops explicitly opted out — never block this row
            'expired'           -- Path stopped earning revenue; auto-cleared
        )),
    status_updated_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    status_updated_by     TEXT,                    -- 'auditor' | 'ops:<name>'
    review_notes          TEXT,

    -- Metrics
    first_blocked_at      TIMESTAMPTZ,             -- When enforcement actually started
    revenue_blocked_usd   NUMERIC(14, 4) NOT NULL DEFAULT 0,
    times_blocked         BIGINT      NOT NULL DEFAULT 0,

    PRIMARY KEY (entity_key, supply_partner_key)
);

CREATE INDEX IF NOT EXISTS idx_compliance_block_list_status
    ON pgam_direct.compliance_path_block_list (status, last_flagged_at DESC);

CREATE INDEX IF NOT EXISTS idx_compliance_block_list_revenue
    ON pgam_direct.compliance_path_block_list (revenue_7d DESC)
    WHERE status IN ('pending_review', 'active');

-- The bidder-edge filter's hot lookup: given LL publisher_id + bundle/
-- domain at bid time, is this path blocked?
CREATE INDEX IF NOT EXISTS idx_compliance_block_list_runtime
    ON pgam_direct.compliance_path_block_list (ll_publisher_id, entity_value)
    WHERE status = 'active';
