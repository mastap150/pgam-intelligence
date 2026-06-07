-- migrations/2026_06_07_compliance_enforcement.sql
--
-- Phase 1 of automated compliance enforcement.
--
-- compliance_path_block_list (created 2026-06-02) is the Stage 1
-- queue: the auditor flags non-compliant (entity × supply_partner)
-- paths. Until now the table was queue-only — no agent consumed it.
--
-- This migration adds the two tables the enforcer needs:
--
--   compliance_enforcement_log
--     Audit trail of every LL mgmt API call the enforcer makes.
--     Records: which path, what action, before/after LL state,
--     dry-run flag, who/what triggered it. Required for:
--       • Reverting a block (look up the before-state)
--       • Diff against a revenue-drop attribution
--       • Compliance/legal record of "we paused supply for cause X"
--
--   compliance_block_snooze
--     Ops exceptions. "Don't auto-block this path for 7 days,
--     we're in outreach with the publisher". Snooze rows let
--     the block_list keep status='pending_review' without
--     escalating to 'active' even when the auditor re-flags.

CREATE SCHEMA IF NOT EXISTS pgam_direct;

-- ── 1. Enforcement audit log ─────────────────────────────────────────
CREATE TABLE IF NOT EXISTS pgam_direct.compliance_enforcement_log (
    log_id                BIGSERIAL    PRIMARY KEY,

    -- Which path was acted on
    entity_key            TEXT        NOT NULL,
    supply_partner_key    TEXT        NOT NULL,
    ll_publisher_id       TEXT,
    demand_id             TEXT,                       -- the LL demand we paused (if applicable)
    entity_value          TEXT,
    revenue_7d_at_action  NUMERIC(14, 4),

    -- What happened
    action                TEXT        NOT NULL
        CHECK (action IN (
            'auto_disable',         -- agent flipped LL demand → disabled
            'auto_revert',          -- agent re-enabled after revenue drop attribution
            'manual_override',      -- ops manually changed status
            'dry_run_would_disable',-- agent in dry-run mode logged intent only
            'snooze_applied',
            'snooze_expired',
            'whitelisted'
        )),
    triggered_by          TEXT        NOT NULL,       -- 'enforcer' | 'cli:<user>' | 'auditor' | 'auto_revert'
    reason                TEXT,                       -- block_list.reason or ad-hoc note
    dry_run               BOOLEAN     NOT NULL DEFAULT TRUE,

    -- State capture for reversal
    ll_state_before       JSONB,                      -- snapshot of `demand_enabled`, etc. so revert can restore
    ll_state_after        JSONB,
    api_response          JSONB,                      -- raw LL mgmt response for debugging
    error                 TEXT,

    created_at            TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_compliance_enforcement_log_path
    ON pgam_direct.compliance_enforcement_log
    (entity_key, supply_partner_key, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_compliance_enforcement_log_recent
    ON pgam_direct.compliance_enforcement_log (created_at DESC);

-- "Last action on this path" — answers "is this path currently disabled
-- by us, and if so when?". Used by the revert path + the daily digest's
-- enforcement-state column.
CREATE INDEX IF NOT EXISTS idx_compliance_enforcement_log_latest
    ON pgam_direct.compliance_enforcement_log
    (entity_key, supply_partner_key, created_at DESC)
    WHERE dry_run = FALSE;


-- ── 2. Snooze table — ops "don't auto-block this" exceptions ────────
CREATE TABLE IF NOT EXISTS pgam_direct.compliance_block_snooze (
    entity_key            TEXT        NOT NULL,
    supply_partner_key    TEXT        NOT NULL,
    snoozed_until         TIMESTAMPTZ NOT NULL,
    reason                TEXT        NOT NULL,       -- "in outreach with publisher", "Q3 commitment", etc.
    snoozed_by            TEXT        NOT NULL,       -- 'cli:<user>' | 'auto:<reason>'
    created_at            TIMESTAMPTZ NOT NULL DEFAULT now(),

    PRIMARY KEY (entity_key, supply_partner_key)
);

-- Plain index on snoozed_until — partial WHERE clauses can't reference
-- now() (must be IMMUTABLE). The enforcer's lookup filters at query time.
CREATE INDEX IF NOT EXISTS idx_compliance_block_snooze_active
    ON pgam_direct.compliance_block_snooze (snoozed_until DESC);
