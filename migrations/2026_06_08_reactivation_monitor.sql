-- migrations/2026_06_08_reactivation_monitor.sql
--
-- Reactivation monitor — surface previously-blocked supply that's now
-- fixed itself. block_list already has state machine
-- (pending_review / active / released / whitelisted / expired) and the
-- daily auditor already flips active→released when the publisher's
-- ads.txt is healthy again. What we lacked: the metadata the operator
-- needs to triage "is this safe to reactivate?", a recommended_action
-- bucket, and an audit trail field for when we last re-checked.
--
-- New columns on compliance_path_block_list:
--   last_recheck_at         — bumped every reactivation_monitor tick
--   recommended_action      — derived enum surfacing the right call
--   current_compliance_state — JSONB snapshot: what's present vs missing
--                              on the publisher's ads.txt RIGHT NOW
--
-- recommended_action values:
--   'reactivate'        — auto-released today and stable, OK to re-enable
--   'monitor'           — auto-released within the last 24h, watch one more day
--   'keep_blocked'      — still non-compliant; no fix detected
--   'whitelist_aging'   — open > 30d with no fix; consider whitelisting
--   'fixed_pre_review'  — pending_review path that's already healthy; never
--                          needed enforcement, can safely close as released

ALTER TABLE pgam_direct.compliance_path_block_list
    ADD COLUMN IF NOT EXISTS last_recheck_at         TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS recommended_action      TEXT,
    ADD COLUMN IF NOT EXISTS current_compliance_state JSONB;

CREATE INDEX IF NOT EXISTS idx_compliance_block_list_action
    ON pgam_direct.compliance_path_block_list (recommended_action, status_updated_at DESC)
    WHERE recommended_action IS NOT NULL;
