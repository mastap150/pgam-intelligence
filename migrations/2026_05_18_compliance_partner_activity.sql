-- migrations/2026_05_18_compliance_partner_activity.sql
--
-- Supply Compliance — gate Phase 1 audits to partners showing activity in LL.
--
-- Adds activity flags to compliance_publishers so Phase 1's universal-DIRECT
-- check fires only against publishers currently earning revenue through LL.
-- Stale sellers.json entries (partner never went live, rolled off, or has
-- zero monetization right now) are silently skipped from critical alerting.
-- They stay in compliance_publishers for hygiene tracking; a follow-up
-- "stale entries" report will surface them at info-level on a weekly cadence.

CREATE SCHEMA IF NOT EXISTS pgam_direct;

ALTER TABLE pgam_direct.compliance_publishers
    ADD COLUMN IF NOT EXISTS revenue_recent_7d     NUMERIC(14, 4),
    ADD COLUMN IF NOT EXISTS impressions_recent_7d BIGINT,
    ADD COLUMN IF NOT EXISTS is_active_recent      BOOLEAN     NOT NULL DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS activity_checked_at   TIMESTAMPTZ;

CREATE INDEX IF NOT EXISTS idx_compliance_publishers_active_recent
    ON pgam_direct.compliance_publishers (is_active_recent, revenue_recent_7d DESC)
    WHERE is_active_recent = TRUE;
