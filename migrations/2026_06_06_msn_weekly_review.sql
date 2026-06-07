-- 2026-06-06: boxingnews weekly content-strategy review
--
-- The boxingnews_weekly_review agent runs every Monday morning and
-- writes ONE row per ISO week into this table. Two payloads land here:
--
--   1. report_md — the human Markdown briefing emailed/Slacked to
--      Priyesh. Lives here for archival and so the admin dashboard at
--      admin.pgammedia.com can render historical reviews without
--      re-running Claude.
--
--   2. strategy — a JSONB policy bundle the boxingnews codebase reads
--      to bias next-week content production. Shape (loose contract;
--      the consumer in boxingnews/src/lib/msn/strategy.ts treats
--      missing keys as empty arrays):
--
--        {
--          "hot_topics":         ["mcgregor", "trump-ufc", "canelo-benavidez", …],
--          "hot_fighters":       ["Conor McGregor", "Saul Alvarez", …],
--          "winning_patterns":   ["P1", "P4", …],   -- pattern keys from the tuner
--          "hot_sources":        ["r/MMA (rising)", "Ariel Helwani (@arielhelwani)", …],
--          "dud_sources":        ["r/Boxing4Beginners (rising)", …],
--          "avoid_phrases":      ["delve into", "in conclusion", …],
--          "notes":              "Free-form analyst notes for next week"
--        }
--
-- Idempotency: iso_week is the PK. Same-week re-runs UPSERT — useful
-- when we tune the agent and want to regenerate without manual cleanup.
CREATE TABLE IF NOT EXISTS pgam_direct.msn_weekly_review (
    iso_week           TEXT          PRIMARY KEY,
    period_start       DATE          NOT NULL,
    period_end         DATE          NOT NULL,
    reads_total        BIGINT        NOT NULL,
    reads_prev_week    BIGINT,
    articles_indexed   INT           NOT NULL,
    revenue_usd_cents  INT           NOT NULL,
    top_article_doc_id TEXT,
    top_article_reads  INT,
    report_md          TEXT          NOT NULL,
    strategy           JSONB         NOT NULL DEFAULT '{}'::jsonb,
    generated_at       TIMESTAMPTZ   NOT NULL DEFAULT NOW()
);

COMMENT ON TABLE pgam_direct.msn_weekly_review IS
    'Weekly boxingnews MSN-syndication postmortem + strategy policy. Written by agents/insights/boxingnews_weekly_review.py every Monday; read by boxingnews headline-tuner & ingest lanes.';

COMMENT ON COLUMN pgam_direct.msn_weekly_review.strategy IS
    'Machine-readable policy for next week. Consumed by boxingnews src/lib/msn/strategy.ts.';

CREATE INDEX IF NOT EXISTS msn_weekly_review_period_end_idx
    ON pgam_direct.msn_weekly_review (period_end DESC);
