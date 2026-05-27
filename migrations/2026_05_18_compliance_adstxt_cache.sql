-- migrations/2026_05_18_compliance_adstxt_cache.sql
--
-- Conditional-GET cache for the ads.txt / app-ads.txt crawler.
--
-- Most partner ads.txt files change rarely (weekly at most). Storing the
-- last ETag + Last-Modified per (publisher_key, variant) lets the crawler
-- send If-None-Match / If-Modified-Since headers on every subsequent run.
-- When the server replies 304 Not Modified we reuse the cached parsed
-- lines + variables instead of re-downloading + re-parsing.
--
-- Net effect at Phase 5.1 scale (~500-1500 entities):
--   * ~90 % of fetches become 304s after the first warm run
--   * Bandwidth + Render CPU drops accordingly
--   * Run wall time at rate_hz=2.0 drops from ~10 min to ~2-3 min
--
-- One row per (publisher_key, variant). Keyed alongside the existing
-- append-only compliance_adstxt_fetches table — this one is overwritten
-- per fetch, that one is the audit trail.

CREATE SCHEMA IF NOT EXISTS pgam_direct;

CREATE TABLE IF NOT EXISTS pgam_direct.compliance_adstxt_cache (
    publisher_key    TEXT        NOT NULL,
    variant          TEXT        NOT NULL,                  -- 'ads.txt' | 'app-ads.txt'
    etag             TEXT,
    last_modified    TEXT,                                   -- raw HTTP header value
    body_sha256      TEXT,
    parsed_lines     JSONB       NOT NULL DEFAULT '[]'::jsonb,
    parsed_variables JSONB       NOT NULL DEFAULT '{}'::jsonb,
    fetched_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    hit_count        INTEGER     NOT NULL DEFAULT 0,         -- # of 304 hits since last 200
    PRIMARY KEY (publisher_key, variant)
);

CREATE INDEX IF NOT EXISTS idx_compliance_adstxt_cache_fetched
    ON pgam_direct.compliance_adstxt_cache (fetched_at DESC);
