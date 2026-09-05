-- Destination.com Video Engine (DVE) — production schema.
--
-- Own schema (`dve`), same Neon project as pgam_direct, so the video system
-- can never collide with SSP tables and can be dropped wholesale if the
-- product is killed.
--
-- Design note: every table carries its queried/indexed scalars as real
-- columns plus the full record as `payload JSONB`. The entity shapes are
-- evolving weekly during Phase 1-2; the scalar columns cover every query the
-- dashboard, scheduler and learning engine actually run. Promote a field out
-- of the payload with a follow-up migration the day a query needs it —
-- do not query into payload from application code.
--
-- The Python store (video/store.py) treats these tables generically:
-- upsert by id, filter by the scalar columns. Table list must stay in sync
-- with video/store.py::TABLES.

CREATE SCHEMA IF NOT EXISTS dve;

-- Shared trigger to maintain updated_at.
CREATE OR REPLACE FUNCTION dve.touch_updated_at() RETURNS trigger AS $$
BEGIN
  NEW.updated_at = now();
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- ---------------------------------------------------------------------------
-- Generic table builder pattern (repeated literally per table so the file is
-- greppable; all tables share: id, status, created_at, updated_at, created_by,
-- payload).
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS dve.content_sources (
  id          text PRIMARY KEY,
  status      text NOT NULL DEFAULT 'active',
  source_type text NOT NULL,           -- article | news | destination_page | fare_alert | trip_planner | newsletter | youtube_comment | search_trend | seasonal | external_news
  source_url  text,
  destination text,
  country     text,
  theme       text,
  publish_date date,
  freshness_score  double precision,
  quality_score    double precision,
  payload     jsonb NOT NULL,
  created_by  text,
  created_at  timestamptz NOT NULL DEFAULT now(),
  updated_at  timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS content_sources_dest_idx ON dve.content_sources (destination);
CREATE INDEX IF NOT EXISTS content_sources_type_idx ON dve.content_sources (source_type, status);

CREATE TABLE IF NOT EXISTS dve.opportunities (
  id          text PRIMARY KEY,
  status      text NOT NULL DEFAULT 'open',   -- open | accepted | dismissed | produced
  source_id   text REFERENCES dve.content_sources(id),
  score       double precision NOT NULL,
  manual_boost double precision NOT NULL DEFAULT 0,
  destination text,
  franchise   text,
  payload     jsonb NOT NULL,                 -- component scores + reasons
  created_by  text,
  created_at  timestamptz NOT NULL DEFAULT now(),
  updated_at  timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS opportunities_score_idx ON dve.opportunities (status, score DESC);

CREATE TABLE IF NOT EXISTS dve.concepts (
  id          text PRIMARY KEY,
  status      text NOT NULL DEFAULT 'candidate',  -- candidate | selected | rejected | produced
  source_id   text REFERENCES dve.content_sources(id),
  franchise   text NOT NULL,
  destination text,
  working_title text,
  confidence_score double precision,
  editorial_score  double precision,
  originality_score double precision,
  predicted_performance_score double precision,
  payload     jsonb NOT NULL,
  created_by  text,
  created_at  timestamptz NOT NULL DEFAULT now(),
  updated_at  timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS concepts_source_idx ON dve.concepts (source_id, status);

CREATE TABLE IF NOT EXISTS dve.hooks (
  id          text PRIMARY KEY,
  status      text NOT NULL DEFAULT 'candidate',
  concept_id  text REFERENCES dve.concepts(id),
  category    text NOT NULL,   -- curiosity | warning | surprise | price | comparison | question | contrarian | luxury | secret | urgency | list | personal
  text        text NOT NULL,
  payload     jsonb NOT NULL,
  created_by  text,
  created_at  timestamptz NOT NULL DEFAULT now(),
  updated_at  timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS hooks_concept_idx ON dve.hooks (concept_id);
CREATE INDEX IF NOT EXISTS hooks_category_idx ON dve.hooks (category);

CREATE TABLE IF NOT EXISTS dve.scripts (
  id          text PRIMARY KEY,
  status      text NOT NULL DEFAULT 'draft',   -- draft | approved | rejected
  concept_id  text REFERENCES dve.concepts(id),
  hook_id     text REFERENCES dve.hooks(id),
  payload     jsonb NOT NULL,   -- voiceover, onscreen_text, shot_list, captions, music_mood, visual_direction, cta, citations, fact_check_notes
  created_by  text,
  created_at  timestamptz NOT NULL DEFAULT now(),
  updated_at  timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS dve.assets (
  id          text PRIMARY KEY,
  status      text NOT NULL DEFAULT 'active',
  asset_type  text NOT NULL,   -- video | image | map | graphic | fare_card | logo_animation
  destination text,
  country     text,
  orientation text,            -- vertical | horizontal | square
  source_tier text,            -- owned | contributor | licensed_stock | partner | generative
  license_type text,
  license_end date,
  rights_verified boolean NOT NULL DEFAULT false,
  ai_generated boolean NOT NULL DEFAULT false,
  usage_count integer NOT NULL DEFAULT 0,
  quality_score double precision,
  payload     jsonb NOT NULL,
  created_by  text,
  created_at  timestamptz NOT NULL DEFAULT now(),
  updated_at  timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS assets_dest_idx ON dve.assets (destination, asset_type) WHERE rights_verified;

CREATE TABLE IF NOT EXISTS dve.fare_deals (
  id          text PRIMARY KEY,
  status      text NOT NULL DEFAULT 'new',   -- new | scored | produced | expired | dismissed
  origin      text NOT NULL,
  destination text NOT NULL,
  fare        double precision NOT NULL,
  currency    text NOT NULL DEFAULT 'USD',
  cabin       text NOT NULL DEFAULT 'economy',
  discount_percentage double precision,
  interest_score double precision,
  expiration  timestamptz,
  payload     jsonb NOT NULL,
  created_by  text,
  created_at  timestamptz NOT NULL DEFAULT now(),
  updated_at  timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS fare_deals_status_idx ON dve.fare_deals (status, interest_score DESC);

CREATE TABLE IF NOT EXISTS dve.videos (
  id          text PRIMARY KEY,
  status      text NOT NULL DEFAULT 'draft', -- draft | rendering | needs_review | approved | rejected | scheduled | published | failed | archived
  concept_id  text REFERENCES dve.concepts(id),
  script_id   text REFERENCES dve.scripts(id),
  hook_id     text REFERENCES dve.hooks(id),
  franchise   text,
  destination text,
  format      text NOT NULL DEFAULT 'short', -- short | longform
  duration_seconds double precision,
  voice_id    text,
  qa_result   text,                          -- pass | warning | fail
  predicted_score double precision,
  video_score double precision,
  payload     jsonb NOT NULL,
  created_by  text,
  created_at  timestamptz NOT NULL DEFAULT now(),
  updated_at  timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS videos_status_idx ON dve.videos (status, created_at DESC);
CREATE INDEX IF NOT EXISTS videos_franchise_idx ON dve.videos (franchise, destination);

CREATE TABLE IF NOT EXISTS dve.render_jobs (
  id          text PRIMARY KEY,
  status      text NOT NULL DEFAULT 'queued', -- queued | running | done | failed | blocked
  video_id    text REFERENCES dve.videos(id),
  payload     jsonb NOT NULL,
  created_by  text,
  created_at  timestamptz NOT NULL DEFAULT now(),
  updated_at  timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS dve.qa_results (
  id          text PRIMARY KEY,
  status      text NOT NULL DEFAULT 'final',
  video_id    text REFERENCES dve.videos(id),
  verdict     text NOT NULL,   -- pass | warning | fail
  payload     jsonb NOT NULL,  -- per-check results
  created_by  text,
  created_at  timestamptz NOT NULL DEFAULT now(),
  updated_at  timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS qa_results_video_idx ON dve.qa_results (video_id, created_at DESC);

CREATE TABLE IF NOT EXISTS dve.approval_events (
  id          text PRIMARY KEY,
  status      text NOT NULL DEFAULT 'final',
  video_id    text REFERENCES dve.videos(id),
  action      text NOT NULL,   -- approve | reject | edit | regenerate | schedule | publish
  actor       text,
  payload     jsonb NOT NULL,
  created_by  text,
  created_at  timestamptz NOT NULL DEFAULT now(),
  updated_at  timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS approval_events_video_idx ON dve.approval_events (video_id, created_at DESC);

CREATE TABLE IF NOT EXISTS dve.publishing_jobs (
  id          text PRIMARY KEY,
  status      text NOT NULL DEFAULT 'queued', -- queued | dry_run | published | failed
  video_id    text REFERENCES dve.videos(id),
  platform    text NOT NULL DEFAULT 'youtube',
  external_id text,                           -- youtube_video_id etc.
  publish_time timestamptz,
  payload     jsonb NOT NULL,                 -- per-platform title/description/tags/thumbnail/visibility/playlist
  created_by  text,
  created_at  timestamptz NOT NULL DEFAULT now(),
  updated_at  timestamptz NOT NULL DEFAULT now()
);
CREATE UNIQUE INDEX IF NOT EXISTS publishing_jobs_dedupe
  ON dve.publishing_jobs (video_id, platform)
  WHERE status IN ('queued', 'published');
CREATE INDEX IF NOT EXISTS publishing_jobs_ext_idx ON dve.publishing_jobs (platform, external_id);

CREATE TABLE IF NOT EXISTS dve.performance_snapshots (
  id          text PRIMARY KEY,
  status      text NOT NULL DEFAULT 'final',
  video_id    text REFERENCES dve.videos(id),
  platform    text NOT NULL DEFAULT 'youtube',
  snapshot_label text NOT NULL,  -- 1h | 6h | 24h | 72h | 7d | 30d
  payload     jsonb NOT NULL,    -- full metric set (§18)
  created_by  text,
  created_at  timestamptz NOT NULL DEFAULT now(),
  updated_at  timestamptz NOT NULL DEFAULT now()
);
CREATE UNIQUE INDEX IF NOT EXISTS perf_snapshot_dedupe
  ON dve.performance_snapshots (video_id, platform, snapshot_label);

CREATE TABLE IF NOT EXISTS dve.comments (
  id          text PRIMARY KEY,
  status      text NOT NULL DEFAULT 'new',   -- new | classified | actioned | spam
  video_id    text REFERENCES dve.videos(id),
  classification text,   -- content_request | question | destination_request | fare_request | positive | negative | complaint | correction | spam
  payload     jsonb NOT NULL,
  created_by  text,
  created_at  timestamptz NOT NULL DEFAULT now(),
  updated_at  timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS comments_class_idx ON dve.comments (classification, status);

CREATE TABLE IF NOT EXISTS dve.recommendations (
  id          text PRIMARY KEY,
  status      text NOT NULL DEFAULT 'open',  -- open | accepted | rejected | expired
  kind        text NOT NULL,                 -- produce_more | stop_producing | hook_style | length | format | longform_expansion | viewer_requested
  payload     jsonb NOT NULL,                -- finding, evidence, suggested action
  created_by  text,
  created_at  timestamptz NOT NULL DEFAULT now(),
  updated_at  timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS dve.experiments (
  id          text PRIMARY KEY,
  status      text NOT NULL DEFAULT 'running', -- running | concluded | abandoned
  variable    text NOT NULL,
  payload     jsonb NOT NULL,
  created_by  text,
  created_at  timestamptz NOT NULL DEFAULT now(),
  updated_at  timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS dve.voices (
  id          text PRIMARY KEY,
  status      text NOT NULL DEFAULT 'active',
  name        text NOT NULL,
  provider    text NOT NULL,   -- elevenlabs | human | silent
  payload     jsonb NOT NULL,  -- style, accent, tone, speed, best_content_types
  created_by  text,
  created_at  timestamptz NOT NULL DEFAULT now(),
  updated_at  timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS dve.attribution_events (
  id          text PRIMARY KEY,
  status      text NOT NULL DEFAULT 'final',
  video_id    text REFERENCES dve.videos(id),
  event_type  text NOT NULL,   -- session | newsletter_signup | flight_search | trip_planner | hotel_click | flight_click | affiliate_click | conversion
  payload     jsonb NOT NULL,
  created_by  text,
  created_at  timestamptz NOT NULL DEFAULT now(),
  updated_at  timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS attribution_video_idx ON dve.attribution_events (video_id, event_type);

CREATE TABLE IF NOT EXISTS dve.settings_audit (
  id          text PRIMARY KEY,
  status      text NOT NULL DEFAULT 'final',
  actor       text,
  payload     jsonb NOT NULL,  -- {key, old, new}
  created_by  text,
  created_at  timestamptz NOT NULL DEFAULT now(),
  updated_at  timestamptz NOT NULL DEFAULT now()
);

-- updated_at triggers for every dve table.
DO $$
DECLARE t text;
BEGIN
  FOR t IN
    SELECT tablename FROM pg_tables WHERE schemaname = 'dve'
  LOOP
    EXECUTE format(
      'DROP TRIGGER IF EXISTS touch_updated_at ON dve.%I;
       CREATE TRIGGER touch_updated_at BEFORE UPDATE ON dve.%I
       FOR EACH ROW EXECUTE FUNCTION dve.touch_updated_at();',
      t, t);
  END LOOP;
END $$;
