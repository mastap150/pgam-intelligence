# Destination.com Video Engine (DVE) — System Architecture

**2026-09-05.** The architecture document the video-automation spec (§37)
requires before large-scale implementation. Code lives in `video/`, schema in
`migrations/2026_09_05_destination_video_engine.sql`, scheduler wiring in
`scheduler.py` behind `PGAM_DVE_ENABLED`.

## What this is

An end-to-end automated video content system for Destination.com: YouTube
Shorts first, long-form weekly, cross-platform later. Not a generic AI-video
factory — the objective is premium, editorial, recognizably branded travel
content, with a closed learning loop:

```
Destination.com content / fares / news / audience signals
  → content intelligence (opportunity scoring)
  → concepts → hooks → scripts → assets → rendered video
  → QA → human approval → YouTube publish
  → analytics snapshots → scoring → learning → recommendations
  → better content tomorrow
```

## Stack audit (spec §37) — what already exists and what we reuse

Audited from this repo and `docs/destination-integration-assessment-2026-08-26.md`:

| Component | Finding | Decision |
|---|---|---|
| Destination.com site | `destination-com` repo, Next.js 16 on Vercel; 732 guide pages, `/news/[slug]`, `/flights/[route]` routes | Ingest editorial via public sitemap/HTML + the markdown corpus already in `content/destination/`. Site-side changes (UTM landing, attribution events) are a separate `destination-com` PR later. |
| Worker infrastructure | This repo: Render worker (`scheduler.py`), agents pattern, env-gated jobs, self-deduplicating runs | DVE jobs register in `scheduler.py` behind `PGAM_DVE_ENABLED=1`, same `_run`/`_import` pattern. |
| Database | Neon Postgres, `pgam_direct` schema, `core/neon.py` helper, SQL migrations in `migrations/` | New **`dve` schema** in the same Neon project. Own DSN override (`DVE_DATABASE_URL`) falling back to `PGAM_DIRECT_DATABASE_URL`. |
| LLM | `anthropic` SDK already a dependency; `intelligence/claude_analyst.py` pattern | `video/llm.py` follows it; model configurable via `DVE_MODEL`. |
| Analytics | GA4 digest via GitHub Actions WIF; `google-api-python-client` already a dependency | YouTube Data + Analytics APIs reuse the installed Google client libs. Website attribution reads GA4 later; MVP generates UTM links now. |
| Newsletter / flight search | Live on destination-com (`NewsletterForm`, `FlightsFromAnywhere`, `/flights/[route]`) | CTAs deep-link to these with UTM params. Fare data: no first-party fare API is exposed to this repo yet, so `fare_deals` accepts manual/CSV/API-pushed deals; the interface is ready for the destination-com fare system. |
| Media storage | None in this repo | Filesystem asset library under `DVE_DATA_DIR` with rights metadata; `file_url` supports http(s) or local paths so S3/R2 can slot in without schema change. |
| Rendering | Nothing existing | FFmpeg (pinned command builder, no new Python deps). Modular timeline → filtergraph; Remotion can replace the renderer behind the same `RenderJob` contract later. |
| Voice | Nothing existing | Provider interface: ElevenLabs HTTP (`ELEVENLABS_API_KEY`) + silent/stub provider for tests and previews. |
| Social publishing | Nothing existing | YouTube Data API v3 (OAuth refresh token). Platform-neutral `videos` model with per-platform `publishing_jobs` so Reels/TikTok/X/Pinterest add as new publishers, not schema changes. |

## Safety posture (defaults)

Following this repo's TBX convention (writes gated twice):

- **Publishing mode defaults to MANUAL** (§32). Every video requires human
  approval. `AUTO` can only be enabled per-franchise in settings, and never
  for `travel_news`.
- **`DVE_ALLOW_PUBLISH=1`** required at the environment level before any
  YouTube upload happens at all; without it every publish is a dry run that
  logs what it would have done.
- **QA gate**: `FAIL` cannot be approved or published; `WARNING` requires a
  human. QA history is stored per video version.
- **Rights gate**: assets with `rights_verified = false` or expired licenses
  are never retrievable by the asset matcher.
- **Editorial safety** (§30) is enforced in QA: fares carry "Fare found at
  time of publishing", news preserves source + publish date, no invented
  facts pass fact-check annotation review.
- **AI disclosure** (§31): every video tracks ai_voice / ai_visuals /
  synthetic_scenes and a disclosure status the publisher maps to YouTube's
  altered-content flag.

## Module map

```
video/
  settings.py     env + admin settings (§33), configurable weights (§4, §19)
  models.py       dataclasses for every entity (§26)
  store.py        storage: JSON file store (default, zero-infra) or Postgres
  db.py           Neon connection for the Postgres store
  llm.py          Claude wrapper (JSON-mode helpers, offline deterministic fallback)
  theme.py        brand design system tokens loaded from config/video_theme.json (§10)
  ingestion.py    content-source normalization: markdown corpus, sitemap/URL, fares (§3)
  opportunity.py  Content Opportunity Score, reasons, manual priority (§4)
  concepts.py     ≥5 concepts per source, franchise-aware, near-dup filter (§5)
  hooks.py        hook generation + 12 hook categories + per-category rollup (§6)
  script.py       short-form script engine: VO, on-screen text, shot list, captions (§7)
  assets.py       asset library, rights enforcement, tiered matching, diversity (§8, §9)
  fatigue.py      recent-frequency diversity thresholds (§22)
  fare_drop.py    fare interest scoring + candidate Fare Drop videos (§14)
  voice.py        narration providers + voice metadata (§12)
  presenter.py    presenter-recording → clip suggestions workflow (§13, scaffold)
  render.py       timeline → FFmpeg filtergraph → 1080×1920 H.264/AAC (§11)
  qa.py           QA agent: PASS/WARNING/FAIL with per-check results (§15)
  youtube.py      upload/metadata/analytics/comments; publish gates (§17)
  analytics.py    snapshot cadence 1h/6h/24h/72h/7d/30d (§18)
  scoring.py      Destination Video Score + dimension scores (§19)
  learning.py     daily learning → recommendations, accept/reject (§20)
  comments.py     comment classification, topics, viewer-requested queue (§21)
  attribution.py  UTM link builder + conversion joins (§24)
  experiments.py  one/two-variable experiments with confidence (§27)
  longform.py     long-form expansion triggers + outline scaffold (§28)
  pipeline.py     the §36 MVP loop, resumable stage by stage
  dashboard.py    approval dashboard: stdlib HTTP, tabs, actions, batch approve (§16, §25)
```

### Storage: two backends, one contract

The cloud sessions that develop this system cannot reach Neon on port 5432
(measured, see CLAUDE.md), and the MVP must run end-to-end on a laptop with
no infra. So `video/store.py` defines one `Store` contract with two backends:

- **`JsonFileStore`** (default): one JSON file per record under
  `DVE_DATA_DIR/store/<table>/<id>.json`. Zero-infra, git-inspectable,
  fine for MVP volumes (≤ a few thousand records).
- **`PostgresStore`**: the `dve` schema. Each table carries the
  indexed/queried scalars as real columns (ids, status, franchise,
  destination, scores, timestamps) plus the full record as `payload JSONB`.
  This is deliberate: the entity shapes are still evolving weekly, and the
  scalar columns cover every query the dashboard and learning engine run.
  Columns get promoted out of the payload when a query needs them, by
  migration.

`DVE_STORE=postgres` switches backends; everything above the store is
identical.

### Rendering

`render.py` builds a declarative `Timeline` (segments with asset refs, text
overlays from theme components, caption track, VO + music audio) and compiles
it to a single FFmpeg invocation:

- 1080×1920, 9:16, H.264 (`-profile high -crf 21` capped bitrate), AAC 192k.
- Captions burned via libass (`.ass` styled from the theme) so brand
  typography is enforced centrally.
- Brand components (§10) — logo, location label, fare card, price card,
  lower third, CTA, end card — are theme-driven drawtext/overlay builders,
  not hard-coded per template. New layouts = new component fns + theme keys.
- If `ffmpeg` is absent the render job records `blocked: ffmpeg missing`
  rather than crashing the scheduler (repo convention: one failing agent
  never kills the process).

Remotion is the likely long-term upgrade for motion design (maps, animated
route graphics); it slots in as an alternative `Renderer` on the same
`RenderJob` contract, which is why the timeline is data, not code.

### The MVP loop (§36)

`video/pipeline.py::run_mvp(article_path_or_url)`:

1. ingest article → content_source
2. opportunity-score it
3. generate ≥5 concepts, score, dedupe
4. select best concept
5. generate 3 hooks
6. generate script (VO / on-screen / shot list / captions / music / CTA / citations / fact-check notes)
7. match assets (rights-verified only, tiered, diversity-penalized)
8. synthesize voiceover
9. assemble timeline + brand components, render vertical video
10. run QA → PASS/WARNING/FAIL
11. queue for approval (dashboard)
12. on approval: publish to YouTube (gated), store youtube_video_id
13. analytics snapshots on cadence → scoring → learning recommendations

Each stage persists its output before the next runs, so a failed render
doesn't lose the script, and any stage can be regenerated from the dashboard
(change hook / voice / footage / title per §16).

## Scheduler jobs (all behind `PGAM_DVE_ENABLED=1`)

| Job | Cadence | What |
|---|---|---|
| `dve_ingestion` | every 6h | pull new/updated sources, normalize, freshness-score |
| `dve_opportunity` | daily 06:00 ET | score the source pool, emit ranked opportunities |
| `dve_production` | every 2h | produce candidate videos up to the daily target, respecting fatigue thresholds |
| `dve_analytics` | hourly | due performance snapshots (1h/6h/24h/72h/7d/30d) |
| `dve_comments` | every 6h | ingest + classify comments, update demand signals |
| `dve_learning` | daily 07:00 ET | compute scores, segment lifts, write recommendations |

The dashboard runs as its own process (`python -m video.dashboard`) — a
Render web service or local — never inside the worker.

## Environment variables

All optional until the corresponding feature is enabled; none are ever
committed. See `.env.example` for the authoritative list.

| Var | Purpose |
|---|---|
| `PGAM_DVE_ENABLED` | register DVE jobs in scheduler.py |
| `DVE_DATA_DIR` | root for store/assets/renders (default `data/video`) |
| `DVE_STORE` | `json` (default) or `postgres` |
| `DVE_DATABASE_URL` | Neon DSN for the `dve` schema (falls back to `PGAM_DIRECT_DATABASE_URL`) |
| `DVE_MODEL` | Claude model for generation (default `claude-sonnet-5`) |
| `ANTHROPIC_API_KEY` | already a repo-wide var; reused |
| `ELEVENLABS_API_KEY` | synthetic narration provider |
| `DVE_ALLOW_PUBLISH` | hard gate: without `1`, YouTube publishes are dry-run |
| `YT_CLIENT_ID` / `YT_CLIENT_SECRET` / `YT_REFRESH_TOKEN` | YouTube OAuth (upload + analytics scopes) |
| `DVE_DASHBOARD_TOKEN` | bearer token the approval dashboard requires |

## Failure handling, logs, retries

- Every module logs through `video.settings.log()` (stdout, the Render
  convention here) with the `[dve:<module>]` prefix.
- External calls (LLM, ElevenLabs, YouTube) retry ×3 with exponential
  backoff on 429/5xx; anything else surfaces immediately.
- Render jobs and publishing jobs are explicit records with status
  transitions (`queued → running → done|failed|blocked`) so a crashed worker
  resumes rather than re-renders.
- The LLM layer has a **deterministic offline fallback** used automatically
  when `ANTHROPIC_API_KEY` is unset — this is what makes the whole pipeline
  unit-testable in CI and in cloud sessions with no credentials. Fallback
  output is clearly marked (`generator: "offline"`) and QA warns on it so it
  can never silently ship.

## 90-day rollout mapping (§34)

- **Phase 1 (now, this PR):** schema, ingestion, concepts, hooks, scripts,
  asset library + rights, theme, renderer, QA, approval dashboard, YouTube
  upload (gated), MVP loop. Publishing MANUAL.
- **Phase 2:** run 3 Shorts/day through the approval queue; hook/length/
  voice variation via `experiments.py`.
- **Phase 3:** analytics ingestion live, scoring + learning + comment
  intelligence on schedule; Fare Drop automated once a fare feed lands;
  presenter workflow.
- **Phase 4:** scale winning franchises; ASSISTED/AUTO per franchise with QA
  history; cross-platform publishers.

## What is deliberately not in this PR

- Instagram/TikTok/X/Facebook/Pinterest publishers (model is platform-neutral;
  publishers are additive).
- destination-com site changes (attribution event capture, fare feed
  endpoint) — separate repo, separate PR.
- Remotion motion-design renderer (FFmpeg contract ready for it).
- Thumbnail generation beyond concept records (§29 scaffolded in longform).
