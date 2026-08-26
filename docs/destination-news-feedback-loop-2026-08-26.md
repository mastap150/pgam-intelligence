# Destination.com news — closing the loop, on the BoxingNews/MSN pattern

**2026-08-26.** Agreed: news needs the same closed loop BoxingNews runs on MSN
— measured per-article performance, feeding back into what gets produced next,
with named human bylines rather than a collective masthead.

This is not a new design. It is a port. BoxingNews already runs the whole
thing, it works, and the parts that make it work are specific and easy to drop
on the floor. What follows maps each one.

---

## 1. What BoxingNews actually does

Five components. The order matters, and component 4 is the one everyone skips.

**1 — Snapshot, continuously.** A puller takes `read_count` per doc per 15-min
tick into `pgam_direct.msn_article_snapshots`. Peak performance is then
`MAX(read_count)` per doc per day (`msn_daily_totals`, `msn_article_peak`).
Not a single reading — a time series, so an article can be judged after it has
aged.

**2 — Segment by things you can act on.**
`agents/insights/boxingnews_weekly_review.py` pulls the prior 7 days and cuts it
by topic, headline pattern (P1–P6 from the tuner taxonomy), origin lane
(breaking vs trending vs programmatic), origin source (which subreddit, which
handle, which feed), day-of-week, and time-since-event. Every one of those is a
lever someone can pull next week.

**3 — Ask a high-judgment model for two artifacts, not one.** Opus produces
`report_md` (the human briefing, emailed) **and** `strategy` (machine-readable
JSON). One row per week UPSERTed into `pgam_direct.msn_weekly_review`.

**4 — The read-back. This is the loop.** The BoxingNews repo's headline-tuner
reads `strategy.winning_patterns`, `hot_topics`, `hot_fighters` and
`avoid_phrases` **on every cron tick**, via `src/lib/msn/strategy.ts`. That
single read is the difference between a loop and a dashboard. Without it you
have a weekly email nobody acts on.

**5 — A cohort trigger with a verdict, not a chart.**
`scripts/hasib_trigger_check.py` compares the AI-byline cohort against the human
writer's cohort weekly and prints one of `KEEP_5` / `CUT_TO_3` / `CUT_TO_2`
against explicit thresholds — AI avg reads-per-ingested-article ≥ 90 for **two
consecutive weeks** AND weekly est. revenue ≥ $350. It does not auto-execute.

**Safety posture worth copying verbatim:** the agent never writes to the content
repo's database. It writes only to its own table. If it crashes or emits a bad
strategy, the tuner reads an empty block and falls back to the static prompt —
no regression versus having no loop at all.

### Two findings from their data that change our plan

**Performance is brutally power-law.** In any 7-day window the top 5 articles
drive ~50% of all reads and the bottom 90% earn under $1 combined. That is the
economic case for the throttle, measured rather than asserted: at ~55/day
(§3 below) we are manufacturing the bottom 90%.

**The metric is per-*ingested* article, not per-published.** Average peak PVs
across articles that actually got distributed. Total views rewards volume;
per-ingested rewards quality, and it is the number the trigger fires on.

---

## 2. Humanizing — already solved next door

BoxingNews assigns **named rotation bylines** via `pickAuthorForContent`: Tom
Rashid, Aaron Clarke, Dan O'Keefe, James Wright, Priya Shah, Sarah Mitchell.
Those are the AI cohort, and because they are distinct bylines the weekly review
can measure them *as a cohort* against a named human's.

Destination.com publishes everything under one collective record —
`src/data/authors.ts` resolves **every** byline to "destination.com editorial",
with empty `expertise`, no `sameAs`, no image. Three consequences:

1. **Nothing to measure.** With one byline there are no cohorts, so the
   Hasib-style trigger has nothing to compare and the loop cannot answer "is the
   human worth it?" — the exact question it exists to answer.
2. **A ceiling on distribution.** Google News, NewsBreak's publisher standards
   and AI citation all weight named, credentialed, verifiable humans.
3. **A disclosure that runs ahead of reality.** The template says "written and
   edited by the destination.com newsroom" on articles no human has touched.

**Port the pattern:** named bylines per franchise (aviation, entry-rules,
deals), real `/authors/[slug]` pages with populated `knowsAbout` and `sameAs`,
and the reviewing editor bylined on what they actually reviewed. Then the byline
becomes a measurable cohort *and* a trust signal, the way it is at BoxingNews.

Humanizing is not only the byline. It is also the **Destination layer** from the
content strategy — what changed, who's affected, what to do next — which is the
part no rewrite pipeline can produce and the only thing that survives Google's
scaled-content policy. The byline makes it attributable; the layer makes it real.

---

## 3. What Destination.com has, and what is missing

| Component | BoxingNews | Destination.com |
|---|---|---|
| Continuous snapshot | MSN puller → `msn_article_snapshots` | ❌ nothing for `/news/` |
| Per-article peak | `msn_article_peak`, `msn_daily_totals` | ❌ |
| Distribution measurement | MSN Partner Hub | ⚠️ GA4 referral only — no NewsBreak account (audit §3) |
| Search measurement | — | ✅ **GSC access already works** (`scripts/gsc-guides-queue.mjs`, `GSC_ACCESS_TOKEN` / `GOOGLE_APPLICATION_CREDENTIALS_JSON`) |
| Weekly segmentation | `boxingnews_weekly_review.py` | ⚠️ `news-editorial-review` reports **production** stats — volume, hero coverage, body length. Inputs, not outcomes. |
| Strategy JSON | `msn_weekly_review.strategy` | ❌ |
| **Read-back into production** | `src/lib/msn/strategy.ts`, every tick | ❌ **`news-opportunity.ts` weights are hardcoded** |
| Cohort trigger + verdict | `hasib_trigger_check.py` | ❌ (and no cohorts to compare — §2) |
| Named bylines | `pickAuthorForContent`, 6 personas | ❌ one collective record |

So: the measurement input is half-solved (GSC works), the segmentation exists
but measures the wrong things, and **the read-back — the part that makes it a
loop — does not exist at all.**

---

## 4. The port, in dependency order

Each step is useless without the one before it. Do not build the dashboard first.

**Step 1 — Snapshot per-article performance daily.** New
`news_performance(slug, as_of, source, impressions, clicks, position, sessions,
engaged_sessions, engagement_time, signups, affiliate_clicks, revenue_usd)`,
PK `(slug, as_of, source)`. Feeders: GSC (reuse the working
`gsc-guides-queue.mjs` auth path, filter to `/news/`), GA4 (sessions,
engagement, and the `newsbreak.com` referral cut), newsletter click log,
affiliate pulls. Daily cron, idempotent UPSERT.

**Step 2 — Let articles age before judging them.** BoxingNews found peak reads
under-count for articles <5 days old and their trigger only reads *completed*
weeks. Same rule here, or every fresh story looks like a failure.

**Step 3 — Segment by levers we can actually pull.** Franchise (aviation /
entry-rules / deals / hotels), **source tier (primary vs secondary — the audit's
central variable)**, headline shape (the disruption / large-geography /
countable-specific signals already computed in `news-opportunity.ts`), publish
hour, day of week, byline, and whether the Destination layer was present.

**Step 4 — Weekly review emitting `report_md` + `strategy` JSON.** Model on
`boxingnews_weekly_review.py` down to the safety posture: write only to our own
table, never to `news_items`.

**Step 5 — The read-back, which is the whole point.**
`news-opportunity.ts` currently hardcodes its weights — the geography weight,
the disruption weight, the `-10` big-stat penalty, the tier bonus. Those should
be *defaults*, overridden by the strategy blob on every cron tick, exactly as
`src/lib/msn/strategy.ts` does. Same fallback: no strategy → static defaults →
no regression versus today. Only after this is the loop closed.

**Step 6 — A trigger with a verdict.** Once bylines create cohorts (§2), the
Hasib-shaped question becomes answerable for us: does a primary-sourced,
human-layered article out-earn an unattended rewrite by enough to justify the
editorial minutes? Thresholds agreed in advance, two consecutive weeks required,
monitor-only — never auto-execute.

---

## 5. What this changes about cadence

The audit recommended throttling on a policy-risk argument. BoxingNews's
power-law finding turns that into an economic one: if the top 5 articles carry
~50% of reads and the bottom 90% earn nothing, then **~55 articles/day is
mostly manufacturing the bottom 90%** — at full model cost, full review debt and
full scaled-content exposure, for close to zero return.

The loop is also what makes the throttle *safe to reverse*. Right now a cadence
change is a guess, because nothing measures the result. Once per-article
performance is tracked by cohort, cutting to 3/day becomes a measurable
experiment — exactly the before/after window comparison
`msn_lane_performance.py` already does for the BoxingNews optimization bundle
(formulaic-cron kill + strategy-bias wiring + publish-time concentration +
daily cap). Note that bundle: **they killed a formulaic cron and concentrated
publishing into a 90-minute window.** We have the formulaic cron.

---

## 6. Sequencing against the rest of the plan

1. **Throttle first** (`MAX_CLUSTERS_PER_RUN` 5 → 1–2). Independent of the loop,
   reversible, stops the bleeding while the loop gets built.
2. **Named bylines + author pages.** Cheap, and a precondition for cohorts.
3. **Steps 1–3** — snapshot, ageing rule, segmentation. This is the real work.
4. **Steps 4–5** — weekly review and the read-back. The loop closes here.
5. **Step 6** — trigger thresholds, once there is enough history to set them
   honestly rather than by guess.

**The one thing not to do:** build the dashboard and stop. BoxingNews's loop
works because `strategy.ts` reads the blob on every tick. A weekly email with no
read-back is where most of these die.
