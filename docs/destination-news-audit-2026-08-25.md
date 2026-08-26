# Destination.com News Strategy Audit — 2026-08-25

Audit of the `/news/` lane on destination.com: what it actually is, why the
Breeze Airways story took ~99% of NewsBreak traffic, and whether news should
become a materially larger share of the content strategy.

**Source of evidence:** `mastap150/destination-com` @ `e98b829` (read-only clone).
**Analyst note on limits:** this session has no `DATABASE_URL` and no Search
Console credentials, and there is no NewsBreak account to read analytics from —
NewsBreak picks the content up unmanaged (§3). Everything below is split into
**Proven** (read from code) and **Unresolved** (needs the queries in
`destination-news-audit-queries.sql`, or access nobody currently holds). Nothing
is inferred and then presented as measured.

---

## 0. The one-paragraph answer

**No — do not significantly increase news publishing output yet.** The volume
lever is already open and is not the binding constraint. The `/news/` lane is
an unattended AI rewrite pipeline: it polls nine secondary blogs every two
hours, has Claude paraphrase them into 350–500 words, and publishes straight
to `status = 'live'` with **no human review gate**, a **collective byline**, a
**hotlinked hero image scraped from the source publisher**, and **`Article`
schema instead of `NewsArticle`**. It is wired to two feeds and a sitemap, and
to essentially nothing else — not the newsletter cron, not X, not Pinterest,
not push. Publishing more of this would multiply a compliance exposure and a
Google-churn risk, not an acquisition channel. The Breeze result says the
*format* can work; it does not say the *pipeline* is ready to scale. Fix the
eight technical defects in §7, add primary sources and a review gate, then
scale volume deliberately.

---

## 1. Current news production

### Proven — the pipeline's designed shape

| Property | Value | Evidence |
|---|---|---|
| Trigger | Vercel cron, every 2 hours (12×/day) | `vercel.json` → `/api/cron/news-ingest`, `0 */2 * * *` |
| Cap per run | 5 clusters (hard max 10 via `?max=`) | `news-ingest.ts` `MAX_CLUSTERS_PER_RUN = 5` |
| Theoretical ceiling | **60 articles/day** | 12 runs × 5 |
| Self-declared expectation | **"3-5 items every 2h"** = 36–60/day | `news-ingest-health/route.ts` alert body |
| Freshness window | source items from last 24h only | `FRESH_WINDOW_MS` |
| Dedup | 3 layers: URL stem, leading-brand + shared token, Jaccard ≥ 0.35 over 7 days | `filterAlreadyCovered()` |
| Length | 350–500 words, 4–6 paragraphs | `SYSTEM_PROMPT` |
| Model | `claude-sonnet-4-5` (`NEWS_MODEL` override) | `news-ingest.ts` |
| Human gate | **none** — `status TEXT NOT NULL DEFAULT 'live'` | `scripts/schema.sql:937` |
| Weekly review | retrospective email only, **not** a gate | `news-editorial-review/route.ts` |

The dedup stack is genuinely well-built — three independent layers with
documented tuning against observed false negatives. It is the strongest part of
the system. In practice it is also what keeps real volume far below the 60/day
ceiling: nine feeds covering the same trade beat produce heavily overlapping
clusters, and a 7-day Jaccard window at 0.35 is aggressive.

### Proven — how old the lane is

Three independent code comments and the row IDs they cite put the pipeline's
first real run at **2026-07-28**:

- `news-sources.ts`: feeds "verified reachable from Vercel egress IPs (2026-07-28)"
- `news-ingest.ts`: "Observed 2026-07-28: ... (id 13) vs ... (id 22)"
- `backfill-news-heroes.mjs`: "We fixed the ingest path 2026-07-28"; pre-fix rows "~22"

Single-digit and low-double-digit row IDs on 2026-07-28 mean the table was
near-empty then. **The news lane is ~4 weeks old as of this audit.**

> ⚠️ Conflict to resolve: `scripts/send-briefing.mjs` claims "The news-ingest
> cron has been running for months." That contradicts the row-ID evidence.
> Query A settles it. I have gone with ~4 weeks because it is corroborated
> three ways.

**Consequence for the brief:** a "last 90 days" breakdown does not exist. Roughly
the first two-thirds of that window predate the pipeline. 7- and 30-day windows
are meaningful; 90-day is not.

### Unresolved — the exact counts you asked for

Articles are rows in a Neon `news_items` table, not files in the repo, so
per-day/per-week/per-month counts **cannot be derived from source**. Run
`destination-news-audit-queries.sql` (Queries A–F) against `DATABASE_URL` and
the entire §1 table below fills in one pass:

- articles/day, /week, /month for 7/30/90d
- category and region distribution
- publish-hour and day-of-week histogram (consistency vs. sporadic)
- source-outlet attribution counts
- hero-image coverage %
- median body length
- kill rate (`status != 'live'`)

### Proven — the answers that don't need the DB

**What categories?** Six, fixed in code: `aviation`, `visa-policy`,
`destination-news`, `industry`, `transit`, `hospitality`
(`news-sources.ts`). Note what is **absent** and that you asked about: no
points/miles category, no cruise category, no deals category, no hotel-loyalty
category. The taxonomy cannot express half of §6's target beats.

**How fast after a story breaks?** Bounded by the 2-hour cron and a 24h source
window, so **0–2h after a source blog files** — but that is 0–2h behind
*Simple Flying*, which is itself hours behind the airline. True latency from the
airline's own announcement is more like **4–12h**. You are structurally second.

**Where do stories come from?** Nine RSS feeds, **all secondary**:

| Source | Category | Type |
|---|---|---|
| Simple Flying | aviation | aggregator blog |
| One Mile at a Time | aviation | blog |
| View from the Wing | aviation | blog |
| Live and Lets Fly | aviation | blog |
| Skift | industry | trade press |
| PhocusWire | industry | trade press |
| Travel Weekly | industry | trade press |
| CNN Travel | destination-news | consumer press |
| Hospitality Net | hospitality | trade press |

**Zero primary sources.** No airline newsroom, no airport, no DOT/FAA/CBP/State
Department, no tourism board, no hotel or loyalty program, no cruise line.

**Original reporting vs. rewrite?** **0% original, 100% rewrite.** The system
prompt's own framing is "take multiple published reports ... and synthesize."
The pipeline has no mechanism to originate a fact. Four of nine sources are
themselves aviation blogs, so a meaningful share of output is a *rewrite of a
rewrite*. The on-page label "This story was written and edited by the
destination.com newsroom" is doing heavy lifting for a process with no human in
it.

**Which writers/processes?** One process (`news-ingest` cron), one model,
one byline (`destination.com editorial`). No human writers.

**Consistent or sporadic?** Cron cadence is perfectly consistent. *Output* is
gated by whether nine feeds produced ≥1 non-duplicate cluster in the prior 24h —
so expect weekday-heavy, weekend-thin volume, since trade press doesn't file on
weekends. Query C confirms.

### Is current news volume sufficient?

**Volume is not the constraint.** The ceiling is 60/day and you are nowhere near
it — not because of throttling but because dedup correctly rejects near-identical
coverage of the same nine feeds. Raising the cap would not produce more stories;
it would produce more *duplicates of the same trade stories*. **The constraint
is source diversity, not publishing capacity.** Adding one airline newsroom feed
does more for volume than doubling `MAX_CLUSTERS_PER_RUN`.

---

## 2. Why the Breeze story outperformed

I cannot read either article body (DB rows) or NewsBreak's per-article
analytics. What follows is **structural analysis of the two slugs and of the
template both were rendered through** — labelled as inference, because it is.

| Dimension | Breeze (633 views, 99.4%) | Southwest Nashville (3 views, 0.5%) |
|---|---|---|
| Named brand, leading | Breeze Airways ✓ | Southwest ✓ |
| Verb | **"pauses"** — change, negative | "fastest-growing" — adjective, static |
| Reader consequence | **Your booked flight may be gone** | None |
| Geography | **Florida** — state, ~23M people, spans dozens of NewsBreak locales | Nashville — one metro, ~2M |
| Time bound | **"through early October"** — dated, actionable | none |
| Specificity | "four routes" — countable, checkable | "2,250 quarterly flights" — a trade statistic |
| Frame | Consumer service disruption | B2B network growth |
| Action implied | Check your booking, rebook | None |

**The mechanism, most to least likely:**

1. **Geographic reach.** NewsBreak's distribution is locality-keyed. "Florida"
   fans out across a large multi-metro audience; "Nashville" targets one. This
   alone could explain a large multiple before any editorial factor applies.
2. **Disruption beats expansion.** A paused route creates an affected party with
   a problem to solve today. A growth statistic creates a reader with nothing to
   do. Engagement follows consequence.
3. **Trade framing.** "2,250 quarterly flights" is a number written for an
   analyst. It signals *industry news*, and NewsBreak's audience is a general
   consumer audience, not a trade one.
4. **Dated urgency.** "through early October" gives the story a shelf life and a
   reason to read now.

**So: yes** — your hypothesis is right. Airline + Florida + route change +
immediate traveler impact is the combination. But the load-bearing element is
almost certainly **large-geography + personal consequence**, not the airline
name.

**The reproducible pattern, without clickbait:**

> `[Named operator] [verb of change] [countable specifics] [in named large geography] [through named date]`

Every slot is a fact. Nothing is withheld or teased — this is the opposite of
clickbait, it is *front-loading the news*. Concretely, prefer:

- Disruption over expansion (pauses, cancels, cuts, suspends, ends, delays)
- Large or multi-metro geographies (a state, a region, "the Southeast") over single cities
- A dated window over an open-ended one
- Countable specifics ("four routes") over aggregate statistics ("2,250 flights")
- Consumer consequence over corporate strategy

**Caveat worth stating plainly:** n=2, 636 views, one week — and the metric is GA4 referral
sessions, i.e. *click-throughs, not impressions*. It therefore cannot separate "readers
didn't click" from "NewsBreak barely distributed it" (§3). Directional signal, not a
validated model. Query G runs the same test across the whole corpus, which is where the
pattern gets confirmed or killed.

---

## 3. NewsBreak: end-to-end audit

### The headline finding

**There is no NewsBreak integration in the codebase.** The string "newsbreak"
appears exactly once in the entire repository — in `docs/seo-playbook.md`, in a
status table, reading:

> "Loose ends: MSN was submitted then dropped, not yet re-submitted;
> **Newsbreak/SmartNews/Taboola pending**"

and again in the priority list:

> "8. **MSN + Newsbreak/SmartNews/Taboola re-engagement** — distribution loops
> built, not fully re-activated."

There is no `/feed/newsbreak` route, no NewsBreak-specific formatting, no
NewsBreak API client, no analytics ingestion. The three feed routes that exist
are `/feed/msn`, `/feed/msn-news`, and `/feed/apple-news`.

**And there is no NewsBreak account either** (confirmed 2026-08-25): NewsBreak picks
up destination.com content on its own. So this is an *unmanaged scrape*, not a
publisher relationship — which resolves the "which pipe?" question and replaces it
with three sharper consequences:

1. **No analytics, no controls, no recourse.** There is no portal to read views from,
   no accept/reject visibility, no way to influence what gets picked up, and no
   agreement to appeal to if distribution stops tomorrow. The 636 views are a gift,
   not a channel.
2. **The image-rights exposure below gets materially worse, not better.** Articles
   carrying other publishers' licensed press imagery are being redistributed onto a
   third-party consumer platform, with no agreement in place on either side. That is
   a worse posture than the same content sitting only on our own domain.
3. **The feed audit still matters — just not for NewsBreak.** `/feed/msn-news`
   governs MSN today, and would govern NewsBreak *if* we applied. Fix it on MSN's
   account.

**Recommendation: apply to NewsBreak's publisher/contributor program.** It is the only
way to turn an uncontrolled scrape into a measurable channel, and the only way §11's
NewsBreak column ever gets real numbers. Do it *after* fix #1 — applying while we are
republishing other outlets' images uncredited invites a rejection that is hard to undo.

### Feed audit — `/feed/msn-news` (MSN today; NewsBreak only if we apply)

Audited against what NewsBreak's ingestion documents ask for:

| Element | Status | Detail |
|---|---|---|
| Valid RSS 2.0 + namespaces | ✅ | content, dc, atom, media all declared |
| `<title>` | ✅ | article headline |
| `<link>` / `<guid isPermaLink>` | ✅ | canonical article URL |
| `<pubDate>` RFC-822 | ✅ | `toUTCString()` — correct format |
| **Update timestamp** | ❌ | **no `<atom:updated>` or equivalent per item** |
| Full text | ✅ | `<content:encoded>` ships full `body_html` |
| Image | ⚠️ | `<media:content>` + `<media:thumbnail>` present, **but see rights issue below** |
| Image credit | ❌ | **`hero_credit` is never populated** |
| Byline | ⚠️ | `dc:creator` = "destination.com editorial" — collective, not a named person |
| Author email | ✅ | `editorial@destination.com` |
| Categories | ✅ | up to 8, from category + region + country + tags |
| Channel logo / language / copyright | ✅ | all present |
| `<atom:link rel="self">` | ✅ | present |
| Item count | 100 | no age filter — feed can serve stale items |
| Cache | 300s s-maxage | fresh enough |

**Feed freshness/errors:** the feed degrades silently — `loadRecent()` catches DB
errors and returns `[]`, producing a valid but **empty** feed rather than a 5xx.
An empty feed looks "healthy" to a poller while delivering nothing. The
`news-ingest-health` cron catches *ingest* failure but not *delivery* failure.

### The serious one: hero image rights

`persistNews()` sets `hero_image_url` from `pickOgImage()` — which scrapes the
**source publisher's `og:image`** out of their article HTML
(`news-ingest.ts`, `fetchArticleText`). That image is then:

- rendered on destination.com with `unoptimized` (hotlinked from their CDN),
- pushed into `/feed/msn-news` as `<media:content>` and inside `<content:encoded>`,
- attributed to nobody — **`hero_credit` is in the table schema but `persistNews`
  never writes it**, so `<figcaption>` never renders.

So every news article on the site is republishing Simple Flying's / CNN's /
Skift's licensed press imagery, uncredited, into a syndication feed. This is:

1. **A licensing exposure.** Those images are frequently Getty/AP/airline
   press-pool assets licensed to *that* publisher.
2. **A NewsBreak rejection and account-risk vector.** NewsBreak's content policy
   requires the publisher to hold image rights.
3. **Free hotlink bandwidth** taken from the outlets you are already rewriting.

This is the single highest-priority fix in the audit, ahead of anything
editorial.

### NewsBreak performance analysis by article/topic/geography/etc.

**Resolved (2026-08-25): the 633 / 3 split is GA4 referral data** — sessions landing on
`/news/` URLs with a `newsbreak.com` referrer. That is good news and bad news.

**Good:** NewsBreak measurement is already solved and costs nothing. It is not a missing
integration, it is a dimension we already collect. It joins to Query G on landing-page
slug, and it collapses §11's separate "NewsBreak feeder" into the GA4 feeder we need
anyway.

**Bad — and this materially limits §2:** referral sessions are **click-throughs, not
impressions**. We can see who arrived; we cannot see how many NewsBreak users were shown
either story. So the 99.4% / 0.5% split **cannot distinguish an editorial failure from a
distribution failure**:

| Possible cause of Southwest's 3 views | Distinguishable from GA4? |
|---|---|
| NewsBreak surfaced it widely, readers didn't click | ✅ that would be an editorial signal |
| NewsBreak surfaced it narrowly or late | ❌ looks identical in GA4 |
| NewsBreak never picked it up at all | ❌ looks identical in GA4 |

Since we hold no NewsBreak account, **there is no way to tell these apart** — a second,
independent argument for applying to their publisher program. Until then, §2's conclusions
should be read as *"headline shape correlates with referral volume"*, not *"headline shape
caused readers not to click."* Query G is what turns a 2-article anecdote into a pattern:
a consistent disruption-vs-expansion gap across 30+ articles survives this confound,
because distribution luck averages out and headline shape doesn't.

**One data-quality check before trusting any of it:** NewsBreak serves links through an
in-app webview and frequently strips or varies the referrer. Confirm in GA4 whether traffic
arrives under `newsbreak.com`, `www.newsbreak.com`, or is partly falling into
Direct/Unassigned. The real number is likely **higher** than 633.

**And the question worth asking before the view count:** GA4 already holds engagement time,
bounce, pages/session and conversions for those 633 sessions. Whether NewsBreak readers do
anything after they land is more decision-relevant than how many landed — see §11.

---

## 4. Google Search / News / Discover audit

| Check | Status | Detail |
|---|---|---|
| `NewsArticle` schema | ❌ | `articleSchema()` hardcodes `'@type': 'Article'` (`seo.tsx:271`). Google's Top Stories / News guidance wants `NewsArticle`. **One-line fix.** |
| Author schema | ⚠️ | Valid `Person` node, resolves to `/authors/editorial`, has `jobTitle` + `worksFor`. But `expertise: []` → no `knowsAbout`, no `sameAs`, no `image`. Weak E-E-A-T for a news vertical. |
| `datePublished` | ✅ | ISO 8601 with Z, from `published_at` |
| `dateModified` | ⚠️ | set to `synthesized_at`, which defaults to `now()` at insert and **is never updated**. So `dateModified` is permanently identical to `datePublished`. Correcting or updating a story does not move it. |
| `headline` | ✅ | matches visible H1 and `news:title` |
| `wordCount` | ❌ | `articleSchema()` **supports** it; `/news/[slug]` doesn't pass it |
| Image in schema | ⚠️ | passes a bare URL string, so the richer `ImageObject` (caption/creator) that `buildImageObject` supports is never populated |
| `canonical` | ✅ | absolute, `https://www.destination.com/news/{slug}` |
| OpenGraph | ⚠️ | `type: 'article'`, title/desc/url/publishedTime/images present. **No `og:site_name`, no Twitter card block** on this template. |
| Crawlability | ✅ | `/news/` not disallowed in `robots.ts` |
| Indexability | ✅ | no `noindex` |
| XML sitemap | ✅ | `/news-sitemap.xml` registered in `sitemap-index.xml` (line 50) |
| News sitemap | ✅ | correct `news:` namespace, 48h window, ISO dates, 1000 cap, 300s revalidate — **well built** |
| IndexNow | ✅ | `/api/indexnow/news` daily at 09:00 UTC |
| Internal linking | ❌ | article template has **3** `Link`s: Home, News, editorial-standards. **Zero links into evergreen guides.** |
| Category architecture | ❌ | **no `/news/[category]` routes exist**. Only `/news` and `/news/[slug]`. Google News sections and topical siloing both need these. |
| Author pages | ✅ | `/authors/[slug]` exists and resolves |
| Publisher info | ✅ | `Organization` + logo in schema |

### Two performance defects that matter for crawling

`/news/[slug]/page.tsx` declares **both** `export const dynamic = 'force-dynamic'`
**and** `export const revalidate = 600`. `force-dynamic` wins — `revalidate` is
dead code. Every request, **including every Googlebot and NewsBreak crawler
hit**, runs a fresh Neon query. Same pattern on `/news/page.tsx` and
`/feed/msn-news`. For a news template that should be aggressively cached and
crawled often, this is backwards: it raises TTFB, burns crawl budget, and puts
your DB in the path of every bot. Dropping `force-dynamic` and keeping
`revalidate` is a two-line change per file.

`/news-sitemap.xml` mixes `/guides/` entries and `/news/` entries in one file.
That is legal, but it means evergreen guides compete for the 48h news window
against actual news. Consider splitting.

### Editorial structure for Search / News / Top Stories / Discover / AI Overviews

The technical base is better than average — the news sitemap in particular is
properly built. The gaps that actually bind are **`NewsArticle`, category
archives, internal links, and a real byline**, in that order. Discover in
particular weights entity clarity and image quality, and the current hero
strategy (someone else's image, uncredited, hotlinked) is the wrong foundation
to build Discover on.

---

## 5. AI / ChatGPT discoverability audit

**Crawler access: ✅ already correct.** `robots.ts` explicitly allows `GPTBot`,
`ClaudeBot`, `PerplexityBot`, `OAI-SearchBot`, and `Google-Extended`. This is
better than most publishers. The access side is done.

**Structure: ❌ this is the real gap.**

The synthesis prompt asks for `"bodyHtml": "<p>...</p><p>...</p>"` — **paragraphs
only**. Which means, across the entire news corpus:

- no `<h2>`/`<h3>` — no scannable structure, no extractable sections
- no `<ul>`/`<ol>` — "which four routes?" is never a list
- no `<table>` — route/date/airport data is never tabular
- no FAQ block
- no summary box
- no `<time datetime>` — dates are prose only
- no entity markup on airlines, airports, or destinations

The `VOICE` sample in the prompt teaches an inverted-pyramid lede, which is
genuinely good for AI extraction. But a wall of 4–6 `<p>` tags is close to the
worst structure for machine citation: an AI system has to infer where the
answer is instead of being shown.

**Your "What Travelers Need to Know" idea is exactly right, and it is a
prompt-level change, not an engineering project.** The body is
`dangerouslySetInnerHTML`, so any HTML the model emits renders as-is. Changing
the `OUTPUT` contract in `SYSTEM_PROMPT` to require a structured block changes
every future article with no template work:

```
"bodyHtml": "<div class=\"tl-dr\"><h2>What travelers need to know</h2><ul>
   <li><strong>What happened:</strong> …</li>
   <li><strong>Who's affected:</strong> …</li>
   <li><strong>Where:</strong> …</li>
   <li><strong>When:</strong> <time datetime=\"YYYY-MM-DD\">…</time></li>
   <li><strong>Why it matters:</strong> …</li>
   <li><strong>What to do:</strong> …</li>
   </ul></div>
   <p>…</p>  — then 350-500 words of reporting, with <h2> subheads,
   and a <table> when the story involves routes, dates, fares, or fees."
```

Then add a matching `ItemList`/`FAQPage` to the JSON-LD when the block is
present. Six facts, above the fold, in a list a model can lift verbatim — that
is what gets cited.

**Source attribution** is already handled well: `source_urls` + `source_names`
render as a "Reported via" block with `rel="nofollow noopener"`. Keep it. It is
one of the few genuine trust signals in the template.

**Quotable facts / original reporting:** structurally unavailable today. A
pipeline that only paraphrases other people's reporting has nothing an AI system
would prefer to cite over the original. This is the ceiling that §6 and §7 exist
to raise.

---

## 6. Categories to own

Judged on the five criteria you set — NewsBreak distribution, Google demand,
Discover potential, AI citation, newsletter engagement, monetization — plus one
you didn't but should: **can we get it from a primary source?**

### Tier 1 — build the moat here

**Airline route and service changes (pauses, cuts, suspensions, new routes).**
This is the Breeze pattern and it is your best category on every axis. Route
changes are geo-tagged by construction, which is exactly what NewsBreak's
distribution wants. They are dated, consequential, and searchable. Crucially,
**they are available from primary sources** — airline newsrooms, and DOT/OAG
schedule filings, which almost no consumer site mines directly. That is a real
original-reporting angle, not a rewrite. Monetization is direct: every route
story maps to a flight-search intent, and the site already has `/flights/[route]`
templates to link into.

**Airport disruption and operational change.** Same geography advantage, higher
urgency, strong local relevance — NewsBreak's core. Primary sources (airport
authorities, FAA) are public and underused.

**Entry requirements, visa and border policy.** Highest AI-citation value in the
set: these are factual, dated, jurisdictional questions people ask assistants
directly. Government sources are primary, authoritative, and free. Already a
category in the taxonomy (`visa-policy`). Lower NewsBreak volume, but the search
and AI value is durable rather than one-day.

### Tier 2 — high value, needs different infrastructure

**Points, miles, transfer bonuses, devaluations.** Best newsletter and
monetization economics in travel — card affiliate revenue is the highest-RPM
inventory on the site. But it is a *specialist* beat with an expert audience that
punishes a rewrite instantly, and your four aviation-blog sources are exactly the
outlets that own it. Not a category to enter with an unattended rewrite pipeline.
Enter it with a named human or not at all.

**Fare sales and deals.** Strong newsletter and affiliate value, and you already
have deal infrastructure (`/deals`, `bake-deals`, `mistake-fare-watcher`). But
deals are perishable in hours and NewsBreak's review latency works against them.
Better as a newsletter and alerts lane than a `/news/` lane.

### Tier 3 — deprioritize

**Hotel openings, cruise announcements, resort fees.** Weak on NewsBreak
(low geographic specificity, low urgency), weak on AI citation, and dominated by
trade press with better access. Cruise has a passionate niche audience but it is
a different distribution game entirely. Cover opportunistically via `hospitality`;
don't build a beat.

### The taxonomy needs changing either way

Current categories are `aviation | visa-policy | destination-news | industry |
transit | hospitality`. Note that **`industry` is a trade-press bucket** — it is
the Southwest-Nashville failure mode encoded as a category. And there is no slot
for points, deals, or cruise. Recommend: keep `aviation`, `visa-policy`,
`hospitality`, `destination-news`, `transit`; **drop or demote `industry`** (it
systematically produces B2B stories for a consumer feed); add `points-miles` and
`deals` when Tier 2 is staffed.

---

## 7. News opportunity engine

The pipeline you asked for — Source → Detect → Score → Review → Write → QA →
Publish → Distribute → Measure — is a good design. Today's pipeline is
**Source → Detect → Write → Publish**. Three of eight stages are missing, and
the two missing in the middle (Score, Review) are the ones that separate a news
operation from a content farm.

### Stage 1 — Sources: add primary feeds

The highest-leverage change in this entire audit. Add, in priority order:

| Source | Why | Access |
|---|---|---|
| Airline newsrooms (Breeze, Southwest, JetBlue, Spirit, Frontier, Delta, United, American) | The Breeze story's origin. First-party, hours ahead of Simple Flying | Most publish RSS |
| DOT / FAA | Route filings, enforcement, disruption data | Public feeds |
| State Dept / CBP / embassy advisories | Owns the `visa-policy` beat outright | RSS |
| Major airport authorities (FL, TX, CA, NY metros) | Local relevance = NewsBreak's currency | Mixed; some RSS |
| Tourism boards | Destination news at source | Mixed |
| Google Trends (travel) | Demand signal for scoring, not a story source | API |

Adding airline newsrooms alone converts a meaningful share of output from
"rewrite of Simple Flying" to "reported from the airline's own announcement" —
which is the difference between a citation and a footnote, and closes the 4–12h
latency gap.

### Stage 3 — Score: the missing gate

Score every detected cluster **before** spending a Claude call. Weights derived
from §2's analysis:

| Signal | Weight | Extractable today? |
|---|---|---|
| Geographic reach (state/multi-metro > metro > none) | 25% | Yes — from headline entities |
| Traveler impact (disruption > policy > expansion > corporate) | 25% | Yes — verb classification |
| Freshness (hours since primary announcement) | 15% | Yes — source `pubDate` |
| Search demand | 15% | Needs Trends API |
| Topical authority fit (Tier 1 > 2 > 3 from §6) | 10% | Yes — category map |
| Commercial value (maps to a flights/hotel/card page?) | 10% | Yes — entity → route map |

Publish above a threshold; queue the rest for human review; drop the floor. This
one change would have ranked Breeze above Southwest-Nashville *before* either was
written.

### Stage 4 — Review: flip the default

Change `status` default from `'live'` to `'review'` for anything below the
auto-publish score threshold, and surface the queue in the existing
`/admin/news` page (it already exists). High-scoring, primary-sourced, Tier-1
stories can keep auto-publishing. Everything else waits for a human. This is a
one-word schema change plus an admin filter.

### Stage 6 — QA: automate the checks that are currently nobody's job

Before publish: hero image is **licensed or first-party** (not scraped), no
verbatim source sentences, all named entities appear in source material, dek
present, ≥1 internal link, `NewsArticle` schema validates.

---

## 8. Recommended cadence

**Do not set a volume target yet. Set a quality gate, then let volume follow.**

The strongest argument here is your own precedent. `daily-content.yml` reads:

> "Cut from twice-daily ... on 2026-07-30 — **the 4-articles/day firehose was
> the biggest Google-churn risk on the site.** Halving to 2/day keeps momentum
> ... while leaving cycles for higher-signal work."

You already learned this lesson on guides four weeks ago, deliberately, and
wrote it down. The news lane is currently provisioned for up to **60/day** — 15×
the cadence you just rejected as a churn risk — with *less* review than guides
get. Scaling it before fixing §7 would repeat a mistake you have already
diagnosed.

**Phased recommendation:**

| Phase | Cadence | Gate |
|---|---|---|
| **Now → day 30** | **Cap at 3/day (~21/week)**, lower `MAX_CLUSTERS_PER_RUN` to 1–2 | Fix hero-image rights, `NewsArticle`, review gate. Volume down, quality up. |
| **Day 30 → 60** | **5/day (~35/week)** | Only after primary sources are live and ≥50% of output is primary-sourced |
| **Day 60 → 90** | **8–10/day (~56–70/week)** | Only if Query G shows Tier-1 categories sustaining per-article performance at 5/day. If per-article views fall as volume rises, hold. |

**Explicit rule: per-article performance is the gate, not total volume.** If
doubling output halves views per article, you have gained nothing and added
risk. The reason to raise the cap is *more primary sources producing more
genuinely distinct stories* — never a bigger number for its own sake.

---

## 9. Newsletter integration

**Better than expected — and undocumented in the brief.** `scripts/send-briefing.mjs`
+ `.github/workflows/briefing.yml` already ship "The Briefing": Monday 13:00 UTC,
4 stories from the last 7 days of `news_items`, Haiku-written "why it matters"
line per story plus an editor's-note synthesis, broadcast via Resend, each card
linking back to the article. **This is essentially the "This Week in Travel"
format you described, and it already exists.**

Two things to know:

1. It runs as a **GitHub Action**, not a Vercel cron — which is why it doesn't
   appear in `vercel.json` and is easy to miss.
2. It replaced the killed `newsletter-weekly` cron, which overlapped Thursday's
   Deal Drop.

**Gaps to close:**

- **Weekly only.** Route changes are perishable; a Monday digest misses a Wednesday
  disruption. Add a **breaking-alert lane** for stories scoring above a high
  threshold in §7 — the same scoring already tells you which ones qualify.
- **No topical segmentation.** `newsletter-lanes.ts` exists and the site has
  beach/family/luxury/inspiration lanes. News feeds none of them. An
  aviation-only or points-only lane is the natural next cut.
- **No signup attribution from news.** `newsletter-attribution.yml` exists but
  news articles carry no newsletter CTA — the article template has no inline
  capture at all. That is a direct, easy conversion win.

---

## 10. Content flywheel

The flywheel you described does not exist. `news_items` is referenced by
**13 files, all of which are news infrastructure**: the ingest cron, health
check, editorial review, IndexNow, admin page, the two page templates, two feeds,
the news sitemap, the homepage strip, and two scripts.

Measured against your diagram:

| Channel | Wired? |
|---|---|
| Google Search / News | ✅ news sitemap + IndexNow |
| NewsBreak | ❓ not in the codebase (§3) |
| MSN | ✅ `/feed/msn-news` |
| Apple News | ✅ `/feed/apple-news` |
| Newsletter | ✅ The Briefing (weekly) |
| X / Twitter | ❌ 8 X workflows exist; **none reads `news_items`** |
| Facebook | ❌ no integration |
| Pinterest | ❌ 10 Pinterest workflows; none reads news |
| Push / alerts | ❌ `push-price-drops` + `push-trip-reminders` exist; no news push |
| AI / ChatGPT | ⚠️ crawlable, poorly structured (§5) |
| Internal links → evergreen | ❌ **zero** |

**The cheapest wins are the ones where the machinery already exists and simply
isn't pointed at news.** You have a mature X engine, a mature Pinterest engine,
and a push system. Each needs a query against `news_items`, not a new system.

**The single highest-value missing link is internal linking.** Every article
already extracts `country`, `region`, `tags`, and `category` at synthesis time —
so the join to existing evergreen guides, `/flights/[route]` and `/tickets/[city]`
pages is available and unused. A news story about Florida routes should link into
your Florida guides and flight pages. That is the actual flywheel: news captures
the spike, evergreen captures the value, and internal links convert one into the
other. Today the spike lands on a page with three links, all navigational, and
leaves.

---

## 11. Measurement

**Nothing measures news today.** No dashboard, no per-article analytics join, no
revenue attribution. The weekly editorial-review email reports *production*
stats (volume, hero coverage, body length, duplicate suspects) — inputs, not
outcomes. Its own header comment concedes the gap: ingest failure details "would
be ideal but we don't persist those yet."

**The blocking dependency is an identity join.** Every metric you listed keys on
one thing: article slug. Build this and the dashboard is mostly assembly:

```sql
CREATE TABLE news_performance (
  slug TEXT REFERENCES news_items(slug),
  as_of DATE,
  source TEXT,          -- 'gsc' | 'newsbreak' | 'ga4' | 'newsletter' | 'social' | 'affiliate'
  impressions BIGINT, clicks BIGINT, pageviews BIGINT,
  signups INT, affiliate_clicks INT, revenue_usd NUMERIC,
  PRIMARY KEY (slug, as_of, source)
);
```

Feeders, in dependency order:
1. **GA4** — now the highest-priority feeder, because it carries three things at once:
   pageviews and engagement per `/news/` URL, returning-visitor rate, **and** the NewsBreak
   signal (referral sessions where source = `newsbreak.com`). No portal, no export, no new
   integration — NewsBreak measurement is a dimension we already collect.
2. **GSC API** → impressions/clicks/position per `/news/` URL (daily)
3. **Newsletter** → `newsletter-click-log.ts` already exists; add news slugs
4. **Affiliate** → `impact-commissions-pull.yml` + `affiliate-conversions-pull.yml`
   already exist; join on landing-page slug

Then per-topic rollups (`views/article` by category, `signups/article`,
`affiliate revenue/article`) are `GROUP BY category` — exactly the
"airline route news generates X, points news generates Y" comparison you want.
**Build the join table before the dashboard.** Everything else is a view over it.

### The GA4 pull to run first

Before any of the above, run one GA4 exploration — it is the cheapest decision-relevant
data available right now and needs no engineering:

- **Dimensions:** Landing page (filter `/news/`) × Session source/medium
- **Metrics:** Sessions, Average engagement time, Engaged sessions %, Pages/session,
  Key events (newsletter signup)
- **Range:** since 2026-07-28 (the lane's first day, §1)

That answers three things the view count cannot: whether NewsBreak referrals are
attributed cleanly or leaking into Direct/Unassigned; whether NewsBreak readers engage or
bounce; and how NewsBreak compares with organic search on the same articles. **If
NewsBreak traffic bounces at 90%+ with single-digit engagement seconds, the strategic case
in §15 weakens considerably** — 633 sessions that read nothing are not an acquisition
channel. If they engage, the case strengthens and the flywheel work in §10 becomes the
priority. Either way it is a better first question than "how many views did we get."

---

## 12. Technical fixes required

Ranked by (risk × reach) ÷ effort. All are in `mastap150/destination-com`, which
this session has **read-only**, so these are specified, not applied.

| # | Fix | File | Severity |
|---|---|---|---|
| 1 | **Stop hotlinking source publishers' `og:image`.** License a stock source, generate originals, or ship no hero. Populate `hero_credit` for anything retained. | `news-ingest.ts` `pickOgImage`/`persistNews` | **Critical — legal + NewsBreak account risk** |
| 2 | `'@type': 'Article'` → `'NewsArticle'` for `/news/` | `seo.tsx:271` (add a variant; don't break guides) | High |
| 3 | Add "What travelers need to know" block + `<h2>`/`<ul>`/`<table>`/`<time>` to the `OUTPUT` contract | `news-ingest.ts` `SYSTEM_PROMPT` | High — fixes AI + Discover at once |
| 4 | Add internal links from news → evergreen guides / `/flights/[route]` | `news/[slug]/page.tsx` | High — the flywheel |
| 5 | Remove `dynamic = 'force-dynamic'`; keep `revalidate` | `news/[slug]`, `news/page.tsx`, `feed/msn-news` | High — crawl budget + TTFB |
| 6 | `status` default `'live'` → `'review'` below score threshold | `schema.sql:937` + `persistNews` | High — governance |
| 7 | Add `/news/[category]` archive routes | new route | Medium — Google News sections |
| 8 | Pass `wordCount` + rich `ImageObject` to `articleSchema` | `news/[slug]/page.tsx` | Medium |
| 9 | Make `dateModified` real (update `synthesized_at` on edit) | `admin/news` write path | Medium |
| 10 | Add per-item update timestamp to the feed | `feed/msn-news/route.ts` | Medium |
| 11 | Alert on empty feed, not just failed ingest | `news-ingest-health` | Medium |
| 12 | Add `og:site_name` + Twitter card | `news/[slug]/page.tsx` | Low |
| 13 | Split guides out of `/news-sitemap.xml` | `news-sitemap.xml/route.ts` | Low |
| 14 | Fix stale comment: `/feed/msn` is described as "mixed guides+news" but reads only `articles.ts` | `feed/msn-news/route.ts` header | Trivial |

---

## 13. Editorial changes required

1. **Add primary sources** (§7). Highest leverage in the audit.
2. **Introduce a scoring gate** before synthesis (§7).
3. **Introduce a human review gate** for anything below auto-publish threshold.
4. **Reconsider the collective byline for news specifically.** The masthead
   policy in `authors.ts` is deliberate and defensible for guides. For news, both
   Google News and NewsBreak weight named, credentialed contributors, and the
   current node carries no `knowsAbout`, `sameAs`, or `image`. At minimum,
   populate `expertise` on the editorial record. Better: one named aviation
   editor.
5. **Fix the newsroom disclosure.** "This story was written and edited by the
   destination.com newsroom" describes a process with no human in it. Either put
   a human in it (recommended) or align the language with the AI-use disclosure
   the `/editorial-standards` page already promises.
6. **Kill or demote the `industry` category** — it systematically produces
   Southwest-Nashville-shaped trade stories for a consumer feed.
7. **Write to the §2 headline pattern deliberately:** named operator + verb of
   change + countable specifics + large geography + dated window.

---

## 14. 30 / 60 / 90-day plan

### Days 1–30 — Fix the foundation, reduce volume

- Fix #1 (image rights) — **do this first, it is a live exposure**
- Fix #2, #3, #5 (NewsArticle, structured body, caching)
- Lower `MAX_CLUSTERS_PER_RUN` to 1–2; cap ~3/day
- **Decide on NewsBreak:** no account exists — content is picked up unmanaged. Apply to
  their publisher program *after* fix #1 lands; wire GA4 `newsbreak.com` referral
  tracking in the meantime
- Run Queries A–G; fill in §1's real numbers and settle the pipeline-age conflict
- Add the first three airline newsrooms
- Build `news_performance` + the GSC feeder

**Exit criteria:** zero scraped hero images live; `NewsArticle` validating; real
baseline numbers in hand.

### Days 31–60 — Add reporting and distribution

- Scoring engine + review gate (#6)
- `/news/[category]` archives (#7)
- Internal linking news → evergreen (#4)
- Point the **existing** X and push machinery at `news_items`
- Add breaking-alert newsletter lane; add newsletter CTA to article template
- Raise to ~5/day **only if** ≥50% of output is primary-sourced

**Exit criteria:** majority primary-sourced; every article links into evergreen;
news feeds ≥3 channels beyond search.

### Days 61–90 — Scale what the data proves

- Full dashboard (§11) with per-topic RPM and signups/article
- Decide Tier 2 (points/deals) on the numbers — staff it with a named human or skip
- Raise to 8–10/day **only if** per-article performance held at 5/day
- Evaluate a named aviation editor against measured revenue/article

**Exit criteria:** a defensible answer to "airline route news generates X
views/article, points news generates Y signups/article" — the data-driven
editorial decisions you asked for.

---

## 15. Should news become a significantly larger part of the strategy?

**Yes as a strategic direction. No as an immediate volume increase.**

**The case for news is real.** The Breeze story is genuinely encouraging: a
single AI-synthesized rewrite of a secondary source, with a hotlinked image and
no promotion, pulled 633 NewsBreak views. That is a signal about the *category*.
Travel news maps unusually well onto every surface you care about — NewsBreak
rewards local relevance and route changes are inherently geo-tagged; Google News
and Discover reward freshness; AI systems reward dated factual claims; travel
news converts to flight and hotel intent better than most verticals. And it is
cheap to produce relative to a 2,500-word guide.

**But the current implementation cannot carry that weight.** It is an unattended
rewrite pipeline built on nine secondary blogs, publishing other publishers'
images without license or credit, with no human gate, no named author, no
internal linking, `Article` instead of `NewsArticle`, and — on the evidence in
this repo — no NewsBreak integration at all. Scaling it multiplies a legal
exposure and a Google-churn risk you already identified on the guides side four
weeks ago and deliberately pulled back from.

**Sequence, not volume.** The Breeze result argues for investing in news. It does
not argue for publishing more of what produced it — it argues for producing
*more things like the Breeze story specifically*: large-geography, consequential,
dated, primary-sourced service journalism. That is a sourcing and editorial
problem, not a throughput problem. Fix §12's top five, add primary sources, put a
scoring gate in front of synthesis, then let measured per-article performance
authorize each volume increase.

**On the deeper goal you stated** — traffic + search authority + NewsBreak
distribution + AI visibility + newsletter subscribers + returning users +
revenue — note that five of those seven are *authority* metrics, and authority is
precisely what a rewrite pipeline cannot accumulate. Search authority, AI
citation, and returning users all accrue to whoever is the *source*. Today
destination.com is structurally downstream of Simple Flying on every story it
publishes. The strategic unlock is not more articles; it is being first and being
primary on a narrow beat — Tier 1 in §6 — often enough that Google, NewsBreak,
and AI systems learn to treat destination.com as the origin rather than the echo.

---

## Appendix — what this audit could not verify

| Question | Blocker | Resolution |
|---|---|---|
| Exact articles/day, /week, /month | No `DATABASE_URL` | Queries A–F |
| Category / region / hour / day distribution | Same | Queries B, C, D |
| Hero coverage %, body length, kill rate | Same | Queries E, F |
| Both articles' body, headline, images, metadata | Same | Query H |
| Pipeline age (4 weeks vs. "months") | Conflicting comments | Query A |
| NewsBreak accept/reject rates, in-app impressions | **No account exists** — content is picked up unmanaged | Apply to NewsBreak's publisher program (§3) |
| Whether NewsBreak *distributed* a story, vs. readers not clicking | GA4 sees click-throughs only, and we hold no account | Apply to NewsBreak's publisher program (§3); Query G averages the confound out |
| GSC impressions/clicks, Discover traffic | No GSC credentials | GSC API |
| Live feed/sitemap output, HTTP status | `destination.com` egress-blocked | Fetch from an unblocked network |
| Revenue / RPM / affiliate per article | No analytics access | GA4 + Impact |
