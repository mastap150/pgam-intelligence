# Destination.com — Content & Publishing Strategy

**2026-08-26.** Follows `destination-news-audit-2026-08-25.md`, which established
what the `/news/` lane currently is. This document sets what it should become:
the publishing model, the franchises, the human-in-the-loop workflow, the article
template, and how the same engine feeds newsletters and distribution.

---

## 1. The central problem, stated precisely

The brief asks for **significantly greater volume while maintaining quality**.
Those pull against each other only if you treat every article as the same object.
They stop competing once you accept two constraints and design around them.

**Constraint 1 — "unoriginal" is a compliance category now, not a quality opinion.**
Google's scaled-content-abuse policy targets content "created primarily for
ranking, without regard for originality or value," *explicitly regardless of
whether AI or humans produced it*. NewsBreak's publisher standards prioritise
original content and can reject publishers that are predominantly aggregating.
Today Destination.com is a pipeline whose stated purpose is to "take multiple
published reports ... and synthesize" — the textbook description of the thing both
policies target. And because NewsBreak scrapes us unmanaged with no account
(audit §3), we would not even get a warning; distribution would simply stop.

**Constraint 2 — the real ceiling is editorial minutes, not AI throughput.**
The pipeline can already produce 60 articles/day and doesn't, because dedup
correctly rejects the overlap. AI capacity has never been the bottleneck.

So the strategy is not "publish more" or "publish less." It is:

> **Make originality structural rather than aspirational, then buy volume with
> editorial capacity.**

The **Destination layer** described in the brief — what changed, why a traveler
should care, who it affects, what to do next, and the flight/hotel/destination
options — is not a nice-to-have formatting idea. **It is the compliance
mechanism.** It is the thing that converts "a rewrite of Simple Flying" into a
piece of service journalism that did not exist before we published it. Every
policy question, every NewsBreak risk, and every AI-citation opportunity in this
document resolves to whether that layer is genuinely present.

---

## 2. The originality test

One gate, applied to every article before publication. It is deliberately narrow
enough to answer in fifteen seconds:

> **Name one fact, figure, comparison, or piece of actionable guidance in this
> article that does not appear in any source we read.**
>
> If you cannot, it does not publish.

Things that pass: a fare we actually priced on the affected route; the rebooking
rule that applies, quoted from the carrier's own contract of carriage; which of
our destination guides the affected travelers need; a comparison against the same
airline's last three route cuts; a table of the four routes with dates and
alternatives. Things that fail: a paraphrase, a "what this means" paragraph that
restates the news, a summary with adjectives changed.

This single test does more for Google-policy safety, NewsBreak standing and AI
citation than any amount of schema work — and it is the one thing an unattended
pipeline structurally cannot pass.

---

## 3. The publishing model: four tiers

Not all articles are the same object, so stop costing them the same way.

| | **A · Alert** | **B · Destination-layered news** | **C · Original / analysis** | **D · Evergreen** |
|---|---|---|---|---|
| **What** | Something changed, travelers need to know now | The news plus the full Destination layer | Reporting with an angle no one else has | Guides, explainers, curated picks |
| **Length** | 200–350 words | 600–900 words | 1,200–2,000 words | 1,500–3,000 words |
| **Turnaround** | 30–60 min | 2–4 hours | 1–3 days | Planned |
| **Source** | **Primary only** | Primary + secondary context | Our own data, pricing, comparison | Research + first-hand |
| **Originality comes from** | Being first + the "what to do" line | The Destination layer | The reporting itself | Depth and curation |
| **Editorial minutes** | **6** | **15** | **120+** | Days |
| **Byline** | Named editor | Named editor | Named author | Named author |
| **Primary surface** | NewsBreak, Discover, alerts | Search, NewsBreak, newsletter | Search, AI citation, backlinks | Search, AI citation, conversion |

**Tier A is the volume tier, and it is legitimately original** — but only under one
condition: it must come from a **primary** source. Reporting an airline's own
announcement within the hour, with a rebooking line attached, is original service
journalism. Rewriting Simple Flying's account of that announcement is aggregation.
The distinction is entirely in the sourcing, which is why §5's source expansion is
the precondition for any volume increase, not a parallel workstream.

**Tier C is the authority tier.** Two or three a week is enough. This is what earns
the citations, the backlinks, and the right to be treated as a source rather than
an echo — and it is the only tier that compounds.

---

## 4. The franchises

The brief proposes nine. All nine are viable; they are not equally valuable, and
the audit's evidence says so. Ranked, with the Destination layer specified for
each — because "add a Destination layer" is meaningless until you say what it is.

### Core three — build the moat here

**1 · Airline & flight news** *(Tier A/B, daily)*
Route cuts, pauses, new service, schedule changes, policy and baggage changes,
disruption. The Breeze pattern lives here, and it is the single best-evidenced
franchise we have.
→ *Destination layer:* who's booked and affected, the rebooking rights that apply,
the alternative routings, current fares on them, and our guide to the destination
they were flying to.
→ *Primary sources:* airline newsrooms, DOT filings, airport authorities.

**2 · Destination developments** *(Tier A/B, daily)*
Entry requirements, visa changes, tourism taxes, closures, restrictions, major
openings.
→ *Destination layer:* what a traveler must do differently, by when, with the form
or fee named — plus whether our existing guide for that destination is now stale.
→ *Primary sources:* State Department, embassies, tourism boards, national parks.
→ *Note:* highest AI-citation value of any franchise. These are dated, factual,
jurisdictional questions people ask assistants directly.

**3 · Deals & value** *(Tier A/B, daily)*
Fare sales, mistake fares, award availability, promotions.
→ *Destination layer:* is it actually good — priced against our own history — who
it suits, and how long it lasts.
→ *We already have the infrastructure:* `mistake-fare-watcher`, `bake-deals`,
`price-history.json`. **This is the only franchise where we hold proprietary data
today**, which makes it the fastest route to genuine originality.

### Strong supporting four

**4 · Hotel & resort openings** *(Tier B, 2–3/week)* — layer: who it's for, what
it costs, what else is nearby, is it worth rerouting a trip for.
**5 · Practical explainers** *(Tier B/C, 3–5/week)* — "What are your rights when an
airline cancels?" Evergreen-adjacent, high AI-citation value, strong internal-link
anchors.
**6 · Events & things to do** *(Tier B, 3–5/week)* — strong local relevance, which
is NewsBreak's currency; pairs naturally with existing city pages.
**7 · Curated recommendations** *(Tier C, 2–3/week)* — the "best X" format, but
earned: our own criteria and testing, not a scrape of other lists.

### Conditional two

**8 · Points & miles** *(Tier B/C)* — best newsletter and affiliate economics in
travel, and an expert audience that detects a rewrite instantly. **Enter only with
a named specialist.** Do not let a general pipeline near this beat.
**9 · Breaking alerts** *(Tier A, as warranted)* — reserve for genuine disruption:
ground stops, strikes, closures, weather events. Cheapened if overused.

**One removal:** kill the `industry` category (audit §6). It is a trade-press
bucket that systematically produces B2B stories for a consumer audience — the
Southwest-Nashville failure mode encoded as a taxonomy.

---

## 5. The workflow: where AI helps and where it must not

AI does the work that scales; humans do the work that carries liability and voice.
The line is not negotiable, because it is the line both Google's policy and
NewsBreak's standards actually test.

| Stage | AI | Human | Why |
|---|---|---|---|
| **1 Monitor** | ✅ Full | — | Poll primary sources, cluster, detect what's new. Already built and good. |
| **2 Score** | ✅ Full | — | Rank by the audit §7 model: geography, traveler impact, freshness, search demand, authority fit, commercial value. |
| **3 Triage** | Proposes | **✅ Decides** | *An editor picks what we cover.* This is where a publication's identity actually lives. Ten minutes each morning. |
| **4 Research** | ✅ Full | — | Pull primary docs, prior coverage, our own guides, fare and price history. High leverage, no risk. |
| **5 Draft** | ✅ Full | — | Structured first draft against the tier template. |
| **6 Destination layer** | Proposes | **✅ Writes/verifies** | The original value. Cannot be delegated — if AI writes it from the same sources, it isn't original. |
| **7 Fact-check** | Flags | **✅ Verifies** | Every number, date, name against a primary source. Non-negotiable. |
| **8 Headline** | Proposes 5 | **✅ Picks/edits** | Uses the §2 pattern. AI is good at variants, bad at judgement. |
| **9 Byline** | — | **✅ Named human** | See §6. |
| **10 Publish** | ✅ On approval | Approves | |
| **11 Newsletter** | ✅ Drafts | Curates | §8. |
| **12 Social** | ✅ Full | — | Low-risk, already-built engines. |

**The three inversions from today's pipeline:** an editor chooses what gets
covered (today nothing does), a human writes the original layer (today nothing
does), and nothing publishes unattended (today everything does — `status DEFAULT
'live'`).

---

## 6. Bylines and authorship

`src/data/authors.ts` resolves every byline to one collective record,
"destination.com editorial," with empty `expertise`, no `sameAs`, no image. That is
a defensible masthead policy for guides. **For news it is a ceiling**, and it is
incompatible with the brief's requirement of an identifiable author.

Google's news guidance, NewsBreak's publisher standards and AI citation behaviour
all weight named, credentialed, verifiable humans. A collective byline on a story
that carries no original reporting is exactly the signature of the content both
platforms are filtering for.

**Recommendation:** named bylines on all Tier A–C news, with real author pages
(`/authors/[slug]` already exists and works), `knowsAbout` populated, and links out
to a real professional profile. Start with two or three named people — including
whoever performs the editorial review, since they are genuinely doing the work.
Keep the collective masthead for evergreen guides where it fits.

Also fix the disclosure. The template currently says "written and edited by the
destination.com newsroom" on articles no human has touched. Under the model above
that statement becomes true — but it must not run ahead of the workflow.

---

## 7. Cadence: the number, and what actually sets it

Volume is a function of editorial minutes. One editor is ~6 productive hours =
**360 minutes/day**. Using the tier costs from §3:

| Staffing | Realistic daily mix | Articles/day | Articles/week |
|---|---|---|---|
| **0 editors** *(today)* | unattended | 0 defensible | — |
| **0.5 editor** | 6 × A, 4 × B | **~10** | ~70 |
| **1 editor** | 8 × A, 8 × B, 1 × C every other day | **~17** | ~119 |
| **2 editors** | 14 × A, 14 × B, 1 × C/day | **~29** | ~200 |

**So the honest answer to "how many should we publish" is: whatever one editor can
review, until you hire a second.** Cadence is a hiring decision, not a technology
decision. Anything above the line for your staffing is unreviewed content, which is
the thing that carries the platform risk.

**Recommended path**, consistent with the audit's phasing and with the precedent in
`daily-content.yml` (guides were deliberately cut from 4/day to 2/day on 2026-07-30
as a Google-churn risk):

- **Weeks 1–4 — cap ~3/day.** Not a target, a deliberate throttle while primary
  sources, the review gate and the template land. Quality of the *system* over
  output.
- **Weeks 5–8 — ~10/day** with 0.5 editor. Majority primary-sourced.
- **Weeks 9–12 — ~17/day** with a full editor, if and only if per-article
  performance held through the previous step.
- **Beyond — ~29/day** at two editors.

**The gate at every step is per-article performance, never total volume.** If
doubling output halves engagement per article, the increase bought nothing and
added risk.

---

## 8. Newsletter

"The Briefing" already exists — `scripts/send-briefing.mjs` + `briefing.yml`,
Mondays 13:00 UTC, four stories from `news_items` with an AI "why it matters" line
each. The bones are right. What's missing is that it's weekly, undifferentiated,
and has no path back into trip planning.

**Target shape — a curated editorial product, not a feed dump:**

| Product | Cadence | Content | Job |
|---|---|---|---|
| **The Briefing** | Daily or 3×/week | 5–7 curated stories, "what happened / why it matters / where you should go" | Habit, returning readers |
| **Breaking alerts** | As warranted | Single high-impact disruption | Urgency, trust |
| **Deal drop** | Weekly *(exists)* | Fares + award value | Affiliate revenue |
| **Destination editions** | Monthly | One place, deeply | Segmentation, trip intent |

Three rules that distinguish curation from duplication: **fewer stories than the
site published** (selection is the product); **every item ends in a next step** —
a guide, a route search, a destination page, not just "read more"; and **AI drafts,
a human picks and writes the top line**, which is the only part subscribers
actually judge.

The site has newsletter capture on most templates but **not on news articles**
(audit §9). That is the cheapest conversion fix available.

---

## 9. Article template

The current template is a bare 760px column: headline, dek, byline, hero, an
undifferentiated `dangerouslySetInnerHTML` body, a sources box, and **three links —
Home, News, editorial-standards.** No subheads, no key facts, no related content,
no newsletter capture, no trip-planning path. Traffic lands and leaves.

Full spec below; a rendered mockup accompanies this document.

**Above the fold**
- Category eyebrow → links to the category archive *(which must be built — audit §12 #7)*
- H1, 40–70 chars, the §2 pattern
- Dek: 1–2 sentences carrying the actual news
- **Byline row:** named author + link to author page · published timestamp ·
  "Updated" timestamp when genuinely updated · est. read time
- Hero image with **caption and credit** — `hero_credit` exists in the schema and
  is never written; it must be populated and rendered

**The key block — immediately below the dek**
The "What travelers need to know" box: what happened / who's affected / where /
when / why it matters / what to do next. Six lines, scannable, front-loaded. This
is simultaneously the best UX element, the AI-extraction surface, and the visible
proof of the Destination layer. Mark it up as `ItemList` in JSON-LD.

**Body**
- 65–75 character measure, ~1.65 line height, 17–18px
- `<h2>` every 200–300 words, written as real subheads
- Tables for anything with three or more comparable facts — routes, dates, fares
- Pull-quotes for genuine quotes only
- **Contextual internal links, 3–6 per article**, to destination guides,
  `/flights/[route]`, `/tickets/[city]`. Every article already extracts `country`,
  `region`, `tags` and `category` at synthesis time, so the join is available and
  unused.
- Source attribution block — keep it, it's one of the few real trust signals present

**Ad placement**
After the first complete section, then every second `<h2>` — **never mid-section,
never mid-sentence**, none between the dek and the key block. That top area is the
article's whole job.

**Affiliate modules**
Contextual and specific ("Fares to Tampa on the affected dates"), clearly
disclosed above the module, at most one or two per article. NewsBreak permits
disclosed affiliate content and penalises excessive promotional linking — the
constraint is real, not stylistic.

**Recirculation — below the story**
More from this franchise → related destination guides → **"Plan this trip"**
(flight search, hotels, the destination hub) → newsletter signup → most-read.
Ordered by intent: readers who finished a Florida route story are closest to a
Florida trip, and the current template offers them nothing.

---

## 10. Technical distribution layer

Audit §12 carries the full ranked list. The items that specifically serve
distribution:

**Feeds.** NewsBreak's spec requires full HTML content (not snippets), canonical
URLs, publication timestamps, author information, summaries and images.
`/feed/msn-news` already ships full `content:encoded`, canonical `guid`, RFC-822
`pubDate`, `dc:creator` and mediaRSS — **it substantially meets the spec already.**
Three gaps: no per-item update timestamp, no image credit, and a collective rather
than named byline. All three close under §6 and §9. A dedicated `/feed/newsbreak`
becomes worth building only if we apply for a publisher account.

**Schema.** `NewsArticle` not `Article`; `wordCount`; a populated `ImageObject`
with caption and creator; `ItemList` for the key block; author `Person` with
`knowsAbout` and `sameAs`.

**Architecture.** Category archives at `/news/[category]`; author pages populated;
canonical URLs already correct; the four-file `force-dynamic` caching bug fixed so
crawlers stop hitting the database on every request.

**Editorial trust surface.** `/editorial-standards` exists — it needs sourcing
policy, AI-use disclosure that matches the actual workflow, corrections policy and
named masthead. Both platforms and AI systems read this page.

---

## 11. Measurement

Per the brief, the scoreboard is **not** publishing volume. Track per article and
roll up by franchise:

| Layer | Metric |
|---|---|
| **Discovery** | Organic entrances, GSC impressions/clicks/position, Discover entrances, NewsBreak referral sessions *(GA4 — already collected)* |
| **Engagement** | Engaged-session rate, engagement time, pages/session, scroll depth |
| **Loyalty** | Returning readers, newsletter signups/article |
| **Revenue** | Affiliate clicks, conversion, revenue/article, RPM |

**Per-franchise economics is the point** — "airline route news generates X
entrances/article, points news generates Y signups/article, deals generate Z
revenue/article." That is what makes editorial decisions data-driven, and it needs
the `news_performance` join table from audit §11 first.

**One health metric above all others:** *pages per session on news entrances.* If
it stays near 1.0, we are renting traffic and the flywheel does not exist,
regardless of how good the top-line numbers look.

---

## 12. Sequence

| | Focus | Ships |
|---|---|---|
| **Weeks 1–2** | Stop the bleeding | Image-rights fix; caching fix; `NewsArticle`; throttle to ~3/day; GA4 baseline pull |
| **Weeks 3–4** | Make originality possible | Primary sources; scoring gate; `status` → `'review'`; named bylines + author pages |
| **Weeks 5–6** | Rebuild the reading experience | New article template; key-facts block; internal linking; recirculation; ad rules |
| **Weeks 7–8** | Turn on the franchises | Core three at ~10/day with 0.5 editor; category archives; newsletter to 3×/week |
| **Weeks 9–12** | Scale on evidence | Full editor → ~17/day; `news_performance` dashboard; Tier C weekly; NewsBreak application |

**The load-bearing decision is staffing.** Every technical item above is days of
work. The volume the brief wants is bounded by how many articles a human can stand
behind — and that is a hire, not a config change.
