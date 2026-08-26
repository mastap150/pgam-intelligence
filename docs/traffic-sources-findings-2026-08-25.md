# Traffic-source findings — Destination.com + BoxingNews, 2026-08-25

What the GA4 digest was hiding, and what it showed once it stopped.

**Data:** GA4 for **2026-08-24** (Destination property `424474915`,
BoxingNews `419680531`), read from `mastap150/pgam-analytics-digest`
`LATEST.md` after run #139. Session-history claims come from the
committed `reports/` series back to 2026-04-24.

**Read the caveat before acting on any number below:** almost everything
here is **one day**. Where a claim rests on the multi-day series it says
so. And the historical series only ever printed the **top 5 sources**, so
"X never appears before date D" means *X was never in the top 5*, not
that X was zero — a distinction that matters for the newsletter finding
in particular.

## What changed in the digest

Three PRs on `pgam-analytics-digest`, all merged 2026-08-25:

| PR | Change |
|---|---|
| [#2](https://github.com/mastap150/pgam-analytics-digest/pull/2) | Source pull 25 deep, prints 10, with share / PV-per-session / engaged% / avg-session. AI-assistant rollup over the full pull. New `sessionSourceMedium × landingPagePlusQueryString` section. |
| [#3](https://github.com/mastap150/pgam-analytics-digest/pull/3) | Row cap 1k→10k (it was eating small sources' tails), loopback referrers filtered out of the landing-page section, `fmtDuration` no longer renders 659.7s as "10m 60s". |
| [#4](https://github.com/mastap150/pgam-analytics-digest/pull/4) | Strips `utm_*` / `fbclid` / `dicbo` and other click IDs before grouping, so one destination is one row. |

The gap that prompted all three: a bare top-5 list with a session count
told us a referrer's *volume* and nothing about *what it picked up* or
*whether the visitors read anything*. On 2026-08-24 it also silently
dropped `chatgpt.com` off Destination's list entirely — NewsBreak and
Google spiked past it — which read as "ChatGPT stopped sending traffic"
when it had gone 33 → 35.

## 1. NewsBreak — nobody submitted anything

**637 sessions, 19.6% of Destination's day, second only to direct.**

`docs/seo-playbook.md` in `destination-com` lists distribution as 8/10
with "Newsbreak/SmartNews/Taboola **pending**". No approach was made,
and NewsBreak's own [publisher
program](https://help.newsbreak.com/hc/en-us/articles/36837190635405-How-do-I-deliver-my-content-to-NewsBreak)
requires an application, approval, and a manually uploaded feed.

**The likely vector is `src/app/feed/msn-news/route.ts`** — a public,
unauthenticated, full-text MRSS feed. It ships complete `body_html` in
`<content:encoded>`, a `media:thumbnail`, and a "Continue reading at
destination.com" backlink. Built for MSN Start. It also happens to match
NewsBreak's published feed spec almost exactly: full articles not
snippets, at least one image each.

Two things sharpen this:

- **It is not the revenue-share program.** NewsBreak's docs say a feed
  that sends readers to your own site can get you removed — and we are
  getting 637 clickthroughs. So this is their aggregation / link-out
  surface, not a partnership.
- **It is one pickup, not a pipe.** 633 of 637 sessions (99.4%) landed
  on a single article, 3 on a second, 1 on a third. Referrer host is
  `newsbreakapp.com`, i.e. the in-app browser, so it surfaced in the app
  feed. The article — "Breeze Airways pauses four Florida routes" — is
  squarely NewsBreak's local-news product, and the story was picked up
  across the aviation press that day.

**Not established:** which route NewsBreak actually used. Two checks
settle it — whether a NewsBreak publisher account exists under our name,
and whether Vercel logs show a NewsBreak crawler UA on `/feed/msn-news`.

**Quality:** 1m 1s, 1.02 PV/session. They read the one article and
leave. 637 sessions ≈ 637 article reads with near-zero site
exploration — real reads, but no funnel.

**Scale context:** `vercel.json` runs `/api/cron/news-ingest` every 2h
(12×/day), capped at 5 clusters per run, and the health check documents
the expectation as "3-5 items every 2h under normal operation" — so
roughly **36–60 articles/day, ~250–420/week**, synthesized from 9 RSS
sources. NewsBreak picked up **2 of ~300** that week. The lever is the
pickup rate, not the production rate. (Exact volume needs
`SELECT count(*) FROM news_items WHERE synthesized_at > now() -
interval '7 days'`; not run here.)

## 2. ChatGPT — best-quality traffic, and the wrong lesson is easy to draw

**35 sessions on Destination (1.1%), 6 on BoxingNews (0.2%).** Present
in the top 5 nearly every day since late May at 8–50/day; peaks 50
(7/29), 49 (7/21), 46 (8/2).

**It is the highest-quality traffic on the site.** 3m 46s average
session against NewsBreak's 1m 1s and direct's 0m 49s; 1.49 PV/session.
BoxingNews's 6 sessions averaged 4m 18s.

Landing pages, Destination 8/24:

| Page | Sessions |
|---|---:|
| `/news/united-airlines-new-international-routes-a321xlr-debut` | 7 |
| `/careers` | 5 |
| `/neighborhoods/los-angeles/west-hollywood` | 2 |
| `(not set)` | 1 |
| `/chat-with/athens/neighborhood-to-avoid` | 1 |
| *19 more pages, 1 session each* | 19 |

**The obvious read — "write more like the A321XLR piece" — is wrong.**
20 of 35 sessions are spread across 19+ distinct pages at one session
each. There is no repeatable winner: one modest head and a very long,
very flat tail. And the tail is not news — it is
`/neighborhoods/...`, `/chat-with/athens/...`, `/careers`. BoxingNews is
the same shape: 6 sessions, 6 different pages, all singletons.

So the lever is **breadth of specific, answerable pages**, which is what
the programmatic templates (`neighborhoods/[city]/[slug]`,
`chat-with/[city]/[question]`, comparison pairs) already produce. They
are earning citations one query at a time. This is the first evidence
the playbook's "more country×month, more airport→dest lanes" line pays
into the AI channel specifically.

Also worth noting:

- **Access is not the constraint.** `src/app/robots.ts` already
  explicitly allows GPTBot, OAI-SearchBot, ClaudeBot, PerplexityBot and
  Google-Extended, and `public/llms.txt` exists.
- **Perplexity, Gemini and Copilot sent zero** on 8/24 despite being
  allowed. ChatGPT is the entire AI channel right now — worth knowing
  before investing across "AI search" generally.
- **`/careers` at #2 is a brand-entity query.** People are asking
  ChatGPT about the company, not about travel.
- **GA4 reclassified ChatGPT mid-flight.** It reported as
  `chatgpt.com / (not set)` through May–June and flipped to
  `ai-assistant` around 6/26. Any trend spanning that date needs both
  labels. The digest's rollup now matches on source host as well as
  medium for this reason.

### Highest-value follow-up: the crawl→citation funnel

`destination-com` already logs AI-crawler hits — `src/lib/ai-bot.ts`
detects 10 crawlers and feeds `/admin/ai-bots`. But `bucketPath()` keeps
only the first path segment, so it can say GPTBot hit `/news` and never
*which* article. That is the same blind spot the digest just fixed on
the GA side, and with a 19-page flat tail this channel cannot be reasoned
about from bucketed data. Fixing it gives: which URLs OpenAI fetched vs
which URLs ChatGPT then sent readers to.

## 3. Newsletter — all buy-in, no return

`newsletter / email` has appeared in the digest **once, ever**: 8/19, 27
sessions. Per the caveat above, the top-5 history cannot prove the other
days were zero — but 8/24's 10-deep table gives a hard bound: **below 13
sessions**, the tenth row.

The telling part is the other direction. `/newsletter/inspiration` was
the **#3 page with 319 views**, and `facebook / cpc` ran 235 sessions at
1.43 PV/session. **We are buying subscribers into the signup page and
getting almost no return traffic out of sends.** The gap is send-side
activation, not capture.

## 4. Outbrain is sending paid-shaped traffic to BoxingNews

`outbrain / referral` (24 sessions) plus `paid.outbrain.com / referral`
(8) = **32 sessions, every URL carrying a `?dicbo=` Outbrain click ID,
at 12.5% engaged and 0m 4s average**.

If that is PGAM spend it is buying bounces. If it is not ours, worth
knowing who is sending it. **Unresolved — nobody in this session could
confirm whether an Outbrain campaign exists.**

## 5. `storage.googleapis.com` is 10.7% of BoxingNews and not human

**410 sessions — 10.7% of the day — at 0.0% engaged, 0m 0s, 1.00
PV/session**, spread across 24+ articles. Running 174–425/day across the
committed series. A scraper or a render harness, not readers.

This matters beyond hygiene: it inflates BoxingNews's session count by
roughly a tenth, and the existing in-market/off-market split does not
catch it because the traffic is US-shaped.

Also present and unexplained: `msn.com / referral`, 10 sessions across 7
articles — MSN still trickles despite the playbook recording it as
"submitted then dropped".

## 6. A dev machine reports into the production GA4 property

Four `127.0.0.1:<port>` referrers, 11 sessions, all to
`/destination.html`. Small, but it means local development writes into
prod analytics. PR #4 keeps them out of the distribution section; it
does not stop them being collected.

## Open items

1. **Outbrain** — is there a live campaign against BoxingNews? (§4)
2. **`storage.googleapis.com`** — identify and, if it is a bot, filter
   it. 10.7% of sessions. (§5)
3. **`bucketPath()` in `ai-bot.ts`** — keep the full path so the
   crawl→citation funnel becomes answerable. (§2)
4. **Newsletter sends** — why does an owned channel with a 319-view/day
   signup page return under 13 sessions? (§3)
5. **NewsBreak** — confirm the ingestion route, then decide whether
   formal onboarding is wanted. Note the tradeoff: the official program
   is on-platform reading with revenue share, which would likely
   *convert* today's 637 clickthroughs into on-platform reads and lose
   the site sessions and ad revenue that come with them. (§1)
6. **Dev GA4 pollution** — gate the measurement ID by environment. (§6)
