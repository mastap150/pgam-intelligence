# BoxingNews fight-week plan — Romero vs Teofimo Lopez, Sat 22 Aug 2026

Owner: Priyesh · Window: Wed 19 Aug → Tue 25 Aug 2026 (all times ET)
Surfaces: boxingnews.com (Sanity) · MSN feed (partner `AA1lKiff`, $4 CPM) · MSN video

## 1. The event

| | |
|---|---|
| Main event | Rolando "Rolly" Romero (17-2) vs Teofimo "The Takeover" Lopez (22-2), 12 rds, 147 lbs |
| At stake | Romero's WBA welterweight title — his first defence |
| Venue | T-Mobile Arena, Las Vegas |
| Broadcast | PBC PPV on **DAZN PPV** and **Prime Video PPV** |
| Timings | Prelims from ~6:00pm · PPV card 8:00pm · main-event ring walks ~10:00pm |
| Market | Lopez favourite (~-285 / 5-11), Romero dog (~+225 / 2-1) — reconfirm Fri/Sat, it will move |

Undercard (as announced): Gary Antonio Russell vs Victor Santillan · Carlos Utria vs
Israel Mercado · Marco Romero vs Kahlil Mitchell · Yoenli Hernandez vs Francisco Veron ·
Blancas vs Mamone · Benjamin Johnson vs Jose Rodriguez.

**Three storylines that carry the week** — every piece should ladder to one:

1. **Lopez moving to 147 for a third divisional title.** The record-chase framing. Best
   evergreen search intent ("can Teofimo win a title in a third weight class").
2. **Former sparring partners, real needle.** Romero's trash talk vs Lopez's cold act.
   This is the social/clip engine — every presser line is a short.
3. **First PBC world-title fight of the DAZN era.** Industry angle. Low volume, high
   authority; one piece, links into everything else.

## 2. Content plan

### Lane split

The site's three ingest lanes each get a job this week. Do not let breaking eat the
whole budget — the programmatic evergreen pages are what still earn on Sunday night.

| Lane | Fight-week job | Volume |
|---|---|---|
| `breaking-news` | Weigh-in result, late changes, result + aftermath | ~8 pieces, Fri–Sun |
| `trending-now` | Presser quotes, face-off, social flashpoints | ~10 pieces, Wed–Sat |
| programmatic | Event hub, how-to-watch, tale of the tape, odds pages | ~8 pieces, front-loaded Wed–Thu |

### The slate

Use the URL templates already live on the site — do not invent new shapes:

- `/event/rolando-romero-vs-teofimo-lopez-2026-08-22` — **the hub.** Full card, start
  times, how to watch, tale of the tape. Publish **Wednesday**, then update in place
  through the week (weights Friday, results Saturday night). This page is the one asset
  that should still be ranking on Monday; everything else links to it.
- `/news/rolando-romero-vs-teofimo-lopez-preview-betting-tips` — the preview/tips piece,
  matching `…-preview-betting-tips` (see Shields–Scott, Diaz–Perry). Thursday.
- `/news/romero-lopez-odds-…` — odds-move piece, matching the `…-odds-…` UFC template.
  Friday after the weigh-in, when the line has actually moved.
- `/schedule/upcoming-2026` and `/odds` — make sure both surface the card at the top by
  Wednesday. These are the two hub pages that catch drive-by search all week.

Per-day:

**Wed 19** — Event hub live. Tale of the tape. "How to watch / what time" piece.
Lopez-to-147 record-chase feature. Seed `/odds` with the opening line.

**Thu 20** — Final presser coverage (2–3 quote pieces from the trending lane, one per
flashpoint, not one omnibus). Preview + betting tips. Undercard piece on Gary Antonio
Russell — he is the only undercard name with independent search volume; the rest get one
combined "undercard preview" and no more.

**Fri 21** — Weigh-in result within 20 minutes (breaking lane, this is the day's peak).
Face-off reaction. Odds-move piece. Refresh the hub with official weights.
Also: **Amanda Serrano vs Lucrecia Manzur is Friday night on TikTok Live** — cheap,
uncontested traffic the night before. One preview, one result. Do not skip it because
the big card is Saturday.

**Sat 22** — Prediction/pick piece by noon. Undercard results as they land, one piece per
notable finish. **Main-event result inside 10 minutes of the decision** — this single
article is the largest read event of the week; have the shell drafted, the two headline
variants pre-written, and a body that only needs the outcome pasted in. Then: post-fight
reaction, scorecard breakdown, what-next for the winner.

**Sun 23** — Aftermath day and it is under-served: full-card results recap, "what's next
for Teofimo / Rolly", ratings & buyrate chatter, best-and-worst of the card. Sunday reads
are a large share of a fight-week total and the competition has gone quiet.

**Mon 24** — `boxingnews_weekly_review` runs 09:30. Read the strategy JSON before
commissioning next week (§5).

### MSN feed rules for this week

- **Headline patterns:** pull `winning_patterns` from the most recent
  `pgam_direct.msn_weekly_review` row and bias the tuner to those. Do not pick P-numbers
  by feel — the taxonomy lives in the boxingnews repo (`src/lib/msn/strategy.ts`) and the
  last review already ranked them on real reads.
- **Avoid list is not optional.** `strategy.avoid_content_words` and `avoid_phrases` are
  populated from real MSN rejections. Fight week is exactly when a generator reaches for
  "blasts", "rips", "warns" — those framings are in the rejection sample. A rejected
  result article on Saturday night is the single most expensive failure available this
  week.
- **MMA check.** The weekly review flags when MMA out-reads boxing on MSN. If the last
  run said MMA-lead, keep the normal MMA cadence running alongside this card — do not
  starve the lane that is actually paying for a one-night boxing spike.
- **Titles:** run the title-change candidate check Sunday, not Monday. Anything from the
  card sitting under 60% of cohort median gets a Partner Hub rewrite while it is still
  inside the traffic window.

## 3. Betting plan

The site already runs betting content (`/odds`, `…-preview-betting-tips`, UFC odds
pieces). This card is the biggest boxing betting event of the month; the plan is to feed
that existing machine rather than build anything new.

**Assets**

1. **Odds hub entry** — Romero vs Lopez pinned on `/odds` from Wednesday, with the line
   refreshed Wed / Fri-after-weights / Sat-morning. Three refreshes, timestamped.
2. **Preview & betting tips** (Thursday) — tale of the tape, form, styles, a stated pick
   with reasoning. Follow the house template exactly.
3. **Odds movement piece** (Friday) — "why the line moved after the weigh-in". Only
   publish if it actually moved; a movement piece with no movement is filler.
4. **Prop/method-of-victory piece** (Saturday morning) — KO round bands, decision, over/
   under rounds. This is where the real betting search volume sits on fight day.
5. **Undercard tips block** — one combined piece, inside the preview, not standalone.

**Positioning.** Lopez -285 favourite over the sitting champion is the story. Romero's
route is one punch and Lopez has been dropped before; the honest angle is "the dog price
is live, the favourite price is not". Write the pick, own it, and grade it Sunday — a
short "how our picks did" post is the cheapest trust-building asset in betting content
and almost nobody does it.

**Monetization.** Odds pages carry affiliate placement; the preview and prop pieces carry
in-body sportsbook links. Confirm which books have live US/UK offers on this card before
Thursday so the links are not dead on the highest-traffic day.

**Compliance — the part that can cost money.**

- **Keep betting content out of the MSN feed lane.** MSN's partner content policy is
  restrictive on gambling promotion, and an account-level strike costs far more than the
  reads a tips article would have earned. Betting pieces stay on-site (search + social +
  affiliate); the MSN lane gets preview, how-to-watch, result, and reaction only.
  Verify the current MSN policy wording before overriding this.
- Every betting page carries 18+/21+ and responsible-gambling wording, and no affiliate
  link fires in any MSN-syndicated body.
- If a betting piece slips into the feed, it will show up in the rejection Details CSV.
  Pull a fresh Content Rejection Report Monday and check `failure_category` before
  assuming the week was clean.

## 4. Video plan

**MSN video is already instrumented and under-used.** `agents/etl/msn_insights_etl.py`
tracks `content_type=4` (video) with `video_start_count` and 25/50/75/100 completion
counts. Video is a separate, measured MSN surface — this card is the right week to put
real volume through it.

**Ship five to eight verticals across the week:**

| # | Piece | Cut | When |
|---|---|---|---|
| 1 | Tale of the tape / stats explainer | 45–60s | Wed |
| 2 | Lopez's third-title chase | 60s | Wed |
| 3 | Presser flashpoint reaction | 30s | Thu, same day as the quote |
| 4 | Weigh-in + face-off | 30s | Fri, within the hour |
| 5 | How to watch / start times | 20s | Fri |
| 6 | Prediction + the betting angle | 45s | Sat AM — **on-site/social only, not MSN** |
| 7 | Result reaction | 45s | Sat night, immediately after the result article |
| 8 | What's next for the winner | 60s | Sun |

**Production.** Use the Higgsfield MCP (shorts studio + `generate_video`) for stats
explainers, record-chase pieces and the how-to-watch cut — these are graphics-and-VO,
no rights problem. Reaction and result cuts run as commentary over licensed stills or
original graphics.

**Rights, stated plainly:** do not cut fight footage, weigh-in stream footage, or
broadcast presser video. PBC/DAZN/Prime footage is not ours. Stills under licence,
original motion graphics, and commentary only. A takedown on the MSN video surface
jeopardises the article lane too.

**Distribution.** Every vertical is embedded in its parent article first (video on-page
lifts the article's own dwell), then pushed to the MSN video surface where policy allows,
then social. There is no boxingnews YouTube channel — if one is wanted, this card is a
reasonable week to start it, but treat that as a separate decision, not part of this plan.

**Measure it.** Video is worth continuing only if the completion curve holds. After the
week, pull `video_start_count` vs `video_viewed_100_count` per piece from the insights
table. Anything under ~30% completion is the wrong format, not the wrong topic.

## 5. What to check on Monday

`boxingnews_weekly_review` runs Mon 09:30 ET and covers Mon 17 → Sun 23, so this whole
fight week lands in one review window. Before reading any of it:

1. **Coverage first.** If `coverage_pct` < 90% the reads are an undercount and nothing
   below is trustworthy — fix the puller and rerun. A fight week measured through a
   half-down puller is a wasted measurement.
2. Fight-week reads vs the prior week — that delta is the honest value of a big card.
3. Which lane won: did breaking (result, weigh-in) beat programmatic (hub, how-to-watch)?
   The answer decides how much of next big card's budget goes to pre-built evergreen.
4. Did the Sunday aftermath slate earn? If yes, it becomes standing policy for every
   PPV weekend.
5. Video completion rates (§4).
6. Rejection funnel — any rejection during fight week is worth a root cause, not a shrug.

## 6. Caveats

- Card, odds and timings above come from public reporting on 17–19 Aug 2026 and were not
  verified against boxingnews.com itself — the domain is blocked by this session's egress
  proxy. Reconfirm the line and the running order Friday; undercards move.
- No live numbers in this plan. This session has no `BOXINGNEWS_DATABASE_URL` or
  `PGAM_DIRECT_DATABASE_URL`, so nothing here is calibrated against actual per-article
  reads. Volumes are structural recommendations, not fitted to measured capacity.
- The P1–P6 headline taxonomy lives in the boxingnews repo, not this one. Pull the
  winning patterns from the last `msn_weekly_review` row rather than guessing them.
