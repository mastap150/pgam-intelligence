# destination.com — Publish Plan

52 drafts in this directory covering all 50 articles in `04_article_ideas_destination.md`,
plus two proposed additions. This document is the sequence, the blockers and the maintenance
commitment. Read it before publishing anything.

**Companion files:** `affiliate-placements.csv` (machine-readable link map),
`pre-publish-checklist.md` (per-article verification items).

---

## Two things to know before you start

### 1. Every draft needs an editor pass, and 32 of 52 need expansion

`07_content_templates.md` sets 1,800–2,500 words for destination guides and 1,500–2,000 for
planning guides. **Thirty-two of the 52 article bodies fall below 1,500 words.** They are
complete — structure, FAQ, disclosure, bottom line all present — but they are lean, and the
later batches are leaner than the earlier ones.

The shortest are `kyoto-hidden-temples` (1,101), `lisbon-3-day-itinerary` (1,138) and
`paris-restaurants-guide` (1,160). The earliest pieces — Tour de France, Camino, solo travel,
Dolomites — are the fullest at 2,200–2,700.

**Treat expansion as part of the edit, not as a separate project.** The structure and the
verified facts are there; what is thin is the connective writing.

### 2. Nothing here has been fact-checked by a human

Facts were verified against published sources at time of writing and every draft names its
sources in the editor notes. But **no cost table has been spot-checked, no ticket price
confirmed against an issuer or operator, and no link click-tested.** The
`pre-publish-checklist.md` file lists 36 specific items. They are not optional.

---

## Publish order

### Tier 0 — blocked, do not publish yet

| Article | Blocker |
|---|---|
| `cuba-travel-guide.md` | **Legal review.** Describes US federal regulations enforced by OFAC to a US audience |
| `sapphire-reserve-vs-amex-platinum.md` | **Compliance review.** Card comparison monetised per approval; FTC disclosure above the fold; verify every credits figure against issuer terms |

### Tier 1 — publish first (hub and dependencies)

**`travel-insurance-guide.md` goes first.** Six other articles link into it — Bali (scooters),
Patagonia, safari, gorilla trekking, long-term travel, solo travel — and publishing it after
them wastes the internal-link equity. It is also the highest-margin category in
`08_monetization_strategy.md` at 10–15%.

Then, in order:

1. `travel-insurance-guide.md` — hub
2. `how-to-avoid-tourist-traps.md` — second hub; seven articles reference it
3. `solo-female-travel-safety.md` — third hub; referenced by India, Colombia, Camino
4. `camino-de-santiago-guide.md` — EF CTA, 2027 Holy Year, strongest single EF match
5. `tour-de-france-2027-guide.md` — EF CTA, dated hook

### Tier 2 — dated hooks, publish to a calendar

These have real deadlines. Publishing them late wastes the reason they exist.

| Article | Publish/promote by | Why |
|---|---|---|
| `japan-2-week-itinerary.md` | **Late September 2026** | JR Pass prices rise 1 October — promote "buy before the rise", then update the figures that week |
| `dolomites-hiking-guide.md` | **Publish now, promote early December** | Rifugio booking opens December–February; peak dates gone by March |
| `japan-cherry-blossom-season.md` | **Publish now, update January** | First 2027 bloom forecasts publish in January; that is the search spike |
| `europe-packing-list.md` | **Verify, then publish** | ETIAS revised EU timeline expected autumn 2026 — i.e. now |
| `camino-de-santiago-guide.md` | **Now** | 2027 Holy Year planning is happening this year |
| `tour-de-france-2027-guide.md` | **Now, update late October** | Full route announced end of October 2026 |

### Tier 3 — the rest

Publish in the order of `04_article_ideas_destination.md`'s own priority sequence (Month 1,
then 2, then 3), except that anything in Tier 1 or 2 above jumps the queue.

---

## The EF Adventures placements

Seven articles carry an EF CTA. All route through `/api/go/cj/ef-adventures?link={id}` so
clicks write an `affiliate_clicks` row, matching the Hotels.com and Vrbo pattern in
`src/data/cj-advertisers.ts`.

| Article | Link ID | Promo ends |
|---|---|---|
| `camino-de-santiago-guide.md` | `17167876` | 28-Jun-2028 |
| `tour-de-france-2027-guide.md` | `17133008` | 28-Jun-2028 |
| `dolomites-hiking-guide.md` | `17167875` | 28-Jun-2028 |
| `greek-islands-comparison.md` | `17167878` | 28-Jun-2028 |
| `croatia-island-hopping.md` | `17167880` | 28-Jun-2028 |
| `portugal-lesser-known-places.md` | `17167879` | 28-Jun-2028 |
| `solo-female-travel-safety.md` | `17315884` | 30-Nov-2027 |

**Before any of these go live:**

1. **Click-test each link by hand.** This session could not — the egress proxy blocks all five
   CJ redirect domains. At $250 a booking, a link landing on the wrong tour is expensive.
2. **`17308033` is excluded everywhere** — its anchor copy advertises a Mallorca cycling tour
   under a Portugal name. Do not substitute it in.
3. **`17315882`** is the better topical match for the Tour de France piece but its promo window
   closed 7 September 2026. Ask CJ whether that was an error — it sells a 2027 departure — and
   swap it in if extended.

**Still unanswered by CJ:** cookie window, and whether the $250 fires on the $1,000 deposit or
on final payment. Both change the expected return materially. See
`docs/affiliate/ef-adventures-evaluation.md`.

---

## Affiliate lines: what is live and what is not

**Confirmed live** (`docs/expedia-affiliate-decision.md`): Expedia via Partnerize,
Hotels.com CJ, Vrbo CJ, Viator/GetYourGuide for activities.

**Listed as opportunities in `08_monetization_strategy.md`, NOT confirmed live:** World
Nomads, SafetyWing, Allianz (insurance), and the credit card programmes.

**Eight articles depend on an unconfirmed programme** — the four Points & Miles pieces, plus
insurance, cheap flights, long-term travel and transfers. **Do not publish placeholder links.**
Either confirm the programme first or publish the article without a CTA and add it later.

**Unexploited categories flagged across the drafts**, in rough order of how much traffic these
articles send at them: car hire (Amalfi, Tuscany, Iceland, Cape Town, Costa Rica), eSIM
(travel apps, packing list), gear (packing list), rail booking (Vienna/Prague, Europe train),
cruise (Galápagos), JR Pass resellers (both Japan pieces). None are in the monetization doc.

---

## Maintenance commitment

This is the part that decides whether the content stays worth having. **Ten articles need
review more often than annually.**

| Cadence | Articles |
|---|---|
| **Quarterly** | `best-business-class-redemptions`, `how-to-earn-miles-without-flying`, `hotel-loyalty-programs-ranked`, `sapphire-reserve-vs-amex-platinum`, `transfer-points-to-airlines`, `flying-business-class-cheap`, `cuba-travel-guide`, `europe-packing-list` (until ETIAS launches) |
| **Every six months** | `machu-picchu-guide` (circuit system restructured twice since 2024), `best-travel-apps` |
| **Annual** | 28 articles — see each file's `REVIEW DATE` |
| **Dated one-offs** | 14 articles with a specific trigger date in front matter |

**The five Points & Miles pieces should be maintained together** as a cluster — they
cross-reference each other's figures, and updating one without the others creates
contradictions on the site.

---

## Deliberate departures from the plan

Each is argued in the relevant file's editor notes. Listed here so they are visible as
decisions rather than discovered as inconsistencies.

| Plan said | Drafted as | Reason |
|---|---|---|
| Solo travel: country-by-country ranking | Risk types and practices | The index measures resident conditions, not visitor risk |
| Portugal: "Hidden Gems" | "Lesser-Known" | Plan contradicts the house style guide's banned-word list |
| Paris: "18 restaurants" | Areas and method | Named restaurants date within a year |
| Bangkok: named stalls | Neighbourhoods + stall selection | Vendors move; named stalls acquire queues |
| Travel apps: "15 best, tested on real trips" | Categories + criteria | Claims testing not done; app lists rot fastest of anything |
| Business class: "8 sweet spots (2025)" | 6, no year in title | Six are verifiable; padding to eight means inventing two |
| Transfer points: partner tables | Process + criteria | Stale ratio tables actively cost readers money |

**Two additions not in the plan:** `tour-de-france-2027-guide.md` and
`dolomites-hiking-guide.md`, both written because EF had inventory and the plan had no article.

---

## What is missing and should probably exist

- **An EES/ETIAS article.** Currently a section inside the packing list. High-volume,
  high-confusion, unserved, and there is an active industry selling ETIAS registrations for a
  scheme that does not exist yet. Strongest single content gap identified.
- **A city-level safety index piece**, using street-safety-at-night data rather than the
  composite WPS index — the honest version of the ranked list the solo travel plan asked for.
- **The budget spreadsheet as a real downloadable.** `08_monetization_strategy.md` already
  lists itinerary templates as a digital product; `travel-budget-template.md` is a natural lead
  magnet and a newsletter signup converts better than any affiliate placement on that page.

---

## Honest assessment

The strongest thing about this batch is that most articles lead with a specific verified fact
that competing pages get wrong — the ZTL cameras, the 2025 Compostela rule change, the JR Pass
break-even, EES being live while ETIAS is not, the Machu Picchu circuit system, Santorini's
cruise cap. That is what will differentiate them, and it is also what will go stale.

The weakest thing is that this is 52 unedited first drafts written in one session by one
writer with no fact-checking pass. **Publishing them as-is would be a mistake.** Publish five,
measure, then decide whether the remaining 47 are worth the editing time — which the click
data will answer better than any estimate in this document.

---

## Appendix: where the affiliate placements actually point

Counted across all 52 drafts (`affiliate-placements.csv` is the per-article detail).

| Line | Articles | Status |
|---|---|---|
| Accommodation (Expedia/Partnerize, Hotels.com CJ, Vrbo CJ) | 32 | **Live** |
| Viator / GetYourGuide activities | 21 | **Live** |
| EF Adventures CJ | 7 | **Live**, links need click-testing |
| Travel insurance | 11 | **Not confirmed live** |
| Credit card | 6 | **Not confirmed live** |
| Car hire | 5 | **No programme exists** |
| eSIM | 1 | **No programme exists** |
| Display only | 8 | n/a |

**Read that table as a priority list for business development**, not just as a build spec. The
two live lines already cover 53 placements, which is most of the site. The 17 placements
waiting on insurance and card programmes are concentrated in the highest-margin categories in
`08_monetization_strategy.md` — insurance at 10–15% and cards at $50–$200 per approval — so
confirming those two programmes is worth more than writing more articles.

Car hire is the largest unserved category by article count: Amalfi, Tuscany, Iceland, Cape
Town and Costa Rica all actively recommend renting a car, and none of them can currently
monetise that recommendation.
