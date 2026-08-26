# The relevance gate, and what the live corpus actually looked like

**2026-08-26.** Written after the throttle landed, from reading what the
pipeline actually published rather than what it was designed to publish.
Companion to `destination-news-audit-2026-08-25.md` (the audit),
`destination-content-strategy-2026-08-26.md` (the plan) and
`destination-news-feedback-loop-2026-08-26.md` (the measurement loop).

## The finding

Cutting volume did not improve quality, because volume was never what
selected the stories.

The throttle took publishing from ~55 articles/day to 4 (cap 5→2, cron
`0 */2 * * *` → `0 11,15 * * *`). The first fully-throttled run, 15:00 UTC on
2026-08-26, published two stories. One of them was:

> US Air Force Awards Boeing $266M to Digitize C-17 Cockpit Before Parts Run Out

That is a defence-procurement story on a travel site, and it was not an
outlier. Pulling all 114 headlines live on the site and reading them one by
one, **63 of 114 have no bearing on planning or taking a trip.** They fall
into four families:

| Family | Examples from the live corpus |
|---|---|
| Military aviation | B-52J modernisation over budget; Travis AFB and Nellis AFB installations; Tyndall AFB hurricane rebuild; P-51 Mustang design features; UK cannot replace Red Arrows |
| Hotel-trade B2B | STR lifts RevPAR outlook; Maestro PMS advocates standardised data; BirchStreet Systems launches AI-driven smart AP platform for hospitality; OTA guest data expiration leaves hotels without contact lists |
| Corporate finance | IHCL to merge with Oriental Hotels in all-stock deal; LATAM secures $505M financing; BNP Paribas sells Room Mate Macarena for €80M |
| Appointments and awards | StayTerra names new CEO; Jagruti Panwala named Peggy Berg Castell Award winner; Skift announces 32 IDEA Award winners |

The trade-B2B family is the largest and the least obvious. It arrives via
Hospitality Net, Skift, PhocusWire and Travel Weekly — all legitimate travel
publications, all writing for **hotel operators**, not for travellers. A
pipeline reading those feeds will produce operator news indefinitely and
every quality signal upstream will say it is fine.

## Why nothing upstream caught it

`clusterCandidates()` ranks by corroboration — how many distinct outlets
filed the story. That is a good proxy for *is this real* and a bad one for
*does our reader care*. The C-17 story was carried by more aviation blogs
than the visa change it outranked, so with a cap of two, it published and
the visa change did not.

The opportunity scorer (`news-opportunity.ts`) scores for NewsBreak
locality, Google News freshness and AI citability. All three are surface-fit
measures. None asks whether the subject matters to a traveller.

So the gate is a new question, not a tuning of an old one.

## Shape of the fix

`destination-com/src/lib/news-relevance.ts`. Deterministic, unit-tested,
runs before the dedup query so an off-topic cluster costs neither a database
round trip nor an Anthropic call.

- Four negative families (military, trade-B2B, corp-finance, personnel) and
  eight positive ones, weighted by how directly a story changes what a
  reader would do — a cancelled route or a new visa rule is actionable
  today; a wellness concept at one property is atmosphere.
- **Relevance leads the ranking; corroboration only breaks ties.** This is
  the part that fixes the C-17 case, more than the reject threshold does.
- Blockers are weighted, not absolute. A tier-one signal (route, disruption,
  entry rules) damps the softer blockers to 40%, so "Southwest Cancels 300
  Flights After IT Outage; CEO Steps Down" still publishes. Military is
  excluded from that damping on purpose.
- `ADMIT_FLOOR = 7`, chosen off a sweep: 54 admitted at 1, 53 at 3, 50 at
  both 5 and 7, 37 at 9. Seven sits on the plateau, so it is not balanced on
  a knife edge.

`?dry=1` on `/api/cron/news-ingest` reports every rejection with its score,
matched signals and matched blockers, so a bad call can be argued with.

## Two process lessons worth keeping

**A regex convention that silently ate real matches.** The first draft
compiled term lists as `\b(?:a|b|c)\b`. A trailing boundary binds to the
whole alternation, not to each branch — so "fighter jet" missed "Fighter
Jets", "B-52" missed "B-52J", "bag siz" missed "bag sizers". All three are
real corpus headlines that scored zero and would have published. This is the
same defect class as the three SQL regex bugs in the audit queries
(`US` matching the pronoun "us", `ground` matching "Groundbreaking"). The
convention is now leading boundary only, with terms that prefix unrelated
words carrying their own: `fee\b`, `dies\b`, `fares?\b`, `miles\b`.

**Calibrate against the corpus, not against examples you invent.** Every
rule above exists because a real published headline demanded it. The
invented-example version of this file would have caught the C-17 story and
missed the entire trade-B2B family, which is four times larger.

## Still open

- The dedup stack has a gap: "Delta Flight Attendant Commutes 5,100 Miles
  From Ghana To New York Base" and "Delta Flight Attendant Commutes from
  Ghana to New York Base" are both live. Same story, two articles.
- The gate is a keyword model. It will drift as feeds change. The Monday
  `news-editorial-review` is the intended place to notice that, but it has
  not yet completed a cycle against a gated corpus.
- No performance data flows back into the gate. Nothing yet connects "this
  story got 633 views" to "admit stories like it". That remains the
  unfinished half of the feedback loop.
