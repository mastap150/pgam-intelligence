# Attune self-serve prototype — engineering handoff

**Prototype:** `docs/dsp/prototype/` in this repo · published at
https://claude.ai/code/artifact/3f4eded5-4205-4927-b47e-6133a6b06d00
**Companion:** `docs/dsp/ss-dashboard-ab-comparison.md` (the A/B review the design came out of)
**Written:** 2026-08-26, revised 2026-08-27. Verified against `pgam-dsp-dashboard@c155b18`-era `main`.

Twelve screens, five campaign detail pages, a five-step builder. This
document is the part that does not survive in a picture: which constraints
in the prototype are real, which figures are modelled, and what has to be
decided before anyone starts building.

Read it as four lists — **verified**, **modelled**, **does a backend exist**,
**open**. If you only read one section, read §3: it maps every control in the
prototype to whether SpringServe can actually do it.

### What changed on 27 August

Thirteen screens now, not twelve. Since the first draft of this document the
prototype gained:

- **PGAM Optimized Network** as the default way to buy on builder step 3 — one
  line across all 53 destinations, rebalanced weekly, with the catalogue kept
  visible but locked underneath it. Choosing channels by hand is the second
  option, not the first.
- **A help centre**: 40 questions across 7 topics, nine of them on attention,
  with search that filters on answer text.
- **Reach and frequency as reported metrics** — households, times each one saw
  it, cost per household. Vibe reports these and we did not.
- **A selectable attribution window** (1/7/14/30 days) that restates the lead
  count and cost per lead for the window chosen.
- **Seven reporting dimensions, not five** — devices and creative added — and
  **an attention score on every breakdown row**, because
  `attentiveImpressions` is already in the payload and nothing read it.
- **An Integrations screen** grouped by job rather than by vendor: measurement,
  audiences in, results out, app attribution. Five pixel install paths, each
  with its own failure mode.
- **The palette from the PGAM/TripleLift one-pager**, sampled from the PDF's
  colour operators. Two tones darkened for AA; `#1E90FF` restricted to fills
  and rules. See §4.1 — this resolves an open decision.
- **A design-system pass**: 25 type sizes to 9, 14 radii to 4, 13 tracking
  values to 3, five transition durations to three on one curve, spacing onto a
  4px grid, shadows off static cards.
- **§3, a control-by-control backend audit** against
  `pgam-dsp-dashboard@docs/springserve-capability-map.md`. Roughly a third of
  what the prototype shows has no backend today, and three of those are claims
  the prototype makes more forcefully than the shipped demo does.
- ⌘K, a working empty-account mode that reaches every screen, a focus trap on
  the dialog, sortable campaigns, deep-linked builder steps, and a live budget
  forecast.

---

## 1. Verified against the codebase

Three things in the prototype were checked against the shipped source, and
in every case the source disagreed with what the demo currently tells a
user. These are product findings, not prototype findings.

### 1.1 Eight objectives exist; six are offered

`GOAL_LABELS`, `GOAL_OUTCOME` and `GOAL_TO_OBJECTIVE` in
`src/lib/ss/campaign-launch.ts` all carry eight goals. `GOAL_CARDS` in
`src/app/(self-serve)/ss-campaigns/new/page.tsx` renders six — `app` and
`abm` are absent from the grid.

The type-level plumbing is complete: `recommendedAudienceForGoal()`,
`metricsForGoal()`, `budgetRangeForGoal()` and `FORECAST_GOAL_MAP` all have
cases, the `VALID` list in the `?goal=` seed accepts both, and the launch route
maps both to an objective.

> **That is not the same as being able to run them, and an earlier draft of
> this document got it wrong.** The comment directly above `GOAL_CARDS` and
> `docs/springserve-capability-map.md` §A.3 both say the omission is
> deliberate: **there is no mobile-app inventory or attribution, and no
> firmographic targeting.** The `Record<Goal, …>` maps are exhaustive because
> TypeScript forces them to be, not because the capability exists.
>
> So this is a build, not a two-line change. Shipping the cards would put two
> objectives in front of advertisers that the platform cannot deliver against
> or measure — the exact failure the capability map files under "UI implies we
> can, but we can't". The prototype shows them marked *not listed* so the gap
> is visible to a reviewer; it should not ship that way to an advertiser
> without the backend behind it.

Per-goal behaviour the prototype surfaces, all from `campaign-launch.ts`:

| Goal | Objective | Default flight | Pacing | Blocks launch without |
|---|---|---|---|---|
| customers | `leads` | 30d | standard | destination phone number |
| sales | `conversions` | 30d | standard | site tag firing a purchase |
| awareness | `awareness` | 30d | standard | — |
| event | `event` | **14d** | **asap** (front-loaded) | phone number + date |
| website | `traffic` | 30d | standard | site tag |
| retarget | `retargeting` | 30d | standard | site tag + a filled pool |
| app | `app_install` | 30d | standard | MMP postback |
| abm | `abm` | 30d | standard | company list → IP ranges |

`defaultFlightDays()` returns 14 only for `event`; `resolvePacing()` returns
`asap` only for `event`. `CALL_DRIVEN_GOALS` is `{customers, event}`.

### 1.2 DMA targeting works — for 38 of ~210 markets

The wizard disables DMA with "available on request". That is out of date.
`src/lib/springserve/geo-targeting.ts` says DMA no longer targets on the
campaign geo profile (SpringServe never accepted `dma_codes` there) but on
the **demand tag** via `metro_area_codes`, resolved through
`src/lib/self-serve/dma-metro-crosswalk.ts`.

That crosswalk was derived read-only from the SS `/report` API on account
2724, `metro_area` dimension, pulled 2026-07-06 over 90 days. Of its 217
entries:

- **23** matched a Nielsen code exactly
- **15** matched on primary-city + state
- **1** pending
- **172** unmatched

> **So:** 38 DMAs resolve. The rest are dropped — and dropped *silently*,
> which is worse than being refused. The prototype offers the 38 and
> disables the rest with a "no id" badge and the reason.

Regenerating the crosswalk means re-running the metro_area report; there is
no lookup endpoint on our token.

### 1.3 Breakdown spend is derived, not measured

`SelfServeReportingView` (`src/lib/self-serve-reporting-view.ts`) returns
six breakdown dimensions, each as
`{ key, impressions, spend, attentiveImpressions }`:

`devices`, `dayOfWeek`, `hourOfDay`, `dma`, `contentGenre`, `appName`.

Step 7b of `src/app/api/v1/self-serve/reporting/view/route.ts` is the
caveat that matters: the SS fetchers return an *aggregate* billable cost,
so the route replaces each breakdown's spend with a figure rescaled from
impressions at the advertiser's weighted-average gross CPM.

> **So:** impressions and attentive impressions are exact. **Spend in a
> breakdown is approximate for any advertiser on more than one CPM.** Exact
> spend lives only in `totals` and `campaigns[]`. The prototype says this in
> the footnote under "Where it ran" — do not drop that sentence when the
> screen is built.

Two consequences the prototype acts on:

- **`attentiveImpressions` is served and nothing uses it.** Attention per
  dimension is PGAM's actual differentiator and it is already in the
  payload. The prototype shows an attention score on every breakdown row.
- **Leads are not in the payload by dimension**, and should not be faked.
  Attribution resolves to a campaign and a household, not to an hour or an
  app. The prototype says so rather than leaving a reader to wonder.

---

## 2. Modelled, not real

Everything below is invented for the demo and must be replaced by real
queries. It is internally consistent — that was the point — but none of it
came from the database.

| Data | Where | Status |
|---|---|---|
| The 24-week ledger (`WK`) | every figure on every screen | modelled |
| Seven reporting dimensions (`RUN`) | Results → Where it ran | modelled volumes, real shape |
| Attention per breakdown row | Results → Where it ran | modelled, rolls up to exactly 77 |
| Households / frequency / cost per household | Results | modelled |
| Integration states | Integrations | modelled; the partner list is real |
| 53-channel catalogue + reach | builder step 3 | **real catalogue**, reach is eMarketer-class estimate |
| Household counts per radius/ZIP/state | builder step 2 | modelled |
| $42.23 CPM | forecast, reach bar | derived from the modelled ledger |
| Campaign names, wallet history, cards | throughout | modelled |

The single invariant to preserve when it is rebuilt on real data: **every
figure is a sum over one weekly ledger.** Spend, leads, cost per lead,
runway, statements and the reporting window are all slices of `WK`. That
is why 2 Feb → 30 Mar returns exactly the Q1 pilot's $24,000 / 168 leads /
$142.86, and why the four statement months total the same $105,920 /
1,012 leads that Results reports. If the build introduces a second source
of truth for any of them, the screens will start to disagree.

A week is attributed to **the month it starts in**. That is what makes
February + March equal the pilot's whole life and March's $138.87 equal
the "previous 8 weeks" figure on Results. Pro-rating across month
boundaries breaks both.

---

## 3. Control by control: is there a backend?

`pgam-dsp-dashboard@docs/springserve-capability-map.md` is the source of
truth for what SpringServe on account `2724` can actually do. This table
maps every control the prototype puts on screen against it. It exists
because a prototype this finished is easy to read as a spec, and roughly a
third of what it shows has nothing behind it today.

**Legend** (the capability map's, so the two can be read together):
✅ works on our token today · 🟡 exists in SS/Magnite but is not on our
token or is collected and never forwarded · 🔧 possible, but only as a
manual action in the Magnite ClearLine console · ❌ no backend ·
◻︎ modelled for the demo, no claim either way.

### Step 1 — Goal

| Control | Backend | Note |
|---|---|---|
| Six objective cards | ❌ | Objective is **label-only**. SS has no bid strategy, no optimization goal, no outcome KPI — confirmed by two independent traces. The card changes copy, metric names and budget floors in our UI; it changes nothing in the buy. Capability map §A.1, "biggest claim risk". |
| Per-goal budget floor, flight length, prerequisites | ✅ | Our own logic, and it is real — it just sits above an SS buy that is identical whichever card was picked. |
| `app` / `abm` cards | ❌ | Deliberately absent from both the demo and the prototype. See §1.1. |

### Step 2 — Who and where

| Control | Backend | Note |
|---|---|---|
| Radius around an address | ✅ | `targeting_geo_profile`, lat/lon + radius. |
| ZIP list | ✅ | aliased to `postal_codes`. |
| States | ✅ | |
| DMA / metro | ✅ | demand-tag `metro_area_codes`. **38 of ~210 markets** are crosswalked; see §1.2 and §4.3. Mixed DMA + ZIP is untested — DMA sits on the tag, ZIP on the campaign geo profile, and whether SS ANDs or ORs across the two objects is unverified. |
| Starting point (interest pool / recent visitors / everyone) | 🟡 | Audience segments are collected in the UI and **never forwarded to SS**. |
| Age | 🟡 | collected, never forwarded. |
| Gender | 🟡 | collected, never forwarded. |
| Household income | 🟡 | Same — and the prototype **adds** this control; the shipped wizard does not have it. |
| Audience segments (8 chips) | 🟡 | collected, never forwarded. |
| Content categories (IAB Tier 1) | ✅ | Real, with one caveat: we send them at **campaign** scope and Magnite now wants **tag** scope. Tier 2 exists and is hidden. |
| Dayparting — the 168-cell grid | 🔧 | The self-serve daypart shape is **dropped with a warning** before it reaches SS. Dayparting works, but only as a ClearLine console action by ops. The prototype's grid is the most convincing non-functional control in the whole file. |

### Step 3 — Channels

| Control | Backend | Note |
|---|---|---|
| Manual channel / app picker (53) | ❌ | Selections are accepted as input and **never mapped**; the supply tag is hardcoded. Inventory arrives through the deal bridge, not through this control. Capability map §A.7. |
| **PGAM Optimized Network** | ❌ | Nothing behind it at all. It promises a weekly rebalance "toward whatever is producing leads for you" — that requires both channel-level control (which we do not have, per the row above) and outcome optimization (which SS does not have, per §A.1). It is the largest single new claim in the prototype and it is mine, not the demo's. |
| Selected reach (609.2M households) | ◻︎ | eMarketer-class estimate. Labelled as such on the screen. |

### Step 4 — Video

| Control | Backend | Note |
|---|---|---|
| Pick an approved creative | ✅ | Video creative + VAST is real, including the approval state. |
| Upload a new video | ✅ | |
| *(no display-ad generator)* | — | Deliberate. The shipped demo has one; display creatives never attach to an SS tag (§A.5), so the prototype drops it. |

### Step 5 — Budget & review

| Control | Backend | Note |
|---|---|---|
| Monthly budget | ✅ | budget metric `revenue`, period `month`. |
| Daily pace | ✅ | as a `budget_period:"day"` entry. |
| Flight dates | ✅ | must sync **both** the campaign `targeting_time_profile` and the tag's flat dates. |
| Frequency cap (5 per day) | ✅ | `targeting_time_profile.frequency_caps[]`. Real, and currently hidden from the shipped wizard. |
| Live forecast — impressions, leads, range | ◻︎ | There is **no avails or forecast API** on our token (§A.2). The prototype's version at least derives from the account's own CPM and cost-per-lead history and shows a range rather than a point, which is more honest than the shipped Strategy Estimate's constants — but it is still a projection from past delivery, not a forecast of available supply. |
| Rate line in the review list | ✅ | This said "Bidding — Automatic" until the audit; SS on 2724 takes a **static** CPM (`bid_floor_type:"static"`) and there is no automatic bidding to describe. Now reads "Rate — $42.23 CPM, fixed". |

### Results and reporting

| Control | Backend | Note |
|---|---|---|
| Seven breakdown dimensions | ✅ | day, hour, DMA, genre, app, device, creative are all real `/report` dimensions. Under-used today, not missing. |
| Attention score on every row | 🟡 | `placement_attention_scores` and its Lambda already produce the data; self-serve reporting stubs it null. Wiring it is the highest-value item on the capability map's opportunity list — it is the brand differentiator and it is already computed. |
| Attribution window selector (1/7/14/30d) | 🟡 | Needs the planned `ss_conversions` date-window JOIN. Not built. |
| Reach, frequency, cost per household | ◻︎ unverified | Vibe reports these and we do not. Whether SS `/report` supports household-level dedup on our token is **not** covered by the capability map and needs checking before this is promised. |
| Spend per breakdown row | ✅ derived | Real, but computed from impressions × CPM rather than measured per row. See §1.3. |

### Integrations

| Control | Backend | Note |
|---|---|---|
| Site tag, pixels, Conversions API | ✅ | Real end-to-end when authed. An earlier trace called this missing; that was a 401 from a missing `ss_advertiser_id` cookie, not a missing route. |
| Call tracking / TFN | ✅ | Real. Calls attribute back to the campaign. |
| App attribution / MMP handoff | ❌ | No mobile-app inventory or attribution exists. Shown in the prototype as a "not available" state, not as a setup flow. |

### The three that need a decision before anyone demos this

Everything above is a fact about the backend. These three are judgement
calls about what the prototype says on top of it:

1. **Dayparting, age, gender, income and audience segments.** All five are
   collected today and none reach SS. The prototype gave dayparting a
   168-cell drag-selectable grid and added household income and IAB
   categories on top. That made a set of non-functional controls
   considerably more convincing than they were. Either gate them behind
   the ops path that does work (dayparting via ClearLine, as a request
   rather than a control), or mark them as narrowing-only estimates, or
   cut them.
2. **The channel picker.** Selections are not sent. The prototype turned
   the picker into a poster wall of brand marks, which is exactly the
   treatment that reads as "this is the lever". It is not a lever.
3. **PGAM Optimized Network.** The copy promises a weekly rebalance on
   the customer's own results. Two capabilities that do not exist have to
   exist first. As a product direction it is the right one — it is the
   honest framing for a buy where we control the mix and the customer
   does not. As a card in a builder it currently describes a service we
   cannot perform.

None of these is an argument against the design. The point of writing
them down is that a prototype gets sold before it gets built, and these
are the three places where that would cost us.

---

## 4. Open decisions

### 4.0 Palette — decided 27 August

Taken from the PGAM/TripleLift one-pager and sampled from the PDF's own colour
operators rather than a screenshot: `#0B1220 / #33415C / #5A6B87` ink,
`#0E72D9` primary, `#1E90FF` accent, `#D6E6F7` rules, `#F7FBFF` ground.

Its neutrals are navy-tinted rather than grey, which is most of why that deck
holds together, and `#1E90FF` is already the Attune mark's blue.

Two tones are darkened in the prototype because they carry body text and did
not clear AA on the tinted grounds: `#5A6B87 → #596A86` and `#0E72D9 →
#0D68C5`. **`#1E90FF` is used only for fills, rules and the top hairline** — it
reaches 2.67 against every ground in the system and fails as text at any size.
If a designer wants it on type, the type has to sit on a dark ground.

### 4.1 Channel marks — needs a decision, not an implementation

The prototype draws each service's short mark on its own brand colour
(`hulu` in Hulu green, a red N on Netflix black, the YouTube play
triangle). They are CSS and SVG approximations, not the platforms' assets,
and the colours are best-known values rather than brand-book extracts.

The current demo's picker took the other route deliberately — its code
comment reads *"These are CSS text marks, NOT the platforms' copyrighted
logos."*

Three options:

1. **Keep monograms.** No legal exposure, weakest recognition. What the
   demo does today.
2. **Keep the drawn brand marks** (what the prototype does). Strong
   recognition; an approximation of a trademark, used nominatively to
   identify inventory — which is ordinary practice in media planning tools,
   but it is a call for someone other than engineering.
3. **License or source real assets.** Best result, needs a person to
   gather ~53 files and confirm terms for each.

Several marks fall under 4.5:1 against their own tile (Netflix's red on
black is 4.11). WCAG 1.4.3 exempts logotypes and the channel name sits
beside every tile in full-contrast ink, so this is compliant under any of
the three options.

### 4.2 Should the two missing objectives ship?

Not until there is something behind them. See §1.1: this needs mobile-app
inventory plus an MMP handoff for `app`, and firmographic / IP-to-company
matching for `abm`. Both are roadmap items, not toggles.

The narrower question worth deciding now is whether `?goal=app` should keep
working as a deep link at all. It currently seeds the wizard with an objective
the platform cannot fulfil, which is a smaller version of the same problem.

### 4.3 Is the DMA crosswalk worth extending?

38 of ~210 is enough for the top markets and nothing else. Extending it
means another `metro_area` report pull over a longer window, or a name-
matching pass over the 172 unmatched entries. Worth deciding before the
geo picker is built, because it changes whether DMA is a first-class
targeting mode or a footnote.

---

## 5. Design decisions worth keeping

Short list of things in the prototype that look like styling but are load-
bearing. Each one exists because the alternative was actively misleading.

- **A dash is never a zero.** A week with spend and no measurement is
  excluded from the lead count, not counted as zero; a campaign with no
  cost per lead sorts to the bottom of that column in both directions.
- **Rate charts get a padded baseline**, labelled `not zero`. Cost per lead
  between $83 and $149 inside a 0–150 axis is a flat line.
- **Share means share of the total**, not share of the largest row, and the
  percentage is printed beside the bar.
- **Only the leader is coloured when the leader is genuinely ahead.** Three
  attention scores within five points all get the same weight; greying two
  of them implies a pass/fail the numbers do not support.
- **Every recommendation names what gates it.** Grow's five signals sit
  under one banner: the wallet covers seven days, and every recommendation
  increases spend.
- **Empty states are per-screen and honest.** The account switch reaches
  every page — Tools keeps its cards because setup *is* the job for a new
  account, and its corner states swap rather than claiming 577 attributed
  calls on an account that has never run.
- **The forecast carries its spread.** $12,000 is "113 to 167 leads, and
  the number above is the midpoint, not a floor", because this account's
  campaigns land between $71.72 and $106.25.

---

## 6. Accessibility baseline

The prototype passes, and the build should hold the line:

- WCAG AA contrast on all twelve routes (logotypes exempt under 1.4.3).
- No horizontal overflow at 390 / 768 / 1024 / 1440.
- Every `th` scoped; every decorative SVG hidden; no unlabelled control.
- The ⌘K dialog sets `inert` on the rest of the page — `aria-modal` alone
  does not stop Tab escaping, and it did before this was added.
- The 168-cell daypart grid takes one tab stop with arrow-key movement.
- The hour-of-day bar chart has a visually-hidden table behind it, because
  a `title` attribute is not reachable by a screen reader.

One token fix the prototype carries that the product should take: shipped
`--att-ink3` (`#6B7383`) scores 4.43:1 on tint and 4.45:1 on the page,
both under AA, and it carries every caption and axis label. Under the new
palette the equivalent is `#596A86`, which clears at 4.52 on the worst ground
in the set.

Two more the audit turned up and the prototype fixes:

- The campaign builder's pending step numbers were `--att-ink3` on the step
  chip: 4.43:1 for 11px text.
- Nothing rendered below 11px any more. The 9px and 10px chart axis labels
  were both illegible and under the floor; they are 11px now.

---

## 7. Where the source lives

`docs/dsp/prototype/` — the built file, the five parts it is assembled from,
and a README with the rebuild command and the invariants the code holds itself
to. It is committed because a prototype that exists only as a published URL
cannot be edited by the next person, and this one carries a fortnight of
decisions in its comments.
