# Attune self-serve prototype — engineering handoff

**Prototype:** https://claude.ai/code/artifact/3f4eded5-4205-4927-b47e-6133a6b06d00
**Companion:** `docs/dsp/ss-dashboard-ab-comparison.md` (the A/B review the design came out of)
**Written:** 2026-08-26. Verified against `pgam-dsp-dashboard@c155b18`-era `main`.

Twelve screens, five campaign detail pages, a five-step builder. This
document is the part that does not survive in a picture: which constraints
in the prototype are real, which figures are modelled, and what has to be
decided before anyone starts building.

Read it as three lists — **verified**, **modelled**, **open**.

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

They are not stubs. `recommendedAudienceForGoal()` has cases for both, the
`VALID` list in the `?goal=` seed effect accepts both, and the launch route
maps both to an objective. `?goal=app` reaches the wizard today.

> **So:** "coming soon" is the wrong framing. Adding the two cards is a
> two-line change; the plumbing is already there. The prototype labels them
> *not in the picker today* rather than *on request*.

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
| Six reporting dimensions (`RUN`) | Results → Where it ran | modelled volumes, real shape |
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

## 3. Open decisions

### 3.1 Channel marks — needs a decision, not an implementation

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

### 3.2 Should the two missing objectives ship?

See §1.1 — the work is already done. Someone needs to say whether `app`
and `abm` belong in a self-serve grid aimed at local advertisers, or stay
deep-link-only for managed accounts.

### 3.3 Is the DMA crosswalk worth extending?

38 of ~210 is enough for the top markets and nothing else. Extending it
means another `metro_area` report pull over a longer window, or a name-
matching pass over the 172 unmatched entries. Worth deciding before the
geo picker is built, because it changes whether DMA is a first-class
targeting mode or a footnote.

---

## 4. Design decisions worth keeping

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

## 5. Accessibility baseline

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
both under AA, and it carries every caption and axis label. `#676F7F`
clears it at 4.69 / 4.72 / 5.05.
