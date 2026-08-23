# Attentive Buying — new design bundle: QA and integration check

**Date:** 2026-08-23
**Bundle:** `design_handoff_attentive_buying` (Shared_link_1.zip / Shared_link_2.zip — byte-identical)
**Landed at:** `docs/design/attentive-buying/`
**Checked against:** `pgam-dsp-dashboard@6d7f9f5`, `core/pubmatic_activate.py`, audit Addendum C
**Status:** design QA complete. **No product code changed** — the implementation
target is `pgam-dsp-dashboard`, not this repo.

---

## 0. Summary

The new bundle **supersedes** the five separate `.dc.html` screens from PR #98
with one prototype carrying all eleven states, plus a 44KB written spec
(`README.md`) that did not exist before. Design tokens are unchanged between
the two — this is an expansion, not a re-tokenisation.

Three things to know:

1. **The prototype is sound.** All eleven states render, every specified
   interaction works, zero console errors, and every one of the "rule behind
   the rules" defect classes it claims to have fixed is genuinely fixed
   (§2, §3).
2. **The one real defect is accessibility**, and it contradicts the spec's own
   claim. 41 of 386 interactive controls cannot be reached by keyboard (§4).
   Following the handoff's own instruction to use native elements in
   production fixes all 41 — but the prototype must not be used as the a11y
   reference.
3. **Nothing syncs to Magnite or PubMatic, and the design is correct to assume
   that.** The curation module in `pgam-dsp-dashboard` contains **zero
   `fetch()` calls** — no writes and no reads. Every `createDeal` on both
   adapters delegates to the manual operator queue. The design's asynchronous
   `requested → building → ready` lifecycle is not a UX affectation; it is an
   accurate description of the only path that exists (§5).

The implementation delta is larger than "restyle" (§6), and one item is a
genuine conflict that needs a product decision rather than a code change (§7).

---

## 1. What was checked, and how

The prototype boots off three CDN scripts (React 18.3.1, ReactDOM 18.3.1,
Babel standalone 7.29.0) and Google Fonts. `unpkg.com` is blocked by this
environment's egress policy, so the three files were fetched from
`registry.npmjs.org` and served through Playwright request interception. **All
three SRI hashes in `support.js` verified against the npm-sourced bytes**, so
what was tested is what the prototype ships:

```
react.production.min.js      sha384-DGyLxAyjq0f9SPpVevD6IgztCFlnMF6oW/XQGmfe+IsZ8TqEiDrcHkMLKI6fiB/Z  ✓
react-dom.production.min.js  sha384-gTGxhz21lVGYNMcdJOyq01Edg0jhn/c22nsx0kyqP0TxaV5WVdsSH1fSDUf5YJj1  ✓
babel.min.js                 sha384-m08KidiNqLdpJqLq95G/LEi8Qvjl/xUYll3QILypMoQ65QorJ9Lvtp2RXYGBFj1y  ✓
```

Google Fonts (Inter 400/500/600, JetBrains Mono 400/500) were localised so
typography renders as specified rather than falling back to a system face.
Chromium 1194, 1500×1000, headless.

---

## 2. The eleven states

All eleven render and behave as specified. **Zero console errors or warnings
across the entire sweep.**

| # | State | Verified |
|---|---|---|
| 0 | Home | "Needs you" (2), "In flight" (3), rail, 36-minute footnote |
| 1 | New deal | 4 cards + 3 disclosure rows, forecast rail, ETA copy |
| 1b | Requested → Building | 3-step timeline, real ETA, no fake spinner |
| 1c | Ready | Deal ID hero, notification receipt, deviation disclosure |
| 2 | Deals | KPI strip, saved views, filters, table, bulk tray |
| 3 | Package detail | All five tabs present and switching |
| 4 | Marketplace | 6 categories, spotlight, sort, compare |
| 5 | Reporting | 5 KPIs, both charts, 5 breakdown dimensions |
| 6 | Fulfilment queue | Staff banner, 4 KPIs, checklist, deliver gate |
| 7 | Methodology | Hero, four inputs, "what attention is not", coverage |
| 8 | Empty & failure states | All six reference states |

Interactions confirmed working: attention-tier switching re-drives all four
forecast values **and renders the delta under each** (`from 4.1M`,
`from 890K`, `from $24.60`, `from 78`); the supply-path disclosure reveals
Advanced; the device-ID checkbox raises the blocker, changes the review count
and relabels Create to "Resolve the blocker to create", and unchecking clears
it; Copy → "Copied"; marketplace sort reorders; ⌘K reaches every screen
including staff and reference.

**Marketplace package counts total exactly 37 as specified** — PGAM Attention
7, Sports 5, Audience 10, Online video 5, Display 4, Seasonal 6.

---

## 3. The defect classes — all genuinely fixed

The spec says two review defects were of the class "the product states
something its own data contradicts", and that both were fixed. Both are
fixed, and the wider rule holds:

**Availability is keyed to package identity, not array position.** Re-sorting
from default to Price reorders all seven visible cards; every availability
read travels with its package. Seven shared packages, **zero drift**:

```
default order                     sort = Price
Live Sports Attention  34% left   High Completion Video       88% open
First-in-Pod CTV       41% left   Attention Efficient CTV     91% open
High Attention CTV     78% open   High Attention CTV          78% open   ← unchanged
...                               Live Sports Attention       34% left   ← unchanged
```

**The format chip is derived from the active category**, so it cannot sit
over contradicting cards:

| Category | Chip |
|---|---|
| PGAM Attention | CTV and online video |
| Sports | CTV and online video |
| Audience | All formats |
| Online video | Online video |
| Display | Display |
| Seasonal | CTV |

**The advertiser rollup states its partiality and the number is true.** The
note reads "4 of 6 advertisers with delivery in range" and the table renders
**exactly 4** advertiser rows above the totals row.

**`—` vs `n/a` vs `0` are honoured.** In that same table Harbour Point Resorts
— an imported deal — renders attention `n/a` and pacing `—`, not `0`. On Home,
the building deal's attention renders `—`.

**Constraint 8 (no cost/fee/margin) holds.** A scan across Home, New deal,
Deals, Marketplace and Reporting found one match for `supply cost`, and it is
the constraint being stated rather than violated: *"Figures are your price
throughout — nothing about supply cost appears."* The staff queue is clean too.

**Constraint 1 (no SSP names in Automatic mode) holds.** No occurrence of
Magnite, PubMatic, SpringServe, ClearLine or Rubicon on any buyer screen in
default mode, other than the mandated `Magnite forecast` provenance badge that
constraint 2 requires.

**Constraint 7 (amber reserved for attention) holds.** Nine amber-token
elements on the builder; all nine are attention.

---

## 4. The one real finding: keyboard reachability

The spec states: *"Every interactive element has a role, a tab stop and
keyboard activation."* Measured across all nine reachable screens, that is not
true of the prototype.

**41 of 386 interactive controls (10.6%) cannot be reached by keyboard** —
they have `cursor:pointer` and a click handler but no `tabindex`, no native
focusable tag, and no focusable ancestor:

| Screen | Interactive | Unreachable | Examples |
|---|---|---|---|
| Home | 50 | 1 | `Reply` |
| New deal | 32 | 6 | `Start from a template`, format segments |
| Deals | 92 | 4 | `Export`, `+ Save this view`, two `▾` |
| Marketplace | 52 | 4 | `See the placement list`, format chip |
| Reporting | 19 | 6 | `Export`, `Build wrap deck`, `Schedule weekly` |
| Detail | 26 | 7 | `Duplicate`, `Pause`, `Edit targeting`, `Apply`, `Dismiss` |
| Queue (staff) | 100 | 4 | `Mine only`, `Ask the buyer`, `Hand off` |
| Methodology | 10 | 1 | `Download the one-pager` |
| States ref | 17 | 8 | `New deal`, `Edit the request`, `Rebuild through PGAM` |
| **Total** | **386** | **41** | |

Also: the Deals library renders **7 elements with `role="checkbox"` and
`aria-checked` unset on every one**. The spec explicitly requires
`role="checkbox"` + `aria-checked`. A screen reader announces these as
checkboxes in an indeterminate state regardless of what the user selected.

What *does* work: the focus ring is correct (`2px solid rgb(11, 60, 255)`,
matching `#0B3CFF`), `role="tablist"`/`role="tab"` is present on the detail tab
bar, and the row checkbox correctly `stopPropagation`s so selecting a row does
not navigate into the deal.

**This does not block the build.** The handoff already says to use native
`<button>`, `<input>` and `<table>` in production instead of the prototype's
`div` + `role` scaffolding — doing that resolves all 41 for free. The action
item is narrower: **do not treat the prototype as the accessibility reference,
and do not carry its scaffolding across.** Given the stated VPAT expectation,
the detail screen's header actions (`Duplicate`, `Pause`, `Edit targeting`) and
the queue's `Ask the buyer` / `Hand off` are the ones that would be noticed
first — they are primary operator paths.

---

## 5. Does it sync to Magnite and PubMatic?

**No — and the design is right not to promise that it does.**

The curation module in `pgam-dsp-dashboard` (`src/lib/curation/`, 3,680 lines)
contains **no `fetch()` call and no POST/PUT/PATCH anywhere**. It is a pure
decision layer plus fixtures. Both SSP adapters declare their limits as data
and hand every write to the operator queue:

| Capability | Magnite | PubMatic |
|---|---|---|
| `supportsProgrammaticCreate` | **false** | **false** |
| `supportsForecast` | **false** | **false** |
| `supportsInventoryDiscovery` | false | false |
| `supportsDealDiscovery` | **false** (unverified) | **true** (in production since 2026-05) |
| `supportsAudienceSync` | false | false |
| `defaultFulfilmentMode` | `manual` | `manual` |

`MagniteAdapter.createDeal()` and `PubMaticAdapter.createDeal()` both return
`this.manual.createDeal(...)`. There is no second code path.

**Why each is blocked, from the adapters and Addendum C:**

- **Magnite.** `DealIdList` writes are rejected account-wide on account 2724 —
  every variant tried returns *"Inventory groups Deal ID lists cannot be
  modified but can be deleted"*. The ClearLine forecast POST returns 422
  "Access token is missing" for this seat. Reads run against
  `performance-analytics-reporting-service.magnite.com` with a **scraped UI
  session token passed as a URL query parameter**, not the documented DV+ REST
  API (three Key/Secret pairs all 401'd; AM escalation open since April). The
  per-deal read is *unverified*, not unavailable — but probing it risks a 422
  on the metric set the live P&L depends on, per Magnite's 2024-11-03 field
  compatibility rules.
- **PubMatic.** Read works and is the one genuinely present capability:
  `reportingSearch` on buyer 69397 has fed the P&L since 2026-05. But it runs
  on a `pubtoken` lifted from the Activate SPA's sessionStorage. The OAuth
  client in this repo (`core/pubmatic_activate.py`) targets Activate org 17496
  and exposes **only reads** — `list_advertisers`, `list_campaigns`,
  `list_deals`, `get_advertiser_fees`, `get_organization`. There is no POST
  wrapper. Whether 69397 and 17496 are one commercial relationship or two is
  still open, and it decides which seat a write would target.

**On both providers the curation-seat read runs on a scraped session token.**
Addendum B7's conclusion stands: do not build writes on that.

**What this means for the design.** Constraint 3 — "never imply instant
creation" — is not a stylistic hedge, it is the only honest rendering of the
system. Screen 6 (the fulfilment queue) is not a nice-to-have operator tool;
it is the mechanism. The design and the backend agree, and the seam is already
built: when an entitlement lands, the change is
`supportsProgrammaticCreate: false → true` on one adapter, with no UX change.

**The design is also correctly ahead of the data in one place.** Screen 5's
"What we can't show you yet" panel blanks Win rate, Bid requests and Fill rate
with reasons rather than zeros. That is exactly right — those are auction-side
metrics that Magnite's compatibility matrix will not return alongside the
`partner` dimension. Keep that panel; it is not filler.

---

## 6. Implementation delta against `pgam-dsp-dashboard`

The curation platform is **already substantially built** — ~4,900 lines of UI
across `src/components/curation/`, routed at exactly the paths this spec names.
**All 289 curation tests pass** (15 files, `vitest run`).

What the routes already cover: `/curation`, `/curation/new`, `/curation/deals`,
`/curation/deals/[id]`, `/curation/marketplace`, `/curation/reporting`,
`/admin/curation/queue`.

What the new bundle adds on top of what is built:

| Area | Built today | New spec | Delta |
|---|---|---|---|
| Marketplace categories | 4 (attention, sports, audience, seasonal) | 6 (adds Online video, Display) | +2 |
| Marketplace packages | 13 | 37 | +24 |
| Marketplace spotlight | absent | required on PGAM Attention | new |
| "Recommended for {advertiser}" strip | absent | 3 cards, each with a stated reason | new |
| Sort (Attention / Price / Scale) | absent | working, reorders grid | new |
| Compare (up to 3) + modal | absent | incl. "Selection basis" row | new |
| Reporting dimensions | 4 (deal, market, publisher, device) | 5 (adds **advertiser**) | +1 |
| Partial-coverage note on rollup | absent | "4 of 6 advertisers…" | new |
| Deals row menu (`⋯`) | absent | 5 items, incl. Copy Deal ID disabled with reason | new |
| Attention methodology screen | **conflicting page exists** | Screen 7 | see §7 |

Everything else in the spec has a counterpart already in the codebase —
the builder blocker, `PGAM Supply` labelling, the Imported badge, the
Jacksonville deviation, pod position on the attention tab, and the
"can't show you yet" reporting panel are all present.

Two things the audit asserted that the codebase confirms, for the record: the
RBAC layer and the saved-views component do exist and should be extended
rather than replaced.

---

## 7. One conflict that needs a product decision, not a code change

**The attention methodology has two incompatible definitions.**

- **The new spec (Screen 7)** defines attention as **four** weighted inputs:
  Screen share 35%, Dwell 30%, Audibility 20%, Pod position 15%. Its copy is
  explicit that it must not be paraphrased, and it is the credibility document
  for the entire product thesis.
- **The codebase** already ships `/methodology` backed by
  `src/lib/attention-weights.ts` with **16 signals** (sustained attention,
  completion rate, attention velocity, engagement rate, …), plus
  `src/lib/benchmarks.ts`.

These are not two renderings of one model; they are two models. Publishing the
four-input page while the sixteen-signal page stays live would put two
different answers to "how do you measure attention?" in front of the same
buyer — the precise failure the spec's tenth rule exists to prevent.

The handoff prompt says: *"If a constraint above appears to conflict with
existing code, stop and raise it rather than picking one."* Raising it.

Three ways out, in the order I would consider them:

1. **Four is the buyer-facing summary of the sixteen.** If the 16 signals roll
   up into those 4 families, the new page is a correct simplification and
   `/curation/attention/methodology` becomes the buyer view over the same
   engine. Cheapest, and most likely — but it needs someone who knows
   `attention-weights.ts` to confirm the rollup is real rather than
   convenient.
2. **The four-input model supersedes the sixteen.** Then the existing
   `/methodology` page and its consumers need retiring, which reaches beyond
   curation into `attention-hero.tsx`, `campaigns/attention-tab.tsx` and the
   reports methodology component.
3. **They are genuinely separate products** (CTV curation attention vs the
   attention engine's scoring). Then both stay, and each page must say which
   it is — because today neither does.

Until this is settled, Screen 7 should not ship. Every other screen can.

---

## 8. Recommended build order

The handoff's own sequence is sound and matches where the codebase already is.
Adjusted for what exists:

1. **Marketplace** — the largest delta and entirely additive: +2 categories,
   +24 packages, spotlight, recommendations, sort, compare. Nothing here
   conflicts with built code.
2. **Deals row menu + bulk tray gaps** — small, self-contained, and the
   "Copy Deal ID disabled: not issued yet" state is a correctness item, not a
   nicety.
3. **Reporting: advertiser dimension + partial-coverage note** — small, and
   the note is the defect-class fix.
4. **Accessibility pass on the existing screens** — native elements, and the
   `aria-checked` gap. Do this before the VPAT is asked for, not after.
5. **Screen 7** — blocked on §7.

Stages 1–3 are independently demoable, and none of them touch the provider
layer, so none of them can regress the Magnite or PubMatic paths.

---

## Appendix — reproducing this QA

```bash
# 1. Serve the bundle
cd docs/design/attentive-buying && npx http-server -p 8811 -s --cors

# 2. Fetch the CDN deps from npm (unpkg is egress-blocked here) and verify SRI
npm pack react@18.3.1 react-dom@18.3.1 @babel/standalone@7.29.0
openssl dgst -sha384 -binary <file> | openssl base64 -A   # compare to support.js

# 3. Drive it with Playwright, intercepting unpkg.com + fonts.googleapis.com
```

Curation test suite in the implementation repo:

```bash
cd pgam-dsp-dashboard && npm ci
npx vitest run tests/lib/curation tests/components/curation-*.test.tsx
# → 15 files, 289 tests, all passing
```
