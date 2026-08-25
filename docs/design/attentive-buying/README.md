# Handoff: PGAM Attentive Buying

## Overview

**Attentive Buying** is a self-service platform where agency media buyers create curated programmatic PMP deals (Deal IDs) which they then activate in their own DSP. The differentiator is **attention**: deal inventory is selected by measured attention scores on CTV placements, and attention is reported back as a post-flight outcome.

Users are agency traders, media planners, programmatic directors and account teams — **not** ad-tech engineers. The product's job is to let a buyer describe what they want to buy and receive a Deal ID, without ever learning what an inventory group is.

Eleven states are designed:

| # | Screen | Route |
|---|---|---|
| 0 | Home | `/curation` |
| 1 | New deal (builder) | `/curation/new` |
| 1b | Requested → Building | `/curation/new` result state |
| 1c | Ready — Deal ID delivered | `/curation/deals/{id}?state=ready` |
| 2 | Deals (library) | `/curation/deals` |
| 3 | Package detail, 5 tabs | `/curation/deals/{id}` |
| 4 | Marketplace | `/curation/marketplace` |
| 5 | Reporting | `/curation/reporting` |
| 6 | Fulfilment queue (PGAM staff) | `/admin/curation/queue` |
| 7 | Attention methodology | `/curation/attention/methodology` |
| 8 | Empty & failure states (reference set) | n/a — wire each into context |

Plus a global **⌘K command palette**.

## About the design files

The files in this bundle are **design references created in HTML** — prototypes showing intended look and behaviour, not production code to copy. `Attentive Buying.dc.html` is a single-file prototype on a lightweight template runtime (`support.js`); its structure is not a component architecture to mirror.

**Recreate these designs in the target codebase's own environment**, using its established patterns, component library, routing and data layer. Do not ship the HTML. Every hex value, font size, weight, letter-spacing, radius and shadow below is intentional — lift the values, not the markup.

The internal audit indicates a React/TypeScript app with an existing RBAC layer (`src/lib/rbac.ts`), a saved-views component (`report_saved_views`), an agent chat component (`agent-chat.tsx`), and an `/inventory/packs` screen the Marketplace should extend. Prefer extending those. If the codebase has diverged from the audit, the codebase wins.

**Fidelity: high.** Final colours, typography, spacing and interaction behaviour.

---

## Design system reconciliation

This project has two systems and they are not in conflict once scoped:

- **Product UI (this spec): light, Inter-based.** An analytical enterprise tool on white and `#F8FAFC` surfaces. Dense tables, tabular figures, hairline borders, 16px card radii. Primary blue `#0B3CFF`.
- **PGAM Media brand system: dark, Space Grotesk / Hanken Grotesk.** Near-black navy, glass chrome, glow accents, brand blue `#1E90FF`. This governs the marketing site and brand surfaces.

**Shared:** the PGAM mark (`assets/pgam-mark.png`), JetBrains Mono for IDs and codes, a blue primary, and the same restraint about naming SSPs.

**Why the product diverges, deliberately:**

1. **Contrast.** `#1E90FF` on white fails WCAG AA for text and small UI. `#0B3CFF` passes, and reads as the same brand blue at UI scale.
2. **Density.** This screen holds nine-column tables of numbers. Inter with `tabular-nums` was chosen for figure alignment and legibility at 13px; Space Grotesk's geometric forms and tight tracking are display faces.
3. **Duration of gaze.** A trader has this open all day. The dark, glowing treatment that makes a marketing hero feel premium is fatiguing as a workspace.

**Implementation rule:** don't mix them. The product uses the tokens in this document. Brand moments inside the product (the top bar, the mark, the wordmark lockup) follow the brand system. If a future decision moves the product to dark, it is a re-tokenisation of this same structure — the layouts, hierarchy and behaviour below are unaffected.

---

## Design tokens

### Typography

Inter throughout. **JetBrains Mono for IDs, seat codes and hashes only — never for metrics.**

Weights: **400, 500, 600 only. Never 700/bold.** Hierarchy from size, colour and spacing; weight is the last lever.

| Role | Size | Weight | Letter-spacing | Notes |
|---|---|---|---|---|
| Page title | 32px | 600 | −0.021em | `line-height:1.1` |
| Deal ID (hero) | 32px | 500 | −0.02em | JetBrains Mono, `line-height:1` |
| Attention hero value | 44px | 600 | −0.028em | Attention tab, methodology, spotlight |
| Section heading | 15–20px | 600 | −0.006 to −0.015em | |
| KPI value | 28px | 600 | −0.026em | `tabular-nums` |
| Micro-label | 11px | 500 | 0.055em | uppercase |
| Body | 14px | 400 | — | |
| Table data | 13px | 400 | — | 500 on emphasised cells |
| Caption / helper | 12px | 400 | — | `line-height:1.45–1.55` |
| Mono inline | 12px | 400/500 | — | JetBrains Mono |

**Every numeric column and KPI carries `font-variant-numeric: tabular-nums`.** Never the mono face for numbers. Use U+2212 (−) for negative deltas, not a hyphen.

### Colour

**Neutrals**
```
#F6F7FA  page background        #E3E7EF  card / field border (1px)
#FFFFFF  card surface           #D8DEE9  dashed "add" border
#FCFDFE  table + card header    #C7CCD6  chevron, plan line, dashed ring
#F8FAFC  inset / disabled       #9098A6  muted / placeholder
#F4F6F9  neutral chip           #6B7383  secondary text
#EEF0F5  hairline divider       #545C6B  body text
#F1F3F7  chart gridline         #0C1017  headline ink
                                #0F1521  top bar, trays, toasts, staff banner
```

**Brand blue** — actions, selection, links, in-progress
```
#0B3CFF primary   #0A34DB hover   #2E6BFF nav underline / tray hover
#0925A8 blue text  #46A8FF gradient tail   #6EA8FF icon on dark
#D5DEFC border    #E8EEFF tint    #F4F7FF lightest tint   #F1F5FF gradient stop
#5B6B9E caption on tint    #3A4A7A body on tint
```

**Amber — RESERVED EXCLUSIVELY FOR ATTENTION.** Never CTAs, never states, never decoration.
```
#E67A1F accent (dot, bar, rail)   #B45309 label / icon stroke
#803F15 value / heading           #8A4513 body    #7C4A1D body on tint
#FBE29C border   #FDEFCA selected fill   #FFF9EC chip   #FFFCF5 card surface
#FDF3D8 lightest band
```

**Warning / advisory** — always with ▲ and advisory copy
```
#FBDCC0 border   #FFF3E8 fill   #FFF9F4 card   #8A4513 text   #B94018 emphasis
```

**Blocker** — destructive, visually distinct from advisories
```
#FADADA border   #FEF2F2 fill   #7F1D1D text   #DC2626 count emphasis
```

**Success / live**
```
#BFE8CF border   #E7F7EE fill   #F5FCF8 field   #0F7A38 text   #17A34A bar
```

**Imported (third-party deal)**
```
#DDD0FA border   #F4F0FF fill   #5B21B6 text
```

### Spacing, radius, shadow

- Radius: **16px** cards · 12–14px inset panels, trays, modals · 10px meta strips, menus · 8px buttons/fields · 6px chips · 5px badges · 4px checkbox · 99px pills
- Borders 1px, `#E3E7EF` default
- Card shadow `0 1px 2px rgba(16,24,40,.03), 0 1px 3px rgba(16,24,40,.04)`; hover lift `0 6px 18px rgba(16,24,40,.07)` + `border-color:#D5DEFC`
- Primary button `0 1px 2px rgba(11,60,255,.28)` · modal `0 24px 60px rgba(12,16,23,.22–.28)` · drawer `-8px 0 32px rgba(12,16,23,.14)` · tray/toast `0 16px 40px rgba(12,16,23,.3)` · row menu `0 12px 32px rgba(12,16,23,.16)`
- Scrim `rgba(12,16,23,.34)`; palette/compare scrim `rgba(12,16,23,.42)`
- Page padding **28px**. Card padding 18–26px. Table cells `13px 18px`, headers `10px 18px`.
- Control heights: 40px primary CTA · 36px field / secondary · 34px toolbar · 32px filter chip · 30px small chip · 28px tab chip · 26px row action
- Layout: `minmax(0,1fr) 380px`, `gap:20px`, rail `position:sticky; top:12px`. Admin queue `minmax(0,1fr) 420px`.
- **Root and every screen wrapper carry `min-width:1360px` and `width:100%`.** Desktop enterprise tool: it scrolls, it doesn't reflow. (Screen wrappers must not rely on inheriting this — set it on each.)

### Motion

```css
transition: background-color .16s cubic-bezier(.16,1,.3,1),
            border-color .16s cubic-bezier(.16,1,.3,1),
            color .16s cubic-bezier(.16,1,.3,1),
            box-shadow .18s cubic-bezier(.16,1,.3,1),
            opacity .16s ease;

@keyframes ab-pulse { 0%,100% { opacity:1 } 50% { opacity:.35 } }  /* 1.8s — building indicators only */
@keyframes ab-in    { from { opacity:0; transform:translateY(6px) } to { opacity:1; transform:none } }
```
Entrances: screen `.35s` · drawer `.28s` · modal `.24s` · tray `.22s` · disclosure `.22s` · palette `.18s` · row menu `.16s`.

---

## Hard product constraints

Not stylistic preferences. Violating any breaks the product.

1. **No SSP names in default "Automatic" supply mode.** Say **"PGAM Supply"**. Magnite, PubMatic, SpringServe, ClearLine may be named only in opt-in **Advanced** mode. Existing policy.
2. **Every forecast number carries its source.** `Magnite forecast` (supply-platform avails) · `PGAM historical` (measured, comparable deals, 90-day window) · `PGAM estimate` (modelled, not a quote) · `measured` (attention, from delivered impressions). **One panel mixes sources, so the affordance is per-field** — a badge beside each value with a `title` explaining it. Never a panel-level label.
3. **Never imply instant creation.** Creation is asynchronous in v1: a human fulfils it in minutes to hours. `requested → building → ready`, with an ETA from a rolling median and a push notification. No spinner pretending to do the work.
4. **Warn at build time, never at submit time.** Unsupported options are **disabled in place with a reason**. No submit-time error dialogs.
5. **Blockers are visually distinct from advisories.** Blockers: red palette, ✕, disable the CTA, which restates why. Advisories: orange palette, ▲, never block. Ranked by severity, blockers first.
6. **The Deal ID is the payload.** 32px mono, copyable, per-DSP activation instructions beside it.
7. **Attention is structural, not a badge.** (a) a selectable inventory source in the builder, (b) a score pre- and post-flight on every deal, (c) a low-attention suppression toggle with an adjustable floor.
8. **No slot anywhere in the buyer UI where supply cost, SSP fees or PGAM margin could appear.** Agencies see only their own price. Never a "cost" or "net" column, not even on staff screens reachable from this app. Label money "your price" where ambiguity is possible.
9. **The builder is one screen with progressive disclosure — not a wizard.** A trader fills ~6 fields and hits Create; a less technical user opens a guided drawer per section. **Same underlying object.**

### The rule behind the rules

**The product never states something its own data contradicts.** Two review defects were exactly this: a hard-coded "CTV" filter chip sitting above display packages, and an advertiser rollup labelled "6 advertisers" over 4 rows. Both are now derived from state. Applied consistently:

- Unknown renders `—` in `#9098A6`/400. Never `0`, never blank.
- `—` = not applicable yet · `n/a` = unmeasurable (imported deals) · `0` = a measured zero. Never substitute.
- Partial coverage is stated: "4 of 6 advertisers with delivery in range", "5 of 32 deals · sorted by spend".
- Filter and category labels are derived, never literals.
- **Per-item values are keyed to item identity, not array position.** A package whose availability changes because the user re-sorted is a lie about live inventory.

---

## Screens

### 0 · Home — `/curation`

The returning trader's landing: what needs me, what's in flight. Header greeting + Browse marketplace / New deal.

**Left column:**
1. **"Needs you"** (2 items). *Deal ID ready* card — border `#D5DEFC` rather than default grey, because it's the one thing that matters; 34px `#E8EEFF` icon tile, title + "Deal ID ready" badge, "Built in 31 minutes · ready 12:48 ET · not yet activated in your seat", the Deal ID in 15px mono right-aligned. Clicking opens the **Ready** state. Second card: *a question on the build* — "Your $9 floor won't clear the attention pool in these markets. We've paused the build rather than guess — reply and we'll finish it." This card exists to prove the async promise is honest.
2. **"In flight"** — rows `minmax(0,1.9fr) 1fr 1.5fr 90px 24px`: name + Attention badge + sub-line, status chip, plain-language note ("Ready about 14:40 ET", "60% of flight, on pace"), attention score, chevron. Footnote: median build time 36 minutes, marketplace inside 15.

**Right rail:** *This quarter* (Delivered · Live deals · Avg attention, that last row separated by an `#FDEFCA` divider and set in amber) and *Pick up where you left off* (three drafts + dashed "Start from a template").

Home was deliberately simplified: no marketplace teaser grid, no separate build-speed card. Discovery lives in Marketplace; the median is a footnote.

---

### 1 · New deal — `/curation/new`

Four visible cards plus three collapsed rows. Header: "Six fields is enough. We build it and send you the Deal ID." Actions: "Start from a template", "Describe it instead" (blue-tinted, opens the describe drawer).

**Card 1 — "The buy"**: 3-col Advertiser / Brand / Deal name, then a row of `minmax(0,1fr) minmax(0,1.15fr)` holding **Format** (4-segment control) and **Markets** (chips + dashed "+ Market or DMA" + the disabled-with-reason note: a neutral `ZIP-level` pill and "Unavailable on this supply path — DMA is the finest granularity supported"). Both tracks must be flexible; a fixed first track collapses the second.

Field pattern: label 13px/500 `#545C6B` above a 36px bordered 8px-radius field, 13px value, `▾` for selects, ellipsis on overflow.

**Card 2 — "Attention · how it picks inventory"** — the only amber card in the builder (`#FFFCF5`/`#FBE29C`). Attention icon + amber micro-label + "Guide me →". A 3-segment control on white with `#FBE29C` border, active `#FDEFCA`/`#803F15`:

| Segment | Note | Avail | Households | CPM | Score |
|---|---|---|---|---|---|
| Prioritise · top quartile | Builds from placements measuring 75+ across your markets — 1,284 placements, refreshed nightly on a 30-day window. | 4.1M | 890K | $24.60 | 78 |
| Monitor only | Buys your full targeting and reports attention as an outcome. Nothing filtered. | 11.4M | 2.1M | $19.80 | 66 |
| Suppress low | Buys broadly but suppresses placements under 40. A floor, not a filter. | 8.7M | 1.6M | $21.40 | 71 |

Switching a tier re-drives all four rail values **and shows the delta beneath each** — "from 11.4M", "from $19.80", "+12 from 66". This is the core interaction; the trade must be visible, not remembered.

**Card 3 — "Flight, budget and delivery"**: 4-col Start / End / Budget / **Floor CPM** (the focused field: `1px solid #0B3CFF` + `0 0 0 3px rgba(11,60,255,.10)`, `$` prefix). Below, a 200px two-segment bar (`#17A34A` 64% + `#FBE29C` 16%) with "**Clears comfortably.** $24 reaches an estimated 64% of the attention pool; $21 is the lowest we'd recommend." Then a hairline and DSP / Seat.

**Card 4 — progressive disclosure.** Three rows; collapsed shows title, current value inline, an optional flag badge, `▾`/`▴`.

| Row | Summary | Contents |
|---|---|---|
| Audience | "Home improvement intent" · flag "1 note" | Contextual/attention audiences work today; demographic and in-market can't sync. **Advisory:** homeowner segments unavailable, home-improvement contextual substituted, household-level needs a data partner. |
| Supply path | "Automatic · PGAM Supply" | Automatic vs Advanced. Contains the **device-ID checkbox**. |
| Lists and exclusions | "Ridgeline global blocks" | List inheritance; changes apply to this deal unless saved to the advertiser. |

Disclosure chips: selected `#E8EEFF`/`#D5DEFC`/`#0925A8`; unselected `#F8FAFC` with a **dashed** `#D8DEE9` border.

**The device-ID blocker.** "Require device ID on every bid request / Enables household match-back. Most CTV apps don't pass one." Checking it raises:

> ✕ **Blocker — device ID requirement drops 91% of your CTV pool.** 1,284 attention-curated placements fall to 112, below the scale your budget needs.

and disables Create, whose label becomes "Resolve the blocker to create".

**Right rail:** *Forecast* (four rows, each with its own source badge and `title`; attention in amber with `/ 100`; footer links to the methodology page) · *Before you send it* (count "1 blocker · 1 to review" in `#DC2626` when blocked else "1 to review" in `#B94018`; blocker panel first, advisory second, then a green ✓ line) · *Create* (40px CTA + "A person builds this deal. Deals like yours are ready in **about 40 minutes** — median over the last 30 days. We'll notify you." + Save as draft).

**Guided drawers** — 420px right drawer, three of them (geo, attention, describe). Header "GUIDED" + question; body of question cards each with question, explanatory answer, choice chips; sticky footer "Apply to form" / "Cancel"; closing note: "Answers write straight back into the form — same fields, same deal. Nothing here is a separate flow." Full copy in the prototype's `DRAWERS`.

---

### 1b · Requested → Building

Centred, `max-width:720px`. "REQUESTED" micro-label, deal name, meta, and a pulsing "Building" chip.

Three-step timeline (`24px minmax(0,1fr)`): *Request received* (filled blue ✓, "Today, 14:02 ET · targeting, floor and seat locked in") · *Being built by our curation team* (blue ring with pulsing dot; "Estimated ready **14:40 ET (~38 min)** — median for CTV deals of this shape over the last 30 days"; 6px progress bar at 14%) · *Deal ID ready to activate* (dashed ring, muted; names the email, browser push and Slack channel). Rails: `#D5DEFC` after complete, `#EEF0F5` after current.

Reassurance panel: "Nothing to wait for — you can close this tab. If anything needs a decision, we come back to you before building rather than guessing."

The ETA and progress must come from real fulfilment telemetry (rolling median by format and deal shape), never a fake timer.

---

### 1c · Ready — Deal ID delivered

Centred, `max-width:760px`.

- **Notification receipt** strip, dark: "✓ Notification sent 12:48 ET · email · browser push · #programmatic". Proves 1b's promise was kept.
- "READY TO ACTIVATE", deal name, meta, and a "Built in 31 min" success chip.
- **Deal ID hero**: `linear-gradient(180deg,#F7F9FF,#F1F5FF)`, `#D5DEFC` border, 2px top accent `linear-gradient(90deg,#0B3CFF,#46A8FF)`, ID at 32px/500 mono, "Paste this into DV360 — it's the only thing you need from us", and a Copy button that becomes "Copied" in the success palette for 2s.
- **4-col meta strip**: Floor CPM · Placements · Seat (mono) · **Pre-flight attention** (amber cell).
- **Timeline, all complete** — including "Built by Marc D. on the curation team, 12:17 – 12:48 ET" and "delivered 12:48 ET · **4 minutes ahead of the estimate you were given**". Naming the human and beating the stated ETA are both deliberate.
- **Deviation disclosure** (`#F4F7FF`/`#D5DEFC`): "One change from what you asked for: **Jacksonville was dropped** — no attention-qualified supply in that market for this flight. Your other 12 markets absorbed the impressions. Nothing else was altered." Every deviation is surfaced here, explicitly and singly.
- Actions: Activation instructions · Email to my trader · See it in Deals.

---

### 2 · Deals — `/curation/deals`

Header + Export / New deal. **KPI strip**: Delivered · Impressions · Avg CPM · **Avg attention 76 / 100** (amber card).

**Saved views** row: chips "All deals 32", "Needs activation 2", "Attention-curated 19", "My live CTV 11", plus a dashed "+ Save this view". Active chip in blue tint. (Reuse the existing `report_saved_views` component.)

**Filters**: search · All advertisers ▾ · Any status ▾ · an always-amber **"Attention-curated only"** toggle (`#FFF9EC` off, `#FDEFCA` on — never blue, it filters on attention) · "Sorted by last activity".

**Table** `22px minmax(0,2.1fr) 1.15fr .85fr .8fr .65fr .85fr .75fr 26px`, `gap:10px`:
- **Select checkbox** (header has select-all with an indeterminate `–` state). Its click must `stopPropagation` so it doesn't open the row.
- Deal (name 500 + badges + 12px sub-line) · Deal ID (12px mono) · Status chip · Format · Floor · Delivered · Attention (`#803F15`) · **row actions `⋯`**.
- Selected rows: `#F4F7FF` + `inset 2px 0 0 #0B3CFF`.

**Row menu** (210px, anchored under the `⋯`): Open deal · Copy Deal ID *(disabled with the reason "Deal ID not issued yet" when there isn't one)* · Duplicate for a new flight · Pause/Resume *(label follows current state)* · Archive (in `#B94018`).

**Bulk tray** — dark, centred, bottom: "*n* deals selected", then Duplicate for a new flight (primary) · Pause · Add to a view · Export · Clear.

Badges: **Attention** (amber) · **Imported** (violet). Status chips: Building (blue tint) · ● Live (success) · Paused / Draft (neutral).

Footnote: "ⓘ **Imported** deals were created outside PGAM — we report on them but didn't build them, so attention isn't measured."

---

### 3 · Package detail — `/curation/deals/{id}`

Breadcrumb → title row (name + "● Live" + "Attention-curated") → meta → five tabs. Active tab 600 `#0C1017` with `inset 0 -2px 0 #0B3CFF`. Header actions: Duplicate · Pause · Edit targeting.

**Tab 1 — Deal ID & activation.** Deal ID card (same gradient hero, "Activated 16 Jun" chip, 3-col DSP / Seat / Floor strip) · **Activation steps** with three DSP chips (The Trade Desk / DV360 / Amazon DSP) swapping the step list — each names the real menu path, sets the supply vendor to **PGAM Supply**, states the fixed rate, and gives an honest time-to-traffic (`DSP_STEPS` in the prototype) · **Delivery so far** (4 KPIs + pacing bar). Rail: **Deal health** (bid requests, win rate, a Tampa advisory, and a "Suggested" block — "Shift 15% of Tampa budget to Orlando — we project **+180K completed views** at the same spend" — with Apply/Dismiss) and **Attention lift** (amber, "+19% vs your run-of-network CTV").

**Tab 2 — Delivery.** Five KPIs (attention amber, "78 forecast"). **Daily spend against plan** — blue bars at `opacity:.88` with a 1px `#C7CCD6` plan line positioned per bar, `title` tooltips. **Per-market table** ending in **cost per completed view**, green when healthy and `#B94018` when not — Tampa's `$0.078` against Orlando's `$0.026` is what the health advisory refers to.

**Tab 3 — Attention.** The product's proof. Amber score card: **81** at 44px, "pre-flight forecast was 78", plus tiles for lift (+19%) and measured impressions (3.31M). **Impressions by attention band** — one 34px stacked bar: 80+ 44% (`#B45309`), 60–79 34% (`#E67A1F`), 40–59 17% (`#FBE29C`), <40 5% (`#FDF3D8`), with "Suppression is on — placements measuring under 40 were dropped on 22 Jun, which is why the lowest band is thin rather than empty." **Placements driving the score** — six rows with **pod position as a first-class column**, because first-in-pod measures roughly twice mid-pod. Rail: a working suppression toggle (34×20 pill, `#E67A1F` on) whose copy changes by state, an attention floor control (40/55/70) where each option states its consequence — the 70 note actively steers the user away — and *How the score is built* with the four input bars and a link to the methodology page.

**Tab 4 — Targeting.** Read-only, one row per facet (`170px minmax(0,1fr)`): Format · Markets · Inventory · Audience · Supply path · Lists · Flight and floor. Chips tinted blue for values, **amber for the attention selection**, neutral for counts. Notes carry the honest parts — Jacksonville dropped, contextual substituted, and "Rebuilt nightly on a 30-day measurement window. Placements entering or leaving the pool are applied without a new Deal ID." Rail: *Editing a live deal* ("the Deal ID stays the same and your DSP needs no change") and **Not applied** — the two things we didn't do.

**Tab 5 — History.** `132px 24px minmax(0,1fr)`. Timestamp, a 9px dot whose fill encodes event type (advisory orange, attention amber, live green, delivery blue, request neutral), title + body. Five events newest-first: the Tampa suggestion, suppression enabled (noting the Deal ID was unchanged), activation, delivery (naming the builder and the 34-minute build), the original request.

---

### 4 · Marketplace — `/curation/marketplace`

**Six categories, 37 packages.** Chips right of the title: **PGAM Attention** (amber when active, with the attention icon) · Sports · Audience · Online video · Display · Seasonal. The amber treatment on the first is the point: it's a different *kind* of category.

| Category | Count | Framing |
|---|---|---|
| PGAM Attention | 7 | "Built from placements we've measured, not publisher reputation. Each package is rebuilt nightly as attention scores move, so the inventory stays current for the life of the deal." Do not soften this. |
| Sports | 5 | Bought on daypart and rights; attention reported, not used as the selection filter. |
| Audience | 10 | Built from content signals rather than purchased segments, because that's what supply supports. Where a real segment would be better, the card says so. |
| Online video | 5 | Cheaper reach than CTV with lower measured attention — judge on completed views, and pair with CTV rather than substituting. |
| Display | 4 | Attention sits well below any video format; reported plainly, priced accordingly. Buy for presence and frequency. Completion renders `—`. |
| Seasonal | 6 | Assembled ahead of the season; finite and priced on demand. |

**Recommendations** — a "Recommended for Ridgeline HVAC" strip of three compact cards, each giving its reason ("Position alone explains most of last quarter's lift", "Closest match to the homeowner audience you keep asking for"). Derived from the advertiser's last six deals.

**Spotlight** — on PGAM Attention only: a wide amber band for the top package (86, Live Sports Attention) with its stats broken out on a bordered right column.

**Toolbar** — search · a **format chip whose label is derived from the active category** ("Online video", "Display", "CTV", "All formats" for cross-format categories) so it can never contradict the cards · Any budget · and a working **sort** (Attention / Price / Scale) that reorders the grid.

**Cards**, 3-col: name + sub-line; a **score tile** top-right — amber with "ATTENTION" for attention packages, neutral reading **"REPORTED"** for everything else (attention *selected* the inventory in one case and is merely *observed* in the other; the card must not blur that); body at `flex:1` so footers align; tags (first tag amber on attention packages); a From / Avail / Completion strip between hairlines; an **availability bar** with an honest read ("78% open", "34% left · filling up", "17% left · limited" — blue bar, red under 40%); a freshness line ("Rebuilt last night as attention scores moved" / "Inventory list reviewed weekly"); then **Get Deal ID** and a **Compare** toggle.

**Availability must be keyed to package identity.** In the prototype it's `AVAIL_PCT[p.name]`. Indexing a sorted array makes the figure change when the user re-sorts — a live-data lie, and a real defect caught in review.

**Compare** — up to three packages; a dark tray appears with the names and a Compare button; the modal lays them side by side (`150px repeat(3, minmax(0,1fr))`) on Package · Attention · **Selection basis** ("Attention-selected" vs "Attention reported only") · From · Avail · Completion · Availability · Inventory, closing with "Attention scores are comparable to each other only within the same 30-day measurement window — all three above were scored last night."

**Activation sheet** — 520px modal: DSP + Seat, then "Marketplace packages are pre-assembled, so these are usually issued within **15 minutes**." → the same honest async state, meta reading "Marketplace package · The Trade Desk · mrdn-ttd-4471".

---

### 5 · Reporting — `/curation/reporting`

Header: Last 30 days ▾ · **vs prior 30 days ▾** (blue-tinted, the comparison is on by default) · a saved view chip · Export.

**Five KPIs**: Spend "+18% vs prior 30 days" · Impressions "+12%" · CPM "−2% · your price" · Completion "+0.4pt" · **Attention 76** "68 baseline" (amber).

**Delivery and attention by day** — 14 blue bars with a 2px `#E67A1F` marker per bar at that day's attention score, forming an attention trend across the top. The card **stretches to the rail's height** (`align-items:stretch`, bar container `flex:1; min-height:168px`) — with `align-items:start` it leaves a ~240px empty band.

**Rail**: *Attention outcome* (amber, "+16% above your non-curated CTV", "79 against a 68 baseline on the same advertisers, markets and flights", plus per-genre bars) and **"What we can't show you yet"** — required, not optional: "Auction-side metrics come from the supply platform, and not every path reports them. We leave them blank rather than showing zero", then Win rate / Bid requests / Fill rate each with a `—` and a reason.

**Attention against what you paid** — a scatter, dot size by spend, amber for attention-curated and grey for not, with the 76 portfolio average drawn as an amber rule. Caption: "Curated deals cost more per thousand and sit higher on attention. The question the chart answers is whether that trade is worth it for this advertiser."

**Movers this period** — four rows of what improved and what got worse, green/red.

**Send this to the client** — wrap-deck card, restating that figures are your price throughout.

**Breakdown table** — five dimensions (By deal / market / publisher / device / **advertiser**), grid `minmax(0,2.1fr) .9fr .8fr .9fr .7fr .8fr .9fr .9fr`: dimension · Spend · **Δ vs prior** · Impressions · CPM · Completion · Attention · Pacing. Totals row on `#FCFDFE` including the aggregate delta. The advertiser rollup is the shape a client report actually takes, and its note states partiality ("4 of 6 advertisers with delivery in range").

Footnote: "ⓘ Figures are your price. Attention is measured on delivered impressions; imported deals report delivery only."

---

### 6 · Fulfilment queue — `/admin/curation/queue` · **PGAM staff only**

The operator console that makes the async promise real. Claim → checklist → paste Deal ID → deliver. **Gate behind `curation.admin`; never grantable to a tenant user.** Entered from an amber-dotted chip in the top bar.

Opens with a dark banner: "● PGAM staff view — Agencies never see this screen. No buyer-facing surface shows what's here."

**4 KPIs**: Unclaimed 4 "oldest waiting 22 min" · In build 3 · Median build time 36 min · **Past the ETA 1** "buyer already notified of delay" — this card uses the *warning* palette, not amber (amber is attention only).

**Queue** `minmax(0,1.9fr) 1fr .9fr .9fr .9fr` with `gap:14px` (without the gap, Waiting and State collide): Request (+ Attention badge + brief line) · Tenant · Waiting (`#B94018` when late) · State chip · Owner. Selected row `#F4F7FF` + `inset 2px 0 0 #0B3CFF`.

**Work panel** (sticky, 420px): checklist with a "3 of 5" count, completed items struck through in `#6B7383` — read the brief · assemble placements from the attention pool · sanity-check the floor against clearing prices · create the deal in the supply platform · confirm the seat is entitled. Then **Paste the Deal ID** (mono field: disabled-looking until the checklist completes, then focused blue, then success-green) with "Paste exactly what the platform returned. We check the format and the seat entitlement before it reaches the buyer." Then **Deliver Deal ID to buyer** — gated on the full checklist, becoming "Delivered · buyer notified". Secondary: "Ask the buyer" (this is what produces the paused-build card on the buyer's Home) · "Hand off". Footer always shows the buyer's promised ETA.

**No economics on this screen.** Cost, fees and margin belong to `/admin/curation/pricing`, gated by `curation.pricing` ⊂ finance, deliberately not designed here.

---

### 7 · Attention methodology

The credibility document for the whole thesis. Reached from every "How we measure it" link and from ⌘K. **Do not paraphrase this copy.**

- Amber hero: "**Attention is measured, not modelled.**" — "Every score comes from observed behaviour on delivered impressions. We do not infer attention from publisher reputation, from price, or from whether an impression was technically viewable. If we cannot measure a placement, it has no score — and we say so rather than estimating one."
- **The four inputs**, each with weight, a real explanation and this deal's value: Screen share 35% (92) · Dwell 30% (84) · Audibility 20% (78) · Pod position 15% (71). Then: scores are normalised to 0–100 against a rolling 30-day distribution of all measured CTV placements; **50 is the median placement, not a pass mark**; a score is only comparable to others from the same window, which is why packages are rebuilt nightly rather than scored once.
- **What attention is not** — not viewability ("a placement can be 100% viewable and score 30") · not a brand-safety score · not a prediction of performance · **not comparable across formats** ("a 62 on display is strong for display").
- Rail: **Coverage** (CTV 94% · online video 88% · display 71% · **imported deals 0%**, the weak ones muted) with "Where coverage is partial, the deal's score is calculated on the measured portion only and the coverage figure travels with it"; **Independence** ("Measurement runs on the impression stream, not on our own sales data. Nobody at PGAM can adjust a placement's score, and a publisher cannot pay to raise one… Independent verification is on the roadmap for v2. Until it lands, treat these as our own measurements and hold us to them."); and a client-facing one-pager download.

---

### 8 · Empty and failure states

A reference screen holding six states (reachable via ⌘K). **Wire each into its real context.** Every one says what happened, what it means for money already in flight, and what to do next — in that order.

1. **First run, Deals** — "No deals yet", both routes in (build one in ~40 min, or take a package in 15), and a note that existing PMPs elsewhere can be imported and reported on.
2. **Request declined** — red-bordered. "We couldn't build this one." "A $9 floor doesn't clear attention-qualified CTV in these markets — the lowest we've seen clear this quarter is $19. We stopped rather than build you a deal that wins nothing." Then **three ways forward** (raise the floor / switch to Monitor only at $14 / keep $9 and move to online video), Edit the request, Talk to the named person, and "Nothing was charged and nothing was created. Your draft is intact."
3. **Supply platform unavailable** — warning-bordered, "New deals are paused. Live deals are fine", split explicitly into **Unaffected** (every live Deal ID still serving; your DSP needs no action), **Delayed** (creation and forecast avails; requests queued in order, not lost), **Behind** (delivery current to 09:00; attention measurement unaffected).
4. **Forecast unavailable, per field** — the panel keeps the fields it has and blanks only the one it lost: available impressions renders `—` with an `unavailable` badge and "Supply platform isn't answering. Retrying every 30 seconds", while CPM and attention still show. "You can still submit. We'll confirm scale before building, and come back to you if it isn't there." **Never a whole-panel error, never a zero.**
5. **No marketplace matches** — names the query, explains *why* the combination doesn't exist ("Display under $5 exists, but not with a homeowner signal — that combination isn't available on this supply path"), then offers the two nearest real packages.
6. **Attention not measured, imported deal** — spend and completion shown, attention `n/a` in grey, and "We didn't build this deal, so we never saw its impression stream — there is no score to show and we won't estimate one. Rebuild it through PGAM and attention starts measuring from day one."

---

## Command palette (⌘K)

Global. ⌘/Ctrl+K toggles, Escape closes, typing filters live, with an empty state ("Nothing matches. Try a deal name, an advertiser, or a Deal ID."). Reaches every screen including the staff queue, the methodology page and the states reference; opens specific deals; and copies a Deal ID as an action. In production this should also search real deals, advertisers and Deal IDs.

---

## Interactions

| Trigger | Result |
|---|---|
| Top-bar nav | Switches screen. Active: white 500 + `inset 0 -2px 0 #2E6BFF`. `submitted` keeps **New deal** active; `ready` and `detail` keep **Deals** active. |
| Builder attention tier | Re-drives all four rail values **and shows a delta under each**. |
| Supply path Advanced | Reveals the device-ID option; leaving Advanced clears it. |
| Device-ID checkbox | Raises the blocker, changes the review count, disables and relabels Create. |
| Create deal | Only when unblocked → building state + toast (7s). |
| Disclosure row | Expands `ab-in .22s`; the flag badge hides while open. |
| "Guide me →" / "Describe it instead" | Opens the 420px drawer. Scrim, ✕ or Escape closes. |
| Copy Deal ID | Clipboard write; button becomes "Copied" in the success palette for 2000ms. Independent states on detail and ready. |
| DSP chips | Swap the activation steps. |
| Detail tabs | Swap the panel; tab resets to `activation` on every navigation into detail. |
| Suppression toggle / attention floor | Flip the pill / swap the consequence note. |
| Saved view chips | Switch the library view. |
| Row checkbox | Selects (must `stopPropagation`); shows the bulk tray. |
| Select-all | All / none, with an indeterminate `–` when partial. |
| Row `⋯` | Opens the row menu (`stopPropagation`); Escape or an action closes it. |
| Marketplace category | Swaps title, blurb, count, format chip and cards; amber only on PGAM Attention. |
| Sort | Reorders cards by attention, price or scale. |
| Compare toggle | Adds to the tray, caps at three. |
| Get Deal ID | 520px sheet → building state with marketplace meta. |
| Reporting dimension | Swaps the first column header, rows and note. |
| Queue row | Loads the request into the work panel. |
| Checklist item | Gates the Deal ID field and the deliver button. |
| ⌘K / Escape | Palette open/close; Escape also closes drawer, sheet, compare and row menu. |
| Enter / Space | Activates any focused control. |

Hover: cards lift, rows go `#F8FAFC`, secondary buttons go `border-color:#D5DEFC; color:#0925A8`, primary `#0A34DB`, nav items white.

---

## Accessibility

Not a follow-up task; it was specified and built.

- Visible focus: `:focus-visible { outline:2px solid #0B3CFF; outline-offset:2px; }`
- Every interactive element has a role, a tab stop and keyboard activation. The prototype uses `role` + `tabIndex="0"` + `data-key-activate="1"` with one document-level Enter/Space handler (84 controls), because it has no component library. **In production use native `<button>`, `<input>`, `<table>` instead** — the semantics come free and the scaffolding disappears.
- `role="tab"` on the detail tab bar; `role="checkbox"` + `aria-checked` on the checklist, device-ID and suppression toggles; `aria-label` on icon-only controls (row actions, palette trigger).
- Text inputs are excluded from global key handling so typing works.
- Expect a VPAT request. Contrast was a factor in choosing `#0B3CFF` over the brand `#1E90FF` — see Design system reconciliation.

---

## State

```
screen      "home" | "builder" | "submitted" | "ready" | "deals" | "detail"
            | "marketplace" | "reporting" | "admin" | "method" | "states"
back        screen to return to from the methodology page
tab         "activation" | "delivery" | "attention" | "targeting" | "history"
tier        0|1|2            builder attention tier
prevTier    prior tier, drives the forecast deltas
deviceId    boolean          Advanced device-ID requirement → blocker
open        { [section]: boolean }   builder disclosure rows
dsp         0|1|2            activation instruction set
cat         0..5             marketplace category
sort        0|1|2            attention | price | scale
compare     string[]         up to 3 package names
compareOpen boolean
view        0..3             saved view
sel         string[]         selected deal names
menu        deal name | null row action menu
dim         0..4             reporting breakdown
suppress    boolean          low-attention suppression
attnFloor   0|1|2            40 | 55 | 70
attnFilter  boolean          library attention-only filter
paletteOpen boolean, query string
copied, copiedReady   boolean (2s)
drawer      null | "geo" | "attn" | "describe"
sheet       null | packageName
toast       boolean (7s)
submittedFrom  "builder" | packageName
work        queue index; checks { [item]: boolean }; delivered boolean
```

### Data the implementation needs

- **Deals**: id, name, advertiser, brand, format, status (`draft|requested|building|blocked|live|paused|ended`), dealId, seat, dsp, floorCpm, flight, budget, delivered, impressions, cpm, completion, **attentionScore (pre and post)**, isAttentionCurated, isImported, targeting, placement count, history events.
- **Forecast** per targeting change, each field tagged with provenance (`magnite_forecast | pgam_historical | pgam_estimate | measured`) **and able to fail individually** — see failure state 4.
- **Capabilities** per supply path — drives which options are disabled *and the human-readable reason shown inline*. A data requirement, not a UI concern.
- **Warnings** per draft: severity (`blocker|advisory|ok`), message, and the field it points at.
- **Attention**: score by deal, placement, band, market, genre; the four measurement inputs; suppression state and floor; **coverage percentage per format**; the measurement window a score belongs to.
- **Packages**: score, selection basis (attention-selected vs reported), cpm, avail, completion, tags, **availability percentage keyed to the package**, last-rebuilt timestamp.
- **Recommendations** per advertiser, each with a stated reason.
- **Fulfilment**: queue entries with tenant, wait time, state, owner, the ETA shown to the buyer, checklist progress, and the median build time by deal shape that produces the ETA.
- **Notifications**: email, web push, Slack channel per user.

Deal creation is a **job**, not a mutation: the client polls or subscribes, and the UI must be correct at every intermediate state including "blocked, waiting on the buyer".

---

## Assets

- `assets/pgam-mark.png` — official PGAM mark, rendered 20×20 in the top bar.
- Fonts: Inter 400/500/600 and JetBrains Mono 400/500. Use however the codebase already loads fonts.
- Icons are inline SVG or single glyphs (`✓ ✕ ▲ ◷ ⓘ ▾ ▴ › — ● ⋯ ⌕ ◌`). Replace with the codebase's icon set, but keep the attention target as-is — it's a product mark: an `r=5` circle stroked `#B45309` at 1.3 with an `r=1.7` `#E67A1F` fill.

## Files in this bundle

| File | What it is |
|---|---|
| `CLAUDE_CODE_PROMPT.md` | **Start here.** Paste into Claude Code as the first message. |
| `Attentive Buying.dc.html` | The prototype — all eleven states, all interactions working. Click through it before coding. |
| `support.js` | Prototype template runtime. **Reference only — do not port.** |
| `assets/pgam-mark.png` | Logo. |
| `pgamcurationdesignbrief.md` | Original design brief. |
| `pgamcurationplatformaudit.md` | Internal product/UX/architecture audit — routes, permissions, reporting architecture, reuse-vs-build-new per screen. Read §5, §11, §12, §14. |

Read the two markdown documents for the reasoning behind the constraints; this README states the constraints themselves.
