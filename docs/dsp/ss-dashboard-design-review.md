# Self-serve dashboard — pre-launch design review

**Surface:** `demo.dsp.pgammedia.com/ss-dashboard` (Attune self-serve, demo host)
**Code:** `mastap150/pgam-dsp-dashboard` @ `1bb5630` —
`src/app/(self-serve)/layout.tsx` (585 lines), `src/app/(self-serve)/ss-dashboard/page.tsx` (1,147 lines),
tokens in `src/app/globals.css` `.ss-redesign`, rules in `docs/design-system-ss.md`
**Date:** 2026-08-18

> The demo host is password-gated and blocked by this session's egress proxy, so this
> review is from the source, not from screenshots. Everything below is a code citation,
> not an impression.

---

## Verdict

The token layer is genuinely good — one typeface, a 3-step ink ramp with fixed WCAG
contrast, two border weights, four radii, four shadows, and `docs/design-system-ss.md`
explaining every choice. **The dashboard barely uses it.** It hand-rolls its own
version of every primitive, stacks three attention-grabbing tinted surfaces before
the user reaches a single number, and offers eight separate paths to "create a
campaign" on one screen.

So this is not a re-skin. It's a **subtraction pass**: make the page obey the system
it already ships with, and cut the redundancy. Elegance here is almost entirely a
removal problem.

---

## A. Structure

### A1 — Every page is double-wrapped
`layout.tsx:573` wraps all children in `mx-auto max-w-[1200px] px-6 py-8`. The
dashboard then opens with its *own* `max-w-[1560px] mx-auto px-6 lg:px-7 py-7 pb-16`
plus a redundant `min-h-screen bg-[var(--ss-bg)]` and a second `.ss-redesign`
(`ss-dashboard/page.tsx:298-300`).

Consequences:
- Side gutters are 48–56px, not 24–28px. Top padding is ~60px.
- `max-w-[1560px]` is dead — the parent clamps to 1200.
- Sibling pages declare *different* widths: 1140 (`ss-help`), 1180 (`ss-grow`,
  `ss-capabilities`), 1240 (`ss-results`, `ss-reporting`, `ss-templates`,
  `ss-measurement`), 1300 (`ss-campaigns/new`), 1560 (`ss-dashboard`). Content
  edges therefore shift page-to-page and **none of them lines up with the 1200px
  nav above**. That misalignment is a large part of why the surface feels unsettled.

**Fix:** the route-group layout owns width, gutters and vertical rhythm. Pages render
sections only — no `min-h-screen`, no `mx-auto max-w-*`, no `.ss-redesign` re-add
(`design-system-ss.md` §8.1 already says this).

### A2 — Two nested right rails
Outer grid `[minmax(0,1fr) 264px]` holds *Learn the basics* / measurement nudge /
*Need a hand?* (`page.tsx:305`, `:731`). Inside the main column, a second grid
`[1.65fr 1fr]` puts *Live activity* in its own right column (`page.tsx:605`). At
≥1100px that's three columns and two rails, one sticky. It reads like two dashboards
sharing a viewport.

### A3 — Eight ways to start a campaign, on one screen
1. Hero free-text → `Build it` (Claude extraction → wizard)
2. Four suggestion chips → `/ss-campaigns/new?goal=…`
3. `Create campaign from scratch` → `/ss-campaigns/new`
4. `AI Quickstart from a URL` → `/ss-campaigns/quickstart`
5. Topbar `+ New campaign` (persistent, `layout.tsx:392`)
6. `New campaign` in the Recent-campaigns card header
7. `Get started` in the empty state
8. Footer `Use the Pro wizard →` → `/ss-campaigns/new-pro`

Behind those sit five creation routes (`new`, `new-pro`, `quickstart`, `url`,
`request`) plus `/ss-templates`. Choice this wide reads as indecision, and the
footer line — *"Need full control over targeting and bidding?"* — quietly tells the
user the main path is the limited one.

**Target: two.** One primary (describe your goal → we plan it) and one secondary
(build it yourself). Everything else becomes a step inside those, or lives on
`/ss-campaigns`.

### A4 — Nav overflow fires at every width
`layout.tsx:349-374` renders the first four links inline and *always* mounts
`MoreMenu` for the rest. There's no width condition, so **Tools and Help sit behind
"More ▾" on a 27" monitor**. Separately, labels are `hidden lg:inline`, so at
768–1023px the nav is six unlabeled icons with no tooltips or `aria-label`.

### A5 — The demo/live nav split is dead code
`NAV_LINKS` and `DEMO_NAV_LINKS` are byte-identical (verified). Yet ~40 lines of
comments describe a 3-item demo nav vs a 9-item live nav, and the `useDemoMode()`
branch still ships with an acknowledged first-paint nav shuffle. Delete the branch
and the comments; keep one array.

### A6 — Six full-width bands before any data
Order today: greeting → draft banner → gradient hero → onboarding checklist → KPI
strip → AI insights → split → footer link. The KPIs — the reason someone opens a
dashboard — are the fifth thing down, under ~500px of chrome. For a returning
advertiser that inverts the priority completely.

---

## B. Look and feel

### B1 — Three shouting surfaces in a row
- Hero: `linear-gradient(115deg,#101A78,#1641DE 52%,#3358F0)` with
  `shadow-[0_16px_38px_rgba(22,65,222,.28)]` and an orange radial blob.
- Onboarding card: `linear-gradient(120deg,var(--blue-50),#fff)`, border `#C9D4FF`.
- AI insights card: **the same gradient and the same border.**

Two identical tinted cards separated only by the KPI strip reads as a template
repeat, and the hero's shadow is roughly 4× the heaviest token (`--sh-pop`). Nothing
recedes, so nothing leads.

### B2 — The dashboard ignores its own design system
| Rule (`design-system-ss.md`) | Dashboard |
|---|---|
| §5 `.ss-card` for containers | **0 uses**, 29 hand-rolled `rounded-[…] border border-[var(--line)]` |
| §5 `.ss-label` for eyebrows | **0 uses**, inline `text-[11px] font-semibold uppercase tracking-[.1em]` ×6 |
| §3 four radii (6/8/10/12) | 2, 8, 10, 12, **14** — 14px is off-scale |
| §1 `text-ui-*` scale | 9 arbitrary `text-[Npx]` values |
| §2 tokens only | 7 raw hexes: `#C9D4FF`, `#EEF1F6`, `#F3D6BC`, `#101A78`, `#3358F0`, `#fff` |
| §1 weights 400/500/600 | `font-semibold` on 11px micro-labels (the doc dropped these to 500 deliberately) |

`/ss-results` does it right one click away (`ss-label` + `num-display`, line 510).
The doc's own §9 already lists "KPI cards exist in three shapes" as a known gap.

### B3 — Two color systems one click apart
`/ss-campaigns` still uses the legacy `nv-/pb-/am-` ramps at 24 call sites, while
`/ss-dashboard` is on `--ink/--line/--blue`. Same session, two palettes.

### B4 — Fabricated data on a real advertiser's screen ⚠️ *ship blocker*
The KPI sparklines are literal arrays: `[40,55,50,70,62,82,100]` for Calls & leads,
`[30,48,58,54,74,86,100]` for Total spend, `[60,60,80,80,80,60,60]` for Active
campaigns. The trend pills are hardcoded `▲ 8%` and `▲ 12%`, shown whenever the
value is non-zero — so an advertiser with $400 of first-week spend sees "▲ 12%"
against a curve that was never computed.

The rest of the file is scrupulous about exactly this: attention shows `Measuring…`
rather than a placeholder, the activity feed shows an honest empty state off-demo,
and `AI_INSIGHTS_ACTIVE` was deleted with a comment explaining that fabricated
per-account insights aren't acceptable. The sparklines are the last holdout. Either
wire them to real daily series or drop them.

### B5 — Smaller polish items
- Onboarding "next step" always renders a `Wallet` icon, whatever the step is
  (`OnboardingItem`, isNext branch).
- Glyphs and icon components mixed in the same rows: `⚡ 60 seconds`, `✎` draft,
  `▲` trends, `●` status, `×` discard, `→` text arrows sitting next to
  `<ArrowRight/>` components.
- `AttuneTitleOverride` runs a `MutationObserver` on `<head>` to rewrite
  `… | PGAM Media` → `… | Attune` on every render. Set the brand at the source.
- Brand stack is doing two jobs at once: Attune mark + wordmark + an
  `A PGAM MEDIA PRODUCT` caption in 9px uppercase.
- Three voices in the copy: warm ("Good afternoon, Priyesh"), urgent ("⚡ 60
  seconds"), instructional ("Tips to get the best results") — plus "Or skip the AI:",
  which frames the flagship feature as something to escape.

---

## C. What "more elegant" concretely means here

1. **One shell.** Layout owns width (1240), gutters, rhythm. Pages emit sections.
2. **One tinted surface per screen.** Hero keeps the gradient; onboarding and
   insights become plain `.ss-card`s. Drop the hero shadow to `--sh-md`.
3. **Data first for returning users.** KPI strip directly under the greeting.
   Hero moves down, or collapses to a single input row once a campaign exists.
4. **Two creation paths, not eight.**
5. **One card, one label, one KPI tile** — real shared components, used by
   dashboard / results / reporting alike.
6. **No decorative data.** Real series or no series.
7. **One rail.** Merge Live activity into the single right rail, or drop it to a
   full-width strip under the campaigns table.
8. **Progressive chrome.** New account → checklist + hero. Funded account with live
   campaigns → numbers, campaigns, one nudge. Same route, two states.

---

## D. Ready-to-paste Claude design prompt

Everything below the line is the prompt. Hand it to Claude Design (or `/design` in
Claude Code) as-is.

---

Redesign the dashboard of **Attune**, a self-serve streaming-TV (CTV) advertising
platform built by PGAM Media. Route is `/ss-dashboard`; it's the first screen an
advertiser sees after logging in. We're days from shipping it live and it currently
feels busy and template-ish. **The goal is elegance through subtraction, not a new
visual language.**

**Who uses it.** Owners and marketing managers at small and mid-sized US businesses —
restaurants, law firms, medical and insurance practices, local services. Budgets
$1k–$25k/month. They are not media buyers. They open this screen to answer three
questions: *Is my money working? What should I do next? How do I launch another one?*
They are not fluent in impressions, CPM, pacing or dayparting, and they will not
explore a gallery of tools.

**Brand.** Confident, calm, modern-American-tech. Adjacent to Linear, Stripe and
Ramp in restraint — not consumer-playful, not enterprise-grey. Light mode only for
now. Trustworthy above all: this screen is about someone's ad spend.

**Design tokens — use these exact values, do not invent new ones.**
- Type: Inter only. 400 / 500 / 600 — never 700. Tabular numerals on every metric.
- Scale: page title 30–32px/600/−0.021em · section heading 15–17px/600 · KPI value
  28px/600/−0.026em · micro-label 11px/500/0.055em uppercase · table data 13px ·
  body 14px · caption 12px · button 13px · nav 13px.
- Ink: `#0C1017` primary · `#545C6B` body · `#6B7383` labels and captions.
- Lines: `#EEF0F5` interior dividers · `#E3E7EF` container edges. Two weights only —
  borders recede, content carries structure.
- Surface: page background `#F6F7FA`, cards white.
- Brand blue: `#1641DE`, dark `#0F32B0`, tints `#F5F8FF` and `#EAEFFF`.
- Semantic: green `#17A34A`, amber `#B45309`, red `#DC2626`, orange `#D96D00`, each
  with a ~8%-tint background.
- Radius: 6 badges · 8 inputs and icon tiles · 10 buttons · 12 cards. Nothing else.
- Elevation: four near-invisible ink-tinted shadows, heaviest reserved for popovers.
  Depth should be felt, not seen. **No large colored glows.**
- Content column: 1240px max, 24–28px gutters, one container — never nested.

**Hard constraints.**
1. **No invented numbers, no decorative charts.** Every value, trend and series must
   be one a real account could produce. Where a metric isn't measurable yet, design
   the honest state ("Measuring…", "—", or a one-line explanation) rather than a
   placeholder curve. This is non-negotiable: we removed fabricated insights and fake
   sparklines from this page for exactly this reason.
2. **One tinted or gradient surface per screen, maximum.** Today three compete.
3. **Reuse one card, one micro-label, one KPI tile** everywhere. If two containers
   differ, they must differ for a reason you can state.
4. WCAG AA: ≥4.5:1 for body and micro-labels; never rely on color alone for campaign
   status.
5. Never center a number. Numeric columns right-aligned, tabular. A delta shares a
   baseline with its value so it reads as an annotation, not a second metric.
6. Wide tables scroll inside their own container; the page body never scrolls
   sideways.

**Content that must appear somewhere** (yours to prioritize, group, or demote):
- Greeting and one orienting line.
- Wallet balance and an `Add funds` path — campaigns are prepaid, so an empty wallet
  is the #1 blocker.
- Four KPIs: Calls & leads · Total spend · Active campaigns · Attention score (0–100,
  PGAM's proprietary metric — treat as a first-class differentiator, and design the
  "not measurable yet" state).
- Recent campaigns: name, date range, status (Live / Paused / Preparing / Draft /
  Complete / Rejected), spend, budget pacing.
- Setup checklist, 4 items: business details · upload a creative · add funds ·
  connect measurement (optional). Dismissible, with progress.
- Resume-an-unfinished-draft affordance.
- **One** primary campaign-creation entry: a free-text goal input, e.g. *"Get more
  weeknight diners at my restaurant for about $2,000/month"* → AI builds the plan.
  Plus **one** secondary "build it myself" path. **Cut from eight to two** — today
  the same screen offers a hero input, four goal chips, "from scratch", "AI Quickstart
  from a URL", a persistent topbar button, a table-header button, an empty-state
  button, and a footer link to a "Pro wizard". Fold the rest into those two, or move
  them to the campaigns page.
- Help and support: a few "learn the basics" links and a `Book a call` path. These
  matter (the audience is non-expert) but must not outweigh the data.
- Measurement nudge, shown only while no conversion source is connected.

**Design these artboards:**
1. **Returning advertiser, desktop 1440×1024** — the primary case. 2–4 live
   campaigns, funded wallet, real numbers. Data should lead; creation should be
   available in one click without dominating. This is the artboard we ship first.
2. **First-run, desktop 1440×1024** — no campaigns, $0 balance, nothing measurable.
   The checklist and the single creation path lead; empty KPI tiles must feel
   intentional and calm, not broken or apologetic.
3. **Mobile 390×844, returning state** — assume owners check this from a phone.
   Decide explicitly what drops away.
4. **Component sheet** — the card, the micro-label, the KPI tile (with value + unit +
   delta + empty variant), the six status pills, the campaign row, a primary and a
   secondary button, the free-text goal input. These become real shared components.

**Judgment calls I want you to make and annotate:**
- Where the creation input sits for a returning user versus a first-run user — same
  route, two states. Should it shrink to a single row once campaigns exist?
- Whether a right rail earns its width at 1240px, or help content belongs under the
  fold / behind Help. Today there are *two nested* rails; at most one survives.
- How the Attention score earns visual weight as a differentiator without inventing
  precision it doesn't have.
- Vertical rhythm: aim for the returning user to see greeting, all four KPIs, and the
  top of the campaigns table above the fold at 1440×900. Today they scroll past
  ~500px of chrome first.
- Navigation: six items (Dashboard · Campaigns · Results · Grow · Tools · Help)
  fronting 28 routes. Fix two bugs while you're there — overflow currently collapses
  Tools and Help behind "More ▾" at *every* width including 27" displays, and at
  768–1023px the nav renders as six unlabeled icons. Show me the topbar at 1440, 1024
  and 390.
- Copy voice. Today three collide: warm ("Good afternoon, Priyesh"), urgent ("⚡ 60
  seconds"), instructional ("Tips to get the best results"), plus "Or skip the AI:" —
  which frames our flagship feature as something to escape. Pick one voice and rewrite
  every string on the screen in it.
- Iconography: standardize on one line-icon set. Today lucide components sit beside
  literal glyphs (`⚡`, `✎`, `▲`, `●`, `×`, `→`) in the same rows.

**For each artboard, annotate:** what you removed and why, what you promoted and why,
and any place where honest empty states changed the layout. If a section can't justify
its pixels for a non-expert advertiser checking whether their ad money is working,
delete it and say so.
