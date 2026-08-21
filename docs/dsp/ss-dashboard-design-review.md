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

Section D is a reference read of how **Vibe** solves the same job — twelve concrete
patterns pulled from their own platform docs via the Vibe MCP. Section E is the
copy-pasteable design prompt, which incorporates them.

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

## D. Reference: how Vibe structures the same job

**Sourced from the Vibe MCP's own platform documentation** (`get_business_rules`:
`getting_started`, `campaign_setup`, `pixel_tracking_reporting`,
`campaign_optimization_playbook`), pulled 2026-08-18.

> **On screenshots:** there are none, and not for lack of trying. The Vibe MCP is a
> data API — campaigns, metrics, targeting, business rules — with no screenshot or
> UI-rendering tool. `vibe.co`, `www.vibe.co` and `app.vibe.co` are all blocked by this
> session's egress proxy (HTTP 000), so their UI can't be captured from here either.
> What follows is therefore an **interaction and IA reference, not a visual one** — and
> it's arguably the more useful half. Vibe's screens don't feel intuitive because of
> their colour choices; they feel intuitive because of the twelve decisions below.
> To add visual reference, capture the screenshots manually from a logged-in Vibe
> session and drop them in `docs/dsp/reference/vibe/`.

Vibe is the closest comparable: same medium (CTV), same self-serve buyer, a genuinely
more complex product underneath — **seven** campaign goals with a per-goal feature
matrix, per-dimension targeting modes, 500+ channels — and it still reads simpler than
ours. That's the point worth internalizing: **their simplicity is not less product, it
is more defaulting.**

### D1 — Their dashboard is five state-driven blocks

Per their own docs, the Vibe dashboard shows: last-7-day trends · personalized
recommendations · the Performance Forecaster · a campaign status panel
(delivering / paused / blocked / draft) · the Vibe feed. Plus the Vibe Agent, their
in-product AI.

Every block answers a question the user already has. Compare ours: a gradient hero
asking *"What do you want to achieve today?"*, then a checklist, then KPIs, then three
static marketing cards.

### D2 — Recommendations are typed, not generic
Six types, each bound to a detectable account state and a specific fix:
**Set performance objective · Fix tracking · Optimize bidding · Optimize budget ·
Expand reach · Launch web traffic.**

And they state their basis: *"projected outcomes come from a rolling 6-month analysis
of how budget, audience and bidding changes correlated with impressions, sessions and
conversions across thousands of Vibe campaigns."*

Ours is a card titled *"Tips to get the best results"* holding three fixed marketing
links (Quickstart / templates / add funds) that never change with account state. Vibe's
version is the same slot doing real work. **This is the single highest-value pattern to
copy** — and note we already have the ingredients: wallet balance, pixel presence,
creative count, campaign pacing, attention score. A typed recommendation engine over
those five signals is honest, useful, and needs no new data.

### D3 — The Performance Forecaster: one concept, three fidelities, honest framing
A budget-vs-outcome curve for a single strategy. Budget on x, projected outcome on y,
outcome metric matched to the goal (impressions for awareness, sessions for
traffic/leads, purchases for sales). Exactly three markers: **Current** ·
**Recommended** · **New** (where the user's slider sits). Below Current, expect to lose
performance; above Recommended, you're scaling past their advice — *"a marker, not a
cap."*

Three surfaces, escalating interactivity: interactive slider on the campaign edit page ·
read-only curve with one-click apply on the dashboard card · read-only preview in a
"Ready to scale" popover in the campaigns list. **One idea, three densities** — the
opposite of our three-different-KPI-card-shapes problem.

Framing to steal verbatim: *"Projections are estimates — never present them as
commitments."* Also: the headline outcome is **always weekly** even when budget is
daily or lifetime, so the number is comparable across campaigns.

### D4 — Status taxonomy where every state names its own cause and exit
> Draft → Upcoming (up to 12h before delivery starts) → Delivering → Paused →
> **Inactive** (balance hit $0; resumes automatically once funded) → Completed (end
> date reached or lifetime budget spent) → Archived (7+ days after completion; cannot
> be reactivated — duplicate instead)

Ours: Live / Paused / Preparing / Draft / Complete / Rejected — where **"Preparing"
silently covers three different backend states** (`pending_review`,
`pending_approval`, `pending`) and tells the advertiser neither what's being waited on
nor how long. Two states we lack that users will hit constantly: an *Upcoming* with a
timebox, and an *Inactive (out of funds, auto-resumes)* that distinguishes "you ran
dry" from "you paused this".

### D5 — Waiting and empty states are designed, per source
Their audience statuses: Created (blue) → Populating (blue) → **No data** (orange, if
size is still zero 24h after populating) → Active (green) → Inactive (grey, unused 2+
weeks, reactivates automatically). Documented separately for manual CRM upload, CRM
integration sync, and web-traffic audiences.

`design-system-ss.md` §9 already concedes *"empty and loading states are the weakest
part of the surface."* Vibe treats "populating", "no data" and "dormant" as three
distinct designed states with colours and recovery paths. We mostly have "0".

### D6 — Collect only mandatory inputs; auto-configure the rest *silently*
Their creation flow's stated principles: collect **advertiser · goal · performance
objective · creative · budget amount · 2–3 audience answers** — and then, verbatim:
*"Do not ask for confirmation on auto-picked settings (campaign name, budget type,
flight dates, targeting, placement, bidding). Surface them only when calling
create_or_update_campaign."*

Auto-config works by finding the most similar prior campaign — same advertiser + goal,
else same goal + same industry — and reusing its targeting, placement and bidding.
Fallbacks are tabulated: name `[Advertiser] - [goal] - [Month YYYY]`, daily budget,
4-week flight starting next business day, suggestions mode, any-time-any-day slots,
automatic bidding, 5/day frequency cap.

**This dissolves our "simple wizard vs Pro wizard" split.** We ship two wizards
(`/ss-campaigns/new` at 4,948 lines and `/ss-campaigns/new-pro` at 3,363) because we
assumed the choice is *few fields vs many fields*. Vibe's answer is that there is one
flow with ~6 required answers and everything else silently defaulted from precedent,
then editable afterwards. Our footer link — *"Need full control over targeting and
bidding? Use the Pro wizard →"* — is the symptom.

### D7 — Two or three conversational questions instead of a targeting wizard
Not a paginated dimension picker. Two general questions, each carrying 2–3 educated
guesses inferred from the advertiser's industry and website:
- *"Where should this campaign run?"* — with suggestions shaped by business type
  (national DTC → nationwide; multi-location → their states; single location → their
  metro).
- *"Who are you trying to reach?"* — one bundled persona in plain language
  ("women 25–44 who are into fitness"), **not** age + gender + income + interests as
  four separate controls.

Their instruction: *"the advertiser should never have to pick segments themselves"*,
and *"do not enumerate every targeting field one by one or paginate dimensions in a
wizard."* The agent translates the answer into segments. Directly relevant: our Step 2
targeting has its own audit doc (`self-serve-step2-targeting-audit.md`).

### D8 — The default is the recommended one, and the escape hatch teaches
Targeting dimensions default to **Suggestions** (soft signals that guide the bidder)
rather than **Controls** (hard filters). Controls apply only when the user explicitly
asks — and then the UI explains the trade-off *before* switching: smaller audience
pool, fewer optimization opportunities, higher cost per result, slower learning,
possible delivery failure if too narrow. There is deliberately **no mode toggle
offered** in the happy path.

That's a general principle worth adopting: don't put the power-user control on screen
next to the recommended one. Make the recommended path the only visible path, and let
the escape hatch cost one question — which doubles as education.

### D9 — Education placed exactly where the confusion happens
- *"No clicks on CTV. Use view-through attribution, not CTR/CPC."*
- *"The Pixel page shows ALL events from ALL sources; the Reports page shows only
  events attributed to Vibe campaigns. Seeing events on the Pixel page but zero in
  Reports is normal early on."*
- *"Why Vibe numbers may differ from GA4: GA4 uses last-click session models, Vibe uses
  IP-based view-through — a modeling difference, not a tracking bug."*
- They even document their own trap: session-only report filters that silently diverge
  from the persisted campaign attribution window, *"a common source of 'why don't my
  numbers match' confusion."*

Every one of these pre-empts a support ticket at the moment of doubt. Our equivalent is
a right-rail card of four links titled "Learn the basics", all pointing at the same
`/ss-learn` hub — help *adjacent* to the product rather than *inside* it. Our attention
score has exactly this problem: it's a proprietary 0–100 number with no in-place
explanation of what it means or how it's derived.

### D10 — Prerequisites stated as arithmetic, with worked examples
Leads campaigns need Page View events in the last 12h, Lead events in the last 7 days,
and ≥0.1% conversion rate — *"5,000 PVs + 5 leads = 0.1% → can publish. 5,000 PVs + 4
leads = 0.08% → cannot publish."* Publish failures are enumerated per goal, and
post-publish troubleshooting is explicit ("allow up to 12 hours", "widen targeting
until the delivery estimator turns green", "raise manual CPM to ≥$18").

Our setup checklist is four labels — Business details / Upload a creative / Add funds /
Connect measurement — with no statement of *why* each gate exists or what breaks
without it. "Connect measurement" is even marked **Optional**, when it's the only
reason the Calls & leads KPI can ever be non-zero.

### D11 — Metrics come with typical ranges
CPM *"typical range $15–$30"* · completed view rate *"CTV typically 95%+"* · ROAS
*"3.0x = $3 earned per $1 spent"* · manual CPM *"stay above $15 for deliverability,
$18+ recommended"*. A number with a range attached is interpretable by a restaurant
owner; a bare number is not. Our KPI tiles give bare values — and then fake the trend.

### D12 — Simplicity argued as strategy, not taste
Their optimization playbook opens with *"Chapter 1 — Setup: Simplicity as a Competitive
Advantage"*: fragmenting into many small strategies starves the bidding algorithm of
the data volume it needs, and a complex setup is harder to diagnose. Their rule: *"if
two strategies share the same objective, target CPA and audience type, they should
almost certainly be one strategy. Add complexity only when you have a specific,
measurable, testable reason."*

That test generalizes to UI. **Two surfaces that serve the same intent for the same
user should be one surface.** Applied to us: two campaign wizards, five creation
routes, eight creation CTAs on the dashboard, two nested right rails, and three KPI
card shapes all fail it.

### What not to copy

- Vibe's product is genuinely more complex than ours underneath. The lesson is the
  **defaulting**, not the feature count — copying their surface area would make our
  problem worse.
- They run a hard tier split (Self-Serve with chat support vs Managed for $60k+/month),
  which lets the self-serve UI be opinionated and refuse to expose everything. We
  currently serve both audiences from one surface, which is exactly the pressure that
  produced the Pro wizard. Worth deciding deliberately rather than by accretion.
- Their conversational flows assume an always-available AI agent that can hold a
  multi-turn exchange. Where our AI is one extraction call against
  `/api/url-to-campaign`, borrow the *question structure* (2–3 questions, guesses
  offered, silent defaults) — not the assumption of open-ended dialogue.

---

## E. Ready-to-paste Claude design prompt

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
- Recent campaigns: name, date range, status, spend, budget pacing. Today's six statuses
  are Live / Paused / Preparing / Draft / Complete / Rejected — redesign the set so each
  state tells the advertiser what's happening and what to do: split the opaque
  "Preparing" into something with a cause and an expected wait, and add an out-of-funds
  state distinct from a user-initiated pause (an empty prepaid wallet is our most common
  stall, and it currently looks identical to "Paused").
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
  matter (the audience is non-expert) but must not outweigh the data — and prefer
  explaining a thing where it appears over linking out to a hub.
- A recommendations slot that changes with account state (see the reference product) —
  replacing today's three fixed marketing cards. Design the card for one recommendation:
  what's wrong or available, the projected effect if we can honestly state one, and a
  single action.
- Measurement nudge, shown only while no conversion source is connected.

**Reference product — study this and borrow its logic, not its pixels.**
Our closest comparable is **Vibe** (`vibe.co`), a self-serve CTV platform for the same
kind of buyer. Their product is *more* complex than ours underneath — seven campaign
goals, per-dimension targeting modes, 500+ channels — and still reads simpler, because
they default aggressively instead of asking. Patterns to carry over:

- **Their dashboard is five state-driven blocks:** last-7-day trends · personalized
  recommendations · a budget-vs-outcome forecaster · a campaign status panel · an
  activity feed. Every block answers a question the user already has. None of it is a
  marketing pitch.
- **Recommendations are typed, not generic.** Six kinds, each bound to a detectable
  account state and one specific fix: set performance objective · fix tracking ·
  optimize bidding · optimize budget · expand reach · launch web traffic. Design our
  equivalent slot this way — we can detect wallet balance, missing pixel, no creative,
  pacing, and attention score, which is enough for real recommendations. Today that
  slot holds three fixed marketing links that never change.
- **One idea at three fidelities.** Their forecaster is a single budget-vs-outcome curve
  with exactly three markers (current · recommended · where you've dragged to), rendered
  as an interactive slider on the edit page, a read-only card with one-click apply on the
  dashboard, and a small preview in a list popover. Do that instead of inventing a
  different card shape per page.
- **Estimates are labelled as estimates.** Their rule: *projections are estimates, never
  present them as commitments* — and their headline figure is always weekly so numbers
  stay comparable. Apply the same discipline to anything modelled or forecast.
- **Every status names its own cause and its exit.** Theirs: draft → upcoming (up to 12h
  before delivery) → delivering → paused → *inactive, balance hit $0, resumes
  automatically once funded* → completed → archived. Ours flattens three different
  backend states into one opaque "Preparing" and has no out-of-funds state at all, even
  though an empty prepaid wallet is our most common stall.
- **Waiting states are designed.** They ship distinct designed states for created ·
  populating · no data after 24h · active · dormant, each with a colour and a recovery
  path. Our own design doc concedes empty and loading states are the weakest part of the
  surface. Treat "nothing here yet", "still measuring" and "this stopped" as three
  different designs.
- **Metrics carry their typical range.** They print CPM $15–$30, completed-view rate
  95%+, "3.0x ROAS = $3 back per $1". A restaurant owner can interpret a number with a
  range attached and cannot interpret a bare one.
- **Education sits where the confusion happens,** not in a help hub — e.g. *"there are
  no clicks on CTV, so we measure view-through, not click-through"* placed next to the
  metric it explains. Our attention score especially needs this: it's a proprietary
  0–100 figure with no in-place explanation.
- **Their stated test for structure:** two things serving the same objective for the
  same user with the same target should be one thing; add complexity only for a
  specific, measurable, testable reason. Apply it ruthlessly to this screen.

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
- The creation flow this dashboard feeds. We currently ship two wizards — a standard
  one and a "Pro" one — because we assumed the choice is *few fields vs many fields*.
  The reference product instead runs one flow with ~6 required answers (advertiser,
  goal, objective metric, objective value, creative, budget) and silently defaults
  everything else from the user's most similar previous campaign, editable afterwards.
  Show me what the dashboard's entry point looks like if we commit to that model.
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
