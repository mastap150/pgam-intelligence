# Implementing the self-serve prototype — session prompt

One prompt against `mastap150/pgam-dsp-dashboard`, in two parts with a gate
between them. Part A is the demo surface and is safe to run now. Part B is
the live port and cannot start until three questions are answered; the
prompt is written to stop there rather than guess.

It is one prompt because the shared half — the palette, the invariants, the
stop conditions — is identical for both and should not be restated twice
with two chances to drift. It is in two parts because the second is a
product decision and the first is not.

**If you would rather run it as two sessions**, paste everything down to the
end of Part A into the first, and the preamble plus Part B into the second.
That is cheaper (`pgam-intelligence@CLAUDE.md`: cost scales with accumulated
context, so split at the seam) and the parts are written to survive it.

---

## Before it runs

1. **Merge PR #129 in `pgam-intelligence`.** The prompt reads
   `docs/dsp/prototype/` and `docs/dsp/ss-dashboard-prototype-handoff.md`,
   and neither is on `main` until #129 lands. A fresh session clones `main`
   and will not find them.
2. **Merge PR #564 in `pgam-dsp-dashboard`** (the partner-label change), so
   Part B is not rebasing around it.
3. **Answer the three questions in handoff §3** — dayparting and the
   demographic filters, the channel picker, PGAM Optimized Network. Part A
   does not need the answers. Part B will not proceed without them.

---

## The prompt

```text
You are picking up a design project that is finished on paper and not yet
built. Read the four sources below before writing any code. They are the
brief; this message is only the framing and the order of work.

SOURCES

1. mastap150/pgam-intelligence @ docs/dsp/prototype/ — the clickable
   prototype, thirteen screens. Read the five part files (p_css / p_shell /
   p_pages2 / p_pages3 / p_js), not the concatenated build; they are the
   source. README.md in that directory lists the invariants. The JS is the
   derivation spec: every figure on every screen is a slice of one weekly
   ledger called WK, and that is why the screens agree with each other.
2. mastap150/pgam-intelligence @ docs/dsp/ss-dashboard-prototype-handoff.md
   — what is real, what is modelled, and §3, which maps each control in the
   prototype to whether a backend exists for it today.
3. This repo @ docs/springserve-capability-map.md — what SpringServe on
   account 2724 can actually do, and the source of truth §3 derives from.
   §A is the list of things our UI implies we can do and cannot.
4. The (attune) /ss-*1 routes in this repo — the same design, one
   generation back. They were rebuilt at 7e8cbf6 against an earlier version
   of the same prototype. Sixteen pages, already typed, already reading the
   real ledger, already reviewed.

═══════════════════════════════════════════════════════════════════════
SHARED — applies to both parts below
═══════════════════════════════════════════════════════════════════════

PALETTE

The palette is the PGAM/TripleLift one-pager's, sampled from that PDF's own
colour operators rather than from a screenshot. globals.css stores colours
as HSL triplets; the prototype's hex tokens convert to these. Put them
behind the existing CSS variable names — do not introduce a parallel system.

  ground        210 100% 98%   (#F7FBFF)
  surface         0   0% 100%
  tint          212  88% 97%   (#EFF6FE)
  ink           220  49%  8%   (#0B1220)
  ink-2         220  29% 28%   (#33415C)
  ink-3         217  20% 44%   (#596A86)
  rule          211  67% 90%   (#D6E6F7)
  primary       210  88% 41%   (#0D68C5)
  accent        210 100% 56%   (#1E90FF)
  positive      142  78% 27%   (#0F7A37)
  warn           30 100% 43%   (#D96D00)
  negative        0  72% 51%   (#DC2626)

Two rules, both from handoff §4.0, neither negotiable:
- #1E90FF (accent) reaches only 2.67:1 against every ground in the system.
  Use it for fills, rules and the top hairline. Never for text, at any
  size, unless the ground is dark.
- The neutrals are navy-tinted, not grey. Do not substitute Tailwind's
  slate/gray scale for them. That tint is most of why the deck holds
  together.

The dark theme in globals.css needs the same treatment. The prototype is
light-only; derive dark by swapping the ink/ground roles, not by inverting.

INVARIANTS

- One ledger. The (attune) pages already read useSsLedger and agg() from
  @/lib/ss/ledger, and the prototype has the same invariant for the same
  reason. Every figure must derive from that call. If a number cannot be
  derived, show a dash and say why in a caption — never type in a constant.
  A dash is not a zero; the prototype is careful about that difference and
  so should the build be.
- Do not weaken a caveat. Where the prototype says a figure is an estimate,
  an eMarketer-class number, or a projection from past delivery rather than
  a forecast of supply, that sentence ships with the component.
- Accessibility: 4.5:1 for body text, 3:1 for large text and UI
  boundaries. WCAG 1.4.3 exempts logotypes, which is what lets the channel
  marks sit at 4.11:1 on their own tiles. Nothing else gets that exemption.
- Match the surrounding code. These pages are heavily commented with the
  reasoning behind each section; write in that register.

PROCESS

- One PR per group listed below. Each independently reviewable, each green
  before the next starts. Not one large commit.
- Branch from main. Run the repo's own checks before every push — tsc,
  lint, next build. CI is typecheck-and-test and it must be green.
- Open every PR as a draft.

STOP AND ASK, rather than deciding, if:
- a screen needs a ledger field that does not exist;
- a control has no equivalent anywhere in this repo and would need a new
  API route;
- the prototype and the shipped code disagree on a fact about the product,
  as opposed to on presentation.

The third has already happened three times on this project, and each time
the prototype was the one that was wrong. Believe the code.

═══════════════════════════════════════════════════════════════════════
PART A — the demo surface.  Start here.
═══════════════════════════════════════════════════════════════════════

Bring the (attune) demo routes — the /ss-*1 surface — up to the 27 August
prototype. This is an evolution of existing pages, not a greenfield build.
Read each page before you change it.

Do NOT touch (self-serve) in this part. Do NOT stand up a third parallel
surface with a "2" suffix — /ss-dashboard1 is already the demo of this
design lineage, a third copy triples the maintenance for a comparison the
published prototype already provides, and the last throwaway concept route
here went up as PR #550 and came back out as #552. If you conclude
otherwise after reading the code, say so and stop rather than building it.

WHAT CHANGED SINCE 7e8cbf6

- Thirteen screens, not twelve. New: an Integrations screen grouped by job
  (measurement / audiences in / results out / app attribution), with five
  pixel install paths, each carrying its own failure mode.
- The builder is five steps, not four: Goal / Who and where / Channels /
  Video / Budget & review.
- Builder step 3 leads with PGAM Optimized Network as the default way to
  buy, with the 53-channel catalogue visible but locked underneath it.
  Manual selection is the second option, not the first.
- Step 2 gains a 168-cell dayparting grid (drag to select; a day or hour
  label takes the whole row or column), household income, and IAB Tier 1
  content categories.
- Results gains a selectable attribution window (1/7/14/30 days) that
  restates lead count and cost per lead; seven breakdown dimensions rather
  than five, adding devices and creative; an attention score on every
  breakdown row; and reach, frequency and cost per household.
- Help is 40 questions across 7 topics, nine of them on attention, with
  search that filters on answer text.
- A design-system pass: 9 type sizes, 4 radii, 3 tracking values, 3
  durations on one curve, spacing on a 4px grid, no shadow on static cards.

ORDER

  A1. Design tokens, the shared layout, the nav.
  A2. Dashboard, Results, Attention, Grow.
  A3. Campaigns list, campaign detail, Creatives.
  A4. The five-step builder. Largest and riskiest — do it once the token
      layer has settled, not before.
  A5. Tools, Billing, Settings, Help, Integrations.

Branch naming: claude/attune-prototype-a1 … -a5.

═══════════════════════════════════════════════════════════════════════
GATE — read before starting Part B
═══════════════════════════════════════════════════════════════════════

Part B is a port of the DESIGN, not of the CLAIMS.

Handoff §3 lists, control by control, what has a backend. Roughly a third
of the prototype does not. Three items the prototype states more forcefully
than the shipped demo does, each needing a human decision first:

  a. Dayparting, age, gender, household income, audience segments. All
     collected today, none forwarded to SpringServe. The prototype gave
     dayparting a 168-cell grid and added household income on top.
  b. The channel picker. Selections are never mapped; the supply tag is
     hardcoded. The prototype turned it into a wall of brand marks.
  c. PGAM Optimized Network. Promises a weekly rebalance on the customer's
     own results. That needs channel-level control and outcome
     optimization. We have neither.

BEFORE ANY PART B CODE: check whether those three have been decided — look
for a commit or a doc update resolving §3, or ask. If they have not been,
finish Part A, report, and stop. Do not port a, b or c on your own
judgement, and do not resolve it by quietly softening the copy.

═══════════════════════════════════════════════════════════════════════
PART B — the live self-serve routes.  Gated.
═══════════════════════════════════════════════════════════════════════

Bring the live (self-serve) /ss-* routes onto the prototype's design.

Port from the /ss-*1 pages you built in Part A, not from the prototype
HTML. By then they are typed, they read the real ledger, and they have been
through review.

PORT REGARDLESS — everything marked ✅ in §3:

- The whole visual layer: palette, type scale, spacing, card and table
  treatments, empty states, the design-system pass.
- Geo: radius, ZIP, state, DMA. Note §1.2 — the DMA crosswalk covers 38 of
  roughly 210 markets, so the picker must say which markets are available
  rather than listing all of them.
- IAB Tier 1 categories, frequency cap, budget, daily pace, flight dates,
  creative selection and upload. All real.
- Results: the seven breakdown dimensions are real SS report dimensions and
  are under-used today. Highest value in the whole port, and it carries no
  claim risk at all.
- Pixels, CAPI, call tracking. Real end-to-end.

FIX WHILE YOU ARE IN THERE

- Attention scores are already computed (placement_attention_scores plus
  its Lambda) and self-serve reporting stubs them null. Wiring them is the
  top item on the capability map's opportunity list and it is the brand
  differentiator. If it is more than a day's work, scope it and come back
  rather than starting it inside this port.
- The Strategy Estimate is inline constants. The prototype's version
  derives from the account's own CPM and cost-per-lead history and shows a
  range rather than a point. Strictly more honest; it should replace the
  constants whether or not anything else here happens.

ADDITIONAL CONSTRAINTS FOR PART B

- Behaviour-preserving unless named above. This is live. A visual port that
  quietly alters what a control does is the failure mode.
- If a §3 ❌ or 🟡 control already exists in the live wizard, leave it
  exactly as it is. Making a dead control prettier is the specific harm §3
  was written to prevent.
- One surface per PR.

Anything that would change what the buy does, as opposed to what the screen
looks like, comes back for a decision first.
```

---

## Notes for whoever runs it

- Part A is safe today. Part B is not, and the gate says so in the prompt's
  own voice rather than relying on the operator to remember.
- Neither part asks for a pixel-for-pixel reproduction. Both point at the
  part files rather than the rendered page, because the JS is where the
  derivation lives and the CSS is where the system lives.
- If the session comes back saying the prototype is wrong about the
  product, believe it before you believe the prototype.
