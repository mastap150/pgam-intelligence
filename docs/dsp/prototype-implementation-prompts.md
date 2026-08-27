# Implementing the self-serve prototype — session prompts

Two prompts, for two separate Claude Code sessions against
`mastap150/pgam-dsp-dashboard`. Run them in order, days apart if need be.
They are deliberately not one prompt: the demo surface is a design exercise
with no product risk, the live port is a product decision with quite a lot
of it, and the second depends on answers the first does not need.

**Why two sessions and not one:** `pgam-intelligence@CLAUDE.md` — cost scales
with accumulated context, so split at the seam. Building thirteen screens and
then porting a subset of them is a seam.

---

## Before either prompt runs

1. **Merge PR #129 in `pgam-intelligence`.** Both prompts read
   `docs/dsp/prototype/` and `docs/dsp/ss-dashboard-prototype-handoff.md`,
   and neither exists on `main` until #129 lands. A fresh session clones
   `main` and will not find them.
2. **Merge PR #564 in `pgam-dsp-dashboard`** (the partner-label change), so
   prompt 2 is not rebasing around it.
3. **Answer the three questions in §3 of the handoff** — dayparting and the
   demographic filters, the channel picker, PGAM Optimized Network. Prompt 1
   does not need the answers. **Prompt 2 cannot start without them**, and
   says so in its own text.

---

## Prompt 1 — the demo surface

Scope: bring the `(attune)` demo routes up to the 27 August prototype. No
production route is touched. Roughly a two-to-three session job on its own;
the prompt tells it to land in reviewable pieces rather than one commit.

```text
Read these three documents before writing any code. They are the brief;
this message is only the framing.

1. mastap150/pgam-intelligence @ docs/dsp/prototype/ — the clickable
   prototype. Read the five part files (p_css / p_shell / p_pages2 /
   p_pages3 / p_js), not the concatenated build; they are the source.
   README.md in that directory lists the invariants. The JS is the
   derivation spec: every figure on every screen is a slice of one weekly
   ledger called WK, and that is why the screens agree with each other.
2. mastap150/pgam-intelligence @ docs/dsp/ss-dashboard-prototype-handoff.md
   — what is real, what is modelled, and §3, which maps each control to
   whether a backend exists. You do not need §3 for this task, but read it
   so you do not "improve" a caveat out of the copy.
3. This repo @ docs/springserve-capability-map.md — the source of truth §3
   is derived from.

TASK

Bring the (attune) demo routes — the /ss-*1 surface — up to the 27 August
prototype. They were last rebuilt at 7e8cbf6 against an earlier version of
the same prototype, so this is an evolution of existing pages, not a
greenfield build. Read each existing page before you change it.

Do NOT touch (self-serve). Do NOT create a third parallel surface with a "2"
suffix — /ss-dashboard1 is already the demo of this design lineage, and a
third copy triples the maintenance for a comparison the published prototype
already provides. If you conclude otherwise after reading the code, say so
and stop rather than building it.

WHAT CHANGED SINCE 7e8cbf6

- Thirteen screens, not twelve. New: an Integrations screen grouped by job
  (measurement / audiences in / results out / app attribution), five pixel
  install paths each with its own failure mode.
- The builder is five steps, not four: Goal / Who and where / Channels /
  Video / Budget & review.
- Builder step 3 leads with PGAM Optimized Network as the default way to
  buy, with the 53-channel catalogue visible but locked underneath it.
  Manual selection is the second option.
- Step 2 gains a 168-cell dayparting grid (drag to select; a day or hour
  label takes the whole row or column), household income, and IAB Tier 1
  content categories.
- Results gains a selectable attribution window (1/7/14/30 days) that
  restates lead count and cost per lead, seven breakdown dimensions rather
  than five (devices and creative added), an attention score on every
  breakdown row, and reach / frequency / cost per household.
- Help is 40 questions across 7 topics, nine of them on attention, with
  search that filters on answer text.
- The palette is the PGAM/TripleLift one-pager's, sampled from the PDF's own
  colour operators. See PALETTE below.
- A design-system pass: 9 type sizes, 4 radii, 3 tracking values, 3
  durations on one curve, spacing on a 4px grid, no shadow on static cards.

PALETTE

globals.css stores colours as HSL triplets. The prototype's tokens convert
to these — put them behind the existing CSS variable names rather than
introducing a parallel system:

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

Two rules that are not negotiable, both from the handoff §4.0:
- #1E90FF (accent) reaches only 2.67:1 against every ground in the system.
  Use it for fills, rules and the top hairline. Never for text, at any
  size, unless the ground is dark.
- The neutrals are navy-tinted, not grey. Do not substitute Tailwind's
  slate/gray scale for them — that tint is most of why the deck holds
  together.

The dark theme in globals.css needs the same treatment. The prototype is
light-only; derive dark by swapping the ink/ground roles, not by inverting.

CONSTRAINTS

- One ledger. These pages already read useSsLedger and agg() from
  @/lib/ss/ledger, and the prototype has the same invariant for the same
  reason. Every new figure must derive from that call. If a number cannot
  be derived, show a dash and say why in a caption — never type in a
  constant. A dash is not a zero, and the prototype is careful about the
  difference; keep it careful.
- Do not weaken an existing caveat. Where the prototype says a figure is an
  estimate, an eMarketer-class number, or a projection from past delivery
  rather than a forecast of supply, that sentence ships with the component.
- Accessibility: 4.5:1 for body text, 3:1 for large text and UI boundaries.
  WCAG 1.4.3 exempts logotypes, which is what lets the channel marks sit at
  4.11:1 on their own tiles. Nothing else gets that exemption.
- Match the surrounding code. These pages are heavily commented with the
  reasoning behind each section; write in that register.

HOW TO LAND IT

Not one commit. Work in this order, one PR per group, each independently
reviewable and each green before the next starts:

  1. Design tokens + the shared layout and nav.
  2. Dashboard, Results, Attention, Grow.
  3. Campaigns list, campaign detail, Creatives.
  4. The five-step builder. Largest and riskiest; do it once the token
     layer has settled.
  5. Tools, Billing, Settings, Help, Integrations.

Branch from main as claude/attune-prototype-<group>. Run the repo's own
checks before every push — tsc, lint, and next build; CI here is
typecheck-and-test and it must be green. Open each PR as a draft.

WHERE TO STOP

Ask me rather than deciding, if:
- a prototype screen needs a ledger field that does not exist;
- a control in the prototype has no equivalent anywhere in this repo and
  would need a new API route;
- you find a place where the prototype and the shipped demo disagree on a
  fact about the product, rather than on presentation.

The third one has already happened three times in this project and each
time the prototype was the one that was wrong.
```

---

## Prompt 2 — the live self-serve side

Scope: port to `(self-serve)`. Blocked on the §3 answers. This prompt is
written to refuse to start without them, which is deliberate.

```text
Read these first:

1. This repo @ docs/springserve-capability-map.md — what SpringServe on
   account 2724 can actually do. §A is the list of things our UI implies we
   can do and cannot.
2. mastap150/pgam-intelligence @ docs/dsp/ss-dashboard-prototype-handoff.md
   §3 — each control in the prototype mapped against that capability map.
3. mastap150/pgam-intelligence @ docs/dsp/prototype/ — the prototype itself.
4. The (attune) /ss-*1 routes in this repo — the same design, already built.
   Port from these, not from the HTML. They are typed, they read the real
   ledger, and they have already been through review.

TASK

Bring the live (self-serve) /ss-* routes onto the prototype's design.

THIS IS A PORT OF THE DESIGN, NOT OF THE CLAIMS.

§3 of the handoff lists, control by control, what has a backend. Roughly a
third of the prototype does not. Three items in particular the prototype
states more forcefully than the shipped demo does, and each needs a
human decision before the corresponding control ships live:

  a. Dayparting, age, gender, household income, audience segments. All
     collected today, none forwarded to SpringServe. The prototype gave
     dayparting a 168-cell grid and added income on top.
  b. The channel picker. Selections are never mapped; the supply tag is
     hardcoded. The prototype turned it into a wall of brand marks.
  c. PGAM Optimized Network. Promises a weekly rebalance on the customer's
     own results. Needs channel-level control and outcome optimization.
     We have neither.

BEFORE YOU WRITE ANY CODE: check whether those three have been decided —
look for a commit or a doc update resolving handoff §3, or ask me. If they
have not been, stop and say so. Do not port a, b or c on your own
judgement, and do not "solve" it by softening the copy without being asked.

WHAT TO PORT REGARDLESS

Everything in §3 marked ✅. Chiefly:

- The whole visual layer — palette, type scale, spacing, the card and
  table treatments, empty states, the design-system pass.
- Geo: radius, ZIP, state, DMA. Note §1.2 — the DMA crosswalk covers 38 of
  about 210 markets, so the picker has to say which markets are available
  rather than listing all of them.
- IAB Tier 1 categories, frequency cap, budget, daily pace, flight dates,
  creative selection and upload — all real.
- Results: the seven breakdown dimensions are real SS report dimensions and
  are under-used today. This is the single highest-value part of the port
  and it carries no claim risk at all.
- Pixels, CAPI and call tracking. Real end-to-end.

WHAT TO FIX WHILE YOU ARE IN THERE

- Attention scores are computed already (placement_attention_scores plus
  its Lambda) and self-serve reporting stubs them null. Wiring them is the
  top item on the capability map's opportunity list and it is our
  differentiator. If it is more than a day's work, scope it and come back
  rather than starting it inside this port.
- The Strategy Estimate is inline constants. The prototype's version
  derives from the account's own CPM and cost-per-lead history and shows a
  range. That is strictly more honest and should replace the constants
  whether or not anything else in this list happens.

CONSTRAINTS

- Behaviour-preserving unless a change is named above. This is live. A
  visual port that quietly alters what a control does is the failure mode.
- One ledger, same as the demo side.
- Every PR draft, every PR green on typecheck-and-test, one surface per PR.
- If a §3 ❌ or 🟡 control already exists in the live wizard, leave it
  exactly as it is. Making a dead control prettier is the specific harm §3
  was written to prevent.

WHERE TO STOP

Anything that would change what the buy does, as opposed to what the screen
looks like, comes back to me first.
```

---

## Notes for whoever runs these

- Prompt 1 is safe to run today. Prompt 2 is not, and its own first
  instruction is to check that.
- Neither prompt asks for the prototype to be reproduced pixel-for-pixel.
  Both point at the part files as the spec rather than the rendered page,
  because the JS is where the derivation lives and the CSS is where the
  system lives.
- If a session comes back saying the prototype is wrong about the product,
  believe it before you believe the prototype. That has been the pattern.
