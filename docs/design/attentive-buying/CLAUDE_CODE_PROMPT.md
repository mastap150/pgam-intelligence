# Claude Code prompt — PGAM Attentive Buying

Paste this whole file as your first message to Claude Code, with the `design_handoff_attentive_buying/` folder present in the repo.

---

You're implementing a designed product surface into a real codebase. The design is complete and specified; your job is to build it, not to redesign it.

## Read these first, in this order

1. `design_handoff_attentive_buying/README.md` — the full design spec. Tokens, every screen, every interaction, the state shape, and the data the API must supply. This is the source of truth for what to build.
2. `design_handoff_attentive_buying/Attentive Buying.dc.html` — the working prototype. Open it in a browser and click through all eleven states before writing code. Nav switches screens; ⌘K opens a command palette that reaches every one including the staff and reference screens.
3. `design_handoff_attentive_buying/pgamcurationplatformaudit.md` — the internal product/architecture audit. §5 (journey), §11 (screens and UX rules), §12 (permissions), §14 (reporting). Read this for *why* the constraints exist.
4. `design_handoff_attentive_buying/pgamcurationdesignbrief.md` — the original brief.

## What this is

**Attentive Buying** lets agency media buyers create curated programmatic PMP deals (Deal IDs) and activate them in their own DSP. The differentiator is attention: inventory is selected by measured attention scores on CTV placements, and attention is reported back as an outcome. Users are traders, planners, programmatic directors and account teams — not ad-tech engineers.

## Do not port the prototype

The prototype uses a small template runtime (`support.js`) that exists only to make the design interactive. **Do not port it, do not copy its markup structure, do not treat its single-file shape as an architecture.** Rebuild the screens in this codebase's own stack, with its existing router, component library, data layer and test setup. Lift the exact values from the README — hex codes, font sizes, letter-spacing, radii, shadows, spacing — into whatever styling system the project already uses.

The audit indicates a React/TypeScript app with an existing RBAC layer (`src/lib/rbac.ts`), a saved-views component (`report_saved_views`), an agent chat component (`agent-chat.tsx`), and an `/inventory/packs` screen the Marketplace should extend. Prefer extending those over new primitives. Verify before you assume — if the codebase differs from the audit, the codebase wins and you should say so in your summary.

## The nine constraints that are not negotiable

These are product law. Breaking any one of them is a bug, not a style choice. README "Hard product constraints" has the full statement of each.

1. **No SSP names in default "Automatic" supply mode** — say "PGAM Supply". Magnite, PubMatic, SpringServe, ClearLine may appear only in opt-in Advanced mode.
2. **Every forecast number carries its own source badge** — `Magnite forecast` / `PGAM historical` / `PGAM estimate` / `measured`. One panel mixes sources, so the affordance is per-field, never panel-level.
3. **Never imply instant creation.** Creation is asynchronous: a human fulfils it. `requested → building → ready`, with an ETA from a rolling median and a real notification. No spinner pretending to work.
4. **Warn at build time, never at submit time.** Unsupported options are disabled in place with a human-readable reason from the capability matrix.
5. **Blockers are visually distinct from advisories.** Blockers: red, ✕, disable the CTA, which restates why. Advisories: orange, ▲, never block. Ranked by severity.
6. **The Deal ID is the payload** — 32px mono, copyable, per-DSP activation instructions beside it.
7. **Attention is structural** — a selectable inventory source, a score pre- and post-flight, and a suppression toggle with an adjustable floor. Amber is reserved exclusively for attention: never for CTAs, states or decoration.
8. **There must be no slot anywhere in the buyer UI where supply cost, SSP fees or PGAM margin could appear.** Never add a cost or net column, not even on staff screens reachable from this app. Label money "your price" where ambiguity is possible.
9. **The builder is one screen with progressive disclosure — not a wizard.** A trader fills six fields and hits Create; a less technical user opens a guided drawer per section. Same object either way.

There's a tenth rule that governs all of them: **the product never states something the data contradicts.** Unknown values render `—`, never `0` and never blank. Partial coverage says so ("4 of 6 advertisers with delivery in range"). A filter chip's label is derived from state so it can't disagree with the rows beneath it. Two defects found in review were exactly this class of error — treat any label you hard-code as suspect.

## Build order

Ship in this sequence; each stage is independently demoable.

1. **Deals library + package detail (Deal ID & activation tab).** Proves the read path and the payload. Include the muting rule and the Imported/Attention badges.
2. **New deal + the async lifecycle** (`requested → building → ready`). This is the product's spine. Deal creation is a **job**, not a mutation — the client polls or subscribes, and the UI must be correct at every intermediate state including "blocked, waiting on the buyer".
3. **Fulfilment queue** (`/admin/curation/queue`, gated on `curation.admin`, never grantable to a tenant user). The async promise isn't real without the operator side.
4. **Marketplace** — six categories, 37 packages, spotlight, sort, per-package availability, compare.
5. **Reporting** — five KPIs, the two charts, the Δ column, five breakdown dimensions, and the "what we can't show you yet" panel. That panel is required, not optional.
6. **Detail tabs** — Delivery, Attention, Targeting, History. The Attention tab is the product's proof; give it the placement-level table with pod position.
7. **Methodology page.** The credibility document for the entire thesis. Do not paraphrase its copy — it has been written carefully, including what attention *is not*.
8. **Empty and failure states.** The prototype has a reference screen with six of them. Wire each into its real context.

## Non-obvious things that will bite you

- **Availability, and anything else shown per item, must be keyed to the item's identity, not its position in a sorted array.** A package that changes its "34% left · limited" because the user re-sorted is a live-data lie. This was a real defect in review.
- **Attention scores are only comparable within the same 30-day measurement window**, which is why packages are rebuilt nightly. Don't cache a score and compare it to a fresh one.
- **Attention is not comparable across formats.** Display sits structurally below video. A 62 on display is good for display.
- **Editing a live deal keeps the same Deal ID.** The buyer's DSP needs no change. Say so in the UI.
- **Suppression changes the placement set nightly without reissuing the Deal ID.**
- **Every deviation from the request is surfaced explicitly on the ready state** (the prototype's example: one market dropped for lack of qualified supply, nothing else altered).
- **`—` vs `n/a` vs `0` mean different things.** `—` is not applicable yet, `n/a` is unmeasurable (imported deals), `0` is a measured zero. Never substitute one for another.

## Accessibility

Not a follow-up task. Every interactive element needs a real role, a tab stop, keyboard activation and a visible focus ring (`2px solid #0B3CFF`, `outline-offset:2px`). Use native `<button>`, `<input>` and `<table>` elements rather than reproducing the prototype's `div` + role scaffolding, which exists only because the prototype has no component library. Tab bars get `role="tab"`, checkboxes and toggles get `role="checkbox"`/`switch` with `aria-checked`. Expect a VPAT request; this audience is enterprise procurement.

## Design system

The README's "Design system reconciliation" section records the decision and its rationale. Follow it. In short: the product UI is the light, Inter-based system specified in the README; the dark PGAM Media brand system governs marketing surfaces. Both share the PGAM mark and a blue primary, with the product using a deeper blue for contrast on white. Don't mix the two.

## When you're done

Report: what you built, where it lives, which existing components you extended rather than replaced, anything in the audit that no longer matches the codebase, and any place you had to deviate from the spec — with the reason. If a constraint above appears to conflict with existing code, stop and raise it rather than picking one.
