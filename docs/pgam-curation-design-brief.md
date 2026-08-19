# PGAM Attentive Buying — design brief

Companion to `docs/pgam-curation-platform-audit.md`. Written to be handed to a
separate design session (Claude Design, or a human designer) so the output fits
the platform instead of needing a rebuild.

**Read the audit's §5 (user journey), §11 (screens) and the Attentive Buying
addendum before designing.** This brief is the constraints layer, not the spec.

---

## 1. Before anything: honor the design system that already exists

`pgam-dsp-dashboard` has a real, documented design system. A design pass that
ignores it produces work that has to be thrown away.

### ⚠ There is a typeface conflict in the repo — resolve it this way

| Source | Says | Status |
|---|---|---|
| `CLAUDE.md` § Brand Standards | "Font: **DM Sans**" | **STALE** |
| `docs/design-system-ss.md` § Typography | "One typeface: **Inter**, loaded from the root layout alongside **JetBrains Mono**." Explicitly lists DM Sans / DM Mono under "Was … Why it went: second system to maintain; split the platform in two" | **CURRENT** (2026-08 visual refresh) |

**Use Inter + JetBrains Mono.** `CLAUDE.md` predates the refresh and should be
corrected — worth a one-line PR so the next person doesn't hit this.

### The system in brief

**Type** — Inter for everything; JetBrains Mono (`font-mono`) for **IDs, codes and
hashes only**. Never mono for metrics: column alignment comes from
`tabular-nums`, not from a monospaced face.

| Role | Size | Weight | Tracking |
|---|---|---|---|
| Page title | 30–32px | 600 | −0.021em |
| Section heading | 15–17px | 600 | −0.006em |
| KPI value | 28px | 600 | −0.026em |
| Micro-label | 11px | 500 | 0.055em, uppercase |
| Table header | 11px | 500 | 0.055em, uppercase |
| Table data | 13px | 400 (numerics 500 + tabular) | — |
| Body / form label | 14px | 400/500 | — |
| Helper / caption | 12px | 400 | — |
| Button | 13px | 500/600 | — |

**Weight discipline: 400/500/600 only. 700 is not used in UI.** The refresh
demoted 439 `font-bold` call sites. Hierarchy comes from size, colour and
spacing; weight is the last lever, not the first. Do not reintroduce bold.

**Colour** — 12-step OKLCH ramps in `globals.css`, wired through
`tailwind.config.ts`. Use ramp tokens (`bg-pb-2`, `text-nv-11`, `border-pb-6`),
not hex:

- `pb-1…12` — primary blue. `pb-9` is solid brand (≈#0B3CFF), `pb-10` hover,
  `pb-11/12` text on tints, `pb-1/2` backgrounds.
- `nv-1…12` — navy neutrals. `nv-7` muted text, `nv-12` headline ink.
- **`am-1…12` — amber. RESERVED for attention metrics.** From `CLAUDE.md`:
  *"The moment a user sees amber in Specta, they should know it means attention.
  Do not use for CTAs, states, or decoration."*

**This is a gift for this product.** Attentive Buying is *about* attention, and
the design system already reserves a colour that means exactly that. Amber should
carry the attention layer throughout — scores, tiers, lift, the PGAM Attention
marketplace category — and nothing else. Resist using it as an accent.

Semantic states stay separate from both accent and amber: success `#16A34A`,
critical `#DC2626`, warning `#F59E0B` (legacy aliases still active).

**Tone** — light theme, white/`#F8FAFC` surfaces, navy-gradient sidebar, 16px
card radius, 1px `#E5E7EB` borders. `CLAUDE.md`: *"No playful UI. Premium,
analytical, enterprise tone."* Dark mode scaffolding exists at
`:root[data-theme='dark']` but no toggle is wired — design light, keep tokens
theme-safe.

---

## 2. Who this is for

Not ad-tech engineers. Four real users, in rough order of volume:

| User | Behaviour that must shape the design |
|---|---|
| **Agency trader** | Creates their eleventh similar deal this month. Wants six fields and a Create button. **Will abandon a nine-step wizard.** Optimize for their speed above all |
| **Media planner** | Building a plan, needs forecast + scale before committing. Cares about provenance of numbers |
| **Programmatic director** | Reviews, approves, compares SSPs. Needs the economics they're allowed to see, and per-SSP comparison |
| **Account team** | Least technical. Needs the guided path and plain-language errors. Must never see an inventory group |

Design for the trader; make the account team's path a drawer off the same screen,
never a separate flow.

---

## 3. What to design, in priority order

| # | Screen | Why this order |
|---|---|---|
| 1 | **Deal builder** (`/curation/new`) | The product. Everything else is around it |
| 2 | **Deal Library** (`/curation/deals`) | Where traders live day two onward |
| 3 | **Package detail** — Overview · Deal IDs & Activation · Targeting · Delivery · Attention · History | Where the Deal ID is collected and performance is read |
| 4 | **Marketplace** (`/curation/marketplace`) incl. the **PGAM Attention** category | The one-click path; the attention story's shop window |
| 5 | **Admin fulfilment queue** (`/admin/curation/queue`) | Internal, but it is the whole v1 fulfilment path — bad design here costs ops real time daily |
| 6 | **Deal Health card** | The differentiator's surface |

---

## 4. Non-negotiable UX constraints from the audit

These come out of verified platform behaviour. Violating them produces a design
that cannot be built or that misleads a buyer.

1. **One screen with progressive disclosure — not a nine-step wizard.** Audit §5.1.
   A trader fills ~6 fields and submits; a novice opens a drawer per section. Both
   produce the *same* object. One validation path, one schema.

2. **Never show a fake instant.** In v1, deal fulfilment is human-in-the-loop
   (~minutes to hours, not seconds). The submit state must say what is really
   happening, with an ETA computed from measured p50 — never a hardcoded "5
   minutes." Design a *requested → building → ready* state with push
   notification, not a spinner that implies synchronous creation. This is the
   single most important honesty requirement in the product.

3. **Label every number's provenance.** Three forecast tiers —
   `Magnite forecast` / `PGAM historical` / `PGAM estimate` — and a single
   forecast legitimately mixes them (Magnite avails + PGAM CPM estimate is the
   expected case). Design a per-field source affordance, not one badge for the
   whole panel.

4. **Never show an SSP name in Automatic supply-path mode.** Existing policy:
   *"Magnite / SpringServe / ClearLine branding is never user-facing."* Say "PGAM
   Supply." Only Advanced mode names them.

5. **Capability-driven disabling, with the reason.** Each SSP publishes what it
   supports (geo granularity, audience sync, formats). Unsupported options are
   disabled **with an explanation inline** — never a submit-time failure.

6. **The Deal ID is the payload.** Large, copyable, unmissable, with per-DSP
   activation steps beside it. It is the thing the buyer came for.

7. **Warn at build time, not submit time.** Floor above market, over-restrictive
   inventory, invalid ZIPs, audience below k-anonymity floor, and the CTV trap:
   `Device ID Required` **drops most CTV inventory**. Rank warnings by severity;
   blockers read differently from advisories.

8. **Amber means attention, everywhere, and only that.** §1 above.

9. **Attention is a first-class field, not a badge.** Attentive Buying is the
   product: attention tier as a selectable inventory source, an Attention Score on
   every deal pre- and post-flight, suppression as a toggle. Design it into the
   spine of the builder, not as a decoration on a generic curation form.

10. **Margin is never visible to an agency.** Buyer sees their price only.
    Enforced server-side — but the design must not have a slot where supply cost
    or PGAM margin would sit for a tenant user.

---

## 5. What NOT to design yet, and why

- **Automated/instant deal creation flows** — until the vendor probes (audit
  §19.6) come back, we don't know if v1 is synchronous. Designing an instant flow
  first risks a rebuild. Design the honest async state; it degrades gracefully to
  instant, not the reverse.
- **Demographic / in-market audience pickers** — audit §3.4: not syncable to
  either SSP today. Design the *attention* and *contextual* audience layers, which
  work now. Leave a slot, don't build the shelf.
- **Multi-SSP side-by-side comparison UX** — needs two providers that can
  actually create. Schema supports it; the UX waits.
- **White-label theming** — the foundation exists (`partner_branding`), but token
  discipline matters more than a theming UI right now. Design with tokens so
  theming is later free.

---

## 6. Paste-ready prompt for a design session

> I'm designing **Attentive Buying** — a self-service platform where agency media
> buyers create curated programmatic PMP deals (Deal IDs) that they then activate
> in their own DSP. The differentiator is attention: deal inventory is selected by
> *measured* attention scores on CTV placements, and attention is reported back as
> an outcome. Users are agency traders, media planners, programmatic directors and
> account teams — **not** ad-tech engineers.
>
> Design these screens, in this order: (1) the deal builder, (2) the deal library,
> (3) package detail with a Deal IDs & Activation tab, (4) a marketplace of
> pre-built deals including a "PGAM Attention" category.
>
> **Honor this existing design system exactly:**
> - Typeface **Inter** throughout; **JetBrains Mono** for IDs/codes/hashes only —
>   never for metrics (use `tabular-nums` for numeric alignment).
> - **Weights 400/500/600 only. Never 700/bold.** Hierarchy from size, colour and
>   spacing; weight is the last lever.
> - Type scale: page title 30–32px/600/−0.021em · section heading 15–17px/600 ·
>   KPI value 28px/600/−0.026em · micro-label 11px/500/0.055em uppercase · table
>   data 13px/400 · body 14px · caption 12px.
> - Colour: 12-step OKLCH ramps. `pb-9` solid brand blue (≈#0B3CFF), `nv-12`
>   headline ink, `nv-7` muted text, `pb-1/2` tinted surfaces.
> - **Amber is reserved exclusively for attention metrics** — never for CTAs,
>   states or decoration. Since attention is this product's whole thesis, let amber
>   carry the attention layer and nothing else.
> - Light theme; white / `#F8FAFC` surfaces; 16px card radius; 1px `#E5E7EB`
>   borders. Premium, analytical, enterprise. **No playful UI.**
>
> **Hard UX constraints:**
> - The builder is **one screen with progressive disclosure**, not a multi-step
>   wizard. A sophisticated trader fills ~6 fields and hits Create; a less
>   technical user opens a guided drawer per section. Same underlying object.
> - Deal creation is **asynchronous** in v1 — a person fulfils it in minutes to
>   hours. Design an honest *requested → building → ready* state with a real ETA
>   and push notification. **Never** imply instant creation.
> - Every forecast number carries its **source** — "Magnite forecast", "PGAM
>   historical" or "PGAM estimate" — and one panel can mix sources, so the
>   affordance is per-field.
> - **Never show SSP names** (Magnite, PubMatic) in the default "Automatic" supply
>   mode — say "PGAM Supply". Only an opt-in Advanced mode names them.
> - Unsupported options are **disabled with an inline reason**, never a
>   submit-time error.
> - **The Deal ID is the payload**: large, copyable, with per-DSP activation
>   instructions beside it.
> - Warnings surface **while building**, ranked by severity, with blockers visually
>   distinct from advisories.
> - Attention is **structural**: attention tier as a selectable inventory source,
>   an Attention Score on every deal pre- and post-flight, low-attention
>   suppression as a toggle.
> - There must be **no slot** where supply cost, SSP fees or PGAM margin would
>   appear — agencies see only their own price.
