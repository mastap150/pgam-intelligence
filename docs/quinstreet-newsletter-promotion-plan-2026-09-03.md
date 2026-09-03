# QuinStreet (CardRatings) × Newsletters — promotion plan

**2026-09-03.** Answers "how do we promote the CardRatings lane via
newsletters — one send a week, travel-rewards style?" against what is
actually built and approved in `mastap150/destination-com`.

---

## 1. Where the program actually stands

- **IO signed 2026-08-20** (non-branded / generic tier). Bryson Rosenberg
  (`brosenberg@quinstreet.com`) is the AM; compliance goes to
  `ccpubreview@quinstreet.com`.
- **Site design + splash pattern approved 2026-08-24** across all six
  surfaces (`docs/affiliate-applications/cardratings-launch-runbook.md`).
- **Tracked category URLs issued** (src=715848 visible in PR #564's
  verification) and the article side is mapped: nine generic category
  pages under `/points-and-miles/cards/*` (marketplace folded in from
  `/best-cards`, PR #554), points-and-miles essays retro-linked (PR #549),
  direct deep links site-wide (PR #565).
- **Email format approved by Bryson 2026-09-03** with two changes —
  footer-only disclosure, direct-to-`cardratings.com/bestcards/*` CTAs —
  applied in **PR #564, which is still open**. Merge it before any send
  work; everything below assumes its format.

**The catch: program approval is not send approval.** Section 3(h) of the
Publisher Terms requires **two written approvals per email creative,
from-line and subject** before a card block reaches a subscriber. The
"Approvals on file" table in
`docs/affiliate-applications/cardratings-email-approval-pack.md` is empty,
and `scripts/lib/cards-email-gate.mjs` renders nothing until
`CARDS_EMAIL_ENABLED=1` and the lane is in `CARDS_EMAIL_LANES` (CI fails
any sender that bypasses the gate). So nothing can be sent this week no
matter what cadence we pick — the approval pack submission is the
critical path.

## 2. Weekly cadence: yes to weekly *presence*, no to a weekly card email

A dedicated weekly credit-card newsletter is the wrong vehicle, for four
reasons grounded in the repo:

1. **The non-branded tier can't sustain weekly editorial.** No card
   names, no issuers, no offers, no rates — only nine generic categories
   with pre-approved copy. There is no fifty-two-issues-a-year of "travel
   rewards cards earn points on spending you already do." The monthly
   rotating-set issue (`send-cards-monthly.mjs`) is already the right
   ceiling for dedicated card sends.
2. **The list can't absorb a ninth lane.** ~2,140 contacts, eight lanes,
   an 18-hour cadence gate, and a flagship already 13pp under the travel
   open-rate benchmark (16.7% vs ~30%) with a documented 41.6%→16.7%
   engagement decay (`docs/NEWSLETTER_AUDIT_2026-08.md`). A weekly
   promotional send to a small list is a Gmail-reputation liability
   before it is a revenue line.
3. **Approval overhead scales per creative.** Every new creative/subject
   needs two written sign-offs. A weekly one-off invents 52 approval
   cycles; approved *templates* (block + subject formulas) invent one.
4. **Cards are not email's best economics yet.** Hotels pay 4% (~$48 per
   4-night stay); the generic card tier's EPC is unproven. The card block
   should ride along, not displace the sections that pay.

**What "weekly, travel-rewards style" should mean instead:** the card
block appears somewhere in the owned-email program every week, always
inside a travel context, never as the headline. The infrastructure for
exactly this is already built and gated:

| Lane | Vehicle | Card placement | Cadence |
|---|---|---|---|
| `trip_plan` | plan-request email | after the budget table; premium category for high/luxury budgets | on request |
| `trip_prep` | prep checklist | after the four prep tools | once per traveller |
| `weekly_digest` | Thursday flagship | "One more thing" slot, category rotating travel/hotel/cash-back/no-annual-fee | every other issue |
| `cards_monthly` | Cards Worth Considering | standalone, category-organised | first Tuesday |

Between the digest (every other week) and the always-on trip_plan /
trip_prep triggers, a card touch lands roughly weekly across the program
without a single new lane — and each CTA already carries
`var2=nl-{yyyy-mm-dd}-{lane}` so QMP segments newsletter revenue on the
`nl-` prefix.

## 3. The travel-rewards integration that makes it feel native

The Newsletter Engine redesign (`docs/NEWSLETTER_ENGINE.md`, designed
2026-08-30, not yet implemented) turns the Thursday flagship into "one
destination, assembled as a whole trip" — hero fare from the reader's
home airport, stays, total week cost, the plan. Its Section 5 ("One more
thing") is the designed home for the card block. That is the
travel-rewards setup: **match the category to the week's story.**

- Fare-led issue → airline or travel category
- Stay-led issue → hotel category
- Budget-destination issue → cash-back or no-annual-fee
- Premium/luxury issue → premium travel

This stays fully generic (compliant), gives the block a fresh frame every
week for free, and the rotation is known eight weeks ahead once the
engine's scoring ships — so card categories can be planned alongside
destinations. Shipping the engine redesign is therefore part of the card
promotion, not a separate project: a 16.7%-open flagship is a weak
carrier for anything.

## 4. Sequence

1. **Merge PR #564** (Bryson's format changes) — everything downstream
   renders through it.
2. **Submit the email approval pack** — the five stems in
   `cardratings-email-approval-pack.md` via Send Preview screenshots to
   `ccpubreview@quinstreet.com`, cc Bryson. Ask for the digest subject
   *formulas* to be approved as templates (the pack already says to) —
   that is what makes a recurring cadence possible without per-send
   approval. Keep `weekly_digest` out of `CARDS_EMAIL_LANES` until the
   formulas clear.
3. **On two written approvals per lane:** set `CARDS_EMAIL_ENABLED=1`
   and `CARDS_EMAIL_LANES` (repo Actions variables), record approvers +
   dates in the pack's table, verify one real send path.
4. **Rollout order:** `trip_plan` + `trip_prep` first (fixed subjects,
   fastest to approve) → `cards_monthly` first-Tuesday issue
   (`--dry` first) → `weekly_digest` every other issue once formulas are
   approved.
5. **Report every editorial URL** that gains a program link via "URL
   Submission" to `ccpubreview@quinstreet.com` — unreported URLs risk a
   month's commissions retroactively.
6. **Grow the cards-interested segment while volume accrues:** newsletter
   capture is missing on news articles (audit §9 — cheapest conversion
   fix on the site) and the retro-linked points essays are natural
   capture surfaces; honour `card_content_opt_out`.
7. **Measure before scaling:** the QMP daily puller (PR #550) plus the
   `nl-` var2 prefix gives revenue per lane; fix the
   `newsletter_clicks`-vs-deliveries join the audit flags before trusting
   any CTR decision. Gate is per-issue performance, not send volume.

## 5. The 60–90 day goal this serves

Branded-tier promotion (per-card tracked URLs, the real money) comes
after proving volume on generic. Weekly integrated placements across
four lanes is exactly that volume proof — and when branded unlocks, the
"Cards Worth Considering" monthly and the dormant `/cards` vertical
(`CARDS_VERTICAL_ENABLED`) are already built for it.
