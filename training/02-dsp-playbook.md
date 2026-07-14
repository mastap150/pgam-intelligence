# DSP Playbook

Everything demand-side. The DSP is at `~/Desktop/pgam-dsp-dashboard`, Next.js 14 + Neon, deployed to Vercel. Prod = `dsp.pgammedia.com`. Demo = `demo.dsp.pgammedia.com` (password-gated, fixture-driven).

## Mental model

The DSP is a thin layer in front of SpringServe/ClearLine. We take orders from advertisers, translate them into SS campaigns + demand tags, and take a 10% platform fee. Two things live only in the DSP (never in SS):

- **Advertiser-facing gross CPM** (what they pay us)
- **Agency name** (Entrepreneur, InHouse, etc.)

The SS demand tag rate is the media-cost CPM only.

## Campaign build flow

1. Advertiser onboarded (Neon `accounts` row + campaign order).
2. Build campaign in the DSP Wizard. Wizard writes to Neon + pushes to SS.
3. QA the SS side manually — the Wizard→SS payload has known field-mapping drops (see below).
4. Attach demand tag(s) with the correct rate and `bid_floor_type: static`.
5. Domain allowlist / blocklist as needed.
6. Freq cap set. **Loosening freq cap is the first lever for under-pacing** (Amazon 3/1 → 10/1 worked). Evaluate this before touching daily_budget.
7. Ship. Monitor pacing + margin.

## Wizard→SS field-mapping drops (known issues)

Audit was done on live Q2 OLV campaign. Fields that don't map cleanly:

- Frequency cap payload shape — SS demand_tag PUT requires **flat** `frequency_caps` with native field names (`frequency_cap_value`, NOT `cap`). The `pushTargetingToClearline` shape silently no-ops if you use `cap`.
- Inventory group requires a **DealList** group (default 271). DomainList/AppBundleList inventory groups get rejected by SS.
- Deal wiring: `POST/GET /deals` returns 404 on the public SS API. PR #122 auto-deal-list is inert in prod. Capture the real endpoint via Chrome DevTools before retrying.

QA every new campaign against these until the wizard is fully fixed.

## Buyer agent + apply levers

The buyer agent auto-optimizes live campaigns. Six apply levers are wired:

1. `freq_cap` — loosen when under-pacing, tighten when over-delivering
2. `publisher_blacklist` — cut low-quality publishers
3. `daily_budget` — nudge up/down
4. `budget_pacing` — asap vs even distribution
5. `creative_pause` — pause bad creatives
6. `margin pause` — halt if margin drops below threshold

Three gates are still needed for full automation (see `pgam_dsp_buyer_agent` memory / audit report for current state). Until then, buyer agent runs suggestions; humans approve.

## Audience framework

Retargeting + Lookalike (LAL) plumbing shipped on `feat/audience-framework` (2026-07-08), flag-gated behind `DSP_AUDIENCES_ENABLED`. Vendor still TBD. Do not turn on for a campaign without Priyesh sign-off on vendor + data flow.

## SpringServe quirks worth memorizing

- SS `/deals` API not exposed publicly (as above)
- Demand tag inventory_groups **only** accept DealList type
- Frequency cap PUT payload shape (as above)
- Never leak gross CPM, agency name, or client identity in SS

## Neon layout (DSP side)

- Project: `round-frog-99233431` (shared with SSP; DSP uses `public` schema, SSP uses `pgam_direct`)
- Env vars: `NEON_*` set are **static** (integration disconnected 2026-04-23 to unblock preview builds). Rotate manually on DB rotation.
- `NEXT_PUBLIC_API_URL` **must end in `/api/v1`.** A bare host breaks every client API call.

## Demo env

- URL: `demo.dsp.pgammedia.com`
- Password-gated
- Fixture-driven via client `fetchApi` short-circuit
- **Hides** margin, finance, admin panels, and agency names
- Use for sales demos and outside stakeholders

## `/ss-marketplace` bridge

Marketplace is wired to canonical ClearLine packs + activate flow (2026-06-23). Demo works today; prod is gated behind `NEXT_PUBLIC_MARKETPLACE_ACTIVATE_ENABLED`. Flip checklist:

1. Seed real dealIDs
2. Confirm SS env
3. Smoke test
4. Flip the flag

See `03-ss-marketplace-playbook.md`.

## Repo pitfalls (things that will bite you)

- Stale `.git/rebase-merge/` Finder dupes cause false "rebasing" status
- Pre-push hook races with a concurrent `next build` in another window
- Multi-session edit drift — always check `git status` before writing
- `.env.local` and `node_modules` symlink required for new worktrees

## Where to look when things break

- `pgam_dsp_repo_pitfalls` memory / doc — first checklist
- Vercel deploy logs
- Neon query console (read-only for most work)
- SS UI for demand tag / inventory group state
