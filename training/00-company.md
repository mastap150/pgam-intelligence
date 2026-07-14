# PGAM at a Glance

PGAM Media runs two independent commercial stacks. Understanding which stack you're touching matters more than any single tool.

## The two-stack rule

**Supply (SSP) stack** — we sell impressions from publishers to demand partners.
- Codebase: `pgam-direct` (SSP product, `~/Desktop/pgam-direct`) + `pgam-intelligence` (optimization agent, `~/Desktop/pgam-intelligence`)
- Platforms: LL and TB (two supply-side platforms running in parallel — see LL and TB playbooks)
- Admin UI: `admin.pgammedia.com` → served by the Next.js 14 app at `pgam-direct/web`
- Revenue target: LL = $10K/day, TB = $15K/day

**Demand (DSP) stack** — we buy impressions on behalf of advertisers.
- Codebase: `pgam-dsp-dashboard` (`~/Desktop/pgam-dsp-dashboard`), separate repo, Next.js 14 + Neon Postgres, deployed to Vercel
- UI: `dsp.pgammedia.com` (prod), `demo.dsp.pgammedia.com` (password-gated fixture demo)
- The DSP fronts SpringServe/ClearLine for direct campaigns; we take a 10% platform fee
- First DSP campaign was Amazon Business Q1 via Entrepreneur agency (brand awareness — VTR/CTR/viewability only). Call-center lead-gen campaigns come after.

**Rule:** the two stacks are separate. Cron jobs, DB rows, revenue, and margin are tracked independently. Never credit LL supply cron activity toward DSP demand readiness, or vice versa.

## Other properties

- **destination.com** — travel platform, Next.js 16, Expedia Partnerize affiliate. Repo: `~/Desktop/destination-com`. Native app in `~/Desktop/destination-app` (Expo + Clerk).
- **boxingnews.com** — content site, Sanity CMS, MSN syndication feed. Repo: `~/Desktop/boxingnews`.
- **healthnation.com** — AI-only content site. Repo: `~/Desktop/healthnation-web`.
- **visage** — celebrity recognition product. Repo: `~/Desktop/visage`.
- **pgam-wealth-agent** — Priyesh's personal wealth agent. Not a company product.
- **pgam-recon** — finance/SSP reconciliation automation. Separate repo.

## Commercial models to internalize

**DSP direct campaigns run under two models:**
- **CPM** — advertiser pays us per-thousand-impression (e.g., Tranzact). Their gross CPM is our top line; we buy media below that on SpringServe demand tags; the delta is margin.
- **CPA** — we bear the media cost and get paid per outcome (e.g., Legal TV Ads at $100/call). Media spend is a cost, not revenue. Margin math is different — track separately.

**One-way CPM ratchet (buyer agent):** setup CPM on a campaign is a **hard ceiling**. The buyer agent may descend to capture margin (with rollback safety) but must never ascend past setup CPM.

**Rate hiding — critical:** the gross CPM the advertiser pays lives in the PGAM dashboard/Neon only. **SpringServe only ever sees the media-cost CPM** on the demand tag rate. Never surface gross rates in SS.

**Agency-name hiding — critical:** Entrepreneur, InHouse, and other agency names live in Neon only. **Never in SS campaign names, tag names, creative names, or notes.** SS is not a trusted surface for that.

## Revenue targets

- LL: $10K/day (flat target, not derived)
- TB: $15K/day (flat target, not derived)
- Combined $1M/month is aspirational, not a plan
- DSP revenue is tracked separately per campaign; margin is what matters, not gross

## Who does what

- **Priyesh** — CEO/founder. Owns strategy, demand, engineering, wrapper.js, GAM, and all commercial decisions. Sole approver on P&L-affecting changes.
- **Claude sessions** — engineering + ops execution.
- **Sagar** — has LL UI access (may make manual UI changes that bypass our ledger — see security doc).
- **Vivek** — engineering contributor. **No P&L access, no financial data.** See CODEOWNERS lockdown in security doc.
- **Joseph Roa** — former engineer, offboarded 2026-05-19. All secrets rotated; auth lockdown shipped. Do not re-grant access.
