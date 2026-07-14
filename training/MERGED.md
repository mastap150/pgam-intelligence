# PGAM Training — Combined

> This is every training doc concatenated. Paste into a Google Doc for a shareable link before Trainual is provisioned. The source of truth is the individual files in `pgam-intelligence/training/` — never edit here.

---


---

# PGAM Training

Source of truth for onboarding and standard operating procedures. Everything a new hire needs to be productive without breaking something expensive.

## How this is organized

- `00-company.md` — what PGAM does, the two-stack rule, revenue targets, who we sell to
- `01-security-nonnegotiables.md` — the rules that, if broken, cost real money or trust. Read first.
- `02-dsp-playbook.md` — DSP (demand) ops: campaigns, ClearLine, buyer agent, rate hiding
- `03-ss-marketplace-playbook.md` — `/ss-marketplace` self-serve marketplace ops
- `04-ll-playbook.md` — LL supply platform ops
- `05-tb-playbook.md` — TB supply platform ops
- `06-engineering-playbook.md` — repos, worktrees, Neon, Vercel, deploy discipline
- `99-scribe-shotlist.md` — the ordered list of Scribe screen recordings to make
- `ONBOARDING.md` — day-1 / week-1 / week-2 sequence
- `MERGED.md` — every file concatenated. Paste into a Google Doc for a shareable link before Trainual is provisioned.

## How to use this

1. **New hire:** read `01-security-nonnegotiables.md` before touching anything. Then follow `ONBOARDING.md`.
2. **Editor (Priyesh / Claude):** edit these markdown files directly. Commit and push. Never let Trainual or a Google Doc drift ahead of the repo — this is the source of truth.
3. **Distribution:**
   - Right now: paste `MERGED.md` into a Google Doc, share the link.
   - Once Trainual is provisioned: each `.md` file becomes one Trainual Topic. Copy-paste in.
4. **Scribe:** work through `99-scribe-shotlist.md` in order. Attach recorded Scribe links inline in the relevant playbook file.

## Keeping this current

Any time a non-obvious operational rule gets established (a new floor, a new freeze, a repo pitfall discovered the hard way), add it here. If it lives only in `CLAUDE.md` / auto-memory, only Claude sessions benefit — humans need it here.


---

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


---

# Non-Negotiables

Rules where breaking one costs real money, trust, or a client. Read this before touching anything. When in doubt, stop and ask.

## Commercial — data that must not leak

1. **Never leak agency names to SpringServe.** Entrepreneur, InHouse, and other agency identifiers stay in Neon only. Never in SS campaign, tag, or creative names. Never in SS notes.
2. **Rate hiding.** Gross CPM (what the advertiser pays us) is PGAM-dashboard-only. SpringServe demand tags only ever carry the media-cost CPM. Do not surface gross rates in SS ever.
3. **DSP CPM one-way ratchet.** The setup CPM on a campaign is a hard ceiling. Descending is fine (that's margin capture, with rollback safety). Ascending past setup CPM is not — the buyer agent must never do it, and neither should you.

## Partner-mandated limits (breaking these damages the relationship)

4. **Unruly write freeze** (dp=5). All automated writes to Unruly are choke-pointed off in `core/partner_freeze.py`. Do not remove the freeze until the compliance root-cause is fixed. If you're building a new writer, check the freeze list first.
5. **9 Dots contract floor.** Demands 692, 693, and 955 have a **$1.70 minimum** floor. Floors may go higher; they may never go lower. This is contractual.
6. **BidMachine partner QPS cap** (dp=40). The QPS cap is partner-mandated, not internal caution. 99% utilization is expected, not an opportunity to raise.

## People / access

7. **No P&L access for Vivek.** Codeowners locks Vivek out of financial paths; Clerk metadata restricts UI; SSP DB role is scoped. Do not widen his access. If a PR touches a P&L path, it does not merge without Priyesh review.
8. **Do not re-grant access to Joseph Roa** (offboarded 2026-05-19). All four DSP secrets were rotated. Auth lockdown shipped (PRs #200/#211/#213/#215).
9. **LL team UI access** — Sagar and possibly others have UI access. If you see unexplained drift in LL state that our ledger didn't cause, ask the team before assuming automation bug.

## Repo / code discipline

10. **Never `git add -A` in a worktree on pgam-dsp-dashboard.** Inspect `git status --short` first. Untracked `.env*` in DSP worktrees leaked prod secrets to main on 2026-07-02. Always add files by name.
11. **`.env` files are gitignored on pgam-dsp-dashboard** (since 2026-07-10). New worktrees must symlink both `node_modules` AND `.env.local` from the main tree, or the pre-push `next build` will fail.
12. **Always commit and push after edits.** Do not leave uncommitted work sitting on Priyesh's machine. Auto-commit is the default.
13. **QA after every ship.** Run the workflow, hit the endpoint, spot-check the DB row. No ship-and-assume.
14. **Never skip pre-commit / pre-push hooks (`--no-verify`, `--no-gpg-sign`)** unless Priyesh explicitly says to. If a hook fails, fix the underlying issue.
15. **Never `git reset --hard`, `push --force` to main, or delete a branch** without explicit approval. Investigate unfamiliar files/branches before overwriting — they may be someone's in-progress work.

## WP / content platform hygiene (learned the hard way)

16. **Dangling DNS is an attack surface.** The 2026-05-07 boxingnews `admin.` subdomain incident: a stale DNS record pointed at a recycled cPanel IP; a writer's WP creds were likely harvested. Delete DNS records for services you're not using.
17. **WP hygiene bar (regardless of host):** 2FA on every account, plugins/core auto-update, writers get Author role (not Editor/Admin), auto-update security patches. Multiple WPE hacks have been reported — the vector is always hygiene, not the host.

## Secret handling

18. **Secret rotation on offboarding.** When anyone with access leaves, every credential they touched rotates. Roa playbook is the reference.
19. **Managed secrets:** `DSP+SSP share one Neon project` (round-frog-99233431); DSP is `public`, SSP is `pgam_direct`. Do not cross the streams.
20. **`NEXT_PUBLIC_API_URL` on DSP must end in `/api/v1`.** A bare host breaks every client API call with `Unexpected token '<' DOCTYPE` errors. Common footgun on new deploys.

## When in doubt

Ask Priyesh before doing anything that:
- writes to a partner's system
- touches P&L data
- rotates or reveals a secret
- deletes anything on a shared system
- pushes to `main` on any repo without a preview build


---

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


---

# /ss-marketplace Playbook

The self-serve marketplace bridges the DSP UI to canonical ClearLine deal packs. Advertisers pick packs, hit activate, and the flow provisions the campaign side of SpringServe.

## Status

- Marketplace UI: live at `/ss-marketplace` in `pgam-dsp-dashboard`
- ClearLine wiring: shipped 2026-06-23
- **Demo:** works today at `demo.dsp.pgammedia.com/ss-marketplace`
- **Prod:** gated behind `NEXT_PUBLIC_MARKETPLACE_ACTIVATE_ENABLED`
- To flip on for prod, work through the flip checklist below

## Flip checklist (prod enable)

1. Seed real dealIDs into the marketplace pack config (`marketplace/packs/*.ts` or DB, depending on latest impl — verify current source)
2. Confirm SS env (`SPRINGSERVE_API_KEY`, `SPRINGSERVE_BASE_URL`) is prod, not sandbox
3. Smoke test end-to-end: pick a pack → activate → confirm SS demand tag created + inventory group attached + rate correct
4. Verify no gross rate or agency name leaks in the SS record (see security doc)
5. Flip `NEXT_PUBLIC_MARKETPLACE_ACTIVATE_ENABLED=true` in Vercel prod env
6. Redeploy
7. Manual QA on a real advertiser account before announcing

## Known constraints when building packs

- Demand tags require a **DealList** inventory group. If a pack references DomainList or AppBundleList, SS rejects the payload. Default DealList group is `271`.
- Frequency cap payload must use `frequency_cap_value` (SS native field), not `cap`. Anything using `cap` silently no-ops.
- SS `/deals` API isn't exposed publicly — `POST/GET /deals` returns 404. PR #122 auto-deal-list is inert in prod. Real endpoint has to be captured via Chrome DevTools before we can auto-provision new deals from the marketplace. Until then, deals used in packs are pre-seeded manually.

## When a marketplace pack activates

Flow (from advertiser POV):
1. Advertiser browses `/ss-marketplace`
2. Selects a pack (curated bundle of dealIDs + rate + config)
3. Clicks Activate
4. DSP creates a campaign in Neon
5. DSP pushes to SS: campaign + demand tag + inventory group + freq caps
6. Advertiser sees campaign live in their DSP dashboard

If any step fails, the whole activation should roll back cleanly. Check `pushTargetingToClearline` and adjacent code for the current transaction shape.

## Common failure modes

- Frequency cap silently no-ops → check payload field names
- Deal not attached → dealID missing or wrong inventory group type
- Rate wrong in SS → check rate source; must be media-cost CPM, not gross
- Agency name leaks into tag name → hard rule violation, see security doc

## Who to loop in

- Marketplace UI/flow issues → engineering
- Pack curation (which deals, which rates) → Priyesh
- Advertiser-facing packaging (naming, description) → Priyesh


---

# LL Playbook

LL is one of the two supply platforms PGAM runs. Target: **$10K/day**. Data does **not** live in Neon — LL has its own backend.

## What LL is

LL is a supply-side platform that connects PGAM publishers to demand partners. We manage floors, allowlists, and demand connections via LL's UI and API. Our automation (in `pgam-intelligence/scheduler.py` + `core/`) reads state and writes changes on a schedule.

## Mental model — floors and freezes

**Floors** are the minimum CPM we accept from a given demand partner on a given supply path. Floors are the main lever: too low and margin evaporates; too high and demand walks.

**Freezes** are choke-points in `core/partner_freeze.py` that prevent automated writes to specific partners. Freezes exist because a partner has broken trust or we're in the middle of a compliance investigation. **Do not remove a freeze without Priyesh sign-off.**

Current LL freezes to know:
- **Unruly (dp=5)** — all automated writes off. Compliance root-cause pending. See security doc.

## Non-negotiable partner rules

- **9 Dots (demands 692 / 693 / 955)** — contract floor **$1.70 minimum**. Floors may go higher, never lower. Contractual.
- **BidMachine (dp=40)** — QPS cap is partner-mandated. 99% utilization is expected, not opportunity.
- **Cas.ai / Nimpha LTD** — dual-role partner (supply and demand). Billing entity is Nimpha LTD (Cyprus), QBO customer 155, Net 60. Handle their supply and demand relationships as separate books.

## LL UI access

Sagar (and possibly others) has LL UI access. This means **manual UI changes bypass our ledger**. If you see unexplained state drift that our ledger didn't produce, ask the team before assuming an automation bug.

## The reporting dashboard

- **Partner Revenue Dashboard** — lives at `admin.pgammedia.com`, unified LL + TB view. Domo replacement. Started 2026-04-28.
- LL data source is LL's backend, not Neon
- TB data source is Neon (`pgam_direct` schema)

## Scheduler jobs that touch LL

Located in `pgam-intelligence/scheduler.py` + `core/`:

- Floor optimization (dry-run by default; ships changes on approve)
- Ledger writes
- Partner health checks
- Reconciliation nudges

Never run a scheduler job that writes to LL from a local shell without knowing exactly what it does. Read the job first.

## Common tasks

- **Check a partner's current floor:** query LL API via our wrappers in `core/ll_client.py` (verify path)
- **Adjust a floor:** normally scheduler decides; manual overrides go through `core/` helper functions with `--dry-run` first
- **Investigate a partner outage:** check LL UI first, then our ledger, then compare
- **Add a new demand connection:** LL UI + confirm freeze list doesn't already block them

## Escalation

- **Partner-side outage / relationship issue** → Priyesh
- **Contract terms question** (floors, minimums, exclusivity) → Priyesh, do not guess
- **Automation writing wrong values** → stop the scheduler job, check `core/partner_freeze.py`, ask before restarting


---

# TB Playbook

TB is the second supply platform, running in parallel to LL. Target: **$15K/day**. TB data **is** in Neon (`pgam_direct` schema on the shared `round-frog-99233431` project). LL data is not.

## What TB is

TB is a supply-side platform. It's active, integrated with pgam-intelligence, and has its own scheduler jobs and monitoring. Ported into pgam-intelligence alongside LL.

## Active scheduler jobs (in `scheduler.py`)

- **`tb_floor_nudge`** — adjusts floors up/down based on fill and CPM signals. Dry-run mode default.
- **`tb_contract_floor_sentry`** — guards contractual floor minimums. Alerts if a floor drops below contract. Dry-run mode default.

Both jobs are currently dry-run. Do not flip live-write on without Priyesh sign-off + a live/dry diff run.

## TB 401 errors are real signal

If you see `401 Unauthorized` errors in TB job logs, it's **not noise** — it means auth broke and writes are silently failing. Investigate immediately: token rotated, IP allowlist changed, or API endpoint moved. Do not filter or suppress.

## Non-negotiable partner rules (TB side)

- **Unruly (dp=5)** — same freeze as LL. `core/partner_freeze.py` covers both platforms.
- **9 Dots contract floor $1.70 minimum** — applies to TB too if 9 Dots demands run through TB.
- **BidMachine (dp=40) QPS cap** — same rule.

## Where TB data lives

- Postgres: `round-frog-99233431` Neon project, `pgam_direct` schema
- Bloat watch: `ss_vast_events` table has tripped SSP probes before (2026-04-29). Upgraded to Neon Launch tier at that point to survive it. Monitor row counts.

## Partner Revenue Dashboard

TB revenue rolls up in `admin.pgammedia.com` alongside LL. Data source is Neon `pgam_direct` for TB.

## Common tasks

- **Read TB state:** query `pgam_direct.*` in Neon
- **Check a floor nudge decision:** grep scheduler logs for `tb_floor_nudge` — job logs what it would have done in dry-run mode
- **Investigate a fill drop:** check TB UI + Neon `ss_*` tables + freeze status
- **Onboard a new demand connection:** TB UI + confirm not on the freeze list + add contract floor to `core/contract_floors.py` (verify path)

## Cas.ai / Nimpha LTD (relevant to TB too)

Dual-role partner. Both supply and demand routes. QBO customer 155, Net 60, Cyprus billing entity. Handle supply/demand as two separate books.

## Escalation

- TB 401 spam → engineering, immediately
- Partner floor dispute → Priyesh
- Contract terms → Priyesh
- Neon bloat → engineering (has happened before, prepare to scale up or archive)


---

# Engineering Playbook

For engineers (Claude sessions + humans). Covers the repo landscape, environment setup, common footguns, and deploy discipline.

## Repo map

| Repo | Path | What it is | Deploy target |
|---|---|---|---|
| `pgam-direct` | `~/Desktop/pgam-direct` | SSP product + admin.pgammedia.com Next.js app | Vercel |
| `pgam-intelligence` | `~/Desktop/pgam-intelligence` | LL/TB optimization agent, scheduler, cross-cutting ops | Local + cron on Priyesh's machine |
| `pgam-dsp-dashboard` | `~/Desktop/pgam-dsp-dashboard` | DSP UI + backend | Vercel |
| `destination-com` | `~/Desktop/destination-com` | Travel platform, Next.js 16 | Vercel |
| `destination-app` | `~/Desktop/destination-app` | Native iOS+Android, Expo + Clerk | Expo EAS |
| `boxingnews` | `~/Desktop/boxingnews` | Content site + Sanity CMS + MSN pipeline | Vercel |
| `healthnation-web` | `~/Desktop/healthnation-web` | AI-only content site | Vercel |
| `visage` | `~/Desktop/visage` | Celebrity recognition product | (verify) |
| `pgam-recon` | `~/Desktop/finance_CC/pgam-recon` | Finance / SSP reconciliation | Local |
| `pgam-wealth-agent` | `~/Desktop/pgam-wealth-agent` | Personal wealth agent (not company) | Local |

## Neon layout

**One Neon project, two stacks:** `round-frog-99233431` on Launch tier.
- DSP → `public` schema
- SSP → `pgam_direct` schema
- Never cross-query without knowing which schema you're in.

**Static env vars:** DSP `NEON_*` env vars are disconnected from the Neon integration (2026-04-23) to unblock preview builds. Rotate them manually.

**HealthNation** has its own dedicated Neon project: `ep-still-pine-aqbb3g84`.

## Environment quirks

- **Node** v24
- **Python** 3.12 and 3.14 side-by-side
- **npm cache** permissions broken → use `/tmp` cache path
- **Homebrew** installed with Postgres + Redis
- **gh CLI** installed 2026-04-18 (may need auth on fresh machines)

## Worktrees on pgam-dsp-dashboard (READ THIS)

Since 2026-07-10, `.env*` files are gitignored on `pgam-dsp-dashboard`. Every new worktree requires:

1. Symlink `node_modules` from the main tree
2. Symlink `.env.local` from the main tree

**If you skip step 2, `next build` in the pre-push hook fails.**

Also — **NEVER `git add -A` in a worktree here.** Inspect `git status --short` first. Untracked `.env*` in DSP worktrees leaked prod secrets to main on 2026-07-02. Always add files by name.

## Pre-push hook gotchas (DSP)

- Hook runs `next build` — this races with a `next build` in another window. Serialize builds.
- Stale `.git/rebase-merge/` directories from Finder duplication cause a false "rebasing" status. Delete the directory.
- Multi-session edit drift — always `git status` before writing, especially in worktrees.

## Vercel deploy discipline

- Every prod push should be tested in a preview build first
- Preview URL for DSP requires manual Vercel env var setup (see Neon static var note above)
- If a deploy fails with `Unexpected token '<' DOCTYPE` on client API calls, `NEXT_PUBLIC_API_URL` is missing `/api/v1` suffix

## Secret handling

- All prod secrets are managed in Vercel env or the appropriate CI secret store
- `.env` files stay local, never committed
- On offboarding, every credential the person touched rotates
- **Never `--no-verify`** to skip commit hooks unless Priyesh explicitly says so

## Committing / pushing

- Commit and push after every edit — default behavior, don't wait for approval
- Never `git reset --hard` or `push --force` to main without explicit approval
- Never delete branches without approval; investigate unfamiliar branches first

## Monday.com (task tracking)

- CLI: `~/Desktop/pgam-intelligence/scripts/monday_cli.py`
- Auth: `MONDAY_API_TOKEN` in `~/Desktop/pgam-intelligence/.env`
- Close a ticket after shipping: `python3 scripts/monday_cli.py close <item_id>`
- Default board: DSP Dev Work (18406313526)

## Analytics / observability

- **GA4 digest** — daily via GitHub Actions WIF for Destination + BoxingNews. No JSON keys (org policy blocks).
- **GSC API** — callable via `npx tsx scripts/gsc.ts ...` in boxingnews repo. SA `analytics-digest@pgam-analytics` reused from GA4 digest.
- **Partner Revenue Dashboard** — `admin.pgammedia.com`, LL+TB unified

## When you get stuck

1. Check `~/.claude/projects/-Users-priyeshpatel-Desktop-pgam-intelligence/memory/MEMORY.md` for a relevant memory
2. Check `git log` on the file to see when it last changed and why
3. Check Monday for related tickets
4. Ask Priyesh — do not guess on P&L or partner-touching decisions


---

# Scribe Shotlist

Ordered list of screen recordings to make in Scribe. Each entry lists: what to record, start point, end point, who watches this in onboarding.

**How to work through this:** open the target tool, click Scribe extension → Record. Do the flow start-to-end at normal speed. Scribe auto-generates the step-by-step. Rename the Scribe to match the entry title, drop the shareable link into the relevant playbook `.md` next to the topic it documents.

## Priority 1 — the fire-risk SOPs (do these first)

- [ ] **DSP: Wizard → SS campaign build (full flow)**
  Start: DSP dashboard home. End: SS shows demand tag live with correct rate + inventory group.
  Audience: every engineer + trafficker

- [ ] **DSP: QA a wizard payload against the field-mapping drops**
  Start: SS demand tag detail page. End: verified frequency cap uses `frequency_cap_value`, inventory group is DealList, rate is media-cost not gross.
  Audience: trafficker

- [ ] **Monday: close a ticket via CLI + via UI**
  Start: terminal + Monday web tab. End: item in Done column.
  Audience: everyone

- [ ] **Freeze list check before writing to a partner**
  Start: open `core/partner_freeze.py`. End: know how to add a partner to the freeze list and how to verify current freezes.
  Audience: engineer, ops

## Priority 2 — supply platform ops

- [ ] **LL: check current floor for a demand partner**
  Start: LL UI. End: floor value known + noted.

- [ ] **LL: verify an automation change against the ledger**
  Start: scheduler log. End: ledger row + LL UI state cross-checked.

- [ ] **TB: read live state from Neon `pgam_direct`**
  Start: Neon console. End: sample query result showing current TB state.

- [ ] **TB: diagnose a 401 error in job logs**
  Start: scheduler log with 401. End: identified cause (token / IP / endpoint) + fix path.

- [ ] **Partner Revenue Dashboard: walk through daily LL + TB check-in**
  Start: `admin.pgammedia.com`. End: daily numbers vs $10K/$15K targets known.

## Priority 3 — DSP demand ops

- [ ] **DSP: `/ss-marketplace` activation flow (demo env)**
  Start: `demo.dsp.pgammedia.com/ss-marketplace`. End: campaign activated + verified in fixtures.

- [ ] **DSP: enable marketplace prod (flag flip checklist)**
  Start: Vercel env. End: `NEXT_PUBLIC_MARKETPLACE_ACTIVATE_ENABLED=true`, smoke test done.

- [ ] **DSP: buyer agent — read a suggestion and apply/reject**
  Start: buyer agent view. End: lever applied, verified in SS.

- [ ] **DSP: loosen freq cap for an under-pacing campaign**
  Start: campaign underdelivering. End: freq cap raised (e.g., 3/1 → 10/1), pacing improved.

- [ ] **DSP: check a campaign for agency-name / gross-rate leakage in SS**
  Start: SS campaign detail. End: verified clean.

- [ ] **DSP demo env: hand-off to a sales prospect**
  Start: demo URL. End: prospect walked through the UI with fixtures, no live data exposed.

## Priority 4 — engineering ops

- [ ] **Create a new worktree on pgam-dsp-dashboard (with env symlinks)**
  Start: main tree. End: worktree with `node_modules` + `.env.local` symlinks, `next build` passes.

- [ ] **Vercel: roll back a bad deploy on DSP**
  Start: Vercel deployments page. End: previous good deploy promoted.

- [ ] **Vercel: deploy a preview and verify `NEXT_PUBLIC_API_URL` is set correctly**
  Start: PR opened. End: preview URL loads without DOCTYPE errors.

- [ ] **Neon: safe read-only query on prod schema**
  Start: Neon console. End: query returned, verified read-only role used.

## Priority 5 — content ops

- [ ] **BoxingNews: Sanity Studio publish flow**
  Start: Sanity Studio. End: article live on boxingnews.com.

- [ ] **BoxingNews: MSN session recovery**
  Start: 401 error on MSN job. End: session bootstrapped via `msn-bootstrap.yml` workflow_dispatch + confirmed live.

- [ ] **HubSpot: enroll a contact in a cold outbound sequence**
  Start: HubSpot deal. End: contact enrolled in Instantly via handoff.

- [ ] **HubSpot: work the `pgam_outbound_*` pipeline stages**
  Start: pipeline view. End: know each stage's meaning + who moves cards.

## Priority 6 — finance / partner mgmt

- [ ] **QBO: create a partner payout invoice**
  Start: QBO dashboard. End: invoice sent.

- [ ] **Managed Stripe invoicing (`/admin/invoicing/managed`)**
  Start: admin panel. End: client-billable Stripe invoice sent.

- [ ] **Invoca: intake a new advertiser's per-partner API key**
  Start: advertiser onboarding form. End: API key stored, first reading pull successful.

- [ ] **pgam-recon: run monthly SSP reconciliation**
  Start: recon repo. End: month closed, discrepancies flagged.

## Priority 7 — first-day setup (for new hire on their own laptop)

- [ ] **Get 1Password + PGAM vaults access**
- [ ] **Clone the repos you need (`pgam-intelligence`, and role-specific)**
- [ ] **Set up Node v24, Python 3.12, Homebrew**
- [ ] **Configure git with your name + email; add SSH key to GitHub**
- [ ] **Get Vercel org invite accepted**
- [ ] **Get Monday.com invite + set up API token if applicable**
- [ ] **Get SpringServe UI access if applicable (trafficker role)**
- [ ] **Get Neon read-only access (engineer role)**
- [ ] **Get Slack invite + join relevant channels**

---

## Scribe hygiene rules

- Record at normal speed, one flow per Scribe.
- Never record with a gross CPM or agency name visible on screen — pause, blank the field, resume.
- Never record with real advertiser PII visible — use the demo env or fixtures.
- Prefer the demo env (`demo.dsp.pgammedia.com`) for anything that would otherwise show real client data.
- Title = the entry title in this file, verbatim, so linking is easy.


---

# PGAM Onboarding

A new hire's first two weeks. Each item is a checkbox — mark it, move on. Role-specific tracks branch after week 1.

## Before day 1 (Priyesh sets up)

- [ ] 1Password vault access + shared vaults granted
- [ ] Slack invite sent
- [ ] Monday.com seat + board access
- [ ] GitHub org invite
- [ ] Trainual seat (once provisioned)
- [ ] Role-specific tool invites (see role tracks below)
- [ ] Laptop provisioned OR BYOD onboarding plan agreed
- [ ] First-week 1:1 scheduled with Priyesh

## Day 1 — orientation and non-negotiables

**Goal by end of day: know what PGAM does, know what not to break.**

- [ ] Read `training/00-company.md` — the two-stack rule, product map, commercial models
- [ ] Read `training/01-security-nonnegotiables.md` — every rule on this list matters
- [ ] 30-minute walkthrough with Priyesh: PGAM commercial model + the current top 3 priorities
- [ ] Get access to all your tools (finish the checklist above)
- [ ] Post a quick intro in the team Slack channel

**Do not touch prod anything on day 1.** Read, ask, take notes.

## Day 2–3 — get oriented in your area

- [ ] Read the playbook that matches your role (see role tracks below)
- [ ] Watch every Priority 1 Scribe recording in `training/99-scribe-shotlist.md`
- [ ] Do at least one Scribe-guided flow in the demo/dev environment for your role
- [ ] Set up your local dev environment if you're an engineer (see `06-engineering-playbook.md`)

## End of week 1 — shadow

- [ ] Sit through a real ops session — a real campaign build, a real floor review, a real partner escalation — with the owner explaining as they go
- [ ] Watch Priority 2 Scribes for your role
- [ ] Ask every question you're saving up in your notes; get it down to a short residual list before the 1:1

**Friday 1:1 with Priyesh — 30 min.** Bring your residual questions.

## Week 2 — do the work with a review gate

- [ ] Take ownership of a small, low-blast-radius task (Priyesh will assign)
- [ ] Every action goes through review before it hits prod / a partner
- [ ] Read `training/06-engineering-playbook.md` if you haven't — even non-engineers benefit from the repo map
- [ ] Watch Priority 3+ Scribes as tasks require them

**End of week 2:** you should be able to run one recurring task in your area independently, with checks (not approvals) from the owner.

---

## Role tracks

### Trafficker (DSP / SS ops)

**Playbooks:** 00, 01, 02, 03
**Scribes:** all Priority 1 + all Priority 3 + Priority 6
**Weekly ritual:** Monday morning — pacing check on all live campaigns; end of week — margin review
**Cannot touch without approval:** freeze list, campaign rates, agency-name-adjacent fields in SS

### Supply ops (LL / TB)

**Playbooks:** 00, 01, 04, 05
**Scribes:** all Priority 1 + all Priority 2 + Priority 6
**Weekly ritual:** daily target check ($10K/$15K), partner health scan, freeze list sanity check
**Cannot touch without approval:** contract floors (9 Dots $1.70+), freeze removals, BidMachine QPS

### Engineer

**Playbooks:** 00, 01, 06 + whichever product playbook (02 for DSP, 03 for marketplace, 04/05 for supply)
**Scribes:** all Priority 1 + Priority 4 + product-specific
**Weekly ritual:** PR review, Vercel deploy health, Neon bloat check
**Cannot touch without approval:** `main` force pushes, secret rotation, freeze removal, CODEOWNERS on P&L paths

### Content / MSN

**Playbooks:** 00, 01
**Scribes:** all Priority 1 + Priority 5
**Weekly ritual:** content queue review, MSN feed health, session bootstrap if needed
**Cannot touch without approval:** any account-level content platform settings, cross-property CMS changes

### Finance / Recon

**Playbooks:** 00, 01
**Scribes:** all Priority 1 + Priority 6
**Weekly ritual:** partner payout schedule, invoicing queue, month-close prep
**Cannot touch without approval:** commercial terms, partner billing entity changes, credit terms

---

## When to escalate to Priyesh (not later — immediately)

- Anything touching a partner relationship or partner-facing surface
- Any P&L question, contract term, or rate decision
- Any secret rotation or access grant/revoke
- Any freeze list change
- Any deploy to `main` that involves finance/margin/agency code paths
- Any unfamiliar file, branch, or config you're about to delete or overwrite
- Any incident (WP hack, DNS incident, credential leak, partner outage)

## When you don't need to escalate

- Reading anything
- Editing training docs and other markdown
- Running dry-run scheduler jobs
- Working in the demo env
- Local dev, tests, preview builds
- Filing your own tickets in Monday

