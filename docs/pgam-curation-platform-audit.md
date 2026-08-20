# PGAM Curation / Deal Desk — Product, UX, Architecture & Integration Audit

**Date:** 2026-08-19
**Scope:** `pgam-intelligence`, `pgam-dsp-dashboard`, `pgam-direct`
**Status:** Audit + architecture proposal. **No product code changed.**
**Author:** Claude Code session (`claude/pgam-curation-platform-audit-ru2tb0`)

---

## 0. Executive summary — read this first

### 0.1 The headline

**The buyer-facing platform you want is roughly 70% built. Curation-side deal
*reading* already works on both SSPs. Deal *writing* — creating a deal and
getting a Deal ID back — has no path on either, supported or otherwise.**

> **Superseded in part.** §0.3, §7 and §8 were written before the reporting
> repos (`pgam-recon`, `pgam-direct/jobs/report-fetchers`) were audited.
> **Addendum B corrects them** — PGAM has more curation-side access than those
> sections credit, and the shape of that access changes the vendor ask.
> Read Addendum B before acting on §7 or §8.

Almost every non-curation subsystem the brief asks for already exists in
`pgam-dsp-dashboard`: agency/advertiser accounts, Clerk auth, per-feature RBAC,
a real multi-tenant white-label foundation, a configurable fee/margin waterfall,
a forecasting engine, a Claude-based AI media planner with vertical playbooks,
audience segment infrastructure with k-anonymity gates, multi-SSP-keyed reporting
rollups, a campaign wizard, an admin surface, and a configurable approval engine.
That is the expensive half of a curation product and it is done.

What does **not** exist — in either repo, for either SSP — is the one thing the
product is named after: **the ability to create a deal at an SSP through an API
and get a Deal ID back.** Today a human does it by hand.

### 0.2 The evidence, stated plainly

`pgam-dsp-dashboard/docs/springserve-quirks.md:338`:

> Deal creation is **manual** in the ClearLine console (~2 min for BG).
> The platform never creates deals — it only attaches them.

`pgam-dsp-dashboard/src/app/admin/deal-sync/page.tsx:1-20` documents the live
workflow as of today:

> Workflow (until Magnite-side auto-grant lands):
> 1. Magnite ops grants PGAM access to a new deal (manual relationship).
> 2. The deal lands in a SpringServe deal_list (Magnite-side sync).
> 3. Ops opens THIS page, sees the deal_id appear under the right list, hits Copy.
> 4. Pastes into `src/lib/clearline-packs.ts` for the matching pack.
> 5. Re-deploy → marketplace flip eligible.

So the end-goal sentence —

> "You don't need to email PGAM every time you need a Deal ID."

— currently describes a workflow in which **PGAM is the email step, and the step
after it is a source-code edit and a redeploy.** That is the gap to close, and
closing it is mostly a commercial/entitlement exercise, not a coding one.

### 0.3 A correction to the brief's premise

The brief says *"We already have relationships/integrations and curation
capabilities with these platforms."* That is half right, and the halves matter:

| | Relationship | API integration | Curation (deal-create) capability |
|---|---|---|---|
| **Magnite** | Yes — a demand/DSP seat (SpringServe **2724**) **plus** working reporting credentials on `api.magnite.com` (OAuth2 client-credentials) and UI-token access to DV+ Performance Analytics | Yes, deep — but see **Addendum B**: the official DV+ analytics REST API is **proven account-blocked**, and what works is a Playwright-scraped UI session token | **No write path.** Deals are created by hand in the ClearLine console. |
| **PubMatic** | **Yes — two curation-side seats with live revenue.** Activate seat `PGAM_Activate_US` (org **17496**) and PMP buyer account **69397** running real deals at a **25.5% contracted take rate** | Real but unsupported. Deal-level **read works today** via `apps.pubmatic.com/api/pmp/deals/reportingSearch`; the official OAuth buyer analytics API returns **zero rows** for our user | **No write path.** No create/POST function exists in any client. |

The important nuance: **PGAM's Magnite integration is buy-side; curation is a
sell-side/curator function.** SpringServe is Magnite's ad server and PGAM talks
to it as a *demand partner*. A curation seat is a different product on a
different API surface. Advanced Curation operates on the sell side — that is
precisely why they can mint Deal IDs in five minutes and PGAM cannot.

PubMatic is the opposite and better story: PGAM **already holds the right kind of
seat**, and it is sitting in `pgam-intelligence` unwired to any product surface.

### 0.4 What this means for sequencing

The brief's Phase 1 is:

> Agency login · Deal creation · CTV/OLV/Display/In-App · Inventory selection ·
> Geography · Magnite · PubMatic · DSP/seat selection · Deal creation ·
> Deal ID retrieval · Deal Library · PGAM Admin

Everything in that list is buildable in weeks **except "Deal creation" and "Deal
ID retrieval" — the two items the phase exists for.** Those depend on API access
PGAM does not currently hold.

**I recommend re-cutting the phases so that engineering is never blocked on a
vendor, and the product ships value before the entitlements land.** Concretely:
build the abstraction layer plus a **Deal Request pipeline** first — same buyer
UX, same database, same Deal Library, but with a human-in-the-loop fulfilment
step that PGAM ops already performs. Every deal flows through the identical
normalized objects and adapter interface, so when an SSP write API is granted you
swap one adapter method from `manual` to `api` and the entire product becomes
instant with **no UX change and no schema migration**. Detail in §17 and §20.

This is not a downgrade of ambition. It is the only sequence in which the
five-minute promise is ever safe to make to an agency, because it lets you
measure and shrink real fulfilment latency instead of discovering on launch day
that a deal takes two hours because someone has to be at a keyboard.

### 0.5 Honest limits of this audit

Per the brief's instruction not to assume API capability:

- **Everything asserted about PGAM's own code and docs is verified by reading
  it**, and cited by file and line.
- **Nothing is live-verified against Magnite or PubMatic.** This session runs in
  an ephemeral cloud container with a fresh clone and **zero project credentials
  injected** (`env` confirms: no `PUBMATIC_*`, `MAGNITE_*`, `SPRINGSERVE_*`,
  `POSTGRES_URL`). I could not authenticate to any vendor.
- Claims about Magnite's and PubMatic's *published* API surfaces come from vendor
  documentation and search results, and `help.magnite.com`, `pubmatic.com`
  community docs and `advancedcuration.com` are all **blocked by this
  environment's network egress proxy**. Those claims are flagged
  **`[VENDOR-DOC, UNVERIFIED]`** and every one has a named probe in §19.6.

Treat §19.6 as the first work item. Three curl commands settle the entire
roadmap, and until they run, any date attached to Phase 1 deal creation is a
guess.

---

## 1. Deliverable 1 — Current PGAM architecture relevant to curation

### 1.1 Repo map (corrected)

`training/06-engineering-playbook.md` in this repo says `pgam-intelligence` is
"LL/TB optimization agent, scheduler, cross-cutting ops" deployed "Local + cron".
That is stale on two counts: it deploys as a **Render worker** (per `CLAUDE.md`,
`render.yaml`, `Procfile`), and it now contains the **only PubMatic curation-seat
client in the company**. Worth fixing, because that omission is why the curation
seat is invisible to anyone reading the playbook.

| Repo | Role for curation | State |
|---|---|---|
| `pgam-dsp-dashboard` | **The product home.** Next.js 14 App Router, Vercel (`dsp.pgammedia.com`). 269 API routes, 79 pages, 124 migrations, 47 test files. All buyer-facing surfaces, RBAC, tenancy, margin, forecasting, AI, reporting. | Production |
| `pgam-intelligence` | **Holds the PubMatic Activate curator client** (`core/pubmatic_activate.py`, 435 lines). Also LL/TB optimization, compliance, schain/sellers.json validation. | Production (Render worker) |
| `pgam-direct` | Self-hosted SSP (Go + Next.js). **Has a real `pgam_direct.deals` table and PGAM-minted deal IDs.** Strategically the long-game answer to the entitlement problem — see §19.5. | **Phase 1 scaffold; "not yet runnable end-to-end"** (`README.md`) |

### 1.2 The two supply paths as they actually work

```
                    ┌──────────────────────────────┐
                    │  pgam-dsp-dashboard (Next)   │
                    │  buyer + operator surfaces   │
                    └──────────────┬───────────────┘
                                   │
              ┌────────────────────┼────────────────────┐
              ▼                    ▼                    ▼
      ┌───────────────┐   ┌────────────────┐   ┌────────────────┐
      │ Neon Postgres │   │  SpringServe   │   │ Vibe (CTV DSP) │
      │ source of     │   │  /api/v0       │   │ api.vibe.co    │
      │ truth         │   │  READ + WRITE  │   │ OAuth, writes  │
      └───────────────┘   └────────┬───────┘   │ ENABLED        │
                                   │           └────────────────┘
                    ┌──────────────┴──────────────┐
                    ▼                             ▼
        ┌───────────────────────┐    ┌────────────────────────┐
        │  Magnite ClearLine    │    │  PubMatic Activate     │
        │  demand-side seat     │    │  DSP-repo client:      │
        │  acct 2724            │    │  INERT (AUTHZ_FAILED)  │
        │                       │    │                        │
        │  deal CREATE: MANUAL  │    │  deal CREATE: absent   │
        │  (console, by hand)   │    │                        │
        └───────────────────────┘    └────────────────────────┘

        ┌─────────────────────────────────────────────────────┐
        │  pgam-intelligence/core/pubmatic_activate.py        │
        │  PubMatic CURATION seat PGAM_Activate_US (17496)    │
        │  1 confirmed GET · rest inferred · no writes        │
        │  ZERO callers — not wired to any product surface    │
        └─────────────────────────────────────────────────────┘
```

The bottom box is the most important thing in this audit. It is the only
curation-seat credential PGAM owns, and nothing uses it.

### 1.3 How a Deal ID reaches a buyer today

Reconstructed from `src/app/api/v1/self-serve/advertisers/[id]/deal-id/route.ts`,
`src/lib/self-serve/activation-gate.ts`, `src/app/admin/deal-ids/page.tsx`,
`src/app/admin/deal-sync/page.tsx`:

1. A human at Magnite grants PGAM access to a deal. Out-of-band relationship.
2. The deal appears in a SpringServe `deal_list` (Magnite-side sync).
3. A PGAM operator opens `/admin/deal-sync`, finds it, clicks **Copy**.
4. For an advertiser: operator pastes it into `/admin/deal-ids`. `PUT` provisions
   a per-advertiser SpringServe `deal_id_list`, `bulk_create`s the string deal
   onto it, and writes `ss_advertisers.magnite_deal_id` +
   `magnite_deal_id_list_id`.
5. That column **is** the activation gate: `activation-gate.ts:9` —
   *"Account activated: `ss_advertisers.magnite_deal_id IS NOT NULL`."*
6. For a marketplace pack: operator pastes it into `src/lib/clearline-packs.ts`
   and **redeploys**.

Steps 1, 3, 4 and 6 are all manual. Step 6 requires an engineer.

### 1.4 The deal data model that exists

| Table | Migration | What it holds | Curation fit |
|---|---|---|---|
| `campaign_deal_lists` | `0064` | `springserve_campaign_id` PK → `springserve_deal_list_id` + `deal_ids TEXT[]` | Campaign→deal mapping only. No deal entity, no owner, no status, no lifecycle. |
| `ss_advertisers.magnite_deal_id` / `.magnite_deal_id_list_id` | `0120` | One deal string per advertiser | 1:1 advertiser→deal. Cannot express many deals per advertiser. |
| `ss_campaigns.magnite_deal_id` | `0120` | Per-campaign deal binding | Same shape limit. |
| `keystone_reporting_rollups` | `0016` | `(vendor, campaign_id, publisher_id, dimension_date)` + impressions, spend, payout, `working_media_pct`, attention overlay | **Already vendor-keyed** (`CHECK (vendor IN ('clearline','activate'))`). Genuinely multi-SSP ready. |
| `pgam_direct.deals` | `pgam-direct/000023` | Real deal entity: `deal_id`, `floor_cpm_usd`, `deal_type`, `media_types[]`, `device_types[]`, `geo_countries[]`, `min_attention`, `allowed_dsp_ids[]`, `tenant_id` | **This is the right shape.** In the wrong (pre-production) repo. See §9. |

**There is no first-class `deal` entity in the DSP database.** Deals exist as
string columns hanging off advertisers and campaigns. Every requirement in the
brief's §7 (Deal Library) and §5 (multi-SSP one-strategy-many-deals) needs this
built. Good news: `pgam-direct/000023_dsp_deal_subscriptions.up.sql` already
contains a well-designed deal schema to copy, including the ops-approved ∪
buyer-subscribed allowlist union pattern that maps neatly onto agency
self-service.

---

## 2. Deliverable 2 — Existing functionality we can reuse

Classification key: **[AE]** Already Exists · **[EE]** Extend Existing ·
**[BN]** Build New · **[REQ-API]** Requires External API

| Brief capability | What exists today | Evidence | Class |
|---|---|---|---|
| **Authentication** | Clerk sessions + legacy `ss_token` cookie fallback; Clerk satellite domains for white-label hosts | `src/lib/self-serve-auth-server.ts`, `src/lib/tenant/clerk-satellite.ts`, `SCHEMA.md` §Auth | **[AE]** |
| **Agency accounts** | `agencies` + `agency_advertiser_hierarchy` + `campaign_agency_overrides`; agency-scoped portal tokens | migrations `0060`, `0061`, `0071`, `0084` | **[AE]** |
| **Advertiser accounts** | `ss_advertisers` (self-serve), `dsp_advertisers` (SS mirror), `advertisers` (CAPI). Three tables, two ID forms — a known seam (`D1`) | `SCHEMA.md` §"Three advertiser tables" | **[EE]** — consolidate before curation multiplies the confusion |
| **Multi-tenancy / white-label** | **Full foundation shipped.** `tenants`, `partner_branding`, `tenant_settings` with `allowedChannels`, `allowedSupplySources`, `platformFeePct`, spend caps, `reportingVisibility` incl. `margin_hidden`, `approvalRule`, `workflowMode`. Tenant roles include `agency_user`, `advertiser_user`, `partner_admin` | migrations `0093`, `0094`, `0095`, `0117`, `0118`; `src/lib/tenant/*` (18 modules); `docs/white-label-dsp-plan.md` | **[AE]** — this is a large, unexpected win. Brief §11 is mostly done |
| **User permissions / RBAC** | Per-feature × per-level (`none`/`view`/`edit`) across `dsp`,`ssp`,`finance`,`pnl`,`team`,`partners`; hard constraints (super-admin allowlist, finance allowlist, P&L owner-only) that cannot be granted at runtime; CODEOWNERS-protected | `src/lib/rbac.ts`, `src/lib/require-role.ts` | **[EE]** — add a `curation` feature + agency-scoped roles |
| **Configurable approval rules** | `none` / `manual` / `auto_below_threshold` with a cents threshold, per tenant; fails closed | `src/lib/tenant/approval.ts` | **[AE]** — brief §12's "trusted agency = instant, new agency = approval" is already implemented |
| **Campaign creation wizard** | Three paths (upload / create-plan / scratch) with targeting, geo, device, daypart, frequency, IAB, creative steps | `src/components/campaign-wizard/*` | **[EE]** — the deal wizard is a sibling, not a rewrite |
| **Media planning** | Rate card (`0087_mediaplan_rate_card`), `/api/v1/mediaplan/rates`, AI plan generator, `ss_ai_plans` (`0101`) | — | **[AE]** |
| **Geography targeting** | Country / state / DMA / ZIP writers, with a known ZIP-validation gap on the Vibe leg | `src/lib/springserve/geo-targeting.ts`, `geo-writer.ts`; open ticket #1 in `docs/platform-state-2026-08.md` | **[AE]** |
| **Inventory selection** | Targeting library — reusable domain / app / IP / ZIP / deal lists; `campaign_domain_lists` (`0123`); `ss_campaigns.inventory_type` (`0124`); SpringServe `domain-lists.ts` | `src/app/targeting`, `src/components/targeting/*` | **[EE]** |
| **CTV / OLV / Display / In-App** | All four supported; CTV deepest (pod position, quartiles, ACR, household frequency, attention v1) | `src/lib/clearline-packs.ts` channel enum; `docs/attention-engine-v1-ctv-spec.md` | **[AE]** |
| **Audiences** | Segment marketplace + activations (`0003`), attention-qualified segments (`0091`), O&O first-party segments + activations (`0135`,`0136`) with k-anonymity floors (1000 households / 500 visitors), ACR conquesting, CRM match, suppression lists | `src/app/api/v1/audiences/*` (8 route groups), `src/app/api/v1/segments/*` | **[EE]** for the catalog — **[REQ-API]** for SSP sync (see §3.4) |
| **Reporting** | `keystone_reporting_rollups` vendor-keyed; `ss_daily_stats` (`0140`); `ss_dimension_rollups` (`0088`); saved views (`0089`); scheduled reports; client portal with honesty gates | migrations as cited; `src/lib/reporting/vibe-merge.ts` | **[EE]** — add `deal_id` dimension |
| **Deal ID functionality** | Consumption only: bind a string, provision a SpringServe list, attach to a tag. `/admin/deal-ids`, `/admin/deal-sync`, `/api/v1/deals`, `/api/v1/inventory/deals` | as cited in §1.3 | **[EE]** for plumbing · **[BN]** for the deal entity · **[REQ-API]** for creation |
| **SSP integrations** | SpringServe deep (20 modules); ClearLine vendor adapter (8 modules, incl. `campaign-writer.ts`); Activate scaffold (4 modules, inert); Vibe (12 modules, **writes live**) | `src/lib/springserve/*`, `src/lib/vendors/*` | **[EE]** |
| **Magnite integration** | Read+write on SpringServe demand objects. **No deal creation. Two write entitlements blocked.** | §7 | **[REQ-API]** |
| **PubMatic integration** | DSP repo: inert. `pgam-intelligence`: real curation seat, 1 confirmed GET, no writes, no callers | §8 | **[REQ-API]** |
| **API infrastructure** | 269 routes, consistent `requireRole` → `enforceTenantOrForbid` → handler pattern; TS contract in `api-types.ts` + stubs in `api-client.ts` | `docs/api-v1-auth-audit.md` | **[AE]** |
| **Webhooks** | Stripe (signature-verified + idempotent), MediaConvert, Invoca (**unverified — seam F1**), Zapier call-events | `SCHEMA.md` §Vendor contract | **[EE]** |
| **Database schemas** | 124 migrations. **But DDL is created in 8 different places with no single runner and duplicate migration numbers** (seams `S1`–`S3`) | `SCHEMA.md` §Schema-creation surfaces | **[EE]** — fix before adding curation tables |
| **Billing** | Stripe wallet (`ss_wallet_accounts`, `ss_wallet_transactions`), postpaid daily billing (`0134`), managed payments (`0126`), QBO/Intuit invoicing | — | **[AE]** |
| **Margin / pricing logic** | **Real waterfall engine.** `computeMargin()` pure function; `fee_config_advertiser` + `fee_config_campaign` overrides; per-SSP platform fee pcts (`MAGNITE_PLATFORM_FEE_PCT=0.10`, `PUBMATIC_PLATFORM_FEE_PCT=0.07`); separate CPA fee profile; `tenant_settings.platformFeePct`; `reportingVisibility: margin_hidden`; supply-path fee schedules | `src/lib/margin.ts`, `src/app/api/v1/margin/route.ts`, `src/lib/keystone/supply-path/fee-schedules.ts` | **[AE]** — brief §13 is substantially solved |
| **Forecasting** | Deterministic heuristic engine (764 lines) + static supply graph (527 lines); `POST /api/v1/forecast`; `/forecasting/new` UI with CSV export; **live Magnite ClearLine forecast client fully coded but POST-blocked** | `src/lib/forecasting/*`, `docs/forecasting.md` | **[AE]** for estimates · **[REQ-API]** for real SSP avails |
| **AI functionality** | Claude Sonnet media planner with 12 vertical playbooks + 6-layer methodology; injects **live segment and deal context** (`SegmentContext`, `DealContext` incl. `floorCpm`, `attentionEstimate`); AI spend rate limiting; buyer-agent action/approval tables (`0098`–`0102`); `ss_ai_plans` | `src/app/api/v1/agent/chat/route.ts`, `playbooks.ts`, `src/lib/ai/models.ts` | **[EE]** — brief §6 is ~70% there; re-point at curation objects |
| **Deal Health / optimization** | Pushback recommendation engine (analyzer, apply, thresholds, gate); campaign health card; anomaly banner; margin watchdog (5-min cron, auto-pauses on margin breach); drift-alert Lambda | `src/lib/keystone/pushback/*`, `src/lib/cpa-watchdog` | **[EE]** — retarget at deals; brief §10's rule list maps almost 1:1 |
| **Attention layer** | Attention engine v1 for CTV: 0–100 per impression, calibrations, placement scoring, household resolution, viewability, quartiles | `docs/attention-engine-v1-ctv-spec.md`, migrations `0056`,`0057`,`0076`,`0085`,`0090` | **[AE]** — **the differentiator** (§4.3) |

### 2.1 How much already exists — the honest number

Counting the brief's 20 deliverables by implementation weight:

| Bucket | Deliverables | Share |
|---|---|---|
| Substantially exists, needs extension | Auth, accounts, RBAC, tenancy/white-label, billing, margin/pricing, reporting substrate, forecasting (estimates), AI, admin, approval rules, targeting, audiences catalog, attention | **~65%** |
| Genuinely new but unblocked | Deal entity + Deal Library + package model, deal wizard, DSP/seat directory, marketplace catalog with real IDs, provider-adapter layer, reliability/status machine | **~25%** |
| Blocked on external API | Deal creation, Deal ID retrieval, live SSP forecast, audience→SSP sync | **~10% — but it is the load-bearing 10%** |

---

## 3. Deliverable 3 — Missing functionality

### 3.1 The deal entity (BN — highest priority, unblocked)

No `deals` table exists in the DSP DB. Missing: deal identity, owner
(tenant/agency/advertiser), status lifecycle, SSP + external deal ID, DSP + seat,
floor, targeting snapshot, audit trail, and the **package** concept that makes
one buyer strategy fan out to several SSP deals (brief §5). Schema proposed in §9.

### 3.2 Deal creation against an SSP (REQ-API — blocked)

Neither adapter has a create path.

- Magnite: `src/lib/vendors/clearline/campaign-writer.ts` writes *campaigns and
  demand tags*, not deals. `src/lib/springserve/deal-lists.ts` creates a
  SpringServe **deal_list** — a container that references deals that must already
  exist. `createDealListForCampaign()` with a ClearLine string deal fails:
  `"Deals has deal id(s) which do not exist: 0"` (SS coerces the string to int 0).
- PubMatic: `src/lib/vendors/activate/` has `activatePost`/`activatePut` helpers
  but no deal function, and `README.md` states the write side is explicitly out of
  scope: *"Mirror `clearline/campaign-writer.ts` as
  `activate/campaign-writer.ts` … Not shipped in this PR — the scaffold stops at
  read-side."* `core/pubmatic_activate.py` has **no POST wrapper at all.**

### 3.3 Provider-adapter abstraction (BN — unblocked, and urgent)

The brief's §14 warning is well-founded but slightly behind the code: the current
coupling is worse than "front end → SSP". It is **front end → SpringServe
semantics**. SpringServe's object model (demand tags, inventory groups,
`DealList` vs `DealIdList`, `domain_white_list` as string-at-tag/bool-at-campaign)
leaks all the way into wizard components and route handlers. `docs/springserve-quirks.md`
is 370 lines of that leakage documented.

Adding PubMatic on top of this without normalizing first will double the
quirk surface. **Build the normalized objects and adapter interface before the
second SSP, not after.** Design in §6.

### 3.4 Audience → SSP sync (REQ-API — blocked, and under-appreciated)

Brief §4 calls audiences "potentially one of PGAM's biggest differentiators."
Correct — but the transport does not exist:

- `src/app/api/v1/audiences/oo/[slug]/activate/route.ts:14-17`: *"The activation
  lands as `status='pending'`. A strategist reviews and backfills the
  `external_id` … the actual push surface (SS audience upload / LiveRamp / etc.)
  is per-advertiser and lives outside this route."* — a **ledger with a manual
  fulfilment step**, exactly like deals.
- `docs/magnite-field-coverage.md` (line-item Targeting tab): *"Audiences — DAA /
  EDAA / Audiences: _none_ at SS tag layer; PGAM-native via campaign-level
  `domain_list_ids`"* — i.e. audiences today reach Magnite disguised as **domain
  lists**. That works for contextual/site-based segments and does **not** work for
  demographic, in-market, purchase-intent or household segments.

So of the eleven audience types in brief §4, the ones PGAM can genuinely attach
to a Magnite deal today are the contextual/domain-shaped ones. The rest need
either a real segment-sync API or a bought identity/onboarding path (LiveRamp,
Audigent, or the SSP's own data marketplace — Magnite Access, PubMatic Connect).
**This is a commercial decision, not a sprint.** It should not sit in Phase 2
unqualified.

### 3.5 Other gaps

| Gap | Class | Note |
|---|---|---|
| DSP + buyer-seat directory | **[BN]** | Brief §7 rightly says extensible, not hardcoded. Small table + admin CRUD. PubMatic claims 125+ DSPs `[VENDOR-DOC, UNVERIFIED]` |
| Deal status machine + idempotency + retry + dead-letter | **[BN]** | Brief §15. Nothing today; deal binding is a fire-and-forget `PUT` that logs and returns 500 |
| Deal-scoped reporting | **[EE]** | Rollups are campaign×publisher×day; need `deal_id` as a first-class dimension |
| Marketplace with real deal IDs | **[EE]** | 12 packs exist; **all deal IDs are placeholders** (`DEAL-CL-SPT-001`…) and the file's own comment says *"(mock — real source is fee_config + Magnite API)"* |
| Inventory discovery from SSP APIs | **[REQ-API]** | Brief §3 asks what can be pulled rather than hand-maintained. `clearline/dimension-ingest.ts` pulls reporting dimensions; `/admin/deal-sync` reads deal lists. Neither is an inventory *catalog*. `featuredPublishers` in packs is hand-typed |
| CI | **[BN]** | `SCHEMA.md`: *"**No CI runs them.** `.github/` does not exist"* for the 47 test files. Shipping a deal-creation engine with no CI is how you mint bad Deal IDs at scale |
| Observability | **[BN]** | Zero Sentry/Datadog/OTel; 238 `console.*` across 269 routes; `metrics.ts` unwired. Brief §15 wants admin alerts and API status |
| Rate limiting | **[EE]** | Exactly one route is limited (`/api/v1/conversions`); seam `A4` |

---

## 4. Deliverable 4 — Advanced Curation competitive analysis

> **Sourcing note.** `advancedcuration.com` is blocked by this environment's
> egress proxy, so I could not walk the product. This is assembled from their
> launch press coverage and vendor material. Claims are theirs, not verified by
> use. **Someone at PGAM should sign up and screenshot the real flow** — it is
> free, and it is the single cheapest research task on this list.

### 4.1 What they are and what they do well

Launched **2026-03-24**. An AI-powered Deal ID creation and management platform
for media buyers at agencies and brands. Positioning:

- **Brief → live deal in under five minutes**, across **CTV, OLV, display and
  in-app**.
- Deal IDs ready to activate across **Magnite, Index Exchange, PubMatic and
  OpenX** — four SSPs at launch.
- **No setup fees, no contracts, no minimum spend**, available in any country.
- **No changes to existing payment processes or DSP relationships** — deliberately
  non-threatening to the agency's incumbent workflow.
- AI-powered deal creation as an alternative to manual parameter entry.

What they got right, and PGAM should copy outright:

1. **They picked the correct wedge.** Deal ID creation is a universally hated
   two-day email thread. Narrow, acute, unambiguous.
2. **Zero-friction commercial model.** No contract, no minimum, free — removes
   every reason for a trader to say "let me check with procurement." Their margin
   is presumably in the curation take rate, invisible to the buyer. That is the
   right shape, and PGAM's fee engine already supports it (§13).
3. **Multi-SSP from day one.** Four SSPs makes them a layer rather than a
   reseller. One SSP would have made them a feature.
4. **Respecting the DSP relationship.** They deliver a Deal ID into the buyer's
   existing DSP and seat. They do not ask the agency to move spend. This is why
   they get adopted.
5. **A speed claim, not a feature list.** "Under five minutes" is a promise a
   trader can evaluate instantly.

### 4.2 What they appear to be missing

Inferred from their positioning; each is a hypothesis to confirm, not a finding:

1. **No measurement or outcome layer.** They create deals. Nothing in their
   material suggests they measure whether the deal *worked* — no attention, no
   completion, no attribution, no lift. A Deal ID is a pipe; buyers are judged on
   what comes out of it.
2. **No proprietary data or audiences.** They appear to curate someone else's
   supply with the buyer's own parameters. No first-party data, no owned
   inventory, no unique segments.
3. **No owned supply.** Pure intermediary. Nothing structurally prevents an SSP
   from shipping the same tool for free — and PubMatic and Magnite both now have
   AI deal curation in their own buyer UIs `[VENDOR-DOC, UNVERIFIED]`, which is
   an existential risk to a pure-play curation front end.
4. **Free is not a moat, it is a runway.** Free + no contract + no minimum means
   no revenue floor and no switching cost. Both cut both ways.
5. **Optimization is unaddressed.** Creating a deal fast is step one. Nothing
   suggests they tell a buyer the floor is too high, scale is too thin, or one
   SSP is beating another.
6. **Likely thin on CTV depth.** Live sports, FAST, pod position, content genre,
   ACR — the parts of CTV curation that need real trading knowledge — are hard to
   deliver from a generic four-SSP abstraction.

### 4.3 What PGAM already has that they structurally cannot copy

This is the strategic core of the whole exercise. PGAM's advantage is **not** that
it can create Deal IDs — Advanced Curation is demonstrably better at that today.
It is everything that happens after the deal starts serving:

| PGAM asset | Why it is defensible | Where it lives |
|---|---|---|
| **Attention measurement at the supply level** | An 0–100 score per CTV impression from 50+ signals, calibrated, household-resolved — not a third-party overlay. A curation competitor cannot bolt this on; it requires impression-level access | `docs/attention-engine-v1-ctv-spec.md`; migrations `0056`,`0057`,`0076`,`0085`,`0090` |
| **Attention-qualified deals** | Deals defined *by measured attention thresholds* rather than by publisher list. `pgam_direct/000023` already seeds `min_attention` 0.75/0.65/0.55 cohorts. **Nobody else can sell this** | `pgam-direct` deals table; `0091_attention_qualified_segments` |
| **First-party O&O data** | destination.com + boxingnews.com behavioural segments with real consent and k-anonymity. Owned, not licensed | `0135`/`0136`, `audience_oo_rules` |
| **A real SSP being built** | `pgam-direct` means PGAM can eventually mint its own Deal IDs with no vendor entitlement at all — a structural escape from the exact blocker in this audit | `pgam-direct` |
| **Attribution + outcome measurement** | Household identity bridge, multi-model attribution, lift studies, holdouts, CPA/call attribution, incrementality | `src/lib/keystone/{attribution,hib,lift,holdout}` |
| **Margin engine + white-label** | Can hand an agency a branded curation desk with margin hidden and per-tenant economics. That is a *platform* sale, not a tool sale | `src/lib/margin.ts`, `src/lib/tenant/*` |
| **Deal Health / pushback loop** | Already-built recommendation engine that pushes optimizations back at supply | `src/lib/keystone/pushback/*` |

### 4.4 The differentiated positioning I recommend

**Do not compete on deal-creation speed.** PGAM will lose that race — Advanced
Curation is live, multi-SSP and free today, and PGAM cannot currently create a
deal by API at all. Racing them means racing to parity on the one axis where
PGAM's assets are irrelevant.

Compete on **the deal that performs**:

> "Anyone can send you a Deal ID. PGAM sends you a Deal ID we can prove worked —
> curated on measured attention, enriched with our own audiences, and optimized
> after it goes live."

Concretely, this means shipping deal creation as **table stakes at parity** (it
must be self-service, it must be fast, it must be multi-SSP) and putting the
product's weight on four things Advanced Curation cannot follow:

1. **Attention-curated deals** — inventory selected by measured attention, not
   publisher reputation. The one genuinely novel product here.
2. **Deal Health** — the deal keeps getting better after creation, and PGAM tells
   the buyer why.
3. **Outcome reporting inside the curation tool** — completion, attention, lift,
   CPA, back into the same UI that made the deal.
4. **White-label** — sell the desk to the agency as their own. Advanced Curation
   is a destination; PGAM can be infrastructure.

That ordering also happens to be the sequence in which PGAM's blockers clear,
which is convenient rather than coincidental: attention, audiences and reporting
need **no** SSP write API.

---

## 5. Deliverable 5 — Proposed user journey

### 5.1 The principle

The brief's `Brief → Curate → Forecast → Create → Activate → Optimize` is right.
One change: **make the fast path the default and the guided path the fallback**,
not the reverse. A programmatic director creating their eleventh NFL deal should
not walk nine steps. Traders abandon wizards.

So: **one screen with progressive disclosure**, not a nine-step funnel.

```
┌────────────────────────────────────────────────────────────────────┐
│  New Deal                                    [Templates ▾] [AI ✨] │
├────────────────────────────────────────────────────────────────────┤
│  WHO      Agency ▾  Advertiser ▾  Brand ▾        Deal name         │
│  WHAT     ○ CTV  ○ OLV  ○ Display  ○ In-App      (multi-select)    │
│  WHERE    Geo picker (country/state/DMA/city/ZIP/saved lists)      │
│  WHO ELSE Audience layers (optional)                               │
│  INVENTORY Publishers / apps / domains / genres / allow+block      │
│  WHEN     Start — End                            Budget            │
│  MONEY    Floor CPM  [PGAM suggests $X]                            │
│  DELIVER  DSP ▾   Seat ▾                                           │
├────────────────────────────────────────────────────────────────────┤
│  ▸ Supply path: Automatic (PGAM chooses)         [Advanced ▾]      │
├────────────────────────────────────────────────────────────────────┤
│  LIVE PANEL (updates as you type)                                  │
│  Est. avail impressions   Reach   Est. CPM   Spend capacity        │
│  ⚠ 2 warnings — floor above market; ZIP list has 3 invalid codes   │
│                                          [ Create Deal ]           │
└────────────────────────────────────────────────────────────────────┘
```

A sophisticated buyer fills six fields and hits Create. A novice opens the
guided drawer per section, or types a brief into the AI box. **Same underlying
object either way** — critical, because it means one validation path, one schema,
one adapter call.

### 5.2 The journey, end to end

| Stage | Buyer sees | System does | Status |
|---|---|---|---|
| **Brief** | Types free text, or picks a template, or fills the form | AI parses to a draft `DealRequest`; templates hydrate it | `draft` |
| **Curate** | Inventory / audience / geo / format pickers with search, counts, recommendations | Validates against catalogs; flags over-restriction | `draft` |
| **Forecast** | Avail impressions, reach, CPM band, spend capacity, publisher + device mix — **labelled `PGAM estimate` or `Magnite forecast`** | Forecast engine; live SSP forecast when entitled | `draft` |
| **Review** | Full summary + warnings, ranked by severity | Pre-flight validation (§15.2) | `validating` |
| **Create** | One click. Then a progress state with an honest ETA | Route to SSP(s); create or enqueue; poll | `submitted` → `creating` |
| **Deal ID** | Deal ID(s) with copy buttons + per-DSP activation instructions | Persist, associate to tenant, notify (in-app + email + Slack) | `active` |
| **Activate** | Step-by-step for their DSP + seat; "mark as activated" | Optional bid-request watch: alert if no requests in 24h | `active` |
| **Optimize** | Deal Health card: pacing, fill, win rate, attention, completion, per-SSP comparison, ranked recommendations with one-click apply | Pushback analyzer retargeted at deals | `active` |

### 5.3 The honesty requirement on speed

While fulfilment is human-in-the-loop (Phase 1 — §17), the UI must **not** imply
instant. The pattern that keeps trust:

- On submit: *"Deal requested. PGAM is building it now — typically within
  {p50_from_real_data}. We'll email you the moment it's live."*
- The number is computed from actual fulfilment times, never hardcoded.
- Buyer can close the tab; notification is push, not poll.
- When the write API lands, the same screen resolves in seconds and the copy
  changes itself.

This is the difference between a product that survives its first week with a real
agency and one that does not. A buyer who is told 90 minutes and gets 40 is
delighted. A buyer promised five minutes who waits 40 never comes back.

---

## 6. Deliverable 6 — Proposed system architecture

### 6.1 Layering

```
┌─────────────────────────────────────────────────────────────────────┐
│ SURFACES                                                            │
│  /curation/*  buyer (agency)   │  /admin/curation/*  PGAM operator  │
│  tenant-branded, margin hidden │  full economics, overrides, retry  │
└───────────────┬─────────────────────────────┬───────────────────────┘
                │                             │
┌───────────────▼─────────────────────────────▼───────────────────────┐
│ PGAM CURATION API   /api/v1/curation/*                              │
│  requireRole('curation.*') → enforceTenantOrForbid → handler        │
│  (identical guard chain as the existing 269 routes)                  │
└───────────────┬─────────────────────────────────────────────────────┘
                │
┌───────────────▼─────────────────────────────────────────────────────┐
│ CURATION ENGINE  (src/lib/curation/)                                │
│                                                                     │
│  normalize/    Deal · Package · Inventory · Audience · Geo · Format  │
│                Floor · DspSeat · Forecast · Report  ← vendor-free    │
│  validate/     pre-flight rules, scale + floor sanity, blockers      │
│  route/        supply-path decision (auto | advanced)               │
│  pricing/      floor + buyer price via existing computeMargin()      │
│  orchestrate/  state machine, idempotency, retry, dead-letter        │
│  forecast/     wraps existing forecasting engine + SSP sources       │
│  health/       wraps existing pushback analyzer, deal-scoped         │
└───────────────┬─────────────────────────────────────────────────────┘
                │  CurationProvider interface — the ONLY seam
   ┌────────────┼────────────┬────────────────┬──────────────┐
   ▼            ▼            ▼                ▼              ▼
MagniteAdapter PubMatic   ManualFulfilment  PgamDirect   (IndexExchange,
(SpringServe/  Adapter    Adapter           Adapter       OpenX, FreeWheel,
 ClearLine/    (Activate  (ops queue —      (own SSP —     Xandr — later)
 MCTV)          17496)     ships FIRST)      no vendor)
```

### 6.2 The adapter interface

The whole design rests on this. Every provider — including the manual one and
PGAM's own SSP — implements the same contract:

```ts
interface CurationProvider {
  readonly id: 'magnite' | 'pubmatic' | 'manual' | 'pgam_direct'
  readonly capabilities: ProviderCapabilities

  createDeal(deal: NormalizedDeal, idem: string): Promise<DealCreateResult>
  getDealStatus(externalRef: string): Promise<DealStatus>
  updateDeal(externalRef: string, patch: Partial<NormalizedDeal>): Promise<DealUpdateResult>
  pauseDeal(externalRef: string): Promise<void>
  resumeDeal(externalRef: string): Promise<void>
  archiveDeal(externalRef: string): Promise<void>

  forecast?(brief: NormalizedForecastBrief): Promise<NormalizedForecast>
  discoverInventory?(q: InventoryQuery): Promise<InventoryPage>
  syncAudience?(seg: NormalizedAudience): Promise<AudienceSyncResult>
  fetchReport(q: NormalizedReportQuery): Promise<NormalizedReportRow[]>
}
```

Two design points that matter more than they look:

**`capabilities` is declarative, and the UI reads it.** Rather than the front end
knowing that Magnite cannot take a ZIP list at tag level or that PubMatic wants a
seat ID, each adapter publishes what it supports:

```ts
interface ProviderCapabilities {
  mediaTypes: MediaType[]
  geoGranularity: ('country'|'state'|'dma'|'city'|'zip')[]
  supportsAudienceSync: boolean
  supportsForecast: boolean
  supportsInventoryDiscovery: boolean
  supportsProgrammaticCreate: boolean   // false → routes to ManualFulfilment
  editableAfterCreate: (keyof NormalizedDeal)[]
  maxAllowListSize: number | null
  fulfilmentLatency: { p50Minutes: number; p95Minutes: number }
}
```

The buyer UI greys out what an SSP cannot do and explains why, instead of failing
at submit. It also means `supportsProgrammaticCreate: false` is a *configuration*
today and a *one-line flip* the day an entitlement lands — no code path changes.

**`ManualFulfilmentAdapter` is a first-class provider, not a hack.** It implements
`createDeal` by writing an ops task and returning
`{ status: 'pending_fulfilment', eta }`. The Deal Library, notifications, status
machine, reporting and buyer UX are all identical to the API path. This is what
makes the "swap one adapter, product becomes instant" claim true rather than
aspirational.

### 6.3 The package model (brief §5)

```
   DealPackage  "NFL Fans / CTV / A25-54 / Florida"        ← what the buyer sees
        │  one strategy, one name, one report, one health score
        ├── Deal (magnite)   external_deal_id  MGNI-…      ← what PGAM manages
        ├── Deal (pubmatic)  external_deal_id  PM-…
        └── Deal (pgam_direct) external_deal_id pgam-…     ← later
```

The buyer sees the package and, if they ask, the individual Deal IDs (they need
them for their DSP). Reporting rolls up across children by default with a
per-SSP breakdown — which is exactly what makes "one SSP is outperforming
another" (brief §10) a computable insight rather than a manual comparison.

### 6.4 Where the code should live

| Component | Path | Rationale |
|---|---|---|
| Curation engine + adapters | `pgam-dsp-dashboard/src/lib/curation/` | Colocated with tenancy, RBAC, margin, forecasting, AI. Sharing a process beats an RPC hop |
| Buyer surface | `src/app/(curation)/curation/*` | New route group; **must be added to `middleware.ts` `isPublicRoute` logic deliberately** — a new group defaults to Clerk-gated (`SCHEMA.md`) |
| Admin surface | `src/app/admin/curation/*` | Alongside `/admin/deal-ids`, `/admin/deal-sync` — same operator mental model |
| PubMatic Activate client | **Port `core/pubmatic_activate.py` → `src/lib/curation/providers/pubmatic/`** | It is the only curation-seat client PGAM owns and it is stranded in a Python worker with zero callers. TypeScript port, reconciled with the inert `src/lib/vendors/activate/` (keep the better auth, delete the duplicate) |

**Do not build this as a separate service.** The brief says "PGAM Curation API"
as an abstraction, and that is right, but abstraction ≠ deployment boundary. A
separate service would need its own copy of tenancy, RBAC and margin — three
things that must never fork.

---

## 7. Deliverable 7 — Magnite integration requirements

### 7.1 What we have, precisely

| Surface | Base | Auth | State |
|---|---|---|---|
| SpringServe API | `console.springserve.com/api/v0` (also `console.clearline.magnite.com/api/v0`) | email/password → token, rotated by `getSpringServeHeaders()` | **Working.** Read+write on campaigns, demand tags, creatives, deal_lists, deal_id_lists |
| ClearLine Forecasting | `console.clearline.magnite.com/api/v1/clearline_forecasts` | reuses SS token | **Read-only.** POST returns 422 |
| Magnite Streaming / MCTV | `api.tremorhub.com/v1/resources/seats/{seatId}/…` | unknown | **No credentials, no code.** Not attempted |

### 7.2 The three blockers, with exact error strings

**Blocker 1 — `DealIdList` writes rejected account-wide.**
`docs/springserve-quirks.md:57-67`. Every variant tried (v0 POST, v0 PUT, v1 PUT,
`_destroy: true`, empty `inventory_groups`, `source_type: "list"`/`"values"`)
returns:

```
Inventory groups Deal ID lists cannot be modified but can be deleted,
Inventory Groups containing Deal ID can only be deleted,
Inventory groups is invalid
```

Confirmed 0 of 25 demand tags use `DealIdList`. Needs an **account-level enable
from SpringServe support**. A ticket is noted as pending (`quirks.md:319`).
Consequence: ClearLine string deals (`MGNI-MD-416-28107`) can be *read* from
`deal_id_lists/1236` but **cannot be attached to a demand tag by API**.

**Blocker 2 — ClearLine forecast POST unauthorized.**
`src/lib/forecasting/sources/magnite.ts:31-40`:

```
422  "Clearline Forecasting Error: Unauthorized: Access token is missing"
```

The SS API token authorizes READ only; the web UI forwards an additional token
ours does not carry. The client is **fully written and correct** — the file header
states *"Once enabled, this file does NOT need to change."* Needs a **Magnite
seat-config ticket** for `pubops@pgammedia.com`.

**Blocker 3 — no deal-creation surface at all on the account we use.**
Deal creation is manual by design on a demand seat.

### 7.3 The unlock: Magnite Streaming seat API

`[VENDOR-DOC, UNVERIFIED]` Magnite Streaming (MCTV) publishes a seat-scoped deal
endpoint:

```
POST https://api.tremorhub.com/v1/resources/seats/{seatId}/deals
```

Documented alongside Ad Source and Package resources; Deal IDs are noted as
unique per DSP across the MCTV Exchange, with guidance to use test Deal ID
strings when testing creation. Magnite has also publicly stated that **buyers and
curators using ClearLine can define deal terms, pricing and targeting directly**,
and packaged with first/third-party audiences via **Magnite Access** — and has
launched **Deal Discovery** for curating open-market inventory into PMP deals.

**This is the single highest-value item in the entire roadmap.** If PGAM can get
a curator/seat credential on this surface, programmatic Magnite deal creation
becomes real and Phase 1 as originally written becomes achievable.

### 7.4 Required actions — Magnite

| # | Action | Owner | Blocking |
|---|---|---|---|
| M1 | Ask Magnite: **does PGAM have (or can it get) a curation/curator seat with API access to deal creation?** Name `api.tremorhub.com/v1/resources/seats/{seatId}/deals` and ClearLine curation explicitly. Request the seat ID, credentials, sandbox and rate limits | Priyesh | **Everything** |
| M2 | SpringServe support: enable `DealIdList` on account 2724 | Priyesh | Attaching string deals by API |
| M3 | Magnite seat-config ticket: programmatic POST on `/api/v1/clearline_forecasts` for `pubops@pgammedia.com` | Priyesh | Live forecasts (§15) |
| M4 | Ask about **Magnite Access** for audience packaging into deals | Priyesh | Brief §4 differentiator |
| M5 | Ask whether Deal Discovery exposes an API | Priyesh | Inventory discovery (§3.5) |
| M6 | Obtain a machine-readable inventory/publisher catalog (or confirm none exists) | Priyesh | Replacing hand-typed `featuredPublishers` |
| M7 | Until M1: instrument the manual path — measure real fulfilment p50/p95 | Eng | Honest ETAs (§5.3) |

### 7.5 Field mapping — already done

`docs/magnite-field-coverage.md` is a field-by-field map of Magnite's four-level
hierarchy (Demand Partner → Campaign → Demand Tag → Creative) against PGAM's
payloads, with per-field status and priority. **Reuse it as the Magnite adapter's
translation spec.** Note its important constraints:

- Geo, devices, content targeting and frequency caps belong at **line-item /
  demand-tag** level (moved there May 12-13), but PGAM sends most at **campaign**
  level — flagged `Mapped, wrong scope`, several **P0**.
- `iab_categories` (positive form) and `advertiser_domain` are **required to
  serve** and **not sent at all** — seam `V1`, marked *critical*.
- `Device ID Required` **drops most CTV inventory** if enabled — must be a
  visible, warned toggle.
- Some fields lock after create; the adapter's `editableAfterCreate` must encode
  this.

Fix `V1` before curation goes live, or deals will be created that cannot serve.

---

## 8. Deliverable 8 — PubMatic integration requirements

### 8.1 Two clients, two repos, one seat — consolidate first

| | `pgam-dsp-dashboard/src/lib/vendors/activate/` | `pgam-intelligence/core/pubmatic_activate.py` |
|---|---|---|
| Base | `api.pubmatic.com/v1` | **`apps.pubmatic.com/api/activate`** |
| Auth | `POST /developer-integrations/developer/token`, `{userName, password}`, `accountType: ADVERTISER` | **OAuth 2.0 `client_secret_basic`**, 4-value credential set, token endpoint `apps.pubmatic.com/v1/developer-integrations/developer/token`; fallback session `pubtoken` |
| Seat | `PUBMATIC_ACCOUNT_ID` (unset) | **`organizationid = 17496` — `PGAM_Activate_US`** |
| Known state | **`AUTHZ_FAILED` — "account lacks API scope. Contact Pubmatic support to enable Developer Integrations"**; `accountType: AGENCY` rejected; flags OFF; `hasActivateCreds()` false ⇒ every path a no-op | Live advertisers on seat: Amazon (26428), JP Morgan (25784), IHG (26641), Bamboo HR (27017), MF (25871) |
| Confirmed endpoints | none | **exactly one**: `GET /fees/custom/{feeType}/advertiser/{id}` (DevTools 2026-07-07) |
| Inferred/unverified | all | `list_advertisers`, `get_advertiser`, `list_campaigns`, `list_deals`, `get_organization` — all marked *"Inferred"* |
| Write capability | helpers exist, **no deal function** | **none — no POST wrapper at all** |
| Wired into product | Yes, but inert | **No — zero callers** |

**The `pgam-intelligence` client is the real one.** It targets the actual Activate
curator surface with the actual curation seat. But its own docstring is candid:

> Session pubtoken wins if both are set — it's the mode that currently works for
> hitting `/api/activate/*` **while our OAuth scope is being sorted with support**.

So as of writing, the only mode that worked was a **browser-captured session
token that expires on logout**. That is not a foundation for a product; it is
proof the surface exists.

### 8.2 What PubMatic publishes

`[VENDOR-DOC, UNVERIFIED]`

- **PubMatic for Buyers** unifies Activate, Connect and the SSP; includes
  **Gen-AI deal curation from natural-language prompts** — direct overlap with
  brief §6, and a competitive threat to any curation front end.
- Curated deals activate **through Activate or the buyer's DSP of choice — 125+
  DSPs, entering a seat ID to activate for a specific buyer.** This is precisely
  the brief §7 requirement, and it implies the seat-ID-per-DSP model PGAM should
  mirror in its DSP directory.
- A **Deal Management Agent** for PMPs, PGs and auction packages.
- **PMP Deal APIs** exist — but the documentation found is titled *"for
  Publishers"*. Whether the same endpoints serve a **curator** seat is
  **unconfirmed and is the pivotal question.**

### 8.3 Required actions — PubMatic

| # | Action | Owner | Blocking |
|---|---|---|---|
| P1 | **Enable Developer Integrations / API scope on `PGAM_Activate_US` (org 17496)** and finish the OAuth scope issue the docstring references. Get the full 4-value credential set working, non-session | Priyesh | Everything PubMatic |
| P2 | Ask PubMatic directly: **is there a documented deal-CREATE endpoint available to a curator seat?** Request the curator API reference, not the publisher PMP docs | Priyesh | Programmatic creation |
| P3 | Confirm the five inferred endpoints; delete the guesses that are wrong | Eng (needs P1) | Read-side reliability |
| P4 | Ask whether **DSP + seat-ID activation** (125+ DSPs) is API-exposed or UI-only | Priyesh | Brief §7 automation |
| P5 | Ask about **PubMatic Connect** audience packaging into curated deals | Priyesh | Brief §4 |
| P6 | Ask whether Activate exposes **forecasting/avails** | Priyesh | §15 |
| P7 | **Reconcile the two clients.** One TS adapter under `src/lib/curation/providers/pubmatic/`, using the `apps.pubmatic.com/api/activate` base + OAuth + `organizationid` header. Delete or clearly deprecate the inert one | Eng | Avoiding a third competing client |
| P8 | Resolve `accountType`: the DSP client uses `ADVERTISER` and notes `AGENCY` is rejected. Confirm the correct type for a **curator** seat | Priyesh + Eng | Auth correctness |

### 8.4 Why PubMatic should be the **first** SSP, not the second

The brief lists Magnite first. I recommend inverting that:

1. **PGAM already holds a PubMatic curation seat with live advertisers.** No new
   commercial relationship is needed — only an API scope enable (P1).
2. The Magnite path needs a **new seat on a surface PGAM has never touched**
   (`api.tremorhub.com`), which is a longer commercial conversation.
3. PubMatic's own material claims curated deals activate into 125+ DSPs by seat
   ID — the exact primitive the product needs.
4. The base URL, auth flow, org header and one endpoint are already proven in
   `core/pubmatic_activate.py`. That is a real head start.

Magnite stays in Phase 1 — via `ManualFulfilmentAdapter`, which is what happens
today anyway, only now instrumented, audited and inside the product.

---

## 9. Deliverable 9 — Required database / schema changes

### 9.1 Prerequisite: fix the migration substrate

Before adding ~10 tables, address `SCHEMA.md` seams `S1`–`S3`: DDL is created in
**8 different places**, only ~25% of migrations have npm scripts, there is no
applied-state tracking, and there are **duplicate migration numbers** (two
`0055`, two `0056`, two `0076`, two `0101`, two `0102`, two `0135`). `pgam-dsp-dashboard`
has a `npm run migrate` runner and `schema_migrations`; `platform-state-2026-08.md`
open ticket #4 asks for a duplicate-number CI guard. **Do that first** — one
afternoon, and it prevents a whole class of production surprise on a
revenue-critical subsystem.

Next free number: **0144** (0144 is reserved in spirit for the parked Zapier
`referral_source` work; coordinate).

### 9.2 New tables

```sql
-- ── Deal packages: one buyer strategy, N SSP deals ──────────────────
CREATE TABLE curation_packages (
  id                  TEXT PRIMARY KEY,          -- pkg_<ts>_<rand>
  tenant_id           TEXT NOT NULL REFERENCES tenants(id),
  agency_id           TEXT,                      -- agencies.id
  advertiser_id       TEXT NOT NULL,             -- ss_advertisers.id
  brand_name          TEXT,
  name                TEXT NOT NULL,
  status              TEXT NOT NULL DEFAULT 'draft'
                      CHECK (status IN ('draft','validating','submitted',
                                        'creating','active','partial',
                                        'failed','paused','archived')),
  -- Normalized, vendor-free config. The single source of truth that
  -- every adapter translates FROM. Versioned so replay is possible.
  config              JSONB NOT NULL,
  config_schema_ver   INT  NOT NULL DEFAULT 1,
  supply_path_mode    TEXT NOT NULL DEFAULT 'auto'
                      CHECK (supply_path_mode IN ('auto','advanced')),
  flight_start        DATE,
  flight_end          DATE,
  budget_total_cents  BIGINT,
  floor_cpm_cents     INT,
  buyer_price_cpm_cents INT,                     -- what the agency sees
  created_by          TEXT NOT NULL,
  created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  template_id         TEXT,                      -- curation_templates.id
  marketplace_item_id TEXT                       -- if activated from marketplace
);
CREATE INDEX ON curation_packages (tenant_id, status);
CREATE INDEX ON curation_packages (advertiser_id);

-- ── Deals: one row per (package × SSP) ──────────────────────────────
CREATE TABLE curation_deals (
  id                  TEXT PRIMARY KEY,          -- deal_<ts>_<rand>
  package_id          TEXT NOT NULL REFERENCES curation_packages(id) ON DELETE CASCADE,
  tenant_id           TEXT NOT NULL,             -- denormalized for RLS-style scoping
  provider            TEXT NOT NULL              -- 'magnite'|'pubmatic'|'manual'|'pgam_direct'
                      CHECK (provider IN ('magnite','pubmatic','manual','pgam_direct')),
  -- THE payload: the Deal ID the buyer puts in their DSP.
  external_deal_id    TEXT,
  external_ref        TEXT,                      -- provider-side object id
  dsp_id              TEXT REFERENCES curation_dsps(id),
  dsp_seat_id         TEXT,                      -- buyer seat at the DSP
  status              TEXT NOT NULL DEFAULT 'draft'
                      CHECK (status IN ('draft','validating','submitted','creating',
                                        'pending_fulfilment','active','failed',
                                        'paused','archived')),
  status_detail       TEXT,                      -- internal, operator-facing
  buyer_message       TEXT,                      -- simplified, agency-facing
  floor_cpm_cents     INT,
  fulfilment_mode     TEXT NOT NULL DEFAULT 'api'
                      CHECK (fulfilment_mode IN ('api','manual')),
  requested_at        TIMESTAMPTZ,
  fulfilled_at        TIMESTAMPTZ,               -- → real latency metrics
  activated_at        TIMESTAMPTZ,               -- buyer confirmed in their DSP
  created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (provider, external_deal_id)
);
CREATE INDEX ON curation_deals (package_id);
CREATE INDEX ON curation_deals (tenant_id, status);
CREATE INDEX ON curation_deals (status) WHERE status IN ('submitted','creating','pending_fulfilment');

-- ── DSP directory (extensible, not hardcoded — brief §7) ────────────
CREATE TABLE curation_dsps (
  id                  TEXT PRIMARY KEY,          -- 'ttd','dv360','amazon',…
  name                TEXT NOT NULL,
  status              TEXT NOT NULL DEFAULT 'active',
  seat_id_label       TEXT,                      -- what this DSP calls a seat
  seat_id_pattern     TEXT,                      -- regex for client validation
  activation_notes    TEXT,                      -- rendered as buyer instructions
  supported_providers TEXT[] NOT NULL DEFAULT '{}',
  sort_order          INT NOT NULL DEFAULT 100
);

-- Per-tenant saved seats, so a trader never retypes a seat ID.
CREATE TABLE curation_tenant_dsp_seats (
  id            TEXT PRIMARY KEY,
  tenant_id     TEXT NOT NULL REFERENCES tenants(id),
  dsp_id        TEXT NOT NULL REFERENCES curation_dsps(id),
  seat_id       TEXT NOT NULL,
  label         TEXT,
  advertiser_id TEXT,                            -- optional narrower scope
  is_default    BOOLEAN NOT NULL DEFAULT FALSE,
  created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (tenant_id, dsp_id, seat_id)
);

-- ── Reliability: every provider call, auditable and replayable ──────
CREATE TABLE curation_provider_calls (
  id             BIGSERIAL PRIMARY KEY,
  deal_id        TEXT REFERENCES curation_deals(id) ON DELETE SET NULL,
  package_id     TEXT,
  provider       TEXT NOT NULL,
  operation      TEXT NOT NULL,                  -- 'createDeal','getDealStatus',…
  idempotency_key TEXT NOT NULL,
  attempt        INT  NOT NULL DEFAULT 1,
  request_body   JSONB,                          -- credentials NEVER included
  response_status INT,
  response_body  JSONB,
  error_code     TEXT,
  duration_ms    INT,
  created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE UNIQUE INDEX ON curation_provider_calls (idempotency_key, attempt);
CREATE INDEX ON curation_provider_calls (deal_id, created_at DESC);

-- Ops work queue — backs ManualFulfilmentAdapter.
CREATE TABLE curation_fulfilment_tasks (
  id            TEXT PRIMARY KEY,
  deal_id       TEXT NOT NULL REFERENCES curation_deals(id) ON DELETE CASCADE,
  provider      TEXT NOT NULL,
  status        TEXT NOT NULL DEFAULT 'open'
                CHECK (status IN ('open','claimed','done','cancelled','blocked')),
  claimed_by    TEXT,
  instructions  JSONB NOT NULL,   -- rendered checklist for the operator
  sla_due_at    TIMESTAMPTZ,
  created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  completed_at  TIMESTAMPTZ
);
CREATE INDEX ON curation_fulfilment_tasks (status, sla_due_at);

-- ── Templates + marketplace (brief §7, §8) ──────────────────────────
CREATE TABLE curation_templates (
  id          TEXT PRIMARY KEY,
  tenant_id   TEXT,                              -- NULL = PGAM-global
  name        TEXT NOT NULL,
  description TEXT,
  config      JSONB NOT NULL,                    -- same shape as packages.config
  created_by  TEXT,
  created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE curation_marketplace_items (
  id                TEXT PRIMARY KEY,            -- 'nfl-ctv','auto-intenders',…
  category          TEXT NOT NULL,               -- 'sports'|'audience'|'ctv'|'attention'
  name              TEXT NOT NULL,
  description       TEXT,
  config            JSONB NOT NULL,
  providers         TEXT[] NOT NULL DEFAULT '{}',
  est_cpm_cents     INT,
  est_attention     NUMERIC(5,2),
  featured_publishers TEXT[],
  -- Replaces the hardcoded arrays in src/lib/clearline-packs.ts.
  -- NULL/empty ⇒ item cannot be activated; enforced in code.
  seed_deal_ids     JSONB,                       -- { magnite: [...], pubmatic: [...] }
  status            TEXT NOT NULL DEFAULT 'draft'
                    CHECK (status IN ('draft','active','retired')),
  min_daily_budget_cents INT,
  curated_by        TEXT,
  last_curated_at   TIMESTAMPTZ
);

-- ── Economics: per-tenant/agency commercial terms (brief §13) ───────
CREATE TABLE curation_pricing_rules (
  id             TEXT PRIMARY KEY,
  scope          TEXT NOT NULL CHECK (scope IN ('global','tenant','agency','advertiser','package')),
  scope_id       TEXT,
  provider       TEXT,                           -- NULL = all
  media_type     TEXT,                           -- NULL = all
  ssp_fee_pct    NUMERIC(6,4),
  curation_cpm_cents INT,
  data_cpm_cents INT,
  attention_premium_pct NUMERIC(6,4),
  pgam_margin_pct NUMERIC(6,4),
  managed_fee_pct NUMERIC(6,4),
  min_floor_cpm_cents INT,
  priority       INT NOT NULL DEFAULT 100,       -- lower wins
  effective_from DATE,
  effective_to   DATE,
  created_by     TEXT,
  created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX ON curation_pricing_rules (scope, scope_id, priority);
```

### 9.3 Alterations to existing tables

```sql
-- Deal-scoped reporting (brief §10). Rollups are already vendor-keyed.
ALTER TABLE keystone_reporting_rollups
  ADD COLUMN IF NOT EXISTS curation_deal_id TEXT,
  ADD COLUMN IF NOT EXISTS curation_package_id TEXT;
CREATE INDEX IF NOT EXISTS idx_krr_curation_deal
  ON keystone_reporting_rollups (curation_deal_id, dimension_date);

-- The CHECK currently allows only ('clearline','activate'). Widen it or
-- curation rows cannot be inserted at all.
ALTER TABLE keystone_reporting_rollups DROP CONSTRAINT IF EXISTS keystone_reporting_rollups_vendor_check;
ALTER TABLE keystone_reporting_rollups ADD CONSTRAINT keystone_reporting_rollups_vendor_check
  CHECK (vendor IN ('clearline','activate','magnite','pubmatic','pgam_direct','vibe'));

-- Link a campaign to the deal it activates against (optional).
ALTER TABLE ss_campaigns ADD COLUMN IF NOT EXISTS curation_deal_id TEXT;

-- Add the curation feature to tenant gating.
ALTER TABLE tenant_settings
  ADD COLUMN IF NOT EXISTS curation_enabled BOOLEAN NOT NULL DEFAULT FALSE,
  ADD COLUMN IF NOT EXISTS curation_allowed_providers TEXT[] NOT NULL DEFAULT '{}',
  ADD COLUMN IF NOT EXISTS curation_approval_rule TEXT NOT NULL DEFAULT 'manual',
  ADD COLUMN IF NOT EXISTS curation_auto_threshold_cents BIGINT NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS curation_max_open_deals INT;
```

### 9.4 Two schema decisions worth making explicitly

**`config JSONB` vs fully normalized columns.** I recommend JSONB for the
targeting config, with a version number. Reasons: the normalized shape will churn
weekly early on; every adapter needs the *whole* object to translate; and replay
("recreate this deal on another SSP") requires the exact original. Enforce shape
in TypeScript + a validation function, not in DDL. Extract to columns only what
must be indexed or aggregated (status, dates, budget, floor).

**Resolve the three-advertiser-table seam first (seam `D1`).** `ss_advertisers`,
`dsp_advertisers` and `advertisers` with two ID forms (`dsp-mirror-<id>`,
`clearline-<id>`) already causes confusion. Curation adds agency→advertiser→brand
→package→deal on top. **Pick one canonical advertiser identity before building
this**, or the Deal Library will need per-surface translation forever.

---

## 10. Deliverable 10 — Required API endpoints

All under `/api/v1/curation/`. All follow the existing guard chain:
`requireRole(...)` → `enforceTenantOrForbid(...)` → handler.

### 10.1 Buyer-facing

| Method | Path | Purpose | Permission |
|---|---|---|---|
| `GET` | `/packages` | Deal Library list — filter by status, advertiser, provider, media type, date | `curation.view` |
| `POST` | `/packages` | Create draft package | `curation.write` |
| `GET` | `/packages/{id}` | Package + child deals + health + delivery | `curation.view` |
| `PATCH` | `/packages/{id}` | Edit draft, or permitted post-create fields | `curation.write` |
| `POST` | `/packages/{id}/validate` | Pre-flight; returns typed warnings/blockers | `curation.write` |
| `POST` | `/packages/{id}/submit` | **The Create Deal action.** Idempotent | `curation.write` |
| `POST` | `/packages/{id}/duplicate` | Clone as new draft | `curation.write` |
| `POST` | `/packages/{id}/pause` · `/resume` · `/archive` | Lifecycle, fans out to children | `curation.write` |
| `POST` | `/packages/{id}/extend` | Push flight end date | `curation.write` |
| `POST` | `/packages/{id}/copy-to-provider` | Add another SSP to an existing package | `curation.write` |
| `POST` | `/packages/{id}/copy-to-dsp` | Same config, different DSP/seat | `curation.write` |
| `GET` | `/packages/{id}/export` | CSV/JSON export | `curation.view` |
| `GET` | `/packages/{id}/activation-instructions` | Per-DSP steps + Deal IDs | `curation.view` |
| `POST` | `/deals/{id}/mark-activated` | Buyer confirms live in their DSP | `curation.write` |
| `GET` | `/deals/{id}/status` | Poll during creation | `curation.view` |
| `POST` | `/forecast` | Forecast a config pre-submit; labels source | `curation.view` |
| `GET` | `/inventory/search` | Publisher/app/domain/genre discovery + est. scale | `curation.view` |
| `GET` | `/audiences` | Available segments for this tenant, with sync capability flags | `curation.view` |
| `GET` | `/dsps` · `/dsps/{id}/seats` | DSP directory + tenant's saved seats | `curation.view` |
| `POST` | `/dsps/{id}/seats` | Save a seat | `curation.write` |
| `GET` | `/marketplace` · `GET /marketplace/{id}` | Pre-built catalog | `curation.view` |
| `POST` | `/marketplace/{id}/activate` | One-click: pick DSP/seat → package | `curation.write` |
| `GET` `POST` | `/templates` | List / save templates | `curation.view` / `.write` |
| `POST` | `/ai/interpret` | Free-text brief → draft config + rationale | `curation.write` |
| `GET` | `/reporting/packages/{id}` | Delivery + performance, per-SSP breakdown | `curation.view` |
| `GET` | `/health/packages/{id}` | Deal Health + ranked recommendations | `curation.view` |
| `POST` | `/health/packages/{id}/apply` | Apply a recommendation | `curation.write` |

### 10.2 Admin-facing

| Method | Path | Purpose |
|---|---|---|
| `GET` | `/admin/curation/packages` | Every package, every tenant |
| `GET` | `/admin/curation/queue` | Fulfilment queue |
| `POST` | `/admin/curation/tasks/{id}/claim` · `/complete` | Operator claims, then supplies the Deal ID |
| `POST` | `/admin/curation/packages/{id}/approve` · `/reject` | Approval gate |
| `POST` | `/admin/curation/deals/{id}/retry` | Replay a failed provider call |
| `POST` | `/admin/curation/deals/{id}/override-routing` | Force a provider |
| `GET` `PUT` | `/admin/curation/pricing-rules` | Margin/floor/fee configuration |
| `GET` `PUT` | `/admin/curation/marketplace/{id}` | Curate catalog incl. real deal IDs — **replaces editing `clearline-packs.ts` + redeploy** |
| `GET` | `/admin/curation/provider-health` | Per-provider auth, latency, error rate, capability flags |
| `GET` | `/admin/curation/calls` | Provider call log / dead-letter |
| `PUT` | `/admin/curation/tenants/{id}/settings` | Enable curation, providers, caps, approval rule |

### 10.3 Internal

| Path | Trigger | Purpose |
|---|---|---|
| `/api/v1/cron/curation/poll-creating` | every 1–2 min | Poll `submitted`/`creating` deals; advance or fail |
| `/api/v1/cron/curation/sla-alerts` | every 15 min | Alert on fulfilment tasks past SLA |
| `/api/v1/cron/curation/reports-ingest` | every 30 min | Deal-scoped delivery into rollups |
| `/api/v1/cron/curation/health-scan` | hourly | Deal Health signals + no-request detection |
| `/api/v1/webhooks/curation/{provider}` | provider push | If/when a provider offers webhooks (verify signatures) |

---

## 11. Deliverable 11 — UI / screens required

| Screen | Route | Notes | Class |
|---|---|---|---|
| Curation home | `/curation` | Recent packages, drafts, alerts, "New Deal", marketplace teasers | **[BN]** |
| **Deal builder** | `/curation/new` | The single-screen builder in §5.1. Progressive disclosure, live forecast panel, inline warnings | **[BN]** — reuse wizard sub-components |
| AI deal builder | `/curation/new?mode=ai` | Prompt box → proposed config diffed against a blank deal, fully editable | **[EE]** from `agent-chat.tsx` |
| **Deal Library** | `/curation/deals` | Table with every brief §7 column; row actions; bulk ops; saved views | **[BN]** — reuse `report_saved_views` (`0089`) |
| Package detail | `/curation/deals/{id}` | Tabs: Overview · Deal IDs & Activation · Targeting · Delivery · Health · History | **[BN]** |
| Marketplace | `/curation/marketplace` | Category browse (Sports, Audience, CTV, PGAM Attention), scale + CPM + attention per item, one-click activate | **[EE]** from `/inventory/packs` |
| Templates | `/curation/templates` | Save/apply/share | **[BN]** |
| Forecast explorer | `/curation/forecast` | Standalone what-if | **[EE]** from `/forecasting/new` |
| DSP seats | `/curation/settings/seats` | Manage saved DSP seats | **[BN]** |
| Admin: queue | `/admin/curation/queue` | **The operator console.** Claim → checklist → paste Deal ID → done. Replaces `/admin/deal-ids` + `/admin/deal-sync` + editing source | **[BN]** |
| Admin: all packages | `/admin/curation/packages` | Cross-tenant, with economics | **[BN]** |
| Admin: approvals | `/admin/curation/approvals` | Reuse `/admin/campaign-approvals` patterns | **[EE]** |
| Admin: pricing | `/admin/curation/pricing` | Rule editor with a live `Supply + SSP fee + Data + Margin = Buyer price` preview | **[BN]** |
| Admin: marketplace curation | `/admin/curation/marketplace` | Edit catalog + attach real deal IDs in the DB | **[BN]** |
| Admin: provider health | `/admin/curation/providers` | Auth state, capability matrix, latency, errors, retry | **[BN]** |

### 11.1 UX rules that matter for this specific product

1. **Never show an SSP name to a buyer unless they opted into Advanced mode.**
   Already policy: `docs/magnite-field-coverage.md` — *"Magnite / SpringServe /
   ClearLine branding is never user-facing — strip it from labels."* In Automatic
   mode say "PGAM Supply"; in Advanced, name them.
2. **Label every number's provenance.** `PGAM estimate` vs `Magnite forecast`.
   Brief §9 asks for this; it is also the difference between a trader trusting
   the tool and quietly discounting everything it says.
3. **Warn at build time, not submit time.** Floor above market, over-restrictive
   inventory, invalid ZIPs, audience below k-anon, `Device ID Required` on CTV.
   Adapter `capabilities` drive most of these client-side.
4. **The Deal ID is the payload.** Big, copyable, with per-DSP instructions. It
   is the thing the buyer came for.
5. **Never show a fake instant.** §5.3.
6. **Do not build for ad-tech engineers.** The whole point is that the buyer never
   learns what an inventory group is.

---

## 12. Deliverable 12 — Permissions / security model

### 12.1 Reuse, extend narrowly

Add `curation` to `FEATURES` in `src/lib/rbac.ts` (`none`/`view`/`edit`), plus
agency-facing tenant roles which already exist in `TenantRole`
(`agency_user`, `advertiser_user`, `partner_admin`). Curation permissions:
`curation.view`, `curation.write`, `curation.approve`, `curation.admin`,
`curation.pricing` (⊂ finance).

Hard constraints in the spirit of the existing ones:

- `curation.pricing` gated by `FINANCE_ALLOWLIST` — margin config is finance.
- `curation.admin` (routing override, retry, cross-tenant) never grantable to a
  tenant user, only PGAM staff.
- Tenant users can never read another tenant's package, deal, seat or economics.

### 12.2 Multi-tenant isolation — the top risk

Curation data is *more* sensitive than campaign data: a Deal ID is a bearer token
for inventory access, and a competitor's deal config is competitive intelligence.

Requirements:
1. `tenant_id NOT NULL` on every curation table, **denormalized onto
   `curation_deals`** so no query needs a join to be safe.
2. Every route calls `enforceTenantOrForbid` — the pattern
   `advertisers/[id]/deal-id/route.ts` already uses, with a comment naming deal
   IDs as *"a revenue-sensitive linkage"* that blocks cross-tenant reads **even
   for Clerk admins**. Extend that stance.
3. **The leak test the white-label plan already specifies**: seed Tenant A data,
   hit every curation list endpoint as Tenant B, assert zero rows. Make it a CI
   gate — which requires standing up CI (§3.5).
4. Fail closed on unresolved tenant.

### 12.3 Credentials

- **No SSP credential ever reaches the client.** All provider calls server-side.
  Currently satisfied structurally; keep it.
- `curation_provider_calls.request_body` must be **scrubbed** — never log
  `Authorization`, `pubtoken`, `client_secret`, passwords.
- Move SSP credentials toward a secret manager rather than plain Vercel env. The
  `ss_connectors` pattern (`0121`) already encrypts partner credentials at the
  application layer into a `BYTEA` column *"so we never accidentally log
  plaintext"* — reuse that approach.
- **Rotate the PubMatic session `pubtoken` path out of existence** once OAuth
  works (P1). A browser-captured token in an env var is not an acceptable
  production credential.
- DSP seat IDs are tenant-confidential, not secret — scope them, don't encrypt.

### 12.4 Data protection

- Audience activation already enforces k-anonymity (1000 households / 500
  visitors) and refuses below-floor segments — keep this in the curation path;
  do not let a deal-builder shortcut bypass it.
- First-party O&O segments carry per-site consent scope; `audience_oo_rules`
  notes cross-property linkage needs separate consent language (Phase 3). **Do
  not let curation quietly join across properties.**
- CCPA/GDPR: curation itself processes little PII, but audience sync does. Any
  segment push to an SSP needs a documented lawful basis and a DPA per provider.
  Flag to counsel before P5/M4 work starts.
- Audit: `curation_provider_calls` + reuse `tenant_audit_log` (the Vibe leg
  already double-logs `.attempt` then `.ok`/`.fail` — copy that discipline).

### 12.5 Pre-existing security issues to fix first

From `SCHEMA.md`, these matter because curation raises the value of the target:

- **`A4`** — only one of 269 routes is rate-limited. Curation endpoints must be.
- **`F1`** — Invoca postback has **no signature verification**. Fix before adding
  any new webhook, so the pattern being copied is a correct one.
- **`A2`** — dual auth (Clerk + legacy `ss_token`) with no flag to disable the
  legacy path. Decide before agency users arrive.
- **`A3`** — 18 of 26 self-serve routes don't resolve advertiser context.

---

## 13. Deliverable 13 — Pricing / margin architecture

### 13.1 What exists is close to what's needed

`src/lib/margin.ts` is a **pure function** — `MarginInputs` in,
`MarginBreakdown` out, no DB or network — with per-SSP platform fees, internal
fee layers from `fee_config_advertiser`/`fee_config_campaign`, a separate CPA
profile, and env defaults (`MAGNITE_PLATFORM_FEE_PCT=0.10`,
`PUBMATIC_PLATFORM_FEE_PCT=0.07`). `tenant_settings` adds `platformFeePct` and
`reportingVisibility: margin_hidden`. That is most of brief §13 already.

### 13.2 What to add

The brief's identity —

```
Supply Cost + SSP Fees + Data Cost + PGAM Margin = Buyer Price
```

— is a *forward* price computation. `computeMargin()` is a *backward* reconciliation
(given spend and revenue, what was the margin). Both are needed:

```ts
// NEW — quote a floor/price before the deal exists.
function computeBuyerPrice(i: {
  estimatedSupplyCpmCents: number       // from forecast or rate card
  provider: SupplyProvider
  sspFeePct: number                     // pricing rule → env default
  dataCpmCents: number                  // audience layers
  attentionPremiumPct: number           // PGAM attention uplift
  pgamMarginPct: number
  managedFeePct: number
  minFloorCpmCents: number | null
}): {
  buyerPriceCpmCents: number
  floorCpmCents: number
  breakdown: PriceBreakdown             // admin-only
  buyerVisible: { priceCpmCents: number }  // agency-only
}
```

Then: **resolve pricing by rule precedence** — `package` → `advertiser` →
`agency` → `tenant` → `global`, lowest `priority` wins, date-bounded. That gives
agency-specific commercial agreements without hardcoding a business model, which
is exactly what the brief asks for.

### 13.3 The visibility discipline

Reuse the client-portal pattern, which is already rigorous. `assertClientPortalSafe`
*"blocks banned terms and any raw vendor spend (`vibe_raw_spend_usd` never leaves
the server). Client spend is always impressions × our retail CPM, never a
vendor's raw number."*

Apply identically to curation:

| Audience | Sees |
|---|---|
| Agency (tenant user) | Buyer price CPM, their own spend, delivery, performance. **Never** supply cost, SSP fee, PGAM margin, or the provider's raw numbers |
| PGAM operator (`curation.pricing`) | Full waterfall, per-provider, per-deal |
| PGAM owner | Above + P&L rollup |

Enforce **server-side**, by not serializing the fields — never by hiding them in
the client. Add a curation equivalent of `assertClientPortalSafe` and unit-test
that a tenant-scoped response never contains margin keys. This is the kind of
thing that leaks once and costs a partner.

### 13.4 Monetization levers, all config

Every model in brief §13 maps to existing or proposed config:
curation CPM (`curation_cpm_cents`), % of media (`pgam_margin_pct`), SSP curation
margin (`ssp_fee_pct`), data margin (`data_cpm_cents`), audience CPM (same),
attention premium (`attention_premium_pct`), managed-service fee
(`managed_fee_pct`), agency-specific terms (rule `scope='agency'`). No code
change to change a business model.

---

## 14. Deliverable 14 — Reporting architecture

### 14.1 Substrate exists and is already multi-SSP

`keystone_reporting_rollups` is keyed `(vendor, campaign_id, publisher_id,
dimension_date)` with impressions, completed views, clicks, spend, publisher
payout, `working_media_pct`, and an attention overlay joined by a scheduled job.
The pushback analyzer *"reads from `keystone_reporting_rollups` without filtering
on `vendor`, so the moment Activate rows start flowing, pushback recommendations
will be emitted for Activate campaigns automatically."* That is genuinely good
design for this purpose.

### 14.2 Changes needed

1. **Add `curation_deal_id` + `curation_package_id`** (§9.3) so a deal is a
   first-class reporting dimension, not inferred from a campaign.
2. **Widen the `vendor` CHECK constraint** — it currently permits only
   `('clearline','activate')`. Overlooked, this silently blocks every insert.
3. **Package-level rollup with per-SSP breakdown.** Reuse the merge discipline in
   `src/lib/reporting/vibe-merge.ts`, which already merges two buying legs
   additively with source-aware freshness and never shows *"zeros-as-real"*.
   Apply the same rules across SSP children of a package — that is what makes
   brief §10's "one SSP is outperforming another" honest.
4. **Deal-level metrics the brief asks for**: spend, impressions, CPM, win rate,
   bid requests, fill, publisher/app/domain, SSP, DSP, geo, device, attention,
   VCR, deal health. Note that **win rate, bid requests and fill are
   supply-side/auction metrics** — available for `pgam_direct` deals and for
   whatever the SSP report exposes for curated deals. That availability is
   **unconfirmed per SSP** and belongs in the §19.6 probes; don't design a UI
   around a metric that may not arrive.

### 14.3 Deal Health

Retarget the existing pushback analyzer. The brief's list maps almost 1:1 to
computable signals:

| Signal | Detection | Recommendation |
|---|---|---|
| Deal created, no requests | zero bid requests > 24h after `activated_at` | Verify seat ID; confirm DSP-side activation; check deal is unpaused at SSP |
| DSP not bidding | requests > 0, bids = 0 | Floor above DSP max bid; creative/format mismatch; seat not authorized |
| Floor too high | fill % far below cohort median at this floor | Suggest a specific lower floor with projected fill |
| Inventory too restrictive | avails far below forecast | Name the narrowing dimension; propose a relaxation |
| Audience limiting scale | reach below forecast with audience layer on | Suggest broader segment or removing a layer |
| Underpacing | delivery vs budget-derived goal (pattern already in the portal) | Raise floor, widen inventory, extend flight |
| One SSP outperforming | per-child comparison on CPM/attention/VCR | Shift budget; pause the laggard |

Reuse `keystone_pushback_recommendations`, the apply/rollback columns, threshold
overrides and the gate — all already built, all currently campaign-scoped.

---

## 15. Deliverable 15 — Forecasting approach

### 15.1 What exists

- **PGAM estimate (works today):** deterministic engine (764 lines) over a static
  supply graph (527 lines) — channel × device × geo × publisher, goal-driven
  channel ordering, blend bias, audience multipliers with diminishing returns,
  attention-qualified impressions, rule-based recommendations. Live rate card
  feeds CPM floors. `POST /api/v1/forecast`, `/forecasting/new`, CSV export.
- **Magnite live forecast (coded, blocked):** full create-and-poll client against
  `console.clearline.magnite.com/api/v1/clearline_forecasts`, returning
  `impressionOpportunities` and `uniques`. **POST returns 422.** Falls back
  silently to the internal graph. Notably, it does **not** return CPM bands, so
  even when enabled the CPM stays a PGAM estimate.
- **PubMatic forecast:** unknown — probe P6.

### 15.2 The approach

**Three-tier, always labelled.**

| Tier | Source | Label | Confidence |
|---|---|---|---|
| 1 | Live SSP forecast API | `Magnite forecast` / `PubMatic forecast` | high |
| 2 | PGAM historical delivery — actual rollups for comparable configs | `PGAM historical` | medium-high |
| 3 | Heuristic supply graph | `PGAM estimate` | medium |

Tier 2 does not exist yet and is **the highest-value forecasting work available
without any vendor dependency.** PGAM has real delivery data in
`keystone_reporting_rollups`, `ss_daily_stats`, `ss_dimension_rollups` and
`ctv_impressions`. A forecast grounded in *what PGAM actually delivered on
comparable inventory* beats a static graph, and is defensible to a trader in a
way a heuristic is not. Build it: index historical delivery by (media type, geo,
publisher/genre, floor band), then interpolate.

Also fix the honesty gap in the current engine: `supply-graph.ts` is described as
a *"static dataset modeling PGAM's channel × device × geo × publisher inventory
shape"* and `clearline-packs.ts` carries mock CPMs. The UI must not present Tier-3
output as if it were avails. Show a range, name the tier, and state the basis.

### 15.3 Distinguishing SSP forecast from PGAM estimate

Brief §9 explicitly requires this. Implement structurally, not cosmetically:

```ts
interface NormalizedForecast {
  availableImpressions: { value: number; source: ForecastSource; confidence: Confidence }
  uniqueReach:          { value: number; source: ForecastSource; confidence: Confidence }
  cpmBand:              { min: number; median: number; max: number; source: ForecastSource }
  byPublisher:          Array<{ name: string; impressions: number; source: ForecastSource }>
  // …device mix, geo mix, audience scale — each independently sourced
  caveats: string[]     // e.g. "CPM is a PGAM estimate; Magnite does not return CPM bands"
}
```

Per-field sourcing matters because a single forecast legitimately mixes tiers —
Magnite avails with a PGAM CPM estimate is the *expected* case once M3 lands.

---

## 16. Deliverable 16 — AI curation architecture

### 16.1 What exists is a strong start

`/api/v1/agent/chat` runs Claude Sonnet as "PGAM Media's Senior AI Media Planner"
with a 6-layer planning methodology (objective → audience behaviour → budget math
→ outcome forecasting → …) and 12 vertical playbooks with real CPM benchmarks and
named DMAs. Critically, it **already injects live platform context**:

```ts
interface SegmentContext { id; name; provider; size; cpm }
interface DealContext    { id; name; publisher; floorCpm; attentionEstimate }
```

That is close to the right shape for curation. There is also AI spend rate
limiting, `ss_ai_plans` (`0101`), and buyer-agent action/approval tables
(`0098`–`0102`) that already model *propose → approve → apply*.

One caveat to fix: the system prompt asserts facts as ground truth — e.g. *"We
access 109M+ US ad-supported CTV households through Magnite"* and *"Deal IDs
activate within existing DSP workflows, no integration required."* For a
marketing planner that is fine. For a tool that will **create real deals**,
unverified constants in a prompt become hallucinated capability. Move all such
figures into injected, sourced context.

### 16.2 Architecture: grounded tool-use, not free generation

The brief's requirement — *"use actual PGAM inventory and integration data
wherever possible rather than hallucinating"* — is best met by **never letting the
model invent an entity**. It selects from real ones via tools.

```
Buyer free text
   │
   ▼
POST /api/v1/curation/ai/interpret
   │
   ├─ Claude (Sonnet) with tool definitions, NOT free-form JSON output:
   │     search_inventory(query, mediaType, geo)   → real publishers/apps/genres
   │     search_audiences(intent)                  → real segments + size + CPM + k-anon
   │     get_marketplace_items(category)           → real packages
   │     resolve_geo(text)                         → real DMA/state/ZIP codes
   │     get_rate_card(mediaType, tier)            → real floors
   │     run_forecast(draftConfig)                 → real forecast, real tier label
   │     get_provider_capabilities()               → what each SSP can actually do
   │
   ├─ Model composes a NormalizedDealConfig using ONLY tool-returned IDs
   │
   ▼
Server-side validation (same validator as the manual path)
   │
   ▼
Draft package + a rationale per decision + explicit uncertainty flags
   │
   ▼
Buyer reviews a DIFF against a blank deal, edits freely, then submits
```

Design rules:

1. **The model never emits an entity ID it did not receive from a tool.** Validate
   every ID against the catalog server-side and reject the response rather than
   the field — a partially-hallucinated config is worse than a failed parse.
2. **The model never submits.** It produces a draft. Human clicks Create. The
   buyer-agent approval tables (`0101_buyer_agent_pending_approvals`) already
   encode this posture.
3. **Rationale per field**, so a trader can audit *why* Florida DMAs and a $22
   floor. Traders reject tools they can't interrogate.
4. **Explicit uncertainty.** If the brief says "homeowners" and no homeowner
   segment is syncable to the chosen SSP, say so and offer the contextual
   proxy — never silently substitute.
5. **Same validator, same schema, same adapter** as the manual path. AI is an
   input method, not a parallel pipeline.
6. Reuse `checkAiSpendRateLimit`; add per-tenant quotas.

### 16.3 Worked example

> *"I need premium CTV inventory targeting homeowners in Florida for a $50,000
> home services campaign."*

| Field | Proposed | Grounded in | Flag |
|---|---|---|---|
| Media type | CTV | explicit in brief | — |
| Geo | FL — DMAs 528, 534, 539, 561, 571, 686 | `resolve_geo("Florida")` | — |
| Inventory | Premium VOD + Live Sports + Local News marketplace items | `get_marketplace_items('ctv')` | ⚠ *marketplace deal IDs are placeholders today* |
| Audience | Homeowners | `search_audiences('homeowners')` | ⚠ **not syncable to Magnite today** — offers home-improvement contextual + high-HHI as proxy |
| Floor | $24 CPM | `get_rate_card('ctv','premium')` | — |
| Budget/flight | $50k over 30d ⇒ ~$1,667/day ⇒ ~2.08M imps | budget math layer | — |
| Forecast | 4.1M avails, 890k reach | `run_forecast` | labelled `PGAM estimate` — Magnite forecast POST blocked |
| SSP | PubMatic (curation seat) | `get_provider_capabilities()` | Magnite via manual fulfilment |
| Verdict | Deliverable | | 2 warnings surfaced, not hidden |

Note how the grounding turns the two blockers from this audit into **visible
product warnings** instead of silent wrong answers. That is the whole design goal.

---

## 17. Deliverable 17 — MVP scope

### 17.1 Why I am changing the brief's Phase 1

The brief's Phase 1 contains "Deal creation" and "Deal ID retrieval" for Magnite
and PubMatic. Those are the only two items in the list that engineering cannot
finish on its own, and everything else in the phase is buildable in weeks. Putting
them together means the whole phase reports "blocked" while 90% of it is done.

So: **keep Phase 1's scope, change its definition of "deal creation" from
`API-only` to `adapter-mediated, fulfilment-agnostic`.** The buyer UX, database,
Deal Library, notifications and admin console are identical either way. Only the
adapter's `supportsProgrammaticCreate` flag differs.

This also has a real benefit beyond unblocking: it forces the manual path to be
**instrumented from day one**, so PGAM finally has p50/p95 fulfilment latency
data. You cannot promise five minutes until you can measure forty.

### 17.2 MVP (Phase 1) — in scope

| # | Item | Class | Vendor-blocked? |
|---|---|---|---|
| 1 | Migration substrate fix: single runner, duplicate-number CI guard | **[EE]** | No |
| 2 | Stand up CI (`.github/workflows`) running the existing 47 test files | **[BN]** | No |
| 3 | Canonical advertiser identity decision (seam `D1`) | **[EE]** | No |
| 4 | Curation schema (§9.2, §9.3) | **[BN]** | No |
| 5 | Normalized objects + `CurationProvider` interface + `capabilities` | **[BN]** | No |
| 6 | **`ManualFulfilmentAdapter`** + ops queue + SLA alerts + latency metrics | **[BN]** | No |
| 7 | `MagniteAdapter` — read + attach + **manual create**; wraps existing SpringServe modules | **[EE]** | Partially (create) |
| 8 | `PubMaticAdapter` — port `core/pubmatic_activate.py` to TS, reconcile with the inert client, read-side live | **[EE]** | Yes (needs P1) |
| 9 | Deal builder UI (§5.1) — CTV/OLV/Display/In-App, geo, inventory, floor, flight, budget | **[BN]** | No |
| 10 | DSP directory + tenant saved seats | **[BN]** | No |
| 11 | Forecast in the builder — Tier 3 now, Tier 2 (historical) if time | **[EE]** | No |
| 12 | Pre-flight validation + warnings, capability-driven | **[BN]** | No |
| 13 | Submit → status machine → idempotency → retry → dead-letter | **[BN]** | No |
| 14 | Deal ID delivery: display, copy, per-DSP activation instructions, notify | **[BN]** | No |
| 15 | Deal Library with brief §7 columns + edit/duplicate/pause/resume/archive/extend | **[BN]** | No |
| 16 | Admin console: queue, all packages, approvals, retry, routing override, provider health | **[BN]** | No |
| 17 | Pricing rules + `computeBuyerPrice` + margin visibility enforcement | **[EE]** | No |
| 18 | RBAC: `curation` feature + agency roles + **cross-tenant leak test in CI** | **[EE]** | No |
| 19 | Fix seam `V1` (`iab_categories`, `advertiser_domain`) — required-to-serve | **[EE]** | No |
| 20 | Deal-scoped reporting: `curation_deal_id` + widened vendor CHECK + package rollup | **[EE]** | No |

### 17.3 Explicitly NOT in MVP

- **Automated deal creation** — flips on per provider as M1/M2/P1/P2 land. No code
  change beyond a capability flag.
- **Audience layering into deals** — catalog and picker can ship read-only with
  honest capability flags; **sync is Phase 2+ and commercially gated** (§3.4).
- Multi-SSP simultaneous deployment — schema supports it from day one; the *UX*
  waits until two providers can actually create.
- Marketplace with real deal IDs — the catalog and admin curation UI ship;
  activation stays gated on `seed_deal_ids` being non-empty.
- AI deal builder, Deal Health, white-label curation branding, additional SSPs,
  agency API access.

### 17.4 The MVP promise to an agency

> "Log into PGAM. Tell us what you want to buy — inventory, geography, format,
> floor, your DSP and seat. We'll validate it, forecast it, price it, and build
> the deal. You'll have the Deal ID in your inbox, typically within
> {measured p50}, and you'll see delivery and performance back in PGAM."

Every clause of that is true on day one of Phase 1. Note what is missing: the word
"instantly." Add it when the probes come back green.

---

## 18. Deliverable 18 — Phase 2 / Phase 3 roadmap

### Phase 2 — automation + differentiation

| Item | Trigger / dependency |
|---|---|
| **Flip Magnite to programmatic create** | M1 (curator seat on `api.tremorhub.com`) or M2 (`DealIdList` enable) |
| **Flip PubMatic to programmatic create** | P1 + P2 |
| Multi-SSP deployment UX (one strategy → N Deal IDs) | ≥2 providers with create |
| **Live SSP forecasting** | M3 / P6 |
| **Tier-2 historical forecasting** | **None — do this in Phase 2 regardless.** Highest-value unblocked forecasting work |
| **AI deal builder** (grounded tool-use, §16) | None — unblocked |
| Templates + template sharing | None |
| Marketplace with real deal IDs | Deal-create automation, or ops backfill via admin UI |
| Audience layering — contextual/domain-shaped only | None — ships on the existing `domain_list_ids` transport |
| Audience layering — demographic/intent/household | **M4 / P5 + commercial + legal.** Do not schedule until answered |
| Deal-scoped reporting depth (win rate, bid requests, fill) | Per-SSP report field availability — probe first |
| Inventory discovery from SSP APIs | M5 / M6 / P-equivalents |

### Phase 3 — platform

| Item | Notes |
|---|---|
| **Deal Health + automated optimization** | Retarget the built pushback engine at deals; auto-apply behind the existing approval gate |
| **Agency white-labeling of the curation desk** | Foundation exists (`tenants`, `partner_branding`, satellite Clerk). Mostly wiring + the leak test |
| **Additional SSPs** — Index Exchange, OpenX, FreeWheel, Xandr | Adapter interface makes each additive. Note Advanced Curation already has IX + OpenX |
| **`pgam_direct` as a provider** | §19.5 — the structural escape from vendor entitlements |
| Advanced audience / data marketplace | Depends on Phase 2 audience outcome |
| Agency API access | Public API + keys + quotas; needs rate limiting (seam `A4`) fixed first |
| Attention-curated deals as a named product | **Pull this forward if possible.** It is the single most defensible thing in the plan and needs no vendor API |

### 18.1 One reordering I'd argue for

The brief puts **AI Deal Builder** in Phase 2 and **Deal Health** in Phase 3. I'd
consider swapping them. Deal Health is where PGAM's measurement assets create
value Advanced Curation cannot match, it reuses an engine that already exists, and
it is the answer to "why not just use the free tool?" The AI builder is
impressive in a demo but it is also the feature both SSPs have already shipped in
their own buyer UIs — so it is the least differentiated thing on the list.

---

## 19. Deliverable 19 — Risks and blockers

### 19.1 Critical

| # | Risk | Impact | Mitigation |
|---|---|---|---|
| R1 | **No deal-creation API on either SSP today.** The product's core promise cannot be automated | Existential for the "self-service" claim | M1, M2, P1, P2 as week-1 commercial actions. `ManualFulfilmentAdapter` ships value meanwhile. **Do not commit a launch date until the probes return** |
| R2 | **PGAM's Magnite seat is demand-side; curation is sell-side.** May require a new commercial relationship, not a config change | Magnite could slip a quarter+ | Lead with PubMatic (§8.4). Treat Magnite as manual-fulfilment in Phase 1 |
| R3 | **PubMatic curator API is one confirmed endpoint and a session token.** OAuth scope was unresolved as of the code being written; write endpoints unknown | The "better" path may also be blocked | P1/P2 immediately. If PubMatic has no curator create endpoint, both SSPs are manual and the whole plan rests on `pgam_direct` (R11) |
| R4 | **Marketplace deal IDs are placeholders.** `DEAL-CL-SPT-001` etc. are not real | A buyer activating a marketplace pack today gets a non-functional deal | Enforce in code: an item with empty `seed_deal_ids` cannot be activated. Do not ship the marketplace as "ready" until backfilled |
| R5 | **Seam `V1` — `iab_categories` + `advertiser_domain` never sent**, and both are *required to serve* since Bryan's May 12–13 change | Deals get created that cannot deliver — the worst possible failure mode | Fix in Phase 1, item 19. Non-negotiable |
| R6 | **No CI on 47 test files; no observability.** 238 `console.*` across 269 routes; `metrics.ts` unwired | A silent deal-creation failure is invisible until an agency complains | Phase 1 items 1–2. Brief §15 demands this anyway |

### 19.2 High

| # | Risk | Mitigation |
|---|---|---|
| R7 | **Multi-tenant leak.** Deal configs are competitive intel; Deal IDs are bearer tokens for inventory | Denormalized `tenant_id`, `enforceTenantOrForbid` everywhere, the seeded cross-tenant leak test as a CI gate |
| R8 | **Margin leakage to agencies.** Curation economics are the business model | Server-side field omission + a `assertCurationSafe` guard + unit tests, mirroring `assertClientPortalSafe` |
| R9 | **Audience differentiation may not be technically deliverable** on Magnite/PubMatic curated deals for anything beyond contextual | Qualify M4/P5 **before** promising audience layering to any agency. Do not put it in a pitch deck yet |
| R10 | **Migration substrate.** DDL in 8 places, no applied-state tracking, 6 duplicate migration numbers | Fix before adding 10 tables |
| R11 | **`pgam-direct` is not runnable end-to-end.** The strategic escape hatch is a Phase-1 scaffold | Don't plan Phase 1 or 2 around it. Do fund it — it is the only path that removes vendor dependency permanently |
| R12 | **Competitive timing.** Advanced Curation shipped 2026-03-24, free, 4 SSPs. PubMatic and Magnite both ship AI curation in their own UIs | Compete on measurement + attention + white-label (§4.4), not on creation speed |

### 19.3 Medium

| # | Risk | Mitigation |
|---|---|---|
| R13 | Advertiser identity fragmentation (3 tables, 2 ID forms, seam `D1`) | Decide canonically in Phase 1 |
| R14 | SpringServe quirk surface leaking into curation | Normalize at the adapter boundary; `springserve-quirks.md` is the test-case list |
| R15 | SpringServe v0/v1 trap — v1 list endpoints silently return `[]` for the pubops token | Documented; adapter must pin v0. Add a test |
| R16 | Deal ID uniqueness per DSP (`[VENDOR-DOC]`: Magnite Deal IDs are unique per DSP across the MCTV Exchange) — implies re-creation per DSP, not reuse | Confirm in M1. Affects the "copy to another DSP" feature's cost model |
| R17 | Rate limiting on one of 269 routes (seam `A4`) | Add to curation routes at minimum |
| R18 | Invoca webhook unverified (seam `F1`) | Fix before copying the webhook pattern |
| R19 | Vibe X-API-KEY sunsets **2027-01-01** | Unrelated but a hard deadline competing for the same engineering time |
| R20 | ZIP validation — stored self-serve ZIP lists contain invalid values, causing 422s on Vibe | Same validator needed in curation; open ticket #1 |

### 19.4 What cannot currently be automated

Direct answer to the brief's closing requirement. Each of these is blocked by the
vendor, not by PGAM:

| Capability | Magnite | PubMatic | Evidence |
|---|---|---|---|
| **Create a deal** | ❌ manual in ClearLine console (~2 min, by hand) | ❌ no create function in either client | `springserve-quirks.md:338`; `activate/README.md`; `pubmatic_activate.py` |
| **Attach a string deal to a line item by API** | ❌ `DealIdList` writes rejected account-wide | n/a | `springserve-quirks.md:57-67` |
| **Programmatic forecast** | ❌ POST 422 "Access token is missing" | ❓ unknown | `forecasting/sources/magnite.ts:31-40`; probe P6 |
| **Sync a non-contextual audience into a deal** | ❌ only via `domain_list_ids` transport | ❓ unknown | `magnite-field-coverage.md`; probe P5 |
| **Inventory catalog / discovery** | ❌ no catalog API in use; publishers hand-typed | ❓ unknown | `clearline-packs.ts`; probes M5/M6 |
| **Pod-position bid multipliers** | ❌ *"SS has no first-class API for per-pod bid shading"* | n/a | `clearline-packs/[id]/activate/route.ts` |
| **Per-creative weighting** | ❌ read-only; set in Magnite UI | n/a | `magnite-field-coverage.md` |
| **Set DSP + buyer seat on a deal** | ❓ unknown | ❓ UI-confirmed, API unknown | probe P4 |
| **Auth at all (DSP-repo client)** | n/a | ❌ `AUTHZ_FAILED` — account lacks Developer Integrations scope | `pubmatic-auth.ts` |

Also worth stating plainly: **PGAM currently has no credentials for the one Magnite
surface that documents a deal-creation endpoint** (`api.tremorhub.com`). It has
never been called from any repo.

### 19.5 The strategic option nobody has costed: `pgam_direct`

`pgam-direct/migrations/postgres/000023_dsp_deal_subscriptions.up.sql` is
titled *"Curated marketplace foundation"* and already contains:

- A real `pgam_direct.deals` table with `deal_id`, `floor_cpm_usd`, `deal_type`,
  `media_types[]`, `device_types[]`, `geo_countries[]`, **`min_attention`**,
  `allowed_dsp_ids[]`, `tenant_id`.
- Three seeded **attention-conditioned deals** — `pgam-attn-ctv-prem` (attn ≥
  0.75, $8.00 floor), `pgam-attn-olv-hi` (≥ 0.65, $4.50), `pgam-attn-display-quality`
  (≥ 0.55, $1.20).
- A `dsp_deal_subscriptions` table implementing **buyer self-subscription** to
  deals, with the runtime allowlist as `ops-approved ∪ self-subscribed`.

That is a curation product's data model, already designed, in PGAM's own SSP.

**Implication:** on `pgam_direct`, PGAM can mint Deal IDs with **zero vendor
entitlement**, price them freely, and define them by measured attention — the one
thing no competitor can copy. The blocker is that `pgam-direct` is *"not yet
runnable end-to-end."*

This should be an explicit strategic decision, not an accident of sequencing.
Magnite and PubMatic give **reach**; `pgam_direct` gives **control and margin**.
The long-run answer is almost certainly both, and the audit's honest read is that
`pgam_direct` deserves more weight in the plan than the brief gives it (it isn't
mentioned).

### 19.6 The probes to run first — settle the roadmap in a day

None of these can run in this session (no credentials, and vendor doc domains are
egress-blocked). Each is small and each removes a large unknown.

| # | Probe | Answers |
|---|---|---|
| **V1** | With a real `.env`: `python -m core.pubmatic_activate config` then `org`, `advertisers`, `deals 26428` | Does the curator seat authenticate today, and which of the 5 inferred endpoints are real? Deletes the guesswork in §8.1 |
| **V2** | Obtain the **PubMatic curator API reference** from PubMatic (P2). Search it for a deal POST | Is programmatic PubMatic deal creation possible at all? **The single most important question in this audit** |
| **V3** | Obtain the **Magnite Streaming / MCTV API docs** (`help.magnite.com`, blocked here) and confirm `POST /v1/resources/seats/{seatId}/deals` — request scope, auth, seat ID | Is programmatic Magnite deal creation possible, and on what seat? |
| **V4** | Re-run the ClearLine forecast POST after filing M3 | Unblocks Tier-1 forecasting |
| **V5** | Ask SpringServe support for status on the `DealIdList` account enable (M2) | Unblocks API deal-attach |
| **V6** | **Sign up for Advanced Curation** (free) and screenshot the real flow end to end | Replaces §4's press-release-based analysis with fact |
| **V7** | Measure current manual fulfilment latency across the last 20 deals | Makes the ETA in §5.3 honest, and quantifies what automation is worth |

**Recommendation: do not commit engineering to a Phase 1 date until V1, V2 and V3
are answered.** They determine whether Phase 1 ships an automated product or an
instrumented manual one — and that is a materially different promise to an agency.

---

## 20. Deliverable 20 — Recommended implementation sequence

### 20.1 Two tracks, in parallel

**Track A — commercial/entitlement (Priyesh, starts immediately, ~0 eng time)**

Week 1: P1 (enable Developer Integrations on org 17496) · P2 (curator API
reference) · M1 (curation seat question) · M2 (SpringServe `DealIdList`) ·
M3 (ClearLine forecast POST) · V6 (sign up for Advanced Curation) ·
V7 (measure manual latency).
Week 2–4: P4, P5, P6, M4, M5, M6 · legal review of audience sync.

Track A gates nothing in Track B's first four weeks. That is the point.

**Track B — engineering (starts immediately, never blocked)**

| Step | Work | Why here |
|---|---|---|
| **0** | Migration runner + duplicate-number guard + CI on the 47 existing tests | Do not add 10 tables to a substrate with 6 duplicate migration numbers and no CI |
| **1** | Canonical advertiser identity decision (seam `D1`) | Cheapest now, most expensive later |
| **2** | Curation schema (§9) | Everything else depends on it |
| **3** | Normalized objects + `CurationProvider` + `capabilities` | The seam that makes every later SSP additive. Must precede the second provider |
| **4** | `ManualFulfilmentAdapter` + ops queue + SLA + **latency instrumentation** | Ships real value on day one and produces the data for honest ETAs |
| **5** | Fix seam `V1` (`iab_categories`, `advertiser_domain`) | Deals that cannot serve are worse than no deals |
| **6** | Deal builder UI + validation + Tier-3 forecast | The buyer-visible product |
| **7** | DSP directory + saved seats | Small, unblocked, needed by submit |
| **8** | Submit pipeline: state machine, idempotency, retry, dead-letter, notify | Brief §15. Deal creation must not silently fail |
| **9** | Deal Library + lifecycle actions | Brief §7 |
| **10** | Admin console: queue, packages, approvals, retry, routing, provider health | Replaces `/admin/deal-ids` + `/admin/deal-sync` + source edits |
| **11** | Pricing rules + `computeBuyerPrice` + visibility guard + tests | Brief §13 |
| **12** | RBAC `curation` + agency roles + **cross-tenant leak test in CI** | Before any external agency user exists |
| **13** | Deal-scoped reporting + widened vendor CHECK + package rollup | Brief §10 substrate |
| **14** | `MagniteAdapter` (read + attach + manual create) · `PubMaticAdapter` (read, live if P1 landed) | Real providers behind the finished interface |
| — | **← Phase 1 complete. Product is live, self-service, honest about latency.** | |
| **15** | Flip `supportsProgrammaticCreate` per provider as Track A lands | **One flag per provider. No UX change, no migration** |
| **16** | Tier-2 historical forecasting | Highest-value unblocked forecasting work |
| **17** | AI deal builder (grounded tool-use) | Unblocked |
| **18** | Multi-SSP deployment UX | Needs ≥2 providers with create |
| **19** | Marketplace real deal IDs + templates | Needs create or ops backfill |
| **20** | **Deal Health** (retarget pushback engine) | The differentiator; consider pulling before 17 |
| **21** | White-label curation branding | Foundation already exists |
| **22** | `pgam_direct` as a provider · additional SSPs · agency API | Phase 3 |

### 20.2 The one-sentence version

> Build the abstraction layer and the buyer product first, ship it with a
> human-in-the-loop fulfilment adapter that is instrumented from day one, chase
> the SSP write entitlements on a parallel commercial track, and flip one
> capability flag per provider as each lands — while putting the product's
> differentiation on attention, audiences, reporting and white-label, none of
> which need a vendor's permission.

### 20.3 Full classification summary

| Feature area | Already Exists | Extend Existing | Build New | Requires External API |
|---|---|---|---|---|
| Auth / accounts / RBAC | ✅ | curation feature + agency roles | — | — |
| Multi-tenant / white-label | ✅ | curation branding | — | — |
| Approval rules | ✅ | curation thresholds | — | — |
| Pricing / margin | ✅ | forward price + rule precedence | — | — |
| Billing / wallet | ✅ | — | — | — |
| Campaign wizard | ✅ | deal builder reuses parts | — | — |
| Targeting (geo/device/daypart/freq) | ✅ | tag-level scope fixes | — | — |
| Inventory lists | ✅ | deal-scoped lists | — | discovery API |
| Audiences — catalog | ✅ | curation picker | — | — |
| Audiences — SSP sync | — | contextual via domain lists | — | **✅ blocked** |
| Attention layer | ✅ | attention-curated deals | — | — |
| Forecasting — estimate | ✅ | Tier-2 historical | — | — |
| Forecasting — SSP live | client coded | — | — | **✅ blocked** |
| Reporting substrate | ✅ | deal dimension + vendor CHECK | — | some metrics |
| Deal Health | engine exists | retarget at deals | — | — |
| AI planner | ✅ | grounded tool-use for curation | — | — |
| **Deal entity / package model** | — | — | **✅** | — |
| **Deal Library** | — | saved views | **✅** | — |
| **Deal builder UI** | — | wizard parts | **✅** | — |
| **Provider adapter layer** | — | vendor modules | **✅** | — |
| **Manual fulfilment adapter + queue** | — | — | **✅** | — |
| **Deal creation** | — | — | interface | **✅ blocked** |
| **Deal ID retrieval** | — | — | interface | **✅ blocked** |
| DSP / seat directory | — | — | **✅** | seat activation unknown |
| Marketplace | packs UI | DB-backed catalog | — | real deal IDs |
| Templates | — | — | **✅** | — |
| Status machine / reliability | — | — | **✅** | — |
| CI / observability | — | — | **✅** | — |

---

## Appendix A — Key evidence index

| Claim | File |
|---|---|
| Deal creation is manual; platform never creates deals | `pgam-dsp-dashboard/docs/springserve-quirks.md:338` |
| `DealIdList` writes rejected account-wide (0/25 tags) | `docs/springserve-quirks.md:40-91` |
| Deals created by hand in ClearLine console (~2 min) | `docs/magnite-field-coverage.md` §Programmatic Supply |
| Current manual Deal-ID workflow incl. source edit + redeploy | `src/app/admin/deal-sync/page.tsx:1-20` |
| ClearLine forecast POST 422 "Access token is missing" | `src/lib/forecasting/sources/magnite.ts:31-40` |
| `iab_categories` / `advertiser_domain` never sent; required to serve | `SCHEMA.md` seam `V1`; `docs/magnite-field-coverage.md` |
| PubMatic `AUTHZ_FAILED` — account lacks API scope | `src/lib/pubmatic-auth.ts` |
| Activate scaffold inert; write side out of scope | `src/lib/vendors/activate/README.md`; `SCHEMA.md` |
| PubMatic curation seat `PGAM_Activate_US` org 17496 + live advertisers | `pgam-intelligence/core/pubmatic_activate.py` docstring |
| Only 1 confirmed Activate endpoint; rest inferred; no writes; OAuth scope unresolved | same file |
| Activate client has zero callers | `grep -rn pubmatic_activate` → only self |
| Marketplace deal IDs are mock | `src/lib/clearline-packs.ts:82` |
| Audience activation is a ledger + manual backfill | `src/app/api/v1/audiences/oo/[slug]/activate/route.ts:14-17` |
| Audiences reach Magnite as domain lists | `docs/magnite-field-coverage.md` §Targeting (line-item) |
| Margin waterfall + per-SSP fee pcts | `src/lib/margin.ts` |
| Multi-tenant white-label foundation | migrations `0093`–`0095`, `0117`, `0118`; `src/lib/tenant/*` |
| Approval rules (none/manual/auto_below_threshold) | `src/lib/tenant/approval.ts` |
| Per-feature RBAC + hard constraints | `src/lib/rbac.ts` |
| Rollups vendor-keyed; CHECK limits to clearline/activate | `migrations/0016_keystone_reporting_rollups.sql` |
| Client-portal margin-safety pattern | `docs/platform-state-2026-08.md` §honesty gates |
| No CI; DDL in 8 places; duplicate migration numbers | `SCHEMA.md` §Tests, §Schema-creation surfaces |
| `pgam_direct` deals + attention cohorts + buyer self-subscription | `pgam-direct/migrations/postgres/000023_dsp_deal_subscriptions.up.sql` |
| `pgam-direct` not runnable end-to-end | `pgam-direct/README.md` |
| Pod-position bid shading has no SS API | `src/app/api/v1/inventory/clearline-packs/[id]/activate/route.ts` |

## Appendix B — External sources

Vendor and competitor claims are marked `[VENDOR-DOC, UNVERIFIED]` in the text.
`advancedcuration.com`, `help.magnite.com` and PubMatic community docs were
**blocked by this environment's egress proxy**; those claims come from vendor
press material and search results and must be confirmed via §19.6.

- Advanced Curation launch coverage (launched 2026-03-24; Magnite, Index Exchange,
  PubMatic, OpenX; brief-to-live under five minutes; no fees/contracts/minimums)
- Magnite — *To Unify Curation and Activation Within ClearLine* (buyers and
  curators define deal terms, pricing and targeting directly; Magnite Access for
  audience packaging)
- Magnite — *Launches Deal Discovery* (curate open-market inventory into PMP deals)
- Magnite Streaming Public API — seat-scoped deal endpoint
  `POST api.tremorhub.com/v1/resources/seats/{seatId}/deals`
- PubMatic — *Gen AI-Powered Buyer Platform* / *Deal Management Agent* (NL prompt
  curation; activation into 125+ DSPs by seat ID)
- PubMatic — PMP Deal APIs (documented "for Publishers"; curator availability
  unconfirmed)

---

# Addendum — "Attentive Buying" as the product package

Added after the audit, in response to the decision to package this as
**Attentive Buying — buying PMPs with attention signals** rather than as a
generic curation desk.

## A21.1 This is the right call, and it changes the sequencing

The audit's §4.4 argued PGAM should not compete on deal-creation speed and should
put its weight on attention. Naming the product after that is stronger than
treating attention as a feature inside "PGAM Curation," for three reasons:

1. **It is the only differentiator that needs no vendor permission.** Everything
   blocked in §19.4 is deal *creation*. Nothing about attention-based curation
   requires Magnite or PubMatic to grant PGAM anything.
2. **It inverts the competitive problem.** As "a curation desk," PGAM is a late,
   single-SSP entrant against a free four-SSP incumbent. As "attentive buying,"
   Advanced Curation is not a competitor at all — they have no attention data, and
   cannot get it without impression-level access they do not have.
3. **It makes the roadmap honest.** A generic curation desk whose deal creation is
   manual is a worse version of a shipping product. An attentive-buying product
   whose deal creation is manual in v1 is still the only one of its kind.

**Sequencing consequence:** the audit put attention-curated deals in Phase 3 and
flagged "consider pulling forward." Packaging it as the product settles that —
**attention moves into Phase 1 as the reason the product exists**, not a later
enhancement.

## A21.2 The mechanism already exists

The key finding: **attention can already be turned into deal inventory today, on
transport PGAM already owns, with zero vendor entitlement.**

`POST /api/v1/audiences/attention-materialize` does precisely this:

> Turns an Attention tier + scope into a SpringServe DomainList and (optionally)
> records the mapping against a campaign so the wizard / buyer agent can attach
> it via `domain_list_ids`.

Tiers are `PRIORITIZE` / `MONITOR` / `SUPPRESS`, over a configurable lookback
(default 30d, max 90) with a `minImpressions` floor (default 500), lineage-tagged
`attention_prioritize` / `attention_suppress`, persisted to
`campaign_domain_lists`. Implementation in
`src/lib/keystone/attention-materializer.ts` + `src/lib/springserve/domain-lists.ts`.

So the chain is complete and shipping-capable:

```
CTV impressions → impression_attention_score → ctv_placement_attention_v1
      → calibration (attention_v1_calibrations)
      → tier the placements (PRIORITIZE / MONITOR / SUPPRESS)
      → materialize to a DomainList
      → that list IS the deal's inventory definition
```

This is the same transport `docs/magnite-field-coverage.md` says audiences already
ride on (`domain_list_ids`) — which was a *limitation* for demographic audiences
(§3.4) but is **exactly the right shape for attention**, because attention is a
property of placements, not of people. The constraint that hurts audience
targeting does not hurt this product at all.

## A21.3 The three-tier product ladder

Package Attentive Buying as three levels of increasing strength, which map
cleanly onto what PGAM can actually deliver and when.

| Tier | Name | What it means | Enforcement point | Ships |
|---|---|---|---|---|
| **1** | **Attention-Curated** | Deal inventory is selected by *measured* placement attention. The allowlist is the product | Pre-bid, via the deal's inventory definition | **Now — no vendor dependency** |
| **2** | **Attention-Verified** | Attention delivered back per deal as an outcome metric, alongside completion and VCR. The buyer can see whether the deal earned attention | Post-delivery reporting | **Now** — rollups already carry `attention_score`, `attention_bucket`, `attention_sample_size` |
| **3** | **Attention-Enforced** | `min_attention` is a hard condition on the deal, enforced in the auction. Inventory below threshold cannot win | In-auction | **`pgam_direct` only** — already a first-class column with three seeded cohorts |

Tier 3 is the reason `pgam-direct` matters strategically (§19.5) and worth
restating: `pgam_direct.deals.min_attention` with seeded cohorts at ≥0.75 / ≥0.65
/ ≥0.55 is a product **no competitor and neither SSP can offer**, because it
requires owning the auction. On Magnite and PubMatic, PGAM can curate and verify
attention; only on its own SSP can it *guarantee* it.

That is a genuinely differentiated three-step commercial story, and each step is
independently sellable.

## A21.4 Honest limits — say these out loud before they reach a pitch deck

| Limit | Detail |
|---|---|
| **Attention is measured post-bid, not targeted pre-bid** | Scores come from delivered impressions (VAST events → Athena → Neon). Tier 1 curation works because *placements* persist — a placement that earned attention last month probably will next month. It is **not** per-impression attention bidding inside someone else's deal. Do not imply it is |
| **CTV only** | Attention engine v1 is a CTV spec. OLV/display/in-app attention is **not** built. Attentive Buying is a CTV product on day one — which is fine, it is the highest-CPM inventory, but the marketing must not over-claim |
| **Cold-start** | A placement with no PGAM delivery history has no score. The `minImpressions` floor (default 500) means new inventory is unscored, so early deals lean on the placements PGAM has already run. Scale grows with delivery |
| **Attention-qualified *audiences* are gated** | `docs/features/attention-qualified-audiences.md` names "PMP deal" as an activation destination — but states the household resolver (W1-04) **"was never built,"** so `resolved_household_id` is a nullable pass-through and **no audience can be built yet.** Slice 1 in progress. AQA is Tier-2-plus and depends on that resolver, not on any vendor |
| **`attention_scores` table is read but not written** | Seam `D2`: read by production routes, written only by `seed-demo.ts`. Confirm which attention tables are live before wiring a *sellable* product to them |
| **Calibration is the credibility risk** | A 0–100 score sold as the basis of a media buy will be challenged by an agency's analytics team. `attention_v1_calibrations` exists; the methodology page (`/methodology`) exists. Have a defensible, written validation story before the first agency pitch |

## A21.5 What the buyer sees

Packaging, in buyer language, with PGAM's internals hidden:

| Buyer-facing | Backed by |
|---|---|
| **Attentive Buying** (the product) | the curation engine in §6 |
| "Attention-Curated CTV" deal type | `PRIORITIZE` tier materialized to the deal's inventory |
| **Attention Score** on every deal, pre- and post-flight | forecast estimate → delivered `attention_score` |
| "Suppress low-attention inventory" toggle | `SUPPRESS` tier as a blocklist |
| **Attention Lift** vs the buyer's business-as-usual | existing lift/holdout modules (`src/lib/keystone/{lift,holdout}`) |
| Marketplace: **PGAM Attention** category — High Attention CTV, High Completion Video, Premium Attention Publishers | `curation_marketplace_items` with `est_attention` (already in the §9.2 schema) |
| **Attention premium** in the price | `attention_premium_pct` — already in the §13 pricing rules |

Note that the pricing engine already has `attention_premium_pct` and the
marketplace schema already has `est_attention`. The monetization hook for
Attentive Buying is **already designed**; it needs populating, not building.

## A21.6 Revised Phase 1 — the Attentive Buying delta

Add to the §17.2 MVP list:

| # | Item | Class | Vendor-blocked? |
|---|---|---|---|
| 21 | **Attention tiering as a first-class deal inventory source** — wire `attention-materialize` into the deal builder as a selectable inventory type, not just a campaign-level helper | **[EE]** | **No** |
| 22 | **Attention Score on the deal object** — forecast estimate pre-flight, delivered score post-flight, on the package and per SSP child | **[EE]** | **No** |
| 23 | **"PGAM Attention" marketplace category** with real `est_attention` values and a stated basis | **[EE]** | needs real deal IDs |
| 24 | **Confirm the live attention tables** (resolve seam `D2`) before selling on them | **[EE]** | **No** |
| 25 | **Write the attention methodology one-pager** for agency diligence | **[BN]** | **No** |

Every one of those is unblocked. That is the point: **Attentive Buying is the part
of this product PGAM can ship without waiting for Magnite or PubMatic to answer a
single email.**

---

# Addendum B — corrected integration picture (reporting repos audited)

**This supersedes §0.3, §7 and §8 where they conflict.** Those sections were
written from `pgam-dsp-dashboard` and `pgam-intelligence` only. Auditing
`pgam-recon` and `pgam-direct/jobs/report-fetchers` changes the picture
materially — and the correction cuts both ways.

## B1. What I got wrong

I said PGAM had no meaningful curation-seat integration. **That was wrong.**
PGAM has **two** PubMatic curation-side seats, one with live revenue and a
contracted take rate, and **deal-level read access that works today**.

| Seat / account | Where | What it is | Status |
|---|---|---|---|
| PubMatic buyer **69397** | `pgam-recon/pgam_recon/fetchers/pubmatic_pmp.py` | **PMP / curation buyer account with live deals and real spend.** P&L wiring: *"Gross = SUM(spend) across all active deals on buyer 69397; Net = Gross × 25.5% (PGAM's contracted take rate on PMP)"* | **Live, revenue-generating** |
| PubMatic Activate **17496** (`PGAM_Activate_US`) | `pgam-intelligence/core/pubmatic_activate.py` | Activate curation seat, advertisers incl. Amazon, JP Morgan, IHG | Live seat, client unwired |
| PubMatic publishers 162623 / 165708 / 166643 | `pgam-recon/.../pubmatic.py` | Sell-side accounts | OAuth API works |
| Magnite DV+ | `pgam-recon/.../magnite.py` + `magnite_refresh.py` | Performance Analytics, partner-level revenue | **Working** via UI token |
| Magnite `api.magnite.com` | `pgam-direct/.../adapters/magnite.py` | OAuth2 client-credentials, `reporting/v1/queries`, secret at `pgam/dsp/magnite/auth` | **Working** |

So PGAM is **already in the curation business commercially.** It is not a
hypothesis. That is a stronger starting position than §0 credited, and it
changes the pitch: this product productizes an existing revenue line rather
than entering a new one.

## B2. What the correction does *not* change

**There is still no write path to create a deal on either SSP** — supported or
unsupported. Every integration found across all four repos is read-only
reporting. No `POST`/`PUT` that creates a deal exists anywhere.

## B3. The real shape of the access — and why it matters

The genuinely important finding is not "read access exists," it is **how**. The
pattern is identical across both vendors and both curation-side accounts:

> **Official OAuth APIs serve the publisher / demand-partner direction.
> The curation / buyer-side surfaces are only reachable by headless-browser
> session-token scraping of internal SPA APIs.**

Both vendors' official buyer-side APIs were tested and found not to work:

**Magnite** — `pgam_recon/fetchers/magnite.py` documents the attempt:

> Magnite's public docs describe a 3-step offline-report REST API on
> `api.rubiconproject.com/analytics/v2/default` … We spent an afternoon on
> 2026-04-21/22 proving that path is gated at the **account** level, not the key
> level: three separate Key/Secret pairs (incl. a freshly minted one) all
> gateway-parsed fine … but returned 401 "Invalid or expired access_token"
> specifically against the `analytics` realm. Their own AM escalation is still
> open; **no code change on our end unblocks it.**

What works instead: `performance-analytics-reporting-service.magnite.com/report/v2/analytics/default`
with the **UI session `access_token` as a URL query param** (~30–60 min sliding
TTL), refreshed by Playwright headless login to `apps.rubiconproject.com`
(`magnite_refresh.py`).

**PubMatic** — `pubmatic_pmp.py`:

> account 69397 is a *buyer* account managed in the apps.pubmatic.com UI, and the
> buyer-side analytics endpoint on api.pubmatic.com **returns zero rows** for our
> user 37,770 across every date range we've tried.

What works instead: Playwright login via Okta → poll `sessionStorage` for
`apiAuthValue` → replay `POST apps.pubmatic.com/api/pmp/deals/reportingSearch`
with that token in a `pubtoken` header. Note this is the same auth mode
`core/pubmatic_activate.py` fell back to on the Activate seat — **three
independent instances of the same conclusion.**

This is a coherent, consistent finding, and it reframes the vendor conversation
(§B6).

## B4. The Phase 1 unlock I under-credited

`reportingSearch` is already called with a rich filter set:

```
filters: [ "status eq 1,status eq 2,status eq 4,status eq 11",
           "dealCategory eq 1,dealCategory eq 2,dealCategory eq 3",
           "channelType eq 0,channelType eq 1,channelType eq 5,channelType eq 6",
           "loggedInOwnerId eq 69397", "loggedInOwnerTypeId eq 7" ]
sort: "-revenue"   ·   pageSize: 9999   ·   fromDate / toDate honoured
```

That returns **per-deal rows with revenue, status, category and channel, date-bounded.**
Which means:

- **The Deal Library can be populated with real deals and real spend in Phase 1**,
  not just with deals the new product creates. Existing PMP deals become the
  product's day-one inventory. That is a much better launch than an empty table.
- **Deal Health has a real input immediately.** The request body carries a
  `dealHealthScore` field — PubMatic already computes one. Worth probing what it
  returns before building a competing score from scratch.
- **Per-deal spend reconciliation exists**, so the margin model in §B5 can be
  validated against booked revenue rather than estimated.

Add to Phase 1 (§17.2): **import existing PMP deals into the Deal Library
read-only**, clearly badged as *externally created*, so the library is useful
from launch and the two populations never get confused.

## B5. Margin model — corrected to the floor-spread model

§13 modelled margin as a markup on estimated supply cost. The actual model is a
**spread between the buyer's floor and the SSP's price**:

```
Buyer floor CPM              ← what the agency tells us they'll pay
  − SSP clearing price CPM   ← what Magnite / PubMatic actually charge
  − SSP fee / take rate      ← e.g. PubMatic PMP 25.5% contracted take
  − data / audience cost     ← audience CPM, attention premium
  ─────────────────────────
  = PGAM margin CPM          ← the spread. Never shown to the agency.
```

Three consequences the builder UI must handle:

1. **Viability is a build-time check, not a post-hoc calculation.** If
   `buyer_floor < ssp_price + fees + min_margin`, the deal cannot clear at a
   profit. This must surface **while the buyer is typing the floor**, as a
   warning with a concrete suggestion ("at $18 this deal won't clear premium CTV
   — $24 is the lowest floor we'd recommend"), never as a submit-time rejection
   and never as a silent loss.
2. **The floor field is the most commercially sensitive input in the product.**
   It is the one number that sets PGAM's margin. It deserves a live, private
   margin readout for operators (`curation.pricing`) and *no* corresponding slot
   in the agency view.
3. **The 25.5% PMP take rate is real and contracted** — it belongs in
   `curation_pricing_rules` as a `provider='pubmatic'` row, not as an env default.
   Magnite's equivalent (~17–21% commission, per `magnite.py`'s note on
   `seller_net_revenue`) should be confirmed and recorded the same way.

Schema change to §9.2 — add to `curation_deals`:

```sql
ALTER TABLE curation_deals
  ADD COLUMN buyer_floor_cpm_cents      INT,   -- what the agency set
  ADD COLUMN ssp_price_cpm_cents        INT,   -- observed/quoted SSP clearing price
  ADD COLUMN ssp_take_rate_pct          NUMERIC(6,4),
  ADD COLUMN pgam_margin_cpm_cents      INT,   -- the spread. Operator-only.
  ADD COLUMN margin_viable              BOOLEAN,
  ADD COLUMN margin_computed_at         TIMESTAMPTZ;
```

`pgam_margin_cpm_cents` must be excluded from every tenant-scoped serializer —
add it by name to the `assertCurationSafe` guard (§13.3) and unit-test the
omission.

## B6. The vendor ask is now far more credible

This is the practical payoff. §7/§8's asks were open questions
(*"is there a curator API?"*). PGAM can now ask something much harder to
deflect, because it can name the exact endpoints it already depends on:

**To PubMatic:** *"We run PMP buyer account 69397 and Activate seat 17496. We
currently read our own deal data by driving your UI with Playwright and lifting
`apiAuthValue` out of `sessionStorage` to call
`POST /api/pmp/deals/reportingSearch`, because the OAuth buyer analytics endpoint
returns zero rows for user 37,770. That is fragile for both of us. Please (a)
enable OAuth access to the buyer-side reporting we already consume, and (b) tell
us whether deal **create/update** is available to a curator seat over OAuth — and
if so, give us the reference."*

**To Magnite:** *"Your DV+ analytics REST API at
`api.rubiconproject.com/analytics/v2/default` has been account-blocked for us
since April — three key pairs, 401 on the `analytics` realm, AM escalation still
open. We're reading Performance Analytics with a scraped UI `access_token`
instead. Separately, we hold working OAuth2 client-credentials on
`api.magnite.com`. Two questions: can the existing escalation be closed, and does
our `api.magnite.com` credential's scope extend — or can it be extended — to deal
creation on a curation seat (we're aware of
`POST /v1/resources/seats/{seatId}/deals` on the Streaming API)?"*

Those are specific, evidenced, and reference an escalation that is already open.

## B7. Do not build writes on scraped session tokens

One firm recommendation, because the temptation will be real once someone
notices reads work this way.

Reads degrade safely: `magnite.py` and `pubmatic_pmp.py` both fall back to
`mirror-fallback` and alert, and a missed day is stale reporting. **A write that
fails halfway leaves a real deal in a live marketplace in an unknown state, with
money attached** — duplicated, misconfigured, or created-but-unrecorded. Layer on
a ~30–60 min sliding token, an SPA whose payload shapes change without notice
(both files carry dated DevTools-capture comments), an explicit MFA/captcha
caveat, and undocumented internal endpoints, and it is not a foundation for
customer-facing deal creation.

**Use scraped-token access for reads only.** Route creation through
`ManualFulfilmentAdapter` (§17) until a supported write API exists. The adapter
interface means that decision costs nothing later: `supportsProgrammaticCreate`
flips per provider, and if PubMatic grants OAuth writes on the endpoints PGAM
already knows, the adapter is a small, well-understood change — because the
payload shapes are already mapped in `pubmatic_pmp.py`.

## B8. Revised probe list

Replaces §19.6 V1–V3:

| # | Probe | Answers |
|---|---|---|
| **B-V1** | Call `reportingSearch` against buyer 69397 and dump the **full item shape**, not just `revenue` — every field per deal | What deal metadata PGAM can already read: targeting? floor? DSP/seat? `dealHealthScore`? Determines how much of the Deal Library is free |
| **B-V2** | Send the §B6 ask to PubMatic | Whether OAuth read + any write exists for a curator seat |
| **B-V3** | Send the §B6 ask to Magnite; chase the open AM escalation | Whether the `analytics` block clears and whether `api.magnite.com` scope reaches deal creation |
| **B-V4** | Run `python -m core.pubmatic_activate config/org/advertisers/deals` on seat 17496 | Whether the Activate seat authenticates independently of the 69397 path |
| **B-V5** | Reconcile 69397 vs 17496 | Are these one commercial relationship or two? Which seat should the product create deals on? **Nobody has written this down** |

**B-V1 and B-V5 need no vendor cooperation and can run this week.** B-V5 in
particular is a gap in institutional knowledge, not a technical unknown.

---

# Addendum C — Capability audit of the two SSP read paths (2026-08-20)

Written after auditing the Magnite and PubMatic **read** surfaces in code
rather than from the integration notes. Refines §B3 and closes out two of the
five probes in §B8.

Supersedes nothing in Addendum B's commercial findings. It changes the
**capability table**, and it moves B-V1 and B-V5 from "probe to run" to
"answered by the next scheduled import".

## C1. There are TWO Magnite reporting surfaces, and the difference matters

§B3 treated Magnite reporting as one capability. It is two, with different
auth, different shapes, and — the load-bearing part — different **market
directions**.

| | `performance-analytics-reporting-service` | `api.magnite.com` |
|---|---|---|
| Read in | `pgam-recon/pgam_recon/fetchers/magnite.py` | `pgam-direct/jobs/report-fetchers/…/adapters/magnite.py` |
| Auth | UI session token, **scraped**, as a URL query param | **OAuth2** client credentials |
| Call shape | single GET | async: POST query → `query_id` → poll → GET CSV |
| Dimensions in use | `date,partner` | `date,publisher_id,country` |
| Metrics in use | `paid_impression, publisher_gross_revenue, ecpm, seller_net_revenue` | `impressions, spend_local, currency` |
| Direction | **demand-side** (`partner`) — the curation seat | **seller-side** (`publisher_id`) — PGAM as publisher |

The second surface is the better-engineered one: real OAuth, no scraped token,
an arbitrary dimension list. It is tempting to read that as "so ask it for a
deal dimension".

**That would be the wrong conclusion.** It is the *seller* direction. A deal
dimension there would report deals against PGAM's own publisher inventory,
which is a different population from the deals on the curation seat. The
curation seat is only reachable through the first surface — the one with the
scraped token and the known-hostile compatibility matrix.

### C1.1 The compatibility matrix is the real constraint

Magnite's 2024-11-03 "Field Compatibility" update means dimensions and metrics
are **not freely combinable**. Adding a dimension can invalidate the metric
set rather than just widen the result — `partner` plus any auction-side metric
returns `422 column_compatibility`. The recon fetcher's comment records this
as having "burned us once".

So on the curation-seat surface, "add a `deal` dimension" is not a safe
widening. It is a request that may return 422, and may return 422 *for the
metrics we currently depend on for the P&L*.

**Conclusion: a per-deal Magnite read is UNVERIFIED, which is not the same as
available, and is not the same as impossible.** It is one probe away — but the
probe must be run against a throwaway metric set, never by editing the live
P&L query.

## C2. PubMatic per-deal read is the one capability that is genuinely present

`reportingSearch` on buyer 69397 returns an `items[]` array of that seat's own
deals, and `pgam-recon` has consumed it in production for the P&L since
2026-05.

The gap is narrower than §B3 implied. It is not "can we read deals" — we
demonstrably can. It is that the fetcher reads `revenue` and **discards every
other key on every item**, so no code we have has ever looked at a deal's
identity fields.

Confirmed, because production depends on it:

- Envelope: `{ metaData: {…}, items: [ … ] }`
- `revenue` (float) and `strRevenue` (formatted string) per item
- `revenue: -1.0` is a **not-ready sentinel**, not a restatement
- `fromDate`/`toDate` must sit **inside** the `request` block; at top level
  they are silently ignored and the response is lifetime totals
- `requestType: "MONTHLY"` with a single-day range is the only combination
  that both honours the dates and returns per-day spend

Not confirmed: how an item spells its deal id, name, or status. Filterable
keys (`status`, `dealCategory`, `channelType`, `loggedInOwnerId`) tell us the
API *models* those as deal fields, but filterable is not the same as returned.

## C3. Corrected capability table

| Capability | Magnite | PubMatic | Basis |
|---|---|---|---|
| Create a deal | **No path** | **No path** | Unchanged from §B3 |
| Read aggregate spend | Yes | Yes | Both in production for the P&L |
| Read **per-deal** | **Unverified** | **Yes** | C1 / C2 |
| Read deal *metadata* (targeting, floor, DSP seat) | Unverified | **Unknown item shape** | C2 |
| OAuth (no scraped token) on the **curation** seat | No | No | C1, §B7 |

The last row is the one to keep in view: on both providers, the curation-seat
read runs on a **scraped session token**. §B7's conclusion stands unchanged —
do not build writes on that.

## C4. B-V1 and B-V5 are now answered by code, not by a probe

Both were listed in §B8 as needing no vendor cooperation. Neither now needs a
human to remember to run it.

**B-V1 — dump the full item shape.** `describeItemShape()` in
`src/lib/curation/discovery.ts` (pgam-dsp-dashboard) takes one saved
`reportingSearch` body — no credentials, no browser — and reports every key,
which logical field each resolved to, and which fields nothing matched. Point
it at one response and the shape question is closed.

It reports key names and **types, never values**. `0145` records that
`external_deal_id` "is effectively a bearer token for inventory access", and a
shape report is something that gets pasted into a ticket; including samples
would make it the quietest available way to leak the seat's deal list.

**B-V5 — are 69397 and 17496 one relationship or two.** Every discovered deal
is stored with the **seat it came from** (`curation_discovered_deals.source`,
migration `0147`). The same `external_deal_id` appearing under both sources is
the evidence they are one seat. The query is a one-liner and the index for it
exists.

### C4.1 Why identity is resolved rather than assumed

Guessing `dealId` would produce a **silent empty Deal Library against a live,
working endpoint** — a failure mode indistinguishable from "the seat has no
deals", and one that would be blamed on the entitlement rather than on us. So
each field has candidate spellings, every item reports which key matched, and
an item whose identity resolves to nothing is marked unidentified and **not
imported** rather than given a fabricated id. The count of dropped rows is
returned, not logged, for the same reason.

## C5. Revised probe list

Replaces §B8.

| # | Probe | Status |
|---|---|---|
| **B-V1** | Dump the full `reportingSearch` item shape | **Implemented** — `describeItemShape()`. Needs one saved response. |
| **B-V5** | Reconcile 69397 vs 17496 | **Implemented** — per-observation `source`. Needs one import from each seat. |
| **C-V1** | Ask the curation-seat Magnite surface for a `deal` dimension, against a **throwaway metric set** | Whether per-deal Magnite reads exist at all. Never run this by editing the live P&L query — the compatibility matrix can 422 the metrics recon depends on. |
| **B-V2** | Send the §B6 ask to PubMatic | Unchanged — OAuth read + any write on a curator seat |
| **B-V3** | Send the §B6 ask to Magnite; chase the open AM escalation | Unchanged |
| **B-V4** | Authenticate seat 17496 independently | Unchanged |

**The only input still needed from a person: one saved `reportingSearch`
response.** Everything downstream of it is built.

## C6. What was deliberately not built

- **No transport in the dashboard.** The Playwright login and `pubtoken` lift
  already work in `pgam-recon`. Reimplementing them in TypeScript would be a
  second system for a solved problem, and that fetcher feeds finance P&L — not
  something to modify as a side effect of a curation feature.
- **No mapping from PubMatic's status codes to our `DealStatus` union.**
  `reportingSearch` filters `status 1/2/4/11` as live-ish, but no code list is
  confirmed. Stored raw. A guessed lifecycle shown to a buyer is worse than a
  raw number shown to an operator.
- **No discovered deals in `curation_deals`.** They have no package, no buyer
  floor and no config. Landing them there would mean either making
  `package_id` nullable — breaking `0145`'s guarantee that every deal belongs
  to a buyer strategy — or inventing a package and therefore a floor. A
  fabricated floor feeding `pricing.ts` and producing a real-looking margin is
  the worst available outcome.
