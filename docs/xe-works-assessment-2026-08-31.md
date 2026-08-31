# xe.works — vendor assessment as a possible TB replacement

Written 2026-08-31 in response to "we are looking at replacing TB and
exploring xe.works".

## 0. Read this first — the evidence base is thin, by force

**I could not reach xe.works from this session.** The Claude cloud
environment's egress policy blocks `xe.works`, `prebid.org` and
`docs.prebid.org` outright (same class of block that stops a cloud session
reaching `api.pgammedia.com`, `docs/teqblaze-new-platform.md` intro). So every
claim about their product below is second-hand: search-result snippets of their
own marketing pages plus third-party listings (ZoomInfo, LinkedIn, LeaseWeb,
Prebid.org's managed-services directory, their GitHub org).

Treat every capability in §2 as **vendor marketing, unverified**. Nothing here
has been demonstrated, priced, or reference-checked. Someone should open
`xe.works/solutions` and `xe.works/terms-of-use` by hand before this document
is used to decide anything.

## 1. What xe.works appears to be

Founded 2016. Offices Kyiv (UA) and Vilnius (LT); customers claimed in US,
Europe, Israel, APAC. CEO listed as Dan Areshkovych. Headcount reported
10–49 depending on source; revenue reported $1–5M (ZoomInfo, low confidence).
LeaseWeb publishes them as an infrastructure customer.

They present with **two identities at once**, and this is the single most
important thing to pin down:

1. **A media business.** Described (ZoomInfo, and their own framing) as a
   "multi-directional media company that buys and sells digital advertising
   space for websites and mobile apps." They ship a Prebid.js bid adapter
   (`xe`) and are listed in Prebid.org's managed-services directory, contact
   `prebid@xe.works`. That is a company you plug in as a *partner*.
2. **A platform vendor.** Their current positioning is "not another ad
   management tool but infrastructure that replaces fragmented tools and
   integrations, giving direct control over demand, supply, and execution."
   That is a company you plug in as a *replacement for TB*.

Both can be true. But if PGAM's marketplace runs on their stack while their
media desk trades in the same market, they can see our demand pricing,
publisher payouts and margins. Teqblaze is a pure software vendor; that is a
structural difference and it should be priced, not hand-waved.

## 2. Claimed capabilities, and how they map onto TB

From their solutions page as surfaced in search:

| Claim | TB equivalent |
|---|---|
| oRTB, VAST, S2S, Prebid ecosystem; 50+ native adapters (Xandr, Magnite, PubMatic) | TB has oRTB in/out + demand-source management; **no Prebid/header-bidding leg** |
| Omnichannel desktop / mobile / CTV, all major formats, full PMP | broadly comparable |
| High-granularity targeting (geo, device, OS, sizes) with bulk upload | comparable (`set_supply_source_fields`, filter lists) |
| Custom reports, dozens of dimensions, 50+ metrics | TBX's reporting surface is already good (`teqblaze-new-platform.md` §2) |
| "Companies tab" — multiple partners, supply and demand connections, credentials, global settings | this is a multi-tenant white-label SSP/AdX console, i.e. **the same product category as TB** |
| "Real-time pricing engine that automatically balances bid shading and floor optimisation" | TB has `source.is_smart_floor` and a QPS tuner; **no bid shading** |

So on paper this is a **lateral move with two possible gains**: a header-bidding
/ Prebid leg PGAM does not have today, and a unified shading+floor engine.

### 2.1 The one gap that would actually justify a swap

`docs/optimization-cadence.md` §3 records the real structural limit of the
current stack: **Teqblaze cannot shape traffic.** Every control is binary or
near-binary — hard `qps_limit`, `status` toggle, routing change, geo blacklist.
There is no "send this DSP 30% of what it gets today." That is why the QPS
discipline has to be *cut, not throttle*, and why 41.5B requests (10.2% of all
QPS) returning $2,407 (0.67% of gross) can only be handled as a binary
disable — with a 2,800× GPM spread across setups and a blended $0.88/M.

If xe.works can shape traffic proportionally per demand partner, that is a
genuine capability gain rather than a logo swap. If their controls are binary
too, the swap buys us the Prebid leg and nothing else — and the Prebid leg is
available without a swap (§4, Option A).

## 3. What makes me cautious

1. **Scale, unevidenced.** 10–49 people and $1–5M revenue against a
   marketplace that ran ~405B bid requests for ~$357.5k gross in the measured
   window. Not disqualifying — Teqblaze is not huge either — but it needs
   sustained/peak single-tenant QPS numbers, not adjectives.
2. **No pricing, no migration references, no self-hosting evidence** anywhere
   public. No named customer who moved off another white-label SSP onto them.
3. **The open-source signal is thinner than it looks.**
   `github.com/xe-works/prebid-server-adapters` is a fork of upstream
   prebid-server (Go): 0 stars, 0 forks, 0 PRs, no visible activity of their
   own, 2,315 commits all inherited. Fine as a deployment artifact; not
   evidence of engineering depth.
4. **Conflict of interest** — §1, and it is the sharpest issue.
5. **Continuity risk.** Kyiv + Vilnius, wartime operations. The LeaseWeb case
   study is a positive signal on infra resilience but it is vendor marketing.
6. **Migration cost is large and it is ours, not theirs.** Repointing a base
   URL is not a migration. The TB dependency today:
   - ~4,400 LOC across `core/tb_api.py`, `core/tb_mgmt.py`, `core/tbx_api.py`,
     `core/tbx_mgmt.py` (plus `tb_data`, `tb_ledger`, `tb_unified`)
   - 8+ scheduler jobs (`tb_revenue_etl`, `tb_segments_etl`, `tb_hour_etl`,
     `tb_ad_format_etl`, `tbx_revenue_etl`, `tb_floor_nudge`,
     `tb_contract_floor_sentry`, `tbx_demand_geo_floor`, `tbx_auto_revert`)
   - two Neon table families, `pgam_direct.tb_daily_*` and `tbx_daily_*`
   - contract-floor minimums that do not carry across platforms
   - **the P&L's TB row** — `finance.daily_pnl_inputs.tb_gross_usd` /
     `tb_gross_profit_usd`, written by `pnl_sync.py` in `mastap150/pgam-recon`
     (`teqblaze-new-platform.md` §7.4). This is the company's profit reporting.

## 4. Timing — the strongest argument against a swap right now

PGAM is **already mid-migration**, TB → TBX, and that migration is not
finished. As of the notes in `docs/teqblaze-new-platform.md`: `TBX_EMAIL` /
`TBX_PASSWORD` are in neither secret store, the hourly `tbx_revenue_etl` has
been no-opping, both P&L switches (`PNL_TB_SOURCE`, `RECON_TB_SOURCE`) are
still on `legacy`, and the tranche-1 reconciliation — does TBX return the same
numbers as the legacy host — has not been run against real data.

Starting a *third* platform on top of that gives us three ID spaces, no
reconciliation baseline, and a live incentive to retire the legacy
`tb_daily_*` tables for the wrong reason. Those tables are the only
independent check on any successor's numbers we will ever have.

Finish the TBX reconciliation first. It is cheap, it is already built
(`scripts/tbx_recon.py`, `scripts/tbx_pnl_check.py`, the
`tbx-neon-reports.yml` workflow), and it answers a question that applies to
xe.works too: **are platform migrations at PGAM actually safe, and can we
detect it when they are not?**

## 5. What we could do — three options, cheapest first

### Option A — take them as demand, not as a platform *(recommended)*

They have a Prebid.js adapter and run S2S. Either add `xe` as a bidder on one
or two owned properties, or onboard them as an ordinary demand source in the
existing marketplace. Then measure them on GPM against the blended **$0.88/M**
using the bands already defined in `optimization-cadence.md` §3.

Cost: no change to `core/tb_*`, no ID remapping, no P&L exposure, reversible in
an afternoon. It tests the company on the only dimension that matters first —
**do they pay** — and if they cannot clear the bar as a partner, the platform
conversation is moot. A 21-day grace period applies, per the existing
safeguards.

### Option B — parallel marketplace on a carve-out

After tranche 1 of the TBX recon passes: run their platform on a handful of
placements, with their own IDs and their own reporting, and reconcile against
TB for 30 days the same way `tbx_recon.py` reconciles TBX. That produces the
one artifact a platform decision actually needs — two platforms, the same
inventory, comparable numbers — instead of a feature grid.

### Option C — full TB replacement *(not now)*

Only justified if all three hold:
1. they demonstrate proportional traffic shaping and/or real bid shading that
   TB structurally cannot do (§2.1);
2. they commit to an entity ID mapping/export and a dual-run period — the
   exact thing that bit us TB → TBX, where inventory IDs changed and
   publisher/demand IDs were never confirmed either way;
3. the pricing beats TB's take by enough to fund ~4,400 LOC of re-integration
   plus a re-verified P&L row.

## 6. Questions to put to them before any of the above

1. Are you a software vendor, a media partner, or both on the same account? If
   both, what contractually walls your media desk off from our pricing,
   publisher payouts and margins?
2. Can you shape traffic proportionally per demand partner — "send this DSP 30%
   of today's volume" — or are the controls binary like TB's?
3. Bid shading: does it operate on our floors, or on your take? Who sees the
   shaded price?
4. Deployment model: multi-tenant SaaS on your infra, dedicated instance, or
   self-hostable? Where does the data sit? Can we get log-level or
   ClickHouse-grade access — our whole ETL depends on report granularity.
5. Sustained and peak QPS for a single tenant today, and what we would be
   capped at.
6. API: is there an OpenAPI spec? A read-only credential role? Rate limits?
   Are entity IDs stable and exportable in bulk?
7. ads.txt / sellers.json / schain posture as seller of record; GVL ID and TCF
   handling; any HUMAN-equivalent IVT integration, and is that contract ours or
   yours? (PGAM holds HUMAN directly today — §3.6 of the TB doc.)
8. Three referenceable customers of comparable scale who migrated onto you off
   another white-label SSP.
9. Pricing shape: take rate vs. flat platform fee vs. per-request. Minimums,
   term, and exit terms — can we get our data out in bulk on exit?

## 7. Bottom line

A credible small vendor with real Prebid credentials and a plausible capability
gain on floors, shading and header bidding. Nothing I can see justifies
replacing TB, and the timing is actively wrong: we are mid-migration on the
platform we already have, with the reconciliation still unrun and the P&L still
on the legacy source.

Take them as demand first. It is cheap, reversible, and it answers the only
question that matters before any of the rest is worth asking.

## Sources

- https://xe.works/ and https://xe.works/solutions (**not read directly** —
  egress-blocked; via search snippets)
- https://prebid.org/managed_services/xeworks/ (egress-blocked; via snippets)
- https://docs.prebid.org/dev-docs/bidders/xe.html (egress-blocked)
- https://github.com/xe-works/prebid-server-adapters (read)
- https://www.zoominfo.com/c/xeworks/560512362, https://lt.linkedin.com/company/xe-works,
  https://theorg.com/org/xeworks, https://www.leaseweb.com/en/customers/xe-works
  (via snippets)
- Internal: `docs/teqblaze-new-platform.md` §1, §2, §3.6, §4, §7, §7.4;
  `docs/optimization-cadence.md` §3; `scheduler.py`; `core/tb_*`, `core/tbx_*`
