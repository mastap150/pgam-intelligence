# Teqblaze new platform — what the API gives us

Assessment of the OpenAPI spec Teqblaze supplied on onboarding
(`docs/api/teqblaze-openapi.json`, `Pgam v1.11.15`, 175 endpoints,
313 schemas), and what PGAM can build on it.

**Status: written from the spec, not yet exercised against a live account.**
Every capability below is what the spec documents; nothing here has been
confirmed against real data.

Two things block a Claude Code cloud session from verifying it directly:

1. **No credentials.** A fresh clone has none, by design.
2. **Egress policy.** The Claude environment's network policy denies *every*
   PGAM host — `api.pgammedia.com`, `ssp.pgammedia.com`, `admin.pgammedia.com`
   and `stats.ortb.net` all return a 403 CONNECT rejection from the agent
   proxy (verified 2026-08-19). Credentials alone would not help; a cloud
   session cannot reach the platform at all.

So verification runs on **GitHub Actions**, which has open network. One step
remains: **add the secrets** — Settings → Secrets and variables → Actions → New
repository secret: `TBX_EMAIL` and `TBX_PASSWORD`. (The workflow itself is on
`main` as of #99, so it appears in the Actions tab and is dispatchable; the
pre-merge push trigger it carried has been removed.)

Then: Actions → "TBX Data Pull (new Teqblaze platform)" → Run workflow. What to
create in the admin, and where each credential belongs, is §5.5.

The workflow is read-only. It publishes results three ways: the full
digest in the job log (which is how a session reads them back), a capability
matrix in the run summary, and full per-surface JSON as a 7-day artifact.

Locally, on a machine that has `.env` and network access:

    python3 scripts/tbx_probe.py --reports        # does each surface answer
    python3 scripts/tbx_pull.py --days 7 --outdir /tmp/tbx   # pull the data

---

## 1. Successor platform — same marketplace, different API

Corrected 2026-08-19 on Priyesh's account. An earlier draft of this doc called
the two hosts separate marketplaces; they are not. `ssp.pgammedia.com` is the
**old** Teqblaze platform PGAM was on, `api.pgammedia.com` is its successor,
and it is **the same underlying data**. Teqblaze confirmed this directly on
2026-08-20 (§8.1.10c): the data was fully transferred from the old ClickHouse to
the new one, so the new platform should return the same reports as the old. That
is now a vendor commitment rather than our inference — which does not remove the
need to check it, but it changes what a failed check *means*: a divergence is now
a bug to escalate, not an ambiguity for us to model around (§7, tranche 1).

What that changes: this is a **migration**, not a second book. The legacy
`tb_*` modules are a leg PGAM eventually steps off. The old host's shutdown is
**under PGAM's control** — Teqblaze confirmed (§8.1.10a) that the legacy UI and
ClickHouse stay up until we confirm, for as long as we need, even after the full
transfer. So the deprecation date is a decision we own rather than a deadline
imposed on us, and the sequencing follows in one direction only: **do not
confirm disconnection until the reconciliation in §7 tranche 1 step 2 has
passed.** Those legacy tables are the only independent check on TBX's numbers we
will ever have; confirming shutdown first would mean giving up the ability to
verify the platform we are migrating onto, permanently, to save nothing.

What it does not change: the API is a genuinely different system, and none of
the plumbing carries over:

| | legacy | new |
|---|---|---|
| Host | `ssp.pgammedia.com/api` | `api.pgammedia.com` |
| Auth | token as a URL path segment | `POST /login` → JWT, `Authorization: Bearer` |
| Modules | `core/tb_api.py`, `core/tb_mgmt.py` | `core/tbx_api.py`, `core/tbx_mgmt.py` |
| Env vars | `TB_EMAIL`, `TB_PASSWORD`, `TB_USER_ID` | `TBX_EMAIL`, `TBX_PASSWORD` |
| Entities | inventory → placement | company → supply source → placement; company → demand source |
| Report shape | flat `adx-report` query params | `POST /report/{hash}` with attributes + metrics + filters |
| Writes | form-encoded, per-field endpoints | JSON, whole-object `.../update` |

**Placement IDs carry across; inventory IDs do not.** Answered by Teqblaze
2026-08-20 (§8.1.10b): they moved the placements over as-is, so placement IDs
*and their settings* are unchanged, while inventory IDs are new. This is better
than the earlier assumption in this doc, which was that nothing was portable.
Two consequences, and they pull in opposite directions:

- **The write path is unblocked at the level that matters.** A placement ID
  derived from a contract sheet is now valid on *both* hosts, and a floor
  written against it targets the same placement either side. No hand re-keying,
  and no risk of re-keying a contract floor onto the wrong placement — which
  was the specific danger flagged here before.
- **It does not populate anything by itself.** `tb_mgmt.PROTECTED_FLOOR_MINIMUMS`
  is *also* empty, so there is no filled-in legacy map to port; the ID stability
  makes populating safe, it does not do it. The one populated contract map we
  hold is LL's (`core/ll_mgmt.py`: 9 Dots ≥ $1.70), and that one is keyed on
  **name tokens**, not IDs — host-independent, and demand-side. So the route to
  filling `PROTECTED_FLOOR_MINIMUMS` in `core/tbx_mgmt.py` is still the contract
  sheets (§7 tranche 3), now with the ID lookup made trustworthy.

**Untested assumption — publisher and demand-source IDs.** Teqblaze confirmed
*placement* IDs and explicitly excluded *inventory* IDs. They said nothing about
publisher IDs or demand-source IDs, and inventory sits directly between
publisher and placement in the legacy hierarchy — so the one level they name as
changed is adjacent to the keys we actually depend on. Our ETL tables key on
neither placement nor inventory: `pgam_direct.tb_daily_publisher_revenue` keys on
`publisher_id`, `tb_daily_demand_revenue` on `demand_id`, and
`tb_daily_publisher_demand_revenue` on the pair. Nothing we have been told
covers those. Treat their stability as **unverified** (§8.1.10d), and see the
join-key caution in §7 tranche 1 step 2 — assuming it is the one way to turn a
clean reconciliation into a fake failure.

**Do not delete or repoint the legacy `tb_*` modules yet** —
`tb_floor_nudge` and `tb_contract_floor_sentry` still run against the legacy
host, and until the new platform is verified against real data, that leg is
the one carrying live floor decisions. Migrate deliberately, per §7, not by
repointing a base URL.

The reconciliation remains the cheapest correctness test we have, and Teqblaze's
answers sharpen it rather than retire it. Same ClickHouse data (§8.1.10c) means
the expected result is now **exact agreement on impressions and spend** for the
same window, not merely "close". Any material gap means one of the two is wrong
about our revenue, and that is worth knowing before either drives a write.

---

## 2. Reporting: a real analytics surface

`POST /report/{hash}` takes **25 attributes × 43 metrics** with per-dimension
and per-metric filters, ET-or-any timezone, and hour/day/month buckets.

Attributes: `date`, `supply_company`, `supply_source`, `supply_source_type`,
`supply_integration_type`, `placement`, `demand_company`, `demand_source`,
`demand_source_type`, `country`, `supply_deal`, `demand_deal`, `seat`,
`traffic_type`, `ad_format`, `size`, `inventory_key`, `publisher`, `os`,
`region`, `inventory_ssp_id`, `inventory_dsp_id`, `crid`, `ab_test_feature`,
`consent_regulation_compliance`.

Metrics worth calling out, because the legacy API exposes no equivalent:

| Metric | Why it matters |
|---|---|
| `timeout_rate` | Which DSPs are timing out. Today PGAM infers this from missing responses. |
| `render_rate` | Wins that never rendered — the gap between billed and delivered. |
| `demand_bid_rate`, `supply_bid_rate` | Bid participation split by side. |
| `margin`, `profit` | Platform-computed, no client-side arithmetic to drift. |
| `avg_demand_bid_floor` / `avg_demand_bid_price` | Demand-side pricing beside the supply-side pair — the input a floor model actually wants. |
| `vcr` + four video quartiles | CTV completion, per placement. |
| `dsp_sync_rate`, `ssp_sync_rate`, `platform_cookies_sum` | Cookie-sync coverage, which caps addressable demand. |
| `supply_srpm`, `demand_srpm` | Revenue per thousand requests on both sides. |
| `ssp_conversion_rate` | Supply request → bid request conversion. |

Metric filters behave like SQL `HAVING` — `{"imps_sum": {"operator": ">",
"value": "1000"}}` — so "every placement over 1k imps with margin under 10%" is
one call rather than a full pull plus local filtering.

Two things to know about the endpoint:

- **`{hash}`** keys a server-side result set so paging stays consistent.
  `tbx_api` derives it as an md5 of the canonical payload; `keep_hash_alive()`
  extends its TTL for a long walk.
- **Totals come back separately** in a `total` block. Use it. Summing `rows`
  gives wrong answers for every rate metric — margin, win rate and VCR are
  ratios.

Also available: `/report/chart/{hash}` (time series), `/report/export/{hash}`
(server-side export), `/report/columns-list` (the authoritative column
catalogue — call it after a platform upgrade, it will list new
attributes/metrics before our constants do), plus presets, scheduled reports
and shareable report links.

---

### 2.1 First observed values from the live platform (2026-08-21)

Read off the Performance Overview dashboard rather than the API, since no
session has had credentials yet — but every tile maps to a metric this client
already sends, so the same figures are one `/report` call away:

`ssp_requests_sum` 4,385,908,185 · `requests_sum` 4,472,194,800 ·
`responses_sum` 64,541,190 · `ssp_wins_sum` 4,179,829 · `imps_sum` 3,284,048 ·
`ssp_price_sum` $2,507.50 · `dsp_price_sum` $3,228.96 · `profit` $721.46 ·
`margin` 22.34% · `supply_srpm` 0.57 · `demand_srpm` 0.72.

Three things worth carrying forward:

1. **Margin 22.34%** against roughly 31% in the legacy 30-day tables. Either
   the mix on a part-day differs from a month's average, or margin has moved.
   Worth a full settled day before treating it as a trend, but it is the
   largest single discrepancy between the two platforms observed so far.

2. **Render rate 78.6%** (3,284,048 imps ÷ 4,179,829 wins), against the 93–99%
   band healthy sources run at in our own data. This is the account-wide
   number, so it is consistent with the per-source render loss the digest
   already flags — and it means that loss is not confined to a few endpoints.

3. **`demand_srpm` is the QPS metric we derive by hand.** $0.7220/M here
   against `qps_waste_sentry`'s blended baseline of $0.7804/M over 14 legacy
   days — close enough to confirm the derivation, and the platform computes it
   natively per source. When the sentry moves to TBX it should read
   `demand_srpm` rather than recompute gross ÷ requests, because a
   platform-computed figure cannot silently disagree with the platform's own
   reporting the way ours can.

Also note the bid response rate: 64.5M responses against 4.47B bid requests is
**1.44%**. That is the QPS efficiency picture in one number, and the reason
traffic shaping matters here.

## 3. Five surfaces that replace work PGAM currently does by hand

This is the biggest finding in the spec. The platform already computes things
the compliance and recon agents reconstruct themselves, at cost.

### 3.1 HUMAN report — the first programmatic IVT feed
`POST /human-report/risk-metrics` → `requests_sum`, `mfa_sum`/`mfa_rate`,
`sivt_sum`/`sivt_rate`, `givt_sum`/`givt_rate`, by date and `inventory_key`.
`traffic-report` gives `impressions_sum` + `charge_amount_sum` (what HUMAN
bills).

Today `agents/compliance` infers quality from ads.txt and schain posture — a
proxy for fraud, not a measurement of it. This is per-domain MFA and IVT rates,
directly. `GET /human-report/settings` tells us whether the integration is
live on our account at all; check that first.

### 3.2 Sellers validation + ads.txt verification — the crawl is already done
`POST /sellers-validation` → per `seller_domain` × `inventory_key`:
sellers.json verification state, ads.txt verification state, node position,
node rank. `POST /ads-txt-verification` → per publisher domain:
`crawled_domain`, `ads_txt_url`, `status`, with a `/history` trail.

`agents/compliance/crawlers` currently fetches partner ads.txt files itself at
a self-imposed 2 req/s (`PGAM_COMPLIANCE_RATE_HZ`), across five phases. A large
part of that is now a query. **Do not rip the crawlers out on the strength of
the spec** — first reconcile a day of platform verification output against a
day of our own findings. Where they agree, retire ours; where they disagree,
we have learned something either way.

### 3.3 Schain utilisation — live traffic, not static config
`POST /schain-utilisation` → incoming vs outgoing node counts,
sellers.json-verified node counts, ads.txt-verified node counts and chain
completeness, per supply source / demand source / `inventory_key`.

Compliance Phase 4 audits the `supplyChainEnabled` / `dontAddSupplyChainNode`
flags — what the config *says*. This is what the bid stream *does*, split by
incoming and outgoing. It also needs no LL UI scrape, so it works without
`LL_UI_EMAIL` / `LL_UI_PASSWORD`.

### 3.4 Discrepancy report — automated revenue recon
`POST /discrepancy-report` → per source: `impressions` / `spend` as the platform
counted them beside `impressions_api` / `spend_api` as the partner reports them,
plus discrepancy %. Filter on the discrepancy to jump to the outliers.

This works only for sources with an API-sync URL registered
(`api_sync.url` + `map_headers` naming the partner's date/imps/spend columns).
`POST /discrepancy-report/validate-api-url` dry-checks a URL first, and
`/sync` pulls on demand rather than waiting for the platform's schedule.
Registering partner URLs is a one-time setup task with an outsized payoff:
month-end recon becomes a query.

### 3.5 Bids overview — why requests get dropped
`POST /bids-overview/{incoming|outgoing|responses}` → per supply source /
placement / demand source: `total_count`, `valid_count`, `dropped_count`,
`drop_rate` and a reason breakdown, with `/details/{type}` expanding one slice
into named reasons (`failure-reasons` dictionary maps the IDs).

`agents/optimization/fill_funnel.py` infers funnel loss from ratios between
metrics. This names the cause.

Adjacent: `POST /scanner-statistics/prebid` → `requests_sum`, `blocked_sum`,
`blocked_rate` per supply source; `postbid` gives `scan_attempts` / `scans`.
Which scanner is blocking, and where.

**These are not HUMAN.** The `/scanner-settings` module covers the third-party
scanners the platform integrates — the spec's payload union names Pixalate,
Protected Media, FraudSensor, MediaGuard and GeoEdge — each toggleable per
scanner and per source. HUMAN is a **separate module** (`/human-report`,
`/human-report/settings`) and its volume does not appear in
`scanner-statistics` at all. An earlier revision of this doc treated them as
one thing; they are not, and the distinction matters for §3.6.

### 3.6 HUMAN — ours to pay for, therefore ours to manage

PGAM holds the HUMAN contract directly. Teqblaze operates the integration and
does not bill us for it, and the scan runs **both pre-bid and post-bid**
(confirmed by Priyesh, 2026-08-19). So HUMAN scan volume is a cost PGAM controls
and, today, cannot see. Three endpoints make it visible:

| Signal | Where |
|---|---|
| Pre-bid scan volume | `human-report/risk-metrics` → `requests_sum` |
| Post-bid scan volume | `human-report/traffic-report` → `impressions_sum` |
| What HUMAN charges | `human-report/traffic-report` → `charge_amount_sum` |
| What the scanning caught | `risk-metrics` → `mfa_sum`, `sivt_sum`, `givt_sum` |

Both cuts break down by `inventory_key`, so cost and value land on the same
domain. That is the whole input to a scan-cost monitor: spend per domain
against invalid traffic actually caught per domain. A domain generating heavy
pre-bid scan volume, a negligible IVT catch rate and little revenue is one we
are paying HUMAN to inspect for nothing.

Two things have to be settled before that monitor can act rather than just
report, and both are questions for Teqblaze (§8.1.7):

1. **Can HUMAN scanning be scoped at all** — per supply source, per traffic
   type, sampled — or is it all-or-nothing for the account? The per-source
   `scanner_settings[]` array keys on `scanner_id` / `setting_id` from the
   `/scanner-settings` module, which does **not** list HUMAN, so there may be
   no per-source lever. If there is none, "manage it on our end" can only mean
   managing which supply we accept, not which we scan — a much blunter
   instrument, and worth knowing before we plan around the finer one.
2. **Does `charge_amount_sum` reconcile with HUMAN's invoice to us?** If it
   does, it is a monthly cross-check on a bill we currently take on trust.

---

## 4. Write surface: the levers

All writes go through `core/tbx_mgmt.py`, which is dry-run by default and
additionally gated on `TBX_ALLOW_WRITES=1`. See §6 before considering a live
write.

### Supply side
| Lever | Where | Helper |
|---|---|---|
| Per-placement bid floor | `source.placements[].floor_price` | `set_placement_floor` |
| Floor vs fixed price | `price_type` | `set_placement_floor(price_type=…)` |
| Per-placement margin | `margin_status/type/min/max` | (via placement array) |
| Placement pause/resume | own endpoint | `set_placement_status` |
| Source floor, spend limit, tmax | `source.*` | `set_supply_source_fields` |
| Teqblaze smart-floor optimiser | `source.is_smart_floor` | `set_supply_source_fields` |
| Dynamic margin % | `source.is_dynamic_margin` | `set_supply_source_fields` |
| Which DSPs may buy this supply | `demand_sources[]`, `companies[]` | `set_supply_allowed_demand` |
| IAB category allow/block | `source.iab_categories.white/black` | (direct `_apply_update`) |
| Traffic type + ad format gating | `source.traffic_type_*`, `ad_format_*` | (direct) |

### Demand side — most of this has no legacy equivalent
| Lever | Where | Helper |
|---|---|---|
| **Per-country bid floor** | `geo_settings.bid_floor[]` | `set_demand_geo_bid_floors` |
| Per-country QPS cap | `geo_settings.qps[]` | `set_demand_geo_qps` |
| Country blocklist | `geo_settings.blacklist[]` | `set_demand_geo_blacklist` (merges — see §6.2) |
| QPS envelope + auto-optimiser | `qps_limit.*` | `set_demand_qps_limit` |
| Spend limit | `spend_limit` | `set_demand_economics` |
| Margin model (fixed/adaptive/range) | `margin_type`, `margin_min/max` | `set_demand_economics` |
| Target sRCPM | `target_srcpm`, `target_srcpm_value` | `set_demand_economics` |
| VCR optimisation | `is_vcr_optimization`, `vcr_optimization` | `set_demand_economics` |
| Schain enforcement | `is_schain`, `is_complete_schain`, `is_only_schain_complete`, `is_only_verified_nodes`, `is_remove_unverified_adstxt`, `schain_node` | `set_demand_schain_policy` |
| IVT-sensitive flag | `is_sensitive` | `set_demand_schain_policy` |
| Traffic filters | `is_ifa_required`, `is_only_synced`, `is_gdpr`, `is_coppa_filter`, `is_lat`, `is_ipv6_filter`, `omsdk` | (direct) |
| Banner/video size filters | `banner_filter[]`, `video_filter[]` | (direct) |
| Seats | `seat[]` | (direct) |
| Which supply a DSP may buy | `supply_sources[]`, `companies[]` | `set_demand_allowed_supply` |

`geo_settings.bid_floor` is the standout. Per-country DSP pricing is a lever
PGAM has not had, and it prices demand without touching publisher-side floors —
no contract-floor exposure.

The schain flags matter differently: they are the **enforcement** counterpart
to the compliance audits. The audit finds an unverified chain; these flags stop
PGAM passing it on.

### Filter lists (block/allow)
`record_type` ∈ `bundle`, `publisher_id`, `site_app_id`, `crid`, `adomain`,
`schain_node_domain`; `type` ∈ `black`/`white`; scopable to specific sources or
platform-wide. `adomain` and `crid` lists give creative-level brand safety;
`schain_node_domain` (with `filtering_node` = all/first/last) blocks a
reseller by node position.

`agents/compliance/block_list.py` and `blocked_domains_agent.py` have a real
enforcement target here.

### Also writable
Deals (SSP + DSP PMP, CPM, auction type, geo/size/seat targeting), companies,
users, roles, adapters, alerts, scheduled reports, presets, dashboard widgets,
ads.txt records, sellers.json, scanner settings.

### Platform-side alerts — cheaper than polling
`POST /alerts/store` fires on `ssp_requests` / `requests` / `responses` /
`imps` / `ssp_price` / `dsp_price` crossing a threshold over a 1/4/12/24-hour
window, to email **or Slack**, scoped by DSP / endpoint / company. Moving the
coarse "requests fell off a cliff" alarms onto the platform frees our agents to
spend their budget on judgement instead of heartbeats.

---

## 5. What is built in this branch

| File | Purpose |
|---|---|
| `core/tbx_api.py` | Auth (JWT + cache + 401 re-login), retry/throttle, pagination, dictionaries, and every analytics surface in §2–3. Read-only. |
| `core/tbx_mgmt.py` | Entity reads + guarded writes for §4. Dry-run default, env gate, clamps, read-modify-write with key-loss detection, verify-after-write, ledger. |
| `scripts/tbx_probe.py` | Read-only probe: auth, one call per module, `--diff-shape` for the read→write round trip. **Run this first.** |
| `scripts/tbx_pull.py` | Read-only data pull across every surface — 20+ report cuts, drop reasons, IVT, schain, sellers validation, discrepancy, dictionaries. Bounded digest to stdout, full JSON to `--outdir`. Per-surface failures are recorded, not fatal. |
| `.github/workflows/tbx-probe.yml` | Manual dispatch that runs both on a GH runner, because the Claude sandbox cannot reach the platform. Read-only; both write gates explicitly shut. |
| `tests/test_tbx.py` | 80 offline checks — payload assembly, clamps, merge semantics, key-loss guard, both write gates, argument validation. No network, no credentials. |
| `docs/api/teqblaze-openapi.json` | The spec, vendored so future sessions don't need the upload. |

No **writer** is wired into `scheduler.py`. That is deliberate — wiring an
autonomous writer against an unverified platform is a separate decision.

One **reader** is: `tbx_revenue_etl` runs hourly (`scheduler.py`, :41) and
UPSERTs `pgam_direct.tbx_daily_{supply,demand,placement}_revenue`. It no-ops
with a log line while `TBX_EMAIL` / `TBX_PASSWORD` are absent, which is what
makes it safe to schedule ahead of its credentials — but it also means the hour
those land in Render, an ETL starts running with no further deploy. That is the
intended behaviour; it is listed here so it is not a surprise. It self-migrates
its tables (`migrations/2026_08_21_tbx_daily_revenue.sql`), reads only, and
never imports `core.tbx_mgmt`.

Setup: add `TBX_EMAIL` + `TBX_PASSWORD` in the **Render** dashboard
(Environment → Add Environment Variable) on `pgam-intelligence-scheduler`.
Declared in `render.yaml` with `sync: false`, and in `.env.example` for local
dev. Not Vercel — see `CLAUDE.md`.

---

## 5.5 Credential handover — one user, two gates

The ask that keeps coming up is "create a user and share the credentials so a
session can do the read/write setup". Two things about this environment shape
the answer.

**A cloud session cannot reach the platform at all.** Egress to
`api.pgammedia.com` is denied by the environment's network policy — re-verified
2026-08-21, `CONNECT tunnel failed, response 403`, same as every PGAM host. So
credentials pasted into a cloud session buy nothing: the client cannot log in,
the probe cannot run, and the reconciliation cannot be attempted. Everything a
session can do against this platform without credentials it has already done
(spec-derived vocabulary, offline tests, payload assembly). Everything that
needs the live host runs through **Actions → TBX Data Pull**, on a runner with
open network, reading `TBX_EMAIL` / `TBX_PASSWORD` from repo secrets.

**And a cloud environment is not a secrets store.** Its variables are readable
by anyone using the environment (`CLAUDE.md`, "Cloud sessions and credentials").
So the credential goes to the two places that are secret stores, and nowhere
else:

| Where | What it powers | How |
|---|---|---|
| GitHub **Actions secrets** | `tbx_probe.py`, `tbx_pull.py`, `--diff-shape` | Settings → Secrets and variables → Actions → `TBX_EMAIL`, `TBX_PASSWORD` |
| **Render** env on `pgam-intelligence-scheduler` | hourly `tbx_revenue_etl` | Environment → Add Environment Variable (declared `sync: false` in `render.yaml`) |

Not the Claude cloud environment. Not `.env` in a commit — the playbook records
a real leak from exactly that shortcut (2026-07-02).

### What to create in the admin

**One user is enough, and read-only is the right start.** The write path is
gated twice in code (`dry_run=True` per call, `TBX_ALLOW_WRITES=1` per
environment) and its prerequisites are unmet: `PROTECTED_FLOOR_MINIMUMS` is
empty and the round trip is unverified (§6). A write-capable credential would
sit in two secret stores for weeks with nothing authorised to use it. Ask
Teqblaze for the read-only permission set (§8.1.3) and give it:

- a **dedicated API user**, not a person's dashboard login — so it can be
  rotated without locking anyone out, and so the ledger's actor is honest;
- **reporting + entity read** scope. `GET /permissions` is the first thing the
  probe calls, and a 403 on a module means the account lacks it rather than the
  endpoint being wrong;
- an **address we control** (an alias, not a personal mailbox).

Then, when tranche 3 actually starts, create a **second, write-capable user**
rather than upgrading the first. Two accounts keep the hourly ETL's token and a
supervised write session's token independent — which matters because we still
do not know whether a second `/login` invalidates the first token (§8.1.5). The
client now caches per account (`_token_cache_path()`), so two users on one host
no longer overwrite each other's JWT; supporting two credential *pairs* in the
env is a small change and should be made at that point, not before.

### First run, in order

1. Add the two Actions secrets.
2. **Actions → TBX Data Pull → Run workflow**, defaults. Read the job log: the
   capability matrix says which modules the account actually licenses. This is
   also the run that tells us whether the read-only scope is too tight.
3. Same workflow with `diff_shape: supply:<id>`, then `demand:<id>`. This is the
   §6 gate; it sends nothing.
4. Only then: `TBX_EMAIL` / `TBX_PASSWORD` into Render, which starts the hourly
   ETL, followed by the tranche 1 step 2 reconciliation.

Nothing in that sequence needs `TBX_ALLOW_WRITES`, and nothing in it needs a
credential to enter a Claude session.

---

### 5.9 Heavy report queries have wildly variable latency

The evidence, all from 2026-08-31, same account, same GitHub-hosted runner
class, same credentials:

| dispatch (UTC) | commit | probe step | `tbx_trim.py` |
|---|---|---|---|
| 12:43 | 2584087 (one 7-day call) | 28s | >34 min, cancelled |
| 12:49 | 188c5d0 (chunked) | 26s | >48 min, cancelled |
| 12:55 | f0aa1cc (chunked + prints) | 24s | **45s** |

The last two differ only by `print` statements. A 60x swing on identical
queries six minutes apart is the host, not the code.

But note the probe column: **24–28 seconds throughout, including during both
slow runs.** So the probe is *not* a canary for this. Its report calls ask for
`imps_sum`, `ssp_price_sum`, `dsp_price_sum`, `profit`, `margin` — none of
which is `ssp_requests_sum`, the raw inbound-request counter that
`tbx_trim.py` aggregates by supply source. That is the heaviest column in the
system, and it is the one whose latency swings.

What follows for anything reading `ssp_requests_sum` or `requests_sum` at
entity grain:

- **A slow run is not necessarily a bug.** Before rewriting a query, re-run it
  once. When it is fast the second time, the first was the host.
- **A fast probe proves nothing** about a heavy query. Do not use it to
  conclude your own query is at fault.
- **Print per-item progress, flushed.** Two runs were cancelled having emitted
  nothing, which is what made a slow call indistinguishable from a hung one
  and cost four dispatches to sort out. `tbx_trim.py` now prints a line per
  day per grain; when healthy those read 3–5s each, so a stalled run is
  obvious within seconds instead of after half an hour.
- **Read step timestamps, not wall-clock guesses.** A job object showing
  `in_progress` may have completed a second later; I misread exactly that
  during this session and drew the wrong conclusion from it twice.
- **Raise `TBX_TIMEOUT` for these queries; 60s is not enough.** The
  `demand_source × country` pair grain is the heaviest read in the repo. On
  2026-08-31 three days returned in 2–3 minutes *each* and the fourth timed
  out on all three retries. The default 60s read timeout is calibrated for
  entity reads, not this; `tbx-probe.yml`'s geo step and `tbx-geo-cut.yml`
  both set 300.
- **Let a slow day fail without losing the fast ones.** The same run threw
  away three good days because the fourth timed out. `pull_pairs` now skips a
  failed day, reports which, and divides per-day figures by the days that
  answered. A short window is a smaller measurement, not a wrong one — but see
  §6.2 on why a write must not accept one.

#### The exit-code collision this exposed

`tbx_geo_waste` exits 1 for "found waste", and its workflow step treats that
as success (`|| [ $? -eq 1 ]`). **An uncaught Python exception also exits 1.**
So the crashed pairs run above reported step success, job success, and
produced no report — the failure was visible only by reading the log. Both geo
scripts now catch `TbxError` at the module guard and exit **3**, which lands
outside that guard. Any script whose caller special-cases exit 1 needs the
same treatment; a bare `sys.exit(main())` will not do.

### 5.10 Never ask this API for a multi-day window

Recorded here because three separate pieces of code have now had to learn it
independently — `tb_revenue_etl` against the legacy host, `tbx_revenue_etl`
(`CHUNK_DAYS = 1`), and `tbx_trim.py`.

A multi-day report request is answered **200 with only the most recent ~5
days in it**. No error, no flag, no short-window marker. A seven-day request
therefore returns five days of data, and any code that divides by seven
understates every figure by ~30% while looking perfectly healthy.

Ask one day at a time, and check the `date` on every row that comes back
rather than trusting a single-day request to have been answered as one.

---

## 6. Before enabling writes

The write path is read-modify-write against endpoints that replace the **whole
object**: `POST /supply-sources/{id}/update` takes the entire
`SupplySourceRequest`. A partial body blanks every field left out. Three things
guard that, and one of them needs human confirmation:

1. `_strip_read_only` drops the fields the read schema returns but the write
   schema rejects — `id` and source-level `margin_type`/`margin_min`/`margin_max`
   on supply; `id`, `operation_systems` and `uuid` on demand.

   `uuid` was **missing from that list until 2026-08-21**. It is the one field
   the two entities disagree about: `SupplySourceRequest` accepts `uuid`,
   `DemandSourceRequest` does not, so a demand-source round trip was posting a
   field the write schema never declared. Found by set-differencing the vendored
   spec rather than by reading the code, which is why the check below is now
   automated: `tests/test_tbx.py` recomputes the read-only set from the spec on
   every run and fails if the hand-maintained tuple has drifted.
2. `_assert_no_key_loss` refuses the write if the merged payload would drop any
   key that was present before. Note what it cannot see: it compares the payload
   against the *stripped* body, so every key it checks is one we just put there.
   It catches a merge bug, not a field the platform will reject.
3. `unknown_write_keys` covers the other direction — keys in the outgoing
   payload that the write schema does not declare. Warned, not refused: an
   undeclared key most likely 422s, which fails safe and changes nothing,
   whereas hard-refusing on a vendored spec that has fallen behind the platform
   would block every write for a reason that is ours. The warning goes to stderr
   and rides along on the result as `unknown_keys`.
4. Whether the platform actually accepts the round trip.
   `python3 scripts/tbx_probe.py --diff-shape supply:<id>` prints the GET
   response beside the exact payload an update would POST, and checks both
   directions against a real account: fields dropped that we did not mean to
   drop, and fields the live response carries that `SupplySourceRequest` /
   `DemandSourceRequest` will not accept. The second is what finds the next
   `uuid` — the account, not the spec, is the authority on what comes back. Any
   finding under either heading means do not set `TBX_ALLOW_WRITES`.

   It found one on the first real run (2026-08-28, supply source 264): the live
   GET returns **`has_inactive_company`**, which the vendored spec declares in
   *neither* schema, so the set difference in item 1 could not derive it and the
   payload was carrying a key `SupplySourceRequest` never advertised accepting.
   Named by hand in `_UNDECLARED_RESPONSE_FIELDS` and re-checked by
   `tests/test_tbx.py`, which now also fails if a re-vendored spec picks the
   field up and makes the exception stale. Re-run `--diff-shape` and get a clean
   round trip before setting `TBX_ALLOW_WRITES`; it takes a comma-separated list
   (`supply:264,supply:65,supply:194`) so a finding can be told apart from an
   account-wide one in a single run.

### 6.1 The supply margin lever is read-only

The same run answered a question the margin work had been assuming. Supply
source 264 carries its margin at the **top level** of the entity:

```json
"margin_type": "range", "margin_min": 5, "margin_max": 30
```

and, separately, inside `source`:

```json
"is_dynamic_margin": false, "dynamic_margin": "0.00"
```

`margin_type` / `margin_min` / `margin_max` are exactly the fields
`SupplySourceRequest` refuses (item 1) — they are read-only over this API.
So the margin actually in force on this source is a 5–30% range that
`POST /supply-sources/{id}/update` **cannot set**, and
`set_supply_source_fields(is_dynamic_margin=…, dynamic_margin=…)` reaches a
different mechanism that is currently switched off.

That matters for how a margin change gets made:

- Turning `is_dynamic_margin` on is **not** an adjustment to the current
  setting; it swaps which mechanism governs the source. Its interaction with
  a live `margin_type: "range"` is undocumented and untested here.
- Moving the 5–30% range itself is a **dashboard change**, not an API one,
  until Teqblaze exposes those fields for write.
- So do not describe a planned margin move as "set `dynamic_margin` via the
  API" without saying which of the two mechanisms it lands on.

**This is a SUPPLY-side restriction only — corrected 2026-08-31.** Earlier
wording here and in `tbx_trim.py` generalised it to "the margin fields are not
writable over this API", which is wrong and was repeated three times before
anyone checked the other entity. Set-differencing the spec:

| entity | `margin_type` / `margin_min` / `margin_max` |
|---|---|
| supply source | in `SupplySourceResource`, **absent from** `SupplySourceRequest` → read-only |
| demand source | in **both** `DemandSourceResource` and `DemandSourceRequest` → **writable** |

`DemandSourceResource - DemandSourceRequest` is only
`{id, operation_systems, uuid}`, so nothing margin-shaped is stripped on the
demand side. `core.tbx_mgmt.set_demand_economics` already exposes all three,
and has since it was written.

Placements carry `margin_status`/`margin_type`/`margin_min`/`margin_max` on
their read resources too, but there is no placement update endpoint that
accepts them — `/supply-sources/{id}/placements/{placement_id}/status` toggles
status and nothing else — so placement margin is read-only in practice as
well.

Read the current shape of any source before proposing a margin change to it;
the realised take rate in the reports is an outcome, not the configuration.

**What the three margin candidates actually carry** (read 2026-08-28, run
`33175637798`, `--diff-shape supply:264,supply:65,supply:194`):

| id | name | `margin_type` | min | max | `is_dynamic_margin` | `is_smart_floor` |
|---|---|---|---|---|---|---|
| 264 | Advetisi - Zmaticoo | `range` | 5 | 30 | false | **true** |
| 65 | Illumin - Video Unruly OTTA | `adaptive` | 5 | 95 | false | **true** |
| 194 | Illumin Display and Video EU Correct | `fixed` | 2 | 0 | false | **true** |

Three different `margin_type` values across three sources, so this is
deliberate per-source configuration and not an account default that happens
to be showing through. Two things follow that were not previously written
down:

- **194 is on a 2% fixed margin.** Whatever it turns over, it turns over at
  2%. If it carries meaningful volume that is a structural drag on blended
  margin, and no floor change or dynamic-margin flip touches it — the number
  is the configuration, and the configuration is a dashboard field.
- **`is_smart_floor` is true on all three.** Teqblaze's own floor optimiser
  already owns the floor on these sources. Per the "pick one owner per lever"
  rule below, a PGAM floor agent must not be pointed at them while that
  holds; that is the April thrash. Whether the platform optimiser is doing a
  good job here is a separate question worth asking, but the answer is not
  "run ours as well".

All three also sit at `floor_price: "0.00"` with
`bid_floor_type: "fixed_bid_price"` — i.e. no source-level floor of our own,
which is consistent with smart floor owning it.

Also unverified: `POST /filter-lists/{id}/import-values`. The spec documents it
as a file/CSV import; `import_filter_values` sends `{"values": [...]}`, which is
a guess. Confirm before relying on it for large lists — `add_filter_values`
(one call per value) is correct but slow.

Guards that are in place: `GLOBAL_MIN_FLOOR` = $0.01 zero-out guard,
`TBX_MAX_FLOOR_DELTA` = ±25% per run (the legacy every-2h tuner moved floors
±39% in one run and tanked revenue — see `render.yaml`), contract minimums via
`PROTECTED_FLOOR_MINIMUMS`, and `core.partner_freeze` on the demand writers
that can reach a frozen partner.

`PROTECTED_FLOOR_MINIMUMS` **is empty**. It has to be populated with this
platform's own IDs before any floor agent runs — the 9 Dots $1.70 contract
minimum is not enforced here until it is. Until then only the $0.01 guard and
the delta cap apply, which stops a zero-out but not a $0.05 write on a $1.70
floor.

Two platform-side optimisers overlap with ours and must not run concurrently
with a PGAM agent doing the same job:

- `source.is_smart_floor` — Teqblaze's own floor optimiser.
- `qps_limit.qps_optimization_by` (rcpm/spend/clicks) — Teqblaze's QPS tuner.

Pick one owner per lever. Two optimisers on one floor is the April thrash
again, with a partner in the loop.

---

### 6.1a Demand-side margin writes: what the platform actually accepts (2026-09-03)

Learned from the first live rollout of `scripts/tbx_connection_margin.py`
(runs 33811533316 → 33813113628, 17 writes verified). Each of these cost a
live write before it was a rule.

| demand `margin_type` | what happens on `POST /demand-sources/{id}/update` |
|---|---|
| `range` | `margin_min` / `margin_max` both honoured. **`margin_max` must be strictly greater than `margin_min`** — equality is `422 "Max Margin Value must be greater than N"` (demand 2408). |
| `fixed` | one number, in `margin_min`. The platform reports `margin_max` as **0** regardless of what is sent, so a verify on that field is noise and the field should not be sent (demand 35). |
| `adaptive` | **`margin_min` is silently ignored**: the POST returns 200 and the value does not change (demand 1986, live 5 → expected 20, verify ✗). Switching the type to `range` with the same bounds makes the floor take; that is a mechanism change and the tool does it only under `--convert-adaptive`. |

The two sides **stack**: realised take exceeded the demand ceiling on 9
connections and the supply ceiling on 2 (2026-09-02 sentry runs). On thin
connections the adaptive/range demand band sits at its floor, and the
realised total moved ~1:1 with the floor when tested (demand 2185: floor
5 → 20 at 08:55 ET, day realised 14.9% ≈ 15/24 h at ~19.7%). Raising a
floor 15 points cost that connection ~30% of its gross; net still improved.

A demand floor is a per-connection lever **only where the connection is
1:1**. A demand endpoint that buys across several supply sources (the OTTA
endpoints on Unruly #65 + Smaato #189; Verve RON Display East across
Dexerto/Cox/TravelReveal/VerticalScope; Zeta - RON Desktop across five)
cannot be landed on a per-connection target from the demand side at all.

**The supply-side field that IS in the write schema.** `margin_type/min/max`
on supply are read-only (above), but `SupplySourceRequest.source` is
`oneOf [DirectInventoryResource, IndirectSuppliersResource]` and the
indirect shape carries `is_dynamic_margin` (bool) and `dynamic_margin`
(number, "%", example 30). Both are writable via
`set_supply_source_fields`. What they govern is unverified — every source
read so far has `is_dynamic_margin: false` — and is the subject of
`scripts/tbx_dynamic_margin.py`, which sets it on one source at a time so a
settled day can answer. If it moves the realised take, the fan-out
connections above become reachable without Teqblaze opening anything.

### 6.2 The three `geo_settings` lists replace wholesale on the wire

`geo_settings.bid_floor[]`, `geo_settings.qps[]` and `geo_settings.blacklist[]`
are each sent as the complete list. There is no "append one country" verb, so
a caller that POSTs only its own entries silently deletes every entry someone
else put there — and a country blacklist is exactly the kind of standing
trading rule a human sets by hand and never revisits. Nothing in the response
would look wrong afterwards; the first signal would be a partner asking why
they started receiving traffic from a country they blocked.

All three helpers in `core/tbx_mgmt.py` therefore **read the current list and
merge into it by default**, and take `replace=True` for a caller that has read
the list and means to drop entries. `set_demand_geo_blacklist` gained this on
2026-08-31; it had been the odd one out, replacing wholesale while its two
neighbours merged. It also returns `added`, `removed`, `blacklist_before` and
`blacklist_after`, so a ledger can record what actually changed rather than
what was requested — which is what makes `scripts/tbx_geo_cut.py --revert`
exact rather than approximate.

A partial measurement must not drive one of these writes. `pull_pairs`
tolerates a day that times out (§5.9), which is right for a report and wrong
for a blacklist: a pair that only traded on the missing day reads as dead.
`tbx_geo_cut.py --apply` therefore requires every requested day to answer
unless `--min-days-with-data` explicitly lowers the bar; a dry run reports on
whatever came back and says so.

Country arguments are the platform's **numeric ids**, not ISO codes. Resolve
with `tbx_api.country_ids(["Brazil", "RU"])`, which warns on anything it
cannot match. Warning, not raising, is the dangerous part for a batch caller:
a partially-resolved list would block a different set than the one reported,
so `tbx_geo_cut.py` refuses a buyer outright unless every one of its countries
resolves.

### 6.3 Demand read-modify-write is blocked: entities are not round-trippable

**Status 2026-08-31: every demand-source write fails. Not one has succeeded on
TBX.** The first live geo-blacklist apply (run 33440341431) attempted 50 and
the platform refused all 50:

| n | HTTP 422 message |
|---|---|
| 38 | `The Advanced Setting -> VCR Optimization value must be a number. (and 2 more errors)` |
| 12 | `The Advanced Setting -> Target sRCPM must be a string. (and 4 more errors)` |

Nothing was applied, and nothing needed reverting — the platform's own
validation was the thing that stopped it, after this repo's two gates had
already opened. That is the failure mode working as intended, but it is luck
that the rejection was total rather than partial.

**This is not a geo problem.** `_apply_update` writes by GET → deep-merge →
POST the whole object, so *every* demand writer goes through it:
`set_demand_economics` (margins, spend limit, target sRCPM),
`set_demand_geo_bid_floors`, `set_demand_geo_qps`, `set_demand_source_status`,
`set_demand_schain_policy`. All of them are currently non-functional against
live data, whatever they are trying to change. The margin raises discussed
earlier in that session would have failed exactly this way.

The supply side is unaffected — the 2026-08-31 supply cut disabled 11 sources
through the same machinery without complaint.

**The spec does not predict it.** `DemandSourceRequest` and
`DemandSourceResource` declare *identical* types for every field, including
`vcr_optimization` (number) and `target_srcpm` (string enum). So this is not
the read/write schema divergence of §6.2 — it is the **live data** disagreeing
with the type both halves declare. An entity as stored cannot be POSTed back
to its own update endpoint unchanged.

**Do not guess the coercion.** The error text is truncated ("and 2 more
errors"), so the full offender list is unknown, and the right repair is not
obvious: `vcr_optimization` arriving as `null` might want `0`, or might want
the key dropped so the platform re-applies a default — and dropping it could
switch VCR optimisation off on a live buyer. Those are different trading
outcomes.

**Measured, 2026-08-31** (`scripts/tbx_type_probe.py`, run 33443545277, 8 live
entities per side):

| field | n/8 | declared | live value |
|---|---|---|---|
| `target_srcpm_value` | 8 | number | `null` |
| `vcr_optimization` | 8 | number | `null` |
| `geo_settings` | 7 | object | `[]` |
| `target_srcpm` | 1 | string | `null` |

Supply: **zero mismatches across all 8**. That is the whole explanation for
why supply writes have always worked and demand writes had never succeeded.

`_normalise_for_write` repairs both shapes, and neither invents a value:

- **A null in a non-nullable field is dropped.** Omitting a key the entity has
  no value for is the closest faithful statement of "unset". Coercing would
  mean choosing one — `vcr_optimization: 0` is a VCR target of zero rather
  than an absent one, and `target_srcpm` `"default"` vs `"target"` is a live
  trading setting. Neither is ours to pick.
- **`[]` where an object is declared becomes `{}`.** PHP's `json_encode`
  renders an empty associative array as `[]`, so this is the platform spelling
  "no geo settings" in a shape its own schema rejects. Only an *empty* list is
  converted; a populated one is real data.

It runs before the merge, so `_assert_no_key_loss` compares like with like — a
deliberate drop must not read as the field-blanking failure that guard exists
to catch — and it logs what it repaired rather than editing a live entity
silently.

Still worth raising with Teqblaze: their update endpoint rejects the entities
their own read endpoint serves, and the 422 truncates its own error list
("and 2 more errors"), which is what forced a diagnostic round-trip instead of
a fix.

## 7. Suggested order of work

Sequenced by payoff over risk. Everything in the first two tranches is
read-only.

**Tranche 1 — verify and observe (read-only)**

1. Add the `TBX_EMAIL` / `TBX_PASSWORD` repo secrets and run the **TBX Data
   Pull** workflow. Record which modules answer; a 403 usually means the
   account lacks that module, so check `GET /permissions`. The pull also diffs
   the account's live `report/columns-list` against this client's constants,
   which is how we learn if the account exposes attributes or metrics the spec
   didn't document.

2. **Reconcile against the legacy platform, before building anything.** Teqblaze
   states the ClickHouse data was transferred wholesale and the reports should
   match (§8.1.10c), so this is now a **confirmation test with a stated expected
   answer** — exact agreement — rather than an open question. Pull a settled
   7-day window from `/report` at `date × supply_source` and compare impressions
   and spend against the legacy `pgam_direct` TB tables for the same window.

   **Which field is which — settled from a live dashboard, 2026-08-21.** The
   Performance Overview screen showed Supply Revenue $2,507.50, Demand Spend
   $3,228.96, Profit $721.46, Margin 22.34%. Those reconcile exactly
   ($3,228.96 − $2,507.50 = $721.46; 721.46 ÷ 3,228.96 = 22.34%), which pins
   the mapping the reconciliation needs and which the spec alone left open:

   | new platform | legacy Neon column | meaning |
   |---|---|---|
   | `dsp_price_sum` ("Demand Spend") | `gross_revenue` | what DSPs paid |
   | `ssp_price_sum` ("Supply Revenue") | `pub_payout` | what publishers get |
   | `profit` | `gross_revenue - pub_payout` | platform-computed |

   Getting this backwards would have compared payout against gross and
   produced a ~22–31% constant offset — which reads exactly like "a fee
   applied at a different stage", the second outcome below. That is a wrong
   diagnosis we would have escalated with confidence, so pin the mapping
   before running the comparison, not after.

   Two traps, and the second is the one that will bite:

   - **Timezone.** Legacy reporting and `filter.timezone` must be set to the same
     zone, or the daily buckets disagree for reasons that have nothing to do with
     the data.
   - **Join keys.** Do *not* join on `publisher_id` or `demand_id` on the
     strength of Teqblaze's ID answer. They confirmed **placement** IDs and
     explicitly excluded **inventory** IDs; our TB tables key on neither
     (`publisher_id`, `demand_id`), and those were not covered either way (§1).
     Joining on an ID that was silently reassigned manufactures row-level
     divergence out of a perfectly matching dataset — and because (c) makes
     divergence escalatable, the failure mode is escalating our own join error to
     the vendor. So **measure the key stability instead of assuming it**: pull the
     window at `placement` granularity too (the one key we have a commitment on),
     reconcile on that plus names, and then check separately whether
     publisher/demand IDs agree with the legacy tables by name. That check is
     cheap, and its result is the answer to §8.1.10d.

   Three outcomes, all informative:
   - **Agreement** → the new platform is trustworthy for revenue, the migration
     is a porting exercise, and PGAM can consider confirming the legacy shutdown
     (§1) — not before.
   - **Constant offset** → likely a fee or margin applied at a different stage.
     Find it before anything downstream inherits it. Contradicts (c), so it is
     also a question back to Teqblaze.
   - **Row-level divergence** → first rule out the two traps above, because both
     produce exactly this signature. If it survives that, one host is wrong about
     our revenue: stop, do not work around it, and escalate — Teqblaze has
     committed to these matching.

   **Run it with `python3 scripts/tbx_recon.py`.** That script is this step,
   implemented: it reports coverage first (so a partial day cannot slip into the
   window), then day totals on both legs with a verdict per metric, then the
   name-keyed demand comparison that measures §8.1.10d instead of assuming it.
   It classifies the result as one of the three outcomes above and says what to
   do about each. Read-only, and it needs only `PGAM_DIRECT_DATABASE_URL` —
   both legs are already in Neon, so this runs anywhere with warehouse access
   and does not need to reach either platform.

   One gap worth knowing: it cannot reconcile at **placement** grain, which is
   the grain with the only real ID commitment behind it. `tbx_daily_placement_
   revenue` exists but the legacy ETL lands no placement table, so that
   comparison needs a live pull from the legacy API rather than a warehouse
   query. The name-keyed demand check is the substitute.

   Cheapest correctness test available, and it gates everything below.

3. ETL the new report into Neon `pgam_direct` beside the legacy TB tables, at
   `supply_source × demand_source × country × date`. Keep the two platforms'
   tables separate until step 2 agrees — a premature union double-counts one
   marketplace.

4. Compare one day of `sellers-validation` + `ads-txt-verification` against one
   day of our own compliance findings. That decides how much of
   `agents/compliance/crawlers` retires.

**Tranche 2 — new intelligence, still read-only**

5. **HUMAN scan-cost monitor.** We pay HUMAN per scan and the platform scans
   pre-bid and post-bid (§3.6), so this is spend we own and currently cannot
   see. Join `risk-metrics` (requests scanned, IVT caught) to `traffic-report`
   (impressions scanned, `charge_amount_sum`) to `/report` (revenue), all keyed
   on `inventory_key`, and rank domains by scan cost against invalid traffic
   caught and revenue earned. Reports only until §8.1.7(a) tells us whether
   scanning can be scoped — no point proposing a change we have no lever to
   make. Pays for itself the first time it finds a domain we are scanning for
   nothing.

6. IVT report from `human-report/risk-metrics` — per-domain MFA/SIVT/GIVT into
   the compliance digest. Genuinely new signal.

7. Drop-reason report from `bids-overview` — feed named reasons into
   `fill_funnel`.

8. Timeout and render-rate monitor per DSP — `timeout_rate` and `render_rate`
   have no legacy equivalent.

9. Register partner API-sync URLs, then a discrepancy digest. Highest
   effort-to-payoff ratio of anything here: it retires manual recon.

**Tranche 3 — writes, supervised**

10. Populate `PROTECTED_FLOOR_MINIMUMS` from the contract sheets. Now materially
    safer than when this doc was written: placement IDs are stable across both
    hosts (§1), so a placement ID read off a contract sheet or the legacy host
    resolves to the same placement here, and the re-keying step that could have
    put a contract floor on the wrong placement is gone. Still hand work — the
    legacy map is empty too, so there is nothing to copy — and still the
    prerequisite for anything on the supply side.

11. Confirm the round trip with `--diff-shape` on one supply and one demand
    source. Blocked on §8.1.1.

12. Geo floor agent on `geo_settings.bid_floor`, proposing to Slack for
    supervised review — mirroring `PGAM_OPTIMIZER_AUTO_APPLY=0`, not
    auto-applying. Demand-side floors carry no publisher contract exposure,
    which makes this the safest first writer.

13. Schain enforcement: compliance audit finds an unverified chain →
    `set_demand_schain_policy` closes it. Propose first, apply on sign-off.

14. Filter-list enforcement for `adomain` / `crid` brand safety.

**Not recommended yet**

- Any autonomous floor writer on the supply side. Contract exposure, an empty
  contract-floor map and an unverified round trip is the exact combination that
  produced the April incidents.
- Retiring the legacy `tb_*` modules, or confirming the legacy shutdown to
  Teqblaze. They still carry live floor decisions, and the legacy tables are the
  only independent check on TBX's revenue numbers until tranche 1 step 2 passes
  (§1). Teqblaze will keep them alive as long as we ask (§8.1.10a), so there is
  no cost to waiting and a permanent cost to not.

---

## 7.4 Internal reporting: where the P&L's TB row comes from

Mapped 2026-08-21, because "sync TBX to internal reporting" turns out to be a
narrower change than it sounds and a riskier one than it looks.

`admin.pgammedia.com/admin/pnl` and `/admin/finance` are **not** served by
`pgam-direct` (which serves the rest of that host) but by **`pgam-dsp-dashboard`**
(`src/app/admin/pnl`, `src/app/admin/finance`). They read the **`finance`**
schema on a *separate* Neon database — `FINANCE_DATABASE_URL`, not
`PGAM_DIRECT_DATABASE_URL`.

The P&L's TB row is defined as:

    TB Gross = DSP Spend (advertiser-paid through Teqblaze)
    TB GP    = the "Profit" column on the TB report

stored in `finance.daily_pnl_inputs.tb_gross_usd` / `tb_gross_profit_usd`, and
written by **`pnl_sync.py` in the `mastap150/pgam-recon` repo** (workflow
`pnl-sync.yml`, daily 10:15 UTC, COALESCE-upsert over a trailing 7 days). A
Vercel cron in `pgam-dsp-dashboard`
(`/api/v1/cron/pnl-sync-watchdog`) re-fires that workflow whenever
`tb_gross_usd` comes back NULL. The fields are also inline-editable in the UI,
so a hand-entered value can sit in any day indefinitely.

**The consequence that matters: TBX reports the same marketplace that row
already counts.** Adding TBX as an additional P&L stream would double-count
every impression — the same trap the ETL avoids by keeping `tbx_daily_*`
separate from `tb_daily_*` (§5). The only coherent change is *repointing* the
existing TB row's source, and the mapping is already in Neon:

| P&L field | from `pgam_direct.tbx_daily_supply_revenue` |
|---|---|
| `tb_gross_usd` | `sum(gross_revenue)` — i.e. `dsp_price_sum` |
| `tb_gross_profit_usd` | `sum(gross_revenue - pub_payout)` — i.e. dsp − ssp |

Both reports run on **Actions → TBX Neon reports (recon + P&L impact)**
(`.github/workflows/tbx-neon-reports.yml`), for the same reason `tbx-probe.yml`
exists: a cloud session cannot hold a database credential, so without a
workflow every run of these needs a person at a terminal with `.env`. The
secrets live in Actions (`PGAM_DIRECT_DATABASE_URL` required,
`FINANCE_DATABASE_URL` optional — without it the recon still runs and the P&L
check skips), and the verdict comes back in the run summary.

Exit codes are the verdict in both scripts: `0` agree, `1` look at it, `2`
misconfigured. The workflow surfaces a `1` as a **warning with the job still
green**, and fails only on `2` or an unexpected code — a red run should mean
the tooling broke, not that the platforms disagree.

**Status, 2026-08-24.** The repointing switch exists and is wired on both
surfaces — `PNL_TB_SOURCE` for `/admin/pnl` and now `RECON_TB_SOURCE` for
`/admin/finance`, each `legacy` (default) | `compare` | `tbx`, each independent
of the other. `pgam-recon` gained a TBX client of its own
(`pgam_recon/fetchers/tbx.py`) because the finance column needs per-partner
grain the Neon rollup does not carry, and `pnl_sync`'s TBX reading now falls
back to that client when the rollup is unavailable — the same host over a
different wire, labelled in the note, not the cross-platform fallback
`_resolve_tb` refuses. The operational sequence is
`docs/tb-data-workflow-integration.md`; what is written below still governs
*why*, and the ordering rule at the end of this section is unchanged.

The remaining blocker is unchanged too, and it is not code: `TBX_EMAIL` /
`TBX_PASSWORD` are in neither secret store, so the hourly ETL has been
no-opping and neither switch can be moved off `legacy`.

`scripts/tbx_pnl_check.py` reports what that repointing *would* do, without
doing it: gaps TBX could fill (days the P&L holds NULL — the safe half),
days where both exist and by how much they differ, and a verdict sharing
`tbx_recon.py`'s classifier. Read-only against both databases.

Sequencing, and it is not optional: this is the company's profit reporting, and
the playbook's rule is "do not guess on P&L". Run `tbx_recon.py` (does TBX match
the legacy host?) **before** `tbx_pnl_check.py` (does TBX match the P&L?). A
disagreement in the second with agreement in the first means the P&L row is
stale or hand-entered — a different problem with a different fix. The writer,
when it is authorised, belongs in `pgam-recon` beside the existing `pnl_sync`:
one writer per table, or this becomes the two-controllers-on-one-lever problem
from §6 with the P&L in the middle.

---

## 7.5 The help centre is the spec — don't go looking for more

`https://ssp-new.pgammedia.com/help-center/management-api` (the new platform's
UI, admin login) renders **exactly** the spec vendored at
`docs/api/teqblaze-openapi.json`. Confirmed 2026-08-20 from a screenshot: the
tag list matches in order, `POST /active-hash/update/{hash}` carries the
identical summary, and the page's "Download OpenApi.json" button produces that
same file. There is no additional API documentation to obtain.

Two things there are worth knowing:

- **The page has a "Try it out" console.** Anyone logged into the UI can
  execute an endpoint by hand, authenticated as themselves — no credential
  shared with anything, no code. That is the fastest way to run a one-off
  mutation (e.g. `POST /demand-sources/{id}/status`) or to check whether an
  entity exists on the new platform yet.
- **Two other spaces exist in the left nav** — "Supply Integrations" and
  "Admin Knowledge Center" — and neither has been read. Those are the only
  places new material could still be hiding.

Also visible: a "Login as" selector, which is `POST /login-as/{id}` in the
spec, so the account in use holds admin scope.

---

## 8. Questions for Teqblaze

Triaged, because vendor attention is finite. **§8.1 is what to actually send** —
things only they can answer. §8.2 is what the API answers itself once we have
credentials; asking those wastes a round trip.

Nothing here is a report of a fault. As of 2026-08-19 we have never reached
`api.pgammedia.com`, so we have no evidence of anything broken on their side.
These are onboarding unknowns, and two of them gate the write path.

### 8.1 Only Teqblaze can answer these

**Blocking the write path — ask first**

1. **Do the `/update` endpoints replace the whole object or patch it?**
   `POST /supply-sources/{id}/update` and `/demand-sources/{id}/update` take
   what looks like a complete entity. If it is a full replace, any field we
   omit is blanked, which on a live supply source means silently dropping
   floors, allowed-demand lists or IAB blocks. Concretely: can we send back the
   `GET /supply-sources/{id}` response with read-only fields stripped, and if
   so **exactly which fields must be stripped**? A canonical round-trip
   example would settle it. Until this is answered our write path stays gated.

2. **Are the platform's own optimisers currently enabled on any of our
   sources?** Specifically `source.is_smart_floor`, `source.is_dynamic_margin`,
   and `qps_limit.qps_optimization_by`. If the platform is already moving a
   floor or a QPS envelope, a PGAM agent on the same lever gives us two
   controllers fighting. We need to know which are on, what each optimises,
   and on what cadence — so we can divide the levers rather than overlap.

**Operational**

3. **A dedicated read-only API user**, separate from a person's dashboard
   login. Scoped to reporting only, so a leaked credential cannot write and
   nobody's individual account is shared. If roles are configurable, which
   permission set corresponds to read-only?

4. **Rate limits and concurrency.** Requests/sec, concurrent queries per user,
   any per-endpoint caps on the reporting surface. Does a throttled request
   return 429, and does it carry `Retry-After`? The legacy host allowed one
   concurrent query per user; we currently serialise calls with a 0.25s floor
   as a guess.

5. **JWT lifetime and sessions.** The spec's example token is 1 hour and there
   is no refresh endpoint, so we assume re-login. Confirm — and confirm whether
   a second `/login` on the same user **invalidates the first token**. If it
   does, a scheduled job and an ad-hoc script running together will knock each
   other out, and we need either separate users or a shared token cache.

6. **`POST /report/{hash}` — what is the hash? — MOSTLY ANSWERED 2026-08-23,
   by the platform rejecting us.** The first live run of `tbx_revenue_etl`
   returned, on all three grains:

       HTTP 422 {"message": "Hash not found, or no longer available",
                 "errors": {"hash": ["Hash not found, or no longer available"]}}

   So the hash is **server-minted, not client-derived** — our md5 of the
   canonical body was never going to resolve. Note the failure shape: the ETL
   logged three failures and still reported success, leaving
   `pgam_direct.tbx_daily_*` existing and empty. An empty table is a quieter
   symptom than a crash and it is what the recon surfaced first.

   `POST /share/report` is the only endpoint in the spec that turns a
   `ReportRequest` into a `{"hash": ...}`, so the sequence is mint → read, and
   `core/tbx_api.py` now does that (`mint_report_hash`). Corroborating detail:
   `POST /active-hash/update/{hash}` extends a TTL, and a pure function of the
   request body could not expire.

   **Still to confirm with Teqblaze**, because the above is inferred from the
   spec rather than stated by them: (a) is `/share/report` really the intended
   mint for `/report/{hash}`, or is it only for the share-a-link feature and
   there is another door? (b) what is the minted hash's TTL — we assume 5
   minutes and re-mint once on a 422, which works but is a guess; (c) when is
   `/active-hash/update/{hash}` actually required rather than optional?

**Commercial / data**

7. **HUMAN — can the scan be scoped?** Settled already: we hold the contract,
   you operate the integration and don't bill us, and the scan runs pre-bid and
   post-bid. What we still need:
   **(a)** Since we pay per scan, can scanning be **limited** — per supply
   source, per traffic type, or by sampling — or is it all-or-nothing for the
   account? We can see the volume via `risk-metrics.requests_sum` and
   `traffic-report.impressions_sum`; what we lack is a lever. The per-source
   `scanner_settings[]` array appears to cover only the `/scanner-settings`
   vendors, which don't include HUMAN — is that right?
   **(b)** Is pre-bid scanning applied to *every* bid request, or only to
   requests that pass some earlier filter? This is the difference between a
   large bill and a small one.
   **(c)** Does `traffic-report.charge_amount_sum` match what HUMAN invoices
   us, so we can use it as a monthly cross-check?
   **(d)** Which of our HUMAN credentials is the integration wired to, and is
   anything on the platform currently *acting* on an MFA/SIVT/GIVT verdict, or
   is it reporting only?

8. **The other scanners.** Separately from HUMAN: which of the
   `/scanner-settings` vendors (Pixalate, Protected Media, FraudSensor,
   MediaGuard, GeoEdge) are provisioned for us, prebid vs postbid, which are
   currently *enabled* on which sources, and who pays for each — you or us? If
   any are billed to us the same cost question as §8.1.7 applies. If they're
   yours, `blocked_rate` still tells us how much they're doing for us.

9. **API sync / discrepancy.** What is the macro syntax in the sync URL (the
   spec example shows `{%Y-m-d%}`)? Can a partner endpoint requiring auth be
   registered — custom headers, basic auth, a token query param? How often does
   the sync run, and what happens on repeated failure?

10. **Migration off the legacy host. — ANSWERED 2026-08-20.** All three parts
    came back; recorded here with what each one changed.
    **(a) Shutdown timing → our call.** *"The legacy UI and ClickHouse will be
    disconnected with your confirmation. We will not shut them down without your
    approval and we can keep them alive for as long as you need, even after the
    full transfer."* Removes the silent-shutdown risk entirely. Sequencing
    consequence in §1: do not confirm until tranche 1 step 2 passes.
    **(b) IDs → placements stable, inventories not.** *"The mapping remains the
    same. We transferred all placements to the new platform as is, so the IDs and
    settings didn't change. Inventory IDs are different (they don't impact
    anything), placement IDs are the same. You don't need to edit or reconnect
    anything placement-wise."* Unblocks the write path at placement level; see §1
    for what it does and does not give us, and (d) below for what it left open.
    **(c) Report parity → yes, same data.** *"Since the data is fully transferred
    from the old ClickHouse to the new one, you will get the same reports as on
    the old platform."* Makes the reconciliation a confirmation test with an
    expected answer of exact agreement.
    **(d) FOLLOW-UP, still open — publisher and demand-source IDs.** (b) confirms
    placement IDs and excludes inventory IDs, and inventory sits between publisher
    and placement. Our daily ETL keys on **neither**: it keys on `publisher_id`
    and `demand_id`. Are *those* unchanged across the two hosts? Ask it plainly,
    because a reassignment there breaks every join in
    `pgam_direct.tb_daily_publisher_revenue`, `tb_daily_demand_revenue` and
    `tb_daily_publisher_demand_revenue` — silently, by matching the wrong rows
    rather than by failing. Tranche 1 step 2 measures this for us as a side
    effect, so the answer is worth having but not worth blocking on.

11. **A sandbox or test account**, so the write path can be exercised without
    touching live money. This is the cheapest way to answer question 1.

12. **Attribution.** Does `filter.timezone` change bucketing only, or which day
    an impression is attributed to? And does the platform's `profit` / `margin`
    match what appears on an invoice, or are there fees applied later?

13. **A/B testing.** `ab_test_feature` comes back as `Main` / `Test` and the
    filter takes `1` / `2`. How is a test configured, is it drivable via API,
    and which is which?

### 8.2 Don't ask — the API answers these

- **Which modules our account licenses** → `GET /permissions`.
- **Whether HUMAN is configured at all** → `GET /human-report/settings`.
  (The billing question in §8.1.7 still needs them.)
- **The unlabelled `[1, 2, 3]` enums** on `sellers_verification_attr`,
  `adstxt_verification_attr` and `seller_domain_node_rank_id_attr` → the
  `verification-list` and `seller-domain-node-rank` dictionaries exist to label
  exactly these. `scripts/tbx_pull.py` fetches both.
- **The authoritative attribute/metric list** → `GET /report/columns-list`. The
  pull diffs it against this client's constants, so a platform upgrade that
  adds vocabulary shows up as a diff rather than a surprise.
- **`POST /filter-lists/{id}/import-values` body shape** → resolved from the
  spec on 2026-08-19: `multipart/form-data` with a single `import` **file**
  field, not JSON. Our first implementation sent JSON and would have 422'd;
  fixed, along with `add-value` and `remove-value`, which are also multipart
  and take `value[]` **arrays** (so a batch is one call, not one per value).
