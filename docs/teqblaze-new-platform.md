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

So verification runs on **GitHub Actions**, which has open network. Setup is
two steps, in this order:

1. **Add the secrets** — Settings → Secrets and variables → Actions → New
   repository secret: `TBX_EMAIL` and `TBX_PASSWORD`.
2. **Land the workflow on `main`.** GitHub only offers `workflow_dispatch` for
   workflows present on the default branch, so `.github/workflows/tbx-probe.yml`
   is not dispatchable — and does not appear in the Actions tab — while it lives
   only on a feature branch. Merge, then run.

Then: Actions → "TBX Data Pull (new Teqblaze platform)" → Run workflow.

The workflow is read-only. It publishes results three ways: the full
digest in the job log (which is how a session reads them back), a capability
matrix in the run summary, and full per-surface JSON as a 7-day artifact.

Locally, on a machine that has `.env` and network access:

    python3 scripts/tbx_probe.py --reports        # does each surface answer
    python3 scripts/tbx_pull.py --days 7 --outdir /tmp/tbx   # pull the data

---

## 1. This is a second, different platform — not an upgrade path

PGAM now talks to two Teqblaze-family hosts, and they are not the same system:

| | legacy | new |
|---|---|---|
| Host | `ssp.pgammedia.com/api` | `api.pgammedia.com` |
| Auth | token as a URL path segment | `POST /login` → JWT, `Authorization: Bearer` |
| Modules | `core/tb_api.py`, `core/tb_mgmt.py` | `core/tbx_api.py`, `core/tbx_mgmt.py` |
| Env vars | `TB_EMAIL`, `TB_PASSWORD`, `TB_USER_ID` | `TBX_EMAIL`, `TBX_PASSWORD` |
| Entities | inventory → placement | company → supply source → placement; company → demand source |
| Report shape | flat `adx-report` query params | `POST /report/{hash}` with attributes + metrics + filters |
| Writes | form-encoded, per-field endpoints | JSON, whole-object `.../update` |

**IDs are not portable between them.** A placement ID from `tb_mgmt` means
nothing here. Contract-floor maps, freeze lists and ledger entries all have to
be re-keyed — `PROTECTED_FLOOR_MINIMUMS` in `core/tbx_mgmt.py` starts empty for
exactly that reason.

Whether the two hosts front the same inventory, or the new platform is a
migration target, is a question for Teqblaze. Until that is answered, treat
them as separate books. **Do not delete or repoint the legacy `tb_*` modules** —
`tb_floor_nudge` and `tb_contract_floor_sentry` still run against the legacy
host.

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
`blocked_rate` per supply source — is the fraud scanning we pay for blocking
anything, and where. `postbid` gives `scan_attempts` / `scans`.

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
| Country blocklist | `geo_settings.blacklist[]` | `set_demand_geo_blacklist` |
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

Nothing is wired into `scheduler.py`. That is deliberate — wiring an autonomous
writer against an unverified platform is a separate decision.

Setup: add `TBX_EMAIL` + `TBX_PASSWORD` in the **Render** dashboard
(Environment → Add Environment Variable) on `pgam-intelligence-scheduler`.
Declared in `render.yaml` with `sync: false`, and in `.env.example` for local
dev. Not Vercel — see `CLAUDE.md`.

---

## 6. Before enabling writes

The write path is read-modify-write against endpoints that replace the **whole
object**: `POST /supply-sources/{id}/update` takes the entire
`SupplySourceRequest`. A partial body blanks every field left out. Three things
guard that, and one of them needs human confirmation:

1. `_strip_read_only` drops the fields the read schema returns but the write
   schema rejects — `id` and source-level `margin_type`/`margin_min`/`margin_max`
   on supply; `id` and `operation_systems` on demand.
2. `_assert_no_key_loss` refuses the write if the merged payload would drop any
   key that was present before.
3. **Unverified:** whether the platform actually accepts the round trip.
   `python3 scripts/tbx_probe.py --diff-shape supply:<id>` prints the GET
   response beside the exact payload an update would POST. If anything beyond
   the known read-only fields appears under `DROPPED`, do not set
   `TBX_ALLOW_WRITES`.

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
2. ETL the new report into Neon `pgam_direct` beside the legacy TB tables, with
   `supply_source × demand_source × country × date` granularity. Do not merge
   the two platforms' tables until §1 is answered.
3. Reconcile: one day of `sellers-validation` + `ads-txt-verification` against
   one day of our own compliance findings. That comparison decides how much of
   `agents/compliance/crawlers` retires.

**Tranche 2 — new intelligence, still read-only**
4. IVT report from `human-report/risk-metrics` — per-domain MFA/SIVT/GIVT into
   the compliance digest. Genuinely new signal.
5. Drop-reason report from `bids-overview` — feed named reasons into
   `fill_funnel`.
6. Timeout and render-rate monitor per DSP — `timeout_rate` and `render_rate`
   have no legacy equivalent.
7. Register partner API-sync URLs, then a discrepancy digest. Highest
   effort-to-payoff ratio of anything here: it retires manual recon.

**Tranche 3 — writes, supervised**
8. Populate `PROTECTED_FLOOR_MINIMUMS` from the contract sheets.
9. Confirm the round trip with `--diff-shape` on one supply and one demand
   source.
10. Geo floor agent on `geo_settings.bid_floor`, proposing to Slack for
    supervised review — mirroring `PGAM_OPTIMIZER_AUTO_APPLY=0`, not
    auto-applying. Demand-side floors carry no publisher contract exposure,
    which makes this the safest first writer.
11. Schain enforcement: compliance audit finds an unverified chain →
    `set_demand_schain_policy` closes it. Propose first, apply on sign-off.
12. Filter-list enforcement for `adomain` / `crid` brand safety.

**Not recommended yet**
- Any autonomous floor writer on the supply side. Contract exposure, an empty
  contract-floor map, and an unverified round trip is the exact combination
  that produced the April incidents.
- Retiring legacy `tb_*` modules. They run against a different host.

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

6. **`POST /report/{hash}` — what is the hash?** Client-generated or must it
   come from the server first? We currently send an md5 of the canonical
   request body so paging is stable. What is the cached result set's TTL, and
   when is `POST /active-hash/update/{hash}` actually required?

**Commercial / data**

7. **HUMAN.** Is the integration live on our account? `traffic-report` returns
   `charge_amount_sum`, so: are we billed per scan, at what rate, and is
   scanning 100% of traffic or sampled? What MFA/SIVT/GIVT thresholds do they
   consider actionable, and does anything enforce automatically today?

8. **Scanners.** Which are provisioned for us, prebid vs postbid, and what does
   each cost? `scanner-statistics` exposes `blocked_rate`, so we can measure
   value — we just need to know what we are paying for.

9. **API sync / discrepancy.** What is the macro syntax in the sync URL (the
   spec example shows `{%Y-m-d%}`)? Can a partner endpoint requiring auth be
   registered — custom headers, basic auth, a token query param? How often does
   the sync run, and what happens on repeated failure?

10. **Relationship to the legacy host.** Do `api.pgammedia.com` and
    `ssp.pgammedia.com` front the same inventory and demand, or two separate
    marketplaces? Is either deprecated, and on what timeline? Is there **any
    ID mapping** between legacy inventories/placements and new supply
    sources/placements? Without one, contract floor minimums (9 Dots $1.70)
    have to be re-derived by hand on the new platform.

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
