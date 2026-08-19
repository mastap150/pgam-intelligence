# Optimisation cadence, alerts, and the QPS cut rule

How PGAM's marketplace gets watched and tuned. Written 2026-08-19 against the
30-day window 2026-07-20 → 2026-08-18, using the real data in Neon
`pgam_direct.tb_daily_*`.

Companion documents: `docs/teqblaze-new-platform.md` §4 for the lever
inventory, §7 for the migration roadmap.

---

## 0. The finding that set the priorities

The window contained a 43% revenue decline, and attribution
(`scripts/tb_whatchanged.py --pivot 2026-08-11`) put nearly half of it on one
relationship:

| Demand source | Before | After | Change | eCPM before → after | Share of decline |
|---|---|---|---|---|---|
| `Zmaticoo - Advetisi #2318` | $28,874 | $1,111 | **−96.2%** | 7.341 → 7.551 | 24.7% |
| `LoopMe - Advetisi #2408` | $30,106 | $3,530 | **−88.3%** | 6.544 → 6.433 | 23.6% |

**The eCPM did not move.** Both buyers still pay ~$6.50–7.55, they simply stopped
taking volume — impressions fell 96% and 88%. Same signature on
`Cas.ai Display #289` (−94.6%), `LoopMe - Cas.ai #2417` (−94.7%) and
`LoopMe - Smaato US #274` (−94.7%): volume to near-zero, price intact.

On the device cut, `roku os` fell **−98.1%** and `tizen` **−98.4%** — and
Advetisi's ~$7 eCPM matches CTV pricing, so Advetisi was very likely the CTV
supply. Advetisi appears as a matched supply/demand pair (partner
`Advetisi - Zmaticoo #264` and demand `Zmaticoo - Advetisi #2318` report
identical gross), which means it is a direct arrangement rather than open-market
flow.

**Why this shapes everything below.** A floor, a margin band, a QPS cap — none
of them recover a buyer who stopped buying at an unchanged price. This is a
commercial question. The lesson for the cadence is that **the first alert has to
distinguish a price problem from a volume problem**, because they route to
completely different people.

---

## 1. Alerts

Three tiers. Everything here is computable from data we already hold — none of
it waits on TBX credentials.

### 1.1 Revenue regression (daily, 08:30 ET)

| | |
|---|---|
| Fires when | 7-day rolling gross falls >15% against the prior 7 days |
| Escalates when | the fall is >30%, or a single partner or demand source loses >50% of its gross while its eCPM moves <10% |
| Payload | the top rows from `tb_whatchanged.py`, with share-of-decline, and the verdict **volume / price / both** |
| Routes to | commercial if volume-driven at flat price; engineering if eCPM moved or render rate dropped |

The volume-vs-price split is the whole point. Flat eCPM with collapsed volume is
a buyer or an integration, not a yield problem — that is what would have caught
Advetisi on about day three instead of day eight.

### 1.2 Delivery health (daily, 08:30 ET)

| Signal | Threshold | Why |
|---|---|---|
| Render rate (`imps / wins`) | <80% for a source over 100k wins/day | Two Verve video endpoints sit at 17–19%; healthy sources are 93–99% |
| Zero-win endpoints | >1B requests, 0 wins, 3 days running | Three Illumin endpoints burned 17.3B requests for nothing |
| Margin drift | blended margin moves >3 points week-on-week | Margin held at ~31% through the decline; a move would mean something different |
| Partner goes dark | a partner with >$100/day drops to <10% of its 7-day mean | Catches a dead integration before month-end |

### 1.3 Platform-side alerts (once TBX credentials land)

`POST /alerts/store` fires natively on `ssp_requests` / `requests` /
`responses` / `imps` / `ssp_price` / `dsp_price` crossing a threshold over a
1/4/12/24-hour window, to email or **Slack**, scoped by DSP, endpoint or
company. Move the coarse "requests fell off a cliff" alarms there — the platform
sees it sooner than a daily ETL can, and it frees our agents for judgement
rather than heartbeats.

---

## 2. Cadence

**Nothing on this schedule changes anything.** Every job reads, works out what
it would recommend, and hands that to a person. No write tier is scheduled, and
none will be until the promotion gate in §3.5 is met.

| When | What | Mode |
|---|---|---|
| Daily 08:30 ET | `marketplace_digest.py` — regression, flat-price collapses, render health, margin drift, QPS proposals | Recommend |
| Weekly Mon 09:00 ET | `tb_headroom.py` — full headroom report | Report |
| Weekly Mon 09:00 ET | `qps_waste_sentry.py` — QPS proposal set | Recommend |
| Fortnightly Wed | Review the standing proposals; **a person applies what they agree with** | Manual |
| Monthly 1st | Recompute the GPM baseline and band thresholds | Config |
| Quarterly | Re-test anything previously cut, 7-day window | Manual |

Three deliberate choices:

- **A proposal must survive two consecutive sweeps** before it is worth acting
  on. A rule reacting to one week would have recommended cutting half the
  marketplace during the Advetisi decline.
- **Act on Wednesdays, never Friday.** Nobody wants to discover a bad cut on a
  Saturday, which is exactly how the April incident got expensive.
- **The digest names an owner per finding** — commercial or engineering. A
  volume collapse at flat price and an eCPM slide are different problems with
  different fixes, and routing them to the same person wastes both.

---

## 3. The QPS cut rule

### Why cutting, not throttling

Teqblaze does not shape traffic. There is no "send this DSP 30% of what it gets
today" — the controls are a hard `qps_limit`, a `status` toggle, a routing
change, or a geo blacklist. All binary or near-binary. So waste cannot be tuned
away gradually; it has to be cut, and cutting is only safe when the decision
follows a stated rule rather than a judgement call in the moment.

### The metric

**GPM — gross revenue per million bid requests.** Request volume is the scarce
resource: every request costs QPS capacity whether or not it returns a dollar.

```
GPM = gross_revenue / (bid_requests / 1_000_000)
```

Measured over the window: ~**405 billion** bid requests for $357.5k, a blended
**$0.88 per million**. Individual setups ran from **$13.70/M**
(`Advetisi - Zmaticoo`) to **$0.005/M** (`BidFuse CTV AdPrime`) — a **2,800×**
spread.

### The bands

Each condition must hold for the **whole 14-day observation window**, not on any
single day.

| Band | Condition | Action |
|---|---|---|
| **CUT** | GPM < 10% of blended **and** gross < $100 in window | Disable the setup |
| **CAP** | GPM < 25% of blended **and** gross ≥ $100 | Hard `qps_limit` at 25% of current volume, re-measure in 14 days |
| **WATCH** | GPM < 50% of blended | Report only |

### What that catches today

Ten setups fall in the CUT band: **41.5 billion requests — 10.2% of all QPS —
returning $2,407, or 0.67% of gross.** Freeing a tenth of our request capacity
for two-thirds of one percent of revenue is the clearest trade on the board.

`BidFuse CTV AdPrime #304` is the extreme: 874M requests for **$4.30**.

### 3.5 Promotion gate — what would justify automating this

The rule above is implemented and tested, and it stays advisory. Automating it
is a separate decision, and these are the conditions that would earn it. All of
them, not a majority:

1. **Three months of proposals reviewed by a person**, with a record of how
   often the recommendation was accepted. If a human overrides the rule
   regularly, the rule is wrong and automating it just makes it fast.
2. **Zero false positives on a live partner** across that period. One
   automated cut of a healthy partner costs more than a quarter of saved QPS.
3. **The Teqblaze ID mapping exists** (`docs/teqblaze-new-platform.md`
   §8.1.10b), so an action addresses the entity it was reasoned about.
4. **A verified rollback.** Teqblaze cannot shape traffic, so a cut is binary;
   we need to have re-enabled a source and watched it recover before trusting
   the reverse direction.
5. **The Advetisi question is closed.** While a 43% decline is unexplained, no
   automated rule should be changing the same marketplace — it makes the next
   attribution impossible.

Until then the sentry has no write path at all: not a flag, not a gated one.
A flag whose only purpose is to attempt writes is an invitation.

### Safeguards

- **Grace period, 21 days.** New integrations ramp, wait on seat approval, and
  look exactly like waste before they work.
- **Blast radius: 5 actions per run, and never more than 15% of total request
  volume in one run.** A rule that *can* cut everything at once eventually
  will, on the day the data is wrong.
- **Partial-coverage guard.** A setup active fewer than 14 of the 14 days is
  downgraded to WATCH — it may be paused rather than wasteful. This is what
  stops the rule cutting Advetisi mid-collapse.
- **Never-cut list.** Contract commitments and `core/partner_freeze.py` (Unruly
  dp=5, BidMachine QPS cap) override the numbers, always.
- **Quarterly re-test.** Anything cut is re-enabled for 7 days once a quarter
  unless explicitly marked dead. One bad fortnight must not permanently remove a
  seasonal partner.
- **Ledger.** Every proposal and action recorded with its evidence, so a
  revenue move afterwards is attributable rather than a guess.

All parameters are env-overridable (`PGAM_QPS_*`) so the cadence can be tuned
without a code change. Defaults are the values argued for above.

---

## 4. Automation, by blast radius

| Tier | Scope | Status |
|---|---|---|
| 1 | Alerts and recommendations — `marketplace_digest.py`, `tb_headroom.py`, `qps_waste_sentry.py`, `tb_whatchanged.py` | **Built, read-only**, no new credentials |
| 2 | Demand-side pricing — `geo_settings.bid_floor[]`, `qps[]`, `blacklist[]`, `margin_type`, `spend_limit` | Needs TBX credentials. No publisher contract exposure, so a mistake costs fill, not a breach |
| 3 | Routing and capacity — `is_allowed_sources`, `qps_limit`, placement `status` | Needs tier 2 proven |
| 4 | Supply-side floors — `placements[].floor_price`, per-placement margin | **Blocked**: `PROTECTED_FLOOR_MINIMUMS` empty (legacy IDs don't map), round trip unverified, `is_smart_floor` state unknown. That combination caused the April incidents |

### Wiring

Nothing here is registered in `scheduler.py` yet. When it is, follow the repo
convention: an env flag per job, default off.

```
PGAM_MARKETPLACE_DIGEST_ENABLED=0    # daily recommendations to Slack
PGAM_QPS_SENTRY_ENABLED=0            # weekly proposal set
```

There is no apply flag, because there is no apply path.
`PGAM_OPTIMIZER_AUTO_APPLY=0` and `PGAM_FLOOR_OPTIMIZER_ENABLED=0` are both
still off after the April incidents; a rule proposing irreversible-in-effect
cuts does not get a softer default than the ones that caused them.

---

## 5. Open dependencies

1. **The Advetisi question is commercial, not technical.** Did the campaign end,
   did budget exhaust, or did the integration break? At an unchanged eCPM, no
   lever in this document recovers it. Nothing else on this list is worth as
   much.
2. **CTV supply.** Roku and Tizen went to near-zero. CTV was 2.3% of impressions
   and 13.6% of revenue at 8× the eCPM of mobile — if Advetisi was the CTV
   route, replacing it is the single biggest revenue action available.
3. **The Teqblaze ID mapping** (`docs/teqblaze-new-platform.md` §8.1.10b).
   Every finding here carries legacy IDs. Tier 2 onward cannot execute until
   those map to entities on `api.pgammedia.com`.
4. **Two ETL gaps found while building this**: the partner×country table is
   missing ~92% of Dexerto's volume, and `tb_segments_etl` drops rows with
   `gross_revenue <= 0`, which hides zero-win pairs entirely.
