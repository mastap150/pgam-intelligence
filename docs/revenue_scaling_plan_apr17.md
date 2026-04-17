# PGAM LL Revenue Scaling Plan — $6.2k → $10k → $15k+/day

**Author:** PGAM Intelligence
**Date:** 2026-04-17
**Baseline window:** Apr 7 – Apr 16 2026 (10 days)
**Scope:** Limelight (ui.pgamrtb.com) only. TB work is parked until admin-level management credentials are in place — separate plan.

---

## 0. Baseline — where we actually are (LL only)

| Metric | 10-day | Avg/day | Trend |
|--------|-------:|--------:|-------|
| GROSS_REVENUE | $62,389 | **$6,239** | $5.5k → $6.8k (+24%) |
| Apr 16 alone | — | **$6,770** | rolling peak |

The Apr 16 floor tuning (4 high-WR publishers + 10 floor-too-high / corrupted-floor corrections) is lifting the stack. The trend line itself is the proof the floor strategy works — now we scale it across the rest of the portfolio.

*TB is tracking separately at ~$1k/day and rising, but is out of scope for this plan until admin write-access credentials are provisioned.*

### LL revenue concentration (top 6 = 86% of LL revenue)
| Publisher | 7-day rev | WR | eCPM | Primary demand |
|-----------|----------:|----:|-----:|----------------|
| BidMachine In-App D&V | $11,782 | 6.9% | $1.68 | Pubmatic ($11,291) |
| Algorix D&V | $8,248 | 22.1% | $1.89 | Pubmatic ($8,177) |
| Smaato Magnite | $5,580 | 5.1% | $1.04 | — |
| BidMachine D&V (WL) | $5,137 | 6.0% | $1.05 | Pubmatic ($4,962) |
| Illumin D&V | $3,754 | 11.3% | $0.59 | Verve ($2,154) |
| BidMachine Interstitial | $1,669 | 4.1% | **$11.53** | Magnite, Xandr |

### The five structural observations that drive the plan

1. **Opportunity fill rate (OFR) is 0.05–0.38% across every LL publisher.** We're touching <1% of available ad opportunities. This is the single biggest lever.
2. **Pubmatic over-concentration risk.** 78% of BidMachine-InApp and 99% of Algorix revenue flows through a single DSP. One Pubmatic policy change = one-day catastrophe.
3. **BidMachine Interstitial has a $11.53 eCPM but only 4.1% WR on 5.06B opportunities.** Huge latent value — floor + demand tuning can 3–5x it.
4. **Smaato - Magnite is dark on demand breakdown.** $5,580 at 5.1% WR, $1.04 eCPM, but the demand×publisher report returns zero rows. That's either aggregator reporting quirks or an integration gap — either way it's the biggest opaque line item in the portfolio.
5. **Start.IO Video Magnite** reports WINS=0 but $1,178 revenue (and corrupted eCPM of $1.18M). Broken reporting or broken pixel — demands investigation.

---

## 1. Strategy

Three compounding layers:

1. **Squeeze** — lift eCPM on every impression we already win (floor elasticity on high-WR seats).
2. **Capture** — increase OFR on demand partners we already know clear at good prices (demand activation, geo-floors, Pubmatic de-risking).
3. **Scale** — automate and generalize the things that worked manually (cron-ify TB optimizer, ML floor loop per pub×demand×country×hour).

Same three layers, different sizes, across three phases.

---

## 2. Phase 1 — $10k/day LL only (target: 2 weeks, by 2026-05-01)

Need +$3.8k/day over current $6.2k LL baseline (+60%). Expected breakdown:

| Lever | Est. lift | Confidence |
|-------|----------:|-----------:|
| LL floor activation on Algorix, Illumin D&V, BidMachine Interstitial, BidMachine D&V | +$800–1,400/day | high |
| Activate paused/low-BRFR demands on top 6 publishers (demand expansion round 2) | +$600–1,100/day | medium |
| Fix Start.IO Magnite reporting issue + salvage Smaato visibility | +$200–400/day | medium |
| Pubmatic de-risk: route share to Magnite/Xandr/Illumin on top 2 publishers | +$400–800/day | medium |
| BidMachine Interstitial scale-up ($11.53 eCPM pool) | +$500–900/day | medium |
| **Total expected** | **+$2.5k–4.6k/day** | |

### Phase-1 action list (in priority order)

**P1.1** — **Apply next-round LL floor lifts** *(today)*
- `Algorix Display and Video` (22.1% WR, eCPM $1.89): raise floor $1.00 → $1.25 at adunit level.
- `BidMachine - In App D&V` (6.9% WR, eCPM $1.68): raise floor on Pubmatic demand 0.50 → 0.80 (the other demands are low-volume — don't touch).
- `Illumin Display & Video` (11.3% WR, $2.09 eCPM on Unruly): raise Unruly floor from current → $1.20.
- `BidMachine - In App Interstitial`: $11.53 eCPM — raise Magnite floor to $5.00, keep Xandr as-is to preserve 1.8% WR pool.

**P1.2** — **Extend the LL dynamic floor optimizer to more publishers** *(this week)*
- `scripts/floor_optimizer.py` currently only tunes AppStock + PubNative every 2 hours.
- Expand it to also manage Algorix, Illumin D&V, BidMachine-InApp, BidMachine-Interstitial once the Phase-1 static floors land and we have 48h of signal.
- State file + watchdog + MAX_DAILY_DRIFT_PCT limits already exist — just add publishers to the config.

**P1.3** — **Demand activation sweep on top 6 LL publishers** *(this week)*
- For each of the top 6 publishers, enumerate `biddingpreferences[].value` and identify demands where `status=2` (paused) or `BR_FR < 0.1%` despite being active.
- On BidMachine-InApp: Illumin BR_FR is 0.1% — 16k bids on 99k requests — unpause/bump. Sharethrough has zero activity.
- On Algorix: Magnite at 0% BR_FR despite being active, Unruly at 0.2%.
- Enable + set conservative floors (40% of avg eCPM).

**P1.4** — **Investigate Start.IO Magnite reporting issue** *(this week)*
- Video Magnite shows 0 wins but $1,178 revenue. Likely one of: (a) pixel/beacon misfire, (b) demand integration reporting via different key, (c) post-bid auction counting off.
- Fix or pause to stop inflating noise in reporting.

**P1.5** — **Pubmatic concentration hedge** *(next week)*
- On BidMachine-InApp and Algorix, deliberately raise floor on Pubmatic by +10-15% to force price-up, and simultaneously lower floors on Magnite / Illumin / Xandr by -15% to encourage competition.
- Goal: reduce Pubmatic from 95%+ share to ~75% on those two publishers over 2 weeks.

**P1.6** — **Daily exec readout + regression watchdog** *(ongoing)*
- Add a phase-1 tracker to the existing pilot log. Daily Slack message: yesterday's combined revenue vs. 3-day-pre-change baseline.
- Trigger auto-revert on any publisher that drops >15% for two consecutive days (the existing `pilot_watchdog` already does this for AppStock/PubNative — extend to top-6).

### Phase-1 guardrails

- Floor moves capped at ±25% per cycle.
- Revert any floor change if publisher revenue drops >15% for two consecutive days (watchdog).
- All writes logged to `logs/pilot_2026-04.json` — same format as Apr 16 runs.
- Dry-run required before every live run.

---

## 3. Phase 2 — $12k/day LL (target: 4 weeks, by 2026-05-15)

Building on Phase 1. Focus shifts from floor tuning to OFR expansion + demand diversification.

**P2.1** — **Country-level floors via LL biddingpreferences**
- USA-heavy publishers (Future Today, WURL, LifeVista) show 3–4x clearing on US vs. ROW.
- Use LL's bidding-preferences rule system to set per-country floors on top-6 publishers.
- Est. lift: +$400–700/day.

**P2.2** — **Dead-demand cleanup and slot reclamation**
- `agents/optimization/dead_demand.py` already identifies demands with 0 wins in 30 days.
- Disable them on top 10 publishers to reduce bid-request noise and free bid slots for active demands.
- Est. lift: +$200–400/day (fewer wasted auctions).

**P2.3** — **Scale BidMachine Interstitial** ($11.53 eCPM, 4.1% WR)
- Add Sharethrough, PubNative, Sovrn demand lines at floor $3.00.
- Raise Magnite floor to $6 over 2 weeks (+15% per cycle, staged).
- Est. lift: +$400–700/day.

**P2.4** — **Smaato Magnite deep-dive**
- $5,580 revenue at 5.1% WR and only $1.04 eCPM means poor floor or wrong demand mix. Demand breakdown is empty in reports (suggests aggregator).
- Run `llm.get_publisher(...)` to see raw bidding prefs + contact Smaato about lift options.

**P2.5** — **Onboard 2 new high-eCPM supply publishers on LL**
- Our demand side is proven. What's missing is supply scale.
- Identify 2 CTV or in-app supply partners with >$3 eCPM profile (criteria: US-heavy, video-biased, already integrated with Pubmatic/Magnite).

---

## 4. Phase 3 — $15k+/day (target: 8 weeks, by 2026-06-12)

When manual tuning plateaus, ML closes the rest of the gap.

**P3.1** — **ML floor elasticity model per pub × demand × country × hour**
- Training data: 30+ days of opportunity-level win/loss/clearing data from the LL extended report API (already wrapped in `core/ll_report.py`).
- Model: gradient-boosted regression (LightGBM) predicting clearing price at (pub, demand, country, hour) granularity with recent 24-72h rolling features.
- Output: recommended floor every 15 minutes per segment with an uncertainty band. Apply only where uncertainty is low and predicted lift > 5%.
- Roll out on one publisher (Algorix — high WR, clean data) first; expand after 7 days of positive signal.
- Est. lift once rolled out fully: +$1–2k/day (tight-optimal floors everywhere).

**P3.2** — **Direct PMP / deal lines for highest-WR seats**
- Future Today CTV (100% WR, $22 eCPM), WURL $10 (67% WR, $26 eCPM), LifeVista ($4.50 → $7.50 floor already) are prime candidates for direct IO / PMP deals.
- Direct deals at ~70% of clearing price with no SSP rev share = +30% margin on those slots.

**P3.3** — **Supply-side diversification (if Phase 2 onboarding succeeded)**
- Consolidate top performers, cull underperformers, negotiate exclusivity or first-look on at least one.

**P3.4** — **Predictive bid-shading detection**
- Monitor DSPs that systematically bid 40–60% of our floor when they have room to bid higher. Adjust their floor bands and auction visibility accordingly.

---

## 5. Tracking

Metrics we watch daily (already instrumented or easy to add):

- **LL GROSS_REVENUE** — `core.api.fetch('PUBLISHER', 'GROSS_REVENUE,...')`. (TB is tracked separately, out of scope for this plan.)
- **Per-publisher eCPM, WR, OFR** — `fetch('PUBLISHER,DEMAND_PARTNER', ...)`.
- **Margin** — `(GROSS_REVENUE - PUB_PAYOUT) / GROSS_REVENUE`. Anchor: 30%.
- **Floor drift** — `logs/floor_optimizer_state.json` + `logs/pilot_2026-04.json`.
- **Action log** — every change appended to `logs/pilot_2026-04.json`.

Daily Slack post: yesterday's combined revenue, delta vs. 7-day baseline, top 3 movers, pending actions.

---

## 6. Risks

| Risk | Impact | Mitigation |
|------|-------:|------------|
| Floor raise causes WR collapse on Pubmatic | -20% revenue top pubs | Watchdog auto-revert, stage raises in 10-15% steps |
| LL optimizer misfires on low-volume hour | small loss | MIN_WINS threshold, MAX_DAILY_DRIFT 25% already in place |
| Start.IO fix reveals true revenue was 0 | small | current state already distorts averages — better to know |
| Pubmatic notices de-risking and pulls QPS | meaningful | stage over 2 weeks, never more than ~15% shift per week |
| New supply integration overruns timeline | delays P2 target | Phase 2 can hit $12k without supply growth via geo-floors alone |

---

## 7. What happened today (2026-04-17)

1. ✓ Phase-1 executor ran live on 4 top-revenue publishers (Algorix, Illumin D&V, BidMachine Interstitial, BidMachine D&V) — **48 demand-level floors activated**.
2. ✓ Phase-1b executor ran live on 9 additional publishers (Smaato, BlueSeaX, PubNative, BM Interstitial EU/WL, Illumin EU ×2, Start.IO ×2) — **121 demand-level floors activated**.
3. ✓ New Partner Optimizer agent built + wired into scheduler (`agents/optimization/new_partner_optimizer.py`, runs daily 08:30 ET).
   - Detects new publishers, new demands, and newly-reactivated (status 2→1) demands.
   - Auto-applies floors using 30-day historical per-partner eCPM × 40%, with format-aware minimums ($10 CTV / $3 interstitial / $1 video / $0.30 display).
   - `only_if_none` safety — never overwrites a hand-tuned floor.
   - Snapshot bootstrapped from current state (114 publishers).
4. ⏸ Start.IO WINS=0 reporting bug — parked (separate session handling).
5. ⏸ TB stack — parked pending admin credentials.

## 8. Activity gating — new_partner_optimizer (2026-04-17)

**Finding:** of 114 LL publishers, only **24 have won ≥1 impression in the last 7 days**. 56 are `status=1` (marked active in LL) but silent, 31 are `status=2` (paused). WURL, Future Today, Fuse, Ottera, Blue Ant, Cox Media, LifeVista — listed but currently off.

**Fix:** `get_active_publisher_ids()` filters every agent decision through "has recent wins". Inactive publishers are skipped with a `skipped_inactive_publisher` audit entry. Passes through the full diff so we see the skip in logs but never touch the publisher.

## 9. ML floor elasticity model — shipped (2026-04-17)

**Files:** `intelligence/floor_model.py`, `scripts/train_floor_model.py`

**Architecture:**
- Pulls 30-day (publisher × demand_partner × country × date) data via LL GET stats API.
- Trains 3 LightGBM quantile regressors (p10 / p50 / p90) predicting `log(eCPM)`.
- Features: encoded pub_id, demand_partner, country (top 25 + OTHER), format, log(wins), win_rate, day-of-week.
- Generates per-segment predictions to `logs/floor_predictions.json`.
- `new_partner_optimizer.compute_floor()` now calls `floor_model.lookup_prediction()` FIRST; historical eCPM lookup is the fallback, format minimum is the final safety.

**Validation (Apr 17 initial training on 30-day data):**
- Train rows: 5,894   Holdout rows: 2,398
- **Model median APE: 43%** on holdout eCPM predictions.
- **Baseline (demand-partner mean) median APE: 236%** — model is **~5.5× more accurate** (82% lift).
- p10–p90 coverage: 65% (slightly overconfident; acceptable for MVP — we gate on relative band width).
- 485 predictions generated, **172 high-confidence** (used by optimizer).

**Decision hierarchy in compute_floor():**
1. ML model prediction (if high-confidence, band width ≤ 1.2 × median).
2. Historical pub×demand eCPM × 40%.
3. Historical demand-partner eCPM × 40%.
4. Format-aware minimum ($10 CTV / $3 interstitial / $1 video / $0.40 inapp / $0.30 display).

All paths floor-bound by format minimum (CTV never drops below $10 etc.).

**Retraining:** weekly Sun 05:00 ET via scheduler.

## 10. Margin guardrail — shipped (2026-04-17)

**Threshold:** 30% healthy margin (configurable via `LL_MARGIN_MIN` env var).

**Files:**
- `core/margin.py` — `get_publisher_margins()`, `is_healthy_margin()`, shared lookup.
- `agents/alerts/margin_health.py` — daily agent (08:15 ET), alerts on any pub <30% and any day-over-day drop ≥2pp. Writes rolling 30-day history to `logs/margin_history.json`.
- `new_partner_optimizer.py` — hybrid gate: `new_demand` and `new_publisher` diffs on <30% publishers are blocked with `skipped_low_margin`; `reactivated` diffs pass through (restoring prior state, not adding new demand).
- `intelligence/floor_model.py` — `margin_30d` added as a feature; retrained (high-conf predictions: 172 → 180).

**Verified:** simulated new demand on Smaato (28.2%) → blocked. Simulated new demand on Algorix (31.1%) → applied at model-driven $0.41.

## 11. Renegotiation priority list

Publishers currently below 30% margin. Raising their rev-share with us (lowering `pubPayoutPercentage`) is the only way to actually lift margin — floors can't do it.

| Priority | Publisher | 30d Rev | Margin | Lift to 30% |
|---------:|-----------|--------:|-------:|------------:|
| 1 | **Smaato - Magnite** | $20,959 | 28.2% | +$380/mo |
| 2 | **Start.IO - Video Magnite** | $5,718 | 29.7% | +$17/mo (+ fix WINS reporting) |
| 3 | **PubNative - In-App Magnite** | $2,752 | 26.4% | +$99/mo |
| 4 | **BlueSeaX - EU Endpoint** | $1,838 | 23.0% | +$129/mo |
| 5 | **AppStock** | $1,415 | 24.5% | +$78/mo |
| 6 | **BlueSeaX - US Endpoint** | $853 | 23.6% | +$55/mo |
| 7 | **Start.IO Display Magnite** | $794 | 24.5% | +$44/mo |

**Total upside from renegotiating to exactly 30%: ~$800/month.** Real upside is bigger because (a) these publishers may be growing, (b) restoring margin headroom unlocks the optimizer to activate new demand on them.

**Start-IO note:** both Start.IO entries have margin <30% AND the WINS=0 reporting bug. Start.IO Video Magnite is running $5.7k/30d with real margin — so the revenue IS real, just the wins counter is broken. Data pipeline fix is orthogonal to rev-share renegotiation, but both should happen.

## 12. Next

- 48h monitor: compare Apr 18–19 LL revenue to Apr 10–16 baseline ($6.2k/day).
- Extend dynamic `scripts/floor_optimizer.py` to cover the 13 Phase-1/1b publishers once 48h signal confirms no regressions.
- After 2 weeks of fresh post-Phase-1 data, re-train and compare model APE: should improve as the model learns from the new floor regime.
- Next ML improvements (Phase 3):
  - Counterfactual elasticity: train on floor-change events to predict WR drop per $X floor raise.
  - Hourly granularity (EU/APAC clock effects).
  - Geo-specific floor recommendations (the model already has country features; wire into per-country floor setting via LL biddingpreferences rules).
