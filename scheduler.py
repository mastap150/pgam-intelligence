"""
scheduler.py

PGAM Intelligence — main scheduler process.

Runs as a single long-lived process. Uses the `schedule` library to trigger
every agent at the correct time (US/Eastern). All agents are self-deduplicating
so it is safe to call them more frequently than they post — they will simply
skip when already sent.

Start:
    python scheduler.py

Deployment:
    Render  → set Start Command to: python scheduler.py
    Railway → same
    Local   → python scheduler.py  (runs indefinitely)

Environment:
    All credentials are read from .env (or host environment variables):
        LL_API_BASE_URL, LL_CLIENT_KEY, LL_SECRET_KEY, LL_UI_EMAIL, LL_UI_PASSWORD
        SLACK_WEBHOOK
        ANTHROPIC_API_KEY
        SENDGRID_KEY, EMAIL_FROM
"""

import gc
import time
import traceback
from datetime import datetime

import pytz
import schedule
from dotenv import load_dotenv

load_dotenv(override=True)

ET = pytz.timezone("US/Eastern")


def _run(agent_name: str, fn):
    """Wrapper that catches all exceptions so one failing agent never
    kills the scheduler. Forces gc.collect() after each agent so we
    don't pile up memory across the hourly tick — Render's starter
    plan is 512MB and we were OOM'ing at the top of every hour because
    multiple heavy ETLs ran back-to-back."""
    def job():
        now = datetime.now(ET).strftime("%Y-%m-%d %H:%M ET")
        print(f"[scheduler] ▶ {agent_name}  ({now})")
        try:
            fn()
        except Exception:
            print(f"[scheduler] ✗ {agent_name} raised an exception:")
            traceback.print_exc()
        else:
            print(f"[scheduler] ✓ {agent_name} completed.")
        finally:
            # Force a full collection. Heavy ETLs hold 400k+ row lists
            # in memory; without this they linger until the next
            # collection cycle and the *next* heavy ETL pushes us OOM.
            gc.collect()
    job.__name__ = agent_name
    return job


# ---------------------------------------------------------------------------
# Lazy-import each agent's run() so import errors in one agent don't stop
# the scheduler from starting.
# ---------------------------------------------------------------------------

def _import(module_path: str, func_name: str = "run"):
    """Return a named function from a dotted module path, or a no-op on failure.

    Defaults to `run` for the usual pattern. Pass `func_name=` to grab a
    different top-level callable (used for intelligence.collector.run_geo)."""
    import importlib
    try:
        mod = importlib.import_module(module_path)
        return getattr(mod, func_name)
    except Exception as exc:
        print(f"[scheduler] WARNING: could not import {module_path}.{func_name}: {exc}")
        return lambda: None


# ---------------------------------------------------------------------------
# Schedule
# ---------------------------------------------------------------------------
#
# All times are US/Eastern.  The schedule library uses local system time, so
# we wrap each call in a timezone check to ensure ET correctness regardless
# of the server's system clock timezone.
#
# Pattern used: call each agent every N minutes; the agent itself checks the
# clock and day-of-week internally and skips if it's not the right time.
# This is simpler and more robust than trying to schedule exact times, because
# it means a scheduler restart never misses an alert.
#
# Agent                  | Frequency  | When it actually fires
# -----------------------|------------|------------------------------------------
# partner_revenue_etl    | every 60m  | UPSERTs LL daily rollup into Neon (today + yesterday)
# ll_dimensions_etl      | every 60m  | UPSERTs LL per-publisher × domain/bundle rollups
# ll_4dim_etl            | every 60m  | UPSERTs LL per-publisher × domain/bundle × demand rollups
# country_revenue_etl    | every 60m  | UPSERTs LL+TB country breakdown for the Geography section
# ll_segments_etl        | every 60m  | UPSERTs LL device/os, hour, and funnel rollups
# tb_segments_etl        | every 60m  | UPSERTs TB pub×demand, pub×country, OS rollups
# dashboard_alerts       | daily 9am  | posts anomalies + recon drift + DSP health to Slack
# tb_revenue_etl         | every 60m  | UPSERTs TB publisher+demand rollups into Neon
# ll_revenue             | every 60m  | any time (55-min cooldown inside agent)
# tb_revenue             | every 60m  | any time (55-min cooldown inside agent)
# revenue_pace           | every 4h   | weekdays 9 AM–8 PM ET (guard inside)
# opp_fill_rate          | every 4h   | daily summary + critical repeat
# floor_gap              | daily 8am  | once per day
# demand_saturation      | daily 8am  | once per day
# app_revenue            | daily 8am  | once per day
# publisher_monetization | daily 8am  | once per day
# action_tracker         | daily 8am  | once per day
# daily_email            | daily 7am  | once per day (hour gate inside)
# win_rate_maximizer     | daily 8am  | once per day
# floor_elasticity       | Mon 8am    | Monday only (weekday guard inside)
# weekly_review          | Mon 7am    | Monday only (weekday + hour guard inside)
# ctv_optimizer          | Mon 8am    | Monday only (weekday guard inside)
# demand_expansion       | Wed 8am    | Wednesday only (weekday guard inside)
# geo_expansion          | Thu 8am    | Thursday only (weekday guard inside)
# weekend_recovery       | Fri 8am    | Friday + Sunday (weekday guard inside)
# fill_funnel            | daily 8:15 | Mon-Fri (weekday guard inside)
# dead_demand            | Mon 8:15   | Monday only (weekday guard inside)
# geo_leak               | Thu 8:15   | Thursday only (weekday guard inside)
# pilot_snapshot         | daily 9am  | PubNative + AppStock daily baseline (dedup inside)
# pilot_watchdog         | daily 9:30 | auto-revert floors if revenue drops >15 % vs baseline
# floor_optimizer        | every 2h   | dynamic floor adjust based on win rate vs yesterday
# revenue_gap            | Sun 9am    | Sunday only (weekday guard inside)
# monthly_forecast       | 1/10/20th  | day-of-month guard inside
# ---------------------------------------------------------------------------

def setup_schedule():
    # ETL: lands LL daily revenue into Neon for the Partner Revenue Dashboard
    # at admin.pgammedia.com/admin/partner-revenue. Hourly UPSERT of today +
    # yesterday; backfill via `python -m agents.etl.partner_revenue_etl --backfill 30`.
    partner_revenue_etl    = _import("agents.etl.partner_revenue_etl")
    # LL per-publisher × domain/bundle drill-down ETL. Powers the
    # Executive Dashboard's "drill into a brand" panel. ~88% of LL's
    # raw rows are zero-revenue and get filtered out at the ETL layer.
    ll_dimensions_etl      = _import("agents.etl.ll_dimensions_etl")
    # 4-dim drill-down ETL: per-publisher × domain/bundle × demand-partner.
    # Powers the Executive Dashboard's "which DSP paid for this domain on
    # this brand" view. Filters zero-rev rows aggressively (~97% drop).
    ll_4dim_etl            = _import("agents.etl.ll_4dim_etl")
    # Geography ETL — feeds the Executive Dashboard's country tab.
    country_revenue_etl    = _import("agents.etl.country_revenue_etl")
    # Device/OS, hour-of-day, and funnel rollups for the Executive
    # Dashboard's Device, Daypart, and Funnel sections.
    ll_segments_etl        = _import("agents.etl.ll_segments_etl")
    # Geo Intelligence cross-cuts: country × {device_type, OS, hour}.
    # Powers /admin/finance/geo-intelligence. LL only — TB's adx-report
    # doesn't expose hour, device_type, state, or DMA dimensions.
    ll_geo_segments_etl    = _import("agents.etl.ll_geo_segments_etl")
    # TB ad_format breakdowns (banner / video / native / rewarded) ×
    # country + ssp_name. Powers the Format section on Geo Intelligence.
    tb_ad_format_etl       = _import("agents.etl.tb_ad_format_etl")
    # TB hour-of-day breakdowns (via day_group=hour). Daypart heatmap
    # + Geo Intelligence hour panel now show LL + TB combined.
    tb_hour_etl            = _import("agents.etl.tb_hour_etl")
    # App-name enrichment — resolves bundle IDs (numeric iOS App
    # Store IDs and reverse-DNS like com.x.y) to readable app names
    # via iTunes Search API. Powers the bundle drill-down on demand
    # detail pages. iTunes is rate-limited so we tick daily, not hourly.
    app_name_enrichment    = _import("agents.enrichment.app_name_enrichment")
    # TB richer per-publisher rollups (pub×demand, pub×country, OS)
    # — feeds symmetric drill-downs and enriches the Geography &
    # Device sections with TB data.
    tb_segments_etl        = _import("agents.etl.tb_segments_etl")
    # Daily Slack post summarising dashboard-derived alerts: anomalies
    # (revenue / CPM swings), recon drift, DSP win-rate WoW. Daily-
    # deduped so the once-an-hour scheduler tick can't spam.
    dashboard_alerts       = _import("agents.alerts.dashboard_alerts")
    # TB analogue of partner_revenue_etl. Pulls TB DATE,PUBLISHER and
    # DATE,DEMAND_PARTNER breakdowns into pgam_direct.tb_daily_publisher_revenue
    # and pgam_direct.tb_daily_demand_revenue. Two-dim breakdown was tried first
    # but ssp.pgammedia.com times out on the cell-count fan-out.
    tb_revenue_etl         = _import("agents.etl.tb_revenue_etl")
    ll_revenue             = _import("agents.alerts.ll_revenue")
    # TB analogue of ll_revenue — hourly Slack snapshot of Teqblaze
    # revenue, pacing, margin, top publishers, and MTD vs $1M combined goal.
    # Self-skips cleanly when TB credentials aren't configured.
    tb_revenue             = _import("agents.alerts.tb_revenue")
    revenue_pace           = _import("agents.alerts.revenue_pace")
    opp_fill_rate          = _import("agents.alerts.opp_fill_rate")
    floor_gap              = _import("agents.alerts.floor_gap")
    demand_saturation      = _import("agents.alerts.demand_saturation")
    app_revenue            = _import("agents.alerts.app_revenue")
    publisher_monetization = _import("agents.alerts.publisher_monetization")
    action_tracker         = _import("agents.alerts.action_tracker")
    daily_email            = _import("agents.reports.daily_email")
    daily_recommendations  = _import("agents.alerts.daily_recommendations")
    win_rate_maximizer     = _import("agents.reports.win_rate_maximizer")
    floor_elasticity       = _import("agents.reports.floor_elasticity")
    weekly_review          = _import("agents.alerts.weekly_review")
    # Auto-generated weekly business digest. Pulls the dashboard's
    # /weekly-digest endpoint (deterministic — no LLM) and posts to
    # Slack. Self-gates to Mon 13:00–15:00 UTC and dedupes by ISO week.
    weekly_digest          = _import("agents.alerts.weekly_digest")
    ctv_optimizer          = _import("agents.alerts.ctv_optimizer")
    demand_expansion       = _import("agents.alerts.demand_expansion")
    geo_expansion          = _import("agents.alerts.geo_expansion")
    weekend_recovery       = _import("agents.alerts.weekend_recovery")
    revenue_gap            = _import("agents.alerts.revenue_gap")
    monthly_forecast       = _import("agents.alerts.monthly_forecast")
    # New extended-API agents
    fill_funnel            = _import("agents.optimization.fill_funnel")
    dead_demand            = _import("agents.optimization.dead_demand")
    geo_leak               = _import("agents.optimization.geo_leak")
    publisher_optimizer      = _import("agents.optimization.publisher_optimizer")
    dsp_optimizer          = _import("agents.optimization.dsp_optimizer")
    ssp_company_optimizer  = _import("agents.optimization.ssp_company_optimizer")
    geo_floor_optimizer    = _import("agents.optimization.geo_floor_optimizer")
    # Revenue-growth agents (added 2026-04-24)
    partner_churn_radar    = _import("agents.alerts.partner_churn_radar")
    demand_concentration   = _import("agents.alerts.demand_concentration")
    yield_compression      = _import("agents.alerts.yield_compression")
    dayparting_floor       = _import("agents.optimization.dayparting_floor_agent")
    size_gap_agent         = _import("agents.optimization.size_gap_agent")
    placement_status_agent = _import("agents.optimization.placement_status_agent")
    placement_autocreate   = _import("agents.optimization.placement_autocreate_agent")
    blocked_domains_agent  = _import("agents.optimization.blocked_domains_agent")
    revenue_guardian       = _import("agents.optimization.revenue_guardian")
    # Partner-scoped floor-lift optimizer. KILL SWITCH: writes only when
    # PARTNER_OPTIMIZER_ENABLED=1 in env; defaults to dry-run-only otherwise.
    # Touches only partner-UNIQUE demands (never RON), strict per-day caps,
    # safety net = auto_revert_harmful (every 4h) reverts on >20% revenue drop.
    partner_revenue_optimizer = _import("agents.optimization.partner_revenue_optimizer")
    tb_contract_floor_sentry = _import("agents.optimization.tb_contract_floor_sentry")
    revenue_health_monitor = _import("agents.alerts.revenue_health_monitor")
    tb_floor_nudge_agent   = _import("agents.optimization.tb_floor_nudge")
    optimal_price_sweep_weekly = _import("scripts.optimal_price_sweep")
    train_floor_model      = _import("scripts.train_floor_model")
    margin_health          = _import("agents.alerts.margin_health")
    # Real-time pacing deviation alert — fires every 2h if revenue is >20%
    # below the 4-week same-DOW same-hour baseline
    pacing_deviation       = _import("agents.alerts.pacing_deviation")
    # Daily defense-in-depth scan for contract-floor violations (catches
    # UI-based drops that bypass the write-path clamp in ll_mgmt)
    contract_floor_sentry  = _import("agents.optimization.contract_floor_sentry")
    # Daily auto-activator for high-confidence demand-gap wirings
    auto_wire_gaps         = _import("agents.optimization.auto_wire_gaps")
    # Daily re-activator for silently-paused inventory (catches accidental UI pauses)
    auto_unpause           = _import("agents.optimization.auto_unpause")
    # Every-4h revert agent for harmful recent floor changes
    auto_revert_harmful    = _import("agents.optimization.auto_revert_harmful")
    # Every-6h adjuster for new-wiring performance (removes dead wirings)
    auto_adjust_wirings    = _import("agents.optimization.auto_adjust_wirings")
    # Daily config health: auto-fix supplyChainEnabled, lurlEnabled, qpsLimit;
    # alert on low-margin demands. Codifies all the manual fixes from 2026-04-25.
    config_health_scanner  = _import("agents.optimization.config_health_scanner")
    # Daily revenue-pattern hunter — finds underpriced demands, DSP-decliners,
    # WoW drops; auto-raises underpriced floors (capped, safety-netted by
    # intervention_journal); Slacks rest as new findings only.
    trend_hunter           = _import("agents.optimization.trend_hunter")
    # Per-write A/B watch — evaluates every floor-changing ledger entry 48h
    # post-write and auto-reverts losers. Catches subtle bleeds.
    intervention_journal   = _import("agents.optimization.intervention_journal")
    # Per-pub margin-experiment A/B watch — evaluates margin_experiment_* ledger
    # entries 48h+ post-write and auto-reverts losers based on NET CONTRIBUTION
    # per day (gross × realized margin), not just gross revenue. Tighter than
    # intervention_journal: max 1 revert/run, escalating thresholds with age.
    margin_experiment_monitor = _import("agents.optimization.margin_experiment_monitor")
    # Real-time supplier-level pacing comparison — today-vs-yesterday Slack
    # digest every 2h. Lets the user see daily trajectory without manual
    # screenshot-and-compare. Posts portfolio + top suppliers in a Slack table.
    pacing_today_vs_yesterday = _import("agents.reports.pacing_today_vs_yesterday")
    # Daily change-accountability digest — Slack post each morning showing
    # what the agents did + outcomes from 48-72h ago + week-to-date impact
    change_outcome_digest  = _import("agents.reports.change_outcome_digest")
    # Weekly Monday digest of proposals needing human review
    weekly_review_digest   = _import("agents.reports.weekly_review_digest")
    # Pilot program
    pilot_snapshot         = _import("scripts.pilot_snapshot")
    pilot_watchdog         = _import("scripts.pilot_watchdog")
    # floor_optimizer removed from scheduler 2026-04-25 — see PR #16 history.
    # The internal env-flag gate (PGAM_FLOOR_OPTIMIZER_ENABLED=0) was being
    # bypassed in production (writes still landing every 2h), so the registration
    # itself is now removed for belt-and-suspenders safety. To revive: review
    # and modernize the tuning logic, then re-add registration here.
    # floor_optimizer        = _import("scripts.floor_optimizer")
    # TB (TechBid) agents removed 2026-04-18 — account is LL-only; agents
    # were erroring daily with 401 against inactive TB endpoints.
    # ML tranche 1 — instrumentation only (no auto-actions)
    ml_collector           = _import("intelligence.collector")                      # hourly — pub × demand
    ml_geo_collector       = _import("intelligence.collector", "run_geo")           # daily — pub × demand × country (split out to avoid OOM on hourly tick)
    ml_bid_landscape       = _import("intelligence.bid_landscape")
    ml_holdout             = _import("intelligence.holdout")
    # ML tranche 2 — optimizer proposal engine + Slack proposer + verifier
    ml_verifier            = _import("intelligence.verifier")
    ml_optimizer           = _import("intelligence.optimizer")
    ml_proposer            = _import("intelligence.proposer")
    # ML tranche 3 — discovery + quarantine
    ml_paused_watchlist    = _import("intelligence.paused_watchlist")
    ml_demand_gap          = _import("intelligence.demand_gap")
    ml_scorecard           = _import("intelligence.scorecard")
    ml_quarantine          = _import("intelligence.quarantine")
    # ML tranche 5 — dayparting rotator (gated by PGAM_DAYPARTING_ENABLED=1)
    ml_dayparting          = _import("intelligence.dayparting")

    # ── Hourly, staggered ───────────────────────────────────────────────────
    #
    # Render's starter plan caps memory at 512 MB. All ETLs scheduled at
    # `every(60).minutes` fire at the same moment after boot, which means
    # they queue up back-to-back. Heavy ones (ll_4dim_etl with 400k+ rows,
    # tb_* with multi-day windows) push us OOM mid-run — which is why the
    # instance was restarting every ~61 min for the last 24+ hours and most
    # ETL data was hours-to-days stale.
    #
    # Stagger by 4 min increments on the hour mark. That gives each heavy
    # agent room to finish + GC before the next one starts. 15 hourly slots
    # × 4 min = 60 min, fits exactly.
    #
    # Use `every().hour.at(":MM")` instead of `every(60).minutes` so the
    # offsets are deterministic across restarts — the same minute mark
    # always belongs to the same agent.
    _hourly_minute = 0
    def _hourly(name, fn):
        nonlocal _hourly_minute
        slot = f":{_hourly_minute:02d}"
        _hourly_minute = (_hourly_minute + 4) % 60
        return schedule.every().hour.at(slot).do(_run(name, fn))

    _hourly("partner_revenue_etl", partner_revenue_etl)   # :00
    _hourly("ll_dimensions_etl",   ll_dimensions_etl)     # :04
    _hourly("ll_4dim_etl",         ll_4dim_etl)           # :08 — heavy
    _hourly("country_revenue_etl", country_revenue_etl)   # :12
    _hourly("ll_segments_etl",     ll_segments_etl)       # :16
    _hourly("ll_geo_segments_etl", ll_geo_segments_etl)   # :20
    _hourly("tb_ad_format_etl",    tb_ad_format_etl)      # :24
    _hourly("tb_hour_etl",         tb_hour_etl)           # :28
    schedule.every().day.at("04:30").do(_run("app_name_enrichment", app_name_enrichment))
    _hourly("tb_segments_etl",     tb_segments_etl)       # :32 — heavy
    # dashboard_alerts is daily-deduped internally; we tick it hourly
    # so it self-heals against missed mornings (the dedup key blocks
    # repeats once it succeeds).
    _hourly("dashboard_alerts",    dashboard_alerts)      # :36
    # Revenue recheck — daily 06:00 ET. Scans current + prior month,
    # snapshots LL/TB cells, flags variances vs prior snapshot. Months
    # in 'paid'/'closed' get scanned but flagged variances are
    # is_carry_forward=true (won't mutate locked invoices).
    revenue_recheck         = _import("agents.recon.revenue_recheck")
    schedule.every().day.at("06:00").do(_run("revenue_recheck", revenue_recheck))
    # Partner-portal scheduled reports — runs every hour, picks up rows
    # in pgam_direct.scheduled_reports due to fire this UTC hour, builds
    # a partner-scoped ZIP via the dashboard admin-download endpoint,
    # emails it via SendGrid. Per-schedule dedupe ensures only one
    # send per day even if multiple ticks land on the same hour.
    partner_scheduled_reports = _import("agents.reports.partner_scheduled_reports")
    schedule.every().hour.at(":52").do(_run("partner_scheduled_reports", partner_scheduled_reports))
    _hourly("tb_revenue_etl",     tb_revenue_etl)         # :40
    _hourly("ll_revenue",         ll_revenue)             # :44
    # ML tranche 1 — collect hourly funnel, rebuild bid-landscape 2x/day,
    # refresh holdout assignments weekly (countries/tuples don't churn fast).
    _hourly("ml_collector",       ml_collector)           # :48
    # tb_revenue Slack snapshot — runs 12 min after tb_revenue_etl so the
    # TB API's single-concurrent-query lock has cleared.
    _hourly("tb_revenue",         tb_revenue)             # :52
    # Geo (pub × demand × country) is much heavier than hourly (it fans rows
    # out ~50×). Run it once daily in a quiet window to avoid OOMing the worker.
    schedule.every().day.at("03:00").do(_run("ml_geo_collector", ml_geo_collector))
    schedule.every().day.at("01:15").do(_run("ml_bid_landscape", ml_bid_landscape))
    schedule.every().day.at("13:15").do(_run("ml_bid_landscape", ml_bid_landscape))
    schedule.every().monday.at("02:00").do(_run("ml_holdout",    ml_holdout))
    # ML tranche 2 cadence:
    #   07:30 ET — verify last 48h of writes (catches silent LL reverts)
    #   07:45 ET — regenerate proposals (reads freshest bid landscape)
    #   08:00 ET — post to Slack; auto-apply anything clearing the autonomy bar
    schedule.every().day.at("07:30").do(_run("ml_verifier",   ml_verifier))
    # Quarantine runs before optimizer so newly-live tuples enter trial
    # before the proposer can consider them.
    schedule.every().day.at("07:40").do(_run("ml_quarantine", ml_quarantine))
    schedule.every().day.at("07:45").do(_run("ml_optimizer",  ml_optimizer))
    schedule.every().day.at("08:00").do(_run("ml_proposer",   ml_proposer))
    # Dayparting — rebuild schedule + rotate per-hour floor at :05 each hour.
    # Gated at registration time so a disabled flag doesn't cost an hourly
    # wakeup. Flip PGAM_DAYPARTING_ENABLED=1 in Render and redeploy to enable.
    import os as _os
    if _os.getenv("PGAM_DAYPARTING_ENABLED") == "1":
        schedule.every().hour.at(":05").do(_run("ml_dayparting", ml_dayparting))

    # ── DSP buyer agent ticks ────────────────────────────────────────────
    # pgam-intelligence drives the DSP buyer agent's ticks because the
    # dashboard's Vercel cron set exceeds plan quota (confirmed 2026-06-01:
    # 4+ hours of expected 5-min ticks produced 0 ledger rows). Two
    # paths:
    #   1. margin_watchdog — full PYTHON PORT of the watchdog logic
    #      (loadActiveCampaigns + compute + decide + write event). Doesn't
    #      depend on Vercel cron OR the deployed Next.js route (which had
    #      its own runtime bug). Direct DSP Neon connection.
    #   2. status_report + auto_rollback — HTTP invokers; the rendering /
    #      apply orchestration lives in the dashboard's TS code, so we
    #      just trigger the deployed routes.
    from agents.dsp_buyer.margin_watchdog import margin_watchdog as dsp_margin_watchdog
    from agents.dsp_buyer.burn_rate_watchdog import burn_rate_watchdog as dsp_burn_rate_watchdog
    from agents.dsp_buyer.retro_generator import retro_generator as dsp_retro_generator
    from agents.dsp_buyer.watchdog_invoker import (
        run_auto_rollback   as dsp_auto_rollback,
        run_status_report   as dsp_status_report,
    )
    schedule.every(5).minutes.do(_run("dsp_margin_watchdog", dsp_margin_watchdog))
    # Burn-rate auto-revert for front_loaded budget_pacing flips. Catches
    # the "perf looks fine but we're burning too fast" failure mode the
    # 6h auto-rollback misses (it only watches CTR/VTR degradation, not
    # spend trajectory). Inspects only campaigns flipped to front_loaded
    # in the last 96h.
    schedule.every(5).minutes.do(_run("dsp_burn_rate_watchdog", dsp_burn_rate_watchdog))
    schedule.every(6).hours.do(  _run("dsp_auto_rollback",   dsp_auto_rollback))
    schedule.every().day.at("09:00").do(_run("dsp_status_report", dsp_status_report))
    # Stage-1 learning loop: post-flight retro for every campaign that ends.
    # Runs daily at 09:30 ET (after midnight-ET campaigns end + after the
    # daily status digest goes out). Idempotent — UNIQUE on campaign_id
    # prevents re-running. Foundation for the knowledge base (Stage 2).
    schedule.every().day.at("09:30").do(_run("dsp_retro_generator", dsp_retro_generator))

    # Weekly — discovery + rep-conversation feeds (Monday mornings)
    schedule.every().monday.at("09:00").do(_run("ml_paused_watchlist", ml_paused_watchlist))
    schedule.every().monday.at("09:05").do(_run("ml_demand_gap",       ml_demand_gap))
    schedule.every().monday.at("09:10").do(_run("ml_scorecard",        ml_scorecard))

    # ── Every 4 hours ────────────────────────────────────────────────────────
    schedule.every(4).hours.do(  _run("revenue_pace",        revenue_pace))
    schedule.every(4).hours.do(  _run("opp_fill_rate",       opp_fill_rate))

    # ── Daily at 7:00 AM ET ───────────────────────────────────────────────────
    schedule.every().day.at("07:00").do(_run("daily_email",    daily_email))
    schedule.every().day.at("07:00").do(_run("weekly_review",  weekly_review))   # Mon guard inside
    # Auto weekly digest: tick hourly Mon morning; the agent self-gates
    # to 13:00-15:00 UTC and ISO-week-dedupes so we get a single Mon
    # post even with multiple ticks.
    schedule.every().hour.do(_run("weekly_digest", weekly_digest))

    # ── Daily at 8:00 AM ET ───────────────────────────────────────────────────
    schedule.every().day.at("08:30").do(_run("daily_recommendations",  daily_recommendations))
    schedule.every().day.at("08:00").do(_run("floor_gap",              floor_gap))
    schedule.every().day.at("08:00").do(_run("demand_saturation",      demand_saturation))
    schedule.every().day.at("08:00").do(_run("app_revenue",            app_revenue))
    schedule.every().day.at("08:00").do(_run("publisher_monetization", publisher_monetization))
    schedule.every().day.at("08:00").do(_run("action_tracker",         action_tracker))
    schedule.every().day.at("08:00").do(_run("win_rate_maximizer",     win_rate_maximizer))
    schedule.every().day.at("08:00").do(_run("floor_elasticity",       floor_elasticity))    # Mon guard inside
    schedule.every().day.at("08:00").do(_run("ctv_optimizer",          ctv_optimizer))       # Mon guard inside
    schedule.every().day.at("08:00").do(_run("demand_expansion",       demand_expansion))    # Wed guard inside
    schedule.every().day.at("08:00").do(_run("geo_expansion",          geo_expansion))       # Thu guard inside
    schedule.every().day.at("08:00").do(_run("weekend_recovery",       weekend_recovery))    # Fri+Sun guard inside
    schedule.every().day.at("08:15").do(_run("fill_funnel",            fill_funnel))         # Mon-Fri guard inside
    schedule.every().day.at("08:15").do(_run("dead_demand",            dead_demand))         # Mon guard inside
    schedule.every().day.at("08:15").do(_run("geo_leak",               geo_leak))            # Thu guard inside
    schedule.every().day.at("08:15").do(_run("margin_health",          margin_health))       # daily — alert if any pub <30% margin
    # Pacing deviation — every 2h, fires Slack alert if revenue pace dips >20% below baseline
    schedule.every(2).hours.do(_run("pacing_deviation",                pacing_deviation))
    # Contract floor sentry — hourly safety net for 9 Dots + future contract
    # minimums. Bumped from daily 06:00 ET → hourly 2026-04-25 to shrink the
    # window between a UI-side / archived-and-recreated floor drop and
    # restoration (was up to 24h, now ≤1h). Slack alerts on every restoration
    # (deduped daily per-demand); P1 escalation if same demand restored ≥2×
    # in trailing 7d (= upstream regression signal).
    schedule.every().hour.at(":15").do(_run("contract_floor_sentry",  contract_floor_sentry))
    # ads.txt monitor — daily at 09:00 ET. Verifies PGAM-owned seats remain
    # DIRECT on each O&O site (destination.com, boxingnews.com); pages P1 on
    # missing/wrong-relationship lines, P2 on fetch errors, P3 (deduped daily)
    # on unfamiliar PGAM-domain DIRECT entries.
    adstxt_monitor = _import("agents.alerts.adstxt_monitor")
    schedule.every().day.at("09:00").do(_run("adstxt_monitor",       adstxt_monitor))
    # Supply Compliance & Quality Intelligence.
    #
    # Retry-until-success: the audit runs 5 times in the morning
    # window (08:00 / 08:30 / 09:00 / 09:30 / 10:00 ET) so a Render
    # OOM-kill on one attempt doesn't lose the daily delivery.
    # runner.run() is idempotent — it checks compliance_runs and
    # no-ops if today already succeeded or another worker is in
    # flight. Each tick that finds NO success + NO live attempt
    # starts a fresh run, which means a mid-execution OOM at 08:05
    # gets retried at 08:30 automatically with fresh memory.
    #
    # At 10:30 ET — 30 min after the last retry — a fallback digest
    # post fires. If today still has no successful audit AND no
    # digest has been delivered to #compliance, it posts the latest
    # available snapshot with a banner ("audit failed today, here's
    # the latest data we have"). Guarantees a daily channel message
    # even when every audit attempt OOMs.
    if _os.getenv("PGAM_COMPLIANCE_ENABLED") == "1":
        compliance_runner = _import("agents.compliance.runner")
        # User start-of-day is 08:00 ET. The runner takes ~15 min to
        # reach the early-digest delivery point (Phase 1-5 audit work
        # before the OOM-prone roundtrip). So we kick off at 07:45 ET
        # so the Slack message LANDS at ~08:00 ET, not ~08:15 ET. The
        # later retry windows are safety nets — runner.run() is
        # idempotent (skips via _should_skip_today if the early attempt
        # already delivered + marked compliance_runs.ok=TRUE).
        for retry_time in ("07:45", "08:00", "08:15", "08:30", "09:00", "10:00"):
            schedule.every().day.at(retry_time).do(
                _run("compliance_runner", compliance_runner))
        compliance_fallback = _import(
            "agents.compliance.runner", "run_fallback_digest")
        schedule.every().day.at("10:30").do(
            _run("compliance_fallback", compliance_fallback))

        # Compliance enforcer — consumes compliance_path_block_list
        # rows with status='active' and pauses the corresponding LL
        # (publisher × demand) pairs. Dry-run by default; flip
        # PGAM_COMPLIANCE_ENFORCE_LIVE=1 in Render env to enable
        # actual LL mutations. Runs hourly so a status='active' flip
        # via scripts/compliance_approve.py lands within an hour.
        # Bounded at 10 actions per tick — safety against rule misfires.
        compliance_enforcer = _import("agents.compliance.enforcer")
        schedule.every().hour.at(":47").do(
            _run("compliance_enforcer", compliance_enforcer))

        # Reactivation monitor — every hour at :57, AFTER the enforcer.
        # Reads compliance_path_block_list state (which the auditor
        # auto-flips active→released when audit shows healthy) and
        # recomputes each row's recommended_action:
        #   • reactivate / monitor / keep_blocked / whitelist_aging /
        #     fixed_pre_review
        # Surfaces "eligible to bring live" inventory in the daily
        # digest's :sparkles: Reactivation candidates section.
        # Re-runs idempotently — same state in, same recommendations out.
        compliance_reactivation = _import(
            "agents.compliance.reactivation_monitor")
        schedule.every().hour.at(":57").do(
            _run("compliance_reactivation", compliance_reactivation))

        # PubMatic drift watch — every hour at :52, between the daily
        # enforcer (:47) and the reactivation monitor (:57). LIVE mode
        # from day 1 — PubMatic termination risk justifies acting
        # immediately rather than the dry-run that gates Phase 1.
        # For every currently-active (publisher × PubMatic-demand)
        # wiring on LL, runs Layer A (PubMatic line + our seat 165708),
        # Layer B (PGAM seat for the supply path), Layer D
        # (supplyChainEnabled=True, dontAddSupplyChainNode=False on the
        # demand config). Any failure → immediate disable via
        # ll_mgmt.disable_publisher_demand + per-action Slack alert +
        # row in compliance_enforcement_log.
        # The WL is defined on LL — whatever's wired & active IS the WL.
        # When PubMatic is fully paused (every demand status=2), this
        # is a cheap no-op.
        pubmatic_drift_watch = _import(
            "agents.compliance.pubmatic_drift_watch")
        schedule.every().hour.at(":52").do(
            _run("pubmatic_drift_watch", pubmatic_drift_watch))
    # Config auditor — daily LL + TB sweep for floors/wirings/rules that look
    # off. P1 contract-floor breaches, P2 zero/outlier floors, P3 orphans &
    # zombie wirings. TB section flags any signs of life (account is supposed
    # to be dormant). Slack digest deduped daily; full JSON in
    # data/config_audit_report.json.
    # Runs 15 min AFTER config_health_scanner so the scanner's auto-fixes
    # (supplyChainEnabled / lurlEnabled / qpsLimit) land first; the auditor
    # then reads the post-fix state and only flags the things humans need to
    # judge (floor anomalies, orphan demands, zombie wirings, TB shadow).
    # Field domains are disjoint — see docs/guardrail-agents.md §3.
    config_auditor = _import("agents.alerts.config_auditor")
    schedule.every().day.at("06:45").do(_run("config_auditor",       config_auditor))
    # Auto-wire qualifying demand-gaps — runs daily after demand_gap Monday refresh
    # (demand_gap runs weekly Monday; re-running this agent daily is idempotent:
    # it skips anything already wired and caps new wirings per run.)
    schedule.every().day.at("09:30").do(_run("auto_wire_gaps",         auto_wire_gaps))
    # Auto-unpause silently-paused inventory (catches accidental UI pauses)
    schedule.every().day.at("09:45").do(_run("auto_unpause",           auto_unpause))
    # Auto-revert harmful floor changes — every 4 hours
    schedule.every(4).hours.do(_run("auto_revert_harmful",             auto_revert_harmful))
    # Partner revenue optimizer — every 4 hours, gated by PARTNER_OPTIMIZER_ENABLED=1
    # env var (defaults to dry-run). Lifts floors on partner-UNIQUE low-yield
    # demands for AppStock / Start.IO Mag / PubNative Mag. Caps: 3 changes/run,
    # 1/partner/day. auto_revert_harmful (above) catches >20% revenue drops.
    schedule.every(4).hours.do(_run("partner_revenue_optimizer",       partner_revenue_optimizer))
    # Auto-adjust new wirings that aren't performing — every 6 hours
    schedule.every(6).hours.do(_run("auto_adjust_wirings",             auto_adjust_wirings))
    # Config health scanner — daily 06:30 ET, after contract_floor_sentry
    schedule.every().day.at("06:30").do(_run("config_health_scanner",  config_health_scanner))
    # Trend hunter — every 6h, dedup'd, auto-executes safe raises only
    schedule.every(6).hours.do(_run("trend_hunter",                    trend_hunter))
    # Intervention journal — every 4h offset by 2h from auto_revert_harmful
    schedule.every(4).hours.do(_run("intervention_journal",            intervention_journal))
    # Margin experiment monitor — every 4h, offset by 1h from intervention_journal
    # so we don't double-fire safety nets simultaneously. Watches pub-level
    # margin experiments and auto-reverts losers based on NET contribution drop.
    schedule.every(4).hours.do(_run("margin_experiment_monitor",       margin_experiment_monitor))
    # Pacing comparison — every 2h during US daytime. Posts supplier-level
    # today-vs-yesterday Slack digest with pace ratio + projection.
    schedule.every(2).hours.do(_run("pacing_today_vs_yesterday",       pacing_today_vs_yesterday))
    # Daily change-accountability digest — 09:15 ET, after weekly_review_digest
    schedule.every().day.at("09:15").do(_run("change_outcome_digest",  change_outcome_digest))
    # Weekly proposal review digest — Monday 09:00 ET (internal Monday+hour guard)
    schedule.every().day.at("09:00").do(_run("weekly_review_digest",   weekly_review_digest))
    schedule.every().day.at("08:45").do(_run("publisher_optimizer",      publisher_optimizer))       # daily — SSP supply partner dead-weight & expand recs
    schedule.every().day.at("09:00").do(_run("dsp_optimizer",          dsp_optimizer))           # daily — downstream DSP prune (dry-run by default, --apply gated)
    schedule.every().day.at("09:15").do(_run("ssp_company_optimizer",  ssp_company_optimizer))   # daily — /ad-exchange/ SSP Company roll-up (Illumin, Smaato, Dexerto, ...)
    schedule.every().day.at("09:30").do(_run("geo_floor_optimizer",    geo_floor_optimizer))     # daily — per-placement × country floor optimization
    # Revenue-growth suite
    schedule.every().day.at("07:30").do(_run("partner_churn_radar",    partner_churn_radar))     # daily — WoW revenue drop alerts per publisher
    schedule.every().day.at("07:45").do(_run("demand_concentration",   demand_concentration))    # daily — single-DSP inventory risk
    schedule.every().day.at("08:00").do(_run("yield_compression",      yield_compression))       # daily — stable imps + rev drop detector
    schedule.every().day.at("09:45").do(_run("dayparting_floor",       dayparting_floor))        # daily — hourly floor schedule builder
    schedule.every().day.at("10:15").do(_run("size_gap_agent",         size_gap_agent))          # daily — missing-size opportunity finder
    schedule.every().day.at("10:30").do(_run("placement_status_agent", placement_status_agent))  # daily — auto-pause 0-imp placements
    schedule.every().day.at("10:45").do(_run("placement_autocreate",   placement_autocreate))    # daily — auto-create for allowlisted inventories
    schedule.every().day.at("11:00").do(_run("blocked_domains_agent",  blocked_domains_agent))   # daily — junk domain hygiene (dry-run by default)
    schedule.every(4).hours.do(        _run("tb_floor_nudge",          tb_floor_nudge_agent))     # every 4h — +10% nudge w/ auto-rollback
    schedule.every(4).hours.do(        _run("revenue_guardian",        revenue_guardian))         # every 4h — verify+act with rollback safety net
    schedule.every().hour.do(          _run("tb_contract_floor_sentry",tb_contract_floor_sentry)) # hourly — restore any contract-floor violation
    schedule.every(6).hours.do(        _run("revenue_health_monitor", revenue_health_monitor))    # every 6h — kill switch on aggregate revenue drop
    schedule.every().monday.at("06:00").do(_run("optimal_price_weekly", optimal_price_sweep_weekly))  # Mon — catch any new placements
    # ── Weekly: retrain floor elasticity ML model (Sun 05:00 ET) ─────────────
    schedule.every().sunday.at("05:00").do(_run("train_floor_model",   train_floor_model))

    # ── Every 2 hours — dynamic floor optimizer ──────────────────────────────
    # floor_optimizer registration removed 2026-04-25. See _import block above.
    # schedule.every(2).hours.do(_run("floor_optimizer", floor_optimizer))

    # ── Daily 9:00 AM ET ─────────────────────────────────────────────────────
    schedule.every().day.at("09:00").do(_run("pilot_snapshot",    pilot_snapshot))     # daily dedup inside
    schedule.every().day.at("09:30").do(_run("pilot_watchdog",   pilot_watchdog))     # checks active floor watches
    schedule.every().day.at("09:00").do(_run("revenue_gap",       revenue_gap))        # Sun guard inside

    # ── Monthly forecast: runs daily at 7:30 AM, day-of-month guard inside ───
    schedule.every().day.at("07:30").do(_run("monthly_forecast",  monthly_forecast))   # 1/10/20 guard inside

    # ── MSN Partner Hub insights (BoxingNews) ────────────────────────────────
    # Playwright-driven puller + lazy docID resolver. Gated by env flag
    # because Playwright/Chromium can't fit on Render's free Python tier;
    # production cron is .github/workflows/msn-insights.yml. To run inside
    # this scheduler (local dev or a Playwright-capable host) set
    # PGAM_MSN_PULLER_ENABLED=1 in .env. The agents themselves no-op
    # gracefully when playwright isn't importable, so it's also safe to
    # leave on even if the deploy target can't actually run Chromium.
    if _os.getenv("PGAM_MSN_PULLER_ENABLED") == "1":
        msn_insights_etl  = _import("agents.etl.msn_insights_etl")
        msn_doc_resolver  = _import("agents.enrichment.msn_doc_resolver")
        msn_puller_health = _import("agents.alerts.msn_puller_health")
        # Every 15 min, on the quarter, lightly offset to avoid colliding
        # with the hourly ETL block at :00. recordCount=123 articles in
        # MSN's 24h window means we capture full per-article time series.
        schedule.every().hour.at(":01").do(_run("msn_insights_etl", msn_insights_etl))
        schedule.every().hour.at(":16").do(_run("msn_insights_etl", msn_insights_etl))
        schedule.every().hour.at(":31").do(_run("msn_insights_etl", msn_insights_etl))
        schedule.every().hour.at(":46").do(_run("msn_insights_etl", msn_insights_etl))
        # docID → boxingnews.com URL backfill — every 30 min, well off
        # the realtime cadence so we don't fight Playwright for resources.
        schedule.every().hour.at(":11").do(_run("msn_doc_resolver", msn_doc_resolver))
        schedule.every().hour.at(":41").do(_run("msn_doc_resolver", msn_doc_resolver))
        # Hourly health check — Slacks once/day if pulls stall or fail
        # in a streak. Read-only against pgam_direct.msn_pull_runs.
        schedule.every().hour.at(":50").do(_run("msn_puller_health", msn_puller_health))

    # ─────────────────────────────────────────────────────────────────
    # BoxingNews weekly content-strategy review
    # ─────────────────────────────────────────────────────────────────
    # Monday 09:30 ET. The agent has its own weekday guard so firing
    # daily would be safe too, but pinning Monday keeps the scheduler
    # log honest about cadence. Reads pgam_direct.msn_article_peak +
    # boxingnews.articles, writes pgam_direct.msn_weekly_review, emails
    # + Slacks the report. Gated by BOXINGNEWS_REVIEW_ENABLED so the
    # boxingnews DB DSN is only required where this agent actually
    # runs.
    if _os.getenv("BOXINGNEWS_REVIEW_ENABLED", "1") == "1":
        boxingnews_weekly_review = _import("agents.insights.boxingnews_weekly_review")
        schedule.every().monday.at("09:30").do(
            _run("boxingnews_weekly_review", boxingnews_weekly_review)
        )
        # Daily ingest-health watchdog. Fires once at 08:30 ET — by then
        # the breaking lane (15-min cron) has had ~96 ticks since
        # midnight ET and the trending lane (2h cron) has had 4. If
        # either produced 0 articles, something is wrong (was true for
        # ~30 days pre-fix and went unnoticed). Self-deduplicates so
        # a multi-day outage doesn't spam Slack.
        boxingnews_ingest_health = _import("agents.alerts.boxingnews_ingest_health")
        schedule.every().day.at("08:30").do(
            _run("boxingnews_ingest_health", boxingnews_ingest_health)
        )

    # ─────────────────────────────────────────────────────────────────
    # Outbound SDR — daily lead loader (Apollo → HubSpot → Instantly)
    # ─────────────────────────────────────────────────────────────────
    # Weekday 09:00 ET. The agent itself is dry-run by default
    # (SDR_DRY_RUN=true) so registering it here is safe even before
    # the API keys / campaign IDs are filled in. Gated by
    # SDR_AGENT_ENABLED so we can park it without code changes.
    if _os.getenv("SDR_AGENT_ENABLED", "1") == "1":
        sdr_agent = _import("agents.outbound.sdr_agent")
        for day in ("monday", "tuesday", "wednesday", "thursday", "friday"):
            getattr(schedule.every(), day).at("09:00").do(
                _run("sdr_agent", sdr_agent)
            )

    print("[scheduler] Schedule registered:")
    for job in schedule.get_jobs():
        print(f"  {job}")


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------

def main():
    print("=" * 60)
    print("PGAM Intelligence Scheduler starting…")
    print(f"System time: {datetime.now()}")
    print(f"ET time:     {datetime.now(ET).strftime('%Y-%m-%d %H:%M %Z')}")
    print("=" * 60)

    setup_schedule()

    # Run LL + TB revenue once immediately on startup so we don't wait up to
    # 60 minutes for the first snapshot.
    print("\n[scheduler] Running startup revenue check (LL + TB)…")
    for job in schedule.get_jobs():
        if job.job_func.__name__ in ("ll_revenue", "tb_revenue"):
            job.run()

    # Restart-resilient compliance run with cooldown protection.
    #
    # The previous version (PR #45) refired on every boot when no
    # successful run row existed for today. If the run repeatedly OOMs
    # mid-execution, that loops forever — container dies, restarts,
    # catch-up refires, OOMs again, ad infinitum. Observed in
    # production on 2026-05-31: 30+ restart cycles in 5 hours, each
    # killing the worker before it could write its completion row.
    #
    # Cooldown gates:
    #   - Skip if a run *started* in the last 60 min (whether or not
    #     it completed). Gives Render time to recover memory between
    #     attempts and prevents the death loop.
    #   - Skip if a successful run already exists for today.
    #
    # Combined with runner.py's start-of-run log insert (so even
    # OOM'd runs leave a tombstone in compliance_runs), this turns
    # the loop into "fire once on boot, wait 60 min, fire once more"
    # — bounded.
    import os as _os
    try:
        if _os.getenv("PGAM_COMPLIANCE_ENABLED") == "1":
            from datetime import date as _date
            now_et = datetime.now(ET)
            if (now_et.hour, now_et.minute) >= (8, 30):
                _ok_today = False
                _recent_attempt = False
                try:
                    from core.neon import connect as _connect
                    with _connect() as _c:
                        with _c.cursor() as _cur:
                            _cur.execute("""
                                SELECT
                                    COUNT(*) FILTER (WHERE ok IS TRUE)        AS ok_count,
                                    COUNT(*) FILTER (
                                      WHERE started_at >= now() - interval '60 minutes'
                                    )                                          AS recent_count
                                FROM pgam_direct.compliance_runs
                                WHERE started_at::date = %s;
                            """, (_date.today(),))
                            _row = _cur.fetchone()
                            _ok_today = (_row[0] or 0) > 0
                            _recent_attempt = (_row[1] or 0) > 0
                except Exception as _e:
                    print(f"[scheduler] compliance run-log check failed "
                          f"(non-fatal): {_e}")
                if _ok_today:
                    print("[scheduler] Compliance run already completed "
                          "today — skipping catch-up.")
                elif _recent_attempt:
                    print("[scheduler] Recent compliance attempt (<60 min) "
                          "found — skipping catch-up to avoid restart-loop. "
                          "Will retry on the next scheduler boot after cooldown.")
                else:
                    print("\n[scheduler] No successful compliance run today "
                          "yet and no recent attempt — firing catch-up now.")
                    for _job in schedule.get_jobs():
                        if _job.job_func.__name__ == "compliance_runner":
                            _job.run()
                            break
    except Exception as _exc:
        print(f"[scheduler] compliance catch-up failed (non-fatal): {_exc}")

    print("\n[scheduler] Entering main loop. Press Ctrl+C to stop.\n")
    while True:
        schedule.run_pending()
        time.sleep(30)   # check every 30 seconds


if __name__ == "__main__":
    main()
