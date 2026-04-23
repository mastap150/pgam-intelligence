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

import time
import traceback
from datetime import datetime

import pytz
import schedule
from dotenv import load_dotenv

load_dotenv(override=True)

ET = pytz.timezone("US/Eastern")


def _run(agent_name: str, fn):
    """Wrapper that catches all exceptions so one failing agent never kills the scheduler."""
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
# ll_revenue             | every 60m  | any time (55-min cooldown inside agent)
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
    ll_revenue             = _import("agents.alerts.ll_revenue")
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
    # Pilot program
    pilot_snapshot         = _import("scripts.pilot_snapshot")
    pilot_watchdog         = _import("scripts.pilot_watchdog")
    floor_optimizer        = _import("scripts.floor_optimizer")
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

    # ── Hourly ───────────────────────────────────────────────────────────────
    schedule.every(60).minutes.do(_run("ll_revenue",         ll_revenue))
    # ML tranche 1 — collect hourly funnel, rebuild bid-landscape 2x/day,
    # refresh holdout assignments weekly (countries/tuples don't churn fast).
    schedule.every(60).minutes.do(_run("ml_collector",       ml_collector))
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
    # Internally no-ops unless PGAM_DAYPARTING_ENABLED=1.
    schedule.every().hour.at(":05").do(_run("ml_dayparting", ml_dayparting))

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
    # Contract floor sentry — daily safety net for 9 Dots + future contract minimums
    schedule.every().day.at("06:00").do(_run("contract_floor_sentry",  contract_floor_sentry))
    # Auto-wire qualifying demand-gaps — runs daily after demand_gap Monday refresh
    # (demand_gap runs weekly Monday; re-running this agent daily is idempotent:
    # it skips anything already wired and caps new wirings per run.)
    schedule.every().day.at("09:30").do(_run("auto_wire_gaps",         auto_wire_gaps))
    # Auto-unpause silently-paused inventory (catches accidental UI pauses)
    schedule.every().day.at("09:45").do(_run("auto_unpause",           auto_unpause))
    schedule.every().day.at("08:45").do(_run("publisher_optimizer",      publisher_optimizer))       # daily — SSP supply partner dead-weight & expand recs
    schedule.every().day.at("09:00").do(_run("dsp_optimizer",          dsp_optimizer))           # daily — downstream DSP prune (dry-run by default, --apply gated)
    schedule.every().day.at("09:15").do(_run("ssp_company_optimizer",  ssp_company_optimizer))   # daily — /ad-exchange/ SSP Company roll-up (Illumin, Smaato, Dexerto, ...)
    schedule.every().day.at("09:30").do(_run("geo_floor_optimizer",    geo_floor_optimizer))     # daily — per-placement × country floor optimization
    # ── Weekly: retrain floor elasticity ML model (Sun 05:00 ET) ─────────────
    schedule.every().sunday.at("05:00").do(_run("train_floor_model",   train_floor_model))

    # ── Every 2 hours — dynamic floor optimizer ──────────────────────────────
    schedule.every(2).hours.do(_run("floor_optimizer", floor_optimizer))

    # ── Daily 9:00 AM ET ─────────────────────────────────────────────────────
    schedule.every().day.at("09:00").do(_run("pilot_snapshot",    pilot_snapshot))     # daily dedup inside
    schedule.every().day.at("09:30").do(_run("pilot_watchdog",   pilot_watchdog))     # checks active floor watches
    schedule.every().day.at("09:00").do(_run("revenue_gap",       revenue_gap))        # Sun guard inside

    # ── Monthly forecast: runs daily at 7:30 AM, day-of-month guard inside ───
    schedule.every().day.at("07:30").do(_run("monthly_forecast",  monthly_forecast))   # 1/10/20 guard inside

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

    # Run LL revenue once immediately on startup so we don't wait up to
    # 60 minutes for the first snapshot.
    print("\n[scheduler] Running startup revenue check (LL)…")
    for job in schedule.get_jobs():
        if job.job_func.__name__ == "ll_revenue":
            job.run()

    print("\n[scheduler] Entering main loop. Press Ctrl+C to stop.\n")
    while True:
        schedule.run_pending()
        time.sleep(30)   # check every 30 seconds


if __name__ == "__main__":
    main()
