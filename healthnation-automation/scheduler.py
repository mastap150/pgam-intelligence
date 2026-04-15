"""
HealthNation Automation — Master Scheduler
Runs as a long-lived process and coordinates all automation tasks:

  08:00 AM daily  →  Publish N new articles  (via daily_runner.run_daily_batch)
  10:00 AM daily  →  SEO-optimise 10 posts   (via seo_optimizer.run_seo_batch)

Usage:
    python scheduler.py                        # Default: 3 articles/day
    python scheduler.py --articles-per-day 5   # 5 articles/day
    python scheduler.py --articles-per-day 2   # 2 articles/day

Stop with Ctrl+C.
"""
import sys
import time
import logging
import argparse
from pathlib import Path
from datetime import datetime

try:
    import schedule
except ImportError:
    print("ERROR: 'schedule' package is not installed. Run: pip install schedule")
    sys.exit(1)

import config
from daily_runner    import run_daily_batch
from seo_optimizer   import run_seo_batch

# ─── Logging ─────────────────────────────────────────────────────────────────
BASE_DIR = Path(__file__).parent
LOG_DIR  = BASE_DIR / "logs"
LOG_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    level    = logging.INFO,
    format   = "%(asctime)s [%(levelname)s] %(message)s",
    handlers = [
        logging.FileHandler(LOG_DIR / "scheduler.log"),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger(__name__)

# ─── Schedule times (24-hour HH:MM strings) ──────────────────────────────────
PUBLISH_TIME = "08:00"
SEO_TIME     = "10:00"
SEO_BATCH    = 10


# ─── Scheduled job wrappers ───────────────────────────────────────────────────

def publish_job(articles_per_day: int):
    """Wrapper called by schedule for the daily publish run."""
    logger.info(f"[SCHEDULER] Triggering daily publish job ({articles_per_day} articles)")
    try:
        run_daily_batch(articles_per_run=articles_per_day)
    except Exception as e:
        logger.exception(f"[SCHEDULER] Daily publish job failed unexpectedly: {e}")


def seo_job():
    """Wrapper called by schedule for the daily SEO optimisation run."""
    logger.info(f"[SCHEDULER] Triggering SEO optimisation job ({SEO_BATCH} posts)")
    try:
        # Advance through pages automatically by tracking the current page.
        # We store state in a small JSON sidecar so restarts continue where
        # they left off.
        import json
        from pathlib import Path

        state_file = Path(__file__).parent / "scheduler_state.json"
        state = {"seo_page": 1}
        if state_file.exists():
            try:
                with open(state_file) as f:
                    state = json.load(f)
            except Exception:
                pass

        page = state.get("seo_page", 1)
        optimized = run_seo_batch(batch_size=SEO_BATCH, page=page)

        # If we got a full batch, advance the page for next run; otherwise
        # wrap back to page 1 (finished a full cycle through all posts).
        if optimized >= SEO_BATCH:
            state["seo_page"] = page + 1
        else:
            state["seo_page"] = 1
            logger.info("[SCHEDULER] SEO page cycle complete — resetting to page 1")

        with open(state_file, "w") as f:
            json.dump(state, f, indent=2)

    except Exception as e:
        logger.exception(f"[SCHEDULER] SEO job failed unexpectedly: {e}")


# ─── Banner ───────────────────────────────────────────────────────────────────

def print_banner(articles_per_day: int):
    """Print a startup banner with schedule information."""
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    banner = f"""
╔══════════════════════════════════════════════════════╗
║      HealthNation Automation — Master Scheduler      ║
╠══════════════════════════════════════════════════════╣
║  Started : {now:<41}║
║  Site    : {config.WP_SITE_URL:<41}║
╠══════════════════════════════════════════════════════╣
║  SCHEDULED JOBS                                      ║
║  ─────────────────────────────────────────────────  ║
║  {PUBLISH_TIME} daily  →  Publish {articles_per_day} new article(s)            ║
║  {SEO_TIME} daily  →  SEO-optimise {SEO_BATCH} existing posts       ║
╠══════════════════════════════════════════════════════╣
║  Press Ctrl+C to stop                                ║
╚══════════════════════════════════════════════════════╝
"""
    print(banner)
    logger.info(
        f"Scheduler started — publish at {PUBLISH_TIME} "
        f"({articles_per_day}/day), SEO at {SEO_TIME} ({SEO_BATCH} posts/day)"
    )


# ─── Next-run helper ──────────────────────────────────────────────────────────

def log_next_runs():
    """Log the next scheduled run times for all jobs."""
    jobs = schedule.get_jobs()
    for job in jobs:
        next_run = job.next_run
        logger.info(f"  Next scheduled: {next_run.strftime('%Y-%m-%d %H:%M')} — {job.job_func.__name__}")


# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="HealthNation master scheduler — runs all automation tasks"
    )
    parser.add_argument(
        "--articles-per-day", type=int, default=3,
        help="Number of new articles to publish per day (default: 3)"
    )
    args = parser.parse_args()

    articles_per_day = args.articles_per_day

    # Validate config before doing anything else
    try:
        config.validate()
    except EnvironmentError as e:
        logger.error(f"Configuration error: {e}")
        sys.exit(1)

    print_banner(articles_per_day)

    # ── Register jobs ──────────────────────────────────────────────────────────
    schedule.every().day.at(PUBLISH_TIME).do(publish_job, articles_per_day=articles_per_day)
    schedule.every().day.at(SEO_TIME).do(seo_job)

    log_next_runs()

    # ── Main loop ──────────────────────────────────────────────────────────────
    logger.info("Scheduler is running. Waiting for scheduled times...")
    try:
        while True:
            schedule.run_pending()
            time.sleep(60)
    except KeyboardInterrupt:
        logger.info("Scheduler stopped by user (KeyboardInterrupt). Goodbye.")
        print("\nScheduler stopped.")
        sys.exit(0)
    except Exception as e:
        logger.exception(f"Scheduler crashed unexpectedly: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
