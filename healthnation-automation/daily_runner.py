"""
HealthNation Automation — Daily Article Runner
Publishes 2 new articles per day automatically.
Picks the next unpublished topics from topics_queue.json by priority.

Run modes:
    python daily_runner.py              # Run once right now (cron-friendly)
    python daily_runner.py --schedule   # Run as a long-lived scheduled process

Cron setup (recommended — add to crontab):
    # Publish 2 articles at 8:00 AM every day
    0 8 * * * /path/to/venv/bin/python /path/to/healthnation-automation/daily_runner.py >> /path/to/logs/cron.log 2>&1
"""
import sys
import json
import time
import logging
import argparse
from pathlib import Path
from datetime import datetime

try:
    import schedule
except ImportError:
    schedule = None

import config
from generate_article    import generate_article
from unsplash_image      import get_or_upload_image
from wordpress_publisher import publish_article, article_exists, init as wp_init

# ─── Logging ─────────────────────────────────────────────────────────────────
LOG_DIR = Path(__file__).parent / "logs"
LOG_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    level   = logging.INFO,
    format  = "%(asctime)s [%(levelname)s] %(message)s",
    handlers= [
        logging.FileHandler(LOG_DIR / "daily_runner.log"),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger(__name__)

# ─── State files ─────────────────────────────────────────────────────────────
BASE_DIR       = Path(__file__).parent
QUEUE_FILE     = BASE_DIR / "topics_queue.json"
PUBLISHED_FILE = BASE_DIR / "published.json"


def load_published() -> dict:
    """Load record of published articles."""
    if PUBLISHED_FILE.exists():
        with open(PUBLISHED_FILE) as f:
            return json.load(f)
    return {"published": []}


def save_published(data: dict):
    with open(PUBLISHED_FILE, "w") as f:
        json.dump(data, f, indent=2)


def get_next_topics(count: int = 2) -> list[dict]:
    """Return the next N unpublished topics, sorted by priority."""
    with open(QUEUE_FILE) as f:
        all_topics = json.load(f)

    published_data = load_published()
    published_kws  = {r["keyword"] for r in published_data["published"]}

    # Filter unpublished, sort by priority then original order
    remaining = [t for t in all_topics if t["keyword"] not in published_kws]
    remaining.sort(key=lambda t: (t.get("priority", 99), all_topics.index(t)))

    return remaining[:count]


def record_published(topic: dict, wp_post_id: int, wp_url: str):
    """Record a successfully published article."""
    data = load_published()
    data["published"].append({
        "keyword":     topic["keyword"],
        "title":       topic["title"],
        "category":    topic.get("category"),
        "wp_post_id":  wp_post_id,
        "wp_url":      wp_url,
        "published_at":datetime.now().isoformat(),
    })
    save_published(data)


# ─── Core publish function ────────────────────────────────────────────────────
def run_daily_batch(articles_per_run: int = config.ARTICLES_PER_DAY):
    """Generate and publish one batch of articles."""
    logger.info(f"{'='*50}")
    logger.info(f"Daily run starting: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"Target: {articles_per_run} articles")

    try:
        config.validate()
    except EnvironmentError as e:
        logger.error(str(e))
        return

    wp_init()
    topics = get_next_topics(articles_per_run)

    if not topics:
        logger.warning("No unpublished topics remaining in queue. Add more to topics_queue.json")
        return

    logger.info(f"Topics selected: {[t['title'] for t in topics]}")

    for i, topic in enumerate(topics, 1):
        logger.info(f"\n[{i}/{len(topics)}] Generating: {topic['title']}")

        # Skip if somehow already in WordPress
        if article_exists(topic["keyword"]):
            logger.info("  Already in WordPress — skipping")
            continue

        try:
            # Step 1: Generate article with Claude
            article = generate_article(topic)
            logger.info(f"  ✓ Article generated ({article.get('estimated_read_time', '?')} min read)")

            # Step 2: Fetch and upload Unsplash hero image
            image_id = get_or_upload_image(article)

            # Step 3: Publish to WordPress
            post = publish_article(article, featured_image_id=image_id)

            if post:
                record_published(topic, post["id"], post["link"])
                logger.info(f"  ✓ Live at: {post['link']}")
            else:
                logger.error(f"  ✗ Publish returned None for: {topic['title']}")

        except Exception as e:
            logger.exception(f"  ✗ Error processing '{topic['title']}': {e}")

        # Polite delay between articles
        if i < len(topics):
            time.sleep(5)

    published = load_published()
    logger.info(f"\nDaily run complete. Total published to date: {len(published['published'])}")
    logger.info(f"{'='*50}")


# ─── Entry point ─────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="HealthNation daily article publisher")
    parser.add_argument("--schedule", action="store_true", help="Run as scheduled process (don't use with cron)")
    parser.add_argument("--time",     default="08:00",     help="Time to run daily if --schedule mode (HH:MM, default: 08:00)")
    parser.add_argument("--count",    type=int, default=config.ARTICLES_PER_DAY, help="Articles per run")
    args = parser.parse_args()

    if args.schedule:
        if not schedule:
            logger.error("'schedule' package not installed. Run: pip install schedule")
            sys.exit(1)
        logger.info(f"Scheduler mode: will run at {args.time} daily")
        schedule.every().day.at(args.time).do(run_daily_batch, articles_per_run=args.count)
        # Run once immediately
        run_daily_batch(articles_per_run=args.count)
        while True:
            schedule.run_pending()
            time.sleep(60)
    else:
        # Single run (used by cron)
        run_daily_batch(articles_per_run=args.count)


if __name__ == "__main__":
    main()
