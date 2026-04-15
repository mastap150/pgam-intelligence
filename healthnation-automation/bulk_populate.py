"""
HealthNation Automation — Bulk Content Populator
Generates and publishes all 50 articles from topics_queue.json.
Run this ONCE to seed your site with initial content.

Usage:
    python bulk_populate.py                   # Publish all topics
    python bulk_populate.py --dry-run         # Generate only, don't publish
    python bulk_populate.py --limit 10        # First 10 topics only
    python bulk_populate.py --priority 1      # Only priority-1 topics
    python bulk_populate.py --category nutrition  # One category only
    python bulk_populate.py --resume          # Skip already-published topics
"""
import sys
import json
import time
import logging
import argparse
from pathlib import Path
from datetime import datetime

import config
from generate_article    import generate_article
from unsplash_image      import get_or_upload_image
from wordpress_publisher import publish_article, article_exists, init as wp_init

# ─── Logging ─────────────────────────────────────────────────────────────────
log_file = Path(__file__).parent / "logs" / f"bulk_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
log_file.parent.mkdir(exist_ok=True)

logging.basicConfig(
    level   = logging.INFO,
    format  = "%(asctime)s [%(levelname)s] %(message)s",
    handlers= [
        logging.FileHandler(log_file),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger(__name__)

# ─── Progress file (for --resume) ────────────────────────────────────────────
PROGRESS_FILE = Path(__file__).parent / "bulk_progress.json"


def load_progress() -> set:
    if PROGRESS_FILE.exists():
        with open(PROGRESS_FILE) as f:
            return set(json.load(f).get("published_keywords", []))
    return set()


def save_progress(published: set):
    with open(PROGRESS_FILE, "w") as f:
        json.dump({"published_keywords": list(published), "updated": datetime.now().isoformat()}, f, indent=2)


# ─── Main ─────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="Bulk populate HealthNation with articles")
    parser.add_argument("--dry-run",   action="store_true", help="Generate articles but don't publish")
    parser.add_argument("--limit",     type=int,            help="Max articles to process")
    parser.add_argument("--priority",  type=int,            help="Only process topics with this priority (1=highest)")
    parser.add_argument("--category",  type=str,            help="Only process topics in this category slug")
    parser.add_argument("--resume",    action="store_true", help="Skip already-published topics")
    parser.add_argument("--delay",     type=int, default=8, help="Seconds between API calls (default: 8)")
    args = parser.parse_args()

    # Validate environment
    try:
        config.validate()
    except EnvironmentError as e:
        logger.error(str(e))
        print("\n" + config.DOTENV_TEMPLATE)
        sys.exit(1)

    # Init WordPress connection (unless dry run)
    if not args.dry_run:
        wp_init()

    # Load topic queue
    queue_file = Path(__file__).parent / "topics_queue.json"
    with open(queue_file) as f:
        all_topics = json.load(f)

    # Apply filters
    topics = all_topics
    if args.priority:
        topics = [t for t in topics if t.get("priority", 1) <= args.priority]
    if args.category:
        topics = [t for t in topics if t.get("category") == args.category]

    # Sort by priority
    topics.sort(key=lambda t: t.get("priority", 99))

    # Apply limit
    if args.limit:
        topics = topics[:args.limit]

    # Resume: skip already-published
    published_kws = set()
    if args.resume:
        published_kws = load_progress()
        before = len(topics)
        topics = [t for t in topics if t["keyword"] not in published_kws]
        logger.info(f"Resume mode: skipped {before - len(topics)} already-published topics")

    logger.info(f"{'DRY RUN — ' if args.dry_run else ''}Starting bulk populate: {len(topics)} articles")
    logger.info(f"Estimated time: ~{len(topics) * (args.delay + 30) // 60} minutes")

    results = {"success": [], "failed": [], "skipped": []}

    for i, topic in enumerate(topics, 1):
        logger.info(f"\n[{i}/{len(topics)}] {topic['title']}")

        # Check if article already exists in WP (unless dry run or resume already handled it)
        if not args.dry_run and not args.resume:
            if article_exists(topic["keyword"]):
                logger.info(f"  ⚠ Already exists — skipping")
                results["skipped"].append(topic["keyword"])
                continue

        try:
            # 1. Generate article content
            article = generate_article(topic)

            if args.dry_run:
                logger.info(f"  DRY RUN — Would publish: '{article['h1']}'")
                logger.info(f"  Meta title: {article['meta_title']}")
                logger.info(f"  Excerpt: {article['excerpt'][:100]}...")
                results["success"].append(topic["keyword"])
                continue

            # 2. Fetch and upload hero image
            image_id = get_or_upload_image(article)

            # 3. Publish to WordPress
            post = publish_article(article, featured_image_id=image_id)

            if post:
                results["success"].append(topic["keyword"])
                published_kws.add(topic["keyword"])
                save_progress(published_kws)
            else:
                results["failed"].append(topic["keyword"])

        except Exception as e:
            logger.error(f"  ✗ Failed: {e}")
            results["failed"].append(topic["keyword"])

        # Respectful delay between API calls
        if i < len(topics):
            logger.info(f"  Waiting {args.delay}s before next article…")
            time.sleep(args.delay)

    # Summary
    logger.info(f"""
{'='*60}
BULK POPULATE COMPLETE
{'='*60}
Total topics:  {len(topics)}
Published:     {len(results['success'])}
Failed:        {len(results['failed'])}
Skipped:       {len(results['skipped'])}
Log file:      {log_file}
{'='*60}""")

    if results["failed"]:
        logger.warning(f"Failed topics: {results['failed']}")


if __name__ == "__main__":
    main()
