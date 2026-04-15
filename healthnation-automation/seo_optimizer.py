"""
HealthNation Automation — SEO Optimizer
Fetches published WordPress posts and uses Claude to improve their SEO meta fields.

Usage:
    python seo_optimizer.py                     # Optimize next 10 posts (page 1)
    python seo_optimizer.py --batch 20          # Optimize 20 posts
    python seo_optimizer.py --page 2            # Process page 2 of published posts
    python seo_optimizer.py --force             # Re-optimize already-processed posts
    python seo_optimizer.py --batch 5 --page 3  # 5 posts from page 3
"""
import sys
import json
import time
import logging
import argparse
from pathlib import Path
from datetime import datetime

import requests
import anthropic

import config

# ─── Logging ─────────────────────────────────────────────────────────────────
BASE_DIR = Path(__file__).parent
LOG_DIR  = BASE_DIR / "logs"
LOG_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    level    = logging.INFO,
    format   = "%(asctime)s [%(levelname)s] %(message)s",
    handlers = [
        logging.FileHandler(LOG_DIR / "seo_optimizer.log"),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger(__name__)

# ─── Constants ────────────────────────────────────────────────────────────────
WP_API         = f"{config.WP_SITE_URL}/wp-json/wp/v2"
WP_AUTH        = None   # Initialised in init()
WP_HEADERS     = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json",
}
PROGRESS_FILE  = BASE_DIR / "seo_progress.json"
INTER_POST_DELAY = 2  # seconds between posts


# ─── Helpers ──────────────────────────────────────────────────────────────────

def _parse_json(resp: requests.Response):
    """Parse JSON response, handling UTF-8 BOM from WP Engine."""
    resp.encoding = "utf-8-sig"
    return json.loads(resp.text)


def init():
    """Initialise shared auth tuple from config."""
    global WP_AUTH
    WP_AUTH = (config.WP_USERNAME, config.WP_APP_PASS.replace(" ", ""))


# ─── Progress tracking ────────────────────────────────────────────────────────

def load_progress() -> dict:
    """Load the SEO optimisation progress tracker."""
    if PROGRESS_FILE.exists():
        with open(PROGRESS_FILE) as f:
            return json.load(f)
    return {"optimized": {}}


def save_progress(data: dict):
    with open(PROGRESS_FILE, "w") as f:
        json.dump(data, f, indent=2)


def mark_optimized(progress: dict, post_id: int, post_title: str, seo_data: dict):
    """Record a post as having been optimised."""
    progress["optimized"][str(post_id)] = {
        "title":        post_title,
        "optimized_at": datetime.now().isoformat(),
        "meta_title":   seo_data.get("meta_title", ""),
        "focus_keyword":seo_data.get("focus_keyword", ""),
    }
    save_progress(progress)


def is_optimized(progress: dict, post_id: int) -> bool:
    return str(post_id) in progress["optimized"]


# ─── WordPress interaction ────────────────────────────────────────────────────

def fetch_posts(page: int = 1, per_page: int = 10) -> list[dict]:
    """
    Fetch a page of published posts from WordPress REST API.
    Returns a list of post dicts.
    """
    try:
        resp = requests.get(
            f"{WP_API}/posts",
            auth    = WP_AUTH,
            headers = WP_HEADERS,
            params  = {
                "status":   "publish",
                "per_page": per_page,
                "page":     page,
                "orderby":  "date",
                "order":    "desc",
                "_fields":  "id,title,excerpt,content,link,meta",
            },
            timeout = 20,
        )
        resp.raise_for_status()
        posts = _parse_json(resp)
        total = resp.headers.get("X-WP-Total", "?")
        total_pages = resp.headers.get("X-WP-TotalPages", "?")
        logger.info(
            f"Fetched page {page}/{total_pages} — "
            f"{len(posts)} posts (total: {total})"
        )
        return posts
    except requests.HTTPError as e:
        if e.response is not None and e.response.status_code == 400:
            logger.warning(f"Page {page} does not exist (no more posts).")
            return []
        logger.error(f"Failed to fetch posts (page {page}): {e}")
        return []
    except Exception as e:
        logger.error(f"Failed to fetch posts: {e}")
        return []


def update_post_seo(post_id: int, seo_data: dict) -> bool:
    """
    PATCH a WordPress post with improved SEO meta fields.
    Supports Rank Math, Yoast, and custom hn_ meta fields.
    """
    meta_title       = seo_data.get("meta_title", "")
    meta_description = seo_data.get("meta_description", "")
    focus_keyword    = seo_data.get("focus_keyword", "")

    payload = {
        "meta": {
            # Rank Math SEO
            "rank_math_title":         meta_title,
            "rank_math_description":   meta_description,
            "rank_math_focus_keyword": focus_keyword,
            # Yoast SEO
            "_yoast_wpseo_title":      meta_title,
            "_yoast_wpseo_metadesc":   meta_description,
            "_yoast_wpseo_focuskw":    focus_keyword,
            # HealthNation custom meta
            "hn_meta_description":     meta_description,
            "hn_focus_keyword":        focus_keyword,
        }
    }

    try:
        resp = requests.post(
            f"{WP_API}/posts/{post_id}",
            auth    = WP_AUTH,
            headers = WP_HEADERS,
            json    = payload,
            timeout = 30,
        )
        resp.raise_for_status()
        logger.info(f"  ✓ Updated post ID {post_id} with new SEO meta fields")
        return True
    except requests.HTTPError as e:
        body = ""
        try:
            body = _parse_json(e.response)
        except Exception:
            pass
        logger.error(f"  ✗ Failed to update post {post_id} (HTTP {e.response.status_code}): {body}")
        return False
    except Exception as e:
        logger.error(f"  ✗ Failed to update post {post_id}: {e}")
        return False


# ─── Claude SEO generation ────────────────────────────────────────────────────

def build_seo_prompt(title: str, excerpt: str) -> str:
    """Build a concise prompt asking Claude for SEO improvements."""
    # Strip HTML tags from excerpt for cleaner context
    import re
    clean_excerpt = re.sub(r"<[^>]+>", "", excerpt or "").strip()[:600]

    return f"""You are an expert health-content SEO specialist.

Given the article title and excerpt below, produce improved SEO metadata.
Return ONLY a valid JSON object with exactly these keys:
- meta_title        (string, max 60 chars, keyword-rich, no quotes needed)
- meta_description  (string, max 160 chars, includes a soft CTA such as "Learn more" or "Discover")
- focus_keyword     (string, single primary keyword phrase, 2–4 words)

Article title: {title}

Excerpt: {clean_excerpt}

Return only the JSON object, no markdown fences, no extra text."""


def generate_seo_improvements(post: dict) -> dict | None:
    """
    Call Claude to analyse a post and return improved SEO fields.
    Returns a dict with keys: meta_title, meta_description, focus_keyword.
    Returns None on failure.
    """
    title   = post.get("title", {}).get("rendered", "")
    excerpt = post.get("excerpt", {}).get("rendered", "")

    if not title:
        logger.warning(f"  Post ID {post.get('id')} has no title — skipping Claude call")
        return None

    prompt = build_seo_prompt(title, excerpt)

    try:
        client = anthropic.Anthropic(api_key=config.ANTHROPIC_API_KEY)
        message = client.messages.create(
            model      = config.CLAUDE_MODEL,
            max_tokens = 512,   # SEO fields are short — no need for 8k
            messages   = [{"role": "user", "content": prompt}],
        )
        raw = message.content[0].text.strip()

        # Strip markdown fences if Claude adds them despite instructions
        if raw.startswith("```"):
            raw = raw.split("```")[1]
            if raw.startswith("json"):
                raw = raw[4:]
            raw = raw.strip()

        seo_data = json.loads(raw)

        # Enforce length limits
        seo_data["meta_title"]       = seo_data.get("meta_title", "")[:60].strip()
        seo_data["meta_description"] = seo_data.get("meta_description", "")[:160].strip()
        seo_data["focus_keyword"]    = seo_data.get("focus_keyword", "").strip()

        logger.info(
            f"  Claude SEO → title: '{seo_data['meta_title']}' | "
            f"kw: '{seo_data['focus_keyword']}'"
        )
        return seo_data

    except json.JSONDecodeError as e:
        logger.error(f"  Claude returned invalid JSON: {e} — raw: {raw[:200]}")
        return None
    except Exception as e:
        logger.error(f"  Claude API call failed: {e}")
        return None


# ─── Core batch runner ────────────────────────────────────────────────────────

def run_seo_batch(batch_size: int = 10, page: int = 1, force: bool = False) -> int:
    """
    Fetch one page of posts and run SEO optimisation on them.

    Args:
        batch_size: Maximum number of posts to optimise in this run.
        page:       Which page of WordPress posts to fetch.
        force:      If True, re-optimise posts that were already processed.

    Returns:
        Number of posts successfully optimised.
    """
    logger.info("=" * 55)
    logger.info(f"SEO Optimizer run: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"Batch size: {batch_size}  |  Page: {page}  |  Force: {force}")

    try:
        config.validate()
    except EnvironmentError as e:
        logger.error(str(e))
        return 0

    init()
    progress = load_progress()

    posts = fetch_posts(page=page, per_page=batch_size)
    if not posts:
        logger.warning("No posts returned — nothing to optimise.")
        return 0

    optimized_count = 0

    for i, post in enumerate(posts, 1):
        post_id    = post["id"]
        post_title = post.get("title", {}).get("rendered", f"Post {post_id}")

        logger.info(f"\n[{i}/{len(posts)}] Post ID {post_id}: {post_title}")

        if not force and is_optimized(progress, post_id):
            logger.info("  Already optimised — skipping (use --force to redo)")
            continue

        seo_data = generate_seo_improvements(post)
        if not seo_data:
            logger.warning(f"  Could not generate SEO data for post {post_id} — skipping")
            if i < len(posts):
                time.sleep(INTER_POST_DELAY)
            continue

        success = update_post_seo(post_id, seo_data)
        if success:
            mark_optimized(progress, post_id, post_title, seo_data)
            optimized_count += 1
        else:
            logger.warning(f"  WP update failed for post {post_id}")

        if i < len(posts):
            time.sleep(INTER_POST_DELAY)

    total_done = len(progress["optimized"])
    logger.info(f"\nBatch complete. Optimised this run: {optimized_count}")
    logger.info(f"Total posts optimised to date: {total_done}")
    logger.info("=" * 55)
    return optimized_count


# ─── Entry point ─────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="HealthNation SEO Optimizer — improves meta fields on existing posts"
    )
    parser.add_argument(
        "--batch", type=int, default=10,
        help="Number of posts to optimise per run (default: 10)"
    )
    parser.add_argument(
        "--page", type=int, default=1,
        help="Which page of WordPress posts to process (default: 1)"
    )
    parser.add_argument(
        "--force", action="store_true",
        help="Re-optimise posts that have already been processed"
    )
    args = parser.parse_args()

    run_seo_batch(batch_size=args.batch, page=args.page, force=args.force)


if __name__ == "__main__":
    main()
