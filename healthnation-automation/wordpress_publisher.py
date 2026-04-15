"""
HealthNation Automation — WordPress REST API Publisher
Posts generated articles to WordPress with all meta fields.
"""
import json
import logging
import requests
from datetime import datetime, timezone
from config import (
    WP_SITE_URL, WP_USERNAME, WP_APP_PASS,
    DEFAULT_POST_STATUS, DEFAULT_AUTHOR_ID,
    DEFAULT_REVIEWER, DEFAULT_REVIEWER_CREDENTIALS,
)

logger     = logging.getLogger(__name__)
WP_API     = f"{WP_SITE_URL}/wp-json/wp/v2"
WP_AUTH    = None   # Set at module load time via init()
CATEGORY_CACHE: dict[str, int] = {}
WP_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json",
}


def _parse_json(resp: requests.Response) -> any:
    """Parse JSON response, handling UTF-8 BOM from WP Engine."""
    resp.encoding = "utf-8-sig"
    return json.loads(resp.text)


def init():
    """Validate WP credentials and warm up category ID cache."""
    global WP_AUTH
    WP_AUTH = (WP_USERNAME, WP_APP_PASS.replace(" ", ""))
    _warm_category_cache()


def _warm_category_cache():
    """Fetch all WordPress categories and store slug→ID mapping."""
    try:
        resp = requests.get(
            f"{WP_API}/categories",
            auth=WP_AUTH,
            headers=WP_HEADERS,
            params={"per_page": 100},
            timeout=15,
        )
        resp.raise_for_status()
        for cat in _parse_json(resp):
            CATEGORY_CACHE[cat["slug"]] = cat["id"]
        logger.info(f"  ✓ Loaded {len(CATEGORY_CACHE)} WP categories: {list(CATEGORY_CACHE.keys())}")
    except Exception as e:
        logger.warning(f"Could not load WP categories: {e}")


def get_or_create_category(slug: str, name: str = None) -> int | None:
    """Return category ID, creating it if it doesn't exist."""
    if slug in CATEGORY_CACHE:
        return CATEGORY_CACHE[slug]

    display_name = name or slug.replace("-", " ").title()
    try:
        resp = requests.post(
            f"{WP_API}/categories",
            auth=WP_AUTH,
            headers=WP_HEADERS,
            json={"name": display_name, "slug": slug},
            timeout=15,
        )
        resp.raise_for_status()
        cat_id = _parse_json(resp).get("id")
        if cat_id:
            CATEGORY_CACHE[slug] = cat_id
            logger.info(f"  ✓ Created WP category: {display_name} (ID {cat_id})")
        return cat_id
    except requests.HTTPError as e:
        if e.response and e.response.status_code == 400:
            search = requests.get(f"{WP_API}/categories", auth=WP_AUTH,
                                  headers=WP_HEADERS,
                                  params={"search": display_name}, timeout=10)
            results = _parse_json(search)
            if results:
                cat_id = results[0]["id"]
                CATEGORY_CACHE[slug] = cat_id
                return cat_id
        logger.error(f"Category create/find failed for '{slug}': {e}")
        return None


def publish_article(article: dict, featured_image_id: int | None = None) -> dict | None:
    """
    Publish a fully generated article dict to WordPress.
    Returns the WordPress post data dict on success, None on failure.
    """
    if not WP_AUTH:
        init()

    cat_slug = article.get("category_slug", "health")
    cat_id   = get_or_create_category(cat_slug)
    cat_ids  = [cat_id] if cat_id else []

    payload = {
        "title":      article["h1"],
        "content":    article["html_content"],
        "excerpt":    article.get("excerpt", ""),
        "status":     DEFAULT_POST_STATUS,
        "author":     DEFAULT_AUTHOR_ID,
        "categories": cat_ids,
        "meta": {
            "hn_reviewer_name":        article.get("source_topic", {}).get("reviewer_name", DEFAULT_REVIEWER),
            "hn_reviewer_credentials": article.get("source_topic", {}).get("reviewer_credentials", DEFAULT_REVIEWER_CREDENTIALS),
            "hn_reviewer_specialty":   article.get("reviewer_specialty", ""),
            "hn_read_time":            article.get("estimated_read_time", 8),
            "hn_citation_count":       article.get("citation_count", 0),
            "hn_key_takeaways":        json.dumps(article.get("key_takeaways", [])),
            "hn_references":           json.dumps(article.get("references", [])),
            "hn_focus_keyword":        article.get("focus_keyword", ""),
            "hn_meta_description":     article.get("meta_description", ""),
            "hn_last_reviewed":        datetime.now(timezone.utc).strftime("%B %Y"),
            "hn_ai_generated":         True,
            # Rank Math SEO
            "rank_math_focus_keyword": article.get("focus_keyword", ""),
            "rank_math_description":   article.get("meta_description", ""),
            "rank_math_title":         article.get("meta_title", ""),
            # Yoast SEO
            "_yoast_wpseo_metadesc":   article.get("meta_description", ""),
            "_yoast_wpseo_focuskw":    article.get("focus_keyword", ""),
            "_yoast_wpseo_title":      article.get("meta_title", ""),
        },
    }

    if featured_image_id:
        payload["featured_media"] = featured_image_id

    try:
        resp = requests.post(
            f"{WP_API}/posts",
            auth    = WP_AUTH,
            headers = WP_HEADERS,
            json    = payload,
            timeout = 60,
        )
        resp.raise_for_status()
        post = _parse_json(resp)

        logger.info(
            f"  ✓ Published: '{post['title']['rendered']}'\n"
            f"    URL:  {post['link']}\n"
            f"    ID:   {post['id']}\n"
            f"    Cat:  {cat_slug}\n"
            f"    Read: {article.get('estimated_read_time', '?')} min"
        )
        return post

    except requests.HTTPError as e:
        body = ""
        try: body = _parse_json(e.response)
        except Exception: pass
        logger.error(f"WordPress publish failed (HTTP {e.response.status_code}): {body}")
        return None
    except Exception as e:
        logger.error(f"WordPress publish failed: {e}")
        return None


def article_exists(keyword: str) -> bool:
    """Check if an article with this focus keyword already exists."""
    try:
        resp = requests.get(
            f"{WP_API}/posts",
            auth    = WP_AUTH,
            headers = WP_HEADERS,
            params  = {"search": keyword, "per_page": 5, "status": "publish,draft"},
            timeout = 15,
        )
        resp.raise_for_status()
        return len(_parse_json(resp)) > 0
    except Exception:
        return False
