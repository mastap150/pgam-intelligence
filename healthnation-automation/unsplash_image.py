"""
HealthNation Automation — Unsplash Image Fetcher
Fetches a relevant high-quality photo and uploads it to WordPress.
"""
import io
import json
import logging
import requests
from config import UNSPLASH_ACCESS_KEY, WP_SITE_URL, WP_USERNAME, WP_APP_PASS

logger = logging.getLogger(__name__)

UNSPLASH_BASE = "https://api.unsplash.com"
WP_MEDIA_URL  = f"{WP_SITE_URL}/wp-json/wp/v2/media"

# Health content query fallbacks by category
CATEGORY_FALLBACK_QUERIES = {
    "nutrition":     "healthy food vegetables colorful",
    "fitness":       "workout exercise gym lifestyle",
    "mental-health": "meditation mindfulness calm nature",
    "longevity":     "active healthy aging lifestyle",
    "sleep":         "bedroom sleep rest calm",
    "conditions":    "doctor health medical lifestyle",
}


def fetch_unsplash_photo(query: str, category_slug: str = "") -> dict | None:
    """
    Search Unsplash for a relevant photo.
    Returns dict with: url, photographer, photo_id, download_url
    """
    if not UNSPLASH_ACCESS_KEY:
        logger.warning("UNSPLASH_ACCESS_KEY not set — skipping image fetch")
        return None

    # Try primary query first, fall back to category query
    for search_query in [query, CATEGORY_FALLBACK_QUERIES.get(category_slug, "health wellness")]:
        try:
            resp = requests.get(
                f"{UNSPLASH_BASE}/search/photos",
                params={
                    "query":       search_query,
                    "per_page":    10,
                    "orientation": "landscape",
                    "content_filter": "high",
                },
                headers={"Authorization": f"Client-ID {UNSPLASH_ACCESS_KEY}"},
                timeout=15,
            )
            resp.raise_for_status()
            data = resp.json()
            photos = data.get("results", [])

            if photos:
                # Pick best photo: prefer ones with good description
                photo = photos[0]
                return {
                    "photo_id":        photo["id"],
                    "url":             photo["urls"]["regular"],  # 1080px wide
                    "url_full":        photo["urls"]["full"],
                    "photographer":    photo["user"]["name"],
                    "photographer_url":photo["user"]["links"]["html"],
                    "alt_description": photo.get("alt_description") or search_query,
                    "download_url":    photo["links"]["download_location"],
                    "width":           photo["width"],
                    "height":          photo["height"],
                }
        except requests.RequestException as e:
            logger.warning(f"Unsplash search failed for '{search_query}': {e}")

    return None


def trigger_unsplash_download(download_url: str):
    """Notify Unsplash of a download (required by their API guidelines)."""
    if not UNSPLASH_ACCESS_KEY or not download_url:
        return
    try:
        requests.get(
            download_url,
            headers={"Authorization": f"Client-ID {UNSPLASH_ACCESS_KEY}"},
            timeout=10,
        )
    except Exception:
        pass  # Non-critical


def upload_image_to_wordpress(photo: dict, post_title: str) -> int | None:
    """
    Download the Unsplash photo and upload it to WordPress Media Library.
    Returns the WordPress attachment ID, or None on failure.
    """
    try:
        # Download photo bytes
        img_resp = requests.get(photo["url"], timeout=30)
        img_resp.raise_for_status()
        img_bytes = img_resp.content

        # Build filename from title
        from slugify import slugify
        filename = slugify(post_title)[:60] + ".jpg"

        wp_headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Content-Disposition": f'attachment; filename="{filename}"',
            "Content-Type":        "image/jpeg",
        }

        # Upload to WordPress
        upload_resp = requests.post(
            WP_MEDIA_URL,
            auth    = (WP_USERNAME, WP_APP_PASS.replace(" ", "")),
            headers = wp_headers,
            data    = img_bytes,
            timeout = 60,
        )
        upload_resp.raise_for_status()
        upload_resp.encoding = "utf-8-sig"
        wp_media = json.loads(upload_resp.text)

        attachment_id = wp_media.get("id")
        if not attachment_id:
            logger.error(f"WordPress media upload returned no ID: {wp_media}")
            return None

        # Update alt text
        requests.post(
            f"{WP_MEDIA_URL}/{attachment_id}",
            auth = (WP_USERNAME, WP_APP_PASS),
            headers = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"},
            json = {
                "alt_text": photo.get("alt_description", post_title),
                "caption":  f'Photo by <a href="{photo["photographer_url"]}?utm_source=healthnation&utm_medium=referral">{photo["photographer"]}</a> on <a href="https://unsplash.com/?utm_source=healthnation&utm_medium=referral">Unsplash</a>',
            },
            timeout = 15,
        )

        # Trigger Unsplash download notification (API guidelines)
        trigger_unsplash_download(photo.get("download_url", ""))

        logger.info(f"  ✓ Image uploaded: attachment ID {attachment_id} (by {photo['photographer']})")
        return attachment_id

    except Exception as e:
        logger.error(f"Image upload failed: {e}")
        return None


def get_or_upload_image(article: dict) -> int | None:
    """
    Main entry point: fetch a relevant photo and upload to WordPress.
    Returns WordPress attachment ID.
    """
    query    = article.get("unsplash_search_query", article.get("focus_keyword", "health"))
    cat_slug = article.get("category_slug", "")
    title    = article.get("h1", "health article")

    photo = fetch_unsplash_photo(query, cat_slug)
    if not photo:
        logger.warning("No Unsplash photo found — article will publish without featured image")
        return None

    return upload_image_to_wordpress(photo, title)
