"""
video/attribution.py — Website attribution (spec §24).

Builds trackable Destination.com URLs per video and records conversion events
joined back to the originating video. Event capture happens site-side
(destination-com repo — GA4 / first-party events keyed on utm_content); this
module owns link construction and the video-side join, and record_event() is
the ingestion point for whatever exporter lands first.
"""

from urllib.parse import urlencode, urlparse, urlunparse, parse_qsl

from video import settings
from video.models import new_id
from video.store import store

EVENT_TYPES = [
    "session", "newsletter_signup", "flight_search", "trip_planner",
    "hotel_click", "flight_click", "affiliate_click", "conversion",
]


def build_utm_url(video_id: str, landing_path: str = "/",
                  campaign: str = "destination_shorts") -> str:
    base = landing_path if landing_path.startswith("http") \
        else settings.SITE_BASE_URL.rstrip("/") + landing_path
    parts = urlparse(base)
    q = dict(parse_qsl(parts.query))
    q.update({
        "utm_source": "youtube",
        "utm_medium": "organic_video",
        "utm_campaign": campaign,
        "utm_content": video_id,
    })
    return urlunparse(parts._replace(query=urlencode(q)))


def record_event(video_id: str, event_type: str, detail: dict | None = None) -> dict:
    if event_type not in EVENT_TYPES:
        raise ValueError(f"event_type must be one of {EVENT_TYPES}")
    rec = {
        "id": new_id("attr"), "video_id": video_id, "event_type": event_type,
        "detail": detail or {}, "status": "final", "created_by": "dve",
    }
    return store().put("attribution_events", rec)


def video_conversions(video_id: str) -> dict[str, int]:
    events = store().find("attribution_events", {"video_id": video_id})
    out = {e: 0 for e in EVENT_TYPES}
    for ev in events:
        out[ev["event_type"]] = out.get(ev["event_type"], 0) + 1
    return out


def merge_into_metrics(video_id: str, metrics: dict) -> dict:
    """Fold attributed conversions into a snapshot's metric payload so the
    video score's website/commercial components see them (§19)."""
    conv = video_conversions(video_id)
    metrics = dict(metrics)
    metrics["website_clicks"] = metrics.get("website_clicks", 0) or conv["session"]
    metrics["commercial_conversions"] = (
        conv["flight_search"] + conv["affiliate_click"] + conv["conversion"]
        + conv["newsletter_signup"])
    return metrics
