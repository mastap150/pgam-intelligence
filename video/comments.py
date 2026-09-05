"""
video/comments.py — Comment Intelligence (spec §21).

Classifies ingested YouTube comments, extracts destination/topic signals, and
maintains the Viewer Requested Content queue (as recommendations of kind
viewer_requested). Classification is rule-first — cheap, deterministic,
auditable — with the LLM reserved for the ambiguous remainder in a later
phase. Demand topics feed opportunity scoring (§4 audience_interest).
"""

import re
from collections import Counter

from video import settings
from video.models import Comment, Recommendation, new_id
from video.store import store
from video.ingestion import COUNTRY_HINTS

_RULES = [
    ("spam", re.compile(r"(http[s]?://|subscribe back|check my channel|promo code)", re.I)),
    ("fare_request", re.compile(r"\b(fare|flight|price)s?\b.*\b(from|to)\b|\bhow much\b", re.I)),
    ("destination_request", re.compile(r"\b(do|cover|make one (about|on)|please do)\b.{0,30}", re.I)),
    ("correction", re.compile(r"\b(actually|incorrect|wrong|not true|it'?s called)\b", re.I)),
    ("complaint", re.compile(r"\b(clickbait|misleading|waste of time)\b", re.I)),
    ("question", re.compile(r"\?\s*$", re.M)),
    ("negative", re.compile(r"\b(bad|boring|terrible|dislike)\b", re.I)),
    ("positive", re.compile(r"\b(love|great|amazing|helpful|thanks|thank you|saved)\b", re.I)),
]


def classify_text(text: str) -> tuple[str, list[str]]:
    """(classification, topics). Topics are known destination/country tokens."""
    topics = sorted({hint for hint in COUNTRY_HINTS if hint in text.lower()})
    for label, pattern in _RULES:
        if pattern.search(text):
            # A destination mention plus an ask is a content request.
            if label in ("destination_request", "question") and topics:
                return ("destination_request", topics)
            return (label, topics)
    return ("content_request" if topics else "positive", topics)


def ingest(video_id: str, raw_comments: list[dict]) -> list[dict]:
    """raw_comments: [{id?, author, text}] from youtube.fetch_comments()."""
    s = store()
    out = []
    for rc in raw_comments:
        cid = rc.get("id") or new_id("cmt")
        existing = s.get("comments", cid)
        if existing:
            continue
        classification, topics = classify_text(rc.get("text", ""))
        c = Comment(id=cid, video_id=video_id, author=rc.get("author", ""),
                    text=rc.get("text", "")[:2000],
                    classification=classification, topics=topics,
                    status="spam" if classification == "spam" else "classified")
        out.append(s.put("comments", c.to_record()))
    settings.log("comments", f"ingested {len(out)} new comments for {video_id}")
    return out


def refresh_viewer_requests(min_mentions: int = 3) -> list[dict]:
    """Roll classified requests into the Viewer Requested Content queue."""
    s = store()
    requests_ = s.find("comments", predicate=lambda c: c.get("classification") in
                       ("destination_request", "fare_request", "content_request")
                       and c.get("status") == "classified")
    counts = Counter(t for c in requests_ for t in (c.get("topics") or []))
    created = []
    for topic, n in counts.items():
        if n < min_mentions:
            continue
        rec_id = f"rec_viewer_{topic.replace(' ', '_')}"
        existing = s.get("recommendations", rec_id)
        if existing and existing["status"] == "open":
            existing["evidence"]["mentions"] = n
            created.append(s.put("recommendations", existing))
            continue
        rec = Recommendation(
            id=rec_id, kind="viewer_requested",
            finding=f"Viewers are asking for {topic.title()} content ({n} requests)",
            evidence={"topic": topic, "mentions": n},
            suggested_action=f"Queue a {topic.title()} Short in the strongest franchise for the topic",
        )
        created.append(s.put("recommendations", rec.to_record()))
    if created:
        settings.log("comments", f"viewer-requested queue: {len(created)} topics")
    return created
