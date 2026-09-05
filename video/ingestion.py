"""
video/ingestion.py — content ingestion layer (spec §3).

Normalizes every input into a ContentSource record. Ingesters:

  * ingest_markdown(path)  — the Destination.com editorial corpus in
    content/destination/*.md (front-matter style headers used there).
  * ingest_url(url)        — a live destination.com article/news page.
  * ingest_fare(deal)      — fare alerts land as FareDeal via fare_drop.py,
    plus a fare-typed ContentSource so the opportunity engine sees them.
  * run()                  — scheduler entry: sweep the local corpus and
    refresh freshness scores on everything active.

Comments, search trends and performance signals enter through their own
modules (comments.py, analytics.py) and influence scoring, not this table.
"""

import re
from datetime import datetime, timezone
from html.parser import HTMLParser
from pathlib import Path

import requests

from video import settings
from video.models import ContentSource, new_id
from video.store import store

# Country inference for the corpus + destination pages. Deliberately small;
# unknown destinations simply carry an empty country.
COUNTRY_HINTS = {
    "japan": "Japan", "tokyo": "Japan", "kyoto": "Japan", "osaka": "Japan",
    "italy": "Italy", "rome": "Italy", "amalfi": "Italy", "venice": "Italy",
    "france": "France", "paris": "France", "greece": "Greece", "santorini": "Greece",
    "spain": "Spain", "barcelona": "Spain", "madrid": "Spain", "ibiza": "Spain",
    "portugal": "Portugal", "lisbon": "Portugal", "iceland": "Iceland",
    "croatia": "Croatia", "morocco": "Morocco", "egypt": "Egypt",
    "thailand": "Thailand", "bangkok": "Thailand", "bali": "Indonesia",
    "vietnam": "Vietnam", "cambodia": "Cambodia", "india": "India",
    "mexico": "Mexico", "colombia": "Colombia", "cuba": "Cuba",
    "peru": "Peru", "machu picchu": "Peru", "galapagos": "Ecuador",
    "costa rica": "Costa Rica", "maldives": "Maldives",
    "miami": "United States", "nyc": "United States", "new york": "United States",
    "cape town": "South Africa", "london": "United Kingdom",
}

FLIGHT_WORDS = ("flight", "airfare", "fare", "airline", "business class", "miles", "points")
AFFILIATE_WORDS = ("hotel", "stay", "resort", "booking", "loyalty")


def _infer_country(text: str) -> str:
    low = text.lower()
    for hint, country in COUNTRY_HINTS.items():
        if hint in low:
            return country
    return ""


def _infer_destination(title: str) -> str:
    low = title.lower()
    for hint in COUNTRY_HINTS:
        if hint in low:
            return hint.title()
    return ""


def freshness_score(publish_date: str) -> float:
    """100 for today, decaying ~linearly to 10 at one year old."""
    if not publish_date:
        return 30.0
    try:
        dt = datetime.fromisoformat(publish_date.replace("Z", "+00:00"))
    except ValueError:
        return 30.0
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    age_days = max(0.0, (datetime.now(timezone.utc) - dt).days)
    return max(10.0, 100.0 - (age_days / 365.0) * 90.0)


def _relevance(text: str, words: tuple) -> float:
    low = text.lower()
    hits = sum(low.count(w) for w in words)
    return min(100.0, hits * 12.0)


def _finish(src: ContentSource) -> ContentSource:
    body = f"{src.headline} {src.summary} {src.body_text}"
    src.country = src.country or _infer_country(body[:4000])
    src.destination = src.destination or _infer_destination(src.headline)
    src.freshness_score = freshness_score(src.publish_date or src.last_updated)
    src.flight_relevance = _relevance(body, FLIGHT_WORDS)
    src.affiliate_relevance = _relevance(body, AFFILIATE_WORDS)
    src.commercial_relevance = max(src.flight_relevance, src.affiliate_relevance)
    return src


# ---------------------------------------------------------------------------
# Markdown corpus (content/destination/*.md)
# ---------------------------------------------------------------------------

def ingest_markdown(path: str | Path) -> ContentSource:
    path = Path(path)
    text = path.read_text()
    meta: dict[str, str] = {}
    body = text
    if text.startswith("---"):
        end = text.find("\n---", 3)
        if end > 0:
            header, body = text[3:end], text[end + 4:]
            key = None
            for line in header.splitlines():
                m = re.match(r"^([A-Z_ ]+):\s*(.*)$", line)
                if m:
                    key = m.group(1).strip().lower().replace(" ", "_")
                    meta[key] = m.group(2).strip()
                elif key and line.strip():
                    meta[key] += " " + line.strip()

    title_m = re.search(r"^#\s+(.+)$", body, re.MULTILINE)
    headline = title_m.group(1).strip() if title_m else meta.get("meta_title", path.stem)
    plain = re.sub(r"\[[^\]]*\]|\*+|#+", " ", body)
    plain = re.sub(r"\s+", " ", plain).strip()

    category = meta.get("category", "")
    keywords = [meta.get("target_keyword", "")] + [
        k.strip() for k in meta.get("secondary", "").split(",") if k.strip()
    ]
    src = ContentSource(
        id=f"src_md_{path.stem}",
        source_type="article",
        source_url=f"{settings.SITE_BASE_URL}/guides/{path.stem}",
        headline=headline,
        summary=meta.get("meta_desc", plain[:280]),
        body_text=plain[:20000],
        theme=category.split("/")[0].strip().lower() if category else "",
        keywords=[k for k in keywords if k],
        source_quality_score=80.0,   # first-party editorial
        rights_status="owned",
        publish_date=meta.get("publish_date", ""),
    )
    if "/" in category:
        src.country = category.split("/")[-1].strip()
    return _finish(src)


# ---------------------------------------------------------------------------
# Live URL (destination.com article / news page)
# ---------------------------------------------------------------------------

class _TextExtractor(HTMLParser):
    SKIP = {"script", "style", "nav", "footer", "header"}

    def __init__(self):
        super().__init__()
        self.title = ""
        self._in_title = False
        self._skip_depth = 0
        self.chunks: list[str] = []

    def handle_starttag(self, tag, attrs):
        if tag == "title":
            self._in_title = True
        if tag in self.SKIP:
            self._skip_depth += 1

    def handle_endtag(self, tag):
        if tag == "title":
            self._in_title = False
        if tag in self.SKIP and self._skip_depth:
            self._skip_depth -= 1

    def handle_data(self, data):
        if self._in_title and not self.title:
            self.title = data.strip()
        elif not self._skip_depth and data.strip():
            self.chunks.append(data.strip())


def ingest_url(url: str, source_type: str = "article") -> ContentSource:
    resp = requests.get(url, timeout=30, headers={"User-Agent": "DVE/1.0 (+destination.com)"})
    resp.raise_for_status()
    parser = _TextExtractor()
    parser.feed(resp.text)
    body = " ".join(parser.chunks)
    src = ContentSource(
        id=new_id("src"),
        source_type=source_type,
        source_url=url,
        headline=parser.title,
        summary=body[:280],
        body_text=body[:20000],
        source_quality_score=80.0 if "destination.com" in url else 50.0,
        rights_status="owned" if "destination.com" in url else "external",
        publish_date=datetime.now(timezone.utc).date().isoformat(),
    )
    return _finish(src)


# ---------------------------------------------------------------------------
# Scheduler entry
# ---------------------------------------------------------------------------

def run(corpus_dir: str | Path | None = None) -> int:
    """Sweep the markdown corpus into the store; refresh freshness on
    existing sources. Idempotent (stable ids, upsert)."""
    corpus = Path(corpus_dir) if corpus_dir else settings.REPO_ROOT / "content" / "destination"
    s = store()
    n = 0
    for md in sorted(corpus.glob("*.md")):
        if md.name.upper().startswith(("PUBLISH", "README")):
            continue
        try:
            src = ingest_markdown(md)
        except Exception as exc:
            settings.log("ingestion", f"WARNING: skipped {md.name}: {exc}")
            continue
        existing = s.get("content_sources", src.id)
        if existing:
            existing["freshness_score"] = freshness_score(
                existing.get("publish_date") or existing.get("last_updated") or "")
            s.put("content_sources", existing)
        else:
            s.put("content_sources", src.to_record())
            n += 1
    settings.log("ingestion", f"corpus sweep complete: {n} new sources")
    return n
