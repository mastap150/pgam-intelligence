"""
video/models.py — entity dataclasses for the video engine (spec §26).

These are the in-memory shapes; persistence is via video/store.py, which
stores each record as scalars + payload. `to_record()` flattens a dataclass
into the dict the store expects; `from_record()` restores it. Fields not
listed as scalar columns in the migration ride in the payload.
"""

import re
import uuid
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from typing import Any


def new_id(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:12]}"


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def slugify(text: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", (text or "").lower()).strip("-")


HOOK_CATEGORIES = [
    "curiosity", "warning", "surprise", "price", "comparison", "question",
    "contrarian", "luxury", "secret", "urgency", "list", "personal",
]

FRANCHISES = {
    # name: (min_seconds, max_seconds, primary_objective)
    "destination_daily":   (20, 40, "reach_and_subscribers"),
    "fare_drop":           (15, 30, "commercial"),
    "know_before_you_go":  (20, 45, "saves_and_utility"),
    "destination_insider": (20, 45, "premium_positioning"),
    "destination_guides":  (300, 720, "longform_authority"),
    "points_miles":        (20, 45, "commercial"),
    "travel_news":         (15, 40, "reach_and_authority"),
}

SOURCE_TYPES = [
    "article", "news", "destination_page", "fare_alert", "trip_planner",
    "newsletter", "youtube_comment", "search_trend", "seasonal", "external_news",
]


@dataclass
class Base:
    id: str = ""
    status: str = ""
    created_at: str = field(default_factory=now_iso)
    updated_at: str = field(default_factory=now_iso)
    created_by: str = "dve"

    def to_record(self) -> dict:
        return asdict(self)

    @classmethod
    def from_record(cls, rec: dict) -> "Base":
        known = {f for f in cls.__dataclass_fields__}  # type: ignore[attr-defined]
        return cls(**{k: v for k, v in rec.items() if k in known})


@dataclass
class ContentSource(Base):
    source_type: str = "article"
    source_url: str = ""
    headline: str = ""
    summary: str = ""
    body_text: str = ""
    destination: str = ""
    country: str = ""
    region: str = ""
    theme: str = ""
    publish_date: str = ""
    last_updated: str = ""
    author: str = ""
    source_quality_score: float = 0.0
    freshness_score: float = 0.0
    commercial_relevance: float = 0.0
    affiliate_relevance: float = 0.0
    flight_relevance: float = 0.0
    seasonality: str = ""
    keywords: list = field(default_factory=list)
    entities: list = field(default_factory=list)
    images: list = field(default_factory=list)
    video_assets: list = field(default_factory=list)
    rights_status: str = "owned"
    status: str = "active"


@dataclass
class Opportunity(Base):
    source_id: str = ""
    title: str = ""
    destination: str = ""
    franchise: str = ""
    score: float = 0.0
    manual_boost: float = 0.0
    component_scores: dict = field(default_factory=dict)
    reasons: list = field(default_factory=list)
    status: str = "open"


@dataclass
class Concept(Base):
    source_id: str = ""
    franchise: str = "destination_daily"
    working_title: str = ""
    hook: str = ""
    angle: str = ""
    destination: str = ""
    target_length: int = 30
    visual_style: str = ""
    commercial_goal: str = ""
    cta: str = ""
    confidence_score: float = 0.0
    editorial_score: float = 0.0
    originality_score: float = 0.0
    predicted_performance_score: float = 0.0
    generator: str = "llm"       # llm | offline
    status: str = "candidate"


@dataclass
class Hook(Base):
    concept_id: str = ""
    category: str = "curiosity"
    text: str = ""
    generator: str = "llm"
    status: str = "candidate"


@dataclass
class ShotSpec:
    seq: int = 0
    start: float = 0.0
    end: float = 0.0
    description: str = ""
    b_roll: str = ""
    onscreen_text: str = ""
    brand_component: str = ""    # optional theme component key (fare_card, map, …)
    asset_id: str = ""           # filled by asset matching


@dataclass
class Script(Base):
    concept_id: str = ""
    hook_id: str = ""
    voiceover: str = ""
    onscreen_text: list = field(default_factory=list)
    shot_list: list = field(default_factory=list)       # list[ShotSpec as dict]
    captions: list = field(default_factory=list)        # [{start, end, text}]
    music_mood: str = ""
    visual_direction: str = ""
    cta: str = ""
    citations: list = field(default_factory=list)
    fact_check_notes: list = field(default_factory=list)
    generator: str = "llm"
    status: str = "draft"


@dataclass
class Asset(Base):
    asset_type: str = "video"
    file_url: str = ""
    thumbnail: str = ""
    destination: str = ""
    location: str = ""
    description: str = ""
    orientation: str = "vertical"
    duration: float = 0.0
    resolution: str = ""
    source: str = ""
    owner: str = ""
    source_tier: str = "owned"   # owned | contributor | licensed_stock | partner | generative
    license_type: str = ""
    license_start: str = ""
    license_end: str = ""
    allowed_platforms: list = field(default_factory=lambda: ["youtube"])
    attribution_required: bool = False
    attribution_text: str = ""
    commercial_use: bool = True
    ai_generated: bool = False
    rights_verified: bool = False
    usage_count: int = 0
    quality_score: float = 0.0
    status: str = "active"


@dataclass
class FareDeal(Base):
    origin: str = ""
    destination: str = ""
    departure_date: str = ""
    return_date: str = ""
    fare: float = 0.0
    currency: str = "USD"
    cabin: str = "economy"
    carrier: str = ""
    normal_fare_estimate: float = 0.0
    discount_percentage: float = 0.0
    points_price: int = 0
    source: str = ""
    deep_link: str = ""
    expiration: str = ""
    availability: str = ""
    interest_score: float = 0.0
    interest_reasons: list = field(default_factory=list)
    status: str = "new"


@dataclass
class Video(Base):
    concept_id: str = ""
    script_id: str = ""
    hook_id: str = ""
    source_id: str = ""
    franchise: str = ""
    destination: str = ""
    format: str = "short"        # short | longform
    title: str = ""
    description: str = ""
    tags: list = field(default_factory=list)
    duration_seconds: float = 0.0
    voice_id: str = ""
    file_path: str = ""
    asset_ids: list = field(default_factory=list)
    qa_result: str = ""          # pass | warning | fail
    predicted_score: float = 0.0
    video_score: float = 0.0
    ai_voice: bool = False
    ai_visuals: bool = False
    synthetic_scenes: bool = False
    disclosure_required: bool = False
    utm_url: str = ""
    scheduled_at: str = ""
    status: str = "draft"


@dataclass
class QAResult(Base):
    video_id: str = ""
    verdict: str = "fail"        # pass | warning | fail
    checks: list = field(default_factory=list)   # [{name, verdict, detail}]
    status: str = "final"


@dataclass
class PublishingJob(Base):
    video_id: str = ""
    platform: str = "youtube"
    external_id: str = ""
    publish_time: str = ""
    title: str = ""
    description: str = ""
    tags: list = field(default_factory=list)
    thumbnail: str = ""
    playlist: str = ""
    visibility: str = "private"
    upload_status: str = ""
    status: str = "queued"       # queued | dry_run | published | failed


@dataclass
class PerformanceSnapshot(Base):
    video_id: str = ""
    platform: str = "youtube"
    snapshot_label: str = "24h"  # 1h | 6h | 24h | 72h | 7d | 30d
    metrics: dict = field(default_factory=dict)
    status: str = "final"


@dataclass
class Comment(Base):
    video_id: str = ""
    author: str = ""
    text: str = ""
    classification: str = ""
    topics: list = field(default_factory=list)
    status: str = "new"


@dataclass
class Recommendation(Base):
    kind: str = "produce_more"
    finding: str = ""
    evidence: dict = field(default_factory=dict)
    suggested_action: str = ""
    status: str = "open"


@dataclass
class Voice(Base):
    name: str = ""
    provider: str = "elevenlabs"     # elevenlabs | human | silent
    provider_voice_id: str = ""
    gender_style: str = ""
    accent: str = ""
    tone: str = ""
    speed: float = 1.0
    best_content_types: list = field(default_factory=list)
    status: str = "active"


@dataclass
class Experiment(Base):
    name: str = ""
    variable: str = ""               # the ONE attribute under test (§27)
    variant_a: dict = field(default_factory=dict)
    variant_b: dict = field(default_factory=dict)
    video_ids_a: list = field(default_factory=list)
    video_ids_b: list = field(default_factory=list)
    result: dict = field(default_factory=dict)
    status: str = "running"
