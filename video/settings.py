"""
video/settings.py — environment config + admin settings for the video engine.

Two layers:
  1. Environment variables (credentials, gates, paths). Never committed.
  2. Admin settings (spec §33) — operational knobs and scoring weights, loaded
     from config/video_engine.json when present, with the defaults below.
     Editors change them from the dashboard; changes are audited to the
     settings_audit table.

Every weight the spec calls configurable (§4 opportunity, §19 video score)
lives here, not in the modules that consume them.
"""

import json
import os
import time
from pathlib import Path

from dotenv import load_dotenv

load_dotenv(override=True)

REPO_ROOT = Path(__file__).resolve().parent.parent

# ---------------------------------------------------------------------------
# Environment
# ---------------------------------------------------------------------------
DATA_DIR = Path(os.environ.get("DVE_DATA_DIR", str(REPO_ROOT / "data" / "video")))
STORE_BACKEND = os.environ.get("DVE_STORE", "json")  # json | postgres
MODEL = os.environ.get("DVE_MODEL", "claude-sonnet-5")
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
ELEVENLABS_API_KEY = os.environ.get("ELEVENLABS_API_KEY", "")

# Hard publish gate, mirroring the TBX_ALLOW_WRITES convention: without this,
# every YouTube publish is a dry run regardless of mode or approval state.
ALLOW_PUBLISH = os.environ.get("DVE_ALLOW_PUBLISH", "0") == "1"

YT_CLIENT_ID = os.environ.get("YT_CLIENT_ID", "")
YT_CLIENT_SECRET = os.environ.get("YT_CLIENT_SECRET", "")
YT_REFRESH_TOKEN = os.environ.get("YT_REFRESH_TOKEN", "")
YT_CHANNEL_ID = os.environ.get("YT_CHANNEL_ID", "")

DASHBOARD_TOKEN = os.environ.get("DVE_DASHBOARD_TOKEN", "")

SITE_BASE_URL = os.environ.get("DVE_SITE_BASE_URL", "https://destination.com")

# ---------------------------------------------------------------------------
# Admin settings (§33) — defaults, overridable via config/video_engine.json
# ---------------------------------------------------------------------------
DEFAULT_SETTINGS: dict = {
    # Cadence & volume
    "daily_short_target": 3,
    "max_daily_videos": 5,
    "weekly_longform_target": 1,
    # Quality gates
    "min_opportunity_score": 60.0,
    "min_qa_verdict": "warning",          # videos at FAIL never pass; warning needs review
    "min_predicted_performance": 40.0,
    # Publishing mode (§32): manual | assisted | auto. Auto is per-franchise.
    "publishing_mode": "manual",
    "auto_publish_franchises": [],        # never includes travel_news
    # Content fatigue thresholds (§22): max in any rolling 7-day window
    "fatigue_window_days": 7,
    "max_per_destination": 2,
    "max_per_country": 3,
    "max_per_hook_category": 4,
    "max_per_franchise": 6,
    "max_asset_reuse": 2,
    # Opportunity score weights (§4) — must sum to 1.0
    "opportunity_weights": {
        "audience_interest": 0.25,
        "youtube_appeal": 0.20,
        "historical_performance": 0.15,
        "editorial_relevance": 0.15,
        "search_demand": 0.10,
        "commercial_potential": 0.10,
        "freshness": 0.05,
    },
    # Destination Video Score weights (§19) — must sum to 1.0
    "video_score_weights": {
        "retention": 0.30,
        "completion": 0.20,
        "shares": 0.15,
        "likes": 0.10,
        "subscriber_conversion": 0.10,
        "comments": 0.05,
        "website_clicks": 0.05,
        "commercial_conversion": 0.05,
    },
    # Fare Drop gate (§14)
    "fare_min_discount_pct": 30.0,
    "fare_min_interest_score": 55.0,
    # Default narration voice per franchise (§12: consistency over novelty)
    "default_voice": "destination_main",
    "franchise_voices": {
        "fare_drop": "deals",
        "points_miles": "points",
        "travel_news": "news",
        "destination_insider": "luxury",
    },
    # Learning engine
    "learning_min_sample": 5,             # videos per segment before a finding is emitted
    "learning_min_lift_pct": 15.0,
}

_SETTINGS_PATH = REPO_ROOT / "config" / "video_engine.json"
_settings_cache: dict | None = None
_settings_mtime: float = 0.0


def settings() -> dict:
    """Admin settings: defaults overlaid with config/video_engine.json."""
    global _settings_cache, _settings_mtime
    try:
        mtime = _SETTINGS_PATH.stat().st_mtime
    except OSError:
        mtime = 0.0
    if _settings_cache is None or mtime != _settings_mtime:
        merged = json.loads(json.dumps(DEFAULT_SETTINGS))  # deep copy
        if mtime:
            try:
                overrides = json.loads(_SETTINGS_PATH.read_text())
                for k, v in overrides.items():
                    if isinstance(v, dict) and isinstance(merged.get(k), dict):
                        merged[k].update(v)
                    else:
                        merged[k] = v
            except (json.JSONDecodeError, OSError) as exc:
                log("settings", f"WARNING: could not read {_SETTINGS_PATH}: {exc}")
        _settings_cache, _settings_mtime = merged, mtime
    return _settings_cache


def log(module: str, msg: str) -> None:
    """Stdout logging, the Render worker convention in this repo."""
    print(f"[dve:{module}] {msg}", flush=True)


def retry(fn, attempts: int = 3, base_delay: float = 2.0, retriable=(Exception,)):
    """Call fn() with exponential backoff. Raises the last error."""
    for i in range(attempts):
        try:
            return fn()
        except retriable:
            if i == attempts - 1:
                raise
            time.sleep(base_delay * (2 ** i))
