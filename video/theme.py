"""
video/theme.py — the Destination video brand system (spec §10).

All brand decisions live in config/video_theme.json — colors, typography,
layout, and per-component settings (logo, location label, fare card, price
card, route graphic, map, captions, lower thirds, source label, CTA, end
card, points card, news alert). Renderers read tokens from here and never
hard-code brand values, which is what makes new layouts additive.
"""

import json
from pathlib import Path

from video.settings import REPO_ROOT, log

_THEME_PATH = REPO_ROOT / "config" / "video_theme.json"
_theme_cache: dict | None = None

COMPONENT_KEYS = [
    "logo", "location_label", "fare_drop_card", "price_card", "route_graphic",
    "map_animation", "lower_third", "article_source_label", "cta", "end_card",
    "points_card", "news_alert",
]


def theme() -> dict:
    global _theme_cache
    if _theme_cache is None:
        _theme_cache = json.loads(_THEME_PATH.read_text())
        missing = [k for k in COMPONENT_KEYS if k not in _theme_cache.get("components", {})]
        if missing:
            log("theme", f"WARNING: theme missing components: {missing}")
    return _theme_cache


def component(name: str) -> dict:
    return theme().get("components", {}).get(name, {})


def color(name: str) -> str:
    return theme().get("colors", {}).get(name, "#FFFFFF")


def layout() -> dict:
    return theme().get("layout", {})


def reload_for_tests(path: Path | None = None):
    global _theme_cache, _THEME_PATH
    if path:
        _THEME_PATH = path
    _theme_cache = None
