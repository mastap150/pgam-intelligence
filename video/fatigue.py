"""
video/fatigue.py — content fatigue / diversity thresholds (spec §22).

Tracks recent publishing frequency by destination, country, hook category,
franchise and asset, against the configurable windows in settings(). The
production loop asks allow_production() before creating a video; the asset
matcher asks asset_penalty() when ranking footage.
"""

from datetime import datetime, timedelta, timezone

from video import settings
from video.store import store


def _recent_videos(window_days: int) -> list[dict]:
    cutoff = (datetime.now(timezone.utc) - timedelta(days=window_days)).isoformat()
    s = store()
    return s.find(
        "videos",
        predicate=lambda v: v.get("status") in ("approved", "scheduled", "published")
        and v.get("created_at", "") >= cutoff,
    )


def check(destination: str = "", country: str = "", franchise: str = "",
          hook_category: str = "") -> tuple[bool, list[str]]:
    """(allowed, reasons_blocked). Allowed only when every dimension is under
    its rolling-window threshold."""
    cfg = settings.settings()
    recent = _recent_videos(cfg["fatigue_window_days"])
    s = store()
    blocked: list[str] = []

    def count(pred):
        return sum(1 for v in recent if pred(v))

    if destination:
        n = count(lambda v: (v.get("destination") or "").lower() == destination.lower())
        if n >= cfg["max_per_destination"]:
            blocked.append(f"destination '{destination}' at {n}/{cfg['max_per_destination']} in window")
    if country:
        n = count(lambda v: country.lower() in (v.get("destination") or "").lower()
                  or country.lower() == (v.get("country") or "").lower())
        if n >= cfg["max_per_country"]:
            blocked.append(f"country '{country}' at {n}/{cfg['max_per_country']} in window")
    if franchise:
        n = count(lambda v: v.get("franchise") == franchise)
        if n >= cfg["max_per_franchise"]:
            blocked.append(f"franchise '{franchise}' at {n}/{cfg['max_per_franchise']} in window")
    if hook_category:
        cats = 0
        for v in recent:
            hook = s.get("hooks", v.get("hook_id", "")) or {}
            if hook.get("category") == hook_category:
                cats += 1
        if cats >= cfg["max_per_hook_category"]:
            blocked.append(f"hook category '{hook_category}' at {cats}/{cfg['max_per_hook_category']} in window")

    return (not blocked), blocked


def asset_penalty(asset: dict) -> float:
    """0..1 multiplier applied to an asset's retrieval score. Assets used at
    or beyond max_asset_reuse in the window are heavily suppressed."""
    cfg = settings.settings()
    recent = _recent_videos(cfg["fatigue_window_days"])
    uses = sum(1 for v in recent if asset["id"] in (v.get("asset_ids") or []))
    if uses >= cfg["max_asset_reuse"]:
        return 0.15
    return 1.0 - (0.25 * uses)
