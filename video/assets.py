"""
video/assets.py — Visual Asset System + Asset Agent (spec §8, §9).

The asset library is metadata-first: every asset row carries full rights
metadata and the matcher REFUSES anything not rights-verified, expired, or
platform-disallowed — enforcement lives here, not in QA alone.

Matching: tier priority (owned > contributor > licensed > partner >
generative) × destination/description relevance × quality × diversity
penalty from fatigue.py. Wrong-place footage is the worst failure mode, so
a destination mismatch is a hard filter, not a score penalty.
"""

from datetime import datetime, timezone

from video import fatigue, settings
from video.models import Asset, new_id
from video.store import store

TIER_SCORE = {
    "owned": 100, "contributor": 85, "licensed_stock": 70,
    "partner": 60, "generative": 40,
}
MIN_VERTICAL_HEIGHT = 1280  # reject low-res for 1080x1920 output


def register(payload: dict) -> dict:
    """Add an asset to the library. rights_verified defaults False — a human
    or the rights-import job flips it after checking the license."""
    asset = Asset(id=payload.get("id") or new_id("ast"), **{
        k: v for k, v in payload.items()
        if k in Asset.__dataclass_fields__ and k != "id"
    })
    return store().put("assets", asset.to_record())


def _license_valid(asset: dict, platform: str) -> bool:
    if not asset.get("rights_verified"):
        return False
    if platform not in (asset.get("allowed_platforms") or []):
        return False
    end = asset.get("license_end")
    if end:
        try:
            if datetime.fromisoformat(end).date() < datetime.now(timezone.utc).date():
                return False
        except ValueError:
            return False
    return True


def _resolution_ok(asset: dict) -> bool:
    res = asset.get("resolution") or ""
    if "x" not in res:
        return True  # unknown: allow, QA warns
    try:
        _, h = res.lower().split("x")
        return int(h) >= MIN_VERTICAL_HEIGHT
    except ValueError:
        return True


def _relevance(asset: dict, shot: dict, destination: str) -> float:
    """0-100 text relevance of the asset to the shot request."""
    want = f"{shot.get('description', '')} {shot.get('b_roll', '')}".lower()
    have = f"{asset.get('description', '')} {asset.get('location', '')}".lower()
    if not want.strip() or not have.strip():
        return 30.0
    want_tokens = {t for t in want.split() if len(t) > 3}
    hits = sum(1 for t in want_tokens if t in have)
    base = min(70.0, 15.0 * hits)
    if destination and destination.lower() in have:
        base += 30.0
    return min(100.0, base)


def match_shot(shot: dict, destination: str, platform: str = "youtube",
               exclude_ids: set | None = None) -> dict | None:
    """Best rights-clean asset for one shot, or None (QA flags gaps)."""
    s = store()
    exclude_ids = exclude_ids or set()
    dest_l = (destination or "").lower()

    candidates = []
    for a in s.find("assets", {"status": "active"}):
        if a["id"] in exclude_ids:
            continue
        if not _license_valid(a, platform) or not _resolution_ok(a):
            continue
        a_dest = (a.get("destination") or "").lower()
        # Hard geography filter (§9: wrong countries/landmarks are the worst
        # failure). Destination-agnostic assets (maps, graphics) pass.
        if a_dest and dest_l and a_dest != dest_l and dest_l not in a_dest and a_dest not in dest_l:
            continue
        score = (
            0.35 * TIER_SCORE.get(a.get("source_tier", "generative"), 40)
            + 0.35 * _relevance(a, shot, destination)
            + 0.30 * float(a.get("quality_score", 50.0))
        ) * fatigue.asset_penalty(a)
        candidates.append((score, a))

    if not candidates:
        return None
    candidates.sort(key=lambda c: c[0], reverse=True)
    return candidates[0][1]


def match_script(script: dict, destination: str, platform: str = "youtube") -> list[dict]:
    """Match every shot; avoids repeating an asset within one video. Returns
    [{shot_seq, asset_id | None}]. Increments usage_count on winners."""
    s = store()
    used: set = set()
    out = []
    for shot in script.get("shot_list", []):
        asset = match_shot(shot, destination, platform, exclude_ids=used)
        if asset:
            used.add(asset["id"])
            asset["usage_count"] = int(asset.get("usage_count", 0)) + 1
            s.put("assets", asset)
            out.append({"shot_seq": shot.get("seq"), "asset_id": asset["id"]})
        else:
            out.append({"shot_seq": shot.get("seq"), "asset_id": None})
    gaps = sum(1 for m in out if not m["asset_id"])
    if gaps:
        settings.log("assets", f"WARNING: {gaps}/{len(out)} shots have no rights-clean asset")
    return out
