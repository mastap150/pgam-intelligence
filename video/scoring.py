"""
video/scoring.py — Destination Video Score (spec §19).

Never raw views: a weighted composite over normalized engagement metrics,
weights configurable in settings()["video_score_weights"]. Dimension scores
(hook / topic / destination / franchise / voice / length / visual style /
CTA) are averages of video scores grouped by that attribute — computed on
demand for the learning engine and dashboard.
"""

from collections import defaultdict

from video import settings
from video.store import store

# Normalizers: metric -> value considered "100". Everything linear-capped.
NORMALIZERS = {
    "retention": 100.0,               # avg_percentage_viewed is already 0-100
    "completion": 100.0,              # completion_rate 0-100
    "shares_per_1k": 8.0,             # 8 shares per 1k views = excellent
    "likes_per_1k": 60.0,
    "subs_per_1k": 4.0,
    "comments_per_1k": 6.0,
    "clicks_per_1k": 10.0,
    "conversions_per_1k": 1.5,
}


def _per_1k(metrics: dict, key: str) -> float:
    views = max(1.0, float(metrics.get("views", 0)))
    return 1000.0 * float(metrics.get(key, 0)) / views


def compute_video_score(metrics: dict) -> float:
    """metrics: the §18 snapshot payload. Returns 0-100."""
    w = settings.settings()["video_score_weights"]
    norm = {
        "retention": float(metrics.get("average_percentage_viewed", 0)),
        "completion": float(metrics.get("completion_rate", 0)),
        "shares": 100.0 * min(1.0, _per_1k(metrics, "shares") / NORMALIZERS["shares_per_1k"]),
        "likes": 100.0 * min(1.0, _per_1k(metrics, "likes") / NORMALIZERS["likes_per_1k"]),
        "subscriber_conversion": 100.0 * min(1.0, _per_1k(metrics, "subscribers_gained")
                                             / NORMALIZERS["subs_per_1k"]),
        "comments": 100.0 * min(1.0, _per_1k(metrics, "comments") / NORMALIZERS["comments_per_1k"]),
        "website_clicks": 100.0 * min(1.0, _per_1k(metrics, "website_clicks")
                                      / NORMALIZERS["clicks_per_1k"]),
        "commercial_conversion": 100.0 * min(1.0, _per_1k(metrics, "commercial_conversions")
                                             / NORMALIZERS["conversions_per_1k"]),
    }
    return round(sum(norm[k] * w[k] for k in w), 1)


def latest_metrics(video_id: str) -> dict | None:
    """The most mature snapshot available for a video."""
    order = {"1h": 1, "6h": 2, "24h": 3, "72h": 4, "7d": 5, "30d": 6}
    s = store()
    snaps = s.find("performance_snapshots", {"video_id": video_id})
    if not snaps:
        return None
    best = max(snaps, key=lambda x: order.get(x.get("snapshot_label", ""), 0))
    return best.get("metrics")


def rescore_all() -> int:
    """Recompute video_score for every published video with snapshots."""
    s = store()
    n = 0
    for v in s.find("videos", {"status": "published"}):
        metrics = latest_metrics(v["id"])
        if not metrics:
            continue
        score = compute_video_score(metrics)
        if score != v.get("video_score"):
            v["video_score"] = score
            s.put("videos", v)
            n += 1
    settings.log("scoring", f"rescored {n} videos")
    return n


def _length_bucket(seconds: float) -> str:
    for lo, hi in ((0, 20), (20, 30), (30, 40), (40, 60)):
        if lo <= seconds < hi:
            return f"{lo}-{hi}s"
    return "60s+"


def dimension_scores() -> dict[str, dict[str, dict]]:
    """Grouped average video scores: hook category, topic/destination,
    franchise, voice, length bucket, visual style, CTA presence (§19)."""
    s = store()
    videos = [v for v in s.find("videos", {"status": "published"}) if v.get("video_score")]
    groups: dict[str, dict[str, list[float]]] = defaultdict(lambda: defaultdict(list))
    for v in videos:
        score = v["video_score"]
        hook = s.get("hooks", v.get("hook_id", "")) or {}
        script = s.get("scripts", v.get("script_id", "")) or {}
        concept = s.get("concepts", v.get("concept_id", "")) or {}
        groups["hook_category"][hook.get("category", "unknown")].append(score)
        groups["destination"][(v.get("destination") or "unknown").lower()].append(score)
        groups["franchise"][v.get("franchise") or "unknown"].append(score)
        groups["voice"][v.get("voice_id") or "unknown"].append(score)
        groups["length"][_length_bucket(v.get("duration_seconds", 0))].append(score)
        groups["visual_style"][concept.get("visual_style") or "unknown"].append(score)
        groups["cta"]["with_cta" if script.get("cta") else "no_cta"].append(score)
    return {
        dim: {key: {"n": len(vals), "avg_score": round(sum(vals) / len(vals), 1)}
              for key, vals in keys.items()}
        for dim, keys in groups.items()
    }
