"""
video/learning.py — Learning Engine (spec §20).

Daily process: rescore videos, compute dimension lifts vs the channel
average, and translate significant lifts (enough sample, enough lift) into
plain-language recommendations editors accept or reject from the dashboard.
Accepted produce_more recommendations raise the matching topics' priority in
the opportunity engine on the next scoring pass.
"""

from video import scoring, settings
from video.models import Recommendation
from video.store import store

_DIM_LABEL = {
    "hook_category": "hook style", "destination": "destination",
    "franchise": "franchise", "voice": "voice", "length": "length",
    "visual_style": "visual style", "cta": "CTA",
}


def _channel_average() -> float | None:
    s = store()
    scored = [v["video_score"] for v in s.find("videos", {"status": "published"})
              if v.get("video_score")]
    return sum(scored) / len(scored) if scored else None


def find_lifts() -> list[dict]:
    """Segments beating/trailing the channel average by the configured lift,
    with the configured minimum sample."""
    cfg = settings.settings()
    avg = _channel_average()
    if not avg:
        return []
    findings = []
    for dim, keys in scoring.dimension_scores().items():
        for key, stats in keys.items():
            if stats["n"] < cfg["learning_min_sample"] or key == "unknown":
                continue
            lift_pct = 100.0 * (stats["avg_score"] - avg) / avg
            if abs(lift_pct) >= cfg["learning_min_lift_pct"]:
                findings.append({
                    "dimension": dim, "key": key, "n": stats["n"],
                    "avg_score": stats["avg_score"], "channel_avg": round(avg, 1),
                    "lift_pct": round(lift_pct, 1),
                })
    findings.sort(key=lambda f: abs(f["lift_pct"]), reverse=True)
    return findings


def run() -> list[dict]:
    """Scheduler entry: rescore, find lifts, upsert recommendations."""
    scoring.rescore_all()
    s = store()
    created = []
    for f in find_lifts():
        direction = "outperforming" if f["lift_pct"] > 0 else "underperforming"
        kind = "produce_more" if f["lift_pct"] > 0 else "stop_producing"
        rec_id = f"rec_lift_{f['dimension']}_{f['key'].replace(' ', '_').replace('/', '_')}"
        label = _DIM_LABEL.get(f["dimension"], f["dimension"])
        finding = (f"{f['key'].title()} {label} is {direction} the channel average by "
                   f"{abs(f['lift_pct']):.0f}% ({f['avg_score']} vs {f['channel_avg']}, "
                   f"n={f['n']})")
        if kind == "produce_more":
            action = f"Produce 2 more Shorts using this {label} this week."
        else:
            action = f"Pause this {label} until a new angle tests better."
        existing = s.get("recommendations", rec_id)
        if existing and existing["status"] in ("accepted", "rejected"):
            continue  # editor already decided; don't nag
        rec = Recommendation(id=rec_id, kind=kind, finding=finding,
                             evidence=f, suggested_action=action)
        created.append(s.put("recommendations", rec.to_record()))
    settings.log("learning", f"{len(created)} recommendations from lift analysis")
    return created


def decide(recommendation_id: str, accept: bool, actor: str) -> dict:
    s = store()
    rec = s.get("recommendations", recommendation_id)
    if not rec:
        raise KeyError(recommendation_id)
    rec["status"] = "accepted" if accept else "rejected"
    rec.setdefault("evidence", {})["decided_by"] = actor
    return s.put("recommendations", rec)
