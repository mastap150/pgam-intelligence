"""
video/longform.py — Long-Form Content Engine (spec §28) + thumbnails (§29).

Phase-3 module, scaffolded now so the trigger logic runs from day one:
when a topic's Shorts show high completion, comments, subscriber conversion
and viewer requests, recommend the long-form expansion. Outline/production
reuses the concept→script machinery with the destination_guides franchise.
"""

from video import settings
from video.models import Recommendation
from video.store import store

TRIGGER = {
    "min_shorts": 3,
    "min_avg_score": 60.0,
    "min_viewer_requests": 2,
}


def find_expansion_candidates() -> list[dict]:
    s = store()
    videos = [v for v in s.find("videos", {"status": "published"})
              if v.get("video_score") and v.get("format") == "short"]
    by_dest: dict[str, list[dict]] = {}
    for v in videos:
        dest = (v.get("destination") or "").lower()
        if dest:
            by_dest.setdefault(dest, []).append(v)

    request_counts = {}
    for rec in store().find("recommendations", {"kind": "viewer_requested"}):
        topic = rec.get("evidence", {}).get("topic", "")
        request_counts[topic] = rec.get("evidence", {}).get("mentions", 0)

    out = []
    for dest, vids in by_dest.items():
        if len(vids) < TRIGGER["min_shorts"]:
            continue
        avg = sum(v["video_score"] for v in vids) / len(vids)
        requests_ = request_counts.get(dest, 0)
        if avg >= TRIGGER["min_avg_score"] or requests_ >= TRIGGER["min_viewer_requests"]:
            out.append({"destination": dest, "shorts": len(vids),
                        "avg_score": round(avg, 1), "viewer_requests": requests_})
    return out


def run() -> list[dict]:
    s = store()
    created = []
    for cand in find_expansion_candidates():
        rec_id = f"rec_longform_{cand['destination'].replace(' ', '_')}"
        if (existing := s.get("recommendations", rec_id)) and existing["status"] != "open":
            continue
        rec = Recommendation(
            id=rec_id, kind="longform_expansion",
            finding=(f"{cand['destination'].title()} Shorts averaging "
                     f"{cand['avg_score']} across {cand['shorts']} videos"
                     + (f", {cand['viewer_requests']} viewer requests" if cand["viewer_requests"] else "")),
            evidence=cand,
            suggested_action=(f"Produce a long-form {cand['destination'].title()} guide "
                              f"(destination_guides franchise) with promo Shorts."),
        )
        created.append(s.put("recommendations", rec.to_record()))
    settings.log("longform", f"{len(created)} long-form expansion recommendations")
    return created


def thumbnail_concepts(title: str, destination: str) -> list[dict]:
    """§29: three thumbnail concepts per long-form video for human selection.
    Rendering comes with the long-form engine; the record shape is stable."""
    return [
        {"concept": "destination hero", "subject": f"{destination} signature vista",
         "text": title.split(":")[0][:24], "style": "high contrast, minimal text"},
        {"concept": "curiosity object", "subject": "single strong detail shot",
         "text": destination.title()[:20], "style": "tight crop, negative space"},
        {"concept": "editorial split", "subject": f"{destination} split with bold label",
         "text": title.split(":")[0][:24], "style": "brand colors, mobile-readable"},
    ]
