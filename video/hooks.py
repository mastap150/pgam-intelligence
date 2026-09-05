"""
video/hooks.py — Hook Generator (spec §6).

The opening 0-2s line is its own optimization layer: stored separately from
the script, categorized, and rolled up by category performance so the
learning engine can say "price-led hooks outperform curiosity hooks on Fare
Drop" with numbers behind it.
"""

from video import llm, settings
from video.models import Hook, HOOK_CATEGORIES, new_id
from video.store import store

_SYSTEM = f"""You write opening hooks for Destination.com travel Shorts.
A hook is the first spoken/displayed line, 0-2 seconds, ≤ 12 words.
It must be honest — the video must actually deliver what the hook promises.

Hook categories: {', '.join(HOOK_CATEGORIES)}.

{llm.STYLE_RULES}"""

_USER_TMPL = """Video concept:
TITLE: {title}
ANGLE: {angle}
FRANCHISE: {franchise}
DESTINATION: {destination}

Write {n} hooks in {n} different categories suited to this concept.
Return a JSON array of objects: {{"category": ..., "text": ...}}."""

_OFFLINE = [
    ("warning", "Going to {dest}? Don't make this mistake."),
    ("curiosity", "Almost nobody plans {dest} the right way."),
    ("personal", "I wish somebody told me this before {dest}."),
]


def generate(concept_id: str, n: int = 3) -> list[dict]:
    s = store()
    concept = s.get("concepts", concept_id)
    if not concept:
        raise KeyError(concept_id)

    def offline():
        dest = concept.get("destination") or "this trip"
        return [{"category": c, "text": t.format(dest=dest)} for c, t in _OFFLINE[:n]]

    user = _USER_TMPL.format(
        title=concept.get("working_title", ""), angle=concept.get("angle", ""),
        franchise=concept.get("franchise", ""), destination=concept.get("destination", ""),
        n=n)
    raw, generator = llm.complete_json(_SYSTEM, user, offline)

    records = []
    for item in raw if isinstance(raw, list) else []:
        cat = str(item.get("category", "curiosity")).lower()
        if cat not in HOOK_CATEGORIES:
            cat = "curiosity"
        text = str(item.get("text", "")).strip()
        if not text:
            continue
        h = Hook(id=new_id("hook"), concept_id=concept_id, category=cat,
                 text=text[:200], generator=generator)
        records.append(s.put("hooks", h.to_record()))
    settings.log("hooks", f"{len(records)} hooks for {concept_id} via {generator}")
    return records


def select(concept_id: str, hook_id: str | None = None) -> dict | None:
    """Choose a hook for production. Default: the category with the best
    historical performance for this franchise; falls back to first candidate."""
    s = store()
    candidates = s.find("hooks", {"concept_id": concept_id, "status": "candidate"})
    if not candidates:
        return None
    if hook_id:
        chosen = next((h for h in candidates if h["id"] == hook_id), None)
    else:
        concept = s.get("concepts", concept_id) or {}
        perf = category_performance(franchise=concept.get("franchise"))
        chosen = max(candidates,
                     key=lambda h: perf.get(h["category"], {}).get("avg_score", 50.0))
    if not chosen:
        return None
    chosen["status"] = "selected"
    return s.put("hooks", chosen)


def category_performance(franchise: str | None = None) -> dict[str, dict]:
    """Per-category rollup over published, scored videos (§6 'over time,
    calculate performance by hook type')."""
    s = store()
    videos = [v for v in s.find("videos", {"status": "published"}) if v.get("video_score")]
    if franchise:
        videos = [v for v in videos if v.get("franchise") == franchise]
    out: dict[str, dict] = {}
    for v in videos:
        hook = s.get("hooks", v.get("hook_id", "")) or {}
        cat = hook.get("category")
        if not cat:
            continue
        bucket = out.setdefault(cat, {"n": 0, "total": 0.0})
        bucket["n"] += 1
        bucket["total"] += v["video_score"]
    for cat, b in out.items():
        b["avg_score"] = round(b["total"] / b["n"], 1)
    return out
