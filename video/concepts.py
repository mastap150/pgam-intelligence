"""
video/concepts.py — Content Concept Generator (spec §5).

Not summarization: for each source we generate multiple distinct video
*angles* mapped onto the content franchises (§2), score them, and filter
near-duplicates. The LLM does the creative work; the offline fallback
produces serviceable franchise-templated concepts so the pipeline (and CI)
runs without credentials — tagged generator="offline" and QA-warned.
"""

from video import llm, settings
from video.models import Concept, FRANCHISES, new_id
from video.store import store

MIN_CONCEPTS = 5

_SYSTEM = f"""You are the senior video producer for Destination.com, a premium
travel editorial brand. You turn one editorial source into distinct
short-video concepts for YouTube Shorts.

Franchises (pick the best fit per concept):
{chr(10).join(f'- {name}: {lo}-{hi}s, objective: {obj}' for name, (lo, hi, obj) in FRANCHISES.items() if name != 'destination_guides')}

{llm.STYLE_RULES}"""

_USER_TMPL = """Source article:
HEADLINE: {headline}
SUMMARY: {summary}
BODY (truncated): {body}

Generate {n} genuinely different video concepts — different angles, not
rephrasings. Each must be supportable from the source text alone.

Return a JSON array of objects with keys:
franchise (one of the franchise ids), working_title, hook (one opening line),
angle (2-3 sentences on the treatment), destination, target_length (seconds,
within the franchise range), visual_style, commercial_goal, cta (empty string
if a CTA would feel forced), confidence_score (0-100: how strongly the source
supports it), editorial_score (0-100: premium/editorial fit),
originality_score (0-100: distance from generic travel content)."""

ANGLE_TEMPLATES = [
    ("know_before_you_go", "3 mistakes first-time visitors make in {dest}",
     "warning-led utility drawn from the source's practical advice"),
    ("destination_daily", "{dest} in 30 seconds",
     "fast visual tour of the strongest moments in the source"),
    ("know_before_you_go", "The best time to visit {dest}",
     "timing and seasonality guidance from the source"),
    ("destination_insider", "The {dest} spot most tourists miss",
     "one under-visited highlight from the source, treated editorially"),
    ("destination_daily", "What I wish I knew before visiting {dest}",
     "personal-recommendation framing of the source's key advice"),
    ("points_miles", "How to get to {dest} on points",
     "redemption framing — only when the source covers fares/points"),
]


def _offline_concepts(src: dict, n: int) -> list[dict]:
    dest = src.get("destination") or src.get("country") or "this destination"
    out = []
    for franchise, title_tmpl, angle in ANGLE_TEMPLATES[:n]:
        lo, hi, obj = FRANCHISES[franchise]
        out.append({
            "franchise": franchise,
            "working_title": title_tmpl.format(dest=dest),
            "hook": f"Before you plan {dest}, watch this.",
            "angle": angle,
            "destination": dest,
            "target_length": min(hi, max(lo, 30)),
            "visual_style": "editorial b-roll",
            "commercial_goal": obj,
            "cta": "",
            "confidence_score": 40.0,
            "editorial_score": 50.0,
            "originality_score": 30.0,
        })
    return out


def _similarity(a: str, b: str) -> float:
    ta = set(a.lower().split())
    tb = set(b.lower().split())
    if not ta or not tb:
        return 0.0
    return len(ta & tb) / len(ta | tb)


def dedupe(concepts: list[Concept], threshold: float = 0.6) -> list[Concept]:
    """Drop near-duplicate concepts within the batch AND against concepts
    already produced for the channel (§5 + §22)."""
    s = store()
    prior_titles = [c.get("working_title", "")
                    for c in s.find("concepts", predicate=lambda c: c.get("status") != "rejected")]
    kept: list[Concept] = []
    for c in concepts:
        against = prior_titles + [k.working_title for k in kept]
        if any(_similarity(c.working_title, t) >= threshold for t in against):
            continue
        kept.append(c)
    return kept


def generate(source_id: str, n: int = MIN_CONCEPTS) -> list[dict]:
    s = store()
    src = s.get("content_sources", source_id)
    if not src:
        raise KeyError(source_id)

    user = _USER_TMPL.format(
        headline=src.get("headline", ""),
        summary=src.get("summary", ""),
        body=(src.get("body_text") or "")[:6000],
        n=max(n, MIN_CONCEPTS),
    )
    raw, generator = llm.complete_json(_SYSTEM, user, lambda: _offline_concepts(src, n))

    concepts = []
    for item in raw if isinstance(raw, list) else []:
        franchise = item.get("franchise", "destination_daily")
        if franchise not in FRANCHISES:
            franchise = "destination_daily"
        lo, hi, _ = FRANCHISES[franchise]
        c = Concept(
            id=new_id("con"),
            source_id=source_id,
            franchise=franchise,
            working_title=str(item.get("working_title", ""))[:200],
            hook=str(item.get("hook", ""))[:300],
            angle=str(item.get("angle", ""))[:1000],
            destination=item.get("destination") or src.get("destination", ""),
            target_length=int(min(hi, max(lo, item.get("target_length", 30)))),
            visual_style=str(item.get("visual_style", "")),
            commercial_goal=str(item.get("commercial_goal", "")),
            cta=str(item.get("cta", "")),
            confidence_score=float(item.get("confidence_score", 0)),
            editorial_score=float(item.get("editorial_score", 0)),
            originality_score=float(item.get("originality_score", 0)),
            generator=generator,
        )
        c.predicted_performance_score = round(
            0.4 * c.confidence_score + 0.3 * c.editorial_score + 0.3 * c.originality_score, 1)
        if c.working_title:
            concepts.append(c)

    concepts = dedupe(concepts)
    records = [s.put("concepts", c.to_record()) for c in concepts]
    settings.log("concepts", f"{len(records)} concepts for {source_id} via {generator}")
    return records


def select_best(source_id: str) -> dict | None:
    """Pick the highest predicted-performance candidate for a source and mark
    it selected."""
    s = store()
    candidates = s.find("concepts", {"source_id": source_id, "status": "candidate"})
    if not candidates:
        return None
    best = max(candidates, key=lambda c: c.get("predicted_performance_score", 0))
    best["status"] = "selected"
    return s.put("concepts", best)
