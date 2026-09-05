"""
video/script.py — Script Engine (spec §7).

Short-form structure enforced in the prompt and validated on output:
  0-2s hook · 2-7s setup · 7-25s primary content · 25-35s payoff ·
  final seconds: light CTA only where the concept carries one.

A script is a complete production document: voiceover, on-screen text, shot
list (with b-roll suggestions and optional brand components), caption
segmentation, music mood, visual direction, CTA, source citations and
fact-check notes for QA.
"""

from video import llm, settings
from video.models import Script, new_id
from video.store import store

WORDS_PER_SECOND = 2.4  # measured conversational VO pace

_SYSTEM = f"""You write scripts for Destination.com YouTube Shorts.

Structure (seconds are guidance, not hard cuts):
0-2 hook · 2-7 context/setup · 7-25 primary content · 25-35 payoff ·
final 2-3s: light CTA only if one is provided — never force one.

Voiceover pace is ~{WORDS_PER_SECOND} words/second; write to the target length.
Captions segment the voiceover into readable 1-2 line beats (≤ 8 words each).
Every factual claim must come from the source material. List each claim you
used under fact_check_notes with the exact supporting phrase from the source.

{llm.STYLE_RULES}"""

_USER_TMPL = """CONCEPT: {title}
ANGLE: {angle}
FRANCHISE: {franchise}
DESTINATION: {destination}
TARGET LENGTH: {length} seconds
HOOK (use verbatim as the opening): {hook}
CTA (omit if empty): {cta}

SOURCE MATERIAL:
{body}

Return a JSON object:
{{
 "voiceover": "full narration text",
 "onscreen_text": ["short overlay lines in order"],
 "shot_list": [{{"seq": 1, "start": 0.0, "end": 2.0, "description": "...",
                "b_roll": "what footage to find", "onscreen_text": "",
                "brand_component": ""}}],
 "captions": [{{"start": 0.0, "end": 1.8, "text": "..."}}],
 "music_mood": "...",
 "visual_direction": "...",
 "cta": "...",
 "citations": ["source url or section"],
 "fact_check_notes": [{{"claim": "...", "support": "exact phrase from source"}}]
}}
brand_component may be one of: location_label, fare_drop_card, price_card,
route_graphic, map_animation, lower_third, points_card, news_alert — or empty."""


def _offline_script(concept: dict, hook_text: str, src: dict) -> dict:
    dest = concept.get("destination") or "your destination"
    length = float(concept.get("target_length", 30))
    vo = (f"{hook_text} Here's the thing about {dest}: the difference between a "
          f"good trip and a great one is planning around what the guidebooks skip. "
          f"Check timing, book the unmissable things early, and leave one day "
          f"completely unplanned. Full guide on Destination.com.")
    third = length / 3
    return {
        "voiceover": vo,
        "onscreen_text": [hook_text, dest.title(), "Destination.com"],
        "shot_list": [
            {"seq": 1, "start": 0.0, "end": 2.0, "description": f"strong opener of {dest}",
             "b_roll": f"{dest} signature landmark", "onscreen_text": hook_text,
             "brand_component": "location_label"},
            {"seq": 2, "start": 2.0, "end": third * 2,
             "description": f"street-level {dest}", "b_roll": f"{dest} daily life",
             "onscreen_text": "", "brand_component": ""},
            {"seq": 3, "start": third * 2, "end": length,
             "description": "payoff visual", "b_roll": f"{dest} golden hour",
             "onscreen_text": "Destination.com", "brand_component": "cta"},
        ],
        "captions": _segment_captions(vo, length),
        "music_mood": "warm, minimal, mid-tempo",
        "visual_direction": "editorial b-roll, natural light, no meme cuts",
        "cta": concept.get("cta", ""),
        "citations": [src.get("source_url", "")],
        "fact_check_notes": [],
    }


def _segment_captions(voiceover: str, length: float) -> list[dict]:
    """Even-paced caption beats from the VO text; ≤8 words per beat."""
    words = voiceover.split()
    if not words:
        return []
    beats = [words[i:i + 7] for i in range(0, len(words), 7)]
    per = length / len(beats)
    return [{"start": round(i * per, 2), "end": round((i + 1) * per, 2),
             "text": " ".join(b)} for i, b in enumerate(beats)]


def generate(concept_id: str, hook_id: str) -> dict:
    s = store()
    concept = s.get("concepts", concept_id)
    hook = s.get("hooks", hook_id)
    if not concept or not hook:
        raise KeyError(f"concept {concept_id} / hook {hook_id}")
    src = s.get("content_sources", concept.get("source_id", "")) or {}

    user = _USER_TMPL.format(
        title=concept.get("working_title", ""), angle=concept.get("angle", ""),
        franchise=concept.get("franchise", ""), destination=concept.get("destination", ""),
        length=concept.get("target_length", 30), hook=hook.get("text", ""),
        cta=concept.get("cta", ""), body=(src.get("body_text") or "")[:8000])
    raw, generator = llm.complete_json(
        _SYSTEM, user, lambda: _offline_script(concept, hook.get("text", ""), src),
        max_tokens=6000)

    if not isinstance(raw, dict) or not raw.get("voiceover"):
        raw = _offline_script(concept, hook.get("text", ""), src)
        generator = "offline"

    length = float(concept.get("target_length", 30))
    captions = raw.get("captions") or _segment_captions(raw["voiceover"], length)
    # Validate VO length against target: trim runaway scripts rather than ship 90s "Shorts".
    max_words = int(length * WORDS_PER_SECOND * 1.35)
    vo_words = raw["voiceover"].split()
    if len(vo_words) > max_words:
        settings.log("script", f"trimming VO {len(vo_words)}→{max_words} words for target {length:.0f}s")
        raw["voiceover"] = " ".join(vo_words[:max_words])
        captions = _segment_captions(raw["voiceover"], length)

    script = Script(
        id=new_id("scr"),
        concept_id=concept_id,
        hook_id=hook_id,
        voiceover=raw["voiceover"],
        onscreen_text=raw.get("onscreen_text", []),
        shot_list=raw.get("shot_list", []),
        captions=captions,
        music_mood=raw.get("music_mood", ""),
        visual_direction=raw.get("visual_direction", ""),
        cta=raw.get("cta", concept.get("cta", "")),
        citations=[c for c in raw.get("citations", []) if c] or [src.get("source_url", "")],
        fact_check_notes=raw.get("fact_check_notes", []),
        generator=generator,
    )
    rec = s.put("scripts", script.to_record())
    settings.log("script", f"script {script.id} for {concept_id} via {generator}")
    return rec
