"""
video/presenter.py — Human Presenter System (spec §13). Phase-3 scaffold.

One long presenter recording → many Shorts: transcribe, find strong
self-contained segments, propose clips, then hand each to the standard
pipeline (captions, branding, b-roll matching, QA, approval).

Transcription providers plug in behind transcribe(); until one is wired
(Whisper locally or a hosted API), suggest_clips() works from any transcript
with timestamps, so an editor can paste one from YouTube Studio or Descript
today.
"""

from video import settings
from video.models import new_id
from video.store import store

MIN_CLIP_S = 12.0
MAX_CLIP_S = 45.0
STRONG_MARKERS = (
    "mistake", "don't", "never", "always", "the best", "nobody", "secret",
    "here's the thing", "what i wish", "most people", "instead",
)


def transcribe(recording_path: str) -> list[dict]:
    """[{start, end, text}] — provider TBD. Raises until wired."""
    raise NotImplementedError(
        "no transcription provider configured; pass a transcript to suggest_clips()")


def suggest_clips(transcript: list[dict], max_clips: int = 30) -> list[dict]:
    """Score transcript segments for standalone-Short potential. Topic
    boundaries are approximated by gaps and discourse markers; a real topic
    segmenter can replace this without changing the return shape."""
    suggestions = []
    for i, seg in enumerate(transcript):
        text = seg.get("text", "")
        dur = float(seg.get("end", 0)) - float(seg.get("start", 0))
        if not (MIN_CLIP_S <= dur <= MAX_CLIP_S):
            continue
        strength = sum(1 for m in STRONG_MARKERS if m in text.lower())
        if strength == 0:
            continue
        suggestions.append({
            "id": new_id("clip"),
            "start": seg["start"], "end": seg["end"],
            "text": text, "strength": strength,
            "suggested_hook": text.split(".")[0][:80],
        })
    suggestions.sort(key=lambda c: c["strength"], reverse=True)
    top = suggestions[:max_clips]
    settings.log("presenter", f"{len(top)} clip suggestions from {len(transcript)} segments")
    return top
