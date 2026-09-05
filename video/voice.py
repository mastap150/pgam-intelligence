"""
video/voice.py — Narration (spec §12).

A small roster of consistent Destination voices (consistency = brand
recognition; no per-video voice roulette). Providers:

  elevenlabs — licensed synthetic narration via the HTTP API
               (ELEVENLABS_API_KEY). ai_voice=True → disclosure tracking.
  human      — an editor drops a recorded file at the expected path.
  silent     — no VO track; also the test/preview provider.

synthesize() returns (audio_path | None, is_ai_voice).
"""

from pathlib import Path

import requests

from video import settings
from video.models import Voice
from video.store import store

DEFAULT_VOICES = [
    Voice(id="destination_main", name="Destination Main", provider="elevenlabs",
          gender_style="neutral warm", accent="US", tone="editorial, calm",
          best_content_types=["destination_daily", "know_before_you_go", "destination_guides"]),
    Voice(id="luxury", name="Destination Luxury", provider="elevenlabs",
          gender_style="low, unhurried", accent="US", tone="premium",
          best_content_types=["destination_insider"]),
    Voice(id="deals", name="Destination Deals", provider="elevenlabs",
          gender_style="bright, quick", accent="US", tone="energetic but not shouty",
          best_content_types=["fare_drop"]),
    Voice(id="news", name="Destination News", provider="elevenlabs",
          gender_style="even, direct", accent="US", tone="newsroom",
          best_content_types=["travel_news"]),
    Voice(id="points", name="Destination Points", provider="elevenlabs",
          gender_style="conversational expert", accent="US", tone="practical",
          best_content_types=["points_miles"]),
]


def seed_voices() -> int:
    """Idempotently install the default voice roster."""
    s = store()
    n = 0
    for v in DEFAULT_VOICES:
        if not s.get("voices", v.id):
            s.put("voices", v.to_record())
            n += 1
    return n


def voice_for_franchise(franchise: str) -> str:
    cfg = settings.settings()
    return cfg["franchise_voices"].get(franchise, cfg["default_voice"])


def synthesize(text: str, voice_id: str, out_path: str | Path) -> tuple[str | None, bool]:
    """Render VO audio. Returns (path or None, ai_voice)."""
    s = store()
    voice = s.get("voices", voice_id)
    if not voice:
        seed_voices()
        voice = s.get("voices", voice_id) or {"provider": "silent"}
    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    provider = voice.get("provider", "silent")
    if provider == "elevenlabs" and settings.ELEVENLABS_API_KEY:
        return _elevenlabs(text, voice, out_path), True
    if provider == "human":
        return (str(out_path) if out_path.exists() else None), False
    if provider == "elevenlabs":
        settings.log("voice", "ELEVENLABS_API_KEY unset — proceeding without VO track")
    return None, False


def _elevenlabs(text: str, voice: dict, out_path: Path) -> str | None:
    pv_id = voice.get("provider_voice_id") or "21m00Tcm4TlvDq8ikWAM"

    def call():
        resp = requests.post(
            f"https://api.elevenlabs.io/v1/text-to-speech/{pv_id}",
            headers={"xi-api-key": settings.ELEVENLABS_API_KEY},
            json={"text": text, "model_id": "eleven_multilingual_v2",
                  "voice_settings": {"stability": 0.55, "similarity_boost": 0.75}},
            timeout=120,
        )
        resp.raise_for_status()
        return resp.content

    try:
        audio = settings.retry(call)
    except Exception as exc:
        settings.log("voice", f"WARNING: ElevenLabs failed: {exc}")
        return None
    out_path.write_bytes(audio)
    return str(out_path)
