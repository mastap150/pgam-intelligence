"""
video/llm.py — Claude wrapper for the generation layers.

Follows intelligence/claude_analyst.py (lazy client, ANTHROPIC_API_KEY from
env) with two additions the video engine needs:

  * JSON-mode helper: prompts demand a single JSON object/array, response is
    parsed and validated by the caller.
  * Deterministic offline fallback: when no API key is configured (CI, cloud
    sessions) or the call fails after retries, the caller-supplied fallback
    runs instead. Its output is tagged generator="offline" downstream and QA
    warns on it, so offline output can never silently ship (§30 credibility
    over volume).
"""

import json
import re
from typing import Callable

from video import settings

_client = None


def _get_client():
    global _client
    if _client is None:
        import anthropic
        _client = anthropic.Anthropic()
    return _client


def available() -> bool:
    return bool(settings.ANTHROPIC_API_KEY)


def _extract_json(text: str):
    """Parse the first JSON object/array in a model response."""
    text = text.strip()
    # Strip a markdown fence if present.
    fence = re.match(r"^```(?:json)?\s*(.*?)\s*```$", text, re.DOTALL)
    if fence:
        text = fence.group(1)
    start = min((i for i in (text.find("{"), text.find("[")) if i >= 0), default=-1)
    if start < 0:
        raise ValueError("no JSON found in model response")
    return json.loads(text[start:])


def complete_json(system: str, user: str, fallback: Callable[[], object],
                  max_tokens: int = 4000) -> tuple[object, str]:
    """Return (parsed_json, generator) where generator is 'llm' or 'offline'."""
    if not available():
        return fallback(), "offline"

    def call():
        resp = _get_client().messages.create(
            model=settings.MODEL,
            max_tokens=max_tokens,
            system=system + "\nRespond with a single JSON value and nothing else.",
            messages=[{"role": "user", "content": user}],
        )
        text = next((b.text for b in resp.content if b.type == "text"), "")
        return _extract_json(text)

    try:
        return settings.retry(call), "llm"
    except Exception as exc:
        settings.log("llm", f"WARNING: generation failed, using offline fallback: {exc}")
        return fallback(), "offline"


# Shared style contract (§7) injected into every generation prompt.
STYLE_RULES = """Writing style, non-negotiable:
- concise, modern, conversational, editorial, useful, premium
- not promotional; no filler; no unnecessary adjectives
- no generic AI phrasing ("nestled", "hidden gem", "breathtaking", "must-see")
- no clickbait that the content does not deliver on
- every factual claim must trace to the source material provided; if the
  source does not support a claim, do not make it
- US audience, US spelling."""
