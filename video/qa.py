"""
video/qa.py — QA Agent (spec §15, enforcing §30 editorial safety and §31 AI
disclosure).

Every video passes through run_qa() before it can appear in the approval
queue. Verdicts: pass | warning | fail. FAIL cannot publish, ever; WARNING
requires human review (which the MANUAL default already forces — the flag is
for ASSISTED/AUTO later). QA history is append-only in qa_results.

Checks are deterministic and explainable — each returns (verdict, detail).
The LLM is deliberately NOT the judge here: the fact-check layer relies on
scripts citing their supporting phrases (script.py demands them), and QA
verifies presence + provenance, flagging what a human must confirm.
"""

import re
from datetime import datetime, timezone

from video import settings
from video.models import QAResult, new_id
from video.store import store

FARE_DISCLAIMER_WORDS = ("fare found at time of publishing", "prices can change",
                         "pricing may change", "availability can change")
FORBIDDEN_PHRASES = (
    "guaranteed", "100% free", "you won't believe", "gone forever",
    "last chance ever", "secret the airlines don't want",
)
MAX_CAPTION_CHARS = 64  # 2 lines × ~32 chars at theme caption size


def _check_generator(video, script, concept, **_) -> tuple[str, str]:
    gens = {script.get("generator"), concept.get("generator")}
    if "offline" in gens:
        return ("warning", "offline fallback generator used — human must review copy")
    return ("pass", f"generators: {sorted(g for g in gens if g)}")


def _check_fact_notes(video, script, source, **_) -> tuple[str, str]:
    notes = script.get("fact_check_notes") or []
    if video.get("franchise") in ("travel_news", "know_before_you_go", "points_miles") and not notes:
        return ("warning", "no fact-check notes on a factual franchise — verify claims manually")
    unsupported = [n for n in notes if isinstance(n, dict) and not n.get("support")]
    if unsupported:
        return ("fail", f"{len(unsupported)} claims without source support: "
                        f"{[n.get('claim', '')[:60] for n in unsupported[:3]]}")
    return ("pass", f"{len(notes)} claims traced to source")


def _check_destination_consistency(video, script, concept, **_) -> tuple[str, str]:
    v, c = (video.get("destination") or "").lower(), (concept.get("destination") or "").lower()
    if v and c and v != c:
        return ("fail", f"destination mismatch: video='{v}' concept='{c}'")
    return ("pass", f"destination '{v or c}' consistent")


def _check_fare_compliance(video, script, source, **_) -> tuple[str, str]:
    if video.get("franchise") != "fare_drop":
        return ("pass", "not fare content")
    text = " ".join([script.get("voiceover", ""), source.get("summary", ""),
                     " ".join(script.get("onscreen_text", []))]).lower()
    if not any(w in text for w in FARE_DISCLAIMER_WORDS):
        return ("fail", "fare video missing 'fare found at time of publishing' disclosure")
    deal_id = source.get("fare_deal_id")
    s = store()
    for deal in s.find("fare_deals", predicate=lambda d: d.get("id") == deal_id) if deal_id else []:
        exp = deal.get("expiration")
        if exp and exp < datetime.now(timezone.utc).isoformat():
            return ("fail", f"fare expired {exp}")
    return ("pass", "fare disclosure present")


def _check_news_provenance(video, script, source, **_) -> tuple[str, str]:
    if video.get("franchise") != "travel_news":
        return ("pass", "not news content")
    if not script.get("citations"):
        return ("fail", "news video without source citation")
    if not source.get("publish_date"):
        return ("fail", "news source without preserved publish date")
    return ("pass", f"source + date preserved ({source.get('publish_date')})")


def _check_misleading(video, script, hook, **_) -> tuple[str, str]:
    text = " ".join([hook.get("text", ""), script.get("voiceover", ""),
                     video.get("title", "")]).lower()
    found = [p for p in FORBIDDEN_PHRASES if p in text]
    if found:
        return ("fail", f"forbidden claim language: {found}")
    return ("pass", "no misleading-claim patterns")


def _check_captions(video, script, **_) -> tuple[str, str]:
    caps = script.get("captions") or []
    if not caps:
        return ("warning", "no captions")
    issues = []
    prev_end = 0.0
    for c in caps:
        if len(c.get("text", "")) > MAX_CAPTION_CHARS:
            issues.append(f"overflow: '{c['text'][:30]}…'")
        if c.get("start", 0) < prev_end - 0.01:
            issues.append(f"overlap at {c.get('start')}")
        prev_end = c.get("end", prev_end)
    dur = video.get("duration_seconds") or 0
    if dur and prev_end > dur + 2.5:
        issues.append(f"captions run {prev_end:.1f}s past {dur:.1f}s video")
    if issues:
        return ("warning", "; ".join(issues[:4]))
    return ("pass", f"{len(caps)} caption beats clean")


def _check_asset_rights(video, **_) -> tuple[str, str]:
    s = store()
    problems = []
    for aid in video.get("asset_ids") or []:
        if not aid:
            continue
        a = s.get("assets", aid)
        if not a:
            problems.append(f"{aid}: missing record")
        elif not a.get("rights_verified"):
            problems.append(f"{aid}: rights not verified")
        else:
            end = a.get("license_end")
            if end and end < datetime.now(timezone.utc).date().isoformat():
                problems.append(f"{aid}: license expired {end}")
    if problems:
        return ("fail", "; ".join(problems[:5]))
    return ("pass", "all matched assets rights-verified")


def _check_placeholders(video, **_) -> tuple[str, str]:
    s = store()
    jobs = s.find("render_jobs", {"video_id": video["id"]})
    done = [j for j in jobs if j.get("status") == "done"]
    if not done:
        return ("warning", "no completed render job")
    n = done[-1].get("placeholders_used", 0)
    if n:
        return ("fail", f"{n} placeholder segments in render — not publishable")
    return ("pass", "no placeholder footage")


def _check_duplicate(video, **_) -> tuple[str, str]:
    s = store()
    title = (video.get("title") or "").lower().split()
    for other in s.find("videos", predicate=lambda v: v.get("status") in
                        ("published", "scheduled") and v["id"] != video["id"]):
        ot = (other.get("title") or "").lower().split()
        if title and ot:
            j = len(set(title) & set(ot)) / len(set(title) | set(ot))
            if j >= 0.7:
                return ("fail", f"near-duplicate of published '{other.get('title')}'")
    return ("pass", "no duplicate title")


def _check_links(video, script, source, **_) -> tuple[str, str]:
    urls = [video.get("utm_url", "")] + (script.get("citations") or [])
    bad = [u for u in urls if u and not re.match(r"^https?://[^\s]+\.[a-z]{2,}", u)]
    if bad:
        return ("warning", f"malformed links: {bad[:3]}")
    return ("pass", "links well-formed")


def _check_disclosure(video, **_) -> tuple[str, str]:
    synthetic = video.get("ai_voice") or video.get("ai_visuals") or video.get("synthetic_scenes")
    if synthetic and not video.get("disclosure_required"):
        return ("fail", "synthetic content without disclosure flag (§31)")
    return ("pass", "disclosure status consistent")


CHECKS = [
    ("generator", _check_generator),
    ("fact_accuracy", _check_fact_notes),
    ("destination_accuracy", _check_destination_consistency),
    ("fare_compliance", _check_fare_compliance),
    ("news_provenance", _check_news_provenance),
    ("misleading_claims", _check_misleading),
    ("subtitle_quality", _check_captions),
    ("asset_rights", _check_asset_rights),
    ("visual_completeness", _check_placeholders),
    ("duplicate_content", _check_duplicate),
    ("links", _check_links),
    ("ai_disclosure", _check_disclosure),
]


def run_qa(video_id: str) -> dict:
    s = store()
    video = s.get("videos", video_id)
    if not video:
        raise KeyError(video_id)
    ctx = {
        "video": video,
        "script": s.get("scripts", video.get("script_id", "")) or {},
        "concept": s.get("concepts", video.get("concept_id", "")) or {},
        "hook": s.get("hooks", video.get("hook_id", "")) or {},
        "source": s.get("content_sources", video.get("source_id", "")) or {},
    }
    results = []
    for name, fn in CHECKS:
        try:
            verdict, detail = fn(**ctx)
        except Exception as exc:  # a broken check must not block QA itself
            verdict, detail = "warning", f"check crashed: {exc}"
        results.append({"name": name, "verdict": verdict, "detail": detail})

    verdicts = {r["verdict"] for r in results}
    overall = "fail" if "fail" in verdicts else ("warning" if "warning" in verdicts else "pass")
    qa = QAResult(id=new_id("qa"), video_id=video_id, verdict=overall, checks=results)
    rec = s.put("qa_results", qa.to_record())

    video["qa_result"] = overall
    if overall == "fail":
        video["status"] = "failed"
    s.put("videos", video)
    settings.log("qa", f"{video_id}: {overall.upper()} "
                       f"({sum(1 for r in results if r['verdict'] != 'pass')} flags)")
    return rec
