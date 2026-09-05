"""
video/pipeline.py — orchestration: the §36 MVP loop, the daily production
job, and the approval actions the dashboard calls.

Every stage persists before the next runs, so failures resume instead of
regenerate, and the dashboard can re-run any stage (change hook / voice /
footage) without touching the others.
"""

from datetime import datetime, timedelta, timezone
from pathlib import Path

from video import (assets, attribution, concepts, fatigue, hooks, ingestion,
                   qa, render, script as script_mod, settings, voice)
from video.models import Video, new_id
from video.store import store


def _make_video(concept: dict, script_rec: dict, hook: dict) -> dict:
    s = store()
    source = s.get("content_sources", concept.get("source_id", "")) or {}
    voice_id = voice.voice_for_franchise(concept.get("franchise", ""))

    video = Video(
        id=new_id("vid"),
        concept_id=concept["id"], script_id=script_rec["id"], hook_id=hook["id"],
        source_id=concept.get("source_id", ""),
        franchise=concept.get("franchise", ""),
        destination=concept.get("destination", ""),
        title=concept.get("working_title", "")[:100],
        description=(source.get("summary", "")[:300] + "\n\n"
                     + " ".join(f"#{k.replace(' ', '')}" for k in (source.get("keywords") or [])[:5])).strip(),
        tags=[t for t in [concept.get("destination", ""), "travel",
                          concept.get("franchise", "").replace("_", " ")] if t],
        voice_id=voice_id,
        predicted_score=concept.get("predicted_performance_score", 0.0),
        status="draft",
    )
    video.utm_url = attribution.build_utm_url(video.id)
    return s.put("videos", video.to_record())


def produce_from_concept(concept_id: str, hook_id: str | None = None) -> dict:
    """concept → hooks → script → assets → VO → render → QA → review queue.
    Returns the video record."""
    s = store()
    concept = s.get("concepts", concept_id)
    if not concept:
        raise KeyError(concept_id)

    generated = s.find("hooks", {"concept_id": concept_id}) or hooks.generate(concept_id)
    hook = hooks.select(concept_id, hook_id) or (generated[0] if generated else None)
    if not hook:
        raise RuntimeError(f"no hooks for concept {concept_id}")

    script_rec = script_mod.generate(concept_id, hook["id"])
    video = _make_video(concept, script_rec, hook)

    matches = assets.match_script(script_rec, concept.get("destination", ""))
    video["asset_ids"] = [m["asset_id"] for m in matches]

    vo_path, is_ai = voice.synthesize(
        script_rec.get("voiceover", ""), video["voice_id"],
        settings.DATA_DIR / "vo" / f"{video['id']}.mp3")
    video["vo_path"] = vo_path or ""
    video["ai_voice"] = is_ai
    video["ai_visuals"] = any(
        (s.get("assets", a) or {}).get("ai_generated") for a in video["asset_ids"] if a)
    video["disclosure_required"] = bool(video["ai_voice"] or video["ai_visuals"])
    s.put("videos", video)

    render.render(video["id"])
    qa.run_qa(video["id"])

    video = s.get("videos", video["id"])
    if video["status"] not in ("failed",):
        video["status"] = "needs_review"
        s.put("videos", video)

    concept["status"] = "produced"
    s.put("concepts", concept)
    settings.log("pipeline", f"video {video['id']} '{video['title']}' → {video['status']} "
                             f"(qa={video.get('qa_result')})")
    return video


def run_mvp(article: str) -> dict:
    """The §36 loop for one Destination.com article (local .md path or URL).
    Stops at the approval queue — publishing is a human action."""
    if article.startswith("http"):
        src = ingestion.ingest_url(article)
    else:
        src = ingestion.ingest_markdown(Path(article))
    s = store()
    s.put("content_sources", src.to_record())

    from video import opportunity
    opp = opportunity.score_source(src.to_record())
    s.put("opportunities", opp.to_record())
    settings.log("pipeline", f"opportunity {opp.score:.0f} — {src.headline[:60]}")

    concepts.generate(src.id)
    best = concepts.select_best(src.id)
    if not best:
        raise RuntimeError("no viable concepts generated")

    hooks.generate(best["id"], n=3)
    return produce_from_concept(best["id"])


# ---------------------------------------------------------------------------
# Daily production (scheduler)
# ---------------------------------------------------------------------------

def _produced_today() -> int:
    today = datetime.now(timezone.utc).date().isoformat()
    return len(store().find("videos", predicate=lambda v: v.get("created_at", "") >= today))


def run_production() -> int:
    """Produce candidate videos from the top open opportunities, up to the
    daily target, respecting fatigue thresholds and the opportunity floor."""
    cfg = settings.settings()
    budget = min(cfg["daily_short_target"], cfg["max_daily_videos"]) - _produced_today()
    if budget <= 0:
        settings.log("pipeline", "daily production target already met")
        return 0

    s = store()
    open_opps = sorted(s.find("opportunities", {"status": "open"}),
                       key=lambda o: o["score"], reverse=True)
    made = 0
    for opp in open_opps:
        if made >= budget:
            break
        if opp["score"] < cfg["min_opportunity_score"]:
            break  # sorted — nothing below clears the floor either
        src = s.get("content_sources", opp["source_id"]) or {}
        ok, reasons = fatigue.check(destination=src.get("destination", ""),
                                    country=src.get("country", ""))
        if not ok:
            settings.log("pipeline", f"fatigue skip {opp['title'][:40]}: {reasons}")
            continue
        try:
            existing = s.find("concepts", {"source_id": opp["source_id"]})
            best = (concepts.select_best(opp["source_id"])
                    if any(c["status"] == "candidate" for c in existing)
                    else None)
            if not best:
                concepts.generate(opp["source_id"])
                best = concepts.select_best(opp["source_id"])
            if not best:
                continue
            fr_ok, fr_reasons = fatigue.check(franchise=best.get("franchise", ""))
            if not fr_ok:
                settings.log("pipeline", f"fatigue skip franchise: {fr_reasons}")
                continue
            produce_from_concept(best["id"])
            opp["status"] = "produced"
            s.put("opportunities", opp)
            made += 1
        except Exception as exc:
            settings.log("pipeline", f"WARNING: production failed for {opp['id']}: {exc}")
    settings.log("pipeline", f"produced {made} candidate videos")
    return made


# ---------------------------------------------------------------------------
# Approval actions (§16) — called by the dashboard
# ---------------------------------------------------------------------------

def _approval_event(video_id: str, action: str, actor: str, detail: dict | None = None):
    store().put("approval_events", {
        "id": new_id("appr"), "video_id": video_id, "action": action,
        "actor": actor, "detail": detail or {}, "status": "final",
        "created_by": actor,
    })


def approve(video_id: str, actor: str) -> dict:
    s = store()
    video = s.get("videos", video_id)
    if not video:
        raise KeyError(video_id)
    if video.get("qa_result") == "fail":
        raise PermissionError("QA=FAIL cannot be approved (§15)")
    video["status"] = "approved"
    _approval_event(video_id, "approve", actor)
    return s.put("videos", video)


def reject(video_id: str, actor: str, reason: str = "") -> dict:
    s = store()
    video = s.get("videos", video_id)
    if not video:
        raise KeyError(video_id)
    video["status"] = "rejected"
    _approval_event(video_id, "reject", actor, {"reason": reason})
    return s.put("videos", video)


def regenerate(video_id: str, actor: str, change: str = "",
               hook_id: str | None = None, voice_id: str | None = None) -> dict:
    """Change hook / voice / footage → a fresh video from the same concept.
    The old video is archived, not mutated (audit history, §26)."""
    s = store()
    old = s.get("videos", video_id)
    if not old:
        raise KeyError(video_id)
    old["status"] = "archived"
    s.put("videos", old)
    _approval_event(video_id, "regenerate", actor, {"change": change})

    concept = s.get("concepts", old["concept_id"])
    concept["status"] = "selected"
    s.put("concepts", concept)
    if voice_id:
        cfg_key = concept.get("franchise", "")
        settings.settings()["franchise_voices"][cfg_key] = voice_id  # session-scoped override
    return produce_from_concept(old["concept_id"], hook_id=hook_id)


def batch_approve(video_ids: list[str], actor: str) -> list[dict]:
    out = []
    for vid in video_ids:
        try:
            out.append(approve(vid, actor))
        except (KeyError, PermissionError) as exc:
            settings.log("pipeline", f"batch approve skipped {vid}: {exc}")
    return out
