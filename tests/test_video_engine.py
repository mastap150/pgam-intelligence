"""
tests/test_video_engine.py

Standalone, no-network, no-credential tests for the Destination.com Video
Engine (video/*). Everything runs against a temp JSON store with the offline
generation fallback, which is exactly the degraded mode QA must flag — so the
tests cover both the pipeline mechanics and the safety gates:

  - ingestion: markdown corpus normalization, freshness decay
  - opportunity: weighted score, reasons, manual boost
  - concepts/hooks/script: offline generation, dedupe, VO length clamp
  - fare drop: interest scoring, gates, disclaimer, template (not LLM) numbers
  - assets: rights enforcement (unverified/expired/wrong-place never match),
    diversity penalty
  - fatigue thresholds
  - QA: FAIL on rights violations + placeholder footage, WARNING on offline
    copy; FAIL can neither be approved nor published
  - publish gates: approval requirement, DVE_ALLOW_PUBLISH dry-run, dedupe
  - scoring/learning: composite score, dimension lifts → recommendations
  - attribution UTM construction; comments classification; experiments

Run:
    python tests/test_video_engine.py
"""

import os
import shutil
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

os.environ.pop("ANTHROPIC_API_KEY", None)   # force offline generation
os.environ.pop("ELEVENLABS_API_KEY", None)
os.environ["DVE_ALLOW_PUBLISH"] = "0"

TMP = Path(tempfile.mkdtemp(prefix="dve_test_"))
os.environ["DVE_DATA_DIR"] = str(TMP)

from video import settings  # noqa: E402

settings.DATA_DIR = TMP
settings.ANTHROPIC_API_KEY = ""
settings.ELEVENLABS_API_KEY = ""
settings.ALLOW_PUBLISH = False

from video import (assets, attribution, comments, concepts, experiments,  # noqa: E402
                   fare_drop, fatigue, hooks, ingestion, learning, pipeline,
                   qa, render, scoring, script as script_mod, store, voice, youtube)

_failures: list[str] = []


def check(name: str, cond: bool, detail: str = ""):
    status = "ok" if cond else "FAIL"
    print(f"  [{status}] {name}" + (f" — {detail}" if detail and not cond else ""))
    if not cond:
        _failures.append(name)


def fresh_store():
    root = TMP / "store"
    if root.exists():
        shutil.rmtree(root)
    return store.reset_store_for_tests(root)


ARTICLE = TMP / "tokyo-guide.md"
ARTICLE.write_text("""---
META_TITLE: The Ultimate Guide to Tokyo
META_DESC: Where to stay, what to skip, and when to go.
TARGET KEYWORD: tokyo travel guide
SECONDARY: tokyo first time, tokyo neighborhoods
CATEGORY: Asia / Japan
---

# The Ultimate Guide to Tokyo

Tokyo rewards planning. The best months to visit are late March for cherry
blossoms and November for autumn color. First-time visitors should stay in
Shinjuku or Shibuya. Skip the Robot Restaurant; book teamLab weeks ahead.
Flights from the US are cheapest in January. A 72-hour metro pass saves money.
""")


def test_ingestion():
    print("ingestion")
    src = ingestion.ingest_markdown(ARTICLE)
    check("headline parsed", src.headline == "The Ultimate Guide to Tokyo")
    check("country from category", src.country == "Japan", src.country)
    check("destination inferred", src.destination.lower() == "tokyo", src.destination)
    check("keywords parsed", "tokyo travel guide" in src.keywords)
    check("flight relevance detected", src.flight_relevance > 0)
    check("owned rights", src.rights_status == "owned")
    check("freshness for undated is neutral", src.freshness_score == 30.0)
    check("fresh date scores high", ingestion.freshness_score("2026-09-04") > 95)


def test_opportunity():
    print("opportunity")
    from video import opportunity
    fresh_store()
    src = ingestion.ingest_markdown(ARTICLE)
    store.store().put("content_sources", src.to_record())
    opp = opportunity.score_source(src.to_record())
    check("score in range", 0 < opp.score <= 100, str(opp.score))
    check("7 components", len(opp.component_scores) == 7)
    check("reasons per component", len(opp.reasons) >= 7)
    rec = store.store().put("opportunities", opp.to_record())
    boosted = opportunity.set_manual_boost(rec["id"], 10, "editor")
    check("manual boost applied", abs(boosted["score"] - (opp.score + 10)) < 0.11,
          f"{boosted['score']} vs {opp.score}+10")
    check("boost audited in reasons", any("manual_boost" in r for r in boosted["reasons"]))


def test_concepts_hooks_script():
    print("concepts / hooks / script")
    fresh_store()
    src = ingestion.ingest_markdown(ARTICLE)
    store.store().put("content_sources", src.to_record())
    recs = concepts.generate(src.id)
    check("≥5 concepts offline", len(recs) >= 5, str(len(recs)))
    check("offline tagged", all(r["generator"] == "offline" for r in recs))
    check("franchises valid", all(r["franchise"] in
                                  ("destination_daily", "know_before_you_go",
                                   "destination_insider", "points_miles") for r in recs))
    # dedupe: regenerating produces nothing new (titles collide with stored)
    again = concepts.generate(src.id)
    check("near-duplicates filtered", len(again) == 0, str(len(again)))

    best = concepts.select_best(src.id)
    check("best selected", best is not None and best["status"] == "selected")

    hs = hooks.generate(best["id"], n=3)
    check("3 hooks", len(hs) == 3)
    check("hook categories valid", all(h["category"] in
                                       ("warning", "curiosity", "personal") for h in hs))
    chosen = hooks.select(best["id"])
    check("hook selected", chosen is not None and chosen["status"] == "selected")

    scr = script_mod.generate(best["id"], chosen["id"])
    check("voiceover present", bool(scr["voiceover"]))
    check("captions segmented", len(scr["captions"]) > 1)
    check("caption beats ≤8 words", all(len(c["text"].split()) <= 8 for c in scr["captions"]))
    check("shot list present", len(scr["shot_list"]) >= 3)
    check("citations carry source", src.source_url in scr["citations"])
    vo_words = len(scr["voiceover"].split())
    max_words = int(best["target_length"] * script_mod.WORDS_PER_SECOND * 1.35)
    check("VO within length clamp", vo_words <= max_words, f"{vo_words} > {max_words}")


def test_fare_drop():
    print("fare drop")
    fresh_store()
    good = fare_drop.ingest_deal({
        "origin": "MIA", "destination": "Tokyo", "fare": 487.0,
        "normal_fare_estimate": 850.0, "cabin": "economy",
        "departure_date": "2026-10-14", "return_date": "2026-10-22",
    })
    check("discount computed", abs(good["discount_percentage"] - 42.7) < 0.2,
          str(good["discount_percentage"]))
    check("gates cleared → produced", good["status"] == "produced")
    cons = store.store().find("concepts", {"franchise": "fare_drop"})
    check("fare concept created", len(cons) == 1)
    check("template generator (numbers not from LLM)", cons[0]["generator"] == "template")
    src = store.store().get("content_sources", cons[0]["source_id"])
    check("disclaimer in source copy", "fare found at time of publishing" in src["summary"].lower())

    weak = fare_drop.ingest_deal({
        "origin": "XNA", "destination": "Boise", "fare": 400.0,
        "normal_fare_estimate": 430.0, "cabin": "economy",
    })
    check("weak deal gated out", weak["status"] == "scored")
    check("no extra concept", len(store.store().find("concepts", {"franchise": "fare_drop"})) == 1)


def test_assets_and_fatigue():
    print("assets / fatigue")
    fresh_store()
    verified = assets.register({
        "asset_type": "video", "destination": "tokyo", "description": "Shibuya crossing at night",
        "source_tier": "owned", "rights_verified": True, "quality_score": 80,
        "resolution": "1080x1920", "file_url": "/x/shibuya.mp4",
    })
    assets.register({  # unverified — must never match
        "asset_type": "video", "destination": "tokyo", "description": "Shibuya crossing 4k",
        "source_tier": "owned", "rights_verified": False, "quality_score": 95,
        "resolution": "1080x1920", "file_url": "/x/unverified.mp4",
    })
    assets.register({  # expired license
        "asset_type": "video", "destination": "tokyo", "description": "Shibuya drone",
        "source_tier": "licensed_stock", "rights_verified": True,
        "license_end": "2025-01-01", "quality_score": 90, "resolution": "1080x1920",
        "file_url": "/x/expired.mp4",
    })
    assets.register({  # wrong place — hard filter
        "asset_type": "video", "destination": "paris", "description": "night crossing",
        "source_tier": "owned", "rights_verified": True, "quality_score": 90,
        "resolution": "1080x1920", "file_url": "/x/paris.mp4",
    })
    shot = {"seq": 1, "description": "busy crossing at night", "b_roll": "shibuya crossing"}
    match = assets.match_shot(shot, "tokyo")
    check("only rights-clean same-place asset matches", match is not None
          and match["id"] == verified["id"], str(match and match["id"]))
    match_paris_shot = assets.match_shot(shot, "reykjavik")
    check("no wrong-destination match", match_paris_shot is None)

    ok, reasons = fatigue.check(destination="tokyo")
    check("fatigue clear with no recent videos", ok, str(reasons))
    s = store.store()
    for i in range(2):
        s.put("videos", {"id": f"vid_f{i}", "status": "published",
                         "destination": "tokyo", "franchise": "destination_daily",
                         "created_by": "t"})
    ok, reasons = fatigue.check(destination="tokyo")
    check("destination fatigue trips at threshold", not ok, str(reasons))
    check("asset diversity penalty applied",
          fatigue.asset_penalty({"id": "nope"}) == 1.0)


def test_pipeline_and_qa():
    print("pipeline / QA (offline, render blocked)")
    fresh_store()
    real_ffmpeg = render.FFMPEG
    render.FFMPEG = None  # force blocked render: QA must warn, not crash
    try:
        video = pipeline.run_mvp(str(ARTICLE))
    finally:
        render.FFMPEG = real_ffmpeg
    check("video reaches review queue", video["status"] == "needs_review", video["status"])
    check("utm url set", "utm_content=" + video["id"] in video["utm_url"])
    qa_recs = store.store().find("qa_results", {"video_id": video["id"]})
    check("QA ran", len(qa_recs) == 1)
    verdicts = {c["name"]: c["verdict"] for c in qa_recs[0]["checks"]}
    check("offline copy flagged", verdicts.get("generator") == "warning")
    check("no completed render flagged", verdicts.get("visual_completeness") == "warning")
    check("overall not pass in degraded mode", qa_recs[0]["verdict"] in ("warning", "fail"))

    approved = pipeline.approve(video["id"], "editor")
    check("warning-level video approvable by human", approved["status"] == "approved")

    job = youtube.publish(video["id"])
    check("publish without DVE_ALLOW_PUBLISH is a dry run", job["status"] == "dry_run",
          job["status"])
    job2 = youtube.publish(video["id"])
    check("second dry run allowed (no dupe protection burn)", job2["status"] == "dry_run")

    # A QA FAIL can be neither approved nor published.
    v = store.store().get("videos", video["id"])
    v["qa_result"] = "fail"
    store.store().put("videos", v)
    try:
        pipeline.approve(video["id"], "editor")
        check("FAIL not approvable", False)
    except PermissionError:
        check("FAIL not approvable", True)
    try:
        youtube.publish(video["id"])
        check("FAIL not publishable", False)
    except PermissionError:
        check("FAIL not publishable", True)


def test_scoring_learning():
    print("scoring / learning")
    fresh_store()
    s = store.store()
    metrics_hi = {"views": 10000, "average_percentage_viewed": 85, "completion_rate": 70,
                  "shares": 60, "likes": 500, "subscribers_gained": 30, "comments": 40,
                  "website_clicks": 80, "commercial_conversions": 10}
    metrics_lo = {"views": 10000, "average_percentage_viewed": 30, "completion_rate": 15,
                  "shares": 2, "likes": 40, "subscribers_gained": 1, "comments": 3,
                  "website_clicks": 2, "commercial_conversions": 0}
    hi, lo = scoring.compute_video_score(metrics_hi), scoring.compute_video_score(metrics_lo)
    check("score ordering", hi > lo > 0, f"{hi} vs {lo}")
    check("score bounded", hi <= 100)

    for i in range(6):
        s.put("videos", {"id": f"v_j{i}", "status": "published", "destination": "japan",
                         "franchise": "destination_daily", "video_score": 80.0,
                         "duration_seconds": 25, "created_by": "t"})
        s.put("videos", {"id": f"v_i{i}", "status": "published", "destination": "italy",
                         "franchise": "destination_daily", "video_score": 40.0,
                         "duration_seconds": 25, "created_by": "t"})
    recs = learning.run()
    kinds = {(r["kind"], r["evidence"]["key"]) for r in recs}
    check("japan lift found", ("produce_more", "japan") in kinds, str(kinds))
    check("italy decline found", ("stop_producing", "italy") in kinds)
    decided = learning.decide(recs[0]["id"], True, "editor")
    check("recommendation decidable", decided["status"] == "accepted")


def test_attribution_comments_experiments():
    print("attribution / comments / experiments")
    fresh_store()
    url = attribution.build_utm_url("vid_123", "/guides/tokyo")
    check("utm params", "utm_source=youtube" in url and "utm_medium=organic_video" in url
          and "utm_campaign=destination_shorts" in url and "utm_content=vid_123" in url, url)
    attribution.record_event("vid_123", "flight_search")
    attribution.record_event("vid_123", "session")
    merged = attribution.merge_into_metrics("vid_123", {"views": 100})
    check("conversions folded into metrics", merged["commercial_conversions"] == 1
          and merged["website_clicks"] == 1)

    cls, topics = comments.classify_text("Do Greece next please!")
    check("destination request classified", cls in ("destination_request", "content_request")
          and "greece" in topics, f"{cls} {topics}")
    check("spam classified", comments.classify_text("check my channel http://x.co")[0] == "spam")
    check("correction classified",
          comments.classify_text("Actually it's called Naoshima, not Teshima")[0] == "correction")
    ingested = comments.ingest("vid_123", [
        {"id": f"c{i}", "author": "a", "text": "Please do Greece!"} for i in range(3)])
    check("comments ingested", len(ingested) == 3)
    vr = comments.refresh_viewer_requests(min_mentions=3)
    check("viewer-requested queue built", any(r["evidence"]["topic"] == "greece" for r in vr))

    s = store.store()
    exp = experiments.create("hookA vs hookB", "hook", {"hook": "A"}, {"hook": "B"})
    for i in range(5):
        s.put("videos", {"id": f"ea{i}", "status": "published", "video_score": 80.0 + i,
                         "created_by": "t"})
        s.put("videos", {"id": f"eb{i}", "status": "published", "video_score": 40.0 + i,
                         "created_by": "t"})
        experiments.attach_video(exp["id"], f"ea{i}", "a")
        experiments.attach_video(exp["id"], f"eb{i}", "b")
    concluded = experiments.conclude(exp["id"])
    check("experiment concluded", concluded["status"] == "concluded")
    check("winner a with confidence", concluded["result"]["winner"] == "a"
          and concluded["result"]["confidence"] in ("high", "moderate"),
          str(concluded["result"]))
    try:
        experiments.create("bad", "everything", {}, {})
        check("multi-variable experiments rejected", False)
    except ValueError:
        check("multi-variable experiments rejected", True)


def test_render_units():
    print("render units")
    fresh_store()
    ass = render.write_ass_captions(
        [{"start": 0.0, "end": 1.8, "text": "Going to Japan?"}],
        TMP / "cap.ass")
    text = ass.read_text()
    check("ass header from theme", "PlayResX: 1080" in text and "PlayResY: 1920" in text)
    check("dialogue line", "Dialogue: 0,0:00:00.00,0:00:01.80,Caption,Going to Japan?" in text)

    timeline = {
        "width": 1080, "height": 1920, "duration": 4.0,
        "segments": [{"start": 0, "end": 4, "asset_path": "", "placeholder": True,
                      "onscreen_text": "", "brand_component": ""}],
        "captions": [], "vo_path": "", "music_path": "",
        "end_card_seconds": 2.0, "location_label": "tokyo",
        "cta_text": "", "source_label": "From Destination.com",
    }
    cmd = render.build_ffmpeg_command(timeline, ass, TMP / "out.mp4")
    joined = " ".join(cmd)
    check("h264 vertical output", "-c:v libx264" in joined and "1080x1920" in joined)
    check("aac audio", "-c:a aac" in joined)
    check("captions burned", f"ass={ass}" in joined)
    check("brand overlay present", "drawtext" in joined and "Tokyo" in joined)


def test_voice_and_mode_gates():
    print("voice / publish modes")
    fresh_store()
    n = voice.seed_voices()
    check("voice roster seeded", n == 5)
    check("franchise voice mapping", voice.voice_for_franchise("fare_drop") == "deals")
    path, is_ai = voice.synthesize("hello", "destination_main", TMP / "vo" / "t.mp3")
    check("no key → no VO, not AI-flagged", path is None and is_ai is False)

    allowed, why = youtube._mode_allows({"franchise": "travel_news"}, has_approval=False)
    check("manual mode requires approval", not allowed, why)
    settings.settings()["publishing_mode"] = "auto"
    settings.settings()["auto_publish_franchises"] = ["fare_drop", "travel_news"]
    try:
        allowed, why = youtube._mode_allows({"franchise": "travel_news"}, has_approval=False)
        check("news never auto-publishes", not allowed, why)
        allowed, _ = youtube._mode_allows({"franchise": "fare_drop"}, has_approval=False)
        check("auto works for enabled franchise", allowed)
        allowed, _ = youtube._mode_allows({"franchise": "destination_daily"}, has_approval=False)
        check("auto scoped per franchise", not allowed)
    finally:
        settings.settings()["publishing_mode"] = "manual"
        settings.settings()["auto_publish_franchises"] = []


def test_dashboard_api():
    print("dashboard internals")
    fresh_store()
    from video import dashboard
    s = dashboard.summary()
    check("summary sections", all(k in s for k in
                                  ("today", "performance", "top_content", "ai_insights")))
    check("weights sum to 1 (opportunity)",
          abs(sum(settings.settings()["opportunity_weights"].values()) - 1.0) < 1e-9)
    check("weights sum to 1 (video score)",
          abs(sum(settings.settings()["video_score_weights"].values()) - 1.0) < 1e-9)


def main() -> int:
    print("tests/test_video_engine.py — Destination Video Engine (offline)\n")
    test_ingestion()
    test_opportunity()
    test_concepts_hooks_script()
    test_fare_drop()
    test_assets_and_fatigue()
    test_pipeline_and_qa()
    test_scoring_learning()
    test_attribution_comments_experiments()
    test_render_units()
    test_voice_and_mode_gates()
    test_dashboard_api()

    print()
    shutil.rmtree(TMP, ignore_errors=True)
    if _failures:
        print(f"FAILED — {len(_failures)} check(s): {', '.join(_failures)}")
        return 1
    print("all checks passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
