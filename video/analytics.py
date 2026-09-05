"""
video/analytics.py — performance snapshot ingestion (spec §18).

Cadence per published video: 1h, 6h, 24h, 72h, 7d, 30d after publish. The
hourly scheduler job asks which snapshots are due, pulls metrics from
YouTube, folds in website attribution, stores the snapshot (deduped by
video+label), and refreshes the video score. Also sweeps fresh comments into
comment intelligence on the 6h/24h marks.
"""

from datetime import datetime, timedelta, timezone

from video import attribution, comments as comments_mod, scoring, settings
from video.models import PerformanceSnapshot, new_id
from video.store import store

SNAPSHOT_SCHEDULE = [
    ("1h", timedelta(hours=1)), ("6h", timedelta(hours=6)),
    ("24h", timedelta(hours=24)), ("72h", timedelta(hours=72)),
    ("7d", timedelta(days=7)), ("30d", timedelta(days=30)),
]
COMMENT_SWEEP_LABELS = {"6h", "24h", "72h", "7d"}


def due_snapshots(now: datetime | None = None) -> list[tuple[dict, str, str]]:
    """[(video, youtube_id, label)] for every snapshot past due and missing."""
    now = now or datetime.now(timezone.utc)
    s = store()
    out = []
    for job in s.find("publishing_jobs", {"platform": "youtube"},
                      predicate=lambda j: j.get("status") == "published"):
        video = s.get("videos", job["video_id"])
        if not video or not job.get("publish_time"):
            continue
        try:
            published = datetime.fromisoformat(job["publish_time"].replace("Z", "+00:00"))
        except ValueError:
            continue
        have = {sn["snapshot_label"] for sn in
                s.find("performance_snapshots", {"video_id": video["id"]})}
        for label, delta in SNAPSHOT_SCHEDULE:
            if label not in have and now >= published + delta:
                out.append((video, job.get("external_id", ""), label))
    return out


def take_snapshot(video: dict, youtube_id: str, label: str,
                  metrics: dict | None = None) -> dict:
    """Store one snapshot. metrics may be injected (tests, backfills);
    otherwise pulled live from YouTube."""
    if metrics is None:
        from video import youtube
        metrics = youtube.fetch_metrics(youtube_id)
    metrics = attribution.merge_into_metrics(video["id"], metrics)
    snap = PerformanceSnapshot(
        id=f"snap_{video['id']}_{label}", video_id=video["id"],
        platform="youtube", snapshot_label=label, metrics=metrics)
    rec = store().put("performance_snapshots", snap.to_record())

    score = scoring.compute_video_score(metrics)
    video["video_score"] = score
    store().put("videos", video)
    settings.log("analytics", f"snapshot {label} for {video['id']}: "
                              f"{metrics.get('views', 0)} views, score {score}")
    return rec


def run() -> int:
    """Scheduler entry (hourly)."""
    n = 0
    for video, youtube_id, label in due_snapshots():
        if not youtube_id:
            continue
        try:
            take_snapshot(video, youtube_id, label)
            if label in COMMENT_SWEEP_LABELS:
                from video import youtube
                comments_mod.ingest(video["id"], youtube.fetch_comments(youtube_id))
            n += 1
        except Exception as exc:
            settings.log("analytics", f"WARNING: snapshot {label} for {video['id']} failed: {exc}")
    if n:
        comments_mod.refresh_viewer_requests()
    settings.log("analytics", f"{n} snapshots taken")
    return n
