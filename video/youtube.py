"""
video/youtube.py — YouTube integration (spec §17) + publish gates (§32).

Uses google-api-python-client (already a repo dependency) with an OAuth
refresh token (YT_CLIENT_ID / YT_CLIENT_SECRET / YT_REFRESH_TOKEN — scopes:
youtube.upload, youtube.readonly, yt-analytics.readonly).

Publishing is gated three times, in order:
  1. video must be status=approved with qa_result != fail (workflow gate)
  2. publishing mode (§32): MANUAL/ASSISTED require an approval event;
     AUTO only for franchises explicitly listed, never travel_news
  3. DVE_ALLOW_PUBLISH=1 at the environment level — without it the job is
     recorded as a dry run with the exact payload it would have sent

Duplicate protection: one live publishing job per (video, platform).
"""

from datetime import datetime, timezone

from video import attribution, settings
from video.models import PublishingJob, new_id
from video.store import store

_service = None


def _yt():
    global _service
    if _service is None:
        from google.oauth2.credentials import Credentials
        from googleapiclient.discovery import build
        creds = Credentials(
            token=None,
            refresh_token=settings.YT_REFRESH_TOKEN,
            client_id=settings.YT_CLIENT_ID,
            client_secret=settings.YT_CLIENT_SECRET,
            token_uri="https://oauth2.googleapis.com/token",
        )
        _service = build("youtube", "v3", credentials=creds, cache_discovery=False)
    return _service


def _credentials_present() -> bool:
    return all([settings.YT_CLIENT_ID, settings.YT_CLIENT_SECRET, settings.YT_REFRESH_TOKEN])


def _mode_allows(video: dict, has_approval: bool) -> tuple[bool, str]:
    cfg = settings.settings()
    mode = cfg.get("publishing_mode", "manual")
    franchise = video.get("franchise", "")
    if mode in ("manual", "assisted"):
        return (has_approval, f"mode={mode}: approval event "
                              f"{'present' if has_approval else 'REQUIRED and missing'}")
    if mode == "auto":
        if franchise == "travel_news":
            return (has_approval, "travel_news always requires human approval")
        if franchise in cfg.get("auto_publish_franchises", []):
            return (True, f"auto-publish enabled for {franchise}")
        return (has_approval, f"{franchise} not in auto_publish_franchises")
    return (False, f"unknown publishing mode {mode!r}")


def publish(video_id: str, visibility: str = "public",
            playlist: str = "", schedule_at: str = "") -> dict:
    """Create/execute the publishing job for an approved video."""
    s = store()
    video = s.get("videos", video_id)
    if not video:
        raise KeyError(video_id)

    # Gate 1: workflow state.
    if video.get("qa_result") == "fail":
        raise PermissionError("QA=FAIL cannot publish (§15)")
    if video.get("status") not in ("approved", "scheduled"):
        raise PermissionError(f"video status {video['status']!r} is not approved")

    # Duplicate protection (§17).
    existing = [j for j in s.find("publishing_jobs", {"video_id": video_id, "platform": "youtube"})
                if j.get("status") in ("queued", "published")]
    if any(j["status"] == "published" for j in existing):
        settings.log("youtube", f"{video_id} already published — skipping")
        return next(j for j in existing if j["status"] == "published")

    # Gate 2: publishing mode.
    approvals = s.find("approval_events", {"video_id": video_id, "action": "approve"})
    allowed, why = _mode_allows(video, bool(approvals))
    if not allowed:
        raise PermissionError(f"publish blocked: {why}")

    description = video.get("description", "")
    utm = video.get("utm_url") or attribution.build_utm_url(video_id)
    if utm not in description:
        description = f"{description}\n\n{utm}".strip()

    job = PublishingJob(
        id=new_id("pub"), video_id=video_id, platform="youtube",
        title=video.get("title", "")[:100],
        description=description[:4900],
        tags=video.get("tags", [])[:30],
        visibility=visibility, playlist=playlist,
        publish_time=schedule_at,
    )

    # Gate 3: environment.
    if not settings.ALLOW_PUBLISH:
        job.status = "dry_run"
        job.upload_status = "dry_run (DVE_ALLOW_PUBLISH unset)"
        settings.log("youtube", f"DRY RUN publish {video_id}: '{job.title}' "
                                f"visibility={visibility} ({why})")
        return s.put("publishing_jobs", job.to_record())

    if not _credentials_present():
        job.status = "failed"
        job.upload_status = "missing YT_CLIENT_ID/YT_CLIENT_SECRET/YT_REFRESH_TOKEN"
        return s.put("publishing_jobs", job.to_record())

    try:
        from googleapiclient.http import MediaFileUpload
        body = {
            "snippet": {
                "title": job.title, "description": job.description,
                "tags": job.tags, "categoryId": "19",  # Travel & Events
            },
            "status": {
                "privacyStatus": "private" if schedule_at else visibility,
                "selfDeclaredMadeForKids": False,
                **({"publishAt": schedule_at} if schedule_at else {}),
                **({"containsSyntheticMedia": True}
                   if video.get("disclosure_required") else {}),
            },
        }
        media = MediaFileUpload(video["file_path"], chunksize=-1, resumable=True)
        request = _yt().videos().insert(part="snippet,status", body=body, media_body=media)
        response = None
        while response is None:
            _, response = request.next_chunk()
        job.external_id = response["id"]
        job.status = "published"
        job.upload_status = "uploaded"
        job.publish_time = schedule_at or datetime.now(timezone.utc).isoformat()
        video["status"] = "scheduled" if schedule_at else "published"
        s.put("videos", video)
        if playlist:
            _yt().playlistItems().insert(part="snippet", body={
                "snippet": {"playlistId": playlist,
                            "resourceId": {"kind": "youtube#video",
                                           "videoId": job.external_id}}}).execute()
        settings.log("youtube", f"published {video_id} → yt:{job.external_id}")
    except Exception as exc:
        job.status = "failed"
        job.upload_status = str(exc)[:1000]
        settings.log("youtube", f"FAILED publish {video_id}: {exc}")
    return s.put("publishing_jobs", job.to_record())


def fetch_metrics(youtube_video_id: str) -> dict:
    """Statistics + (where scoped) analytics for one video (§18 payload)."""
    resp = _yt().videos().list(part="statistics,contentDetails",
                               id=youtube_video_id).execute()
    items = resp.get("items", [])
    if not items:
        return {}
    stats = items[0].get("statistics", {})
    return {
        "views": int(stats.get("viewCount", 0)),
        "likes": int(stats.get("likeCount", 0)),
        "comments": int(stats.get("commentCount", 0)),
        # retention/completion/shares/subs need the Analytics API; the ETL
        # fills them when yt-analytics scope is present.
    }


def fetch_comments(youtube_video_id: str, max_results: int = 100) -> list[dict]:
    resp = _yt().commentThreads().list(
        part="snippet", videoId=youtube_video_id,
        maxResults=min(100, max_results), textFormat="plainText").execute()
    out = []
    for item in resp.get("items", []):
        top = item["snippet"]["topLevelComment"]["snippet"]
        out.append({"id": item["id"], "author": top.get("authorDisplayName", ""),
                    "text": top.get("textDisplay", "")})
    return out
