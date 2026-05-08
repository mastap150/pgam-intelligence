"""
agents/alerts/weekly_digest.py

Posts the auto-generated weekly business digest to Slack on Mondays.

Replaces the manual "what happened this week?" Slack write-up that
typically falls on whoever's around Monday morning. Pulls
/api/reporting/partner-revenue/weekly-digest from app.pgammedia.com,
which composes the summary deterministically from existing data
(partner-revenue + reconciliation + recommendations + floor recs +
ETL health). No LLM in the loop.

Cadence: scheduler runs us hourly; we self-gate to Mondays 13:00–15:00
UTC (= 9:00–11:00 ET so the team has it before standup) and dedupe
once-per-week. Mid-week ad-hoc invocations skip silently.
"""

import os
import sys
import time
import urllib.request
from datetime import datetime, timezone

from core.slack import send_blocks, already_sent_today, mark_sent

DASHBOARD_BASE = os.environ.get("PGAM_DASHBOARD_BASE", "https://app.pgammedia.com")
DASHBOARD_SERVICE_TOKEN = os.environ.get("PGAM_DASHBOARD_SERVICE_TOKEN")


def _api_get(path: str, timeout: int = 60) -> dict | None:
    if not DASHBOARD_SERVICE_TOKEN:
        return None
    url = f"{DASHBOARD_BASE}{path}"
    req = urllib.request.Request(url)
    req.add_header("Authorization", f"Bearer {DASHBOARD_SERVICE_TOKEN}")
    req.add_header("User-Agent", "PGAM-Intelligence/1.0 weekly_digest")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            import json
            return json.loads(resp.read().decode("utf-8"))
    except Exception as exc:
        print(f"[weekly_digest] {path} failed: {exc}", flush=True)
        return None


def _build_blocks(digest: dict) -> list[dict]:
    """Render the digest payload into Slack Block Kit. The endpoint
    already returns a markdown text blob — we use it as the body and
    add a header + dashboard link footer."""
    meta = digest.get("meta", {})
    text = digest.get("text", "")

    blocks: list[dict] = [
        {
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": ":calendar: Weekly business digest",
                "emoji": True,
            },
        },
        {
            "type": "section",
            "text": {"type": "mrkdwn", "text": text},
        },
        {
            "type": "context",
            "elements": [{
                "type": "mrkdwn",
                "text": (
                    f"<{DASHBOARD_BASE}/admin/executive-dashboard|Open dashboard> · "
                    f"<{DASHBOARD_BASE}/admin/executive-dashboard/opportunities|Opportunities board> · "
                    f"_window {meta.get('current_from')} → {meta.get('current_to')}_"
                ),
            }],
        },
    ]
    return blocks


def run() -> dict:
    """Self-gates to Monday 13:00-15:00 UTC. Once-per-week dedupe."""
    now = datetime.now(timezone.utc)
    # 0 = Monday in Python's weekday()
    if now.weekday() != 0:
        print(f"[weekly_digest] not Monday (weekday={now.weekday()}), skipping", flush=True)
        return {"ok": True, "skipped": "not_monday"}
    if not (13 <= now.hour < 15):
        print(f"[weekly_digest] outside post window (hour={now.hour}), skipping", flush=True)
        return {"ok": True, "skipped": "outside_window"}

    # ISO week dedupe — one post per Mon morning.
    iso_year, iso_week, _ = now.isocalendar()
    dedup_key = f"weekly_digest:{iso_year}-W{iso_week:02d}"
    if already_sent_today(dedup_key):
        print(f"[weekly_digest] already sent this week ({dedup_key})", flush=True)
        return {"ok": True, "skipped": "deduped"}

    if not DASHBOARD_SERVICE_TOKEN:
        print("[weekly_digest] PGAM_DASHBOARD_SERVICE_TOKEN not set — cannot post", flush=True)
        return {"ok": False, "error": "missing_service_token"}

    digest = _api_get("/api/reporting/partner-revenue/weekly-digest")
    if not digest:
        return {"ok": False, "error": "fetch_failed"}

    blocks = _build_blocks(digest)
    fallback = (
        f"Weekly digest — {digest.get('meta', {}).get('current_from')} → "
        f"{digest.get('meta', {}).get('current_to')}"
    )
    send_blocks(blocks=blocks, text=fallback)
    mark_sent(dedup_key)
    print(f"[weekly_digest] posted digest for {dedup_key}", flush=True)
    return {"ok": True, "key": dedup_key}


if __name__ == "__main__":
    res = run()
    sys.exit(0 if res.get("ok") else 1)
