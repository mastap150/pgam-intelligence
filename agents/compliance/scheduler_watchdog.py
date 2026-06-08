"""
agents/compliance/scheduler_watchdog.py

Render scheduler heartbeat monitor.

The 2026-06-08 incident: Render's scheduler stopped ticking after 20:09
ET 2026-06-07, but the user discovered it only ~12h later when no
morning digest landed. This module detects the failure mode within ~2h
of it occurring by checking two proxy heartbeats:

  1. compliance_runs.started_at — the runner inserts a tombstone row at
     the start of every audit attempt. Should fire 5+ times in the
     07:45-10:00 ET window plus the 10:30 ET fallback.

  2. compliance_enforcement_log.created_at — the enforcer agent runs at
     :47 past every hour and writes at least one log row (even when
     dry-run mode has nothing to do, it logs the no-op).

If both have been silent for 2+ hours during business hours (09:00-
22:00 ET), the scheduler is almost certainly down on Render. The
watchdog posts a single Slack alert to #compliance with the diagnostic
data the operator needs to act:
  • Last heartbeat times (UTC + ET)
  • Specific dashboard URL hint
  • Whether yesterday's runs completed normally (helps distinguish
    "deploy broke things" from "Render service stopped")

Dedup: only one alert per ET-calendar-day, keyed
`render_scheduler_outage`. So once you've been alerted, repeated
runs of this script won't spam the channel.

Wired into .github/workflows/compliance-watchdog.yml — fires every
hour from 09:00 to 22:00 ET via GitHub Actions cron. Runs outside
Render, so it can detect Render being down.
"""
from __future__ import annotations

import json
import os
import sys
import urllib.request
from datetime import datetime, timezone, timedelta


HEARTBEAT_STALE_HOURS = float(os.environ.get("PGAM_WATCHDOG_STALE_HRS", "2.0"))
DEDUP_KEY = "render_scheduler_outage"

# ET business hours when we expect activity. Outside this window we
# don't alert (scheduler is allowed to be quiet at 3 AM ET).
BUSINESS_START_HOUR_ET = 9
BUSINESS_END_HOUR_ET   = 22


def _et_now() -> datetime:
    """Naive ET datetime — sufficient for hour comparisons."""
    from zoneinfo import ZoneInfo
    return datetime.now(ZoneInfo("America/New_York"))


def _check_heartbeats() -> dict:
    """Query Neon for the two scheduler heartbeats."""
    from core.neon import connect
    out = {"runs_last": None, "enforcement_last": None,
           "runs_today_count": 0, "enforcement_today_count": 0,
           "last_ok_run": None}
    with connect() as c, c.cursor() as cur:
        cur.execute("SELECT MAX(started_at) FROM pgam_direct.compliance_runs")
        out["runs_last"] = cur.fetchone()[0]

        cur.execute("""
            SELECT MAX(created_at) FROM pgam_direct.compliance_enforcement_log
        """)
        out["enforcement_last"] = cur.fetchone()[0]

        cur.execute("""
            SELECT COUNT(*) FROM pgam_direct.compliance_runs
            WHERE (started_at AT TIME ZONE 'America/New_York')::date
                = (now() AT TIME ZONE 'America/New_York')::date
        """)
        out["runs_today_count"] = cur.fetchone()[0]

        cur.execute("""
            SELECT COUNT(*) FROM pgam_direct.compliance_enforcement_log
            WHERE (created_at AT TIME ZONE 'America/New_York')::date
                = (now() AT TIME ZONE 'America/New_York')::date
        """)
        out["enforcement_today_count"] = cur.fetchone()[0]

        cur.execute("""
            SELECT MAX(started_at) FROM pgam_direct.compliance_runs
            WHERE ok IS TRUE
        """)
        out["last_ok_run"] = cur.fetchone()[0]
    return out


def _staleness_hours(ts: datetime | None) -> float:
    """Hours since ts (UTC-aware). Returns inf if ts is None."""
    if ts is None:
        return float("inf")
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    delta = datetime.now(timezone.utc) - ts
    return delta.total_seconds() / 3600.0


def _post_alert(hb: dict, max_stale_h: float, webhook: str) -> bool:
    from core import slack as _slack
    if _slack.already_sent_today_shared(DEDUP_KEY):
        print(f"[watchdog] alert already posted today ({DEDUP_KEY}) — no-op")
        return False

    runs_stale = _staleness_hours(hb["runs_last"])
    enf_stale  = _staleness_hours(hb["enforcement_last"])

    from zoneinfo import ZoneInfo
    def _et(ts):
        if ts is None: return "<never>"
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        return ts.astimezone(ZoneInfo("America/New_York")).strftime("%Y-%m-%d %H:%M ET")

    blocks = [
        {"type":"section","text":{"type":"mrkdwn","text":
            f":rotating_light: *RENDER SCHEDULER APPEARS DOWN*\n"
            f"_No heartbeat from the scheduler in {max_stale_h:.1f}+ hours. "
            f"Today's compliance digest will *NOT* fire automatically until this is resolved._"
        }},
        {"type":"section","text":{"type":"mrkdwn","text":
            f"*Heartbeat status:*\n"
            f"```\n"
            f"compliance_runs    last started:  {_et(hb['runs_last']):26} ({runs_stale:.1f}h stale)\n"
            f"enforcement_log    last write:    {_et(hb['enforcement_last']):26} ({enf_stale:.1f}h stale)\n"
            f"runs today:        {hb['runs_today_count']}\n"
            f"enforcer ticks today: {hb['enforcement_today_count']}\n"
            f"last ok=TRUE run:  {_et(hb['last_ok_run'])}\n"
            f"```"
        }},
        {"type":"section","text":{"type":"mrkdwn","text":
            f"*Action — Render dashboard:* "
            f"https://dashboard.render.com → `pgam-intelligence-scheduler`\n"
            f"  1. Check service status (Suspended / Failed → resume)\n"
            f"  2. If status looks OK: *Manual Deploy → Clear build cache & deploy*\n"
            f"  3. Verify Instance Type is *Standard* (Settings → Instance Type)\n"
            f"  4. Paste any red error from latest deploy log if it still won't start\n\n"
            f"*Fallback already firing:* GitHub Actions `compliance-fallback.yml` "
            f"runs at 09:00 ET + 11:30 ET — so the *daily digest still lands* even "
            f"while Render is down. Outage is in the *full audit + hourly enforcer*, "
            f"not in delivery itself."
        }},
        {"type":"context","elements":[{"type":"mrkdwn","text":
            f":robot_face: Watchdog runs from GitHub Actions every hour 09:00–22:00 ET. "
            f"This alert dedups per day — you'll get exactly one until tomorrow."
        }]}
    ]

    body = json.dumps({"blocks": blocks, "text": "Render scheduler appears down"}).encode()
    req = urllib.request.Request(webhook, data=body, headers={"Content-Type":"application/json"})
    with urllib.request.urlopen(req, timeout=15) as r:
        status = r.status
    _slack.mark_sent_shared(DEDUP_KEY)
    print(f"[watchdog] alert posted, status={status}")
    return True


def run() -> dict:
    """Entry point. Returns a result dict.

    Detection logic (simplified after 2026-06-08 incident learnings):
      • `compliance_runs` is the reliable heartbeat — at least one row
        gets inserted between 07:45 and 10:30 ET each day by the
        retry+fallback windows. The enforcer's log table isn't a good
        heartbeat because it's legitimately empty when no paths are
        in 'active' status (no work = no row).
      • Alert if BOTH:
          (a) we're past 10:30 ET (all morning windows should have
              fired by then), AND
          (b) zero compliance_runs rows exist for today (ET-calendar)
      • Secondary alert: it's past 11:00 ET and we have runs but none
        successful — likely the OOM-in-roundtrip pattern.
    """
    et = _et_now()
    if not (BUSINESS_START_HOUR_ET <= et.hour < BUSINESS_END_HOUR_ET):
        print(f"[watchdog] outside business window ({et.hour:02d}:xx ET); skipping")
        return {"ok": True, "skipped": "outside_business_window"}

    webhook = os.environ.get("COMPLIANCE_SLACK_WEBHOOK", "").strip()
    if not webhook:
        print("[watchdog] COMPLIANCE_SLACK_WEBHOOK not set; can't alert")
        return {"ok": False, "skipped": "no_webhook"}

    hb = _check_heartbeats()
    runs_stale = _staleness_hours(hb["runs_last"])
    print(f"[watchdog] runs_stale={runs_stale:.2f}h  "
          f"runs_today={hb['runs_today_count']}  "
          f"enf_today={hb['enforcement_today_count']}  "
          f"last_ok={hb['last_ok_run']}")

    # Primary: did ANY compliance_run fire today?
    # By the time this watchdog runs (>=09:00 ET), the 07:45 and 08:00
    # ET runs should have both inserted tombstones — even if they OOMed
    # mid-execution, the tombstone goes in BEFORE the heavy work.
    # Zero runs by 10:30 ET = scheduler is down.
    if et.hour >= 10 and hb["runs_today_count"] == 0:
        print(f"[watchdog] OUTAGE: zero compliance_runs today by {et.strftime('%H:%M')} ET")
        alerted = _post_alert(hb, max(runs_stale, 0.0), webhook)
        return {"ok": True, "alerted": alerted, "outage_kind": "no_runs_today",
                "runs_today": hb["runs_today_count"]}

    # Secondary: runs fired today but none completed successfully —
    # different failure mode (OOM mid-execution, broken finalize, etc.)
    # Only alert after 11:30 ET so we don't catch in-progress runs.
    if et.hour >= 11 and hb["runs_today_count"] > 0 and hb["last_ok_run"]:
        from zoneinfo import ZoneInfo
        last_ok_et = hb["last_ok_run"]
        if last_ok_et.tzinfo is None:
            last_ok_et = last_ok_et.replace(tzinfo=timezone.utc)
        last_ok_et_date = last_ok_et.astimezone(ZoneInfo("America/New_York")).date()
        if last_ok_et_date < et.date():
            print(f"[watchdog] DEGRADED: runs fired today but none ok=TRUE "
                  f"(last_ok ET-date={last_ok_et_date})")
            alerted = _post_alert(hb, runs_stale, webhook)
            return {"ok": True, "alerted": alerted, "outage_kind": "no_successful_run",
                    "runs_today": hb["runs_today_count"]}

    print(f"[watchdog] heartbeat OK ({hb['runs_today_count']} runs today)")
    return {"ok": True, "alerted": False,
            "runs_stale_h": runs_stale,
            "runs_today": hb["runs_today_count"]}


if __name__ == "__main__":
    print(json.dumps(run(), indent=2, default=str))
