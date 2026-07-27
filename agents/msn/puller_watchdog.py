"""
agents/msn/puller_watchdog.py

MSN Partner Hub puller heartbeat monitor. The puller runs from GH
Actions every 15 min (msn-insights.yml) against the OAuth refresh-token
chain in pgam_direct.msn_oauth_token. The chain breaks if we miss a
>24h window; Microsoft's OAuth then returns invalid_grant and every
subsequent tick fails until scripts/msn_oauth_capture.py is re-run
locally with headed Chromium + MFA.

The 2026-07-23 incident: chain expired, puller went silent for 3 days,
weekly review + headline tuner ran on stale data. This watchdog cuts
that from days to hours.

Detection: last ok=TRUE row in pgam_direct.msn_pull_runs. Alert if
>PGAM_MSN_WATCHDOG_STALE_HRS (default 4h) stale. Uses the shared
compliance_alert_state dedup so we don't spam.
"""
from __future__ import annotations

import json
import os
import sys
from datetime import datetime, timedelta, timezone


DEDUP_KEY = "msn_puller_outage"
STALE_HOURS = float(os.environ.get("PGAM_MSN_WATCHDOG_STALE_HRS", "4.0"))


def _staleness_hours(ts: datetime | None) -> float:
    if ts is None:
        return float("inf")
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    return (datetime.now(timezone.utc) - ts).total_seconds() / 3600.0


def _check_puller() -> dict:
    from core.neon import connect
    out: dict = {
        "last_ok_at": None,
        "last_attempt_at": None,
        "last_error": None,
        "fail_streak": 0,
        "runs_last_24h": 0,
        "ok_last_24h": 0,
        "token_updated_at": None,
        "token_refresh_expires_at": None,
    }
    with connect() as c, c.cursor() as cur:
        cur.execute("SELECT MAX(started_at) FROM pgam_direct.msn_pull_runs WHERE ok IS TRUE")
        out["last_ok_at"] = cur.fetchone()[0]

        cur.execute("""
            SELECT started_at, ok, error_message
            FROM pgam_direct.msn_pull_runs
            ORDER BY id DESC LIMIT 1
        """)
        row = cur.fetchone()
        if row:
            out["last_attempt_at"] = row[0]
            out["last_error"] = row[2]

        cur.execute("""
            SELECT COUNT(*) FROM pgam_direct.msn_pull_runs
            WHERE started_at >= now() - INTERVAL '24 hours'
        """)
        out["runs_last_24h"] = cur.fetchone()[0]

        cur.execute("""
            SELECT COUNT(*) FROM pgam_direct.msn_pull_runs
            WHERE ok IS TRUE AND started_at >= now() - INTERVAL '24 hours'
        """)
        out["ok_last_24h"] = cur.fetchone()[0]

        cur.execute("""
            WITH recent AS (
              SELECT ok FROM pgam_direct.msn_pull_runs
              ORDER BY id DESC LIMIT 96
            ),
            failing AS (
              SELECT ok, ROW_NUMBER() OVER () AS rn FROM recent
            )
            SELECT COUNT(*) FROM failing
            WHERE rn <= COALESCE(
              (SELECT MIN(rn) - 1 FROM failing WHERE ok IS TRUE),
              (SELECT MAX(rn) FROM failing)
            )
        """)
        out["fail_streak"] = cur.fetchone()[0] or 0

        cur.execute("""
            SELECT updated_at, refresh_expires_at
            FROM pgam_direct.msn_oauth_token
            ORDER BY updated_at DESC LIMIT 1
        """)
        row = cur.fetchone()
        if row:
            out["token_updated_at"] = row[0]
            out["token_refresh_expires_at"] = row[1]
    return out


def _classify_error(err: str | None) -> str:
    if not err:
        return "unknown"
    if "invalid_grant" in err:
        return "oauth_chain_expired"
    if "HTTP 401" in err or "unauthorized" in err.lower():
        return "access_token_rejected"
    if "HTTP 400" in err:
        return "oauth_bad_request"
    return "other"


def _post_slack(hb: dict, stale_h: float, kind: str) -> bool:
    from core import slack as _slack
    if _slack.already_sent_today_shared(DEDUP_KEY):
        print(f"[msn-watchdog] alert already posted today ({DEDUP_KEY}) — no-op")
        return False

    def _fmt(ts):
        if ts is None:
            return "<never>"
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        try:
            from zoneinfo import ZoneInfo
            return ts.astimezone(ZoneInfo("America/New_York")).strftime("%Y-%m-%d %H:%M ET")
        except Exception:
            return ts.strftime("%Y-%m-%d %H:%M UTC")

    kind_headline = {
        "oauth_chain_expired": (":rotating_light: *MSN puller: OAuth refresh chain EXPIRED*",
                                "Chain broke — we missed the >24h refresh window. Re-mint required."),
        "access_token_rejected": (":warning: *MSN puller: access_token rejected*",
                                  "api.msn.com is 401'ing our minted tokens; scope may have changed."),
        "oauth_bad_request": (":warning: *MSN puller: OAuth 400*",
                              "Token endpoint returning 400; check payload / client_id."),
        "other": (":warning: *MSN puller: silent*",
                  f"No successful pull in {stale_h:.1f}h; last error attached."),
        "unknown": (":warning: *MSN puller: silent*",
                    f"No successful pull in {stale_h:.1f}h; no error captured."),
    }[kind]

    last_err = (hb.get("last_error") or "").strip()
    if len(last_err) > 400:
        last_err = last_err[:400] + "…"

    remediation = (
        "*Fix (local machine only, needs MFA):*\n"
        "```\n"
        "cd ~/Desktop/pgam-intelligence\n"
        "python3 scripts/msn_oauth_capture.py\n"
        "```\n"
        "Complete MSN sign-in + MFA in the visible Chromium window. "
        "New refresh_token lands in `pgam_direct.msn_oauth_token`; "
        "next scheduled msn-insights run picks it up automatically."
    )

    blocks = [
        {"type": "section", "text": {"type": "mrkdwn", "text":
            f"{kind_headline[0]}\n_{kind_headline[1]}_"}},
        {"type": "section", "text": {"type": "mrkdwn", "text":
            f"*Heartbeat:*\n```\n"
            f"last ok=TRUE run:  {_fmt(hb['last_ok_at']):26} ({stale_h:.1f}h stale)\n"
            f"last attempt:      {_fmt(hb['last_attempt_at'])}\n"
            f"runs last 24h:     {hb['runs_last_24h']} ({hb['ok_last_24h']} ok)\n"
            f"fail streak:       {hb['fail_streak']}\n"
            f"token updated_at:  {_fmt(hb['token_updated_at'])}\n"
            f"refresh_expires:   {_fmt(hb['token_refresh_expires_at'])}\n"
            f"```"}},
        {"type": "section", "text": {"type": "mrkdwn", "text":
            f"*Last error*\n```{last_err or '(none)'}```"}},
        {"type": "section", "text": {"type": "mrkdwn", "text": remediation}},
        {"type": "context", "elements": [{"type": "mrkdwn", "text":
            ":robot_face: MSN puller watchdog — hourly from GH Actions. "
            "Dedups per day, one alert until tomorrow."}]},
    ]

    import urllib.request
    webhook = os.environ.get("MSN_SLACK_WEBHOOK", "").strip() or \
              os.environ.get("COMPLIANCE_SLACK_WEBHOOK", "").strip()
    if not webhook or not webhook.startswith(("https://", "http://")):
        # Missing or malformed webhook — don't try to POST. GH Actions
        # env-injects an empty secret as literal '' or '***' and urllib's
        # `Request(bad_url)` raises 'unknown url type', crashing the run
        # BEFORE it can mark the dedup, so every subsequent tick would
        # crash the same way. Short-circuit cleanly instead.
        print(f"[msn-watchdog] no valid webhook configured (got {webhook!r}) — logging only")
        print(f"[msn-watchdog] would-have-alerted: kind={kind} stale={stale_h:.1f}h "
              f"fail_streak={hb.get('fail_streak')}")
        return False
    body = json.dumps({"blocks": blocks, "text": "MSN puller outage"}).encode()
    try:
        req = urllib.request.Request(webhook, data=body, headers={"Content-Type": "application/json"})
        with urllib.request.urlopen(req, timeout=15) as r:
            status = r.status
    except Exception as exc:
        print(f"[msn-watchdog] slack POST failed: {exc}")
        return False
    _slack.mark_sent_shared(DEDUP_KEY)
    print(f"[msn-watchdog] alert posted status={status}")
    return True


def _open_monday_item(hb: dict, kind: str) -> str | None:
    """Best-effort Monday tracker on DSP Dev Work board. Silent on failure."""
    token = os.environ.get("MONDAY_API_TOKEN", "").strip()
    if not token:
        return None
    import subprocess
    root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    cli = os.path.join(root, "scripts", "monday_cli.py")
    name = f"MSN puller outage ({kind}) — re-run scripts/msn_oauth_capture.py"
    note = (
        f"Auto-opened by agents/msn/puller_watchdog.\n"
        f"kind={kind} last_ok={hb.get('last_ok_at')} last_error={(hb.get('last_error') or '')[:200]}"
    )
    try:
        out = subprocess.run(
            ["python3", cli, "create", name, "--status", "In Progress", "--note", note],
            check=False, capture_output=True, text=True, timeout=20,
        )
        return (out.stdout or "").strip() or None
    except Exception as exc:
        print(f"[msn-watchdog] monday create failed: {exc}")
        return None


def run() -> dict:
    hb = _check_puller()
    stale = _staleness_hours(hb["last_ok_at"])
    kind = _classify_error(hb.get("last_error"))
    print(f"[msn-watchdog] stale={stale:.2f}h fail_streak={hb['fail_streak']} kind={kind}")

    if stale <= STALE_HOURS:
        return {"ok": True, "alerted": False, "stale_h": stale}

    posted = _post_slack(hb, stale, kind)
    monday_out = _open_monday_item(hb, kind) if posted else None
    return {
        "ok": True,
        "alerted": posted,
        "stale_h": stale,
        "kind": kind,
        "monday": monday_out,
    }


if __name__ == "__main__":
    print(json.dumps(run(), indent=2, default=str))
