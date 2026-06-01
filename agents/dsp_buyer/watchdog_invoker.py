"""
agents/dsp_buyer/watchdog_invoker.py
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Reliable invoker for the DSP buyer agent's two Vercel routes:

  - /api/v1/cpa/margin-watchdog   (margin floor check — should run every 5 min)
  - /api/v1/buyer-agent/auto-rollback (auto-rollback sweep — every 6 hours)
  - /api/v1/buyer-agent/status-report (daily digest — once per day)

Why it lives here
-----------------
Vercel cron quietly drops cron entries when a project exceeds its plan's
quota. As of 2026-06-01 the pgam-dsp-dashboard `vercel.json` has 67 cron
entries — well over Pro's 40 cap. The margin-watchdog ticks stopped
firing in production (zero ledger rows between 17:50 and 22:30+ on
2026-06-01) even though the route is healthy on direct invocation.

Until the dashboard's cron set is consolidated, pgam-intelligence
(which runs on Render/Railway and isn't quota-limited) drives the
critical buyer-agent ticks by HTTP-invoking the routes on a schedule.

Auth uses the same `Authorization: Bearer $CRON_SECRET` header Vercel
itself sends, so the routes' `cronAuthOk()` check passes without any
route-side changes.

Behavior
--------
- HTTP failure (non-2xx or timeout) → log + Slack alert (P3-style).
- HTTP success → log evaluated/posted counts.
- Idempotent: hitting the watchdog twice per tick is harmless (the route
  produces a fresh audit row each time).
"""

from __future__ import annotations

import os
import time
from typing import Optional

import requests
from dotenv import load_dotenv

from core.slack import send_text

load_dotenv(override=True)

DEFAULT_BASE = "https://dsp.pgammedia.com"
DEFAULT_TIMEOUT_S = 25


def _base_url() -> str:
    return os.environ.get("DSP_DASHBOARD_URL", DEFAULT_BASE).rstrip("/")


def _cron_secret() -> Optional[str]:
    # The dashboard's cronAuthOk() reads CRON_SECRET. We pull from
    # pgam-intelligence's env (rotate from Vercel when it rotates there).
    return os.environ.get("DSP_CRON_SECRET") or os.environ.get("CRON_SECRET")


def _invoke(path: str, label: str, alert_on_fail: bool = True) -> dict:
    """Hit a dashboard cron route. Returns the parsed JSON or an error dict.

    Never raises — failure to invoke shouldn't crash the scheduler.
    """
    secret = _cron_secret()
    if not secret:
        msg = f"[dsp_buyer/{label}] no CRON_SECRET configured — skipping"
        print(msg)
        return {"ok": False, "error": "no_cron_secret"}

    url = f"{_base_url()}{path}"
    started = time.time()
    try:
        res = requests.get(
            url,
            headers={"Authorization": f"Bearer {secret}"},
            timeout=DEFAULT_TIMEOUT_S,
        )
        duration_s = round(time.time() - started, 2)
        if res.status_code >= 400:
            err = f"HTTP {res.status_code} body={res.text[:200]}"
            print(f"[dsp_buyer/{label}] {err} ({duration_s}s)")
            if alert_on_fail:
                send_text(
                    f":warning: DSP buyer agent — {label} invoke failed: {err}",
                )
            return {"ok": False, "status": res.status_code, "error": err}
        body = res.json()
        evaluated = body.get("evaluated") or body.get("campaigns") or 0
        print(
            f"[dsp_buyer/{label}] ok ({duration_s}s) evaluated={evaluated}"
        )
        return {"ok": True, "duration_s": duration_s, **body}
    except requests.exceptions.RequestException as e:
        duration_s = round(time.time() - started, 2)
        err = f"{type(e).__name__}: {e}"
        print(f"[dsp_buyer/{label}] {err} ({duration_s}s)")
        if alert_on_fail:
            send_text(
                f":warning: DSP buyer agent — {label} invoke failed: {err}",
            )
        return {"ok": False, "error": err}


def run_margin_watchdog() -> dict:
    """Invoke the margin-watchdog route. Idempotent; safe at every-5-min cadence."""
    return _invoke("/api/v1/cpa/margin-watchdog", "margin-watchdog")


def run_auto_rollback() -> dict:
    """Invoke the auto-rollback route. Runs every 6h."""
    return _invoke("/api/v1/buyer-agent/auto-rollback", "auto-rollback")


def run_status_report() -> dict:
    """Invoke the daily digest route. Runs once per day at 9am ET."""
    return _invoke("/api/v1/buyer-agent/status-report", "status-report")


if __name__ == "__main__":
    import pprint
    pprint.pprint(run_margin_watchdog())
