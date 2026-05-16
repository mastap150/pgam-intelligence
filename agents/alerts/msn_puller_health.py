"""
agents/alerts/msn_puller_health.py

Hourly Slack alert if the MSN Partner Hub puller has stalled.

The puller runs every 15 min via either scheduler.py (local) or
.github/workflows/msn-insights.yml (GH Actions). If a successful pull
hasn't landed in `STALE_AFTER_MIN` minutes we Slack-page once per
calendar day so we hear about a wedged Playwright session before
half a day's data is lost.

We also alert on persistent error streaks (≥3 consecutive failures in
the last 24h, even if the latest run was nominally "successful") —
that pattern usually means the session is half-broken and limping.

Dedupe: one key per day per condition (stale / streak), via the
existing core/slack.already_sent_today helper.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

from core.neon import connect
from core.slack import already_sent_today, mark_sent, send_text

STALE_AFTER_MIN = 45         # 15-min cron with 1 missed tick = 30min; alert at 45.
STREAK_THRESHOLD = 3          # ≥3 consecutive failed runs in trailing 24h


def _load_status() -> dict[str, object]:
    """Return latest pull-run health metrics from Neon."""
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    MAX(started_at)                                  AS last_pull_at,
                    MAX(started_at) FILTER (WHERE ok = TRUE)         AS last_success_at,
                    COUNT(*) FILTER (WHERE started_at > now() - interval '24 hours')                 AS pulls_24h,
                    COUNT(*) FILTER (WHERE started_at > now() - interval '24 hours' AND ok = FALSE)  AS errors_24h
                FROM pgam_direct.msn_pull_runs
            """)
            r = cur.fetchone()
            if not r:
                return {
                    "last_pull_at": None, "last_success_at": None,
                    "pulls_24h": 0, "errors_24h": 0, "last_streak": 0,
                    "last_error": None,
                }
            last_pull_at, last_success_at, pulls_24h, errors_24h = r

            # Count the most recent consecutive failed runs.
            cur.execute("""
                SELECT ok, error_message
                  FROM pgam_direct.msn_pull_runs
                 ORDER BY started_at DESC
                 LIMIT 20
            """)
            recent = cur.fetchall()
            streak = 0
            last_error: Optional[str] = None
            for ok, err in recent:
                if ok:
                    break
                streak += 1
                if last_error is None and err:
                    last_error = err

    return {
        "last_pull_at":    last_pull_at,
        "last_success_at": last_success_at,
        "pulls_24h":       int(pulls_24h or 0),
        "errors_24h":      int(errors_24h or 0),
        "last_streak":     streak,
        "last_error":      last_error,
    }


def _minutes_since(ts: Optional[datetime]) -> Optional[float]:
    if ts is None:
        return None
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    delta = datetime.now(tz=timezone.utc) - ts
    return delta.total_seconds() / 60.0


def run() -> dict[str, object]:
    """Slack-alert if the MSN puller has stalled or is failing repeatedly.

    Returns a status dict for scheduler logging.
    """
    try:
        status = _load_status()
    except Exception as exc:  # noqa: BLE001
        # Table missing on a fresh DB? Don't blow up the scheduler.
        msg = f"[msn_puller_health] could not read msn_pull_runs: {exc}"
        print(msg)
        return {"ok": False, "error": msg, "alerted": False}

    age_min = _minutes_since(status["last_success_at"])  # type: ignore[arg-type]
    streak = int(status["last_streak"] or 0)
    last_err = status["last_error"]
    alerted = False

    # Condition 1: no successful pull within window
    if age_min is None or age_min > STALE_AFTER_MIN:
        key = "msn_puller_stale"
        if not already_sent_today(key):
            age_str = "never" if age_min is None else f"{age_min:.0f} min ago"
            send_text(
                f":rotating_light: *MSN puller stale* — last successful pull {age_str}. "
                f"24h pulls: {status['pulls_24h']}, errors: {status['errors_24h']}. "
                f"Latest error: `{(last_err or '—')[:200]}`"
            )
            mark_sent(key)
            alerted = True
            print(f"[msn_puller_health] ALERTED stale (age={age_str})")

    # Condition 2: error streak of N+ consecutive failed runs
    elif streak >= STREAK_THRESHOLD:
        key = "msn_puller_streak"
        if not already_sent_today(key):
            send_text(
                f":warning: *MSN puller error streak* — {streak} consecutive failed runs. "
                f"Latest error: `{(last_err or '—')[:200]}`"
            )
            mark_sent(key)
            alerted = True
            print(f"[msn_puller_health] ALERTED streak={streak}")

    if not alerted:
        print(
            f"[msn_puller_health] ok — last success "
            f"{f'{age_min:.0f}min ago' if age_min is not None else 'never'}, "
            f"streak={streak}, errors_24h={status['errors_24h']}"
        )

    return {
        "ok":               True,
        "alerted":          alerted,
        "age_minutes":      age_min,
        "streak":           streak,
        "errors_24h":       status["errors_24h"],
    }


if __name__ == "__main__":
    print(run())
