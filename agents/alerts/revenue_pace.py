"""
agents/alerts/revenue_pace.py

Monitors intraday revenue pacing vs. yesterday's baseline and fires a Slack
alert when we fall significantly behind.

Zero-revenue guard
------------------
API data can lag by several minutes after a period boundary. To avoid false
positives we require ZERO_THRESHOLD consecutive zero-revenue checks before
treating a zero as real. The count is persisted in ZERO_STATE_FILE so it
survives between cron invocations.

Alert conditions
----------------
- Current ET hour >= MIN_ALERT_HOUR (9 AM)
- Today's revenue >= THRESHOLDS["min_revenue_for_alert"] (or zero guard fires)
- Pacing gap >= THRESHOLDS["revenue_behind_pct"] (40 %)
- Alert key not already sent today (dedup via core/slack.py)
"""

import json
import os
from datetime import datetime

import pytz

from core.api import fetch, today, yesterday, sf, fmt_usd, pct
from core.config import THRESHOLDS
from core.slack import already_sent_today, mark_sent, send_blocks
from intelligence.claude_analyst import analyze_revenue_pacing

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
METRIC           = "GROSS_REVENUE"
BREAKDOWN        = "DATE"
ALERT_KEY        = "revenue_pace_behind"
ZERO_ALERT_KEY   = "revenue_zero"
MIN_ALERT_HOUR   = 9          # don't alert before 9 AM ET
ZERO_THRESHOLD   = 3          # consecutive zero checks before zero alert fires
ZERO_STATE_FILE  = "/tmp/pgam_revenue_zero_state.json"
ET               = pytz.timezone("US/Eastern")


# ---------------------------------------------------------------------------
# Zero-revenue consecutive-check persistence
# ---------------------------------------------------------------------------

def _load_zero_state() -> dict:
    if not os.path.exists(ZERO_STATE_FILE):
        return {}
    try:
        with open(ZERO_STATE_FILE) as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError):
        return {}


def _save_zero_state(state: dict):
    with open(ZERO_STATE_FILE, "w") as f:
        json.dump(state, f)


def _increment_zero_count(date_str: str) -> int:
    """Increment and persist today's consecutive-zero counter. Returns new count."""
    state = _load_zero_state()
    # Reset counts from previous dates to keep the file small
    state = {k: v for k, v in state.items() if k == date_str}
    state[date_str] = state.get(date_str, 0) + 1
    _save_zero_state(state)
    return state[date_str]


def _reset_zero_count(date_str: str):
    """Reset the zero counter once real revenue is observed."""
    state = _load_zero_state()
    state[date_str] = 0
    _save_zero_state(state)


# ---------------------------------------------------------------------------
# Revenue helpers
# ---------------------------------------------------------------------------

def _sum_revenue(rows: list) -> float:
    """Sum GROSS_REVENUE across all rows returned by the API."""
    total = 0.0
    for row in rows:
        # The field name may be lowercase or mixed-case depending on the API
        for key in ("GROSS_REVENUE", "gross_revenue", "grossRevenue", "revenue"):
            if key in row:
                total += sf(row[key])
                break
    return total


def _expected_revenue(yesterday_total: float, hour_et: int) -> float:
    """
    Linear interpolation of expected revenue at the current ET hour.

    At hour H (0-23) we expect (H / 24) of yesterday's full-day total.
    Clamp to a minimum of hour 1 so we never divide by zero.
    """
    fraction = max(hour_et, 1) / 24.0
    return yesterday_total * fraction


def _build_traffic_mix(rows: list) -> dict:
    """Return a dict suitable for passing to analyze_revenue_pacing."""
    return {
        str(row.get("date", row.get("DATE", i))): sf(
            row.get("GROSS_REVENUE", row.get("gross_revenue", 0))
        )
        for i, row in enumerate(rows)
    }


# ---------------------------------------------------------------------------
# Slack Block Kit payload
# ---------------------------------------------------------------------------

def _build_alert_blocks(
    today_rev: float,
    expected_rev: float,
    yest_rev: float,
    behind_pct: float,
    hour_et: int,
    claude_analysis: str,
) -> list:
    gap        = expected_rev - today_rev
    pace_emoji = ":rotating_light:" if behind_pct >= 60 else ":warning:"
    date_label = datetime.now(ET).strftime("%A, %B %-d")

    return [
        {
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": f"{pace_emoji}  Revenue Pacing Alert — {date_label}",
                "emoji": True,
            },
        },
        {"type": "divider"},
        {
            "type": "section",
            "fields": [
                {
                    "type": "mrkdwn",
                    "text": f"*Today so far:*\n{fmt_usd(today_rev)}",
                },
                {
                    "type": "mrkdwn",
                    "text": f"*Expected at {hour_et}:00 ET:*\n{fmt_usd(expected_rev)}",
                },
                {
                    "type": "mrkdwn",
                    "text": f"*Gap:*\n{fmt_usd(gap)} behind",
                },
                {
                    "type": "mrkdwn",
                    "text": f"*Pacing at:*\n{100 - behind_pct:.1f}% of expected",
                },
                {
                    "type": "mrkdwn",
                    "text": f"*Yesterday full-day:*\n{fmt_usd(yest_rev)}",
                },
                {
                    "type": "mrkdwn",
                    "text": f"*Behind pace by:*\n:red_circle: {behind_pct:.1f}%",
                },
            ],
        },
        {"type": "divider"},
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"*:robot_face: Claude's Analysis*\n{claude_analysis}",
            },
        },
        {
            "type": "context",
            "elements": [
                {
                    "type": "mrkdwn",
                    "text": f"PGAM Intelligence · Revenue Pace Agent · {datetime.now(ET).strftime('%H:%M ET')}",
                }
            ],
        },
    ]


def _build_zero_blocks(consecutive: int, hour_et: int) -> list:
    date_label = datetime.now(ET).strftime("%A, %B %-d")
    return [
        {
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": f":red_circle:  Zero Revenue Detected — {date_label}",
                "emoji": True,
            },
        },
        {"type": "divider"},
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": (
                    f"*No gross revenue has been recorded today* as of *{hour_et}:00 ET*.\n\n"
                    f"This has been confirmed across *{consecutive} consecutive checks* — "
                    f"ruling out API reporting lag.\n\n"
                    f":mag: Immediate investigation recommended: check bidder connectivity, "
                    f"SSP integrations, and demand partner status."
                ),
            },
        },
        {
            "type": "context",
            "elements": [
                {
                    "type": "mrkdwn",
                    "text": f"PGAM Intelligence · Revenue Pace Agent · {datetime.now(ET).strftime('%H:%M ET')}",
                }
            ],
        },
    ]


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def run():
    """
    Execute the revenue pacing check. Designed to be called by a scheduler
    or run directly: `python -m agents.alerts.revenue_pace`.
    """
    now_et   = datetime.now(ET)
    hour_et  = now_et.hour
    date_str = now_et.strftime("%Y-%m-%d")

    # ── 1. Respect quiet hours ──────────────────────────────────────────────
    if hour_et < MIN_ALERT_HOUR:
        print(f"[revenue_pace] Skipping — before {MIN_ALERT_HOUR}:00 ET (now {hour_et}:00).")
        return

    # ── 2. Fetch revenue data ───────────────────────────────────────────────
    today_str = today()
    yest_str  = yesterday()

    try:
        today_rows = fetch(BREAKDOWN, METRIC, today_str, today_str)
        yest_rows  = fetch(BREAKDOWN, METRIC, yest_str,  yest_str)
    except Exception as exc:
        print(f"[revenue_pace] API fetch failed: {exc}")
        return

    today_rev = _sum_revenue(today_rows)
    yest_rev  = _sum_revenue(yest_rows)

    print(f"[revenue_pace] Today: {fmt_usd(today_rev)}  |  Yesterday: {fmt_usd(yest_rev)}  |  Hour ET: {hour_et}")

    # ── 3. Zero-revenue guard ───────────────────────────────────────────────
    if today_rev == 0.0:
        count = _increment_zero_count(date_str)
        print(f"[revenue_pace] Zero revenue — consecutive check #{count}/{ZERO_THRESHOLD}.")

        if count >= ZERO_THRESHOLD and not already_sent_today(ZERO_ALERT_KEY):
            send_blocks(
                blocks=_build_zero_blocks(count, hour_et),
                text=f"ALERT: Zero revenue detected after {count} consecutive checks.",
            )
            mark_sent(ZERO_ALERT_KEY)
            print("[revenue_pace] Zero-revenue alert sent.")
        return

    # Real revenue observed — reset zero counter
    _reset_zero_count(date_str)

    # ── 4. Check minimum revenue threshold ─────────────────────────────────
    min_rev = THRESHOLDS["min_revenue_for_alert"]
    if today_rev < min_rev:
        print(f"[revenue_pace] Revenue {fmt_usd(today_rev)} below minimum threshold {fmt_usd(min_rev)} — skipping.")
        return

    # ── 5. Calculate pacing gap ─────────────────────────────────────────────
    if yest_rev <= 0:
        print("[revenue_pace] No yesterday revenue to compare against — skipping.")
        return

    expected_rev = _expected_revenue(yest_rev, hour_et)
    behind_pct   = max(pct(expected_rev - today_rev, expected_rev), 0.0)

    print(f"[revenue_pace] Expected: {fmt_usd(expected_rev)}  |  Behind: {behind_pct:.1f}%")

    # ── 6. Check threshold and dedup ────────────────────────────────────────
    threshold = THRESHOLDS["revenue_behind_pct"]
    if behind_pct < threshold:
        print(f"[revenue_pace] Pacing gap {behind_pct:.1f}% is within threshold ({threshold}%) — no alert.")
        return

    if already_sent_today(ALERT_KEY):
        print("[revenue_pace] Alert already sent today — skipping.")
        return

    # ── 7. Get Claude's analysis ────────────────────────────────────────────
    traffic_mix = _build_traffic_mix(today_rows)
    try:
        analysis = analyze_revenue_pacing(
            today_spend=today_rev,
            expected_spend=expected_rev,
            yesterday_spend=yest_rev,
            hour_et=hour_et,
            traffic_mix=traffic_mix,
        )
    except Exception as exc:
        print(f"[revenue_pace] Claude analysis failed: {exc}")
        analysis = "Claude analysis unavailable at this time."

    # ── 8. Send Slack alert ─────────────────────────────────────────────────
    blocks = _build_alert_blocks(
        today_rev=today_rev,
        expected_rev=expected_rev,
        yest_rev=yest_rev,
        behind_pct=behind_pct,
        hour_et=hour_et,
        claude_analysis=analysis,
    )
    fallback = (
        f"Revenue pacing alert: {fmt_usd(today_rev)} collected vs "
        f"{fmt_usd(expected_rev)} expected ({behind_pct:.1f}% behind pace)."
    )
    send_blocks(blocks=blocks, text=fallback)
    mark_sent(ALERT_KEY)
    print(f"[revenue_pace] Alert sent — {behind_pct:.1f}% behind pace.")


if __name__ == "__main__":
    run()
