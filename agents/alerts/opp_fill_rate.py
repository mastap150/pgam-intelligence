"""
agents/alerts/opp_fill_rate.py

Monitors MTD opportunity fill rate (IMPRESSIONS / OPPORTUNITIES).

PGAM must maintain a fill rate above 0.05% (0.0005) to avoid contractual fees.
This agent runs two independent notification paths:

  1. Daily summary   — Posted once per day. Shows MTD fill rate, 7-day daily
                       trend, and a pass/fail status badge.

  2. Critical alert  — Posted every 4 hours while the MTD fill rate remains
                       below the 0.0005 threshold. Uses a timestamp-based
                       state file (not the date-keyed dedup used elsewhere)
                       so it can re-fire within the same calendar day.

State files
-----------
  /tmp/pgam_opp_fill_state.json
      {
        "summary_date":        "YYYY-MM-DD",   # date last summary was sent
        "critical_last_sent":  1712345678.0    # Unix timestamp of last critical alert
      }
"""

import json
import os
import time
from datetime import date, datetime, timedelta

import pytz

from core.api import fetch, sf, fmt_n, pct
from core.config import THRESHOLDS
from core.slack import send_blocks

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
BREAKDOWN           = "DATE"
METRICS             = ["IMPRESSIONS", "OPPORTUNITIES"]
FILL_THRESHOLD      = THRESHOLDS["opp_fill_threshold"]   # 0.0005 (0.05%)
CRITICAL_RESEND_SEC = 4 * 3600                            # 4 hours in seconds
TREND_DAYS          = 7
STATE_FILE          = "/tmp/pgam_opp_fill_state.json"
ET                  = pytz.timezone("US/Eastern")


# ---------------------------------------------------------------------------
# State helpers (timestamp-based, not date-keyed)
# ---------------------------------------------------------------------------

def _load_state() -> dict:
    if not os.path.exists(STATE_FILE):
        return {}
    try:
        with open(STATE_FILE) as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError):
        return {}


def _save_state(state: dict):
    with open(STATE_FILE, "w") as f:
        json.dump(state, f)


def _summary_already_sent_today(today_str: str) -> bool:
    return _load_state().get("summary_date") == today_str


def _mark_summary_sent(today_str: str):
    state = _load_state()
    state["summary_date"] = today_str
    _save_state(state)


def _critical_alert_due() -> bool:
    """Return True if ≥4 hours have passed since the last critical alert."""
    last = _load_state().get("critical_last_sent", 0.0)
    return (time.time() - last) >= CRITICAL_RESEND_SEC


def _mark_critical_sent():
    state = _load_state()
    state["critical_last_sent"] = time.time()
    _save_state(state)


# ---------------------------------------------------------------------------
# Date helpers
# ---------------------------------------------------------------------------

def _first_of_month() -> str:
    return date.today().replace(day=1).strftime("%Y-%m-%d")


def _today_str() -> str:
    return date.today().strftime("%Y-%m-%d")


def _last_n_days(n: int) -> list[str]:
    """Return a list of the last n calendar dates as 'YYYY-MM-DD', oldest first."""
    today = date.today()
    return [(today - timedelta(days=i)).strftime("%Y-%m-%d") for i in range(n - 1, -1, -1)]


# ---------------------------------------------------------------------------
# Data helpers
# ---------------------------------------------------------------------------

def _extract(row: dict, *keys) -> float:
    for k in keys:
        if k in row:
            return sf(row[k])
    return 0.0


def _parse_rows(rows: list) -> dict[str, dict]:
    """
    Return a dict keyed by date string, each value:
        {"impressions": float, "opportunities": float, "fill_rate": float}
    """
    by_date: dict[str, dict] = {}
    for row in rows:
        dt = (
            row.get("DATE")
            or row.get("date")
            or row.get("reportDate")
            or row.get("report_date")
            or ""
        )
        if not dt:
            continue
        imps  = _extract(row, "IMPRESSIONS", "impressions")
        opps  = _extract(row, "OPPORTUNITIES", "opportunities")
        fill  = (imps / opps) if opps > 0 else 0.0
        by_date[str(dt)] = {"impressions": imps, "opportunities": opps, "fill_rate": fill}
    return by_date


def _mtd_totals(by_date: dict) -> dict:
    """Sum all rows for MTD impressions, opportunities, and derived fill rate."""
    total_imps  = sum(v["impressions"]  for v in by_date.values())
    total_opps  = sum(v["opportunities"] for v in by_date.values())
    fill        = (total_imps / total_opps) if total_opps > 0 else 0.0
    return {"impressions": total_imps, "opportunities": total_opps, "fill_rate": fill}


# ---------------------------------------------------------------------------
# Formatting helpers
# ---------------------------------------------------------------------------

def _fmt_fill(fill_rate: float) -> str:
    """Format fill rate as a percentage with 4 decimal places, e.g. '0.0523%'."""
    return f"{fill_rate * 100:.4f}%"


def _fmt_fill_short(fill_rate: float) -> str:
    """Compact 2-decimal version for trend tables."""
    return f"{fill_rate * 100:.4f}%"


def _status_emoji(fill_rate: float) -> str:
    if fill_rate >= FILL_THRESHOLD * 2:
        return ":large_green_circle:"
    if fill_rate >= FILL_THRESHOLD:
        return ":large_yellow_circle:"
    return ":red_circle:"


def _trend_row(day: str, data: dict | None) -> str:
    if data is None:
        return f"`{day}`  —  no data"
    fill    = data["fill_rate"]
    emoji   = _status_emoji(fill)
    bar_len = min(int(fill / FILL_THRESHOLD * 10), 20)
    bar     = "█" * bar_len or "░"
    return (
        f"{emoji} `{day}`  "
        f"fill: *{_fmt_fill_short(fill)}*  |  "
        f"imps: {fmt_n(data['impressions'])}  |  "
        f"opps: {fmt_n(data['opportunities'])}  "
        f"`{bar}`"
    )


# ---------------------------------------------------------------------------
# Slack Block Kit builders
# ---------------------------------------------------------------------------

def _build_summary_blocks(
    mtd: dict,
    by_date: dict,
    trend_dates: list[str],
    month_label: str,
    now_label: str,
) -> list:
    fill_status = _status_emoji(mtd["fill_rate"])
    threshold_pct = f"{FILL_THRESHOLD * 100:.4f}%"
    mtd_pct       = _fmt_fill(mtd["fill_rate"])
    above_below   = (
        f"*{round((mtd['fill_rate'] / FILL_THRESHOLD - 1) * 100, 1)}% above threshold*"
        if mtd["fill_rate"] >= FILL_THRESHOLD
        else f":warning: *{round((1 - mtd['fill_rate'] / FILL_THRESHOLD) * 100, 1)}% BELOW threshold*"
    )

    trend_text = "\n".join(_trend_row(d, by_date.get(d)) for d in trend_dates)

    return [
        {
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": f":mag:  Opportunity Fill Rate — {month_label}",
                "emoji": True,
            },
        },
        {"type": "divider"},
        {
            "type": "section",
            "fields": [
                {
                    "type": "mrkdwn",
                    "text": f"*MTD Fill Rate:*\n{fill_status}  *{mtd_pct}*",
                },
                {
                    "type": "mrkdwn",
                    "text": f"*Required Threshold:*\n{threshold_pct}",
                },
                {
                    "type": "mrkdwn",
                    "text": f"*MTD Impressions:*\n{fmt_n(mtd['impressions'])}",
                },
                {
                    "type": "mrkdwn",
                    "text": f"*MTD Opportunities:*\n{fmt_n(mtd['opportunities'])}",
                },
                {
                    "type": "mrkdwn",
                    "text": f"*Status:*\n{above_below}",
                },
            ],
        },
        {"type": "divider"},
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"*:calendar: Last {TREND_DAYS}-Day Daily Trend*\n{trend_text}",
            },
        },
        {
            "type": "context",
            "elements": [
                {
                    "type": "mrkdwn",
                    "text": f"PGAM Intelligence · Opp Fill Rate Agent · {now_label}",
                }
            ],
        },
    ]


def _build_critical_blocks(
    mtd: dict,
    hours_until_next: float,
    month_label: str,
    now_label: str,
) -> list:
    mtd_pct        = _fmt_fill(mtd["fill_rate"])
    threshold_pct  = f"{FILL_THRESHOLD * 100:.4f}%"
    below_by_pct   = round((1 - mtd["fill_rate"] / FILL_THRESHOLD) * 100, 1)

    return [
        {
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": f":rotating_light:  CRITICAL: Fill Rate Below Threshold — {month_label}",
                "emoji": True,
            },
        },
        {"type": "divider"},
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": (
                    f":red_circle: MTD opportunity fill rate is *{mtd_pct}* — "
                    f"*{below_by_pct}% below* the required *{threshold_pct}* threshold.\n\n"
                    f"*Risk:* Contractual fee exposure if fill rate is not recovered "
                    f"before end of month.\n\n"
                    f":loudspeaker: This alert will repeat every *4 hours* until the threshold is met."
                ),
            },
        },
        {"type": "divider"},
        {
            "type": "section",
            "fields": [
                {
                    "type": "mrkdwn",
                    "text": f"*Current MTD Fill Rate:*\n:red_circle:  *{mtd_pct}*",
                },
                {
                    "type": "mrkdwn",
                    "text": f"*Required:*\n{threshold_pct}",
                },
                {
                    "type": "mrkdwn",
                    "text": f"*MTD Impressions:*\n{fmt_n(mtd['impressions'])}",
                },
                {
                    "type": "mrkdwn",
                    "text": f"*MTD Opportunities:*\n{fmt_n(mtd['opportunities'])}",
                },
                {
                    "type": "mrkdwn",
                    "text": f"*Gap to threshold:*\n{below_by_pct}% short",
                },
                {
                    "type": "mrkdwn",
                    "text": (
                        f"*Impressions needed to recover:*\n"
                        f"{fmt_n(max(0.0, mtd['opportunities'] * FILL_THRESHOLD - mtd['impressions']))}"
                    ),
                },
            ],
        },
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": (
                    ":mag: *Immediate checks:* SSP integration health · "
                    "Bidder response rates · Floor price aggressiveness · "
                    "Traffic quality filters"
                ),
            },
        },
        {
            "type": "context",
            "elements": [
                {
                    "type": "mrkdwn",
                    "text": f"PGAM Intelligence · Opp Fill Rate Agent · {now_label}",
                }
            ],
        },
    ]


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def run():
    """
    Execute the opportunity fill rate check. Designed to be called by a
    scheduler or run directly: `python -m agents.alerts.opp_fill_rate`.
    """
    now_et     = datetime.now(ET)
    today_str  = _today_str()
    now_label  = now_et.strftime("%H:%M ET")
    month_label = now_et.strftime("%B %Y")

    # ── 1. Fetch MTD data (first of month → today) ───────────────────────────
    mtd_start = _first_of_month()
    print(f"[opp_fill] Fetching MTD data {mtd_start} → {today_str}…")
    try:
        mtd_rows = fetch(BREAKDOWN, METRICS, mtd_start, today_str)
    except Exception as exc:
        print(f"[opp_fill] MTD fetch failed: {exc}")
        return

    if not mtd_rows:
        print("[opp_fill] No MTD data returned — aborting.")
        return

    # ── 2. Parse rows and compute MTD totals ─────────────────────────────────
    by_date = _parse_rows(mtd_rows)
    mtd     = _mtd_totals(by_date)

    print(
        f"[opp_fill] MTD: imps={fmt_n(mtd['impressions'])}  "
        f"opps={fmt_n(mtd['opportunities'])}  "
        f"fill={_fmt_fill(mtd['fill_rate'])}"
    )

    # ── 3. Build 7-day trend date list (may overlap with MTD data) ───────────
    trend_dates = _last_n_days(TREND_DAYS)

    # Fetch any trend dates not already covered by the MTD window
    # (this happens if lookback extends before the 1st of the month)
    earliest_mtd = min(by_date.keys()) if by_date else today_str
    pre_mtd_dates = [d for d in trend_dates if d < earliest_mtd]

    if pre_mtd_dates:
        pre_start = min(pre_mtd_dates)
        pre_end   = max(pre_mtd_dates)
        print(f"[opp_fill] Fetching pre-MTD trend data {pre_start} → {pre_end}…")
        try:
            pre_rows  = fetch(BREAKDOWN, METRICS, pre_start, pre_end)
            by_date.update(_parse_rows(pre_rows))
        except Exception as exc:
            print(f"[opp_fill] Pre-MTD trend fetch failed (non-fatal): {exc}")

    # ── 4. Daily summary (once per day) ──────────────────────────────────────
    if not _summary_already_sent_today(today_str):
        summary_blocks = _build_summary_blocks(
            mtd=mtd,
            by_date=by_date,
            trend_dates=trend_dates,
            month_label=month_label,
            now_label=now_label,
        )
        fill_status_word = "PASS" if mtd["fill_rate"] >= FILL_THRESHOLD else "FAIL"
        fallback = (
            f"Opp Fill Rate {month_label}: MTD fill={_fmt_fill(mtd['fill_rate'])} "
            f"({fill_status_word}, threshold={FILL_THRESHOLD * 100:.4f}%)"
        )
        send_blocks(blocks=summary_blocks, text=fallback)
        _mark_summary_sent(today_str)
        print("[opp_fill] Daily summary sent.")
    else:
        print("[opp_fill] Daily summary already sent today — skipping.")

    # ── 5. Critical threshold alert (every 4 hours while below threshold) ────
    if mtd["fill_rate"] < FILL_THRESHOLD:
        if _critical_alert_due():
            critical_blocks = _build_critical_blocks(
                mtd=mtd,
                hours_until_next=CRITICAL_RESEND_SEC / 3600,
                month_label=month_label,
                now_label=now_label,
            )
            fallback_critical = (
                f"CRITICAL: MTD fill rate {_fmt_fill(mtd['fill_rate'])} is BELOW "
                f"the {FILL_THRESHOLD * 100:.4f}% threshold. Fee risk."
            )
            send_blocks(blocks=critical_blocks, text=fallback_critical)
            _mark_critical_sent()
            print("[opp_fill] Critical threshold alert sent.")
        else:
            elapsed = time.time() - _load_state().get("critical_last_sent", 0.0)
            remaining = (CRITICAL_RESEND_SEC - elapsed) / 3600
            print(
                f"[opp_fill] Below threshold but critical alert not due yet "
                f"({remaining:.1f}h until next)."
            )
    else:
        print(
            f"[opp_fill] Fill rate {_fmt_fill(mtd['fill_rate'])} is above threshold "
            f"({FILL_THRESHOLD * 100:.4f}%) — no critical alert."
        )


if __name__ == "__main__":
    run()
