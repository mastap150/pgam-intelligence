"""
agents/alerts/pacing_deviation.py

Real-time pacing-deviation alert. Fires a Slack message when today's
revenue (through hour H) is >20% below the 4-week same-DOW same-hour
baseline. Helps catch day-of issues (integration breaks, partner throttle,
misconfigurations) before the 9 PM revenue reconciliation.

Flow
----
1. Read the hourly funnel store `data/hourly_pub_demand.json.gz`.
2. Compute today's cumulative revenue through the current UTC hour.
3. Compute the median cumulative revenue through the same UTC hour
   on the last 4 same-DOW days. Use median (not mean) to be robust
   to one outlier week.
4. If today / baseline < (1 - DEVIATION_THRESHOLD), fire.

Cooldown: 2h between posts, to avoid spam if the deviation persists.
Only runs between UTC hours 12 and 22 (roughly 08:00 ET - 18:00 ET);
morning/late-night noise isn't actionable anyway.

State file
----------
  /tmp/pgam_pacing_deviation_state.json  { "last_sent": <unix_ts>, "last_ratio": float }
"""
from __future__ import annotations

import gzip
import json
import os
import time
from collections import defaultdict
from datetime import date, datetime, timezone
from pathlib import Path
from statistics import median

from core import slack

DATA_DIR = Path(__file__).parent.parent.parent / "data"
HOURLY_PATH = DATA_DIR / "hourly_pub_demand.json.gz"

STATE_FILE = "/tmp/pgam_pacing_deviation_state.json"
COOLDOWN_SEC = 2 * 60 * 60  # 2 hours

# Alert trigger
DEVIATION_THRESHOLD = 0.20  # today > 20% below baseline → alert
MIN_BASELINE_DOLLARS = 500.0  # don't fire if baseline itself is tiny
LOOKBACK_WEEKS = 4

# Quiet hours (UTC) — don't alert outside these (early AM = sparse data)
QUIET_UTC_HOUR_START = 12   # ~08:00 ET
QUIET_UTC_HOUR_END = 22     # ~18:00 ET


def _load_state() -> dict:
    if not os.path.exists(STATE_FILE):
        return {}
    try:
        with open(STATE_FILE) as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError):
        return {}


def _save_state(state: dict) -> None:
    try:
        with open(STATE_FILE, "w") as f:
            json.dump(state, f)
    except OSError:
        pass


def _cumulative_through_hour(rows: list[dict], target_date: str, through_hour_utc: int) -> float:
    total = 0.0
    for r in rows:
        if str(r.get("DATE", "")) != target_date:
            continue
        if int(r.get("HOUR", 0)) > through_hour_utc:
            continue
        total += float(r.get("GROSS_REVENUE", 0) or 0)
    return total


def _baseline_median(rows: list[dict], today_date: date, through_hour: int,
                     lookback_weeks: int = LOOKBACK_WEEKS) -> float:
    """Median cumulative revenue on the last N same-DOW days through the same hour."""
    from datetime import timedelta
    samples = []
    for w in range(1, lookback_weeks + 1):
        past_date = (today_date - timedelta(days=7 * w)).isoformat()
        v = _cumulative_through_hour(rows, past_date, through_hour)
        if v > 0:
            samples.append(v)
    return median(samples) if samples else 0.0


def run() -> dict:
    """Scheduler entry: check and alert if today is meaningfully behind pace."""
    if not HOURLY_PATH.exists():
        return {"skipped": True, "reason": "hourly data not available"}

    now_utc = datetime.now(timezone.utc)
    if not (QUIET_UTC_HOUR_START <= now_utc.hour <= QUIET_UTC_HOUR_END):
        return {"skipped": True, "reason": f"outside active window (UTC {now_utc.hour})"}

    state = _load_state()
    if (time.time() - state.get("last_sent", 0)) < COOLDOWN_SEC:
        return {"skipped": True, "reason": "cooldown"}

    with gzip.open(HOURLY_PATH, "rt") as f:
        rows = json.load(f)

    today = now_utc.date()
    through_hour = now_utc.hour - 1  # only include completed hours
    if through_hour < 0:
        return {"skipped": True, "reason": "too early in UTC day"}

    today_cum = _cumulative_through_hour(rows, today.isoformat(), through_hour)
    baseline = _baseline_median(rows, today, through_hour)

    if baseline < MIN_BASELINE_DOLLARS:
        return {"skipped": True, "reason": f"baseline too small (${baseline:.0f})"}

    ratio = today_cum / baseline if baseline > 0 else 1.0
    shortfall_pct = (1 - ratio) * 100

    result = {
        "today": today.isoformat(),
        "through_hour_utc": through_hour,
        "today_cum": round(today_cum, 2),
        "baseline_median": round(baseline, 2),
        "ratio": round(ratio, 3),
        "shortfall_pct": round(shortfall_pct, 1),
    }

    if ratio < (1 - DEVIATION_THRESHOLD):
        # Fire
        dow_names = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
        dow = dow_names[today.weekday()]
        msg = (
            f":rotating_light: *Revenue pacing alert* — {dow} {today} through UTC {through_hour:02d}\n"
            f"  • Today so far:   ${today_cum:>8,.0f}\n"
            f"  • 4-wk baseline:  ${baseline:>8,.0f}  (median of last {LOOKBACK_WEEKS} same-DOW days)\n"
            f"  • Pacing at *{ratio*100:.0f}%* of baseline ({shortfall_pct:+.0f}% shortfall)\n\n"
            f"_Next automatic check: in {COOLDOWN_SEC//3600}h if still below threshold. "
            f"Ping Claude to investigate or take action._"
        )
        slack.send_text(msg)
        state["last_sent"] = time.time()
        state["last_ratio"] = ratio
        _save_state(state)
        result["fired"] = True
    else:
        result["fired"] = False

    return result
