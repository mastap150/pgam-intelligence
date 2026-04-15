"""
agents/alerts/publisher_monetization.py

Tracks new publisher revenue ramp trajectories and surfaces status changes
and new arrivals to the team every morning.

Definitions
-----------
  new publisher    — a publisher whose first observed revenue date falls
                     AFTER the start of our 30-day window AND after yesterday
                     (so we exclude established publishers that simply appear
                     on window day 1 because they existed before)

  established      — first revenue on window_start; used only for benchmarks
                     IF a sufficient number of new publishers are unavailable

  day N            — N calendar days since first_revenue_date (1-indexed)

Benchmark curves (built from peer publishers — other new arrivals in the window)
---------------------------------------------------------------------------
  top_curve        mean daily revenue of the top-10 new publishers
                   by cumulative revenue, aligned to their own day N
  avg_curve        mean of all new publishers at each day N
  under_curve      25th-percentile at each day N

Status classification  (based on last-3-day average vs avg_curve)
---------------------------------------------------------------------------
  Outperforming    last-3-day avg  >  top_curve[current_day]
  On Track         last-3-day avg  >= avg_curve[current_day]
  At Risk          last-3-day avg  <  avg_curve[current_day]

Revenue at risk
---------------------------------------------------------------------------
  For At Risk publishers: sum of (avg_curve[d] - actual[d]) for all days
  where actual was below avg_curve.

State persistence  (/tmp/pgam_pub_monetization_state.json)
---------------------------------------------------------------------------
  {
    "date":     "YYYY-MM-DD",        ← date this state was saved
    "statuses": {"PubA": "On Track", ...}
  }

  Every run: load yesterday's state, compute today's statuses, find diffs,
  post only changes + new arrivals, save new state.
"""

import json
import os
import statistics
from collections import defaultdict
from datetime import date, datetime, timedelta

import pytz

from core.api import fetch, n_days_ago, sf, fmt_usd, fmt_n
from core.config import THRESHOLDS
from core.slack import send_blocks

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
BREAKDOWN        = "DATE,PUBLISHER"
METRICS          = ["GROSS_REVENUE", "IMPRESSIONS", "WINS", "BIDS"]
LOOKBACK_DAYS    = 30
MIN_REVENUE_DAY1 = 1.0      # publisher must have at least $1 on day 1 to be tracked
NEW_PUB_WINDOW   = 28       # publishers first seen within this many days are "new"
                             # (leaving 2-day buffer at window start for established pubs)
TOP_N_BENCHMARK  = 10       # top N publishers used for "top curve"
UNDER_PCT        = 25       # percentile for under-performer curve
MIN_DAYS_FOR_BENCH = 5      # publisher must have ≥ this many days to contribute to benchmarks
STATE_FILE       = "/tmp/pgam_pub_monetization_state.json"
ET               = pytz.timezone("US/Eastern")
ALERT_KEY        = "pub_monetization_morning"


# ---------------------------------------------------------------------------
# State helpers
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
        json.dump(state, f, indent=2)


def _load_yesterday_statuses() -> dict[str, str]:
    """Return {publisher_name: status_string} from yesterday's saved state."""
    state = _load_state()
    today_str = date.today().isoformat()
    saved_date = state.get("date", "")
    yesterday_str = (date.today() - timedelta(days=1)).isoformat()
    if saved_date not in (yesterday_str, today_str):
        return {}
    return state.get("statuses", {})


def _save_today_statuses(statuses: dict[str, str]):
    _save_state({"date": date.today().isoformat(), "statuses": statuses})


# ---------------------------------------------------------------------------
# Data ingestion
# ---------------------------------------------------------------------------

def _extract(row: dict, *keys) -> float:
    for k in keys:
        if k in row:
            return sf(row[k])
    return 0.0


def _pub_name(row: dict) -> str:
    return str(
        row.get("PUBLISHER_NAME") or row.get("PUBLISHER") or row.get("publisher")
        or row.get("pubName") or row.get("pub_name") or "Unknown"
    )


def _row_date(row: dict) -> str:
    return str(
        row.get("DATE") or row.get("date")
        or row.get("reportDate") or row.get("report_date") or ""
    )


def _build_pub_date_index(rows: list) -> dict[str, dict[str, float]]:
    """
    Returns {publisher_name: {date_str: revenue}} for all rows.
    Revenue is the primary signal for ramp analysis.
    """
    index: dict[str, dict[str, float]] = defaultdict(dict)
    for row in rows:
        name    = _pub_name(row)
        dt      = _row_date(row)
        revenue = _extract(row, "GROSS_REVENUE", "gross_revenue")
        if dt:
            index[name][dt] = index[name].get(dt, 0.0) + revenue
    return dict(index)


# ---------------------------------------------------------------------------
# Publisher classification
# ---------------------------------------------------------------------------

def _first_revenue_date(rev_by_date: dict[str, float]) -> str | None:
    """Return the earliest date with revenue > 0, or None."""
    candidates = sorted(
        (dt for dt, rev in rev_by_date.items() if rev >= MIN_REVENUE_DAY1)
    )
    return candidates[0] if candidates else None


def _build_trajectory(
    rev_by_date: dict[str, float],
    first_date_str: str,
    max_days: int,
) -> list[float]:
    """
    Build a list of daily revenues indexed by day number (1-based).
    Day 1 = first_date_str. Missing dates get 0.0.
    Length = max_days.
    """
    first = date.fromisoformat(first_date_str)
    result = []
    for i in range(max_days):
        dt = (first + timedelta(days=i)).isoformat()
        result.append(rev_by_date.get(dt, 0.0))
    return result


def _is_new_publisher(first_date_str: str, window_start_str: str) -> bool:
    """
    True if the publisher first appeared strictly after the window start
    plus the 2-day buffer (meaning they were NOT running before our window).
    """
    buffer_date = (
        date.fromisoformat(window_start_str) + timedelta(days=2)
    ).isoformat()
    return first_date_str > buffer_date


# ---------------------------------------------------------------------------
# Benchmark curve construction
# ---------------------------------------------------------------------------

def _build_benchmark_curves(
    trajectories: dict[str, list[float]],
    max_days: int,
) -> dict:
    """
    Build top, average, and under-performer benchmark curves.

    trajectories: {publisher_name: [day1_rev, day2_rev, ...]}  (equal-length lists)
    max_days:     length of trajectory arrays

    Returns:
        {
          "top":   list[float | None],   # mean of top-N at each day
          "avg":   list[float | None],   # mean of all at each day
          "under": list[float | None],   # 25th-pct at each day
          "n":     list[int],            # count of publishers at each day
        }
    """
    # Only use publishers with enough data to anchor benchmarks
    eligible = {
        name: traj for name, traj in trajectories.items()
        if sum(1 for r in traj if r > 0) >= MIN_DAYS_FOR_BENCH
    }

    if not eligible:
        empty = [None] * max_days
        return {"top": empty, "avg": empty, "under": empty, "n": [0] * max_days}

    # Rank by cumulative revenue to identify "top" publishers
    cum_rev   = {name: sum(t) for name, t in eligible.items()}
    top_names = set(sorted(cum_rev, key=lambda n: cum_rev[n], reverse=True)[:TOP_N_BENCHMARK])

    top_curve   = []
    avg_curve   = []
    under_curve = []
    n_curve     = []

    for day_idx in range(max_days):
        # Collect all publishers that have a non-zero value on this day
        # (zero could mean no data, so we only include if day is within their observed range)
        all_vals = []
        top_vals = []

        for name, traj in eligible.items():
            # Only include if this day is within the publisher's active range
            # (publisher started ≤ day_idx and has non-zero revenue at some point after)
            if day_idx < len(traj):
                rev = traj[day_idx]
                all_vals.append(rev)
                if name in top_names:
                    top_vals.append(rev)

        n_curve.append(len(all_vals))

        if all_vals:
            avg_curve.append(statistics.mean(all_vals))
            sorted_vals = sorted(all_vals)
            under_idx   = max(0, int(len(sorted_vals) * UNDER_PCT / 100) - 1)
            under_curve.append(sorted_vals[under_idx])
        else:
            avg_curve.append(None)
            under_curve.append(None)

        top_curve.append(statistics.mean(top_vals) if top_vals else avg_curve[-1])

    return {"top": top_curve, "avg": avg_curve, "under": under_curve, "n": n_curve}


# ---------------------------------------------------------------------------
# Status classification
# ---------------------------------------------------------------------------

def _classify_status(
    trajectory: list[float],
    curves: dict,
    current_day: int,   # 1-indexed day count since first revenue
) -> tuple[str, float, float]:
    """
    Returns (status, vs_avg_pct, revenue_at_risk).

    Uses the mean of the last 3 available days (or fewer if publisher is new).
    """
    day_idx    = current_day - 1          # 0-indexed
    window     = min(3, current_day)
    recent_rev = trajectory[max(0, day_idx - window + 1): day_idx + 1]
    recent_avg = statistics.mean(recent_rev) if recent_rev else 0.0

    # Benchmark at current day
    avg_at_day   = (curves["avg"][day_idx]   if day_idx < len(curves["avg"])   and curves["avg"][day_idx]   is not None else 0.0)
    top_at_day   = (curves["top"][day_idx]   if day_idx < len(curves["top"])   and curves["top"][day_idx]   is not None else 0.0)

    vs_avg_pct = ((recent_avg - avg_at_day) / avg_at_day * 100) if avg_at_day > 0 else 0.0

    # Revenue at risk = cumulative gap below avg
    revenue_at_risk = 0.0
    for i, actual in enumerate(trajectory[:day_idx + 1]):
        bench = (curves["avg"][i] if i < len(curves["avg"]) and curves["avg"][i] is not None else 0.0)
        if actual < bench:
            revenue_at_risk += bench - actual

    if avg_at_day > 0 and recent_avg > top_at_day:
        status = "Outperforming"
    elif recent_avg >= avg_at_day:
        status = "On Track"
    else:
        status = "At Risk"

    return status, round(vs_avg_pct, 1), round(revenue_at_risk, 2)


# ---------------------------------------------------------------------------
# Publisher record builder
# ---------------------------------------------------------------------------

def _build_publisher_record(
    name: str,
    rev_by_date: dict[str, float],
    first_date_str: str,
    today_str: str,
    curves: dict,
    max_days: int,
) -> dict:
    days_since_start = (
        date.fromisoformat(today_str) - date.fromisoformat(first_date_str)
    ).days + 1

    trajectory      = _build_trajectory(rev_by_date, first_date_str, min(days_since_start, max_days))
    current_day_rev = trajectory[-1] if trajectory else 0.0
    cumulative_rev  = sum(trajectory)
    trajectory_7d   = trajectory[-7:] if len(trajectory) >= 7 else trajectory

    status, vs_avg_pct, revenue_at_risk = _classify_status(
        trajectory, curves, min(days_since_start, max_days)
    )

    return {
        "name":             name,
        "first_revenue":    first_date_str,
        "days_since_start": days_since_start,
        "status":           status,
        "current_daily_rev":round(current_day_rev, 2),
        "cumulative_rev":   round(cumulative_rev, 2),
        "vs_avg_pct":       vs_avg_pct,
        "revenue_at_risk":  revenue_at_risk,
        "trajectory":       [round(r, 2) for r in trajectory],
        "trajectory_7d":    [round(r, 2) for r in trajectory_7d],
    }


# ---------------------------------------------------------------------------
# Change detection
# ---------------------------------------------------------------------------

def _detect_changes(
    publishers: list[dict],
    yesterday_statuses: dict[str, str],
    yesterday_str: str,
) -> tuple[list[dict], list[dict], list[dict]]:
    """
    Returns (new_arrivals, status_changes, unchanged).

    new_arrivals:    publishers whose first_revenue == yesterday_str
    status_changes:  publishers whose status differs from yesterday's saved value
    unchanged:       everyone else (not posted to Slack)
    """
    new_arrivals   = []
    status_changes = []
    unchanged      = []

    for pub in publishers:
        is_new_arrival = pub["first_revenue"] == yesterday_str

        if is_new_arrival:
            new_arrivals.append(pub)
        elif pub["name"] in yesterday_statuses:
            prev_status = yesterday_statuses[pub["name"]]
            if prev_status != pub["status"]:
                pub["previous_status"] = prev_status
                status_changes.append(pub)
            else:
                unchanged.append(pub)
        else:
            # First time we're seeing this publisher (not yesterday, not a known publisher)
            # Treat as informational new arrival
            new_arrivals.append(pub)

    return new_arrivals, status_changes, unchanged


# ---------------------------------------------------------------------------
# Slack Block Kit builders
# ---------------------------------------------------------------------------

STATUS_EMOJI = {
    "Outperforming": ":large_green_circle:",
    "On Track":      ":large_yellow_circle:",
    "At Risk":       ":red_circle:",
}

URGENCY_EMOJI = {"immediate": ":rotating_light:", "this_week": ":warning:"}


def _trajectory_sparkline(traj: list[float], width: int = 10) -> str:
    """Rough ASCII sparkline for the trajectory (last `width` days)."""
    vals = traj[-width:] if len(traj) >= width else traj
    if not vals or max(vals) == 0:
        return "░" * len(vals)
    mx = max(vals)
    bars = "▁▂▃▄▅▆▇█"
    return "".join(bars[min(int(v / mx * 7), 7)] for v in vals)


def _pub_section(pub: dict, tag: str = "") -> dict:
    """Build one Slack section block for a publisher."""
    status_e  = STATUS_EMOJI.get(pub["status"], ":white_circle:")
    spark     = _trajectory_sparkline(pub["trajectory_7d"])
    change_tag = ""
    if "previous_status" in pub:
        prev_e = STATUS_EMOJI.get(pub["previous_status"], ":white_circle:")
        change_tag = f"  {prev_e} → {status_e}"
    else:
        change_tag = f"  {status_e}"

    vs_sign = "+" if pub["vs_avg_pct"] >= 0 else ""
    risk_str = (
        f"  :money_with_wings: rev at risk: {fmt_usd(pub['revenue_at_risk'])}"
        if pub["status"] == "At Risk" and pub["revenue_at_risk"] > 0
        else ""
    )

    text = (
        f"*{pub['name']}*{change_tag}  {tag}\n"
        f"  day *{pub['days_since_start']}*  |  "
        f"today: {fmt_usd(pub['current_daily_rev'])}  |  "
        f"cumul: {fmt_usd(pub['cumulative_rev'])}  |  "
        f"vs avg: *{vs_sign}{pub['vs_avg_pct']:.1f}%*"
        f"{risk_str}\n"
        f"  `{spark}` ← 7-day trend"
    )
    return {"type": "section", "text": {"type": "mrkdwn", "text": text}}


def _claude_pub_block(item: dict, section: str) -> dict:
    """One block for Claude's demand_attention or self_serve classification."""
    urgency = item.get("urgency", "")
    urg_e   = URGENCY_EMOJI.get(urgency, "")
    if section == "demand":
        header = f"{urg_e} *{item['publisher']}*  ({urgency.replace('_', ' ')})"
    else:
        header = f":white_check_mark: *{item['publisher']}*"
    return {
        "type": "section",
        "text": {
            "type": "mrkdwn",
            "text": f"{header}\n  _{item.get('reason', '')}_ ",
        },
    }


def _build_slack_blocks(
    new_arrivals: list[dict],
    status_changes: list[dict],
    claude_result: dict,
    total_tracked: int,
    date_label: str,
    now_label: str,
    benchmarks: dict,
) -> list:
    n_at_risk       = sum(1 for p in new_arrivals + status_changes if p["status"] == "At Risk")
    n_outperforming = sum(1 for p in new_arrivals + status_changes if p["status"] == "Outperforming")

    blocks = [
        {
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": f":seedling:  Publisher Monetization — {date_label}",
                "emoji": True,
            },
        },
        {
            "type": "section",
            "fields": [
                {"type": "mrkdwn", "text": f"*New publishers tracked:*\n{total_tracked}"},
                {"type": "mrkdwn", "text": f"*New arrivals today:*\n{len(new_arrivals)}"},
                {"type": "mrkdwn", "text": f"*Status changes:*\n{len(status_changes)}"},
                {"type": "mrkdwn", "text": f"*At Risk:*\n:red_circle: {n_at_risk}"},
                {"type": "mrkdwn", "text": f"*Outperforming:*\n:large_green_circle: {n_outperforming}"},
                {"type": "mrkdwn", "text": f"*Avg benchmark day-7:*\n{fmt_usd(benchmarks.get('avg_day7', 0))}"},
            ],
        },
        {"type": "divider"},
    ]

    # ── New arrivals ──────────────────────────────────────────────────────────
    if new_arrivals:
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": f":new: *New Publishers — {len(new_arrivals)} arrived yesterday*"},
        })
        for pub in sorted(new_arrivals, key=lambda p: p["cumulative_rev"], reverse=True):
            blocks.append(_pub_section(pub, tag=":new:"))
        blocks.append({"type": "divider"})

    # ── Status changes ────────────────────────────────────────────────────────
    if status_changes:
        # Sort: At Risk first, then by revenue at risk desc
        status_changes.sort(
            key=lambda p: (p["status"] != "At Risk", -p.get("revenue_at_risk", 0))
        )
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": f":arrows_counterclockwise: *Status Changes — {len(status_changes)} overnight*"},
        })
        for pub in status_changes:
            blocks.append(_pub_section(pub))
        blocks.append({"type": "divider"})

    # ── Claude's classifications ──────────────────────────────────────────────
    demand_list = claude_result.get("demand_attention", [])
    self_serve  = claude_result.get("self_serve", [])
    summary     = claude_result.get("summary", "")

    if demand_list or self_serve:
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": ":robot_face: *Claude's Classification*"},
        })

        if demand_list:
            blocks.append({
                "type": "section",
                "text": {"type": "mrkdwn", "text": f"*:wrench: Needs Demand Partner Attention ({len(demand_list)})*"},
            })
            for item in demand_list:
                blocks.append(_claude_pub_block(item, "demand"))

        if self_serve:
            blocks.append({
                "type": "section",
                "text": {"type": "mrkdwn", "text": f"*:herb: Natural Self-Serve ({len(self_serve)})*"},
            })
            for item in self_serve:
                blocks.append(_claude_pub_block(item, "self"))

        if summary:
            blocks.append({"type": "divider"})
            blocks.append({
                "type": "section",
                "text": {"type": "mrkdwn", "text": f":speech_balloon: _{summary}_"},
            })

    blocks.append({"type": "divider"})
    blocks.append({
        "type": "context",
        "elements": [
            {
                "type": "mrkdwn",
                "text": (
                    f"Benchmarks: top-{TOP_N_BENCHMARK} / avg / {UNDER_PCT}th-pct of {LOOKBACK_DAYS}-day new publisher cohort  |  "
                    f"PGAM Intelligence · Publisher Monetization · {now_label}"
                ),
            }
        ],
    })

    return blocks


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def run():
    """
    Execute the publisher monetization morning check. Designed to run daily
    at 6-8 AM ET, or directly: `python -m agents.alerts.publisher_monetization`.

    Posts ONLY publishers whose status changed overnight + yesterday's new arrivals.
    If nothing changed, logs silently and exits without posting.
    """
    now_et      = datetime.now(ET)
    today_str   = date.today().isoformat()
    yesterday_str = (date.today() - timedelta(days=1)).isoformat()
    date_label  = now_et.strftime("%A, %B %-d")
    now_label   = now_et.strftime("%H:%M ET")

    # ── 1. Load yesterday's saved statuses ────────────────────────────────────
    yesterday_statuses = _load_yesterday_statuses()
    print(
        f"[pub_monetization] Loaded {len(yesterday_statuses)} publisher statuses "
        f"from yesterday's state."
    )

    # ── 2. Fetch 30-day DATE,PUBLISHER data ───────────────────────────────────
    window_start = n_days_ago(LOOKBACK_DAYS)
    print(f"[pub_monetization] Fetching {BREAKDOWN} data {window_start} → {today_str}…")
    try:
        rows = fetch(BREAKDOWN, METRICS, window_start, today_str)
    except Exception as exc:
        print(f"[pub_monetization] Fetch failed: {exc}")
        return

    if not rows:
        print("[pub_monetization] No data — aborting.")
        return

    # ── 3. Build publisher-date revenue index ─────────────────────────────────
    pub_date_index = _build_pub_date_index(rows)
    print(f"[pub_monetization] {len(pub_date_index)} publishers in dataset.")

    # ── 4. Identify new publishers and their first revenue dates ──────────────
    pub_first_dates: dict[str, str] = {}
    for name, rev_by_date in pub_date_index.items():
        first = _first_revenue_date(rev_by_date)
        if first and _is_new_publisher(first, window_start):
            pub_first_dates[name] = first

    print(f"[pub_monetization] {len(pub_first_dates)} new publishers identified.")

    if not pub_first_dates:
        print("[pub_monetization] No new publishers in window — saving state and exiting.")
        _save_today_statuses({})
        return

    # ── 5. Build benchmark curves from the new publisher cohort ───────────────
    # Align all trajectories from each publisher's own day 1
    new_trajectories: dict[str, list[float]] = {}
    for name, first_date in pub_first_dates.items():
        days = (date.fromisoformat(today_str) - date.fromisoformat(first_date)).days + 1
        traj = _build_trajectory(pub_date_index[name], first_date, days)
        # Pad to LOOKBACK_DAYS so all arrays are the same length
        padded = traj + [0.0] * (LOOKBACK_DAYS - len(traj))
        new_trajectories[name] = padded

    curves = _build_benchmark_curves(new_trajectories, LOOKBACK_DAYS)

    # Benchmark reference milestones for Claude
    def _curve_at_day(curve: list, day: int) -> float:
        idx = day - 1
        if 0 <= idx < len(curve) and curve[idx] is not None:
            return round(curve[idx], 2)
        return 0.0

    benchmarks = {
        "avg_day1":   _curve_at_day(curves["avg"], 1),
        "avg_day3":   _curve_at_day(curves["avg"], 3),
        "avg_day7":   _curve_at_day(curves["avg"], 7),
        "avg_day14":  _curve_at_day(curves["avg"], 14),
        "top_day1":   _curve_at_day(curves["top"], 1),
        "top_day7":   _curve_at_day(curves["top"], 7),
        "top_day14":  _curve_at_day(curves["top"], 14),
        "under_day7": _curve_at_day(curves["under"], 7),
    }

    # ── 6. Build publisher records with status ────────────────────────────────
    publisher_records: list[dict] = []
    for name, first_date in pub_first_dates.items():
        record = _build_publisher_record(
            name=name,
            rev_by_date=pub_date_index[name],
            first_date_str=first_date,
            today_str=today_str,
            curves=curves,
            max_days=LOOKBACK_DAYS,
        )
        publisher_records.append(record)

    # ── 7. Detect changes and new arrivals ────────────────────────────────────
    new_arrivals, status_changes, unchanged = _detect_changes(
        publisher_records, yesterday_statuses, yesterday_str
    )

    print(
        f"[pub_monetization] New arrivals: {len(new_arrivals)}  |  "
        f"Status changes: {len(status_changes)}  |  "
        f"Unchanged: {len(unchanged)}"
    )

    # ── 8. Save today's state (regardless of whether we post) ─────────────────
    today_statuses = {p["name"]: p["status"] for p in publisher_records}
    _save_today_statuses(today_statuses)

    # ── 9. Skip Slack post if nothing to report ────────────────────────────────
    publishers_to_report = new_arrivals + status_changes
    if not publishers_to_report:
        print("[pub_monetization] No changes or new arrivals — nothing to post.")
        return

    # ── 10. Claude classification (on publishers we're reporting) ─────────────
    claude_input = [
        {
            "name":             p["name"],
            "days_since_start": p["days_since_start"],
            "status":           p["status"],
            "cumulative_rev":   p["cumulative_rev"],
            "current_daily_rev":p["current_daily_rev"],
            "vs_avg_pct":       p["vs_avg_pct"],
            "revenue_at_risk":  p["revenue_at_risk"],
            "trajectory_7d":    p["trajectory_7d"],
        }
        for p in publishers_to_report
    ]

    try:
        from intelligence.claude_analyst import analyze_publisher_monetization
        claude_result = analyze_publisher_monetization(claude_input, benchmarks)
    except Exception as exc:
        print(f"[pub_monetization] Claude analysis failed: {exc}")
        at_risk = [p for p in publishers_to_report if p["status"] == "At Risk"]
        on_track = [p for p in publishers_to_report if p["status"] != "At Risk"]
        claude_result = {
            "demand_attention": [
                {"publisher": p["name"], "reason": f"{p['vs_avg_pct']:.1f}% below average after {p['days_since_start']} days.", "urgency": "immediate"}
                for p in at_risk
            ],
            "self_serve": [
                {"publisher": p["name"], "reason": "Trajectory on or above average benchmark."}
                for p in on_track
            ],
            "summary": f"{len(publishers_to_report)} publishers reported today.",
        }

    # ── 11. Build and post Slack message ──────────────────────────────────────
    blocks = _build_slack_blocks(
        new_arrivals=new_arrivals,
        status_changes=status_changes,
        claude_result=claude_result,
        total_tracked=len(publisher_records),
        date_label=date_label,
        now_label=now_label,
        benchmarks=benchmarks,
    )

    n_demand  = len(claude_result.get("demand_attention", []))
    n_self    = len(claude_result.get("self_serve", []))
    fallback  = (
        f"Publisher Monetization {date_label}: "
        f"{len(new_arrivals)} new arrivals, {len(status_changes)} status changes. "
        f"Needs attention: {n_demand}, self-serve: {n_self}."
    )

    send_blocks(blocks=blocks, text=fallback)
    print(
        f"[pub_monetization] Posted — {len(new_arrivals)} new, "
        f"{len(status_changes)} changed, "
        f"{n_demand} need demand attention."
    )


if __name__ == "__main__":
    run()
