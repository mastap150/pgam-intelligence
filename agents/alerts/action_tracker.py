"""
agents/alerts/action_tracker.py
──────────────────────────────────────────────────────────────────────────────
Closes the loop on every recommendation the system makes.

PUBLIC API (imported by other agents)
──────────────────────────────────────
    from agents.alerts.action_tracker import log_recommendation

    log_recommendation(
        agent_name            = "floor_gap",
        publisher             = "BidMachine - In App Display",
        metric_affected       = "avg_floor_price",
        recommended_change    = "Raise floor from $1.50 to $2.10",
        expected_impact_dollars = 45.0,
    )

Log schema  (/tmp/pgam_action_log.json)
───────────────────────────────────────
[{
    "recommendation_id":      "sha256[:12]",
    "agent_name":             "floor_gap",
    "date_fired":             "YYYY-MM-DD",
    "publisher":              "BidMachine ...",
    "metric_affected":        "avg_floor_price",
    "recommended_change":     "Raise floor from $1.50 to $2.10",
    "expected_impact_dollars": 45.0,
    "status":                 "pending" | "successful" | "ineffective",
    "date_checked":           "YYYY-MM-DD" | null,
    "before_revenue_3d":      123.45 | null,
    "after_revenue_3d":       167.89 | null,
    "actual_impact_dollars":  44.44 | null
}]

Outcome check (runs daily as part of Sunday post)
───────────────────────────────────────────────────
For every "pending" rec where date_fired ≤ today − 3:
  • before_period = [date_fired − 3d, date_fired − 1d]
  • after_period  = [date_fired + 1d, date_fired + 3d]
  • Fetch PUBLISHER breakdown for both periods.
  • If publisher revenue improved > 10%  →  "successful"
  • Otherwise                            →  "ineffective"

Sunday post
───────────
Runs every Sunday.  Deduped via already_sent_today("action_tracker_sunday").
Calls Claude to find which recommendation types have the highest hit rate.
"""

from __future__ import annotations

import hashlib
import json
import math
from collections import defaultdict
from datetime import datetime, date, timedelta
from pathlib import Path

import pytz

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

LOG_FILE    = Path("/tmp/pgam_action_log.json")
ALERT_KEY   = "action_tracker_sunday"
ET          = pytz.timezone("America/New_York")
OUTCOME_LAG = 3          # days after firing before we check outcomes
IMPROVE_PCT = 10.0       # % revenue improvement to call a rec "successful"
SUNDAY      = 6
MIN_HOUR_ET = 8          # post Sunday summary at or after 08:00 ET

BREAKDOWN_PUB = "PUBLISHER"
METRICS_PUB   = "GROSS_REVENUE,IMPRESSIONS,WINS,BIDS"


# ---------------------------------------------------------------------------
# Lazy imports
# ---------------------------------------------------------------------------

def _imports():
    from core.api   import fetch, sf, fmt_usd
    from core.slack import send_blocks, send_text, already_sent_today, mark_sent
    from intelligence.claude_analyst import analyze_action_patterns
    return fetch, sf, fmt_usd, send_blocks, send_text, already_sent_today, mark_sent, analyze_action_patterns


# ---------------------------------------------------------------------------
# Log I/O
# ---------------------------------------------------------------------------

def _load_log() -> list[dict]:
    try:
        if LOG_FILE.exists():
            data = json.loads(LOG_FILE.read_text())
            return data if isinstance(data, list) else []
    except Exception:
        pass
    return []


def _save_log(entries: list[dict]) -> None:
    try:
        LOG_FILE.write_text(json.dumps(entries, indent=2))
    except Exception as exc:
        print(f"[action_tracker] Log write failed: {exc}")


def _rec_id(agent_name: str, publisher: str, date_fired: str, recommended_change: str) -> str:
    """Deterministic 12-char hex ID — ensures idempotent logging."""
    raw = f"{agent_name}|{publisher}|{date_fired}|{recommended_change}"
    return hashlib.sha256(raw.encode()).hexdigest()[:12]


# ---------------------------------------------------------------------------
# Public API — called by other agents
# ---------------------------------------------------------------------------

def log_recommendation(
    agent_name:             str,
    publisher:              str,
    metric_affected:        str,
    recommended_change:     str,
    expected_impact_dollars: float,
) -> str:
    """
    Append a recommendation to the action log.  Idempotent — calling with the
    same (agent, publisher, today, change) a second time is a no-op.

    Returns:
        str: The recommendation_id.
    """
    today_str = datetime.now(ET).strftime("%Y-%m-%d")
    rec_id    = _rec_id(agent_name, publisher, today_str, recommended_change)

    log = _load_log()

    # Idempotency check
    existing_ids = {e["recommendation_id"] for e in log}
    if rec_id in existing_ids:
        return rec_id

    entry = {
        "recommendation_id":       rec_id,
        "agent_name":              agent_name,
        "date_fired":              today_str,
        "publisher":               publisher,
        "metric_affected":         metric_affected,
        "recommended_change":      recommended_change,
        "expected_impact_dollars": round(float(expected_impact_dollars), 2),
        "status":                  "pending",
        "date_checked":            None,
        "before_revenue_3d":       None,
        "after_revenue_3d":        None,
        "actual_impact_dollars":   None,
    }

    log.append(entry)
    _save_log(log)
    print(f"[action_tracker] Logged: {rec_id} ({agent_name} / {publisher})")
    return rec_id


# ---------------------------------------------------------------------------
# Outcome checking
# ---------------------------------------------------------------------------

def _sf(v) -> float:
    if v is None:
        return 0.0
    try:
        f = float(v)
        return 0.0 if math.isnan(f) or math.isinf(f) else f
    except (TypeError, ValueError):
        return 0.0


def _pub_name(row: dict) -> str:
    return str(row.get("PUBLISHER_NAME") or row.get("PUBLISHER") or "").strip()


def _revenue_for_publisher(rows: list, publisher: str) -> float:
    """Sum GROSS_REVENUE for a specific publisher across all rows."""
    total = 0.0
    pub_lower = publisher.lower()
    for row in rows:
        name = _pub_name(row).lower()
        if name == pub_lower or (len(pub_lower) > 20 and pub_lower[:20] in name):
            total += _sf(row.get("GROSS_REVENUE"))
    return total


def check_outcomes(fetch_fn=None) -> int:
    """
    Check all pending recommendations that are >= OUTCOME_LAG days old.
    Updates their status in-place.

    Returns:
        int: Number of recommendations newly resolved.
    """
    if fetch_fn is None:
        from core.api import fetch as fetch_fn  # type: ignore[assignment]

    today_et  = datetime.now(ET).date()
    log       = _load_log()
    resolved  = 0

    # Group pending recs by their before/after date windows to batch fetches
    # Key: (before_start, before_end, after_start, after_end)
    windows: dict[tuple, list[int]] = defaultdict(list)

    for i, entry in enumerate(log):
        if entry.get("status") != "pending":
            continue
        try:
            fired = date.fromisoformat(entry["date_fired"])
        except (ValueError, TypeError):
            continue
        days_since = (today_et - fired).days
        if days_since < OUTCOME_LAG:
            continue

        before_start = (fired - timedelta(days=3)).isoformat()
        before_end   = (fired - timedelta(days=1)).isoformat()
        after_start  = (fired + timedelta(days=1)).isoformat()
        after_end    = (fired + timedelta(days=3)).isoformat()

        # Don't check future "after" periods
        if date.fromisoformat(after_end) > today_et:
            continue

        key = (before_start, before_end, after_start, after_end)
        windows[key].append(i)

    # Fetch data for each unique window and resolve recs
    for (bstart, bend, astart, aend), indices in windows.items():
        try:
            before_rows = fetch_fn(BREAKDOWN_PUB, METRICS_PUB, bstart, bend)
            after_rows  = fetch_fn(BREAKDOWN_PUB, METRICS_PUB, astart, aend)
        except Exception as exc:
            print(f"[action_tracker] Fetch failed for window {bstart}–{aend}: {exc}")
            continue

        for i in indices:
            entry     = log[i]
            publisher = entry["publisher"]

            before_rev = _revenue_for_publisher(before_rows, publisher)
            after_rev  = _revenue_for_publisher(after_rows, publisher)

            if before_rev > 0:
                pct_change = (after_rev - before_rev) / before_rev * 100
                status = "successful" if pct_change >= IMPROVE_PCT else "ineffective"
            else:
                # No revenue before — if there's revenue after, count as successful
                status = "successful" if after_rev > 0 else "ineffective"

            actual_impact = round(after_rev - before_rev, 2)

            log[i]["status"]                = status
            log[i]["date_checked"]          = today_et.isoformat()
            log[i]["before_revenue_3d"]     = round(before_rev, 2)
            log[i]["after_revenue_3d"]      = round(after_rev, 2)
            log[i]["actual_impact_dollars"] = actual_impact

            resolved += 1
            sign = "✓" if status == "successful" else "✗"
            print(
                f"[action_tracker] {sign} {entry['recommendation_id']} "
                f"({publisher}): {status}  "
                f"before=${before_rev:,.0f}  after=${after_rev:,.0f}"
            )

    if resolved:
        _save_log(log)

    return resolved


# ---------------------------------------------------------------------------
# Sunday summary data
# ---------------------------------------------------------------------------

def _weekly_stats(log: list[dict], window_days: int = 7) -> dict:
    """Compute summary stats for the past window_days of recommendations."""
    today_et   = datetime.now(ET).date()
    cutoff     = (today_et - timedelta(days=window_days)).isoformat()

    this_week  = [e for e in log if e.get("date_fired", "") >= cutoff]
    checked    = [e for e in this_week if e.get("status") != "pending"]
    successful = [e for e in this_week if e.get("status") == "successful"]
    pending    = [e for e in this_week if e.get("status") == "pending"]

    pct_actioned = len(checked) / len(this_week) * 100 if this_week else 0.0

    avg_impact = 0.0
    if successful:
        impacts = [e.get("actual_impact_dollars") or 0 for e in successful]
        avg_impact = sum(impacts) / len(impacts)

    # Top 3 pending by expected impact
    top_pending = sorted(pending, key=lambda e: e.get("expected_impact_dollars", 0), reverse=True)[:3]

    # Historical hit rate by agent and by metric
    all_checked   = [e for e in log if e.get("status") in ("successful", "ineffective")]
    by_agent: dict[str, dict] = defaultdict(lambda: {"total": 0, "successful": 0})
    by_metric: dict[str, dict] = defaultdict(lambda: {"total": 0, "successful": 0})

    for e in all_checked:
        agent  = e.get("agent_name", "unknown")
        metric = e.get("metric_affected", "unknown")
        by_agent[agent]["total"]  += 1
        by_metric[metric]["total"] += 1
        if e.get("status") == "successful":
            by_agent[agent]["successful"]  += 1
            by_metric[metric]["successful"] += 1

    agent_rates  = {a: round(v["successful"] / v["total"] * 100, 1) if v["total"] else 0
                    for a, v in by_agent.items()}
    metric_rates = {m: round(v["successful"] / v["total"] * 100, 1) if v["total"] else 0
                    for m, v in by_metric.items()}

    return {
        "recs_this_week":      len(this_week),
        "checked_this_week":   len(checked),
        "successful_this_week": len(successful),
        "pct_actioned":        round(pct_actioned, 1),
        "avg_revenue_impact":  round(avg_impact, 2),
        "top_pending":         top_pending,
        "agent_hit_rates":     agent_rates,
        "metric_hit_rates":    metric_rates,
        "total_logged_all_time": len(log),
        "total_checked_all_time": len(all_checked),
        "total_successful_all_time": len([e for e in log if e.get("status") == "successful"]),
    }


# ---------------------------------------------------------------------------
# Slack Block Kit builder
# ---------------------------------------------------------------------------

def _build_summary_blocks(stats: dict, claude_insight: str, date_str: str) -> list:
    pct = stats["pct_actioned"]
    pct_emoji = ":large_green_circle:" if pct >= 60 else (":large_yellow_circle:" if pct >= 30 else ":white_circle:")

    # Best-performing agent for status line
    best_agent = ""
    if stats["agent_hit_rates"]:
        best_agent = max(stats["agent_hit_rates"], key=lambda a: stats["agent_hit_rates"][a])

    status_line = (
        f":clipboard: *Recommendation Tracker — {date_str}:* "
        f"{stats['recs_this_week']} recs fired this week, "
        f"{stats['successful_this_week']} successful, "
        f"avg impact *${stats['avg_revenue_impact']:,.0f}*."
        + (f"  Best agent: *{best_agent}* ({stats['agent_hit_rates'][best_agent]:.0f}% hit rate)." if best_agent else "")
    )

    blocks: list = [
        {
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": ":clipboard:  Weekly Recommendation Tracker — Sunday Review",
            },
        },
        # ── Status line ──────────────────────────────────────────────────────
        {
            "type": "section",
            "text": {"type": "mrkdwn", "text": status_line},
        },
        {
            "type": "context",
            "elements": [{
                "type": "mrkdwn",
                "text": (
                    f"*{date_str}*  ·  "
                    f"{stats['total_logged_all_time']} total logged  ·  "
                    f"{stats['total_checked_all_time']} evaluated  ·  "
                    f"{stats['total_successful_all_time']} successful all-time"
                ),
            }],
        },
        {"type": "divider"},
        # ── Claude's pattern insight is the centerpiece ───────────────────────
    ]

    if claude_insight:
        blocks += [
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f":bulb: *Claude's Pattern Analysis*\n{claude_insight}",
                },
            },
            {"type": "divider"},
        ]

    # ── This week scorecard ───────────────────────────────────────────────────
    blocks += [
        {
            "type": "section",
            "fields": [
                {
                    "type": "mrkdwn",
                    "text": (
                        f"*This Week*\n"
                        f"{stats['recs_this_week']} recommendations fired\n"
                        f"{stats['checked_this_week']} evaluated  ·  "
                        f"{stats['successful_this_week']} successful"
                    ),
                },
                {
                    "type": "mrkdwn",
                    "text": (
                        f"*Outcome Rate*\n"
                        f"{pct_emoji} *{pct:.0f}%* evaluated\n"
                        f"Avg revenue impact: *${stats['avg_revenue_impact']:,.0f}*"
                    ),
                },
            ],
        },
        {"type": "divider"},
    ]

    # Hit rates by agent
    if stats["agent_hit_rates"]:
        rate_lines = []
        for agent, rate in sorted(stats["agent_hit_rates"].items(), key=lambda x: x[1], reverse=True):
            bar_len = max(0, min(int(rate / 10), 10))
            bar = "█" * bar_len + "░" * (10 - bar_len)
            rate_lines.append(f"`{bar}` *{rate:.0f}%*  _{agent}_")
        blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": "*Hit Rate by Agent (successful / evaluated):*\n" + "\n".join(rate_lines),
            },
        })
        blocks.append({"type": "divider"})

    # Top 3 pending
    if stats["top_pending"]:
        pending_lines = []
        for rec in stats["top_pending"]:
            age = (datetime.now(ET).date() - date.fromisoformat(rec["date_fired"])).days
            pending_lines.append(
                f":hourglass: *{rec['publisher']}*  _{rec['agent_name']}_  "
                f"({age}d ago)\n"
                f"  {rec['recommended_change'][:120]}  "
                f"·  exp. ${rec['expected_impact_dollars']:,.0f}"
            )
        blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": "*Top 3 Pending (by expected impact):*\n\n" + "\n\n".join(pending_lines),
            },
        })
        blocks.append({"type": "divider"})

    blocks.append({
        "type": "context",
        "elements": [{
            "type": "mrkdwn",
            "text": (
                f":information_source:  Outcome = publisher revenue {OUTCOME_LAG} days before vs after recommendation.  "
                f"Successful = >{IMPROVE_PCT:.0f}% revenue improvement.  "
                f"Pending = not yet evaluated (recommendation < {OUTCOME_LAG} days old or after-period in future)."
            ),
        }],
    })

    return blocks


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def run():
    now_et  = datetime.now(ET)
    weekday = now_et.weekday()
    hour_et = now_et.hour

    if weekday != SUNDAY:
        print(f"[action_tracker] Not Sunday (weekday={weekday}). Exiting.")
        return

    if hour_et < MIN_HOUR_ET:
        print(f"[action_tracker] Too early ({hour_et:02d}:xx). Sends at {MIN_HOUR_ET:02d}:00.")
        return

    (fetch, sf, fmt_usd, send_blocks, send_text,
     already_sent_today, mark_sent, analyze_action_patterns) = _imports()

    if already_sent_today(ALERT_KEY):
        print("[action_tracker] Already sent this Sunday. Exiting.")
        return

    # ── 1. Check pending outcomes ─────────────────────────────────────────────
    resolved = check_outcomes(fetch_fn=fetch)
    print(f"[action_tracker] Resolved {resolved} outcomes.")

    # ── 2. Compute weekly stats ───────────────────────────────────────────────
    log   = _load_log()
    stats = _weekly_stats(log)

    print(
        f"[action_tracker] This week: {stats['recs_this_week']} recs, "
        f"{stats['pct_actioned']:.0f}% evaluated, "
        f"avg impact ${stats['avg_revenue_impact']:,.0f}"
    )

    # ── 3. Ask Claude for pattern analysis ────────────────────────────────────
    claude_insight = ""
    try:
        claude_insight = analyze_action_patterns(
            agent_hit_rates  = stats["agent_hit_rates"],
            metric_hit_rates = stats["metric_hit_rates"],
            recent_log       = [e for e in log if e.get("status") in ("successful","ineffective")][-30:],
            stats            = stats,
        )
        print("[action_tracker] Claude insight generated.")
    except Exception as exc:
        print(f"[action_tracker] Claude failed (non-fatal): {exc}")

    # ── 4. Build and post Slack message ───────────────────────────────────────
    date_str = now_et.strftime("%Y-%m-%d")
    blocks   = _build_summary_blocks(stats, claude_insight, date_str)

    fallback = (
        f":clipboard: Action Tracker | {stats['recs_this_week']} recs this week | "
        f"{stats['pct_actioned']:.0f}% evaluated | "
        f"avg impact ${stats['avg_revenue_impact']:,.0f}"
    )

    try:
        send_blocks(blocks, text=fallback)
        mark_sent(ALERT_KEY)
        print("[action_tracker] Sunday summary posted.")
    except Exception as exc:
        print(f"[action_tracker] Slack post failed: {exc}")


if __name__ == "__main__":
    run()
