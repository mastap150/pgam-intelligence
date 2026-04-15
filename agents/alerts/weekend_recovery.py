"""
agents/alerts/weekend_recovery.py
──────────────────────────────────────────────────────────────────────────────
Weekend floor-price optimiser.

Demand partners bid less aggressively on weekends, so floors set for weekday
traffic are often too high and suppress win rates.  This agent:

  • Fetches DATE,PUBLISHER data for the last ~56 days (8 weekends).
  • Splits every publisher's rows into weekday vs. weekend buckets.
  • Fits a linear win-rate model per publisher on the weekend bucket:
        win_rate = intercept + slope × (avg_floor / avg_bid)
  • Derives the analytically optimal weekend floor:
        f* = –intercept × avg_bid / (2 × slope)   [clamped to [0.10·bid, 0.95·bid]]
  • Flags publishers where f* < weekday_floor × 0.85 (>15% gap).
  • Estimates daily revenue recovery:
        recovery = avg_weekend_bids_per_day × Δwin_rate × avg_weekend_ecpm / 1000

Posts to Slack:
  • Every Friday ≥ 16:00 ET → recommended weekend floor adjustments.
  • Every Monday ≥ 09:00 ET → recap comparing this past weekend vs. prior.

State persisted in /tmp/pgam_weekend_recovery_state.json.
"""

from __future__ import annotations

import json
import math
from collections import defaultdict
from datetime import datetime, date, timedelta
from pathlib import Path

import numpy as np
import pytz

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

LOOKBACK_DAYS      = 56           # ~8 weekends
MIN_WEEKEND_ROWS   = 4            # minimum weekend data-points per publisher
MIN_WEEKEND_BIDS   = 200          # minimum total weekend bids (skip tiny pubs)
FLOOR_GAP_PCT      = 0.15         # flag if optimal floor < weekday_floor × (1-0.15)
FLOOR_LOWER_CLAMP  = 0.10         # optimal floor ≥ 10% of avg bid
FLOOR_UPPER_CLAMP  = 0.95         # optimal floor ≤ 95% of avg bid
TOP_N_PUBLISHERS   = 20

FRIDAY_HOUR_ET     = 16           # post Friday alert at ≥ 16:00 ET
MONDAY_HOUR_ET     = 9            # post Monday recap at ≥ 09:00 ET

BREAKDOWN          = "DATE,PUBLISHER"
METRICS            = "GROSS_REVENUE,BIDS,WINS,IMPRESSIONS,GROSS_ECPM,AVG_FLOOR_PRICE,AVG_BID_PRICE"

STATE_FILE         = Path("/tmp/pgam_weekend_recovery_state.json")
ET                 = pytz.timezone("America/New_York")

FRIDAY             = 4
MONDAY             = 0


# ---------------------------------------------------------------------------
# Lazy imports
# ---------------------------------------------------------------------------

def _imports():
    from core.api   import fetch, n_days_ago, today, sf, fmt_usd, fmt_n
    from core.slack import send_blocks, already_sent_today, mark_sent
    from intelligence.claude_analyst import analyze_weekend_floors
    return fetch, n_days_ago, today, sf, fmt_usd, fmt_n, send_blocks, already_sent_today, mark_sent, analyze_weekend_floors


# ---------------------------------------------------------------------------
# Field helpers
# ---------------------------------------------------------------------------

def _sf(v) -> float:
    """Safe float — treats None and 'NaN' strings as 0.0."""
    if v is None:
        return 0.0
    try:
        f = float(v)
        return 0.0 if math.isnan(f) or math.isinf(f) else f
    except (TypeError, ValueError):
        return 0.0


def _pub_name(row: dict) -> str:
    return str(
        row.get("PUBLISHER_NAME") or row.get("PUBLISHER") or row.get("publisher")
        or row.get("pubName") or "Unknown"
    ).strip() or "Unknown"


def _is_weekend(date_str: str) -> bool:
    """Return True if the date falls on Saturday (5) or Sunday (6)."""
    try:
        return datetime.strptime(date_str, "%Y-%m-%d").weekday() >= 5
    except ValueError:
        return False


# ---------------------------------------------------------------------------
# State helpers
# ---------------------------------------------------------------------------

def _load_state() -> dict:
    try:
        if STATE_FILE.exists():
            return json.loads(STATE_FILE.read_text())
    except Exception:
        pass
    return {}


def _save_state(state: dict) -> None:
    try:
        STATE_FILE.write_text(json.dumps(state, indent=2))
    except Exception as exc:
        print(f"[weekend_recovery] State write failed: {exc}")


# ---------------------------------------------------------------------------
# Data parsing
# ---------------------------------------------------------------------------

def _parse_rows(rows: list) -> dict[str, dict[str, list[dict]]]:
    """
    Returns {publisher: {"weekend": [...], "weekday": [...]}} where each entry
    is one day's cleaned metrics dict.
    """
    by_pub: dict[str, dict[str, list]] = defaultdict(lambda: {"weekend": [], "weekday": []})

    for row in rows:
        pub       = _pub_name(row)
        date_str  = str(row.get("DATE", "") or "")
        if not date_str or pub == "Unknown":
            continue

        revenue   = _sf(row.get("GROSS_REVENUE"))
        bids      = _sf(row.get("BIDS"))
        wins      = _sf(row.get("WINS"))
        imps      = _sf(row.get("IMPRESSIONS"))
        ecpm      = _sf(row.get("GROSS_ECPM"))
        avg_floor = _sf(row.get("AVG_FLOOR_PRICE"))
        avg_bid   = _sf(row.get("AVG_BID_PRICE"))

        # Skip rows with no useful data
        if bids < 1 or avg_bid <= 0 or avg_floor <= 0:
            continue

        entry = {
            "date":      date_str,
            "revenue":   revenue,
            "bids":      bids,
            "wins":      wins,
            "imps":      imps,
            "ecpm":      ecpm,
            "avg_floor": avg_floor,
            "avg_bid":   avg_bid,
            "win_rate":  wins / bids,
        }

        bucket = "weekend" if _is_weekend(date_str) else "weekday"
        by_pub[pub][bucket].append(entry)

    return by_pub


# ---------------------------------------------------------------------------
# Optimisation model
# ---------------------------------------------------------------------------

def _fit_optimal_floor(weekend_rows: list[dict]) -> dict | None:
    """
    Fit win_rate = intercept + slope × (avg_floor / avg_bid) on weekend rows.
    Returns optimal floor dict or None if insufficient / invalid data.
    """
    if len(weekend_rows) < MIN_WEEKEND_ROWS:
        return None

    floor_ratios = np.array([r["avg_floor"] / r["avg_bid"] for r in weekend_rows])
    win_rates    = np.array([r["win_rate"] for r in weekend_rows])
    bids_arr     = np.array([r["bids"] for r in weekend_rows])

    if bids_arr.sum() < MIN_WEEKEND_BIDS:
        return None

    # Require some variance in floor ratio to fit a useful model
    if floor_ratios.std() < 1e-4:
        return None

    try:
        slope, intercept = np.polyfit(floor_ratios, win_rates, 1)
    except Exception:
        return None

    # Model only useful when slope < 0 (higher floor → lower win rate)
    if slope >= 0:
        return None

    # Optimal floor ratio: maximises (intercept + slope·x)·x
    # d/dx = intercept + 2·slope·x = 0  →  x* = -intercept/(2·slope)
    opt_ratio = -intercept / (2 * slope)
    if opt_ratio <= 0:
        return None

    # Weighted averages from weekend data (weight by bids)
    total_bids  = bids_arr.sum()
    avg_bid_we  = float(np.average([r["avg_bid"] for r in weekend_rows],   weights=bids_arr))
    avg_ecpm_we = float(np.average([r["ecpm"]    for r in weekend_rows],   weights=bids_arr))
    avg_rev_day = sum(r["revenue"] for r in weekend_rows) / len(weekend_rows)
    bids_per_day = total_bids / len(weekend_rows)

    optimal_floor = opt_ratio * avg_bid_we
    # Clamp to sensible range
    optimal_floor = max(FLOOR_LOWER_CLAMP * avg_bid_we,
                        min(FLOOR_UPPER_CLAMP * avg_bid_we, optimal_floor))

    # R² as confidence signal
    predicted = intercept + slope * floor_ratios
    ss_res = float(np.sum((win_rates - predicted) ** 2))
    ss_tot = float(np.sum((win_rates - win_rates.mean()) ** 2))
    r2 = max(0.0, 1.0 - ss_res / ss_tot) if ss_tot > 1e-12 else 0.0

    # Win-rate at current average floor vs optimal floor
    avg_floor_we = float(np.average([r["avg_floor"] for r in weekend_rows], weights=bids_arr))
    current_ratio  = avg_floor_we / avg_bid_we
    current_wr     = max(0.0, intercept + slope * current_ratio)
    optimal_wr     = max(0.0, intercept + slope * opt_ratio)
    delta_wr       = optimal_wr - current_wr

    # Estimated daily revenue recovery
    ecpm_for_calc  = avg_ecpm_we if avg_ecpm_we > 0 else (avg_bid_we * 1000 * optimal_wr)
    daily_recovery = bids_per_day * delta_wr * ecpm_for_calc / 1000

    return {
        "optimal_floor":     round(optimal_floor, 3),
        "current_weekend_floor": round(avg_floor_we, 3),
        "avg_bid_weekend":   round(avg_bid_we, 3),
        "avg_ecpm_weekend":  round(avg_ecpm_we, 4),
        "avg_rev_per_day":   round(avg_rev_day, 2),
        "bids_per_day":      round(bids_per_day, 0),
        "win_rate_current":  round(current_wr * 100, 2),
        "win_rate_optimal":  round(optimal_wr * 100, 2),
        "delta_win_rate_pp": round(delta_wr * 100, 2),
        "daily_recovery":    round(daily_recovery, 2),
        "r2":                round(r2, 3),
        "n_weekend_days":    len(weekend_rows),
    }


def _weekday_avg_floor(weekday_rows: list[dict]) -> float:
    """Weighted average floor price on weekdays (weight by bids)."""
    if not weekday_rows:
        return 0.0
    total_bids = sum(r["bids"] for r in weekday_rows)
    if total_bids == 0:
        return 0.0
    return sum(r["avg_floor"] * r["bids"] for r in weekday_rows) / total_bids


# ---------------------------------------------------------------------------
# Candidate analysis
# ---------------------------------------------------------------------------

def build_candidates(rows: list) -> list[dict]:
    """
    Parse raw rows and return the top candidates sorted by daily_recovery.
    Each candidate includes the publisher name, floors, recovery estimate.
    """
    by_pub     = _parse_rows(rows)
    candidates = []

    for pub, buckets in by_pub.items():
        weekend_rows = buckets["weekend"]
        weekday_rows = buckets["weekday"]

        if len(weekend_rows) < MIN_WEEKEND_ROWS:
            continue

        model = _fit_optimal_floor(weekend_rows)
        if model is None:
            continue

        wd_floor = _weekday_avg_floor(weekday_rows) if weekday_rows else model["current_weekend_floor"]
        if wd_floor <= 0:
            wd_floor = model["current_weekend_floor"]

        # Only flag if optimal floor is >15% below weekday floor and recovery is positive
        if model["optimal_floor"] >= wd_floor * (1 - FLOOR_GAP_PCT):
            continue
        if model["daily_recovery"] <= 0:
            continue

        gap_pct = (wd_floor - model["optimal_floor"]) / wd_floor * 100

        candidates.append({
            "publisher":             pub,
            "weekday_floor":         round(wd_floor, 3),
            "recommended_floor":     model["optimal_floor"],
            "current_weekend_floor": model["current_weekend_floor"],
            "avg_bid_weekend":       model["avg_bid_weekend"],
            "avg_ecpm_weekend":      model["avg_ecpm_weekend"],
            "avg_rev_per_day":       model["avg_rev_per_day"],
            "daily_recovery":        model["daily_recovery"],
            "win_rate_current_pct":  model["win_rate_current"],
            "win_rate_optimal_pct":  model["win_rate_optimal"],
            "delta_win_rate_pp":     model["delta_win_rate_pp"],
            "gap_pct":               round(gap_pct, 1),
            "r2":                    model["r2"],
            "n_weekend_days":        model["n_weekend_days"],
            "weekend_7d_revenue":    round(sum(r["revenue"] for r in weekend_rows[-14:]), 2),
        })

    candidates.sort(key=lambda c: c["daily_recovery"], reverse=True)
    return candidates[:TOP_N_PUBLISHERS]


# ---------------------------------------------------------------------------
# Weekend revenue comparison (for Monday recap)
# ---------------------------------------------------------------------------

def _get_weekend_dates(target_week_offset: int = 0) -> tuple[str, str]:
    """
    Return (saturday_str, sunday_str) for the most recent complete weekend,
    offset back by target_week_offset weeks.

    offset=0 → this past weekend
    offset=1 → the weekend before that
    """
    today_et = datetime.now(ET).date()
    # Find most recent Sunday
    days_since_sunday = (today_et.weekday() + 1) % 7  # Mon=0..Sun=6 → Sun=0 after shift
    # Actually: weekday() Mon=0..Sun=6; Sunday is 6
    # Days since last Sunday:
    days_since_sun = (today_et.weekday() - 6) % 7
    last_sunday = today_et - timedelta(days=days_since_sun)
    last_saturday = last_sunday - timedelta(days=1)

    last_saturday -= timedelta(weeks=target_week_offset)
    last_sunday   -= timedelta(weeks=target_week_offset)

    return last_saturday.strftime("%Y-%m-%d"), last_sunday.strftime("%Y-%m-%d")


def _compute_weekend_revenue_by_pub(rows: list, sat: str, sun: str) -> dict[str, float]:
    """Sum revenue per publisher for rows matching Saturday or Sunday dates."""
    result: dict[str, float] = defaultdict(float)
    for row in rows:
        d = str(row.get("DATE", ""))
        if d not in (sat, sun):
            continue
        pub = _pub_name(row)
        result[pub] += _sf(row.get("GROSS_REVENUE"))
    return dict(result)


# ---------------------------------------------------------------------------
# Slack Block Kit builders
# ---------------------------------------------------------------------------

def _confidence_label(r2: float) -> str:
    if r2 >= 0.70:
        return ":large_green_circle: High"
    if r2 >= 0.40:
        return ":large_yellow_circle: Med"
    return ":white_circle: Low"


def _build_friday_blocks(candidates: list[dict], date_str: str, claude_analysis: str = "") -> list:
    total_daily_recovery = sum(c["daily_recovery"] for c in candidates)
    total_weekend_recovery = total_daily_recovery * 2  # Sat + Sun
    top = candidates[0] if candidates else None

    status_line = (
        f":calendar: *Weekend Floor Optimisation — {date_str}:* "
        f"{len(candidates)} publisher{'s' if len(candidates) != 1 else ''} flagged — "
        f"*${total_weekend_recovery:,.0f} est. recovery this weekend.*"
        + (
            f"  Top action: lower *{top['publisher']}* floor "
            f"${top['weekday_floor']:.3f} → ${top['recommended_floor']:.3f} "
            f"(+${top['daily_recovery']:.0f}/day)."
            if top else ""
        )
    )

    blocks: list = [
        {
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": ":calendar:  Weekend Floor Optimisation — Friday Briefing",
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
                    f"*{date_str}*  ·  {len(candidates)} publishers flagged  ·  "
                    f"Est. recovery: *${total_weekend_recovery:,.0f} this weekend*  "
                    f"(${total_daily_recovery:,.0f}/day × 2)"
                ),
            }],
        },
        {"type": "divider"},
    ]

    # ── Claude's analysis is the centerpiece ─────────────────────────────────
    if claude_analysis:
        blocks += [
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f":robot_face: *Claude's Weekend Floor Analysis*\n{claude_analysis}",
                },
            },
            {"type": "divider"},
        ]

    for c in candidates:
        floor_change = c["weekday_floor"] - c["recommended_floor"]
        pct_lower    = c["gap_pct"]
        rec_label    = _confidence_label(c["r2"])
        wr_arrow     = f"+{c['delta_win_rate_pp']:.1f}pp win rate" if c["delta_win_rate_pp"] > 0 else ""

        blocks.append({
            "type": "section",
            "fields": [
                {
                    "type": "mrkdwn",
                    "text": (
                        f"*{c['publisher']}*\n"
                        f"Weekday floor: `${c['weekday_floor']:.3f}`  →  "
                        f"Weekend rec: `${c['recommended_floor']:.3f}` "
                        f"(_↓{pct_lower:.0f}%_)"
                    ),
                },
                {
                    "type": "mrkdwn",
                    "text": (
                        f"*+${c['daily_recovery']:.0f}/day recovery*\n"
                        f"{wr_arrow}  ·  {rec_label}  ·  "
                        f"eCPM ${c['avg_ecpm_weekend']:.2f}"
                    ),
                },
            ],
        })

    blocks += [
        {"type": "divider"},
        {
            "type": "context",
            "elements": [{
                "type": "mrkdwn",
                "text": (
                    f":information_source:  Floors are lowered only for Sat–Sun.  "
                    f"Optimal floor = analytically derived from last 8 weekends of bid data.  "
                    f"Confidence = R² of win-rate model.  "
                    f"Floors flagged where recommendation is >15% below weekday floor.  "
                    f"Remember to restore weekday floors Monday morning."
                ),
            }],
        },
    ]

    return blocks


def _build_monday_blocks(
    candidates:   list[dict],
    this_weekend: dict[str, float],   # pub → actual revenue this past weekend
    prev_weekend: dict[str, float],   # pub → actual revenue prior weekend
    predictions:  dict[str, dict],    # pub → {recommended_floor, daily_recovery, weekday_floor}
    date_str:     str,
    sat_str:      str,
    sun_str:      str,
) -> list:
    # Build comparison rows for publishers we had predictions for
    recap_rows: list[dict] = []
    for pub, pred in predictions.items():
        this_rev  = this_weekend.get(pub, 0.0)
        prior_rev = prev_weekend.get(pub, 0.0)
        if this_rev == 0 and prior_rev == 0:
            continue
        change = this_rev - prior_rev
        change_pct = (change / prior_rev * 100) if prior_rev > 0 else None
        exp_recovery = pred.get("daily_recovery", 0) * 2  # Sat+Sun
        vs_prediction = this_rev - prior_rev - 0  # actual improvement vs prior
        recap_rows.append({
            "publisher":     pub,
            "this_rev":      this_rev,
            "prior_rev":     prior_rev,
            "change":        change,
            "change_pct":    change_pct,
            "exp_recovery":  exp_recovery,
        })

    recap_rows.sort(key=lambda r: r["change"], reverse=True)

    # Publishers not in predictions but with significant change
    other_rows: list[dict] = []
    for pub, this_rev in this_weekend.items():
        if pub in predictions:
            continue
        prior_rev = prev_weekend.get(pub, 0.0)
        change = this_rev - prior_rev
        if abs(change) >= 20:
            change_pct = (change / prior_rev * 100) if prior_rev > 0 else None
            other_rows.append({
                "publisher": pub,
                "this_rev":  this_rev,
                "prior_rev": prior_rev,
                "change":    change,
                "change_pct": change_pct,
            })
    other_rows.sort(key=lambda r: r["change"], reverse=True)

    total_this  = sum(this_weekend.values())
    total_prior = sum(prev_weekend.values())
    total_change = total_this - total_prior
    total_pct    = (total_change / total_prior * 100) if total_prior > 0 else None

    change_emoji = ":chart_with_upwards_trend:" if total_change >= 0 else ":chart_with_downwards_trend:"
    change_sign  = "+" if total_change >= 0 else ""

    blocks: list = [
        {
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": ":bar_chart:  Weekend Revenue Recap — Monday Morning",
            },
        },
        {
            "type": "context",
            "elements": [{
                "type": "mrkdwn",
                "text": (
                    f"*{sat_str} – {sun_str}*  ·  "
                    f"Weekend total: *${total_this:,.0f}*  "
                    f"{change_emoji} {change_sign}${total_change:,.0f} "
                    f"({change_sign}{total_pct:.1f}% vs prior weekend)"
                ) if total_pct is not None else (
                    f"*{sat_str} – {sun_str}*  ·  Weekend total: *${total_this:,.0f}*"
                ),
            }],
        },
        {"type": "divider"},
    ]

    # Publishers we had floor recommendations for
    if recap_rows:
        rec_lines = []
        for r in recap_rows[:10]:
            sign  = "+" if r["change"] >= 0 else ""
            pct_s = f" ({sign}{r['change_pct']:.0f}%)" if r["change_pct"] is not None else ""
            arrow = ":small_green_square:" if r["change"] >= 0 else ":small_red_triangle_down:"
            rec_lines.append(
                f"{arrow} *{r['publisher']}*  "
                f"${r['this_rev']:,.0f}{pct_s}  vs prior ${r['prior_rev']:,.0f}"
                + (f"  _(exp. recovery ${r['exp_recovery']:,.0f})_" if r["exp_recovery"] > 0 else "")
            )
        blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": "*Publishers with Friday floor recommendations:*\n" + "\n".join(rec_lines),
            },
        })
        blocks.append({"type": "divider"})

    # Other movers
    if other_rows:
        other_lines = []
        for r in other_rows[:5]:
            sign  = "+" if r["change"] >= 0 else ""
            pct_s = f" ({sign}{r['change_pct']:.0f}%)" if r["change_pct"] is not None else ""
            arrow = ":small_green_square:" if r["change"] >= 0 else ":small_red_triangle_down:"
            other_lines.append(
                f"{arrow} {r['publisher']}  ${r['this_rev']:,.0f}{pct_s}"
            )
        blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": "*Other significant movers (no Friday recommendation):*\n" + "\n".join(other_lines),
            },
        })
        blocks.append({"type": "divider"})

    blocks.append({
        "type": "context",
        "elements": [{
            "type": "mrkdwn",
            "text": (
                f":information_source:  Comparing {sat_str}–{sun_str} vs prior weekend.  "
                f"Revenue change reflects all factors (demand, seasonality, fill rate), "
                f"not floor price alone."
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

    is_friday = weekday == FRIDAY and hour_et >= FRIDAY_HOUR_ET
    is_monday = weekday == MONDAY and hour_et >= MONDAY_HOUR_ET

    if not is_friday and not is_monday:
        day_name = now_et.strftime("%A")
        print(f"[weekend_recovery] Not a posting window ({day_name} {hour_et:02d}:xx ET). Exiting.")
        return

    (fetch, n_days_ago, today, sf, fmt_usd, fmt_n,
     send_blocks, already_sent_today, mark_sent,
     analyze_weekend_floors) = _imports()

    today_str = now_et.strftime("%Y-%m-%d")

    # ── Friday: floor recommendations ────────────────────────────────────
    if is_friday:
        alert_key = "weekend_recovery_friday"
        if already_sent_today(alert_key):
            print("[weekend_recovery] Friday alert already sent today. Exiting.")
            return

        start = n_days_ago(LOOKBACK_DAYS)
        end   = today()

        print(f"[weekend_recovery] Friday — fetching {start} → {end}…")
        try:
            rows = fetch(BREAKDOWN, METRICS, start, end)
        except Exception as exc:
            print(f"[weekend_recovery] Fetch failed: {exc}")
            return

        if not rows:
            print("[weekend_recovery] No data returned.")
            mark_sent(alert_key)
            return

        candidates = build_candidates(rows)
        print(f"[weekend_recovery] {len(candidates)} candidates found.")

        if not candidates:
            print("[weekend_recovery] No publishers meet the 15% gap threshold.")
            mark_sent(alert_key)
            return

        # Persist predictions for Monday recap
        state = _load_state()
        state["last_friday_date"] = today_str
        state["friday_predictions"] = {
            c["publisher"]: {
                "recommended_floor": c["recommended_floor"],
                "daily_recovery":    c["daily_recovery"],
                "weekday_floor":     c["weekday_floor"],
            }
            for c in candidates
        }
        _save_state(state)

        # Ask Claude to explain the weekend pattern and prioritise actions
        claude_analysis = ""
        try:
            claude_analysis = analyze_weekend_floors(candidates)
        except Exception as exc:
            print(f"[weekend_recovery] Claude failed (non-fatal): {exc}")
            # Specific fallback — real numbers, no generic text
            lines = []
            for c in candidates[:3]:
                lines.append(
                    f"• *{c['publisher']}:* lower weekend floor "
                    f"${c['weekday_floor']:.3f} → ${c['recommended_floor']:.3f} "
                    f"({c['gap_pct']:.0f}% reduction). "
                    f"Win rate {c['win_rate_current_pct']:.1f}% → "
                    f"{c['win_rate_optimal_pct']:.1f}% (+{c['delta_win_rate_pp']:.1f}pp). "
                    f"Est. +${c['daily_recovery']:.0f}/day on "
                    f"${c['avg_rev_per_day']:.0f} avg weekend daily (R²={c['r2']:.2f})."
                )
            claude_analysis = "\n".join(lines)

        blocks   = _build_friday_blocks(candidates, today_str, claude_analysis)
        total_wr = sum(c["daily_recovery"] for c in candidates) * 2
        fallback = (
            f":calendar: Weekend Floor Optimisation | {len(candidates)} publishers | "
            f"Est. weekend recovery: ${total_wr:,.0f}"
        )
        try:
            send_blocks(blocks, text=fallback)
            mark_sent(alert_key)
            print("[weekend_recovery] Friday alert posted.")
        except Exception as exc:
            print(f"[weekend_recovery] Slack post failed: {exc}")

    # ── Monday: revenue recap ────────────────────────────────────────────
    elif is_monday:
        alert_key = "weekend_recovery_monday"
        if already_sent_today(alert_key):
            print("[weekend_recovery] Monday recap already sent today. Exiting.")
            return

        # Determine the dates for this past weekend and the one before
        sat0, sun0 = _get_weekend_dates(target_week_offset=0)   # this past weekend
        sat1, sun1 = _get_weekend_dates(target_week_offset=1)   # previous weekend

        # Fetch data spanning both weekends
        fetch_start = sat1  # earliest date we need
        fetch_end   = sun0

        print(f"[weekend_recovery] Monday recap — fetching {fetch_start} → {fetch_end}…")
        try:
            rows = fetch(BREAKDOWN, METRICS, fetch_start, fetch_end)
        except Exception as exc:
            print(f"[weekend_recovery] Fetch failed: {exc}")
            return

        this_weekend = _compute_weekend_revenue_by_pub(rows, sat0, sun0)
        prev_weekend = _compute_weekend_revenue_by_pub(rows, sat1, sun1)

        state       = _load_state()
        predictions = state.get("friday_predictions", {})

        if not this_weekend:
            print("[weekend_recovery] No weekend revenue data found.")
            mark_sent(alert_key)
            return

        # We also need current floor candidates for the blocks header
        candidates_for_next: list[dict] = []
        try:
            lookback_rows = fetch(BREAKDOWN, METRICS, n_days_ago(LOOKBACK_DAYS), today())
            candidates_for_next = build_candidates(lookback_rows)
        except Exception:
            pass  # non-fatal

        blocks   = _build_monday_blocks(
            candidates=candidates_for_next,
            this_weekend=this_weekend,
            prev_weekend=prev_weekend,
            predictions=predictions,
            date_str=today_str,
            sat_str=sat0,
            sun_str=sun0,
        )

        total_this = sum(this_weekend.values())
        fallback   = (
            f":bar_chart: Weekend Recap | {sat0}–{sun0} | "
            f"Total: ${total_this:,.0f}"
        )
        try:
            send_blocks(blocks, text=fallback)
            mark_sent(alert_key)
            print("[weekend_recovery] Monday recap posted.")
        except Exception as exc:
            print(f"[weekend_recovery] Slack post failed: {exc}")


if __name__ == "__main__":
    run()
