"""
agents/alerts/weekly_review.py
──────────────────────────────────────────────────────────────────────────────
Monday morning weekly executive briefing.

Runs every Monday at 7 AM ET.  Fetches the previous 7 days plus the week
before that for week-on-week comparison, then asks Claude to write the entire
Slack message.  No Block Kit — Claude's prose is the post.

Data collected
──────────────
• DATE breakdown (last 14 days)  →  this week + prior week daily series
• PUBLISHER breakdown (last 7 days)  →  top 5 publishers
• DEMAND_PARTNER breakdown (last 7 days)  →  top 5 demand partners
• MTD DATE breakdown (month start → yesterday)  →  monthly pacing

Claude answers
──────────────
1. Are we on track for the $1 M monthly target?
2. What was the single biggest win last week?
3. What was the single biggest missed opportunity?
4. What are the three most important actions for this week?

The complete Claude response is posted as the Slack message.
"""

from __future__ import annotations

import calendar
import math
from datetime import datetime, date, timedelta

import pytz

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

MONTHLY_TARGET   = 1_000_000.0   # $1 M
ALERT_KEY        = "weekly_review_monday"
MIN_HOUR_ET      = 7             # send at or after 07:00 ET
ET               = pytz.timezone("America/New_York")

BREAKDOWN_DATE   = "DATE"
BREAKDOWN_PUB    = "PUBLISHER"
BREAKDOWN_DP     = "DEMAND_PARTNER"
METRICS_FULL     = "GROSS_REVENUE,PUB_PAYOUT,IMPRESSIONS,WINS,BIDS"
METRICS_DP       = "GROSS_REVENUE,IMPRESSIONS,WINS,BIDS"


# ---------------------------------------------------------------------------
# Lazy imports
# ---------------------------------------------------------------------------

def _imports():
    from core.api   import fetch, n_days_ago, today, sf, fmt_usd, fmt_n
    from core.slack import send_text, already_sent_today, mark_sent
    from intelligence.claude_analyst import write_weekly_briefing
    return (fetch, n_days_ago, today, sf, fmt_usd, fmt_n,
            send_text, already_sent_today, mark_sent,
            write_weekly_briefing)


# ---------------------------------------------------------------------------
# Field helpers
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
    return str(
        row.get("PUBLISHER_NAME") or row.get("PUBLISHER") or "Unknown"
    ).strip() or "Unknown"


def _dp_name(row: dict) -> str:
    return str(
        row.get("DEMAND_PARTNER_NAME") or row.get("demand_partner_name") or "Unknown"
    ).strip() or "Unknown"


# ---------------------------------------------------------------------------
# Aggregation helpers
# ---------------------------------------------------------------------------

def _week_stats(date_rows: list, start_str: str, end_str: str) -> dict:
    """
    Compute aggregate stats for rows whose DATE falls within [start_str, end_str].
    """
    in_window = [
        r for r in date_rows
        if start_str <= str(r.get("DATE", "")) <= end_str
    ]

    if not in_window:
        return {}

    daily: list[dict] = []
    for row in in_window:
        rev    = _sf(row.get("GROSS_REVENUE"))
        payout = _sf(row.get("PUB_PAYOUT"))
        imps   = _sf(row.get("IMPRESSIONS"))
        wins   = _sf(row.get("WINS"))
        bids   = _sf(row.get("BIDS"))
        margin = (rev - payout) / rev * 100 if rev > 0 else 0.0
        daily.append({
            "date":    str(row.get("DATE", "")),
            "revenue": round(rev, 2),
            "payout":  round(payout, 2),
            "margin":  round(margin, 1),
            "imps":    int(imps),
            "wins":    int(wins),
            "bids":    int(bids),
            "win_rate": round(wins / bids * 100, 2) if bids > 0 else 0.0,
        })

    daily.sort(key=lambda d: d["date"])

    total_rev    = sum(d["revenue"] for d in daily)
    total_payout = sum(d["payout"] for d in daily)
    total_imps   = sum(d["imps"] for d in daily)
    total_wins   = sum(d["wins"] for d in daily)
    total_bids   = sum(d["bids"] for d in daily)
    avg_rev      = total_rev / len(daily) if daily else 0.0
    avg_margin   = (total_rev - total_payout) / total_rev * 100 if total_rev > 0 else 0.0

    best_day  = max(daily, key=lambda d: d["revenue"]) if daily else {}
    worst_day = min(daily, key=lambda d: d["revenue"]) if daily else {}

    # Margin trend: compare first half vs second half
    mid = len(daily) // 2
    first_half_margin = (
        sum(d["margin"] for d in daily[:mid]) / mid if mid > 0 else 0.0
    )
    second_half_margin = (
        sum(d["margin"] for d in daily[mid:]) / (len(daily) - mid)
        if len(daily) - mid > 0 else 0.0
    )
    margin_trend = "improving" if second_half_margin > first_half_margin + 0.5 else (
        "declining" if second_half_margin < first_half_margin - 0.5 else "stable"
    )

    return {
        "total_revenue":    round(total_rev, 2),
        "total_payout":     round(total_payout, 2),
        "avg_daily_revenue": round(avg_rev, 2),
        "avg_margin_pct":   round(avg_margin, 1),
        "margin_trend":     margin_trend,
        "total_impressions": total_imps,
        "total_wins":       total_wins,
        "total_bids":       total_bids,
        "win_rate_pct":     round(total_wins / total_bids * 100, 2) if total_bids > 0 else 0.0,
        "best_day":         best_day,
        "worst_day":        worst_day,
        "daily_series":     daily,
        "n_days":           len(daily),
    }


def _top_publishers(pub_rows: list, n: int = 5) -> list[dict]:
    pubs = []
    for row in pub_rows:
        rev    = _sf(row.get("GROSS_REVENUE"))
        payout = _sf(row.get("PUB_PAYOUT"))
        imps   = _sf(row.get("IMPRESSIONS"))
        wins   = _sf(row.get("WINS"))
        bids   = _sf(row.get("BIDS"))
        margin = (rev - payout) / rev * 100 if rev > 0 else 0.0
        pubs.append({
            "publisher":  _pub_name(row),
            "revenue":    round(rev, 2),
            "payout":     round(payout, 2),
            "margin_pct": round(margin, 1),
            "impressions": int(imps),
            "win_rate_pct": round(wins / bids * 100, 2) if bids > 0 else 0.0,
        })
    pubs.sort(key=lambda p: p["revenue"], reverse=True)
    return pubs[:n]


def _top_demand_partners(dp_rows: list, n: int = 5) -> list[dict]:
    dps = []
    for row in dp_rows:
        rev  = _sf(row.get("GROSS_REVENUE"))
        imps = _sf(row.get("IMPRESSIONS"))
        wins = _sf(row.get("WINS"))
        bids = _sf(row.get("BIDS"))
        dps.append({
            "demand_partner": _dp_name(row),
            "revenue":        round(rev, 2),
            "impressions":    int(imps),
            "win_rate_pct":   round(wins / bids * 100, 2) if bids > 0 else 0.0,
        })
    dps.sort(key=lambda d: d["revenue"], reverse=True)
    return dps[:n]


def _mtd_pacing(date_rows: list, month_start: str, yesterday: str) -> dict:
    """Compute MTD revenue and project against the monthly target."""
    today_et   = datetime.now(ET).date()
    year       = today_et.year
    month      = today_et.month
    days_in_mo = calendar.monthrange(year, month)[1]

    # Days elapsed = from month start through yesterday (inclusive)
    start_date = date(year, month, 1)
    yest_date  = today_et - timedelta(days=1)
    days_elapsed = max(1, (yest_date - start_date).days + 1)

    mtd_rev = sum(
        _sf(r.get("GROSS_REVENUE"))
        for r in date_rows
        if month_start <= str(r.get("DATE", "")) <= yesterday
    )

    daily_run_rate  = mtd_rev / days_elapsed
    projected_month = daily_run_rate * days_in_mo
    pct_of_target   = projected_month / MONTHLY_TARGET * 100 if MONTHLY_TARGET > 0 else 0.0
    days_remaining  = days_in_mo - days_elapsed
    rev_needed_rest = max(0.0, MONTHLY_TARGET - mtd_rev)
    needed_per_day  = rev_needed_rest / days_remaining if days_remaining > 0 else 0.0

    return {
        "mtd_revenue":        round(mtd_rev, 2),
        "days_elapsed":       days_elapsed,
        "days_remaining":     days_remaining,
        "days_in_month":      days_in_mo,
        "daily_run_rate":     round(daily_run_rate, 2),
        "projected_monthly":  round(projected_month, 2),
        "monthly_target":     MONTHLY_TARGET,
        "pct_of_target":      round(pct_of_target, 1),
        "on_track":           projected_month >= MONTHLY_TARGET * 0.95,
        "revenue_needed_rest": round(rev_needed_rest, 2),
        "needed_per_day_rest": round(needed_per_day, 2),
    }


# ---------------------------------------------------------------------------
# Date range helpers
# ---------------------------------------------------------------------------

def _prev_week_range(now_et: datetime) -> tuple[str, str]:
    """Return (start, end) strings for Mon–Sun of the previous 7 days."""
    yest = (now_et.date() - timedelta(days=1))
    wk_start = yest - timedelta(days=6)
    return wk_start.strftime("%Y-%m-%d"), yest.strftime("%Y-%m-%d")


def _prior_week_range(now_et: datetime) -> tuple[str, str]:
    """Return (start, end) strings for the 7 days before the previous week."""
    yest       = now_et.date() - timedelta(days=1)
    prior_end  = yest - timedelta(days=7)
    prior_start = prior_end - timedelta(days=6)
    return prior_start.strftime("%Y-%m-%d"), prior_end.strftime("%Y-%m-%d")


def _month_start(now_et: datetime) -> str:
    return now_et.date().replace(day=1).strftime("%Y-%m-%d")


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def run():
    now_et  = datetime.now(ET)
    weekday = now_et.weekday()   # 0 = Monday
    hour_et = now_et.hour

    if weekday != 0:
        print(f"[weekly_review] Not Monday (weekday={weekday}). Exiting.")
        return

    if hour_et < MIN_HOUR_ET:
        print(f"[weekly_review] Too early ({hour_et:02d}:xx ET). Sends at {MIN_HOUR_ET:02d}:00. Exiting.")
        return

    (fetch, n_days_ago, today, sf, fmt_usd, fmt_n,
     send_text, already_sent_today, mark_sent,
     write_weekly_briefing) = _imports()

    if already_sent_today(ALERT_KEY):
        print("[weekly_review] Already sent this Monday. Exiting.")
        return

    # ── Date ranges ─────────────────────────────────────────────────────────
    wk_start,    wk_end    = _prev_week_range(now_et)
    prior_start, prior_end = _prior_week_range(now_et)
    mo_start               = _month_start(now_et)
    yesterday              = (now_et.date() - timedelta(days=1)).strftime("%Y-%m-%d")

    print(f"[weekly_review] This week: {wk_start} → {wk_end}")
    print(f"[weekly_review] Prior week: {prior_start} → {prior_end}")

    # ── Fetch data ───────────────────────────────────────────────────────────
    try:
        # 14 days of daily data covers both weeks in one call
        date_rows = fetch(BREAKDOWN_DATE, METRICS_FULL, prior_start, wk_end)
    except Exception as exc:
        print(f"[weekly_review] Date fetch failed: {exc}")
        return

    try:
        pub_rows = fetch(BREAKDOWN_PUB, METRICS_FULL, wk_start, wk_end)
    except Exception as exc:
        print(f"[weekly_review] Publisher fetch failed: {exc}")
        pub_rows = []

    try:
        dp_rows = fetch(BREAKDOWN_DP, METRICS_DP, wk_start, wk_end)
    except Exception as exc:
        print(f"[weekly_review] Demand partner fetch failed: {exc}")
        dp_rows = []

    # MTD fetch (may overlap with date_rows if early in month; easier to fetch separately)
    try:
        mtd_rows = fetch(BREAKDOWN_DATE, METRICS_FULL, mo_start, yesterday)
    except Exception as exc:
        print(f"[weekly_review] MTD fetch failed (non-fatal): {exc}")
        mtd_rows = date_rows  # fall back to what we have

    if not date_rows:
        print("[weekly_review] No daily data returned. Exiting.")
        return

    # ── Aggregate ────────────────────────────────────────────────────────────
    this_week  = _week_stats(date_rows, wk_start,    wk_end)
    prior_week = _week_stats(date_rows, prior_start, prior_end)
    top_pubs   = _top_publishers(pub_rows)
    top_dps    = _top_demand_partners(dp_rows)
    mtd        = _mtd_pacing(mtd_rows, mo_start, yesterday)

    if not this_week:
        print("[weekly_review] No data for this week window. Exiting.")
        return

    # Week-on-week change
    wow_rev = 0.0
    wow_pct = None
    if prior_week and prior_week.get("total_revenue", 0) > 0:
        wow_rev = this_week["total_revenue"] - prior_week["total_revenue"]
        wow_pct = wow_rev / prior_week["total_revenue"] * 100

    this_week["wow_revenue_change"] = round(wow_rev, 2)
    this_week["wow_pct_change"]     = round(wow_pct, 1) if wow_pct is not None else None

    print(
        f"[weekly_review] This week: ${this_week['total_revenue']:,.0f}  "
        f"WoW: {f'{wow_pct:+.1f}%' if wow_pct is not None else 'N/A'}  "
        f"MTD: ${mtd['mtd_revenue']:,.0f} ({mtd['pct_of_target']:.0f}% of target run-rate)"
    )

    # ── Ask Claude to write the briefing ─────────────────────────────────────
    try:
        message = write_weekly_briefing(
            this_week   = this_week,
            prior_week  = prior_week,
            top_pubs    = top_pubs,
            top_dps     = top_dps,
            mtd         = mtd,
            week_label  = f"{wk_start} – {wk_end}",
        )
        print("[weekly_review] Claude briefing generated.")
    except Exception as exc:
        print(f"[weekly_review] Claude failed: {exc}")
        # Fallback: plain-text summary without Claude
        wow_s = f"{wow_pct:+.1f}%" if wow_pct is not None else "N/A"
        message = (
            f"*Weekly Review — {wk_start} to {wk_end}*\n\n"
            f"Revenue: ${this_week['total_revenue']:,.0f}  ({wow_s} WoW)\n"
            f"Avg daily: ${this_week['avg_daily_revenue']:,.0f}  "
            f"Margin: {this_week['avg_margin_pct']:.1f}%\n"
            f"Best day: {this_week.get('best_day', {}).get('date', '—')} "
            f"(${this_week.get('best_day', {}).get('revenue', 0):,.0f})\n\n"
            f"MTD: ${mtd['mtd_revenue']:,.0f} / ${MONTHLY_TARGET:,.0f} target  "
            f"({mtd['pct_of_target']:.0f}% run-rate)\n\n"
            f"_Claude briefing unavailable: {exc}_"
        )

    # ── Post to Slack ─────────────────────────────────────────────────────────
    try:
        send_text(message)
        mark_sent(ALERT_KEY)
        print("[weekly_review] Slack message posted.")
    except Exception as exc:
        print(f"[weekly_review] Slack post failed: {exc}")


if __name__ == "__main__":
    run()
