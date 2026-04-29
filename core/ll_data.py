"""
core/ll_data.py
───────────────
Pure-data Limelight (LL) helpers for reports + analytics.

No Slack, no state, no side effects. Wraps core.api.fetch with parsing
and aggregation that's shared between agents (ll_revenue.py for hourly
Slack, daily_email.py for daily report, future warehouse loaders).

Public surface:
    fetch_summary(start, end)         -> dict      (totals + derived metrics)
    fetch_top_publishers(start, end)  -> list[dict]
    avg_per_day(summary, n_days)      -> dict      (scale totals → daily avg)
"""

from __future__ import annotations

from core.api import fetch, sf, pct

BD_DATE      = "DATE"
BD_PUBLISHER = "PUBLISHER"
METRICS      = ["GROSS_REVENUE", "PUB_PAYOUT", "IMPRESSIONS", "WINS", "BIDS", "OPPORTUNITIES"]


def _f(row: dict, *keys) -> float:
    for k in keys:
        if k in row:
            return sf(row[k])
    return 0.0


def _sum(rows: list) -> dict:
    return {
        "revenue":       sum(_f(r, "GROSS_REVENUE",  "gross_revenue",  "grossRevenue")  for r in rows),
        "payout":        sum(_f(r, "PUB_PAYOUT",     "pub_payout",     "pubPayout")     for r in rows),
        "impressions":   sum(_f(r, "IMPRESSIONS",    "impressions")                     for r in rows),
        "wins":          sum(_f(r, "WINS",           "wins")                            for r in rows),
        "bids":          sum(_f(r, "BIDS",           "bids")                            for r in rows),
        "opportunities": sum(_f(r, "OPPORTUNITIES",  "opportunities")                   for r in rows),
    }


def _derive(t: dict) -> dict:
    rev, pay = t["revenue"], t["payout"]
    imp, wins, bids, opp = t["impressions"], t["wins"], t["bids"], t["opportunities"]
    return {
        **t,
        "margin":    pct(rev - pay, rev),
        "ecpm":      (rev / imp * 1000) if imp > 0 else 0.0,
        "win_rate":  pct(wins, bids),
        "fill_rate": pct(imp, opp),
    }


# ---------------------------------------------------------------------------
# Public fetchers
# ---------------------------------------------------------------------------

def fetch_summary(start: str, end: str) -> dict:
    """Aggregated LL metrics for a date range. Returns {} on fetch failure."""
    try:
        rows = fetch(BD_DATE, METRICS, start, end)
    except Exception as exc:
        print(f"[ll_data] summary fetch failed ({start}..{end}): {exc}")
        return {}
    if not rows:
        return _derive({"revenue": 0, "payout": 0, "impressions": 0,
                        "wins": 0, "bids": 0, "opportunities": 0})
    return _derive(_sum(rows))


def fetch_top_publishers(start: str, end: str, n: int = 20) -> list[dict]:
    try:
        rows = fetch(BD_PUBLISHER, METRICS, start, end)
    except Exception as exc:
        print(f"[ll_data] publishers fetch failed ({start}..{end}): {exc}")
        return []
    pubs = []
    for r in rows:
        name = (r.get("PUBLISHER_NAME") or r.get("PUBLISHER") or r.get("publisher")
                or r.get("pubName") or r.get("pub_name") or "Unknown")
        rev  = _f(r, "GROSS_REVENUE", "gross_revenue", "grossRevenue")
        pay  = _f(r, "PUB_PAYOUT",    "pub_payout",    "pubPayout")
        imp  = _f(r, "IMPRESSIONS",   "impressions")
        wins = _f(r, "WINS",          "wins")
        bids = _f(r, "BIDS",          "bids")
        pubs.append({
            "name":        str(name),
            "revenue":     rev,
            "payout":      pay,
            "impressions": imp,
            "ecpm":        (rev / imp * 1000) if imp > 0 else 0.0,
            "margin":      pct(rev - pay, rev),
            "win_rate":    pct(wins, bids),
        })
    pubs.sort(key=lambda x: x["revenue"], reverse=True)
    return pubs[:n]


def avg_per_day(summary: dict, n_days: int) -> dict:
    """Scale a multi-day summary into a daily-average summary. Margin/eCPM unchanged."""
    if not summary or n_days <= 0:
        return {}
    scaled = {
        "revenue":       summary.get("revenue", 0)       / n_days,
        "payout":        summary.get("payout", 0)        / n_days,
        "impressions":   summary.get("impressions", 0)   / n_days,
        "wins":          summary.get("wins", 0)          / n_days,
        "bids":          summary.get("bids", 0)          / n_days,
        "opportunities": summary.get("opportunities", 0) / n_days,
    }
    return _derive(scaled)
