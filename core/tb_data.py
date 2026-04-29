"""
core/tb_data.py
───────────────
Pure-data Teqblaze (TB) helpers for reports + analytics.

No Slack, no state, no side effects. Wraps core.api.fetch_tb with parsing
and aggregation shared between agents (tb_revenue.py for hourly Slack,
daily_email.py for daily report, future warehouse loaders).

TB API quirks
─────────────
  - Allows only one concurrent query per user — call sleep_between() between
    successive fetches in the same flow.
  - Rows use the same UPPER_CASE keys as LL (GROSS_REVENUE, PUB_PAYOUT, …).
"""

from __future__ import annotations

import time

from core.api import fetch_tb, tb_configured, sf, pct

BD_DATE      = "DATE"
BD_PUBLISHER = "PUBLISHER"
METRICS      = ["GROSS_REVENUE", "PUB_PAYOUT", "IMPRESSIONS", "WINS", "BIDS"]
INTER_CALL_SLEEP = 5  # seconds


def sleep_between() -> None:
    """Sleep between successive TB fetches to satisfy the one-query-per-user lock."""
    time.sleep(INTER_CALL_SLEEP)


def _f(row: dict, *keys) -> float:
    for k in keys:
        if k in row:
            return sf(row[k])
    return 0.0


def _sum(rows: list) -> dict:
    return {
        "revenue":     sum(_f(r, "GROSS_REVENUE", "gross_revenue", "grossRevenue") for r in rows),
        "payout":      sum(_f(r, "PUB_PAYOUT",    "pub_payout",    "pubPayout")    for r in rows),
        "impressions": sum(_f(r, "IMPRESSIONS",   "impressions")                   for r in rows),
        "wins":        sum(_f(r, "WINS",          "wins")                          for r in rows),
        "bids":        sum(_f(r, "BIDS",          "bids")                          for r in rows),
    }


def _derive(t: dict) -> dict:
    rev, pay, imp, wins, bids = t["revenue"], t["payout"], t["impressions"], t["wins"], t["bids"]
    return {
        **t,
        "margin":   pct(rev - pay, rev),
        "ecpm":     (rev / imp * 1000) if imp > 0 else 0.0,
        "win_rate": pct(wins, bids),
    }


# ---------------------------------------------------------------------------
# Public fetchers
# ---------------------------------------------------------------------------

def fetch_summary(start: str, end: str) -> dict:
    """Aggregated TB metrics for a date range. Returns {} if TB not configured or on error."""
    if not tb_configured():
        return {}
    try:
        rows = fetch_tb(BD_DATE, METRICS, start, end)
    except Exception as exc:
        print(f"[tb_data] summary fetch failed ({start}..{end}): {exc}")
        return {}
    if not rows:
        return _derive({"revenue": 0, "payout": 0, "impressions": 0, "wins": 0, "bids": 0})
    return _derive(_sum(rows))


def fetch_summary_by_day(start: str, end: str) -> dict:
    """
    Resilient multi-day TB fetch — issues one DATE call per day with sleeps.

    Slower than fetch_summary() but tolerates TB's long-range timeouts and
    partial failures: any individual day that fails just contributes 0,
    rather than aborting the whole window.
    """
    from datetime import datetime, timedelta
    if not tb_configured():
        return {}
    try:
        d0 = datetime.strptime(start, "%Y-%m-%d").date()
        d1 = datetime.strptime(end,   "%Y-%m-%d").date()
    except ValueError as exc:
        print(f"[tb_data] bad date range ({start}..{end}): {exc}")
        return {}

    totals = {"revenue": 0.0, "payout": 0.0, "impressions": 0.0, "wins": 0.0, "bids": 0.0}
    days_with_data = 0
    cur = d0
    first = True
    while cur <= d1:
        if not first:
            sleep_between()
        first = False
        s = cur.strftime("%Y-%m-%d")
        try:
            rows = fetch_tb(BD_DATE, METRICS, s, s)
            day = _sum(rows) if rows else None
            if day and day["revenue"] > 0:
                for k in totals:
                    totals[k] += day[k]
                days_with_data += 1
        except Exception as exc:
            print(f"[tb_data] day fetch failed ({s}): {exc}")
        cur += timedelta(days=1)

    derived = _derive(totals)
    derived["days_with_data"] = days_with_data
    return derived


def fetch_top_publishers(start: str, end: str, n: int = 20) -> list[dict]:
    if not tb_configured():
        return []
    try:
        rows = fetch_tb(BD_PUBLISHER, METRICS, start, end)
    except Exception as exc:
        print(f"[tb_data] publishers fetch failed ({start}..{end}): {exc}")
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


def fetch_pub_demand_combos(start: str, end: str, retries: int = 1) -> list[dict]:
    """
    TB 2-way breakdown: PUBLISHER × DEMAND_PARTNER.
    Returns one row per (publisher, demand) combo. TB doesn't expose bundle
    or domain breakdowns, so this is the finest granularity available.

    TB API is flaky on multi-dim breakdowns — retries once with extra backoff
    before giving up.
    """
    if not tb_configured():
        return []
    rows = None
    for attempt in range(retries + 1):
        if attempt > 0:
            time.sleep(INTER_CALL_SLEEP * 3)  # extra cool-down before retry
            print(f"[tb_data] retrying pub×demand ({start}..{end}), attempt {attempt + 1}…")
        try:
            rows = fetch_tb("PUBLISHER,DEMAND_PARTNER", METRICS, start, end)
            break
        except Exception as exc:
            print(f"[tb_data] pub×demand fetch failed ({start}..{end}): {exc}")
            rows = None
    if not rows:
        return []
    out = []
    for r in rows:
        pub  = (r.get("PUBLISHER_NAME") or r.get("PUBLISHER") or r.get("publisher") or "Unknown").strip()
        dem  = (r.get("DEMAND_PARTNER_NAME") or r.get("DEMAND_PARTNER") or r.get("demand_partner") or "Unknown").strip()
        rev  = _f(r, "GROSS_REVENUE", "gross_revenue", "grossRevenue")
        pay  = _f(r, "PUB_PAYOUT",    "pub_payout",    "pubPayout")
        imp  = _f(r, "IMPRESSIONS",   "impressions")
        wins = _f(r, "WINS",          "wins")
        bids = _f(r, "BIDS",          "bids")
        if rev <= 0:
            continue
        out.append({
            "publisher":   pub,
            "demand":      dem,
            "revenue":     rev,
            "payout":      pay,
            "impressions": imp,
            "ecpm":        (rev / imp * 1000) if imp > 0 else 0.0,
            "margin":      pct(rev - pay, rev),
            "win_rate":    pct(wins, bids),
        })
    return out


def fetch_by_country(start: str, end: str, n: int = 20, retries: int = 1) -> list[dict]:
    """TB COUNTRY_NAME breakdown — sorted by revenue. Retries once on timeout."""
    if not tb_configured():
        return []
    rows = None
    for attempt in range(retries + 1):
        if attempt > 0:
            time.sleep(INTER_CALL_SLEEP * 3)
            print(f"[tb_data] retrying country ({start}..{end}), attempt {attempt + 1}…")
        try:
            rows = fetch_tb("COUNTRY_NAME", METRICS, start, end)
            break
        except Exception as exc:
            print(f"[tb_data] country fetch failed ({start}..{end}): {exc}")
            rows = None
    if not rows:
        return []
    out = []
    for r in rows:
        country = (r.get("COUNTRY") or r.get("country") or r.get("COUNTRY_NAME") or "Unknown").strip()
        rev  = _f(r, "GROSS_REVENUE", "gross_revenue", "grossRevenue")
        pay  = _f(r, "PUB_PAYOUT",    "pub_payout",    "pubPayout")
        imp  = _f(r, "IMPRESSIONS",   "impressions")
        wins = _f(r, "WINS",          "wins")
        bids = _f(r, "BIDS",          "bids")
        if rev <= 0 and imp <= 0:
            continue
        out.append({
            "country":     country,
            "revenue":     rev,
            "payout":      pay,
            "impressions": imp,
            "ecpm":        (rev / imp * 1000) if imp > 0 else 0.0,
            "margin":      pct(rev - pay, rev),
            "win_rate":    pct(wins, bids),
        })
    out.sort(key=lambda x: x["revenue"], reverse=True)
    return out[:n]


def fetch_by_demand_partner(start: str, end: str, n: int = 30, retries: int = 1) -> list[dict]:
    """TB DEMAND_PARTNER breakdown — for partner profitability ranking. Retries once."""
    if not tb_configured():
        return []
    rows = None
    for attempt in range(retries + 1):
        if attempt > 0:
            time.sleep(INTER_CALL_SLEEP * 3)
            print(f"[tb_data] retrying demand_partner ({start}..{end}), attempt {attempt + 1}…")
        try:
            rows = fetch_tb("DEMAND_PARTNER", METRICS, start, end)
            break
        except Exception as exc:
            print(f"[tb_data] demand_partner fetch failed ({start}..{end}): {exc}")
            rows = None
    if not rows:
        return []
    out = []
    for r in rows:
        name = (r.get("DEMAND_PARTNER_NAME") or r.get("DEMAND_PARTNER") or
                r.get("demand_partner") or "Unknown").strip()
        rev  = _f(r, "GROSS_REVENUE", "gross_revenue", "grossRevenue")
        pay  = _f(r, "PUB_PAYOUT",    "pub_payout",    "pubPayout")
        imp  = _f(r, "IMPRESSIONS",   "impressions")
        wins = _f(r, "WINS",          "wins")
        bids = _f(r, "BIDS",          "bids")
        if rev <= 0:
            continue
        out.append({
            "demand":      name,
            "revenue":     rev,
            "payout":      pay,
            "impressions": imp,
            "ecpm":        (rev / imp * 1000) if imp > 0 else 0.0,
            "margin":      pct(rev - pay, rev),
            "win_rate":    pct(wins, bids),
        })
    out.sort(key=lambda x: x["revenue"], reverse=True)
    return out[:n]


def avg_per_day(summary: dict, n_days: int) -> dict:
    """
    Scale a multi-day summary into a daily-average summary.

    If `summary` carries a `days_with_data` field (set by fetch_summary_by_day),
    that's used in preference to `n_days` so platforms with shorter history
    don't get under-counted.
    """
    if not summary or n_days <= 0:
        return {}
    divisor = summary.get("days_with_data") or n_days
    if divisor <= 0:
        return {}
    scaled = {
        "revenue":     summary.get("revenue", 0)     / divisor,
        "payout":      summary.get("payout", 0)      / divisor,
        "impressions": summary.get("impressions", 0) / divisor,
        "wins":        summary.get("wins", 0)        / divisor,
        "bids":        summary.get("bids", 0)        / divisor,
    }
    out = _derive(scaled)
    out["days_with_data"] = divisor
    return out
