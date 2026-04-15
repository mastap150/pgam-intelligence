"""
agents/alerts/ctv_optimizer.py

CTV inventory optimiser — surfaces scale opportunities for Connected TV supply.

Context
-------
CTV is the highest-eCPM format in the network (~$1.94) but represents only
~5.9% of total impressions. The gap between premium pricing and low volume is
the primary growth lever.

Data fetching strategy
----------------------
The core fetch() function supports comma-separated multi-dimensional breakdowns.
We make three API calls:

  1. AD_UNIT_TYPE              — 14-day aggregate by unit type (network context)
  2. PUBLISHER,AD_UNIT_TYPE    — 14-day per-publisher × unit-type (CTV publishers)
  3. DATE,AD_UNIT_TYPE         — 14-day daily × unit-type (trend line)

All rows are filtered client-side to CTV using a set of known identifiers.
If a 4th call for DATE,PUBLISHER,AD_UNIT_TYPE is supported by the API, per-
publisher daily trends are also computed; otherwise this step is skipped
gracefully.

Scale opportunity scoring
-------------------------
For each CTV publisher:
    fill_gap       = network_avg_fill - publisher_fill_rate   (clamped ≥ 0)
    opportunity    = fill_gap × publisher_opportunities_daily × ecpm / 1000

This answers: "how much revenue would this publisher generate if it reached
network-average fill rate?" Publishers with high eCPM and large fill gaps score
highest.

Revenue projections
-------------------
Based on network-wide avg daily CTV revenue:
    tier_10:  current_daily × 1.10
    tier_25:  current_daily × 1.25
    tier_50:  current_daily × 1.50
    annualised by × 365

Posts
-----
  Weekly Monday Slack post    (ALERT_KEY deduped via core/slack.py)
  export_ctv_section()        importable by daily email agent
"""

import statistics
from datetime import date, datetime, timedelta
from collections import defaultdict

import pytz

from core.api import fetch, n_days_ago, today, sf, fmt_usd, fmt_n, pct
from core.config import THRESHOLDS
from core.slack import already_sent_today, mark_sent, send_blocks
from intelligence.claude_analyst import analyze_ctv_opportunity

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
BREAKDOWN_UNIT      = "AD_UNIT_TYPE"
BREAKDOWN_PUB_UNIT  = "PUBLISHER,AD_UNIT_TYPE"
BREAKDOWN_DATE_UNIT = "DATE,AD_UNIT_TYPE"
BREAKDOWN_DATE_PUB_UNIT = "DATE,PUBLISHER,AD_UNIT_TYPE"   # optional 3D breakdown

METRICS             = [
    "GROSS_REVENUE", "IMPRESSIONS", "OPPORTUNITIES",
    "WINS", "BIDS", "AVG_FLOOR_PRICE", "AVG_BID_PRICE", "GROSS_ECPM",
]
LOOKBACK_DAYS       = 14
ALERT_KEY           = "ctv_optimizer_weekly"
ET                  = pytz.timezone("US/Eastern")

# CTV identifier variants (API may return any of these)
CTV_IDENTIFIERS = {
    "ctv", "connected tv", "connected_tv", "connectedtv",
    "ott", "over-the-top", "streaming", "tv",
}

TOP_SCALE_PUBS      = 10    # publishers to show in Slack / send to Claude
MIN_PUB_REVENUE     = 10.0  # minimum 14d revenue to be included


# ---------------------------------------------------------------------------
# Row field extraction helpers
# ---------------------------------------------------------------------------

def _extract(row: dict, *keys) -> float:
    for k in keys:
        if k in row:
            return sf(row[k])
    return 0.0


def _unit_type(row: dict) -> str:
    return str(
        row.get("AD_UNIT_TYPE") or row.get("ad_unit_type")
        or row.get("adUnitType") or row.get("unit_type") or ""
    ).strip()


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


def _is_ctv(row: dict) -> bool:
    return _unit_type(row).lower() in CTV_IDENTIFIERS


# ---------------------------------------------------------------------------
# Metric calculations
# ---------------------------------------------------------------------------

def _calc_metrics(revenue, impressions, opportunities, wins, bids,
                  avg_floor, avg_bid) -> dict:
    fill_rate   = (impressions / opportunities) if opportunities > 0 else 0.0
    win_rate    = (wins / bids)                 if bids > 0         else 0.0
    floor_ratio = (avg_floor / avg_bid)         if avg_bid > 0      else 0.0
    ecpm        = (revenue / impressions * 1_000) if impressions > 0 else 0.0
    return {
        "fill_rate":   fill_rate,
        "win_rate":    win_rate,
        "floor_ratio": floor_ratio,
        "ecpm":        ecpm,
    }


# ---------------------------------------------------------------------------
# Data aggregation helpers
# ---------------------------------------------------------------------------

def _aggregate_ctv_rows(rows: list) -> dict:
    """Sum all CTV rows into one aggregate dict."""
    totals = {
        "revenue": 0.0, "impressions": 0.0, "opportunities": 0.0,
        "wins": 0.0, "bids": 0.0,
        "floor_sum": 0.0, "bid_sum": 0.0, "ecpm_sum": 0.0, "n": 0,
    }
    for row in rows:
        if not _is_ctv(row):
            continue
        totals["revenue"]      += _extract(row, "GROSS_REVENUE",    "gross_revenue")
        totals["impressions"]  += _extract(row, "IMPRESSIONS",      "impressions")
        totals["opportunities"]+= _extract(row, "OPPORTUNITIES",    "opportunities")
        totals["wins"]         += _extract(row, "WINS",             "wins")
        totals["bids"]         += _extract(row, "BIDS",             "bids")
        totals["floor_sum"]    += _extract(row, "AVG_FLOOR_PRICE",  "avg_floor_price", "avgFloorPrice")
        totals["bid_sum"]      += _extract(row, "AVG_BID_PRICE",    "avg_bid_price",   "avgBidPrice")
        totals["ecpm_sum"]     += _extract(row, "GROSS_ECPM",       "gross_ecpm",      "ecpm")
        totals["n"]            += 1
    return totals


def _build_ctv_summary(
    ctv_rows: list,
    all_rows: list,
    lookback_days: int,
) -> dict:
    """Build an overall CTV summary dict including network share."""
    ctv = _aggregate_ctv_rows(ctv_rows if ctv_rows else all_rows)
    if ctv["n"] == 0:
        return {}

    # Network total (all unit types)
    net_revenue = sum(_extract(r, "GROSS_REVENUE", "gross_revenue") for r in all_rows)

    metrics = _calc_metrics(
        revenue=ctv["revenue"],
        impressions=ctv["impressions"],
        opportunities=ctv["opportunities"],
        wins=ctv["wins"],
        bids=ctv["bids"],
        avg_floor=ctv["floor_sum"] / ctv["n"] if ctv["n"] else 0,
        avg_bid=ctv["bid_sum"]   / ctv["n"] if ctv["n"] else 0,
    )
    avg_ecpm = (ctv["ecpm_sum"] / ctv["n"]) if ctv["n"] else metrics["ecpm"]

    return {
        "total_revenue_14d":  round(ctv["revenue"], 2),
        "avg_daily_revenue":  round(ctv["revenue"] / lookback_days, 2),
        "avg_ecpm":           round(avg_ecpm, 4),
        "fill_rate":          round(metrics["fill_rate"], 6),
        "win_rate":           round(metrics["win_rate"], 6),
        "floor_ratio":        round(metrics["floor_ratio"], 4),
        "pct_of_network":     round(pct(ctv["revenue"], net_revenue), 2),
        "total_impressions":  int(ctv["impressions"]),
        "total_opportunities":int(ctv["opportunities"]),
        "total_bids":         int(ctv["bids"]),
    }


def _build_ctv_publishers(pub_unit_rows: list, lookback_days: int) -> list[dict]:
    """
    Build per-publisher CTV stats from PUBLISHER,AD_UNIT_TYPE breakdown rows.
    Returns list sorted by revenue descending.
    """
    by_pub: dict[str, dict] = defaultdict(lambda: {
        "revenue": 0.0, "impressions": 0.0, "opportunities": 0.0,
        "wins": 0.0, "bids": 0.0,
        "floor_vals": [], "bid_vals": [], "ecpm_vals": [],
    })

    for row in pub_unit_rows:
        if not _is_ctv(row):
            continue
        pub = _pub_name(row)
        rec = by_pub[pub]
        rec["revenue"]       += _extract(row, "GROSS_REVENUE",   "gross_revenue")
        rec["impressions"]   += _extract(row, "IMPRESSIONS",     "impressions")
        rec["opportunities"] += _extract(row, "OPPORTUNITIES",   "opportunities")
        rec["wins"]          += _extract(row, "WINS",            "wins")
        rec["bids"]          += _extract(row, "BIDS",            "bids")
        f = _extract(row, "AVG_FLOOR_PRICE", "avg_floor_price", "avgFloorPrice")
        b = _extract(row, "AVG_BID_PRICE",   "avg_bid_price",   "avgBidPrice")
        e = _extract(row, "GROSS_ECPM",      "gross_ecpm",      "ecpm")
        if f > 0: rec["floor_vals"].append(f)
        if b > 0: rec["bid_vals"].append(b)
        if e > 0: rec["ecpm_vals"].append(e)

    results = []
    for pub, rec in by_pub.items():
        if rec["revenue"] < MIN_PUB_REVENUE:
            continue
        avg_floor = statistics.mean(rec["floor_vals"]) if rec["floor_vals"] else 0.0
        avg_bid   = statistics.mean(rec["bid_vals"])   if rec["bid_vals"]   else 0.0
        avg_ecpm  = statistics.mean(rec["ecpm_vals"])  if rec["ecpm_vals"]  else (
            rec["revenue"] / rec["impressions"] * 1_000 if rec["impressions"] > 0 else 0.0
        )
        m = _calc_metrics(
            revenue=rec["revenue"], impressions=rec["impressions"],
            opportunities=rec["opportunities"], wins=rec["wins"],
            bids=rec["bids"], avg_floor=avg_floor, avg_bid=avg_bid,
        )
        results.append({
            "publisher":         pub,
            "revenue_14d":       round(rec["revenue"], 2),
            "avg_daily_revenue": round(rec["revenue"] / lookback_days, 2),
            "impressions_14d":   int(rec["impressions"]),
            "opportunities_14d": int(rec["opportunities"]),
            "ecpm":              round(avg_ecpm, 4),
            "fill_rate":         round(m["fill_rate"], 6),
            "win_rate":          round(m["win_rate"], 6),
            "floor_ratio":       round(m["floor_ratio"], 4),
            "avg_floor":         round(avg_floor, 4),
            "avg_bid":           round(avg_bid, 4),
        })

    results.sort(key=lambda p: p["revenue_14d"], reverse=True)
    return results


def _build_daily_ctv_trend(date_unit_rows: list) -> list[dict]:
    """
    Build a daily time series from DATE,AD_UNIT_TYPE rows filtered to CTV.
    Returns list of {date, revenue, ecpm, fill_rate} sorted by date.
    """
    by_date: dict[str, dict] = defaultdict(lambda: {
        "revenue": 0.0, "impressions": 0.0, "opportunities": 0.0,
        "wins": 0.0, "bids": 0.0, "ecpm_sum": 0.0, "n": 0,
    })
    for row in date_unit_rows:
        if not _is_ctv(row):
            continue
        dt  = _row_date(row)
        rec = by_date[dt]
        rec["revenue"]       += _extract(row, "GROSS_REVENUE",  "gross_revenue")
        rec["impressions"]   += _extract(row, "IMPRESSIONS",    "impressions")
        rec["opportunities"] += _extract(row, "OPPORTUNITIES",  "opportunities")
        rec["wins"]          += _extract(row, "WINS",           "wins")
        rec["bids"]          += _extract(row, "BIDS",           "bids")
        e = _extract(row, "GROSS_ECPM", "gross_ecpm", "ecpm")
        if e > 0:
            rec["ecpm_sum"] += e
            rec["n"] += 1

    trend = []
    for dt in sorted(by_date.keys()):
        rec  = by_date[dt]
        imps = rec["impressions"]
        opps = rec["opportunities"]
        trend.append({
            "date":      dt,
            "revenue":   round(rec["revenue"], 2),
            "ecpm":      round(rec["ecpm_sum"] / rec["n"] if rec["n"] else (rec["revenue"] / imps * 1_000 if imps else 0), 4),
            "fill_rate": round(imps / opps if opps > 0 else 0, 6),
        })
    return trend


def _build_pub_daily_trends(date_pub_unit_rows: list) -> dict[str, list[float]]:
    """
    Build per-publisher 14-day daily revenue from DATE,PUBLISHER,AD_UNIT_TYPE rows.
    Returns {publisher: [day1_rev, day2_rev, ...]} sorted by date.
    """
    by_pub_date: dict[str, dict[str, float]] = defaultdict(dict)
    for row in date_pub_unit_rows:
        if not _is_ctv(row):
            continue
        pub = _pub_name(row)
        dt  = _row_date(row)
        rev = _extract(row, "GROSS_REVENUE", "gross_revenue")
        by_pub_date[pub][dt] = by_pub_date[pub].get(dt, 0.0) + rev

    result: dict[str, list[float]] = {}
    for pub, date_rev in by_pub_date.items():
        result[pub] = [date_rev[dt] for dt in sorted(date_rev.keys())]
    return result


# ---------------------------------------------------------------------------
# Scale opportunity scoring
# ---------------------------------------------------------------------------

def _score_scale_opportunities(
    publishers: list[dict],
    network_fill: float,
    network_ecpm: float,
) -> list[dict]:
    """
    Rank publishers by how much revenue they would gain by closing the fill gap
    to network average.

    opportunity = fill_gap × daily_opportunities × ecpm / 1000
    """
    scored = []
    for pub in publishers:
        fill_gap = max(0.0, network_fill - pub["fill_rate"])
        daily_opps = pub["opportunities_14d"] / 14.0
        ecpm = pub["ecpm"] if pub["ecpm"] > 0 else network_ecpm

        # Estimated additional daily revenue at network-avg fill
        opportunity = fill_gap * daily_opps * ecpm / 1_000

        scored.append({
            **pub,
            "fill_gap":        round(fill_gap, 6),
            "opportunity_score": round(opportunity, 2),
            "is_high_ecpm":    pub["ecpm"] >= network_ecpm,
        })

    scored.sort(key=lambda p: p["opportunity_score"], reverse=True)
    return scored


# ---------------------------------------------------------------------------
# Revenue projections
# ---------------------------------------------------------------------------

def _build_projections(avg_daily_revenue: float) -> dict:
    return {
        "current_daily":  round(avg_daily_revenue, 2),
        "tier_10_daily":  round(avg_daily_revenue * 1.10, 2),
        "tier_25_daily":  round(avg_daily_revenue * 1.25, 2),
        "tier_50_daily":  round(avg_daily_revenue * 1.50, 2),
        "tier_10_annual": round(avg_daily_revenue * 1.10 * 365, 2),
        "tier_25_annual": round(avg_daily_revenue * 1.25 * 365, 2),
        "tier_50_annual": round(avg_daily_revenue * 1.50 * 365, 2),
    }


# ---------------------------------------------------------------------------
# Trend direction helper
# ---------------------------------------------------------------------------

def _trend_direction(daily_revs: list[float]) -> str:
    """Return 'up', 'down', or 'flat' based on simple half-split comparison."""
    if len(daily_revs) < 4:
        return "flat"
    mid   = len(daily_revs) // 2
    first = statistics.mean(daily_revs[:mid])
    last  = statistics.mean(daily_revs[mid:])
    if first == 0:
        return "flat"
    change = (last - first) / first
    if change >  0.05:
        return "up"
    if change < -0.05:
        return "down"
    return "flat"


def _sparkline(vals: list[float], width: int = 10) -> str:
    recent = vals[-width:] if len(vals) >= width else vals
    if not recent or max(recent) == 0:
        return "░" * len(recent)
    mx   = max(recent)
    bars = "▁▂▃▄▅▆▇█"
    return "".join(bars[min(int(v / mx * 7), 7)] for v in recent)


# ---------------------------------------------------------------------------
# Public export for daily email
# ---------------------------------------------------------------------------

def export_ctv_section(top_n: int = 5) -> dict:
    """
    Run the CTV analysis and return a structured dict suitable for embedding
    in a daily email report. Returns {} on failure.

    Importable by agents/reports/daily_email.py.
    """
    start = n_days_ago(LOOKBACK_DAYS)
    end   = today()

    try:
        unit_rows     = fetch(BREAKDOWN_UNIT,     METRICS, start, end)
        pub_unit_rows = fetch(BREAKDOWN_PUB_UNIT, METRICS, start, end)
    except Exception as exc:
        print(f"[ctv_optimizer/export] Fetch failed: {exc}")
        return {}

    summary    = _build_ctv_summary(unit_rows, unit_rows, LOOKBACK_DAYS)
    publishers = _build_ctv_publishers(pub_unit_rows, LOOKBACK_DAYS)
    if not summary or not publishers:
        return {}

    net_fill   = summary.get("fill_rate", 0.0)
    net_ecpm   = summary.get("avg_ecpm", 0.0)
    scored     = _score_scale_opportunities(publishers, net_fill, net_ecpm)
    projections= _build_projections(summary["avg_daily_revenue"])

    return {
        "summary":          summary,
        "top_publishers":   scored[:top_n],
        "projections":      projections,
        "n_publishers":     len(publishers),
    }


# ---------------------------------------------------------------------------
# Slack Block Kit builders
# ---------------------------------------------------------------------------

def _trend_emoji(direction: str) -> str:
    return {"up": ":chart_with_upwards_trend:", "down": ":chart_with_downwards_trend:",
            "flat": ":arrow_right:"}.get(direction, ":arrow_right:")


def _fill_bar(fill_rate: float, network_fill: float, width: int = 10) -> str:
    target_pct = min(fill_rate / network_fill, 1.0) if network_fill > 0 else 0.0
    filled = max(0, min(int(target_pct * width), width))
    return "█" * filled + "░" * (width - filled)


def _build_slack_blocks(
    summary: dict,
    scored_pubs: list[dict],
    trend: list[dict],
    pub_trends: dict[str, list[float]],
    projections: dict,
    claude_result: dict,
    date_label: str,
    now_label: str,
) -> list:
    net_fill    = summary.get("fill_rate", 0.0)
    net_ecpm    = summary.get("avg_ecpm", 0.0)
    trend_vals  = [d["revenue"] for d in trend]
    net_trend   = _trend_direction(trend_vals)
    spark_14d   = _sparkline(trend_vals, width=14)

    # Business case narrative from Claude
    biz_case   = claude_result.get("business_case", "")
    top_action = claude_result.get("top_action", "")
    proj_narr  = claude_result.get("projections_narrative", "")
    priority_pubs = claude_result.get("priority_publishers", [])

    blocks = [
        {
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": f":tv:  CTV Optimizer — {date_label}",
                "emoji": True,
            },
        },
        # ── Network CTV summary ─────────────────────────────────────────────
        {
            "type": "section",
            "fields": [
                {"type": "mrkdwn", "text": f"*Avg eCPM (14d):*\n{fmt_usd(net_ecpm)}"},
                {"type": "mrkdwn", "text": f"*Avg Daily Revenue:*\n{fmt_usd(summary['avg_daily_revenue'])}"},
                {"type": "mrkdwn", "text": f"*Network Share:*\n{summary['pct_of_network']:.1f}% of total"},
                {"type": "mrkdwn", "text": f"*Fill Rate:*\n{net_fill*100:.4f}%"},
                {"type": "mrkdwn", "text": f"*Win Rate:*\n{summary['win_rate']*100:.2f}%"},
                {"type": "mrkdwn", "text": f"*CTV Publishers:*\n{summary.get('total_publishers', len(scored_pubs))}"},
            ],
        },
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": (
                    f"{_trend_emoji(net_trend)} *14-day revenue trend*\n"
                    f"`{spark_14d}`  {net_trend.upper()}"
                ),
            },
        },
        {"type": "divider"},
        # ── Revenue projections ─────────────────────────────────────────────
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": (
                    f":moneybag: *Revenue Growth Projections*\n"
                    f"  10% volume increase → *{fmt_usd(projections['tier_10_daily'])}/day*  "
                    f"({fmt_usd(projections['tier_10_annual'])}/yr)\n"
                    f"  25% volume increase → *{fmt_usd(projections['tier_25_daily'])}/day*  "
                    f"({fmt_usd(projections['tier_25_annual'])}/yr)\n"
                    f"  50% volume increase → *{fmt_usd(projections['tier_50_daily'])}/day*  "
                    f"({fmt_usd(projections['tier_50_annual'])}/yr)"
                ),
            },
        },
        {"type": "divider"},
        # ── Claude business case ────────────────────────────────────────────
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f":robot_face: *Business Case*\n{biz_case}",
            },
        },
    ]

    if top_action:
        blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f":dart: *Top Action This Week*\n{top_action}",
            },
        })

    if proj_narr:
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": f":bar_chart: _{proj_narr}_"},
        })

    blocks.append({"type": "divider"})

    # ── Priority publishers from Claude ─────────────────────────────────────
    if priority_pubs:
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": ":telescope: *Claude's Priority Publishers*"},
        })
        for i, pp in enumerate(priority_pubs, 1):
            rank_e = [":one:", ":two:", ":three:"][i - 1] if i <= 3 else f"*{i}.*"
            blocks.append({
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": (
                        f"{rank_e} *{pp['publisher']}*\n"
                        f"  :speech_balloon: _{pp.get('rationale', '')}_\n"
                        f"  :wrench: *{pp.get('approach', '')}*"
                    ),
                },
            })

    blocks.append({"type": "divider"})

    # ── Scale opportunity publishers ─────────────────────────────────────────
    blocks.append({
        "type": "section",
        "text": {
            "type": "mrkdwn",
            "text": (
                f":seedling: *Top {min(len(scored_pubs), TOP_SCALE_PUBS)} Scale Opportunities*  "
                f"_(high eCPM, fill gap vs network average)_"
            ),
        },
    })

    for pub in scored_pubs[:TOP_SCALE_PUBS]:
        daily_revs = pub_trends.get(pub["publisher"], [])
        trend_dir  = _trend_direction(daily_revs)
        spark      = _sparkline(daily_revs) if daily_revs else "no daily data"
        fill_bar   = _fill_bar(pub["fill_rate"], net_fill)
        ecpm_flag  = ":star:" if pub["is_high_ecpm"] else ""

        blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": (
                    f"*{pub['publisher']}* {ecpm_flag}  "
                    f"{_trend_emoji(trend_dir)}\n"
                    f"  eCPM: *{fmt_usd(pub['ecpm'])}*  |  "
                    f"fill: `{fill_bar}` {pub['fill_rate']*100:.4f}%  |  "
                    f"fill gap: {pub['fill_gap']*100:.4f}%\n"
                    f"  14d rev: {fmt_usd(pub['revenue_14d'])}  |  "
                    f"floor/bid: {pub['floor_ratio']:.2f}×  |  "
                    f"win rate: {pub['win_rate']*100:.2f}%  |  "
                    f"opp/day: {fmt_n(pub['opportunities_14d']//14)}\n"
                    f"  :moneybag: opp at avg fill: *+{fmt_usd(pub['opportunity_score'])}/day*  "
                    f"| `{spark if daily_revs else '░░░░░░░░░░'}`"
                ),
            },
        })

    blocks.append({"type": "divider"})
    blocks.append({
        "type": "context",
        "elements": [
            {
                "type": "mrkdwn",
                "text": (
                    f"Opportunity = fill_gap × daily_opps × eCPM/1000  |  "
                    f"CTV identifiers: {', '.join(sorted(CTV_IDENTIFIERS))}  |  "
                    f"PGAM Intelligence · CTV Optimizer · {now_label}"
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
    Execute the CTV optimiser weekly report. Designed to be scheduled on
    Mondays, or run directly: `python -m agents.alerts.ctv_optimizer`.
    """
    now_et     = datetime.now(ET)
    date_label = now_et.strftime("%A, %B %-d")
    now_label  = now_et.strftime("%H:%M ET")

    # ── 1. Monday gate ────────────────────────────────────────────────────────
    if now_et.weekday() != 0:
        print(
            f"[ctv_optimizer] Skipping — today is {now_et.strftime('%A')}, "
            f"this report runs on Mondays."
        )
        return

    # ── 2. Dedup ──────────────────────────────────────────────────────────────
    if already_sent_today(ALERT_KEY):
        print("[ctv_optimizer] Weekly report already sent today — skipping.")
        return

    start = n_days_ago(LOOKBACK_DAYS)
    end   = today()

    # ── 3. Fetch data (3 API calls, 4th optional) ─────────────────────────────
    print(f"[ctv_optimizer] Fetching CTV data {start} → {end}…")
    try:
        unit_rows     = fetch(BREAKDOWN_UNIT,     METRICS, start, end)
        pub_unit_rows = fetch(BREAKDOWN_PUB_UNIT, METRICS, start, end)
        date_unit_rows= fetch(BREAKDOWN_DATE_UNIT,METRICS, start, end)
    except Exception as exc:
        print(f"[ctv_optimizer] Core fetch failed: {exc}")
        return

    # Optional: per-publisher daily trends (graceful fallback if API rejects 3D breakdown)
    pub_daily_trends: dict[str, list[float]] = {}
    try:
        date_pub_unit_rows = fetch(BREAKDOWN_DATE_PUB_UNIT, METRICS, start, end)
        pub_daily_trends   = _build_pub_daily_trends(date_pub_unit_rows)
        print(f"[ctv_optimizer] Per-publisher daily trends available for {len(pub_daily_trends)} publishers.")
    except Exception:
        print("[ctv_optimizer] 3D breakdown not supported — publisher trends unavailable.")

    # ── 4. Aggregate and compute ──────────────────────────────────────────────
    summary    = _build_ctv_summary(unit_rows, unit_rows, LOOKBACK_DAYS)
    publishers = _build_ctv_publishers(pub_unit_rows, LOOKBACK_DAYS)
    trend      = _build_daily_ctv_trend(date_unit_rows)

    if not summary:
        print("[ctv_optimizer] No CTV data found in API response — check unit type identifiers.")
        mark_sent(ALERT_KEY)
        return

    summary["total_publishers"] = len(publishers)
    net_fill    = summary["fill_rate"]
    net_ecpm    = summary["avg_ecpm"]

    print(
        f"[ctv_optimizer] CTV summary: "
        f"rev={fmt_usd(summary['avg_daily_revenue'])}/day  "
        f"eCPM={fmt_usd(net_ecpm)}  "
        f"fill={net_fill*100:.4f}%  "
        f"publishers={len(publishers)}"
    )

    # ── 5. Score scale opportunities ──────────────────────────────────────────
    scored_pubs = _score_scale_opportunities(publishers, net_fill, net_ecpm)
    projections = _build_projections(summary["avg_daily_revenue"])

    print(
        f"[ctv_optimizer] Top opportunity: "
        f"{scored_pubs[0]['publisher']} "
        f"(+{fmt_usd(scored_pubs[0]['opportunity_score'])}/day at avg fill)"
        if scored_pubs else "[ctv_optimizer] No scale opportunities computed."
    )

    # ── 6. Get Claude's business case ─────────────────────────────────────────
    claude_input_pubs = [
        {
            "publisher":         p["publisher"],
            "ecpm":              p["ecpm"],
            "fill_rate":         p["fill_rate"],
            "win_rate":          p["win_rate"],
            "revenue_14d":       p["revenue_14d"],
            "avg_daily_revenue": p["avg_daily_revenue"],
            "fill_gap":          p["fill_gap"],
            "opportunity_score": p["opportunity_score"],
        }
        for p in scored_pubs[:TOP_SCALE_PUBS]
    ]

    print(f"[ctv_optimizer] Requesting Claude business case…")
    try:
        claude_result = analyze_ctv_opportunity(summary, claude_input_pubs, projections)
    except Exception as exc:
        print(f"[ctv_optimizer] Claude analysis failed: {exc}")
        top_pub = scored_pubs[0]["publisher"] if scored_pubs else "top CTV publisher"
        claude_result = {
            "business_case": (
                f"CTV generates {fmt_usd(net_ecpm)} eCPM but represents only "
                f"{summary['pct_of_network']:.1f}% of network revenue. "
                f"Closing fill gaps across {len(publishers)} publishers unlocks "
                f"incremental premium-CPM revenue."
            ),
            "priority_publishers": [
                {
                    "publisher": p["publisher"],
                    "rationale": f"eCPM {fmt_usd(p['ecpm'])}, fill gap {p['fill_gap']*100:.4f}%",
                    "approach":  "Review floor price and demand partner configuration.",
                }
                for p in scored_pubs[:3]
            ],
            "top_action": f"Lower floor price on {top_pub} to increase CTV fill rate.",
            "projections_narrative": (
                f"A 10% volume lift adds {fmt_usd(projections['tier_10_daily'])}/day; "
                f"50% adds {fmt_usd(projections['tier_50_daily'])}/day "
                f"({fmt_usd(projections['tier_50_annual'])}/yr)."
            ),
        }

    # ── 7. Build and post Slack message ───────────────────────────────────────
    blocks = _build_slack_blocks(
        summary=summary,
        scored_pubs=scored_pubs,
        trend=trend,
        pub_trends=pub_daily_trends,
        projections=projections,
        claude_result=claude_result,
        date_label=date_label,
        now_label=now_label,
    )

    total_opp = sum(p["opportunity_score"] for p in scored_pubs)
    fallback  = (
        f"CTV Optimizer ({date_label}): {len(publishers)} publishers, "
        f"avg eCPM {fmt_usd(net_ecpm)}, "
        f"total daily opportunity {fmt_usd(total_opp)}. "
        f"Top action: {claude_result.get('top_action', 'See report.')}"
    )

    send_blocks(blocks=blocks, text=fallback)
    mark_sent(ALERT_KEY)
    print(
        f"[ctv_optimizer] Report sent — {len(publishers)} publishers, "
        f"{fmt_usd(total_opp)} total daily scale opportunity."
    )


if __name__ == "__main__":
    run()
