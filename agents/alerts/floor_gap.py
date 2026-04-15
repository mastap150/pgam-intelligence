"""
agents/alerts/floor_gap.py

Scans publisher-level CPM floor price gaps over the last 7 days and surfaces
two categories of actionable publishers to the ad ops team via Slack:

  RAISE candidates  — avg bid > 2× floor  → inventory is underpriced
  LOWER candidates  — avg bid < 0.5× floor → floors are too aggressive,
                                              suppressing fill

Filtering
---------
- Minimum 5,000 bids over the lookback window (low-traffic publishers are noisy)
- Uses THRESHOLDS from core/config.py for the raise/lower ratios

Recommended new floor
---------------------
  raise: new_floor = avg_bid_price × floor_raise_ratio   (conservative upward move)
  lower: new_floor = avg_bid_price × 1.1                 (just above the bid level)

Deduplication
-------------
Alert fires once per day via already_sent_today() / mark_sent() from core/slack.py.
"""

from datetime import datetime

import pytz

from core.api import fetch, n_days_ago, today, sf, fmt_usd, fmt_n, pct
from core.config import THRESHOLDS
from core.ll_report import report_pub_demand
from core.slack import already_sent_today, mark_sent, send_blocks
from core.ui_nav import floor_change, demand_seat_floor
from intelligence.claude_analyst import analyze_floor_gaps
from agents.alerts.action_tracker import log_recommendation

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
BREAKDOWN  = "PUBLISHER"
METRICS    = [
    "GROSS_REVENUE",
    "PUB_PAYOUT",
    "BIDS",
    "WINS",
    "AVG_FLOOR_PRICE",
    "AVG_BID_PRICE",
]
ALERT_KEY   = "floor_gap_daily"
MIN_BIDS    = 5_000
ET          = pytz.timezone("US/Eastern")

# Pull ratio thresholds from central config
RAISE_RATIO = THRESHOLDS["floor_raise_ratio"]   # 2.0  — bid > 2× floor → raise
LOWER_RATIO = THRESHOLDS["floor_lower_ratio"]   # 0.5  — bid < 0.5× floor → lower


# ---------------------------------------------------------------------------
# Row parsing helpers
# ---------------------------------------------------------------------------

def _get(row: dict, *keys) -> float:
    """
    Try multiple field-name variants (upper, lower, camelCase) and return
    the first match as a float. Returns 0.0 if none found.
    """
    for k in keys:
        if k in row:
            return sf(row[k])
    return 0.0


def _parse_publisher(row: dict) -> dict:
    """Normalise a raw API row into a clean publisher dict."""
    name      = (
        row.get("PUBLISHER_NAME")
        or row.get("PUBLISHER")
        or row.get("publisher")
        or row.get("pubName")
        or row.get("pub_name")
        or "Unknown"
    )
    bids      = _get(row, "BIDS", "bids")
    wins      = _get(row, "WINS", "wins")
    revenue   = _get(row, "GROSS_REVENUE", "gross_revenue", "grossRevenue")
    payout    = _get(row, "PUB_PAYOUT", "pub_payout", "pubPayout")
    avg_floor = _get(row, "AVG_FLOOR_PRICE", "avg_floor_price", "avgFloorPrice")
    avg_bid   = _get(row, "AVG_BID_PRICE",   "avg_bid_price",   "avgBidPrice")

    win_rate  = pct(wins, bids)
    margin    = pct(revenue - payout, revenue)

    return {
        "name":       str(name),
        "bids":       bids,
        "wins":       wins,
        "win_rate":   win_rate,
        "revenue":    revenue,
        "payout":     payout,
        "margin":     margin,
        "avg_floor":  avg_floor,
        "avg_bid":    avg_bid,
    }


# ---------------------------------------------------------------------------
# Classification logic
# ---------------------------------------------------------------------------

def _classify(publishers: list[dict]) -> tuple[list[dict], list[dict]]:
    """
    Split publishers into raise and lower candidates.

    Returns (raise_list, lower_list), each publisher dict extended with:
      - bid_to_floor_ratio
      - recommended_floor
      - est_revenue_impact  (rough directional estimate)
    """
    raise_list = []
    lower_list = []

    for pub in publishers:
        if pub["bids"] < MIN_BIDS:
            continue

        floor   = pub["avg_floor"]
        bid     = pub["avg_bid"]
        revenue = pub["revenue"]

        if floor <= 0 or bid <= 0:
            continue

        ratio = bid / floor

        if ratio >= RAISE_RATIO:
            # Inventory is underpriced — raise floor to capture more value
            new_floor      = round(bid * RAISE_RATIO / 2, 2)   # midpoint between floor and bid
            revenue_upside = round(revenue * (ratio - 1) * 0.3, 2)  # conservative 30% capture
            raise_list.append({
                **pub,
                "bid_to_floor_ratio":  round(ratio, 2),
                "recommended_floor":   new_floor,
                "est_revenue_impact":  revenue_upside,
            })

        elif ratio <= LOWER_RATIO:
            # Floor is too aggressive — lower it to recover fill and volume
            new_floor       = round(bid * 1.1, 2)   # 10% above avg bid
            fill_upside_pct = round((1 - ratio) * 100, 1)
            raise_list_item = {
                **pub,
                "bid_to_floor_ratio":  round(ratio, 2),
                "recommended_floor":   new_floor,
                "est_fill_upside_pct": fill_upside_pct,
            }
            lower_list.append(raise_list_item)

    # Sort each list by revenue descending (highest-value publishers first)
    raise_list.sort(key=lambda x: x["revenue"], reverse=True)
    lower_list.sort(key=lambda x: x["revenue"], reverse=True)

    return raise_list, lower_list


# ---------------------------------------------------------------------------
# Per-demand breakdown (extended API)
# ---------------------------------------------------------------------------

def _build_pub_demand_lookup(rows: list[dict]) -> dict[str, list[dict]]:
    """
    Build a lookup from publisher name → list of per-demand dicts.

    Each demand dict contains:
        demand_name, bids, wins, impressions, bid_requests,
        win_rate, bid_fill, gross_revenue
    """
    from collections import defaultdict

    # Aggregate across the date range (rows are already summed by the
    # extended API, but guard against duplicates just in case)
    agg: dict[tuple[str, str], dict] = defaultdict(lambda: {
        "bids": 0.0, "wins": 0.0, "impressions": 0.0,
        "bid_requests": 0.0, "gross_revenue": 0.0,
    })

    for row in rows:
        pub_name    = str(row.get("PUBLISHER_NAME") or "Unknown").strip()
        demand_name = str(row.get("DEMAND_NAME")    or "Unknown").strip()
        if pub_name == "Unknown" or demand_name == "Unknown":
            continue

        key = (pub_name, demand_name)
        agg[key]["bids"]          += sf(row.get("BIDS",          0))
        agg[key]["wins"]          += sf(row.get("WINS",          0))
        agg[key]["impressions"]   += sf(row.get("IMPRESSIONS",   0))
        agg[key]["bid_requests"]  += sf(row.get("BID_REQUESTS",  0))
        agg[key]["gross_revenue"] += sf(row.get("GROSS_REVENUE", 0))

    lookup: dict[str, list[dict]] = defaultdict(list)
    for (pub_name, demand_name), m in agg.items():
        bids      = m["bids"]
        wins      = m["wins"]
        bid_reqs  = m["bid_requests"]
        # win_rate  = wins / bids (pct)
        win_rate  = pct(wins, bids)
        # bid_fill  = bids / bid_requests (pct) — how often demand responds
        bid_fill  = pct(bids, bid_reqs)

        # Implied optimal floor: gross_revenue / impressions * 1000 (eCPM)
        imps = m["impressions"]
        implied_floor = (m["gross_revenue"] / imps * 1000) if imps > 0 else 0.0

        lookup[pub_name].append({
            "demand_name":         demand_name,
            "bids":                int(bids),
            "wins":                int(wins),
            "impressions":         int(imps),
            "bid_requests":        int(bid_reqs),
            "win_rate":            round(win_rate, 2),
            "bid_fill":            round(bid_fill, 2),
            "gross_revenue":       round(m["gross_revenue"], 2),
            "implied_optimal_floor": round(implied_floor, 4),
        })

    # Sort each publisher's demand list by gross_revenue descending
    for pub_name in lookup:
        lookup[pub_name].sort(key=lambda d: -d["gross_revenue"])

    return dict(lookup)


def _demand_breakdown_text(demand_rows: list[dict]) -> str:
    """
    Build a compact Slack-markdown string summarising per-demand performance
    for a single publisher.

    Flags:
      - bid_fill > 20 % but win_rate < 10 % → floor too high for this buyer
      - bid_fill < 5 %                       → not responding — consider removing
    """
    if not demand_rows:
        return "_No per-demand data available._"

    lines = ["*Per-demand breakdown:*"]
    for d in demand_rows[:8]:   # cap to keep message concise
        name         = d["demand_name"]
        win_rate     = d["win_rate"]
        bid_fill     = d["bid_fill"]
        impl_floor   = d["implied_optimal_floor"]
        revenue      = d["gross_revenue"]

        # Diagnostic flag
        flag = ""
        if bid_fill > 20 and win_rate < 10:
            flag = " :warning: _floor too high for this buyer specifically_"
        elif bid_fill < 5:
            flag = " :x: _not responding — consider removing_"

        lines.append(
            f"  • *{name}*  "
            f"win rate: `{win_rate:.1f}%`  "
            f"bid fill: `{bid_fill:.1f}%`  "
            f"implied floor: `{fmt_usd(impl_floor)}`  "
            f"rev: {fmt_usd(revenue)}"
            + flag
        )

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Slack Block Kit builders
# ---------------------------------------------------------------------------

def _pub_raise_row(pub: dict) -> str:
    return (
        f"*{pub['name']}*  "
        f"bid/floor: `{pub['bid_to_floor_ratio']}×` | "
        f"floor: `{fmt_usd(pub['avg_floor'])}` → `{fmt_usd(pub['recommended_floor'])}` | "
        f"rev: {fmt_usd(pub['revenue'])} | "
        f"est. upside: {fmt_usd(pub['est_revenue_impact'])}\n"
        + floor_change(pub['name'], pub['avg_floor'], pub['recommended_floor'])
    )


def _pub_lower_row(pub: dict) -> str:
    return (
        f"*{pub['name']}*  "
        f"bid/floor: `{pub['bid_to_floor_ratio']}×` | "
        f"floor: `{fmt_usd(pub['avg_floor'])}` → `{fmt_usd(pub['recommended_floor'])}` | "
        f"win rate: {pub['win_rate']:.1f}% | "
        f"est. fill upside: ~{pub.get('est_fill_upside_pct', 0):.0f}%\n"
        + floor_change(pub['name'], pub['avg_floor'], pub['recommended_floor'])
    )


def _build_blocks(
    raise_list: list[dict],
    lower_list: list[dict],
    claude_analysis: str,
    lookback_days: int,
    date_label: str,
    pub_demand_lookup: dict[str, list[dict]] | None = None,
) -> list:
    n_raise = len(raise_list)
    n_lower = len(lower_list)
    top_pub = raise_list[0] if raise_list else lower_list[0] if lower_list else None
    status_line = (
        f":bar_chart: *Floor Gap — {date_label}:* "
        f"{n_raise} raise candidate{'s' if n_raise != 1 else ''}, "
        f"{n_lower} lower candidate{'s' if n_lower != 1 else ''}."
        + (f"  Top action: *{top_pub['name']}* (${top_pub['avg_floor']:.3f} → ${top_pub['recommended_floor']:.3f})." if top_pub else "")
    )

    blocks = [
        {
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": f":bar_chart:  Floor Gap Report — {date_label}",
                "emoji": True,
            },
        },
        # ── Status line: one-sentence verdict up front ───────────────────────
        {
            "type": "section",
            "text": {"type": "mrkdwn", "text": status_line},
        },
        {
            "type": "context",
            "elements": [
                {
                    "type": "mrkdwn",
                    "text": (
                        f"Analysis window: last *{lookback_days} days* | "
                        f"Min bid volume: *{fmt_n(MIN_BIDS)}* | "
                        f"Raise threshold: *{RAISE_RATIO}×* | "
                        f"Lower threshold: *{LOWER_RATIO}×*"
                    ),
                }
            ],
        },
        {"type": "divider"},
        # ── Claude's analysis is the centerpiece — before data tables ────────
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f":robot_face: *Claude's Prioritization*\n{claude_analysis}",
            },
        },
        {"type": "divider"},
    ]

    # ── Raise candidates ─────────────────────────────────────────────────────
    if raise_list:
        blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f":arrow_up_small: *Floor Raise Candidates ({len(raise_list)} publishers)*",
            },
        })
        for pub in raise_list[:10]:   # cap at 10 to keep message readable
            blocks.append({
                "type": "section",
                "text": {"type": "mrkdwn", "text": _pub_raise_row(pub)},
            })
            # Per-demand breakdown (if extended API data is available)
            if pub_demand_lookup:
                demand_rows = pub_demand_lookup.get(pub["name"], [])
                if demand_rows:
                    blocks.append({
                        "type": "section",
                        "text": {
                            "type": "mrkdwn",
                            "text": _demand_breakdown_text(demand_rows),
                        },
                    })
    else:
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": ":arrow_up_small: *Floor Raise Candidates* — none identified."},
        })

    blocks.append({"type": "divider"})

    # ── Lower candidates ─────────────────────────────────────────────────────
    if lower_list:
        blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f":arrow_down_small: *Floor Reduction Candidates ({len(lower_list)} publishers)*",
            },
        })
        for pub in lower_list[:10]:
            blocks.append({
                "type": "section",
                "text": {"type": "mrkdwn", "text": _pub_lower_row(pub)},
            })
            # Per-demand breakdown (if extended API data is available)
            if pub_demand_lookup:
                demand_rows = pub_demand_lookup.get(pub["name"], [])
                if demand_rows:
                    blocks.append({
                        "type": "section",
                        "text": {
                            "type": "mrkdwn",
                            "text": _demand_breakdown_text(demand_rows),
                        },
                    })
    else:
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": ":arrow_down_small: *Floor Reduction Candidates* — none identified."},
        })

    blocks.append({"type": "divider"})

    blocks.append({
        "type": "context",
        "elements": [
            {
                "type": "mrkdwn",
                "text": (
                    f"PGAM Intelligence · Floor Gap Agent · "
                    f"{datetime.now(ET).strftime('%H:%M ET')}"
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
    Execute the floor gap analysis. Designed to be called by a scheduler
    or run directly: `python -m agents.alerts.floor_gap`.
    """
    now_et     = datetime.now(ET)
    date_label = now_et.strftime("%A, %B %-d")

    # ── 1. Deduplication check ───────────────────────────────────────────────
    if already_sent_today(ALERT_KEY):
        print("[floor_gap] Report already sent today — skipping.")
        return

    # ── 2. Fetch publisher data over the lookback window ────────────────────
    lookback   = THRESHOLDS["lookback_days"]
    start_date = n_days_ago(lookback)
    end_date   = today()

    print(f"[floor_gap] Fetching {BREAKDOWN} data {start_date} → {end_date}…")
    try:
        rows = fetch(BREAKDOWN, METRICS, start_date, end_date)
    except Exception as exc:
        print(f"[floor_gap] API fetch failed: {exc}")
        return

    if not rows:
        print("[floor_gap] No data returned from API — aborting.")
        return

    print(f"[floor_gap] {len(rows)} publisher rows received.")

    # ── 2b. Fetch publisher × demand data for per-demand breakdown ───────────
    pub_demand_lookup: dict | None = None
    try:
        print(f"[floor_gap] Fetching publisher × demand breakdown {start_date} → {end_date}…")
        demand_rows = report_pub_demand(start_date, end_date)
        if demand_rows:
            pub_demand_lookup = _build_pub_demand_lookup(demand_rows)
            print(f"[floor_gap] Per-demand data available for {len(pub_demand_lookup)} publishers.")
        else:
            print("[floor_gap] Extended API returned no rows — falling back to publisher-only analysis.")
    except Exception as exc:
        print(f"[floor_gap] Extended API fetch failed (non-fatal): {exc}")
        pub_demand_lookup = None

    # ── 3. Parse and classify ────────────────────────────────────────────────
    publishers = [_parse_publisher(row) for row in rows]
    raise_list, lower_list = _classify(publishers)

    print(
        f"[floor_gap] Raise candidates: {len(raise_list)} | "
        f"Lower candidates: {len(lower_list)}"
    )

    if not raise_list and not lower_list:
        print("[floor_gap] No actionable floor gaps found — no alert needed.")
        return

    # ── 4. Build Claude-ready summaries (cap at 20 each to control token cost) ──
    def _for_claude(pub_list: list[dict], cap: int = 20) -> list[dict]:
        return [
            {
                "publisher":          p["name"],
                "avg_bid":            round(p["avg_bid"], 4),
                "avg_floor":          round(p["avg_floor"], 4),
                "bid_to_floor_ratio": p["bid_to_floor_ratio"],
                "recommended_floor":  p["recommended_floor"],
                "revenue_7d":         round(p["revenue"], 2),
                "bids":               int(p["bids"]),
                "win_rate_pct":       round(p["win_rate"], 1),
            }
            for p in pub_list[:cap]
        ]

    # ── 5. Get Claude's prioritization (centerpiece — built before blocks) ──
    total_raise_upside = sum(p.get("est_revenue_impact", 0) for p in raise_list)
    total_lower_upside = sum(p["revenue"] * 0.1 for p in lower_list)

    top_raise = raise_list[0] if raise_list else None
    top_lower = lower_list[0] if lower_list else None

    try:
        claude_analysis = analyze_floor_gaps(
            raise_list=_for_claude(raise_list),
            lower_list=_for_claude(lower_list),
        )
    except Exception as exc:
        print(f"[floor_gap] Claude analysis failed: {exc}")
        # Specific fallback using real data, not a generic "unavailable" message
        parts = []
        if top_raise:
            parts.append(
                f"*Immediate action — raise {top_raise['name']}:* "
                f"avg bid is ${top_raise['avg_bid']:.3f} against a ${top_raise['avg_floor']:.3f} floor "
                f"({top_raise['bid_to_floor_ratio']}× ratio). "
                f"Set floor to ${top_raise['recommended_floor']:.3f} — "
                f"est. +${top_raise.get('est_revenue_impact', 0):,.0f} on ${top_raise['revenue']:,.0f}/7d revenue.\n"
                + floor_change(top_raise['name'], top_raise['avg_floor'], top_raise['recommended_floor'])
            )
        if top_lower:
            parts.append(
                f"*Immediate action — lower {top_lower['name']}:* "
                f"floor ${top_lower['avg_floor']:.3f} is {top_lower['bid_to_floor_ratio']}× avg bid "
                f"(${top_lower['avg_bid']:.3f}). "
                f"Lower to ${top_lower['recommended_floor']:.3f} to recover ~{top_lower.get('est_fill_upside_pct', 0):.0f}% fill.\n"
                + floor_change(top_lower['name'], top_lower['avg_floor'], top_lower['recommended_floor'])
            )
        # Append per-demand colour to fallback if available
        if pub_demand_lookup:
            demand_addenda = []
            for target_pub in [top_raise, top_lower]:
                if not target_pub:
                    continue
                d_rows = pub_demand_lookup.get(target_pub["name"], [])
                if d_rows:
                    demand_addenda.append(
                        f"_Per-demand for {target_pub['name']}:_\n"
                        + _demand_breakdown_text(d_rows)
                    )
            if demand_addenda:
                parts.extend(demand_addenda)

        claude_analysis = "\n\n".join(parts) if parts else "No actionable gaps identified this window."

    # ── 6. Post to Slack ─────────────────────────────────────────────────────
    blocks = _build_blocks(
        raise_list=raise_list,
        lower_list=lower_list,
        claude_analysis=claude_analysis,
        lookback_days=lookback,
        date_label=date_label,
        pub_demand_lookup=pub_demand_lookup,
    )

    n_raise = len(raise_list)
    n_lower = len(lower_list)
    fallback = (
        f"Floor Gap Report: {n_raise} raise candidate(s), "
        f"{n_lower} reduction candidate(s) identified."
    )

    send_blocks(blocks=blocks, text=fallback)
    mark_sent(ALERT_KEY)
    print(f"[floor_gap] Report sent — {n_raise} raise, {n_lower} lower.")

    # ── 7. Log recommendations to action tracker ─────────────────────────────
    for pub in raise_list[:10]:
        try:
            log_recommendation(
                agent_name              = "floor_gap",
                publisher               = pub["name"],
                metric_affected         = "avg_floor_price",
                recommended_change      = (
                    f"Raise floor from ${pub['avg_floor']:.3f} to "
                    f"${pub['recommended_floor']:.3f} "
                    f"(bid/floor ratio {pub['bid_to_floor_ratio']}×)"
                ),
                expected_impact_dollars = pub.get("est_revenue_impact", 0.0),
            )
        except Exception as exc:
            print(f"[floor_gap] log_recommendation failed for {pub['name']}: {exc}")

    for pub in lower_list[:10]:
        try:
            log_recommendation(
                agent_name              = "floor_gap",
                publisher               = pub["name"],
                metric_affected         = "avg_floor_price",
                recommended_change      = (
                    f"Lower floor from ${pub['avg_floor']:.3f} to "
                    f"${pub['recommended_floor']:.3f} "
                    f"(bid/floor ratio {pub['bid_to_floor_ratio']}×, "
                    f"fill upside ~{pub.get('est_fill_upside_pct', 0):.0f}%)"
                ),
                expected_impact_dollars = pub.get("revenue", 0.0) * 0.1,
            )
        except Exception as exc:
            print(f"[floor_gap] log_recommendation failed for {pub['name']}: {exc}")


if __name__ == "__main__":
    run()
