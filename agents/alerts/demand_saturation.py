"""
agents/alerts/demand_saturation.py

Finds publishers where bid density (BIDS / OPPORTUNITIES) is below 3.0,
indicating DSPs are willing to buy but aren't getting enough auction chances.

Eligibility filters
-------------------
  bid_density    < 3.0        (below target saturation)
  opportunities  > 100,000    (meaningful traffic volume over the lookback window)
  revenue        > $50        (publisher is actively monetising)

Revenue opportunity estimate
-----------------------------
If bid density were raised to TARGET_DENSITY (5), the publisher would receive
more bids → more wins → more impressions → more revenue.

  additional_bids        = (TARGET_DENSITY - current_density) × opportunities_7d
  additional_impressions = additional_bids × win_rate          [win_rate = wins/bids]
  revenue_opportunity    = (additional_impressions / 1_000) × eCPM

Publishers are ranked by revenue_opportunity descending. The top 10 are sent
to Claude, which selects the 3 to action this week and returns one specific
recommendation per publisher.

Deduplication
-------------
Alert key "demand_saturation_daily" fires once per day via core/slack.py.
"""

from datetime import datetime

import pytz

from core.api import fetch, n_days_ago, today, sf, fmt_usd, fmt_n, pct
from core.config import THRESHOLDS
from core.slack import already_sent_today, mark_sent, send_blocks
from core.ui_nav import demand_seat_add, floor_change
from intelligence.claude_analyst import analyze_demand_saturation
from agents.alerts.action_tracker import log_recommendation

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
BREAKDOWN        = "PUBLISHER"
METRICS          = ["GROSS_REVENUE", "OPPORTUNITIES", "BIDS", "IMPRESSIONS", "WINS"]
ALERT_KEY        = "demand_saturation_daily"

MIN_BID_DENSITY  = 3.0      # flag publishers below this threshold
TARGET_DENSITY   = 5.0      # density used for opportunity projection
MIN_OPPORTUNITIES= 100_000  # minimum 7-day opportunity count
MIN_REVENUE      = 50.0     # minimum 7-day revenue ($)
TOP_FOR_CLAUDE   = 10       # send this many to Claude for prioritisation
TOP_SHOWN        = 3        # Claude selects this many for the Slack post

ET = pytz.timezone("US/Eastern")


# ---------------------------------------------------------------------------
# Row parsing
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


def _parse_publishers(rows: list) -> list[dict]:
    """Parse raw API rows into normalised publisher dicts."""
    result = []
    for row in rows:
        name         = _pub_name(row)
        revenue      = _extract(row, "GROSS_REVENUE",  "gross_revenue",  "grossRevenue")
        opportunities= _extract(row, "OPPORTUNITIES",  "opportunities")
        bids         = _extract(row, "BIDS",           "bids")
        impressions  = _extract(row, "IMPRESSIONS",    "impressions")
        wins         = _extract(row, "WINS",           "wins")

        bid_density  = (bids / opportunities) if opportunities > 0 else 0.0
        ecpm         = (revenue / impressions * 1_000) if impressions > 0 else 0.0
        win_rate     = pct(wins, bids)          # as percentage, e.g. 4.2

        result.append({
            "name":          name,
            "revenue":       revenue,
            "opportunities": opportunities,
            "bids":          bids,
            "impressions":   impressions,
            "wins":          wins,
            "bid_density":   bid_density,
            "ecpm":          ecpm,
            "win_rate":      win_rate,
        })
    return result


# ---------------------------------------------------------------------------
# Eligibility & opportunity scoring
# ---------------------------------------------------------------------------

def _is_eligible(pub: dict) -> bool:
    return (
        pub["bid_density"]   <  MIN_BID_DENSITY
        and pub["opportunities"] >  MIN_OPPORTUNITIES
        and pub["revenue"]       >= MIN_REVENUE
    )


def _revenue_opportunity(pub: dict) -> float:
    """
    Estimate additional weekly revenue if bid density reached TARGET_DENSITY.

    additional_bids        = (target - current) × opportunities
    win_rate_fraction      = wins / bids  (not percentage)
    additional_impressions = additional_bids × win_rate_fraction
    additional_revenue     = additional_impressions / 1_000 × eCPM
    """
    if pub["bids"] <= 0 or pub["ecpm"] <= 0:
        return 0.0

    win_rate_frac       = pub["wins"] / pub["bids"]
    additional_bids     = (TARGET_DENSITY - pub["bid_density"]) * pub["opportunities"]
    additional_imps     = additional_bids * win_rate_frac
    return (additional_imps / 1_000) * pub["ecpm"]


# ---------------------------------------------------------------------------
# Slack Block Kit builder
# ---------------------------------------------------------------------------

def _density_bar(density: float, target: float = TARGET_DENSITY, width: int = 10) -> str:
    """Visual bar showing current density vs target."""
    filled = min(int(density / target * width), width)
    return "█" * filled + "░" * (width - filled)


def _build_blocks(
    top3: list[dict],           # Claude's 3 picks, each with "publisher","action","reasoning"
    pub_by_name: dict[str, dict],  # full parsed data keyed by name
    lookback_days: int,
    date_label: str,
    now_label: str,
    total_flagged: int,
) -> list:
    top_pub    = top3[0] if top3 else None
    top_pub_data = pub_by_name.get(top_pub["publisher"], {}) if top_pub else {}
    total_opp  = sum(pub_by_name.get(p["publisher"], {}).get("revenue_opp", 0.0) for p in top3)
    status_line = (
        f":signal_strength: *Demand Saturation — {date_label}:* "
        f"{total_flagged} publishers below {MIN_BID_DENSITY:.0f} bids/opportunity — "
        f"*${total_opp:,.0f}/week* recoverable across top 3."
        + (
            f"  Lead: *{top_pub['publisher']}* "
            f"(density {top_pub_data.get('bid_density', 0):.2f}, "
            f"${top_pub_data.get('revenue_opp', 0):,.0f}/wk opp)."
            if top_pub else ""
        )
    )

    blocks = [
        {
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": f":signal_strength:  Demand Saturation — {date_label}",
                "emoji": True,
            },
        },
        # ── Status line: one-sentence verdict ───────────────────────────────
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
                        f"Last *{lookback_days} days*  |  "
                        f"*{total_flagged}* publishers below {MIN_BID_DENSITY:.0f} bids/opportunity  |  "
                        f"min opps: {fmt_n(MIN_OPPORTUNITIES)}  |  "
                        f"target density: {TARGET_DENSITY:.0f}  |  "
                        f"Showing top 3 by revenue opportunity"
                    ),
                }
            ],
        },
        {"type": "divider"},
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": (
                    ":robot_face: *Claude's top 3 priorities this week* "
                    "— ranked by revenue opportunity and actionability"
                ),
            },
        },
    ]

    for i, pick in enumerate(top3, 1):
        pub   = pub_by_name.get(pick["publisher"], {})
        name  = pick["publisher"]
        action    = pick.get("action",    "Review DSP seat configuration.")
        reasoning = pick.get("reasoning", "")

        density   = pub.get("bid_density", 0.0)
        rev_opp   = pub.get("revenue_opp", 0.0)
        revenue   = pub.get("revenue", 0.0)
        opps      = pub.get("opportunities", 0.0)
        ecpm      = pub.get("ecpm", 0.0)
        win_rate  = pub.get("win_rate", 0.0)
        bar       = _density_bar(density)

        rank_emoji = [":one:", ":two:", ":three:"][i - 1]

        blocks.append({"type": "divider"})
        blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": (
                    f"{rank_emoji}  *{name}*\n"
                    f"  `{bar}`  density *{density:.2f}* / {TARGET_DENSITY:.0f} target\n"
                    f"  7d revenue: {fmt_usd(revenue)}   opps: {fmt_n(opps)}   "
                    f"eCPM: {fmt_usd(ecpm)}   win rate: {win_rate:.1f}%\n"
                    f"  :moneybag: *Revenue opportunity: {fmt_usd(rev_opp)}/week* "
                    f"if density → {TARGET_DENSITY:.0f}"
                ),
            },
        })
        blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": (
                    f"  :wrench: *Action:* {action}\n"
                    f"  :speech_balloon: _{reasoning}_"
                ),
            },
        })

    blocks.append({"type": "divider"})
    blocks.append({
        "type": "context",
        "elements": [
            {
                "type": "mrkdwn",
                "text": f"PGAM Intelligence · Demand Saturation Agent · {now_label}",
            }
        ],
    })

    return blocks


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def run():
    """
    Execute the demand saturation analysis. Designed to be called by a
    scheduler or run directly: `python -m agents.alerts.demand_saturation`.
    """
    now_et     = datetime.now(ET)
    now_label  = now_et.strftime("%H:%M ET")
    date_label = now_et.strftime("%A, %B %-d")

    # ── 1. Dedup ─────────────────────────────────────────────────────────────
    if already_sent_today(ALERT_KEY):
        print("[demand_sat] Report already sent today — skipping.")
        return

    # ── 2. Fetch publisher metrics over the lookback window ──────────────────
    lookback   = THRESHOLDS["lookback_days"]
    start_date = n_days_ago(lookback)
    end_date   = today()

    print(f"[demand_sat] Fetching {BREAKDOWN} data {start_date} → {end_date}…")
    try:
        rows = fetch(BREAKDOWN, METRICS, start_date, end_date)
    except Exception as exc:
        print(f"[demand_sat] API fetch failed: {exc}")
        return

    if not rows:
        print("[demand_sat] No data returned — aborting.")
        return

    # ── 3. Parse and filter eligible publishers ───────────────────────────────
    publishers = _parse_publishers(rows)
    eligible   = [p for p in publishers if _is_eligible(p)]

    print(
        f"[demand_sat] {len(publishers)} publishers total, "
        f"{len(eligible)} eligible (density < {MIN_BID_DENSITY}, "
        f"opps > {fmt_n(MIN_OPPORTUNITIES)}, rev > {fmt_usd(MIN_REVENUE)})."
    )

    if not eligible:
        print("[demand_sat] No underserved publishers found — no alert needed.")
        mark_sent(ALERT_KEY)
        return

    # ── 4. Score and rank by revenue opportunity ─────────────────────────────
    for pub in eligible:
        pub["revenue_opp"] = _revenue_opportunity(pub)

    eligible.sort(key=lambda p: p["revenue_opp"], reverse=True)
    top_candidates = eligible[:TOP_FOR_CLAUDE]

    total_weekly_opp = sum(p["revenue_opp"] for p in eligible)
    print(
        f"[demand_sat] Top candidate: {top_candidates[0]['name']} "
        f"({fmt_usd(top_candidates[0]['revenue_opp'])}/wk opp).  "
        f"Total pool opportunity: {fmt_usd(total_weekly_opp)}/wk."
    )

    # ── 5. Build Claude payload ───────────────────────────────────────────────
    claude_input = [
        {
            "name":              p["name"],
            "bid_density":       round(p["bid_density"], 3),
            "opportunities_7d":  int(p["opportunities"]),
            "revenue_7d":        round(p["revenue"], 2),
            "ecpm":              round(p["ecpm"], 4),
            "win_rate_pct":      round(p["win_rate"], 2),
            "revenue_opp":       round(p["revenue_opp"], 2),
        }
        for p in top_candidates
    ]

    # ── 6. Ask Claude to prioritise and recommend ────────────────────────────
    print(f"[demand_sat] Sending {len(claude_input)} publishers to Claude…")
    try:
        top3_picks = analyze_demand_saturation(claude_input)
    except Exception as exc:
        print(f"[demand_sat] Claude analysis failed: {exc}")
        # Specific fallback — names the publisher, density gap, and exact ask
        top3_picks = [
            {
                "publisher": p["name"],
                "action": (
                    f"Lower floor for {p['name']} to increase bid density from "
                    f"{p['bid_density']:.2f} to {TARGET_DENSITY:.0f} target — "
                    f"need {int((TARGET_DENSITY - p['bid_density']) * p['opportunities'] / 7):,} "
                    f"additional daily bids. Expected weekly uplift: ${p['revenue_opp']:,.0f}.\n"
                    + floor_change(p["name"], p.get("ecpm", 0) * 0.8, p.get("ecpm", 0) * 0.6)
                ),
                "reasoning": (
                    f"Bid density {p['bid_density']:.2f} on {p['opportunities']:,.0f} opps/7d — "
                    f"raising to {TARGET_DENSITY:.0f} would add "
                    f"${p['revenue_opp']:,.0f}/week at {p['ecpm']:.2f} eCPM."
                ),
            }
            for p in top_candidates[:TOP_SHOWN]
        ]

    # Attach the full numeric data to each pick for Slack rendering
    pub_by_name = {p["name"]: p for p in eligible}

    # ── 7. Build and post Slack message ──────────────────────────────────────
    blocks = _build_blocks(
        top3=top3_picks,
        pub_by_name=pub_by_name,
        lookback_days=lookback,
        date_label=date_label,
        now_label=now_label,
        total_flagged=len(eligible),
    )

    fallback_parts = [
        f"{p['publisher']}: density {pub_by_name.get(p['publisher'], {}).get('bid_density', 0):.2f} "
        f"(opp: {fmt_usd(pub_by_name.get(p['publisher'], {}).get('revenue_opp', 0))}/wk)"
        for p in top3_picks
    ]
    fallback = (
        f"Demand Saturation: {len(eligible)} underserved publishers. "
        f"Top 3: {' | '.join(fallback_parts)}"
    )

    send_blocks(blocks=blocks, text=fallback)
    mark_sent(ALERT_KEY)
    print(f"[demand_sat] Report sent — {len(eligible)} flagged, top 3 shown.")

    # ── 8. Log recommendations to action tracker ─────────────────────────────
    for pick in top3_picks:
        pub_data = pub_by_name.get(pick["publisher"], {})
        try:
            log_recommendation(
                agent_name              = "demand_saturation",
                publisher               = pick["publisher"],
                metric_affected         = "bid_density",
                recommended_change      = pick.get("action", "Audit DSP seat configuration."),
                expected_impact_dollars = pub_data.get("revenue_opp", 0.0),
            )
        except Exception as exc:
            print(f"[demand_sat] log_recommendation failed for {pick['publisher']}: {exc}")


if __name__ == "__main__":
    run()
