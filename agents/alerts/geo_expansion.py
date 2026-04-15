"""
agents/alerts/geo_expansion.py
──────────────────────────────────────────────────────────────────────────────
Geographic expansion opportunity agent.

Maps where your top demand partners are buying versus where you have supply
and quantifies the gap.  Posts to Slack every Thursday.

Algorithm
─────────
1. Fetch COUNTRY_NAME breakdown (last 7 days) → supply coverage per country.
2. Fetch COUNTRY_NAME,DEMAND_PARTNER breakdown (same window) → per-DP
   activity by country.
3. Identify top 10 demand partners by total 7-day gross revenue.
4. For each (DP, country) where supply exists but the DP is absent or thin,
   estimate the revenue opportunity:
       opp_rev = supply_impressions × dp_win_rate × dp_avg_ecpm / 1000
5. Rank all gaps by estimated 7-day revenue; send top 20 to Claude.
6. Claude returns the 3 highest-priority expansion moves with specific actions.
7. Post a Block Kit Slack message every Thursday.
"""

from __future__ import annotations

import json
from collections import defaultdict
from datetime import datetime
from pathlib import Path

import pytz

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

LOOKBACK_DAYS  = 7
ALERT_KEY      = "geo_expansion_weekly"
ET             = pytz.timezone("America/New_York")

BREAKDOWN_COUNTRY    = "COUNTRY_NAME"
BREAKDOWN_COUNTRY_DP = "COUNTRY_NAME,DEMAND_PARTNER"
METRICS              = "GROSS_REVENUE,BIDS,WINS,IMPRESSIONS,GROSS_ECPM"

TOP_DP_COUNT         = 10    # demand partners to analyse
TOP_GAPS_FOR_CLAUDE  = 20    # gaps sent to Claude for prioritisation
MIN_SUPPLY_IMPS      = 5_000  # minimum 7-day impressions to count as real supply
DP_ACTIVE_MIN_BIDS   = 50    # minimum 7-day bids for a DP to be "active" in a country
GAP_INACTIVE_BIDS    = 20    # DP is "absent" if bids < this threshold


# ---------------------------------------------------------------------------
# Lazy imports
# ---------------------------------------------------------------------------

def _imports():
    from core.api    import fetch, n_days_ago, today, sf, fmt_usd, fmt_n, pct
    from core.slack  import send_blocks, already_sent_today, mark_sent
    from core.ui_nav import geo_target_add
    from intelligence.claude_analyst import analyze_geo_expansion
    return fetch, n_days_ago, today, sf, fmt_usd, fmt_n, pct, \
           send_blocks, already_sent_today, mark_sent, analyze_geo_expansion, geo_target_add


# ---------------------------------------------------------------------------
# Field helpers
# ---------------------------------------------------------------------------

def _sf(v) -> float:
    try:
        return float(v) if v is not None else 0.0
    except (TypeError, ValueError):
        return 0.0


def _country(row: dict) -> str:
    return str(row.get("COUNTRY_NAME") or row.get("COUNTRY") or "Unknown").strip() or "Unknown"


def _dp_name(row: dict) -> str:
    return str(row.get("DEMAND_PARTNER_NAME") or row.get("demand_partner_name") or "Unknown").strip() or "Unknown"


# ---------------------------------------------------------------------------
# Data builders
# ---------------------------------------------------------------------------

def _build_supply(rows: list) -> dict[str, dict]:
    """
    Build country-level supply summary from COUNTRY_NAME breakdown rows.

    Returns:
        { country_name: {revenue, impressions, wins, bids, ecpm} }
    """
    supply: dict[str, dict] = {}
    for row in rows:
        country = _country(row)
        if country in ("Unknown", ""):
            continue
        supply[country] = {
            "revenue":     _sf(row.get("GROSS_REVENUE")),
            "impressions": _sf(row.get("IMPRESSIONS")),
            "wins":        _sf(row.get("WINS")),
            "bids":        _sf(row.get("BIDS")),
            "ecpm":        _sf(row.get("GROSS_ECPM")),
        }
    return supply


def _build_dp_activity(rows: list) -> dict[str, dict[str, dict]]:
    """
    Build per-DP, per-country activity from COUNTRY_NAME,DEMAND_PARTNER rows.

    Returns:
        { dp_name: { country_name: {revenue, bids, wins, impressions, ecpm} } }
    """
    activity: dict[str, dict[str, dict]] = defaultdict(lambda: defaultdict(lambda: {
        "revenue": 0.0, "bids": 0, "wins": 0, "impressions": 0, "ecpm": 0.0,
    }))

    for row in rows:
        country = _country(row)
        dp      = _dp_name(row)
        if country in ("Unknown", "") or dp in ("Unknown", ""):
            continue

        rev  = _sf(row.get("GROSS_REVENUE"))
        bids = int(_sf(row.get("BIDS")))
        wins = int(_sf(row.get("WINS")))
        imps = int(_sf(row.get("IMPRESSIONS")))
        ecpm = _sf(row.get("GROSS_ECPM"))

        existing = activity[dp][country]
        existing["revenue"]     += rev
        existing["bids"]        += bids
        existing["wins"]        += wins
        existing["impressions"] += imps
        # Weighted eCPM update
        total_imps = existing["impressions"]
        if total_imps > 0:
            existing["ecpm"] = existing["revenue"] / total_imps * 1000
        else:
            existing["ecpm"] = max(existing["ecpm"], ecpm)

    return activity


def _top_dps(activity: dict[str, dict[str, dict]], n: int = TOP_DP_COUNT) -> list[str]:
    """Return the top-n demand partners by total 7-day revenue."""
    totals: dict[str, float] = defaultdict(float)
    for dp, countries in activity.items():
        for stats in countries.values():
            totals[dp] += stats["revenue"]
    ranked = sorted(totals, key=lambda d: totals[d], reverse=True)
    return ranked[:n]


def _dp_profile(dp_name: str, country_data: dict[str, dict]) -> dict:
    """
    Compute aggregate stats for a demand partner across its active countries.

    Returns:
        {
          total_revenue, total_bids, total_wins, total_impressions,
          avg_ecpm, win_rate, active_countries: [country, ...]
        }
    """
    total_rev  = 0.0
    total_bids = 0
    total_wins = 0
    total_imps = 0

    for stats in country_data.values():
        total_rev  += stats["revenue"]
        total_bids += stats["bids"]
        total_wins += stats["wins"]
        total_imps += stats["impressions"]

    avg_ecpm  = (total_rev / total_imps * 1000) if total_imps > 0 else 0.0
    win_rate  = total_wins / total_bids if total_bids > 0 else 0.0
    active    = [c for c, s in country_data.items() if s["bids"] >= DP_ACTIVE_MIN_BIDS]

    return {
        "dp_name":        dp_name,
        "total_revenue":  round(total_rev, 2),
        "total_bids":     total_bids,
        "total_wins":     total_wins,
        "avg_ecpm":       round(avg_ecpm, 4),
        "win_rate":       round(win_rate, 6),
        "active_countries": sorted(active, key=lambda c: country_data[c]["revenue"], reverse=True),
    }


def _find_gaps(
    supply:   dict[str, dict],
    activity: dict[str, dict[str, dict]],
    top_dps:  list[str],
) -> list[dict]:
    """
    For each (DP, country) where supply exists but the DP is absent/thin,
    estimate the weekly revenue opportunity.

    opportunity_7d = supply_impressions × dp_win_rate × dp_avg_ecpm / 1000
    """
    gaps: list[dict] = []

    for dp in top_dps:
        country_data = activity.get(dp, {})
        profile      = _dp_profile(dp, country_data)

        if profile["win_rate"] <= 0 or profile["avg_ecpm"] <= 0:
            continue  # DP has no usable historical rates

        for country, sup in supply.items():
            if sup["impressions"] < MIN_SUPPLY_IMPS:
                continue  # not enough supply to matter

            dp_in_country = country_data.get(country, {})
            dp_bids       = dp_in_country.get("bids", 0)

            if dp_bids >= DP_ACTIVE_MIN_BIDS:
                continue  # DP already active here — not a gap

            # Estimated revenue if DP were to buy at its network rates
            opp_7d = sup["impressions"] * profile["win_rate"] * profile["avg_ecpm"] / 1000
            if opp_7d < 1.0:
                continue  # too small to bother

            current_rev = dp_in_country.get("revenue", 0.0)
            supply_ecpm = sup["ecpm"]

            gaps.append({
                "demand_partner":          dp,
                "country":                 country,
                "supply_impressions_7d":   int(sup["impressions"]),
                "supply_revenue_7d":       round(sup["revenue"], 2),
                "supply_ecpm":             round(supply_ecpm, 4),
                "dp_active_countries":     len(profile["active_countries"]),
                "dp_total_revenue_7d":     profile["total_revenue"],
                "dp_avg_ecpm":             profile["avg_ecpm"],
                "dp_win_rate":             round(profile["win_rate"] * 100, 3),  # pct
                "dp_current_rev_here":     round(current_rev, 4),
                "dp_current_bids_here":    dp_bids,
                "opportunity_7d":          round(opp_7d, 2),
                "opportunity_daily":       round(opp_7d / 7, 2),
                "opportunity_annual":      round(opp_7d / 7 * 365, 0),
            })

    gaps.sort(key=lambda g: g["opportunity_7d"], reverse=True)
    return gaps


# ---------------------------------------------------------------------------
# Slack Block Kit builders
# ---------------------------------------------------------------------------

def _opportunity_bar(opp_7d: float, max_opp: float, width: int = 8) -> str:
    if max_opp <= 0:
        return "░" * width
    ratio  = min(opp_7d / max_opp, 1.0)
    filled = max(0, min(int(ratio * width), width))
    return "█" * filled + "░" * (width - filled)


def _country_flag(country: str) -> str:
    """Return a best-effort flag emoji for common countries, else globe."""
    flags = {
        "United States": "🇺🇸", "US": "🇺🇸",
        "United Kingdom": "🇬🇧", "UK": "🇬🇧",
        "Canada": "🇨🇦", "Australia": "🇦🇺",
        "Germany": "🇩🇪", "France": "🇫🇷",
        "Japan": "🇯🇵", "India": "🇮🇳",
        "Brazil": "🇧🇷", "Mexico": "🇲🇽",
        "Indonesia": "🇮🇩", "Nigeria": "🇳🇬",
        "South Africa": "🇿🇦", "Netherlands": "🇳🇱",
        "Spain": "🇪🇸", "Italy": "🇮🇹",
        "Singapore": "🇸🇬", "Thailand": "🇹🇭",
        "Philippines": "🇵🇭", "Malaysia": "🇲🇾",
        "Vietnam": "🇻🇳", "Pakistan": "🇵🇰",
        "Bangladesh": "🇧🇩", "Argentina": "🇦🇷",
        "Colombia": "🇨🇴", "Egypt": "🇪🇬",
        "Turkey": "🇹🇷", "Saudi Arabia": "🇸🇦",
        "UAE": "🇦🇪", "Kenya": "🇰🇪",
    }
    return flags.get(country, ":earth_americas:")


def _build_slack_blocks(
    gaps:          list[dict],     # all scored gaps, sorted
    claude_picks:  list[dict],     # Claude's top 3: {country, demand_partner, action, rationale}
    date_range:    str,
    top_n:         int = 5,
) -> list:
    total_opp_7d  = sum(g["opportunity_7d"] for g in gaps)
    total_opp_ann = total_opp_7d / 7 * 365
    max_opp       = gaps[0]["opportunity_7d"] if gaps else 1.0
    n_countries   = len({g["country"] for g in gaps})
    n_dps         = len({g["demand_partner"] for g in gaps})
    displayed     = gaps[:top_n]

    blocks: list = [
        # ── Header ────────────────────────────────────────────────────────
        {
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": ":earth_americas:  Geographic Expansion Opportunities",
            },
        },
        {
            "type": "context",
            "elements": [{
                "type": "mrkdwn",
                "text": (
                    f"*{date_range}*  ·  "
                    f"{n_countries} countries with gaps  ·  "
                    f"{n_dps} demand partners analysed  ·  "
                    f"Total 7-day opp: *${total_opp_7d:,.0f}*  ·  "
                    f"Annualised: *${total_opp_ann:,.0f}*"
                ),
            }],
        },
        {"type": "divider"},
    ]

    # ── Claude's top 3 recommendations ────────────────────────────────────
    if claude_picks:
        rec_lines = []
        rank_emojis = [":one:", ":two:", ":three:"]
        for i, pick in enumerate(claude_picks[:3]):
            flag    = _country_flag(pick.get("country", ""))
            country = pick.get("country", "")
            dp      = pick.get("demand_partner", "")
            action  = pick.get("action", "")
            rat     = pick.get("rationale", "")
            rec_lines.append(
                f"{rank_emojis[i]}  *{flag} {country}  ×  {dp}*\n"
                f"    :dart: {action}\n"
                f"    _{rat}_"
            )

        blocks += [
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": (
                        "*:bulb: Claude's Top 3 Expansion Moves*\n"
                        + "\n\n".join(rec_lines)
                    ),
                },
            },
            {"type": "divider"},
        ]

    # ── Top gaps table ─────────────────────────────────────────────────────
    if displayed:
        blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"*:bar_chart: Top {len(displayed)} Revenue Gaps*",
            },
        })

        for g in displayed:
            flag    = _country_flag(g["country"])
            bar     = _opportunity_bar(g["opportunity_7d"], max_opp)
            dp_ecpm = g["dp_avg_ecpm"]
            sup_ecpm = g["supply_ecpm"]
            ecpm_diff = dp_ecpm - sup_ecpm
            ecpm_arrow = ":arrow_up_small:" if ecpm_diff > 0 else ":arrow_down_small:"

            blocks.append({
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": (
                        f"{flag}  *{g['country']}*  ·  _{g['demand_partner']}_\n"
                        f"`{bar}`  *${g['opportunity_7d']:,.0f}/wk*  "
                        f"(${g['opportunity_daily']:,.0f}/day  ·  ${g['opportunity_annual']:,.0f}/yr)\n"
                        f"Supply: {g['supply_impressions_7d']:,} imps  ·  "
                        f"Supply eCPM: ${sup_ecpm:.2f}  ·  "
                        f"DP eCPM: ${dp_ecpm:.2f} {ecpm_arrow}  ·  "
                        f"DP win rate: {g['dp_win_rate']:.2f}%  ·  "
                        f"DP active in {g['dp_active_countries']} countries"
                    ),
                },
            })

        blocks.append({"type": "divider"})

    # ── Footer ────────────────────────────────────────────────────────────
    blocks.append({
        "type": "context",
        "elements": [{
            "type": "mrkdwn",
            "text": (
                f":information_source:  Gaps = supply exists (≥{MIN_SUPPLY_IMPS:,} imps/7d) "
                f"but demand partner has <{GAP_INACTIVE_BIDS} bids.  "
                f"Opportunity = supply imps × DP win rate × DP eCPM.  "
                f"Top 10 DPs by 7-day revenue.  Weekly  ·  Thursday."
            ),
        }],
    })

    return blocks


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def run():
    now_et   = datetime.now(ET)
    weekday  = now_et.weekday()  # 0=Mon … 6=Sun

    # Thursday gate (weekday == 3)
    if weekday != 3:
        print(f"[geo_expansion] Not Thursday (weekday={weekday}). Skipping.")
        return

    (fetch, n_days_ago, today, sf, fmt_usd, fmt_n, pct,
     send_blocks, already_sent_today, mark_sent,
     analyze_geo_expansion, geo_target_add) = _imports()

    if already_sent_today(ALERT_KEY):
        print("[geo_expansion] Already sent this week. Exiting.")
        return

    start = n_days_ago(LOOKBACK_DAYS)
    end   = today()

    # ── 1. Fetch data ──────────────────────────────────────────────────────
    print(f"[geo_expansion] Fetching {start} → {end}…")

    try:
        country_rows = fetch(BREAKDOWN_COUNTRY, METRICS, start, end)
    except Exception as exc:
        print(f"[geo_expansion] Country fetch failed: {exc}")
        return

    try:
        dp_country_rows = fetch(BREAKDOWN_COUNTRY_DP, METRICS, start, end)
    except Exception as exc:
        print(f"[geo_expansion] DP×Country fetch failed: {exc}")
        return

    if not country_rows or not dp_country_rows:
        print("[geo_expansion] No data returned.")
        mark_sent(ALERT_KEY)
        return

    # ── 2. Build supply + DP activity maps ────────────────────────────────
    supply   = _build_supply(country_rows)
    activity = _build_dp_activity(dp_country_rows)
    top_dps  = _top_dps(activity, TOP_DP_COUNT)

    print(f"[geo_expansion] Supply: {len(supply)} countries  |  DPs: {len(top_dps)} top partners")

    if not top_dps:
        print("[geo_expansion] No demand partner data. Exiting.")
        mark_sent(ALERT_KEY)
        return

    # ── 3. Find and score gaps ────────────────────────────────────────────
    gaps = _find_gaps(supply, activity, top_dps)

    print(f"[geo_expansion] Found {len(gaps)} revenue gaps")

    if not gaps:
        print("[geo_expansion] No gaps above threshold — nothing to post.")
        mark_sent(ALERT_KEY)
        return

    # ── 4. Ask Claude for the top 3 moves ─────────────────────────────────
    total_supply_revenue = sum(s["revenue"] for s in supply.values())
    supply_summary = {
        "total_countries_with_supply": len(supply),
        "total_7d_revenue":            round(total_supply_revenue, 2),
        "top_supply_countries":        sorted(
            [{"country": c, "revenue_7d": round(s["revenue"], 2),
              "impressions_7d": int(s["impressions"]), "ecpm": round(s["ecpm"], 4)}
             for c, s in supply.items() if s["impressions"] >= MIN_SUPPLY_IMPS],
            key=lambda x: x["revenue_7d"], reverse=True
        )[:10],
    }
    dp_summary = [
        {
            "dp": dp,
            "total_revenue_7d": round(sum(s["revenue"] for s in activity[dp].values()), 2),
            "active_countries": len([c for c, s in activity[dp].items() if s["bids"] >= DP_ACTIVE_MIN_BIDS]),
        }
        for dp in top_dps
    ]

    try:
        claude_picks = analyze_geo_expansion(
            gaps=gaps[:TOP_GAPS_FOR_CLAUDE],
            supply_summary=supply_summary,
            dp_summary=dp_summary,
        )
    except Exception as exc:
        print(f"[geo_expansion] Claude analysis failed: {exc}")
        # Specific fallback with execute steps
        claude_picks = [
            {
                "country":        g["country"],
                "demand_partner": g["demand_partner"],
                "action": (
                    f"Activate {g['demand_partner']} in {g['country']} — "
                    f"DP earns ~{fmt_usd(g['dp_avg_ecpm'])} eCPM across "
                    f"{g.get('dp_active_countries', '?')} active countries; "
                    f"{fmt_n(int(g['supply_impressions_7d']))} imps/7d available at "
                    f"{fmt_usd(g['supply_ecpm'])} eCPM. "
                    f"Est. ${g['opportunity_7d']:,.0f}/wk.\n"
                    + geo_target_add(g["demand_partner"], g["country"])
                ),
                "rationale": (
                    f"{g['demand_partner']} is absent from {g['country']} supply "
                    f"despite {fmt_n(int(g['supply_impressions_7d']))} weekly impressions available."
                ),
                "opportunity_7d": g["opportunity_7d"],
            }
            for g in gaps[:3]
        ]

    # ── 5. Build and post Slack message ───────────────────────────────────
    date_range = f"{start} → {end}"
    blocks     = _build_slack_blocks(gaps, claude_picks, date_range, top_n=5)

    fallback = (
        f":earth_americas: Geo Expansion | {len(gaps)} gaps found | "
        f"Top opp: {gaps[0]['country']} × {gaps[0]['demand_partner']} "
        f"${gaps[0]['opportunity_7d']:,.0f}/wk"
    )

    try:
        send_blocks(blocks, text=fallback)
        mark_sent(ALERT_KEY)
        print("[geo_expansion] Slack message posted.")
    except Exception as exc:
        print(f"[geo_expansion] Slack post failed: {exc}")


if __name__ == "__main__":
    run()
