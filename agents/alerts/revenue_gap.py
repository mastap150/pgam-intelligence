"""
agents/alerts/revenue_gap.py
──────────────────────────────────────────────────────────────────────────────
Strategic growth agent — Sunday weekly strategy memo.

Milestone ladder: $650K → $1M → $3M per month.
Current target:   $1,000,000 / month  ($33,333 / day).

Every Sunday the agent:
  1.  Computes the daily revenue gap between the current 7-day run rate and
      the $33,333/day pace needed to hit $1M this month.
  2.  Finds where that gap can be closed across three dimensions:

      PUBLISHERS   — earning less than 50% of their bid-density potential
                     (high bids, low wins = floor pressure suppressing revenue)

      DEMAND       — partners with a >15% week-on-week revenue decline that
                     represent recoverable spend

      GEOGRAPHY    — high-eCPM countries with impression volume well below
                     the network median (undersupplied markets)

  3.  Passes all three analyses plus the dollar target to Claude, which writes
      a strategic Sunday briefing — not a data dump.  The memo answers:
        • Here is exactly where the $X/day gap is coming from
        • Here are the three specific actions that close it fastest
        • Here is the revenue trajectory if we action all three vs none

Delivery
────────
Plain-text Slack message (Claude's prose is the post).
Deduped: fires once per Sunday via already_sent_today / mark_sent.
"""

from __future__ import annotations

import math
from collections import defaultdict
from datetime import datetime, timedelta

import pytz

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

MONTHLY_TARGET   = 1_000_000.0
DAILY_TARGET     = MONTHLY_TARGET / 30.0          # ≈ $33,333/day
ALERT_KEY        = "revenue_gap_sunday"
ET               = pytz.timezone("America/New_York")
SUNDAY           = 6                               # weekday index

LOOKBACK         = 14                              # days fetched per dimension
PUBLISHER_WINDOW = 14                              # days for pub analysis
DP_WINDOW        = 7                               # days per WoW window
COUNTRY_WINDOW   = 14

# Publisher underperformance: flag if actual daily < this fraction of potential
UNDERPERFORM_THRESHOLD = 0.50
TARGET_WIN_RATE        = 0.10    # 10% win rate used to estimate publisher potential
MIN_BIDS_PUB           = 1_000   # ignore tiny publishers

# Demand partner decline threshold
DP_DECLINE_THRESHOLD   = 0.15   # flag if WoW revenue drop > 15%
MIN_DP_REVENUE         = 100.0  # minimum 7d revenue to qualify

# Geography gap: flag countries with eCPM above this multiple of network avg
COUNTRY_ECPM_RATIO     = 1.5
MIN_COUNTRY_IMPS       = 1_000  # ignore negligible markets


# ---------------------------------------------------------------------------
# Lazy imports
# ---------------------------------------------------------------------------

def _imports():
    from core.api   import fetch, n_days_ago, today, sf, fmt_usd
    from core.slack import send_text, already_sent_today, mark_sent
    from intelligence.claude_analyst import write_revenue_gap_memo
    return fetch, n_days_ago, today, sf, fmt_usd, send_text, already_sent_today, mark_sent, write_revenue_gap_memo


# ---------------------------------------------------------------------------
# Safe numeric helpers
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
        row.get("PUBLISHER_NAME") or row.get("PUBLISHER") or row.get("publisher") or "Unknown"
    ).strip() or "Unknown"


def _dp_name(row: dict) -> str:
    return str(
        row.get("DEMAND_PARTNER_NAME") or row.get("demand_partner_name") or "Unknown"
    ).strip() or "Unknown"


def _country_name(row: dict) -> str:
    return str(row.get("COUNTRY_NAME") or row.get("country_name") or "Unknown").strip() or "Unknown"


# ---------------------------------------------------------------------------
# Run-rate calculation
# ---------------------------------------------------------------------------

def _compute_run_rate(date_rows: list, days: int = 7) -> dict:
    """
    Compute the current daily run rate from the most-recent `days` days of
    DATE-breakdown data.

    Returns a dict with: daily_rate, monthly_rate, daily_gap, weekly_gap,
    monthly_gap, pct_of_target, daily_series.
    """
    daily: dict[str, float] = {}
    for row in date_rows:
        d   = str(row.get("DATE", ""))
        rev = _sf(row.get("GROSS_REVENUE"))
        if d:
            daily[d] = daily.get(d, 0.0) + rev

    sorted_days = sorted(daily.items())
    recent_days = sorted_days[-days:]                  # last `days` entries

    if not recent_days:
        return {"daily_rate": 0.0, "monthly_rate": 0.0, "daily_gap": DAILY_TARGET,
                "monthly_gap": MONTHLY_TARGET, "pct_of_target": 0.0, "daily_series": []}

    daily_rate    = sum(v for _, v in recent_days) / len(recent_days)
    monthly_rate  = daily_rate * 30
    daily_gap     = max(0.0, DAILY_TARGET - daily_rate)
    monthly_gap   = max(0.0, MONTHLY_TARGET - monthly_rate)
    pct_of_target = daily_rate / DAILY_TARGET * 100 if DAILY_TARGET > 0 else 0.0

    return {
        "daily_rate":    round(daily_rate, 2),
        "monthly_rate":  round(monthly_rate, 2),
        "daily_gap":     round(daily_gap, 2),
        "monthly_gap":   round(monthly_gap, 2),
        "pct_of_target": round(pct_of_target, 1),
        "n_days_used":   len(recent_days),
        "daily_series":  [{"date": d, "revenue": round(v, 2)} for d, v in sorted_days],
    }


# ---------------------------------------------------------------------------
# Publisher underperformance analysis
# ---------------------------------------------------------------------------

def _analyze_publisher_gaps(pub_rows: list) -> dict:
    """
    Find publishers earning significantly less than their bid-density potential.

    Potential daily revenue = (BIDS / window_days) × TARGET_WIN_RATE × eCPM / 1000
    Flag if actual_daily < UNDERPERFORM_THRESHOLD × potential_daily.

    Returns: { "underperformers": [...], "total_daily_gap": float,
               "total_publishers_analyzed": int }
    """
    underperformers = []

    for row in pub_rows:
        name  = _pub_name(row)
        if name == "Unknown":
            continue

        rev   = _sf(row.get("GROSS_REVENUE"))
        bids  = _sf(row.get("BIDS"))
        wins  = _sf(row.get("WINS"))
        imps  = _sf(row.get("IMPRESSIONS"))
        ecpm  = _sf(row.get("GROSS_ECPM"))
        floor = _sf(row.get("AVG_FLOOR_PRICE"))
        bid_p = _sf(row.get("AVG_BID_PRICE"))
        pay   = _sf(row.get("PUB_PAYOUT"))

        if bids < MIN_BIDS_PUB or ecpm <= 0:
            continue

        # If eCPM is missing, estimate from revenue and impressions
        if ecpm <= 0 and imps > 0:
            ecpm = rev / imps * 1000

        if ecpm <= 0:
            continue

        win_rate      = wins / bids if bids > 0 else 0.0
        actual_daily  = rev / PUBLISHER_WINDOW
        potential_daily = (bids / PUBLISHER_WINDOW) * TARGET_WIN_RATE * ecpm / 1000
        daily_gap     = max(0.0, potential_daily - actual_daily)

        if potential_daily <= 0:
            continue
        if actual_daily >= UNDERPERFORM_THRESHOLD * potential_daily:
            continue

        utilisation = actual_daily / potential_daily if potential_daily > 0 else 0.0
        margin      = (rev - pay) / rev * 100 if rev > 0 else 0.0

        underperformers.append({
            "publisher":       name,
            "actual_daily":    round(actual_daily, 2),
            "potential_daily": round(potential_daily, 2),
            "daily_gap":       round(daily_gap, 2),
            "utilisation_pct": round(utilisation * 100, 1),
            "win_rate_pct":    round(win_rate * 100, 2),
            "bids_7d":         int(bids),
            "current_floor":   round(floor, 3),
            "avg_bid":         round(bid_p, 3),
            "ecpm":            round(ecpm, 4),
            "revenue_14d":     round(rev, 2),
            "margin_pct":      round(margin, 1),
        })

    underperformers.sort(key=lambda x: x["daily_gap"], reverse=True)
    total_gap = sum(p["daily_gap"] for p in underperformers)

    return {
        "underperformers":            underperformers[:10],
        "total_daily_gap":            round(total_gap, 2),
        "total_publishers_analyzed":  len([r for r in pub_rows if _pub_name(r) != "Unknown"]),
        "n_flagged":                  len(underperformers),
    }


# ---------------------------------------------------------------------------
# Demand partner trend analysis
# ---------------------------------------------------------------------------

def _analyze_dp_trends(
    this_week_rows:  list,
    prior_week_rows: list,
) -> dict:
    """
    Compare demand partner revenue this week vs prior week.
    Flag partners with >DP_DECLINE_THRESHOLD WoW revenue drop.

    Returns: { "declining": [...], "total_recoverable_daily": float,
               "total_dps_analyzed": int }
    """
    def _sum_by_dp(rows: list) -> dict[str, float]:
        totals: dict[str, float] = defaultdict(float)
        for row in rows:
            name = _dp_name(row)
            if name != "Unknown":
                totals[name] += _sf(row.get("GROSS_REVENUE"))
        return dict(totals)

    this_rev  = _sum_by_dp(this_week_rows)
    prior_rev = _sum_by_dp(prior_week_rows)

    all_dps = set(this_rev) | set(prior_rev)
    declining = []

    for dp in all_dps:
        this_r  = this_rev.get(dp, 0.0)
        prior_r = prior_rev.get(dp, 0.0)

        if prior_r < MIN_DP_REVENUE:
            continue                           # too small to flag
        if this_r >= prior_r:
            continue                           # not declining
        if prior_r <= 0:
            continue

        drop_pct      = (prior_r - this_r) / prior_r
        if drop_pct < DP_DECLINE_THRESHOLD:
            continue

        lost_daily    = (prior_r - this_r) / 7.0
        recoverable   = lost_daily             # conservative: full recovery

        declining.append({
            "demand_partner":       dp,
            "this_week_rev":        round(this_r, 2),
            "prior_week_rev":       round(prior_r, 2),
            "wow_drop_pct":         round(drop_pct * 100, 1),
            "daily_revenue_lost":   round(lost_daily, 2),
            "recoverable_daily":    round(recoverable, 2),
        })

    declining.sort(key=lambda x: x["daily_revenue_lost"], reverse=True)
    total_recoverable = sum(d["recoverable_daily"] for d in declining)

    return {
        "declining":               declining[:8],
        "total_recoverable_daily": round(total_recoverable, 2),
        "total_dps_analyzed":      len(all_dps),
        "n_declining":             len(declining),
    }


# ---------------------------------------------------------------------------
# Country gap analysis
# ---------------------------------------------------------------------------

def _analyze_country_gaps(country_rows: list) -> dict:
    """
    Identify high-eCPM countries with low impression volume — supply gaps.

    A country is flagged when its eCPM exceeds COUNTRY_ECPM_RATIO × network
    average eCPM AND its impressions are below the median country impressions.

    Returns: { "gaps": [...], "total_daily_opportunity": float,
               "network_avg_ecpm": float, "n_countries": int }
    """
    countries = []
    for row in country_rows:
        name = _country_name(row)
        if name == "Unknown":
            continue

        rev  = _sf(row.get("GROSS_REVENUE"))
        imps = _sf(row.get("IMPRESSIONS"))
        wins = _sf(row.get("WINS"))
        bids = _sf(row.get("BIDS"))
        ecpm = _sf(row.get("GROSS_ECPM"))

        if imps < MIN_COUNTRY_IMPS:
            continue

        if ecpm <= 0 and imps > 0:
            ecpm = rev / imps * 1000

        win_rate = wins / bids if bids > 0 else 0.0

        countries.append({
            "country":      name,
            "revenue_14d":  round(rev, 2),
            "impressions":  int(imps),
            "ecpm":         round(ecpm, 4),
            "win_rate_pct": round(win_rate * 100, 2),
            "bids_14d":     int(bids),
        })

    if not countries:
        return {
            "gaps": [], "total_daily_opportunity": 0.0,
            "network_avg_ecpm": 0.0, "n_countries": 0,
        }

    total_rev  = sum(c["revenue_14d"] for c in countries)
    total_imps = sum(c["impressions"] for c in countries)
    net_avg_ecpm = total_rev / total_imps * 1000 if total_imps > 0 else 0.0

    imps_values = sorted(c["impressions"] for c in countries)
    median_imps = imps_values[len(imps_values) // 2]

    gaps = []
    for c in countries:
        ecpm_ratio = c["ecpm"] / net_avg_ecpm if net_avg_ecpm > 0 else 0.0
        if ecpm_ratio < COUNTRY_ECPM_RATIO:
            continue
        if c["impressions"] >= median_imps:
            continue

        # Opportunity: what if this country reached median impressions?
        imps_gap         = max(0, median_imps - c["impressions"])
        opportunity_14d  = imps_gap * c["ecpm"] / 1000
        opportunity_daily = opportunity_14d / COUNTRY_WINDOW

        gaps.append({
            **c,
            "ecpm_vs_network": round(ecpm_ratio, 2),
            "median_imps":     int(median_imps),
            "imps_gap":        int(imps_gap),
            "opportunity_daily": round(opportunity_daily, 2),
        })

    gaps.sort(key=lambda x: x["opportunity_daily"], reverse=True)
    total_opp = sum(g["opportunity_daily"] for g in gaps)

    return {
        "gaps":                    gaps[:8],
        "total_daily_opportunity": round(total_opp, 2),
        "network_avg_ecpm":        round(net_avg_ecpm, 4),
        "n_countries":             len(countries),
        "n_gaps":                  len(gaps),
    }


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def run():
    now_et  = datetime.now(ET)
    weekday = now_et.weekday()

    if weekday != SUNDAY:
        print(f"[revenue_gap] Not Sunday (weekday={weekday}). Exiting.")
        return

    (fetch, n_days_ago, today, sf, fmt_usd,
     send_text, already_sent_today, mark_sent,
     write_revenue_gap_memo) = _imports()

    if already_sent_today(ALERT_KEY):
        print("[revenue_gap] Already sent this Sunday. Exiting.")
        return

    date_end        = today()
    date_14d_start  = n_days_ago(LOOKBACK)
    date_7d_start   = n_days_ago(DP_WINDOW)
    date_prior_start = n_days_ago(LOOKBACK)       # prior DP window = days 8-14
    date_prior_end  = n_days_ago(DP_WINDOW + 1)   # day 8 ago through day 14 ago

    print(f"[revenue_gap] Fetching data for {date_14d_start} → {date_end}…")

    # ── Fetch all data ────────────────────────────────────────────────────────
    date_rows = []
    try:
        date_rows = fetch("DATE", "GROSS_REVENUE,PUB_PAYOUT", date_14d_start, date_end)
        print(f"[revenue_gap] DATE: {len(date_rows)} rows")
    except Exception as exc:
        print(f"[revenue_gap] DATE fetch failed: {exc}")
        return

    pub_rows = []
    try:
        pub_rows = fetch(
            "PUBLISHER",
            "GROSS_REVENUE,PUB_PAYOUT,BIDS,WINS,IMPRESSIONS,GROSS_ECPM,AVG_FLOOR_PRICE,AVG_BID_PRICE",
            date_14d_start, date_end,
        )
        print(f"[revenue_gap] PUBLISHER: {len(pub_rows)} rows")
    except Exception as exc:
        print(f"[revenue_gap] PUBLISHER fetch failed (non-fatal): {exc}")

    dp_this_rows, dp_prior_rows = [], []
    try:
        dp_this_rows  = fetch("DEMAND_PARTNER", "GROSS_REVENUE,BIDS,WINS,IMPRESSIONS,GROSS_ECPM",
                               date_7d_start, date_end)
        dp_prior_rows = fetch("DEMAND_PARTNER", "GROSS_REVENUE,BIDS,WINS,IMPRESSIONS,GROSS_ECPM",
                               date_prior_start, date_prior_end)
        print(f"[revenue_gap] DEMAND_PARTNER this/prior: {len(dp_this_rows)}/{len(dp_prior_rows)} rows")
    except Exception as exc:
        print(f"[revenue_gap] DEMAND_PARTNER fetch failed (non-fatal): {exc}")

    country_rows = []
    try:
        country_rows = fetch(
            "COUNTRY_NAME",
            "GROSS_REVENUE,IMPRESSIONS,WINS,BIDS,GROSS_ECPM",
            date_14d_start, date_end,
        )
        print(f"[revenue_gap] COUNTRY_NAME: {len(country_rows)} rows")
    except Exception as exc:
        print(f"[revenue_gap] COUNTRY_NAME fetch failed (non-fatal): {exc}")

    if not date_rows:
        print("[revenue_gap] No date data — aborting.")
        return

    # ── Compute gap and three analyses ───────────────────────────────────────
    run_rate    = _compute_run_rate(date_rows, days=7)
    pub_gaps    = _analyze_publisher_gaps(pub_rows)    if pub_rows    else {"underperformers": [], "total_daily_gap": 0.0, "n_flagged": 0}
    dp_trends   = _analyze_dp_trends(dp_this_rows, dp_prior_rows)
    country_gaps = _analyze_country_gaps(country_rows) if country_rows else {"gaps": [], "total_daily_opportunity": 0.0}

    daily_gap      = run_rate["daily_gap"]
    total_addressable = (
        pub_gaps.get("total_daily_gap", 0.0)
        + dp_trends.get("total_recoverable_daily", 0.0)
        + country_gaps.get("total_daily_opportunity", 0.0)
    )

    print(
        f"[revenue_gap] Daily rate: ${run_rate['daily_rate']:,.0f}  "
        f"Gap to target: ${daily_gap:,.0f}/day  "
        f"Addressable: ${total_addressable:,.0f}/day"
    )

    # ── Ask Claude to write the memo ─────────────────────────────────────────
    week_label = f"{date_14d_start} – {date_end}"
    try:
        memo = write_revenue_gap_memo(
            run_rate      = run_rate,
            pub_gaps      = pub_gaps,
            dp_trends     = dp_trends,
            country_gaps  = country_gaps,
            daily_target  = DAILY_TARGET,
            monthly_target = MONTHLY_TARGET,
            week_label    = week_label,
        )
        print("[revenue_gap] Claude memo generated.")
    except Exception as exc:
        print(f"[revenue_gap] Claude failed: {exc}")
        # Fallback: bare-bones summary
        memo = (
            f"*Revenue Gap — Week of {week_label}*\n\n"
            f"Current run rate: ${run_rate['daily_rate']:,.0f}/day "
            f"({run_rate['pct_of_target']:.0f}% of the ${DAILY_TARGET:,.0f}/day target)\n"
            f"Gap to $1M/mo: ${daily_gap:,.0f}/day (${daily_gap*30:,.0f}/mo)\n\n"
            f"Addressable opportunity identified: ${total_addressable:,.0f}/day\n"
            f"  • Publisher floor pressure: ${pub_gaps.get('total_daily_gap',0):,.0f}/day "
            f"across {pub_gaps.get('n_flagged',0)} publishers\n"
            f"  • Recoverable DP spend: ${dp_trends.get('total_recoverable_daily',0):,.0f}/day "
            f"across {dp_trends.get('n_declining',0)} declining partners\n"
            f"  • Country supply gaps: ${country_gaps.get('total_daily_opportunity',0):,.0f}/day "
            f"across {country_gaps.get('n_gaps',0)} markets\n\n"
            f"_Claude memo unavailable: {exc}_"
        )

    # ── Reality-check the memo against live LL state ─────────────────────────
    # If the upstream data fed to the LLM doesn't match current state,
    # replace the memo with a "data stale" alert. Prevents misleading prose
    # like the 2026-04-26 BidMachine $3.25 floor recommendation that pointed
    # at floors which didn't exist.
    try:
        from intelligence.advisory_verifier import verify_or_replace
        memo, was_replaced, issues = verify_or_replace(
            memo,
            source_data={
                "pub_gaps": pub_gaps,
                "dp_trends": dp_trends,
                "country_gaps": country_gaps,
            },
            advisory_label=f"Sunday revenue memo {week_label}",
        )
        if was_replaced:
            print(f"[revenue_gap] Memo REPLACED — {len(issues)} fact-check issues:")
            for issue in issues[:5]:
                print(f"   • {issue.get('explanation','')}")
        else:
            print("[revenue_gap] Memo verified vs live LL state.")
    except Exception as exc:
        print(f"[revenue_gap] Verifier failed (posting memo as-is): {exc}")

    # ── Post to Slack ─────────────────────────────────────────────────────────
    try:
        send_text(memo)
        mark_sent(ALERT_KEY)
        print("[revenue_gap] Slack memo posted.")
    except Exception as exc:
        print(f"[revenue_gap] Slack post failed: {exc}")


if __name__ == "__main__":
    run()
