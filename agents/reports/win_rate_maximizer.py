"""
agents/reports/win_rate_maximizer.py
──────────────────────────────────────────────────────────────────────────────
Win-rate maximiser — finds publisher × demand-partner combinations that are
receiving strong bid volume but losing too many auctions, indicating the floor
price is just slightly too high.

Algorithm
─────────
1. Fetch PUBLISHER,DEMAND_PARTNER for the last 7 days.
2. Filter: bids > 2,000 AND win_rate < 8% AND revenue > $50.
3. Per combination, estimate the floor adjustment to reach 10% win rate:

       new_floor = current_floor × (1 − target_wr) / (1 − current_wr)

   Derivation: modelling bids as uniformly distributed on [0, bid_max], the
   win probability is (bid_max − floor) / bid_max.  Calibrating bid_max from
   the current (floor, win_rate) pair:
       current_wr = (bid_max − floor) / bid_max  →  bid_max = floor / (1 − wr)
   Substituting the same bid_max for the target win rate:
       new_floor = floor × (1 − target_wr) / (1 − current_wr)

4. Estimate additional daily revenue:
       additional_wins_per_day = (bids/7) × (target_wr − current_wr)
       additional_rev_per_day  = additional_wins × ecpm_conservative / 1000
   where  ecpm_conservative = current_ecpm × 0.85  (discounts eCPM dilution).

5. Rank by recoverable daily revenue; send top 10 to Claude for review.
6. Post Slack alert when total recoverable revenue > $500/day (daily dedup).
7. Export top-N data for the daily email via export_win_rate_section().
"""

from __future__ import annotations

import math
from collections import defaultdict
from datetime import datetime
from pathlib import Path

import pytz

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

LOOKBACK_DAYS      = 7
BREAKDOWN          = "PUBLISHER,DEMAND_PARTNER"
METRICS            = "GROSS_REVENUE,BIDS,WINS,IMPRESSIONS,GROSS_ECPM,AVG_BID_PRICE"

# Separate publisher-level fetch for floor prices (not available in DP breakdown)
BREAKDOWN_PUB      = "PUBLISHER"
METRICS_PUB        = "GROSS_REVENUE,BIDS,WINS,AVG_FLOOR_PRICE,AVG_BID_PRICE,GROSS_ECPM"

MIN_BIDS           = 2_000   # minimum 7-day bids
MAX_WIN_RATE       = 0.08    # flag combos with win rate below 8%
MIN_REVENUE        = 50.0    # minimum 7-day gross revenue ($)
TARGET_WIN_RATE    = 0.10    # target win rate after floor adjustment
ECPM_DISCOUNT      = 0.85    # conservative eCPM estimate after floor lowering
TOP_FOR_CLAUDE     = 10
SLACK_THRESHOLD    = 500.0   # alert if total daily recovery > $500
ALERT_KEY          = "win_rate_maximizer_daily"

ET                 = pytz.timezone("America/New_York")


# ---------------------------------------------------------------------------
# Lazy imports
# ---------------------------------------------------------------------------

def _imports():
    from core.api    import fetch, n_days_ago, today, sf, fmt_usd, fmt_n
    from core.slack  import send_blocks, already_sent_today, mark_sent
    from core.ui_nav import demand_seat_floor
    from intelligence.claude_analyst import analyze_win_rate_maximizer
    return (fetch, n_days_ago, today, sf, fmt_usd, fmt_n,
            send_blocks, already_sent_today, mark_sent,
            analyze_win_rate_maximizer, demand_seat_floor)


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
        row.get("PUBLISHER_NAME") or row.get("PUBLISHER") or row.get("publisher") or "Unknown"
    ).strip() or "Unknown"


def _dp_name(row: dict) -> str:
    return str(
        row.get("DEMAND_PARTNER_NAME") or row.get("demand_partner_name") or "Unknown"
    ).strip() or "Unknown"


# ---------------------------------------------------------------------------
# Core analysis
# ---------------------------------------------------------------------------

def _estimate_new_floor(current_floor: float, current_wr: float,
                        target_wr: float = TARGET_WIN_RATE) -> float:
    """
    Estimate the floor price that achieves target_wr, given the current pair.
    Returns 0.0 if the model cannot be calibrated (e.g. current_wr == 1.0).
    """
    denominator = 1.0 - current_wr
    if denominator <= 0:
        return 0.0
    return current_floor * (1.0 - target_wr) / denominator


def _floor_adjustment_pct(current_floor: float, new_floor: float) -> float:
    if current_floor <= 0:
        return 0.0
    return (new_floor - current_floor) / current_floor * 100.0


def build_publisher_floors(pub_rows: list) -> dict[str, float]:
    """
    Build a publisher → avg_floor_price mapping from PUBLISHER breakdown rows.
    Used to join floor data onto PUBLISHER,DEMAND_PARTNER rows.
    """
    floors: dict[str, float] = {}
    for row in pub_rows:
        pub   = _pub_name(row)
        floor = _sf(row.get("AVG_FLOOR_PRICE"))
        if pub != "Unknown" and floor > 0:
            floors[pub] = floor
    return floors


def analyze_combinations(rows: list, pub_floors: dict[str, float] | None = None) -> list[dict]:
    """
    Parse raw API rows, apply filters, compute floor adjustments and revenue
    estimates, and return sorted list of opportunity dicts.

    Args:
        rows       (list): PUBLISHER,DEMAND_PARTNER breakdown rows.
        pub_floors (dict): Optional publisher → floor price map from a separate
                           PUBLISHER breakdown fetch.  When supplied, overrides
                           any AVG_FLOOR_PRICE present in the row.
    """
    combos: list[dict] = []

    for row in rows:
        pub   = _pub_name(row)
        dp    = _dp_name(row)
        bids  = _sf(row.get("BIDS"))
        wins  = _sf(row.get("WINS"))
        rev   = _sf(row.get("GROSS_REVENUE"))
        imps  = _sf(row.get("IMPRESSIONS"))
        ecpm  = _sf(row.get("GROSS_ECPM"))
        bid_p = _sf(row.get("AVG_BID_PRICE"))

        # Floor: prefer pub-level map (more reliable), fall back to row value
        floor = (pub_floors or {}).get(pub) or _sf(row.get("AVG_FLOOR_PRICE"))

        if pub == "Unknown" or dp == "Unknown":
            continue
        if bids < MIN_BIDS:
            continue
        if rev < MIN_REVENUE:
            continue

        win_rate = wins / bids if bids > 0 else 0.0
        if win_rate >= MAX_WIN_RATE:
            continue
        if floor <= 0:
            continue

        new_floor = _estimate_new_floor(floor, win_rate, TARGET_WIN_RATE)
        if new_floor <= 0 or new_floor >= floor:
            continue

        adj_pct       = _floor_adjustment_pct(floor, new_floor)
        bids_per_day  = bids / LOOKBACK_DAYS
        delta_wr      = TARGET_WIN_RATE - win_rate
        add_wins_day  = bids_per_day * delta_wr
        ecpm_cons     = ecpm * ECPM_DISCOUNT if ecpm > 0 else bid_p * 1000 * TARGET_WIN_RATE * ECPM_DISCOUNT
        add_rev_day   = add_wins_day * ecpm_cons / 1000
        add_rev_week  = add_rev_day * 7

        # Margin impact proxy: lower floor → lower avg clearing price
        # Rough estimate: margin impact ≈ (floor - new_floor) × wins_per_day / 1000
        margin_impact_day = (floor - new_floor) * (wins / LOOKBACK_DAYS) / 1000

        combos.append({
            "publisher":         pub,
            "demand_partner":    dp,
            "bids_7d":           int(bids),
            "wins_7d":           int(wins),
            "revenue_7d":        round(rev, 2),
            "win_rate_pct":      round(win_rate * 100, 3),
            "target_win_rate_pct": TARGET_WIN_RATE * 100,
            "current_floor":     round(floor, 3),
            "new_floor":         round(new_floor, 3),
            "floor_adj_pct":     round(adj_pct, 1),
            "avg_bid":           round(bid_p, 3),
            "ecpm_current":      round(ecpm, 4),
            "ecpm_conservative": round(ecpm_cons, 4),
            "bids_per_day":      round(bids_per_day, 0),
            "add_wins_per_day":  round(add_wins_day, 0),
            "add_rev_per_day":   round(add_rev_day, 2),
            "add_rev_per_week":  round(add_rev_week, 2),
            "margin_impact_day": round(margin_impact_day, 2),
        })

    combos.sort(key=lambda c: c["add_rev_per_day"], reverse=True)
    return combos


# ---------------------------------------------------------------------------
# Export for daily email
# ---------------------------------------------------------------------------

def export_win_rate_section(top_n: int = 10) -> dict:
    """
    Run the analysis and return structured data for the daily email report.
    Returns {} on failure.

    Importable by agents/reports/daily_email.py.
    """
    try:
        from core.api import fetch, n_days_ago, today
    except Exception as exc:
        print(f"[win_rate_maximizer/export] Import failed: {exc}")
        return {}

    start = n_days_ago(LOOKBACK_DAYS)
    end   = today()

    try:
        rows     = fetch(BREAKDOWN,     METRICS,     start, end)
        pub_rows = fetch(BREAKDOWN_PUB, METRICS_PUB, start, end)
    except Exception as exc:
        print(f"[win_rate_maximizer/export] Fetch failed: {exc}")
        return {}

    pub_floors = build_publisher_floors(pub_rows)
    combos     = analyze_combinations(rows, pub_floors)
    if not combos:
        return {}

    total_daily = sum(c["add_rev_per_day"] for c in combos)
    total_weekly = sum(c["add_rev_per_week"] for c in combos)

    return {
        "top_combinations":    combos[:top_n],
        "total_combos_found":  len(combos),
        "total_daily_recovery": round(total_daily, 2),
        "total_weekly_recovery": round(total_weekly, 2),
        "date_range":          f"{start} to {end}",
    }


# ---------------------------------------------------------------------------
# Slack Block Kit builder
# ---------------------------------------------------------------------------

def _win_rate_bar(wr_pct: float, target_pct: float = 10.0, width: int = 8) -> str:
    ratio  = min(wr_pct / target_pct, 1.0) if target_pct > 0 else 0.0
    filled = max(0, min(int(ratio * width), width))
    return "█" * filled + "░" * (width - filled)


def _build_slack_blocks(
    combos:       list[dict],
    claude_picks: list[dict],   # [{publisher, demand_partner, recommended_floor, weekly_recovery, rationale}]
    total_daily:  float,
    date_range:   str,
    top_n:        int = 8,
) -> list:
    total_weekly = total_daily * 7

    blocks: list = [
        {
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": ":dart:  Win Rate Maximiser — Floor Pressure Alert",
            },
        },
        {
            "type": "context",
            "elements": [{
                "type": "mrkdwn",
                "text": (
                    f"*{date_range}*  ·  {len(combos)} qualifying combinations  ·  "
                    f"Total recoverable: *${total_daily:,.0f}/day*  "
                    f"(${total_weekly:,.0f}/wk)"
                ),
            }],
        },
        {"type": "divider"},
    ]

    # Claude's reviewed recommendations
    if claude_picks:
        rec_lines = []
        rank_emojis = [":one:", ":two:", ":three:", ":four:", ":five:",
                       ":six:", ":seven:", ":eight:", ":nine:", ":keycap_ten:"]
        for i, p in enumerate(claude_picks):
            floor_s   = f"${p.get('recommended_floor', 0):.3f}" if p.get('recommended_floor') else "—"
            rec_wkly  = p.get("weekly_recovery", 0)
            rationale = p.get("rationale", "")
            rec_lines.append(
                f"{rank_emojis[i]}  *{p['publisher']}*  ×  _{p['demand_partner']}_\n"
                f"    Floor → `{floor_s}`  ·  Est. +${rec_wkly:,.0f}/wk  ·  {rationale}"
            )
        blocks += [
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": "*:bulb: Claude-Reviewed Recommendations*\n" + "\n\n".join(rec_lines),
                },
            },
            {"type": "divider"},
        ]

    # Top combinations table
    blocks.append({
        "type": "section",
        "text": {
            "type": "mrkdwn",
            "text": f"*:bar_chart: All Flagged Combinations (top {min(top_n, len(combos))})*",
        },
    })

    for c in combos[:top_n]:
        bar = _win_rate_bar(c["win_rate_pct"])
        blocks.append({
            "type": "section",
            "fields": [
                {
                    "type": "mrkdwn",
                    "text": (
                        f"*{c['publisher']}*  ×  _{c['demand_partner']}_\n"
                        f"`{bar}` {c['win_rate_pct']:.2f}% win rate  "
                        f"({c['bids_7d']:,} bids/7d)"
                    ),
                },
                {
                    "type": "mrkdwn",
                    "text": (
                        f"Floor `${c['current_floor']:.3f}` → `${c['new_floor']:.3f}` "
                        f"_{c['floor_adj_pct']:+.1f}%_\n"
                        f"*+${c['add_rev_per_day']:,.0f}/day*  ·  "
                        f"+${c['add_rev_per_week']:,.0f}/wk  ·  "
                        f"Rev 7d: ${c['revenue_7d']:,.0f}"
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
                    f":information_source:  Filter: bids > {MIN_BIDS:,}  ·  win rate < {MAX_WIN_RATE*100:.0f}%  ·  "
                    f"revenue > ${MIN_REVENUE:.0f}/7d.  "
                    f"New floor formula: current_floor × (1 − {TARGET_WIN_RATE*100:.0f}%) / (1 − current_wr).  "
                    f"eCPM discounted {(1-ECPM_DISCOUNT)*100:.0f}% for floor dilution.  "
                    f"Margin impact reflects lower clearing prices on existing wins."
                ),
            }],
        },
    ]

    return blocks


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def run():
    (fetch, n_days_ago, today, sf, fmt_usd, fmt_n,
     send_blocks, already_sent_today, mark_sent,
     analyze_win_rate_maximizer, demand_seat_floor) = _imports()

    if already_sent_today(ALERT_KEY):
        print("[win_rate_maximizer] Already sent today. Exiting.")
        return

    start = n_days_ago(LOOKBACK_DAYS)
    end   = today()

    print(f"[win_rate_maximizer] Fetching {start} → {end}…")
    try:
        rows     = fetch(BREAKDOWN,     METRICS,     start, end)
        pub_rows = fetch(BREAKDOWN_PUB, METRICS_PUB, start, end)
    except Exception as exc:
        print(f"[win_rate_maximizer] Fetch failed: {exc}")
        return

    if not rows:
        print("[win_rate_maximizer] No data returned.")
        return

    pub_floors = build_publisher_floors(pub_rows)
    combos     = analyze_combinations(rows, pub_floors)
    if not combos:
        print("[win_rate_maximizer] No combinations meet filter criteria.")
        return

    total_daily  = sum(c["add_rev_per_day"] for c in combos)
    total_weekly = sum(c["add_rev_per_week"] for c in combos)

    print(f"[win_rate_maximizer] {len(combos)} combinations found. "
          f"Total recoverable: ${total_daily:,.0f}/day  (${total_weekly:,.0f}/wk)")

    # Threshold gate — only alert if opportunity is large enough
    if total_daily < SLACK_THRESHOLD:
        print(f"[win_rate_maximizer] Total daily recovery ${total_daily:,.0f} < "
              f"threshold ${SLACK_THRESHOLD:,.0f}. No alert.")
        return

    # Claude analysis
    claude_picks: list[dict] = []
    try:
        claude_picks = analyze_win_rate_maximizer(combos[:TOP_FOR_CLAUDE])
        print(f"[win_rate_maximizer] Claude reviewed {len(claude_picks)} picks.")
    except Exception as exc:
        print(f"[win_rate_maximizer] Claude failed (non-fatal): {exc}")
        # Specific fallback with exact floor values and execute steps
        claude_picks = [
            {
                "publisher":         c["publisher"],
                "demand_partner":    c["demand_partner"],
                "recommended_floor": c["new_floor"],
                "weekly_recovery":   c["add_rev_per_week"],
                "rationale": (
                    f"Win rate {c['win_rate_pct']:.2f}% on {c['bids_7d']:,} bids — "
                    f"floor ${c['current_floor']:.3f} → ${c['new_floor']:.3f} "
                    f"targets {TARGET_WIN_RATE*100:.0f}% win rate "
                    f"(+${c['add_rev_per_week']:,.0f}/wk).\n"
                    + demand_seat_floor(
                        c["publisher"], c["demand_partner"],
                        c["current_floor"], c["new_floor"],
                    )
                ),
            }
            for c in combos[:TOP_FOR_CLAUDE]
        ]

    # Build and post Slack message
    date_range = f"{start} → {end}"
    blocks     = _build_slack_blocks(combos, claude_picks, total_daily, date_range, top_n=8)
    fallback   = (
        f":dart: Win Rate Alert | {len(combos)} combos | "
        f"Recoverable: ${total_daily:,.0f}/day"
    )

    try:
        send_blocks(blocks, text=fallback)
        mark_sent(ALERT_KEY)
        print("[win_rate_maximizer] Slack alert posted.")
    except Exception as exc:
        print(f"[win_rate_maximizer] Slack post failed: {exc}")

    # Log each qualifying combination to the action tracker
    try:
        from agents.alerts.action_tracker import log_recommendation
        for combo in combos:
            try:
                log_recommendation(
                    agent_name              = "win_rate_maximizer",
                    publisher               = combo["publisher"],
                    metric_affected         = "avg_floor_price",
                    recommended_change      = (
                        f"Lower floor for {combo['demand_partner']} from "
                        f"${combo['current_floor']:.3f} to ${combo['new_floor']:.3f} "
                        f"(win rate {combo['win_rate_pct']:.2f}% → target {TARGET_WIN_RATE*100:.0f}%)"
                    ),
                    expected_impact_dollars = combo["add_rev_per_day"],
                )
            except Exception as exc:
                print(f"[win_rate_maximizer] log_recommendation failed for "
                      f"{combo['publisher']}×{combo['demand_partner']}: {exc}")
    except ImportError as exc:
        print(f"[win_rate_maximizer] action_tracker import failed: {exc}")


if __name__ == "__main__":
    run()
