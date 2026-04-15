"""
agents/reports/floor_elasticity.py

Finds the mathematically optimal floor price for each publisher using a
30-day elasticity model.

Model design
------------
We observe daily snapshots of (floor_price, bid_price, win_rate, revenue) for
each publisher. The core insight is that win_rate is a decreasing function of
the floor-to-bid ratio: as the floor climbs toward (and past) the average bid,
fewer bids clear the floor and win rate falls.

We fit a linear model per publisher:

    win_rate  =  α  +  β · (floor / avg_bid)          β < 0

Then the expected daily revenue at any proposed floor f is:

    revenue(f) = daily_bids · win_rate(f) · f
               = daily_bids · [α + β·(f/avg_bid)] · f
               = daily_bids · [α·f − |β|·f²/avg_bid]

Setting d(revenue)/df = 0 and solving:

    optimal_floor  =  α · avg_bid / (2 · |β|)

This is the closed-form optimum. We clamp it to [0.1·avg_bid, 0.95·avg_bid] to
avoid degenerate predictions, and discard any publisher where the slope β is
non-negative (flat or upward-sloping win rate — model assumptions violated).

Confidence score
----------------
    confidence = 0.6 · R²  +  0.4 · (1 − CoV_revenue)

where CoV = coefficient of variation of daily revenue (lower variance → higher
confidence). Scores range 0–1 and are clamped to [0, 1].

Eligibility filters
-------------------
  ≥ 20 days with non-zero data in the 30-day window
  ≥ 5,000 daily average bids
  ≥ 3 distinct observed floor prices   (need variance for a meaningful fit)
  β < 0                               (win rate must decrease as floor rises)

Output
------
  · get_optimization_data(top_n)  — importable by email / other agents
  · run()                         — Slack weekly summary (Mondays only, once/day)
"""

import math
from datetime import datetime, date

import numpy as np
import pytz

from core.api import fetch, n_days_ago, today, sf, fmt_usd, fmt_n, pct
from core.config import THRESHOLDS
from core.slack import already_sent_today, mark_sent, send_blocks
from core.ui_nav import floor_change
from intelligence.claude_analyst import analyze_floor_elasticity

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
BREAKDOWN       = "DATE,PUBLISHER"       # daily × publisher granularity
METRICS         = [
    "GROSS_REVENUE", "WINS", "BIDS",
    "IMPRESSIONS", "AVG_FLOOR_PRICE", "AVG_BID_PRICE",
]
LOOKBACK_DAYS   = 30
MIN_DATA_DAYS   = 20        # minimum days with usable data per publisher
MIN_DAILY_BIDS  = 5_000     # minimum average daily bids
MIN_FLOOR_VALS  = 3         # minimum distinct floor prices (for variance)
TOP_FOR_CLAUDE  = 10        # send this many to Claude for prioritisation
FLOOR_MIN_FRAC  = 0.10      # clamp optimal floor ≥ 10% of avg_bid
FLOOR_MAX_FRAC  = 0.95      # clamp optimal floor ≤ 95% of avg_bid
ALERT_KEY       = "floor_elasticity_weekly"
ET              = pytz.timezone("US/Eastern")


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


def _row_date(row: dict) -> str:
    return str(
        row.get("DATE") or row.get("date")
        or row.get("reportDate") or row.get("report_date") or ""
    )


def _group_by_publisher(rows: list) -> dict[str, list[dict]]:
    """Group daily rows by publisher name."""
    groups: dict[str, list] = {}
    for row in rows:
        name = _pub_name(row)
        groups.setdefault(name, []).append(row)
    return groups


def _parse_daily(row: dict) -> dict | None:
    """
    Extract a usable daily observation. Returns None if critical metrics are
    missing or zero (API lag / weekend gaps).
    """
    revenue   = _extract(row, "GROSS_REVENUE", "gross_revenue")
    bids      = _extract(row, "BIDS",          "bids")
    wins      = _extract(row, "WINS",          "wins")
    imps      = _extract(row, "IMPRESSIONS",   "impressions")
    avg_floor = _extract(row, "AVG_FLOOR_PRICE", "avg_floor_price", "avgFloorPrice")
    avg_bid   = _extract(row, "AVG_BID_PRICE",   "avg_bid_price",   "avgBidPrice")

    if bids < 100 or avg_bid <= 0 or avg_floor <= 0:
        return None

    win_rate    = wins / bids                       # fraction, not percent
    floor_ratio = avg_floor / avg_bid
    ecpm        = (revenue / imps * 1_000) if imps > 0 else 0.0

    return {
        "date":        _row_date(row),
        "revenue":     revenue,
        "bids":        bids,
        "wins":        wins,
        "imps":        imps,
        "avg_floor":   avg_floor,
        "avg_bid":     avg_bid,
        "win_rate":    win_rate,
        "floor_ratio": floor_ratio,
        "ecpm":        ecpm,
    }


# ---------------------------------------------------------------------------
# Elasticity model
# ---------------------------------------------------------------------------

def _fit_model(daily: list[dict]) -> dict | None:
    """
    Fit win_rate = α + β·floor_ratio and return model params.
    Returns None if the fit is degenerate or eligibility is not met.
    """
    if len(daily) < MIN_DATA_DAYS:
        return None

    avg_daily_bids = sum(d["bids"] for d in daily) / len(daily)
    if avg_daily_bids < MIN_DAILY_BIDS:
        return None

    floor_vals = [d["avg_floor"] for d in daily]
    if len(set(round(f, 4) for f in floor_vals)) < MIN_FLOOR_VALS:
        return None  # No variance in floor — model would be meaningless

    x = np.array([d["floor_ratio"] for d in daily])
    y = np.array([d["win_rate"]    for d in daily])

    # Linear fit: y = α + β·x
    coeffs = np.polyfit(x, y, deg=1)
    beta, alpha = float(coeffs[0]), float(coeffs[1])

    # β must be negative: win rate decreases as floor rises
    if beta >= 0:
        return None

    # R² of the fit
    y_hat  = np.polyval(coeffs, x)
    ss_res = float(np.sum((y - y_hat) ** 2))
    ss_tot = float(np.sum((y - y.mean()) ** 2))
    r2     = 1.0 - (ss_res / ss_tot) if ss_tot > 0 else 0.0

    # Coefficient of variation of daily revenue (lower = more stable)
    revs = np.array([d["revenue"] for d in daily])
    cov  = float(revs.std() / revs.mean()) if revs.mean() > 0 else 1.0
    cov  = min(cov, 1.0)   # cap at 1.0 so it doesn't dominate

    confidence = float(np.clip(0.6 * r2 + 0.4 * (1.0 - cov), 0.0, 1.0))

    return {
        "alpha":          alpha,
        "beta":           beta,
        "r_squared":      round(r2, 4),
        "confidence":     round(confidence, 4),
        "avg_daily_bids": round(avg_daily_bids, 1),
        "days_of_data":   len(daily),
    }


def _optimal_floor(model: dict, avg_bid: float) -> float:
    """
    Closed-form optimal floor from the linear win-rate model.

    revenue(f) = D · (α·f − |β|·f²/avg_bid)
    f*         = α · avg_bid / (2 · |β|)
    """
    alpha = model["alpha"]
    beta  = abs(model["beta"])   # positive magnitude
    if beta == 0:
        return avg_bid * FLOOR_MAX_FRAC
    f_star = alpha * avg_bid / (2.0 * beta)
    # Clamp to a sensible range
    return float(np.clip(f_star, avg_bid * FLOOR_MIN_FRAC, avg_bid * FLOOR_MAX_FRAC))


def _project_revenue(model: dict, floor: float, avg_bid: float, daily_bids: float) -> float:
    """
    Estimate daily revenue at a given floor using the fitted model.

    revenue = daily_bids · win_rate(floor) · floor
    """
    floor_ratio = floor / avg_bid if avg_bid > 0 else 0.0
    win_rate    = model["alpha"] + model["beta"] * floor_ratio
    win_rate    = max(win_rate, 0.0)     # can't be negative
    return daily_bids * win_rate * floor


# ---------------------------------------------------------------------------
# Per-publisher optimisation
# ---------------------------------------------------------------------------

def _optimise_publisher(name: str, daily: list[dict]) -> dict | None:
    """
    Run the full elasticity pipeline for one publisher.
    Returns an optimisation result dict or None if ineligible.
    """
    model = _fit_model(daily)
    if model is None:
        return None

    # Representative averages for projection
    avg_bid    = float(np.mean([d["avg_bid"]   for d in daily]))
    avg_floor  = float(np.mean([d["avg_floor"] for d in daily]))
    avg_rev    = float(np.mean([d["revenue"]   for d in daily]))

    opt_floor      = _optimal_floor(model, avg_bid)
    current_rev    = _project_revenue(model, avg_floor,  avg_bid, model["avg_daily_bids"])
    projected_rev  = _project_revenue(model, opt_floor,  avg_bid, model["avg_daily_bids"])
    uplift         = projected_rev - current_rev
    uplift_pct     = (uplift / current_rev * 100) if current_rev > 0 else 0.0

    # Discard if projected uplift is trivially small
    if abs(uplift) < 1.0:
        return None

    direction = "raise" if opt_floor > avg_floor else "lower"

    return {
        "publisher":          name,
        "current_floor":      round(avg_floor, 4),
        "optimal_floor":      round(opt_floor, 4),
        "current_daily_rev":  round(avg_rev, 2),          # observed average
        "projected_daily_rev":round(projected_rev, 2),    # model projection
        "daily_rev_uplift":   round(uplift, 2),
        "uplift_pct":         round(uplift_pct, 2),
        "confidence":         model["confidence"],
        "r_squared":          model["r_squared"],
        "days_of_data":       model["days_of_data"],
        "avg_daily_bids":     round(model["avg_daily_bids"], 0),
        "avg_bid_price":      round(avg_bid, 4),
        "direction":          direction,
    }


# ---------------------------------------------------------------------------
# Public data export (used by email report and other agents)
# ---------------------------------------------------------------------------

def get_optimization_data(top_n: int = 10) -> list[dict]:
    """
    Run the full floor elasticity analysis and return the top_n results
    sorted by abs(daily_rev_uplift) × confidence.

    This is importable by agents/reports/daily_email.py and other consumers.
    Returns [] on any fetch / computation failure.
    """
    start_date = n_days_ago(LOOKBACK_DAYS)
    end_date   = today()

    try:
        rows = fetch(BREAKDOWN, METRICS, start_date, end_date)
    except Exception as exc:
        print(f"[floor_elasticity] Fetch failed: {exc}")
        return []

    if not rows:
        return []

    pub_groups = _group_by_publisher(rows)
    results    = []

    for name, raw_rows in pub_groups.items():
        daily = [r for r in (_parse_daily(rr) for rr in raw_rows) if r is not None]
        opt   = _optimise_publisher(name, daily)
        if opt:
            results.append(opt)

    # Score = abs uplift × confidence — balances impact with reliability
    results.sort(key=lambda r: abs(r["daily_rev_uplift"]) * r["confidence"], reverse=True)
    return results[:top_n]


# ---------------------------------------------------------------------------
# Slack Block Kit builder
# ---------------------------------------------------------------------------

def _priority_emoji(priority: str) -> str:
    return {"high": ":red_circle:", "medium": ":large_yellow_circle:", "low": ":large_green_circle:"}.get(
        priority.lower(), ":white_circle:"
    )


def _direction_arrow(direction: str) -> str:
    return ":arrow_up_small:" if direction == "raise" else ":arrow_down_small:"


def _confidence_bar(score: float, width: int = 8) -> str:
    filled = max(0, min(int(score * width), width))
    return "█" * filled + "░" * (width - filled)


def _build_weekly_blocks(
    ranked: list[dict],          # Claude's ranked list (dicts with publisher, rank, priority, rationale, caution)
    opt_by_name: dict[str, dict],
    date_label: str,
    now_label: str,
    total_analysed: int,
    total_eligible: int,
) -> list:
    total_weekly_uplift = sum(
        opt_by_name.get(r["publisher"], {}).get("daily_rev_uplift", 0) * 7
        for r in ranked
    )

    blocks = [
        {
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": f":bar_chart:  Floor Elasticity Report — Week of {date_label}",
                "emoji": True,
            },
        },
        {
            "type": "section",
            "fields": [
                {
                    "type": "mrkdwn",
                    "text": f"*Publishers analysed:*\n{total_analysed}",
                },
                {
                    "type": "mrkdwn",
                    "text": f"*Eligible (model fit):*\n{total_eligible}",
                },
                {
                    "type": "mrkdwn",
                    "text": f"*Lookback window:*\n{LOOKBACK_DAYS} days",
                },
                {
                    "type": "mrkdwn",
                    "text": f"*Total weekly uplift (top {len(ranked)}):*\n{fmt_usd(total_weekly_uplift)}",
                },
            ],
        },
        {"type": "divider"},
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": (
                    ":robot_face: *Claude's priority ranking* "
                    "— sorted by confidence × revenue impact"
                ),
            },
        },
    ]

    for item in ranked:
        opt = opt_by_name.get(item["publisher"], {})
        if not opt:
            continue

        p_emoji  = _priority_emoji(item.get("priority", "medium"))
        d_arrow  = _direction_arrow(opt["direction"])
        conf_bar = _confidence_bar(opt["confidence"])
        weekly   = opt["daily_rev_uplift"] * 7

        blocks.append({"type": "divider"})
        blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": (
                    f"{p_emoji}  *#{item['rank']} — {opt['publisher']}*\n"
                    f"  {d_arrow} floor: `{fmt_usd(opt['current_floor'])}` → `{fmt_usd(opt['optimal_floor'])}`  "
                    f"({opt['direction']})\n"
                    f"  daily rev: {fmt_usd(opt['current_daily_rev'])} → *{fmt_usd(opt['projected_daily_rev'])}*  "
                    f"(+{fmt_usd(opt['daily_rev_uplift'])}/day  ·  *{fmt_usd(weekly)}/week*)\n"
                    f"  confidence: `{conf_bar}` {opt['confidence']:.2f}  "
                    f"R²: {opt['r_squared']:.3f}  "
                    f"days: {opt['days_of_data']}  "
                    f"avg bids/day: {fmt_n(opt['avg_daily_bids'])}"
                ),
            },
        })

        rationale = item.get("rationale", "")
        caution   = item.get("caution",   "")
        context_parts = []
        if rationale:
            context_parts.append(f":speech_balloon: {rationale}")
        if caution:
            context_parts.append(f":warning: *Caution:* {caution}")

        if context_parts:
            blocks.append({
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": "  " + "\n  ".join(context_parts),
                },
            })

    blocks.append({"type": "divider"})
    blocks.append({
        "type": "context",
        "elements": [
            {
                "type": "mrkdwn",
                "text": (
                    f"Model: linear win-rate elasticity (win_rate = α + β·floor/avg_bid)  |  "
                    f"Confidence = 0.6·R² + 0.4·(1−CoV)  |  "
                    f"PGAM Intelligence · Floor Elasticity Agent · {now_label}"
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
    Post the weekly floor elasticity Slack summary.

    Designed to be scheduled weekly on Mondays. Guards:
      1. Only runs on Monday (weekday == 0). Override by setting force=True.
      2. Once-daily dedup via core/slack.py prevents duplicate Monday posts.

    Can also be run directly: `python -m agents.reports.floor_elasticity`
    """
    now_et     = datetime.now(ET)
    date_label = now_et.strftime("%B %-d, %Y")
    now_label  = now_et.strftime("%H:%M ET")

    # ── 1. Day-of-week gate (Monday = 0) ─────────────────────────────────────
    if now_et.weekday() != 0:
        print(
            f"[floor_elasticity] Skipping — today is {now_et.strftime('%A')}, "
            f"this report runs on Mondays."
        )
        return

    # ── 2. Dedup ─────────────────────────────────────────────────────────────
    if already_sent_today(ALERT_KEY):
        print("[floor_elasticity] Weekly report already sent today — skipping.")
        return

    # ── 3. Run the full optimisation ─────────────────────────────────────────
    print(
        f"[floor_elasticity] Running {LOOKBACK_DAYS}-day elasticity analysis "
        f"({n_days_ago(LOOKBACK_DAYS)} → {today()})…"
    )
    start_date = n_days_ago(LOOKBACK_DAYS)
    end_date   = today()

    try:
        rows = fetch(BREAKDOWN, METRICS, start_date, end_date)
    except Exception as exc:
        print(f"[floor_elasticity] API fetch failed: {exc}")
        return

    if not rows:
        print("[floor_elasticity] No data returned — aborting.")
        return

    pub_groups     = _group_by_publisher(rows)
    total_analysed = len(pub_groups)
    print(f"[floor_elasticity] {total_analysed} publishers in dataset.")

    all_results: list[dict] = []
    skipped = {"days": 0, "bids": 0, "floor_variance": 0, "slope": 0, "uplift": 0}

    for name, raw_rows in pub_groups.items():
        daily = [r for r in (_parse_daily(rr) for rr in raw_rows) if r is not None]

        if len(daily) < MIN_DATA_DAYS:
            skipped["days"] += 1
            continue

        avg_bids = sum(d["bids"] for d in daily) / len(daily)
        if avg_bids < MIN_DAILY_BIDS:
            skipped["bids"] += 1
            continue

        opt = _optimise_publisher(name, daily)
        if opt is None:
            # Reasons tracked above in _fit_model/_optimise_publisher; approximate here
            skipped["slope"] += 1
            continue

        all_results.append(opt)

    total_eligible = len(all_results)
    print(
        f"[floor_elasticity] {total_eligible} eligible publishers.  "
        f"Skipped — days:{skipped['days']} bids:{skipped['bids']} "
        f"slope:{skipped['slope']} uplift:{skipped['uplift']}"
    )

    if not all_results:
        print("[floor_elasticity] No optimisation opportunities found.")
        mark_sent(ALERT_KEY)
        return

    # Sort by abs(uplift) × confidence and take top N for Claude
    all_results.sort(
        key=lambda r: abs(r["daily_rev_uplift"]) * r["confidence"],
        reverse=True,
    )
    top_candidates = all_results[:TOP_FOR_CLAUDE]

    # ── 4. Get Claude's ranked prioritisation ─────────────────────────────────
    print(f"[floor_elasticity] Sending top {len(top_candidates)} to Claude…")
    try:
        ranked = analyze_floor_elasticity(top_candidates)
    except Exception as exc:
        print(f"[floor_elasticity] Claude ranking failed: {exc}")
        ranked = [
            {
                "publisher": o["publisher"],
                "rank":      i + 1,
                "priority":  "medium",
                "rationale": (
                    f"Model projects +{fmt_usd(o['daily_rev_uplift'])}/day "
                    f"({o['direction']} floor {fmt_usd(o['current_floor'])} → {fmt_usd(o['optimal_floor'])}, "
                    f"confidence {o['confidence']:.2f}).\n"
                    + floor_change(o["publisher"], o["current_floor"], o["optimal_floor"])
                ),
                "caution": "",
            }
            for i, o in enumerate(top_candidates)
        ]

    # ── 5. Build and post Slack message ───────────────────────────────────────
    opt_by_name = {r["publisher"]: r for r in all_results}

    blocks = _build_weekly_blocks(
        ranked=ranked,
        opt_by_name=opt_by_name,
        date_label=date_label,
        now_label=now_label,
        total_analysed=total_analysed,
        total_eligible=total_eligible,
    )

    total_weekly = sum(
        opt_by_name.get(r["publisher"], {}).get("daily_rev_uplift", 0) * 7
        for r in ranked
    )
    fallback = (
        f"Floor Elasticity Report — {date_label}: "
        f"{total_eligible} publishers optimised. "
        f"Top {len(ranked)} weekly uplift: {fmt_usd(total_weekly)}."
    )

    send_blocks(blocks=blocks, text=fallback)
    mark_sent(ALERT_KEY)
    print(
        f"[floor_elasticity] Weekly report sent — "
        f"{len(ranked)} opportunities, {fmt_usd(total_weekly)} total weekly uplift."
    )


if __name__ == "__main__":
    run()
