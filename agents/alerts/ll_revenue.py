"""
agents/alerts/ll_revenue.py

Hourly Limelight (LL) revenue trend update posted to Slack.

Every run posts a fresh snapshot — no daily dedup. A 55-minute cooldown
prevents accidental double-posts from scheduler jitter while still letting
an operator re-run manually after a gap.

Metrics computed
----------------
  pacing_pct     Today's revenue vs linear interpolation of $3,500 daily target
  margin_pct     (gross_revenue - pub_payout) / gross_revenue × 100
  eCPM           gross_revenue / impressions × 1,000
  win_rate_pct   wins / bids × 100
  dod_change_pct Today's running total vs yesterday's same-hour linear estimate

LL API reset (after 8 PM ET)
-----------------------------
The Limelight platform resets its real-time counters after 8 PM ET each day,
making today's numbers unreliable until the next morning. When the current ET
hour is >= RESET_HOUR (20), the agent flips to "yesterday complete" mode:
  - Yesterday's full-day numbers become the primary display
  - Today's (post-reset) data is noted but not used for pacing or comparisons
  - Top publishers are sourced from yesterday instead

State file
----------
  /tmp/pgam_ll_revenue_state.json   { "last_sent": <unix_timestamp> }
"""

import json
import os
import time
from datetime import datetime

import pytz

from core.api import fetch, today, yesterday, sf, fmt_usd, fmt_n, pct

ET          = pytz.timezone("US/Eastern")
BREAKDOWN   = "DATE"
PUB_BREAKDOWN = "PUBLISHER"
METRICS     = ["GROSS_REVENUE", "PUB_PAYOUT", "IMPRESSIONS", "WINS", "BIDS"]
DAILY_TARGET   = 10_000.0    # LL platform daily revenue target ($)
RESET_HOUR     = 20          # 8 PM ET — API resets after this hour
COOLDOWN_SEC   = 55 * 60     # 55 minutes between posts
STATE_FILE     = "/tmp/pgam_ll_revenue_state.json"
TOP_PUB_COUNT  = 5


# ---------------------------------------------------------------------------
# State / cooldown
# ---------------------------------------------------------------------------

def _load_state() -> dict:
    if not os.path.exists(STATE_FILE):
        return {}
    try:
        with open(STATE_FILE) as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError):
        return {}


def _post_due() -> bool:
    last = _load_state().get("last_sent", 0.0)
    return (time.time() - last) >= COOLDOWN_SEC


def _mark_sent():
    state = _load_state()
    state["last_sent"] = time.time()
    with open(STATE_FILE, "w") as f:
        json.dump(state, f)


# ---------------------------------------------------------------------------
# Row parsing
# ---------------------------------------------------------------------------

def _extract(row: dict, *keys) -> float:
    for k in keys:
        if k in row:
            return sf(row[k])
    return 0.0


def _sum_metrics(rows: list) -> dict:
    """Collapse all rows into a single totals dict."""
    revenue    = sum(_extract(r, "GROSS_REVENUE",  "gross_revenue",  "grossRevenue")  for r in rows)
    payout     = sum(_extract(r, "PUB_PAYOUT",     "pub_payout",     "pubPayout")     for r in rows)
    impressions= sum(_extract(r, "IMPRESSIONS",    "impressions")                     for r in rows)
    wins       = sum(_extract(r, "WINS",           "wins")                            for r in rows)
    bids       = sum(_extract(r, "BIDS",           "bids")                            for r in rows)
    return {
        "revenue":     revenue,
        "payout":      payout,
        "impressions": impressions,
        "wins":        wins,
        "bids":        bids,
    }


def _parse_pub_rows(rows: list) -> list[dict]:
    """Parse PUBLISHER breakdown rows, return list of dicts sorted by revenue desc."""
    pubs = []
    for row in rows:
        name = (
            row.get("PUBLISHER_NAME") or row.get("PUBLISHER") or row.get("publisher")
            or row.get("pubName") or row.get("pub_name") or "Unknown"
        )
        revenue     = _extract(row, "GROSS_REVENUE",  "gross_revenue",  "grossRevenue")
        impressions = _extract(row, "IMPRESSIONS",    "impressions")
        wins        = _extract(row, "WINS",           "wins")
        bids        = _extract(row, "BIDS",           "bids")
        ecpm        = (revenue / impressions * 1000) if impressions > 0 else 0.0
        win_rate    = pct(wins, bids)
        pubs.append({
            "name":        str(name),
            "revenue":     revenue,
            "impressions": impressions,
            "ecpm":        ecpm,
            "win_rate":    win_rate,
        })
    pubs.sort(key=lambda x: x["revenue"], reverse=True)
    return pubs


# ---------------------------------------------------------------------------
# Metric computations
# ---------------------------------------------------------------------------

def _compute(totals: dict, hour_et: int, target: float = DAILY_TARGET) -> dict:
    """Derive all display metrics from raw totals."""
    revenue     = totals["revenue"]
    payout      = totals["payout"]
    impressions = totals["impressions"]
    wins        = totals["wins"]
    bids        = totals["bids"]

    margin      = pct(revenue - payout, revenue)
    ecpm        = (revenue / impressions * 1000) if impressions > 0 else 0.0
    win_rate    = pct(wins, bids)

    # Linear pacing: expected at current hour = target × (hour / 24)
    expected_at_hour = target * (max(hour_et, 1) / 24.0)
    pacing_pct       = pct(revenue, expected_at_hour)
    on_track         = pacing_pct >= 90.0          # within 10% is "on track"

    return {
        "revenue":          revenue,
        "payout":           payout,
        "impressions":      impressions,
        "wins":             wins,
        "bids":             bids,
        "margin":           margin,
        "ecpm":             ecpm,
        "win_rate":         win_rate,
        "pacing_pct":       pacing_pct,
        "expected_revenue": expected_at_hour,
        "on_track":         on_track,
    }


def _dod_change(today_revenue: float, yest_revenue: float, hour_et: int) -> float:
    """
    Day-over-day change: compare today's running total against what yesterday
    looked like at the same hour (linear interpolation of the full-day total).
    Returns percentage change (positive = ahead, negative = behind).
    """
    yest_at_hour = yest_revenue * (max(hour_et, 1) / 24.0)
    if yest_at_hour <= 0:
        return 0.0
    return ((today_revenue - yest_at_hour) / yest_at_hour) * 100.0


# ---------------------------------------------------------------------------
# Slack Block Kit builders
# ---------------------------------------------------------------------------

def _pacing_bar(pacing_pct: float, width: int = 12) -> str:
    """Simple ASCII progress bar capped at 100% visual width."""
    filled = min(int(pacing_pct / 100 * width), width)
    return "█" * filled + "░" * (width - filled)


def _pacing_emoji(pacing_pct: float) -> str:
    if pacing_pct >= 100:
        return ":large_green_circle:"
    if pacing_pct >= 80:
        return ":large_yellow_circle:"
    return ":red_circle:"


def _change_str(pct_change: float) -> str:
    arrow = "▲" if pct_change >= 0 else "▼"
    sign  = "+" if pct_change >= 0 else ""
    return f"{arrow} {sign}{pct_change:.1f}% DoD"


def _build_live_blocks(
    computed: dict,
    yest_totals: dict,
    top_pubs: list[dict],
    hour_et: int,
    now_label: str,
    today_str: str,
    yest_str: str,
) -> list:
    dod   = _dod_change(computed["revenue"], yest_totals["revenue"], hour_et)
    bar   = _pacing_bar(computed["pacing_pct"])
    pace_emoji = _pacing_emoji(computed["pacing_pct"])

    # Progress toward daily target
    pct_of_target = pct(computed["revenue"], DAILY_TARGET)
    remaining     = max(DAILY_TARGET - computed["revenue"], 0.0)

    blocks = [
        {
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": f":chart_with_upwards_trend:  Limelight Revenue — {now_label}",
                "emoji": True,
            },
        },
        # ── Pacing bar ──────────────────────────────────────────────────────
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": (
                    f"{pace_emoji}  *Daily target progress:*  "
                    f"*{fmt_usd(computed['revenue'])}* of *{fmt_usd(DAILY_TARGET)}*  "
                    f"({pct_of_target:.1f}%)\n"
                    f"`{bar}`  "
                    f"pacing at *{computed['pacing_pct']:.1f}%* of expected  |  "
                    f"{fmt_usd(remaining)} to go"
                ),
            },
        },
        {"type": "divider"},
        # ── Core metrics grid ────────────────────────────────────────────────
        {
            "type": "section",
            "fields": [
                {
                    "type": "mrkdwn",
                    "text": f"*Gross Revenue:*\n{fmt_usd(computed['revenue'])}",
                },
                {
                    "type": "mrkdwn",
                    "text": f"*Pub Payout:*\n{fmt_usd(computed['payout'])}",
                },
                {
                    "type": "mrkdwn",
                    "text": f"*Margin:*\n{computed['margin']:.1f}%",
                },
                {
                    "type": "mrkdwn",
                    "text": f"*eCPM:*\n{fmt_usd(computed['ecpm'])}",
                },
                {
                    "type": "mrkdwn",
                    "text": f"*Impressions:*\n{fmt_n(computed['impressions'])}",
                },
                {
                    "type": "mrkdwn",
                    "text": f"*Win Rate:*\n{computed['win_rate']:.2f}%",
                },
                {
                    "type": "mrkdwn",
                    "text": f"*Bids:*\n{fmt_n(computed['bids'])}",
                },
                {
                    "type": "mrkdwn",
                    "text": f"*vs Yesterday (same hour):*\n{_change_str(dod)}",
                },
            ],
        },
    ]

    # ── Top publishers ───────────────────────────────────────────────────────
    if top_pubs:
        blocks.append({"type": "divider"})
        pub_lines = [f"*:trophy: Top {len(top_pubs)} Publishers Today*"]
        for i, pub in enumerate(top_pubs, 1):
            pub_lines.append(
                f"{i}. *{pub['name']}*  —  "
                f"{fmt_usd(pub['revenue'])}  |  "
                f"eCPM {fmt_usd(pub['ecpm'])}  |  "
                f"win rate {pub['win_rate']:.1f}%"
            )
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": "\n".join(pub_lines)},
        })

    # ── Yesterday reference ──────────────────────────────────────────────────
    yest_ecpm = (
        yest_totals["revenue"] / yest_totals["impressions"] * 1000
        if yest_totals["impressions"] > 0 else 0.0
    )
    yest_margin = pct(
        yest_totals["revenue"] - yest_totals["payout"],
        yest_totals["revenue"],
    )
    blocks.append({"type": "divider"})
    blocks.append({
        "type": "context",
        "elements": [
            {
                "type": "mrkdwn",
                "text": (
                    f":calendar: *Yesterday ({yest_str}) full day:*  "
                    f"{fmt_usd(yest_totals['revenue'])}  |  "
                    f"margin {yest_margin:.1f}%  |  "
                    f"eCPM {fmt_usd(yest_ecpm)}  |  "
                    f"imps {fmt_n(yest_totals['impressions'])}"
                ),
            }
        ],
    })
    blocks.append({
        "type": "context",
        "elements": [
            {
                "type": "mrkdwn",
                "text": f"PGAM Intelligence · Limelight Revenue Agent · {now_label}",
            }
        ],
    })

    return blocks


def _build_reset_blocks(
    yest_computed: dict,
    top_pubs: list[dict],
    now_label: str,
    yest_str: str,
) -> list:
    """
    Post-reset view (after 8 PM ET). Shows yesterday's completed numbers as the
    primary display with a clear note that today's API counters have reset.
    """
    blocks = [
        {
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": f":chart_with_upwards_trend:  Limelight Revenue (Final) — {now_label}",
                "emoji": True,
            },
        },
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": (
                    ":information_source: *LL API reset after 8 PM ET* — "
                    "today's counters have cleared. Showing *yesterday's completed numbers*."
                ),
            },
        },
        {"type": "divider"},
        {
            "type": "section",
            "fields": [
                {
                    "type": "mrkdwn",
                    "text": f"*Gross Revenue ({yest_str}):*\n{fmt_usd(yest_computed['revenue'])}",
                },
                {
                    "type": "mrkdwn",
                    "text": f"*Daily Target:*\n{fmt_usd(DAILY_TARGET)}",
                },
                {
                    "type": "mrkdwn",
                    "text": f"*Target Achievement:*\n{yest_computed['pacing_pct']:.1f}%",
                },
                {
                    "type": "mrkdwn",
                    "text": f"*Pub Payout:*\n{fmt_usd(yest_computed['payout'])}",
                },
                {
                    "type": "mrkdwn",
                    "text": f"*Margin:*\n{yest_computed['margin']:.1f}%",
                },
                {
                    "type": "mrkdwn",
                    "text": f"*eCPM:*\n{fmt_usd(yest_computed['ecpm'])}",
                },
                {
                    "type": "mrkdwn",
                    "text": f"*Impressions:*\n{fmt_n(yest_computed['impressions'])}",
                },
                {
                    "type": "mrkdwn",
                    "text": f"*Win Rate:*\n{yest_computed['win_rate']:.2f}%",
                },
            ],
        },
    ]

    if top_pubs:
        blocks.append({"type": "divider"})
        pub_lines = [f"*:trophy: Top {len(top_pubs)} Publishers — Yesterday ({yest_str})*"]
        for i, pub in enumerate(top_pubs, 1):
            pub_lines.append(
                f"{i}. *{pub['name']}*  —  "
                f"{fmt_usd(pub['revenue'])}  |  "
                f"eCPM {fmt_usd(pub['ecpm'])}  |  "
                f"win rate {pub['win_rate']:.1f}%"
            )
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": "\n".join(pub_lines)},
        })

    blocks.append({
        "type": "context",
        "elements": [
            {
                "type": "mrkdwn",
                "text": f"PGAM Intelligence · Limelight Revenue Agent · {now_label}",
            }
        ],
    })

    return blocks


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def run():
    """
    Execute the Limelight hourly revenue update. Designed to be called by a
    scheduler or run directly: `python -m agents.alerts.ll_revenue`.
    """
    # Lazy import here so the module can be loaded without Slack config
    from core.slack import send_blocks

    now_et    = datetime.now(ET)
    hour_et   = now_et.hour
    now_label = now_et.strftime("%a %b %-d, %H:%M ET")
    today_str = today()
    yest_str  = yesterday()

    # ── 1. Cooldown check ────────────────────────────────────────────────────
    if not _post_due():
        elapsed = (time.time() - _load_state().get("last_sent", 0.0)) / 60
        print(f"[ll_revenue] Cooldown active ({elapsed:.0f}m elapsed, need {COOLDOWN_SEC//60}m) — skipping.")
        return

    # ── 2. Fetch today's and yesterday's DATE-breakdown data ─────────────────
    print(f"[ll_revenue] Fetching today ({today_str}) and yesterday ({yest_str}) data…")
    try:
        today_rows = fetch(BREAKDOWN, METRICS, today_str, today_str)
        yest_rows  = fetch(BREAKDOWN, METRICS, yest_str,  yest_str)
    except Exception as exc:
        print(f"[ll_revenue] DATE fetch failed: {exc}")
        return

    today_totals = _sum_metrics(today_rows)
    yest_totals  = _sum_metrics(yest_rows)

    print(
        f"[ll_revenue] Today: {fmt_usd(today_totals['revenue'])}  |  "
        f"Yesterday: {fmt_usd(yest_totals['revenue'])}  |  "
        f"Hour ET: {hour_et}"
    )

    # ── 3. Determine display mode ────────────────────────────────────────────
    post_reset = hour_et >= RESET_HOUR

    if post_reset:
        # After 8 PM: show yesterday's completed numbers
        print(f"[ll_revenue] Post-reset mode (hour {hour_et} >= {RESET_HOUR}).")

        # Top publishers from yesterday
        try:
            pub_rows  = fetch(PUB_BREAKDOWN, METRICS, yest_str, yest_str)
            top_pubs  = _parse_pub_rows(pub_rows)[:TOP_PUB_COUNT]
        except Exception as exc:
            print(f"[ll_revenue] Publisher fetch failed (non-fatal): {exc}")
            top_pubs  = []

        # For pacing in reset mode, use full 24 hours (it's a completed day)
        yest_computed = _compute(yest_totals, hour_et=24)

        blocks   = _build_reset_blocks(
            yest_computed=yest_computed,
            top_pubs=top_pubs,
            now_label=now_label,
            yest_str=yest_str,
        )
        fallback = (
            f"LL Revenue (final, {yest_str}): "
            f"{fmt_usd(yest_computed['revenue'])} — "
            f"{yest_computed['pacing_pct']:.1f}% of ${DAILY_TARGET:,.0f} target."
        )

    else:
        # Normal live mode
        try:
            pub_rows = fetch(PUB_BREAKDOWN, METRICS, today_str, today_str)
            top_pubs = _parse_pub_rows(pub_rows)[:TOP_PUB_COUNT]
        except Exception as exc:
            print(f"[ll_revenue] Publisher fetch failed (non-fatal): {exc}")
            top_pubs = []

        computed = _compute(today_totals, hour_et=hour_et)

        blocks   = _build_live_blocks(
            computed=computed,
            yest_totals=yest_totals,
            top_pubs=top_pubs,
            hour_et=hour_et,
            now_label=now_label,
            today_str=today_str,
            yest_str=yest_str,
        )
        fallback = (
            f"LL Revenue {now_label}: "
            f"{fmt_usd(computed['revenue'])} collected — "
            f"pacing {computed['pacing_pct']:.1f}% of expected toward "
            f"${DAILY_TARGET:,.0f} target."
        )

    # ── 4. Post to Slack ─────────────────────────────────────────────────────
    send_blocks(blocks=blocks, text=fallback)
    _mark_sent()
    print(f"[ll_revenue] Update posted ({'post-reset' if post_reset else 'live'} mode).")


if __name__ == "__main__":
    run()
