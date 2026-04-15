"""
agents/alerts/tb_revenue.py

Hourly Teqblaze (TB) total platform revenue snapshot posted to Slack.

Every run posts a fresh snapshot — no daily dedup. A 55-minute cooldown
prevents accidental double-posts from scheduler jitter while still letting
an operator re-run manually after a gap.

Metrics computed
----------------
  pacing_pct     Today's revenue vs linear interpolation of $33,333 daily target
                 ($1M/month ÷ 30 days)
  margin_pct     (gross_revenue - pub_payout) / gross_revenue × 100
  eCPM           gross_revenue / impressions × 1,000
  win_rate_pct   wins / bids × 100
  dod_change_pct Today's running total vs yesterday's same-hour linear estimate
  mtd_pct        Month-to-date revenue vs pro-rated $1M monthly target

State file
----------
  /tmp/pgam_tb_revenue_state.json   { "last_sent": <unix_timestamp> }
"""

import json
import os
import time
from datetime import datetime, date

import pytz

from core.api import fetch_tb as fetch, tb_configured, today, yesterday, n_days_ago, sf, fmt_usd, fmt_n, pct

ET             = pytz.timezone("US/Eastern")
BREAKDOWN      = "DATE"
PUB_BREAKDOWN  = "PUBLISHER"
METRICS        = ["GROSS_REVENUE", "PUB_PAYOUT", "IMPRESSIONS", "WINS", "BIDS"]
LL_DAILY_TARGET    = 10_000.0              # LL platform daily target
COMBINED_MONTHLY   = 1_000_000.0          # Combined TB + LL monthly target
COMBINED_DAILY     = COMBINED_MONTHLY / 30.0   # ≈ $33,333/day combined
# TB daily target = combined minus LL share
DAILY_TARGET       = COMBINED_DAILY - LL_DAILY_TARGET   # ≈ $23,333/day for TB alone
COOLDOWN_SEC   = 55 * 60                 # 55 minutes between posts
STATE_FILE     = "/tmp/pgam_tb_revenue_state.json"
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
    revenue     = sum(_extract(r, "GROSS_REVENUE",  "gross_revenue",  "grossRevenue")  for r in rows)
    payout      = sum(_extract(r, "PUB_PAYOUT",     "pub_payout",     "pubPayout")     for r in rows)
    impressions = sum(_extract(r, "IMPRESSIONS",    "impressions")                     for r in rows)
    wins        = sum(_extract(r, "WINS",           "wins")                            for r in rows)
    bids        = sum(_extract(r, "BIDS",           "bids")                            for r in rows)
    return {
        "revenue": revenue, "payout": payout,
        "impressions": impressions, "wins": wins, "bids": bids,
    }


def _parse_pub_rows(rows: list) -> list[dict]:
    pubs = []
    for row in rows:
        name = (
            row.get("PUBLISHER_NAME") or row.get("PUBLISHER") or row.get("publisher")
            or row.get("pubName") or row.get("pub_name") or "Unknown"
        )
        revenue     = _extract(row, "GROSS_REVENUE", "gross_revenue", "grossRevenue")
        impressions = _extract(row, "IMPRESSIONS",   "impressions")
        wins        = _extract(row, "WINS",          "wins")
        bids        = _extract(row, "BIDS",          "bids")
        ecpm        = (revenue / impressions * 1000) if impressions > 0 else 0.0
        win_rate    = pct(wins, bids)
        pubs.append({"name": str(name), "revenue": revenue,
                     "impressions": impressions, "ecpm": ecpm, "win_rate": win_rate})
    pubs.sort(key=lambda x: x["revenue"], reverse=True)
    return pubs


# ---------------------------------------------------------------------------
# Metric computations
# ---------------------------------------------------------------------------

def _compute(totals: dict, hour_et: int) -> dict:
    revenue     = totals["revenue"]
    payout      = totals["payout"]
    impressions = totals["impressions"]
    wins        = totals["wins"]
    bids        = totals["bids"]

    margin   = pct(revenue - payout, revenue)
    ecpm     = (revenue / impressions * 1000) if impressions > 0 else 0.0
    win_rate = pct(wins, bids)

    expected_at_hour = DAILY_TARGET * (max(hour_et, 1) / 24.0)
    pacing_pct       = pct(revenue, expected_at_hour)
    on_track         = pacing_pct >= 90.0

    return {
        "revenue": revenue, "payout": payout, "impressions": impressions,
        "wins": wins, "bids": bids, "margin": margin, "ecpm": ecpm,
        "win_rate": win_rate, "pacing_pct": pacing_pct,
        "expected_revenue": expected_at_hour, "on_track": on_track,
    }


def _dod_change(today_rev: float, yest_rev: float, hour_et: int) -> float:
    yest_at_hour = yest_rev * (max(hour_et, 1) / 24.0)
    if yest_at_hour <= 0:
        return 0.0
    return ((today_rev - yest_at_hour) / yest_at_hour) * 100.0


def _mtd_stats(now_et: datetime) -> dict:
    """
    Fetch MTD revenue and compute progress.
    Shows TB revenue vs TB target, plus combined context vs $1M goal.
    """
    d = now_et.date()
    month_start  = d.replace(day=1).strftime("%Y-%m-%d")
    today_str    = d.strftime("%Y-%m-%d")
    days_elapsed = d.day

    try:
        rows    = fetch(BREAKDOWN, METRICS, month_start, today_str)
        totals  = _sum_metrics(rows)
        mtd_rev = totals["revenue"]
    except Exception:
        return {
            "mtd_revenue": 0.0, "mtd_tb_target": 0.0, "mtd_tb_pct": 0.0,
            "combined_estimate": 0.0, "combined_target": 0.0,
            "combined_pct": 0.0, "days_elapsed": days_elapsed,
        }

    mtd_tb_target      = DAILY_TARGET * days_elapsed
    mtd_combined_target = COMBINED_DAILY * days_elapsed
    # Estimate combined: TB MTD + LL pro-rated (LL contribution assumed at target)
    ll_mtd_estimate    = LL_DAILY_TARGET * days_elapsed
    combined_estimate  = mtd_rev + ll_mtd_estimate

    return {
        "mtd_revenue":        mtd_rev,
        "mtd_tb_target":      mtd_tb_target,
        "mtd_tb_pct":         pct(mtd_rev, mtd_tb_target),
        "combined_estimate":  combined_estimate,
        "combined_target":    mtd_combined_target,
        "combined_pct":       pct(combined_estimate, mtd_combined_target),
        "days_elapsed":       days_elapsed,
    }


# ---------------------------------------------------------------------------
# Slack Block Kit builder
# ---------------------------------------------------------------------------

def _pacing_bar(pacing_pct: float, width: int = 12) -> str:
    filled = min(int(pacing_pct / 100 * width), width)
    return "█" * filled + "░" * (width - filled)


def _pacing_emoji(pacing_pct: float) -> str:
    if pacing_pct >= 100:  return ":large_green_circle:"
    if pacing_pct >= 80:   return ":large_yellow_circle:"
    return ":red_circle:"


def _change_str(pct_change: float) -> str:
    arrow = "▲" if pct_change >= 0 else "▼"
    sign  = "+" if pct_change >= 0 else ""
    return f"{arrow} {sign}{pct_change:.1f}% DoD"


def _build_blocks(
    computed:    dict,
    yest_totals: dict,
    top_pubs:    list[dict],
    mtd:         dict,
    hour_et:     int,
    now_label:   str,
    today_str:   str,
    yest_str:    str,
) -> list:
    dod        = _dod_change(computed["revenue"], yest_totals["revenue"], hour_et)
    bar        = _pacing_bar(computed["pacing_pct"])
    pace_emoji = _pacing_emoji(computed["pacing_pct"])
    pct_of_target = pct(computed["revenue"], DAILY_TARGET)
    remaining     = max(DAILY_TARGET - computed["revenue"], 0.0)

    # ── Status line ──────────────────────────────────────────────────────────
    status_line = (
        f"{pace_emoji} *Teqblaze Revenue — {now_label}:* "
        f"*{fmt_usd(computed['revenue'])}* collected today "
        f"({pct_of_target:.1f}% of {fmt_usd(DAILY_TARGET)} daily target) — "
        f"pacing at *{computed['pacing_pct']:.1f}%* of expected.  "
        f"{_change_str(dod)}."
    )

    blocks = [
        {
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": f":chart_with_upwards_trend:  Teqblaze Revenue — {now_label}",
                "emoji": True,
            },
        },
        {
            "type": "section",
            "text": {"type": "mrkdwn", "text": status_line},
        },
        # ── Pacing bar ───────────────────────────────────────────────────────
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": (
                    f"`{bar}`  {fmt_usd(computed['revenue'])} / {fmt_usd(DAILY_TARGET)}  "
                    f"({fmt_usd(remaining)} to go)"
                ),
            },
        },
        {"type": "divider"},
        # ── Core metrics grid ────────────────────────────────────────────────
        {
            "type": "section",
            "fields": [
                {"type": "mrkdwn", "text": f"*Gross Revenue:*\n{fmt_usd(computed['revenue'])}"},
                {"type": "mrkdwn", "text": f"*Pub Payout:*\n{fmt_usd(computed['payout'])}"},
                {"type": "mrkdwn", "text": f"*Margin:*\n{computed['margin']:.1f}%"},
                {"type": "mrkdwn", "text": f"*eCPM:*\n{fmt_usd(computed['ecpm'])}"},
                {"type": "mrkdwn", "text": f"*Impressions:*\n{fmt_n(computed['impressions'])}"},
                {"type": "mrkdwn", "text": f"*Win Rate:*\n{computed['win_rate']:.2f}%"},
                {"type": "mrkdwn", "text": f"*Bids:*\n{fmt_n(computed['bids'])}"},
                {"type": "mrkdwn", "text": f"*vs Yesterday (same hr):*\n{_change_str(dod)}"},
            ],
        },
        {"type": "divider"},
        # ── MTD progress ─────────────────────────────────────────────────────
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": (
                    f":calendar: *Month-to-date ({mtd['days_elapsed']} days):*\n"
                    f"  TB:  {fmt_usd(mtd['mtd_revenue'])} / {fmt_usd(mtd['mtd_tb_target'])} pro-rated  "
                    f"({mtd['mtd_tb_pct']:.1f}%)\n"
                    f"  Combined (TB + LL est.):  {fmt_usd(mtd['combined_estimate'])} / "
                    f"{fmt_usd(mtd['combined_target'])} toward {fmt_usd(COMBINED_MONTHLY)} goal  "
                    f"({mtd['combined_pct']:.1f}%)"
                ),
            },
        },
    ]

    # ── Top publishers ────────────────────────────────────────────────────────
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

    # ── Yesterday reference ───────────────────────────────────────────────────
    yest_ecpm   = (yest_totals["revenue"] / yest_totals["impressions"] * 1000
                   if yest_totals["impressions"] > 0 else 0.0)
    yest_margin = pct(yest_totals["revenue"] - yest_totals["payout"], yest_totals["revenue"])

    blocks.append({"type": "divider"})
    blocks.append({
        "type": "context",
        "elements": [{
            "type": "mrkdwn",
            "text": (
                f":calendar: *Yesterday ({yest_str}) full day:*  "
                f"{fmt_usd(yest_totals['revenue'])}  |  "
                f"margin {yest_margin:.1f}%  |  "
                f"eCPM {fmt_usd(yest_ecpm)}  |  "
                f"imps {fmt_n(yest_totals['impressions'])}"
            ),
        }],
    })
    blocks.append({
        "type": "context",
        "elements": [{
            "type": "mrkdwn",
            "text": f"PGAM Intelligence · Teqblaze Revenue Agent · {now_label}",
        }],
    })

    return blocks


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def run():
    """
    Execute the Teqblaze hourly revenue update.
    Called by scheduler.py every 60 minutes.
    """
    from core.slack import send_blocks

    now_et    = datetime.now(ET)
    hour_et   = now_et.hour
    now_label = now_et.strftime("%a %b %-d, %H:%M ET")
    today_str = today()
    yest_str  = yesterday()

    # ── 0. TB credentials check ──────────────────────────────────────────────
    if not tb_configured():
        print("[tb_revenue] TB API not configured yet — set TB_API_BASE_URL, "
              "TB_CLIENT_KEY, TB_SECRET_KEY in .env to enable.")
        return

    # ── 1. Cooldown check ────────────────────────────────────────────────────
    if not _post_due():
        elapsed = (time.time() - _load_state().get("last_sent", 0.0)) / 60
        print(f"[tb_revenue] Cooldown active ({elapsed:.0f}m elapsed, need {COOLDOWN_SEC//60}m) — skipping.")
        return

    # ── 2. Fetch today + yesterday DATE-breakdown ────────────────────────────
    # TB API allows only one concurrent query per user — add a small pause between
    # sequential calls so the server's "in-progress" lock has time to clear.
    _TB_INTER_CALL_SLEEP = 5   # seconds between TB fetches

    print(f"[tb_revenue] Fetching today ({today_str}) and yesterday ({yest_str})…")
    try:
        today_rows = fetch(BREAKDOWN, METRICS, today_str, today_str)
        time.sleep(_TB_INTER_CALL_SLEEP)
        yest_rows  = fetch(BREAKDOWN, METRICS, yest_str,  yest_str)
    except Exception as exc:
        print(f"[tb_revenue] DATE fetch failed: {exc}")
        return

    today_totals = _sum_metrics(today_rows)
    yest_totals  = _sum_metrics(yest_rows)
    computed     = _compute(today_totals, hour_et=hour_et)

    print(
        f"[tb_revenue] Today: {fmt_usd(today_totals['revenue'])}  |  "
        f"Yesterday: {fmt_usd(yest_totals['revenue'])}  |  "
        f"Pacing: {computed['pacing_pct']:.1f}%  |  "
        f"Hour ET: {hour_et}"
    )

    # ── 3. Fetch top publishers (today) ──────────────────────────────────────
    time.sleep(_TB_INTER_CALL_SLEEP)
    try:
        pub_rows = fetch(PUB_BREAKDOWN, METRICS, today_str, today_str)
        top_pubs = _parse_pub_rows(pub_rows)[:TOP_PUB_COUNT]
    except Exception as exc:
        print(f"[tb_revenue] Publisher fetch failed (non-fatal): {exc}")
        top_pubs = []

    # ── 4. MTD stats ─────────────────────────────────────────────────────────
    time.sleep(_TB_INTER_CALL_SLEEP)
    mtd = _mtd_stats(now_et)

    # ── 5. Build and post Slack message ──────────────────────────────────────
    blocks = _build_blocks(
        computed=computed,
        yest_totals=yest_totals,
        top_pubs=top_pubs,
        mtd=mtd,
        hour_et=hour_et,
        now_label=now_label,
        today_str=today_str,
        yest_str=yest_str,
    )
    fallback = (
        f"TB Revenue {now_label}: "
        f"{fmt_usd(computed['revenue'])} — "
        f"pacing {computed['pacing_pct']:.1f}% toward {fmt_usd(DAILY_TARGET)}/day TB target.  "
        f"MTD combined est: {fmt_usd(mtd['combined_estimate'])} ({mtd['combined_pct']:.1f}% of $1M goal)."
    )

    send_blocks(blocks=blocks, text=fallback)
    _mark_sent()
    print(f"[tb_revenue] Posted. MTD: {fmt_usd(mtd['mtd_revenue'])} ({mtd['mtd_tb_pct']:.1f}% of pro-rated TB target).")


if __name__ == "__main__":
    run()
