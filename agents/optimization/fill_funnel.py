"""
agents/optimization/fill_funnel.py

Daily agent (08:00 ET, weekdays only) that finds publishers where the
opportunity → impression funnel is broken. Uses the extended reporting API
(pub × demand breakdown) to pinpoint exactly WHERE volume is leaking for
each publisher and what to do about it.

Funnel stages
-------------
  NO_DEMAND_COVERAGE  — bid_request_rate < 5%    : demand not connected / geo-blocked
  LOW_BID_RESPONSE    — bid_rate < 10%            : wrong format/geo/floor too high
  LOSING_AUCTIONS     — win_rate < 15%            : floor too high or competing SSP winning
  RENDER_FAILURE      — imp_rate < 80%            : won but not rendering (VAST/tag issue)
  HEALTHY             — all thresholds passed

Filtering
---------
  OPPORTUNITIES > 1,000,000 (publisher-level, yesterday)
  stage != "HEALTHY"

Revenue upside estimate
-----------------------
  If a broken publisher had the median opp_fill of healthy publishers, what
  revenue would it generate?
  revenue_lost_estimate = (median_opp_fill - pub_opp_fill) × opportunities × ecpm

Deduplication
-------------
Alert fires once per day via already_sent_today() / mark_sent().
STATE_FILE: /tmp/pgam_fill_funnel_state.json
"""

import json
import os
from datetime import datetime

import pytz

from core.api import yesterday, sf, fmt_usd, fmt_n
from core.ll_report import report_pub_demand
from core.slack import send_blocks

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
ALERT_KEY        = "fill_funnel_daily"
STATE_FILE       = "/tmp/pgam_fill_funnel_state.json"
MIN_OPPORTUNITIES = 1_000_000
MAX_PUBS_IN_SLACK = 8

# Funnel threshold constants
BID_REQUEST_RATE_THRESHOLD = 0.05   # < 5%  → NO_DEMAND_COVERAGE
BID_RATE_THRESHOLD         = 0.10   # < 10% → LOW_BID_RESPONSE
WIN_RATE_THRESHOLD         = 0.15   # < 15% → LOSING_AUCTIONS
IMP_RATE_THRESHOLD         = 0.80   # < 80% → RENDER_FAILURE

ET = pytz.timezone("US/Eastern")

# Stage display metadata: (badge_emoji, label, action_template)
STAGE_META = {
    "NO_DEMAND_COVERAGE": (
        ":red_circle:",
        "NO_DEMAND_COVERAGE",
        "-> *Execute:* Connect additional demand partners in LL UI "
        "-> Publishers -> {pub} -> Demand tab",
    ),
    "LOW_BID_RESPONSE": (
        ":large_yellow_circle:",
        "LOW_BID_RESPONSE",
        "-> *Execute:* Review geo/format targeting or lower floor "
        "-> Publishers -> {pub} -> Floor Prices",
    ),
    "LOSING_AUCTIONS": (
        ":large_orange_circle:",
        "LOSING_AUCTIONS",
        "-> *Execute:* Reduce floor price "
        "-> Publishers -> {pub} -> Floor Prices tab -> lower CPM by 15-20%",
    ),
    "RENDER_FAILURE": (
        ":large_yellow_circle:",
        "RENDER_FAILURE",
        "-> *Execute:* Check VAST tag health "
        "-> Publishers -> {pub} -> Ad Units -> test tag",
    ),
}


# ---------------------------------------------------------------------------
# State / deduplication (own state file, not shared core/slack STATE_FILE)
# ---------------------------------------------------------------------------

def _load_state() -> dict:
    if not os.path.exists(STATE_FILE):
        return {}
    try:
        with open(STATE_FILE, "r") as fh:
            return json.load(fh)
    except (json.JSONDecodeError, OSError):
        return {}


def _save_state(state: dict) -> None:
    with open(STATE_FILE, "w") as fh:
        json.dump(state, fh)


def _already_sent_today() -> bool:
    today_str = datetime.now(ET).date().isoformat()
    state = _load_state()
    return ALERT_KEY in state.get(today_str, [])


def _mark_sent() -> None:
    today_str = datetime.now(ET).date().isoformat()
    state = _load_state()
    # Drop stale dates
    state = {k: v for k, v in state.items() if k == today_str}
    sent_today = state.setdefault(today_str, [])
    if ALERT_KEY not in sent_today:
        sent_today.append(ALERT_KEY)
    _save_state(state)


# ---------------------------------------------------------------------------
# Data aggregation
# ---------------------------------------------------------------------------

def _rollup_to_publisher(rows: list[dict]) -> list[dict]:
    """
    Aggregate pub × demand rows to publisher level by summing all funnel
    metrics.  Returns one dict per PUBLISHER_ID.
    """
    accum: dict[str, dict] = {}
    for row in rows:
        pub_id   = str(row.get("PUBLISHER_ID", ""))
        pub_name = str(row.get("PUBLISHER_NAME") or pub_id or "Unknown")

        if pub_id not in accum:
            accum[pub_id] = {
                "pub_id":        pub_id,
                "pub_name":      pub_name,
                "OPPORTUNITIES": 0.0,
                "BID_REQUESTS":  0.0,
                "BIDS":          0.0,
                "WINS":          0.0,
                "IMPRESSIONS":   0.0,
                "GROSS_REVENUE": 0.0,
                "PUB_PAYOUT":    0.0,
            }

        a = accum[pub_id]
        for metric in ("OPPORTUNITIES", "BID_REQUESTS", "BIDS", "WINS",
                       "IMPRESSIONS", "GROSS_REVENUE", "PUB_PAYOUT"):
            a[metric] += sf(row.get(metric, 0))

    return list(accum.values())


# ---------------------------------------------------------------------------
# Funnel rate computation and classification
# ---------------------------------------------------------------------------

def _safe_rate(numerator: float, denominator: float) -> float:
    """Return numerator / denominator, or 0.0 if denominator is zero."""
    return (numerator / denominator) if denominator > 0 else 0.0


def _compute_funnel(pub: dict) -> dict:
    """Compute per-publisher funnel rates and add them to the dict in-place."""
    opps  = pub["OPPORTUNITIES"]
    breqs = pub["BID_REQUESTS"]
    bids  = pub["BIDS"]
    wins  = pub["WINS"]
    imps  = pub["IMPRESSIONS"]

    pub["opp_fill"]          = _safe_rate(imps,  opps)
    pub["bid_request_rate"]  = _safe_rate(breqs, opps)
    pub["bid_rate"]          = _safe_rate(bids,  breqs)
    pub["win_rate"]          = _safe_rate(wins,  bids)
    pub["imp_rate"]          = _safe_rate(imps,  wins)
    return pub


def _classify_stage(pub: dict) -> str:
    """Return the funnel leak stage name for a publisher."""
    if pub["bid_request_rate"] < BID_REQUEST_RATE_THRESHOLD:
        return "NO_DEMAND_COVERAGE"
    if pub["bid_rate"] < BID_RATE_THRESHOLD:
        return "LOW_BID_RESPONSE"
    if pub["win_rate"] < WIN_RATE_THRESHOLD:
        return "LOSING_AUCTIONS"
    if pub["imp_rate"] < IMP_RATE_THRESHOLD:
        return "RENDER_FAILURE"
    return "HEALTHY"


def _revenue_lost_estimate(pub: dict, median_healthy_fill: float) -> float:
    """
    Estimate potential revenue if this publisher matched the median healthy
    opp_fill rate.

    Formula:
        additional_imps  = (median_fill - pub_fill) × opportunities
        ecpm             = gross_revenue / (impressions / 1000)   [or 0]
        revenue_estimate = (additional_imps / 1000) × ecpm
    """
    imps = pub["IMPRESSIONS"]
    rev  = pub["GROSS_REVENUE"]
    opps = pub["OPPORTUNITIES"]
    fill = pub["opp_fill"]

    if fill >= median_healthy_fill:
        return 0.0

    ecpm = (rev / (imps / 1_000)) if imps > 0 else 0.0
    additional_imps = (median_healthy_fill - fill) * opps
    return (additional_imps / 1_000) * ecpm


# ---------------------------------------------------------------------------
# Slack Block Kit builder
# ---------------------------------------------------------------------------

def _pct_str(rate: float) -> str:
    """Format a 0-1 fraction as a percentage string, e.g. 0.452 → '45.2%'."""
    return f"{rate * 100:.1f}%"


def _funnel_line(pub: dict) -> str:
    opps  = pub["OPPORTUNITIES"]
    breqs = pub["BID_REQUESTS"]
    return (
        f"Opps: {fmt_n(opps)}"
        f" → BidReq: {_pct_str(pub['bid_request_rate'])}"
        f" → Bids: {_pct_str(pub['bid_rate'])}"
        f" → Wins: {_pct_str(pub['win_rate'])}"
        f" → Imps: {_pct_str(pub['imp_rate'])}"
    )


def _pub_block(pub: dict) -> dict:
    """Build a single Slack section block for one broken publisher."""
    stage    = pub["stage"]
    emoji, label, action_tpl = STAGE_META[stage]
    action   = action_tpl.format(pub=pub["pub_name"])
    rev_lost = pub.get("revenue_lost_estimate", 0.0)

    text = (
        f"*{pub['pub_name']}*  {emoji} `{label}`\n"
        f"{_funnel_line(pub)}\n"
        f"Revenue yesterday: {fmt_usd(pub['GROSS_REVENUE'])}  |  "
        f"Estimated upside: {fmt_usd(rev_lost)}\n"
        f"{action}"
    )
    return {"type": "section", "text": {"type": "mrkdwn", "text": text}}


def _build_blocks(
    broken: list[dict],
    date_label: str,
    now_label: str,
    total_opps: float,
    total_upside: float,
) -> list:
    n_broken = len(broken)

    blocks = [
        {
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": f":funnel:  Fill Funnel Analysis — {date_label}",
                "emoji": True,
            },
        },
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": (
                    f"{n_broken} publisher{'s' if n_broken != 1 else ''} with funnel breaks "
                    f"across {fmt_n(total_opps)} opportunities "
                    f"— est. {fmt_usd(total_upside)} recoverable revenue"
                ),
            },
        },
        {"type": "divider"},
    ]

    for pub in broken[:MAX_PUBS_IN_SLACK]:
        blocks.append(_pub_block(pub))
        blocks.append({"type": "divider"})

    blocks.append({
        "type": "context",
        "elements": [
            {
                "type": "mrkdwn",
                "text": f"PGAM Intelligence · Fill Funnel Agent · {now_label}",
            }
        ],
    })

    return blocks


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def run():
    """
    Execute the fill funnel analysis. Designed to be called by a scheduler
    or run directly: `python -m agents.optimization.fill_funnel`.
    """
    now_et     = datetime.now(ET)
    date_label = now_et.strftime("%A, %B %-d")
    now_label  = now_et.strftime("%H:%M ET")

    # ── 0. Weekday guard (Mon=0 … Fri=4) ────────────────────────────────────
    if now_et.weekday() > 4:
        print("[fill_funnel] Weekend — skipping.")
        return

    # ── 1. Deduplication check ───────────────────────────────────────────────
    if _already_sent_today():
        print("[fill_funnel] Report already sent today — skipping.")
        return

    # ── 2. Fetch yesterday's publisher × demand data ─────────────────────────
    yest = yesterday()
    print(f"[fill_funnel] Fetching pub x demand data for {yest}…")
    try:
        rows = report_pub_demand(yest, yest)
    except Exception as exc:
        print(f"[fill_funnel] API fetch failed: {exc}")
        return

    if not rows:
        print("[fill_funnel] No data returned from API — aborting.")
        return

    print(f"[fill_funnel] {len(rows)} pub x demand rows received.")

    # ── 3. Roll up to publisher level ────────────────────────────────────────
    publishers = _rollup_to_publisher(rows)
    print(f"[fill_funnel] {len(publishers)} unique publishers after rollup.")

    # ── 4. Compute funnel rates and classify ─────────────────────────────────
    for pub in publishers:
        _compute_funnel(pub)
        pub["stage"] = _classify_stage(pub)

    # ── 5. Identify healthy publishers and compute median fill rate ──────────
    healthy = [p for p in publishers if p["stage"] == "HEALTHY"]
    if healthy:
        fills = sorted(p["opp_fill"] for p in healthy)
        mid   = len(fills) // 2
        median_healthy_fill = (
            (fills[mid - 1] + fills[mid]) / 2 if len(fills) % 2 == 0 else fills[mid]
        )
    else:
        median_healthy_fill = 0.5   # sensible default if no healthy pubs

    print(
        f"[fill_funnel] {len(healthy)} healthy publishers | "
        f"median opp_fill: {median_healthy_fill * 100:.1f}%"
    )

    # ── 6. Filter and sort broken publishers ─────────────────────────────────
    broken = [
        p for p in publishers
        if p["stage"] != "HEALTHY" and p["OPPORTUNITIES"] > MIN_OPPORTUNITIES
    ]

    if not broken:
        print("[fill_funnel] No broken publishers with meaningful traffic — no alert needed.")
        _mark_sent()
        return

    # Attach revenue lost estimate
    for pub in broken:
        pub["revenue_lost_estimate"] = _revenue_lost_estimate(pub, median_healthy_fill)

    # Sort by potential revenue lost (highest upside first)
    broken.sort(key=lambda p: p["revenue_lost_estimate"], reverse=True)

    total_opps   = sum(p["OPPORTUNITIES"] for p in broken)
    total_upside = sum(p["revenue_lost_estimate"] for p in broken)

    print(
        f"[fill_funnel] {len(broken)} broken publishers | "
        f"total opps: {fmt_n(total_opps)} | "
        f"est. upside: {fmt_usd(total_upside)}"
    )

    # ── 7. Build and post Slack message ──────────────────────────────────────
    blocks = _build_blocks(
        broken=broken,
        date_label=date_label,
        now_label=now_label,
        total_opps=total_opps,
        total_upside=total_upside,
    )

    fallback = (
        f"Fill Funnel Analysis: {len(broken)} publisher(s) with funnel breaks "
        f"across {fmt_n(total_opps)} opps — est. {fmt_usd(total_upside)} recoverable."
    )

    send_blocks(blocks=blocks, text=fallback)
    _mark_sent()
    print(f"[fill_funnel] Report sent — {len(broken)} broken publishers flagged.")


if __name__ == "__main__":
    run()
