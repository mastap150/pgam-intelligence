"""
scripts/pilot_snapshot.py
~~~~~~~~~~~~~~~~~~~~~~~~~
Daily baseline snapshot agent for the PubNative (supplier 28) and AppStock
(supplier 33) pilot.

Scheduled for 09:00 ET each day.  Fetches yesterday's publisher x demand data
via the date-accurate GET stats API, computes per-supplier funnel metrics,
writes a JSON log entry, and posts a structured Slack summary.

State / dedup
-------------
``logs/pilot_state.json`` — keyed by date string ("YYYY-MM-DD").  If today's
entry already exists the agent exits immediately without re-posting.

Usage
-----
    python -m scripts.pilot_snapshot
    # or directly:
    python scripts/pilot_snapshot.py
"""

import json
import os
from datetime import date, datetime, timedelta

import pytz

from core.api import yesterday, sf, fmt_usd, fmt_n, pct
from core import ll_report

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

PILOT_SUPPLIERS = {
    "pubnative": {"supplier_id": 28, "label": "PubNative"},
    "appstock":  {"supplier_id": 33, "label": "AppStock"},
}

LOG_DIR = os.path.join(os.path.dirname(__file__), "..", "logs")

ET = pytz.timezone("US/Eastern")

# State file for daily dedup
_STATE_FILE = os.path.join(LOG_DIR, "pilot_state.json")


# ---------------------------------------------------------------------------
# State helpers
# ---------------------------------------------------------------------------

def _load_state() -> dict:
    if not os.path.exists(_STATE_FILE):
        return {}
    try:
        with open(_STATE_FILE) as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError):
        return {}


def _already_ran(date_str: str) -> bool:
    return date_str in _load_state()


def _mark_ran(date_str: str):
    state = _load_state()
    state[date_str] = True
    os.makedirs(LOG_DIR, exist_ok=True)
    with open(_STATE_FILE, "w") as f:
        json.dump(state, f, indent=2)


# ---------------------------------------------------------------------------
# Log file helpers
# ---------------------------------------------------------------------------

def _log_path(date_str: str) -> str:
    """Return the monthly rolling log file path for the given date."""
    ym = date_str[:7]  # "YYYY-MM"
    return os.path.join(LOG_DIR, f"pilot_{ym}.json")


def _load_log(date_str: str) -> list[dict]:
    path = _log_path(date_str)
    if not os.path.exists(path):
        return []
    try:
        with open(path) as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError):
        return []


def _append_log(entry: dict):
    date_str = entry["date"]
    path = _log_path(date_str)
    os.makedirs(LOG_DIR, exist_ok=True)
    entries = _load_log(date_str)
    # Replace existing entry for same date if present
    entries = [e for e in entries if e.get("date") != date_str]
    entries.append(entry)
    with open(path, "w") as f:
        json.dump(entries, f, indent=2)


def _find_entry(date_str: str) -> dict | None:
    """Return the log entry for a specific date, or None if not found."""
    for entry in _load_log(date_str):
        if entry.get("date") == date_str:
            return entry
    return None


def _first_snapshot_date() -> str | None:
    """Return the earliest date we have a log entry for, scanning monthly files."""
    import glob as _glob
    files = sorted(_glob.glob(os.path.join(LOG_DIR, "pilot_*.json")))
    for path in files:
        try:
            with open(path) as f:
                entries = json.load(f)
            if entries:
                dates = [e.get("date", "") for e in entries if e.get("date")]
                if dates:
                    return min(dates)
        except (json.JSONDecodeError, OSError):
            continue
    return None


# ---------------------------------------------------------------------------
# Data helpers
# ---------------------------------------------------------------------------

def _matches_supplier(name: str, key: str) -> bool:
    """True if the publisher name string matches the pilot supplier key."""
    return key.lower() in str(name).lower()


def _extract(row: dict, *keys) -> float:
    for k in keys:
        if k in row:
            return sf(row[k])
    return 0.0


def _filter_rows(rows: list[dict], supplier_key: str) -> list[dict]:
    """Return rows whose publisher name matches the given supplier key."""
    name_keys = ("PUBLISHER_NAME", "publisher_name", "PUBLISHER", "publisher")
    result = []
    for row in rows:
        name = ""
        for k in name_keys:
            if row.get(k):
                name = str(row[k])
                break
        if _matches_supplier(name, supplier_key):
            result.append(row)
    return result


def _compute_supplier_totals(pub_rows: list[dict]) -> dict:
    """Aggregate publisher-level rows into a single totals dict."""
    gross_revenue = sum(_extract(r, "GROSS_REVENUE", "gross_revenue") for r in pub_rows)
    pub_payout    = sum(_extract(r, "PUB_PAYOUT",    "pub_payout")    for r in pub_rows)
    opportunities = sum(_extract(r, "OPPORTUNITIES", "opportunities") for r in pub_rows)
    bid_requests  = sum(_extract(r, "BID_REQUESTS",  "bid_requests")  for r in pub_rows)
    bids          = sum(_extract(r, "BIDS",          "bids")          for r in pub_rows)
    wins          = sum(_extract(r, "WINS",          "wins")          for r in pub_rows)
    impressions   = sum(_extract(r, "IMPRESSIONS",   "impressions")   for r in pub_rows)

    margin_pct    = pct(gross_revenue - pub_payout, gross_revenue)
    opp_fill_pct  = pct(impressions,   opportunities)
    bid_fill_pct  = pct(bids,          bid_requests)
    win_rate_pct  = pct(wins,          bids)
    ecpm          = (gross_revenue / impressions * 1000) if impressions > 0 else 0.0

    return {
        "gross_revenue": round(gross_revenue, 4),
        "pub_payout":    round(pub_payout,    4),
        "margin_pct":    round(margin_pct,    4),
        "opportunities": int(opportunities),
        "bid_requests":  int(bid_requests),
        "bids":          int(bids),
        "wins":          int(wins),
        "impressions":   int(impressions),
        "opp_fill_pct":  round(opp_fill_pct,  6),
        "bid_fill_pct":  round(bid_fill_pct,  4),
        "win_rate_pct":  round(win_rate_pct,  4),
        "ecpm":          round(ecpm,           4),
    }


def _compute_demand_breakdown(demand_rows: list[dict]) -> list[dict]:
    """
    Aggregate demand-partner rows for a single supplier into a sorted list.
    Each row should contain DEMAND_PARTNER_NAME (or DEMAND_PARTNER) plus metrics.
    """
    by_demand: dict[str, dict] = {}
    name_keys = ("DEMAND_PARTNER_NAME", "DEMAND_PARTNER", "demand_partner_name",
                 "demand_partner", "DEMAND_NAME", "demand_name")
    for row in demand_rows:
        name = ""
        for k in name_keys:
            if row.get(k):
                name = str(row[k])
                break
        if not name:
            name = "Unknown"

        if name not in by_demand:
            by_demand[name] = {
                "gross_revenue": 0.0,
                "bid_requests":  0.0,
                "bids":          0.0,
                "wins":          0.0,
                "impressions":   0.0,
            }
        by_demand[name]["gross_revenue"] += _extract(row, "GROSS_REVENUE", "gross_revenue")
        by_demand[name]["bid_requests"]  += _extract(row, "BID_REQUESTS",  "bid_requests")
        by_demand[name]["bids"]          += _extract(row, "BIDS",          "bids")
        by_demand[name]["wins"]          += _extract(row, "WINS",          "wins")
        by_demand[name]["impressions"]   += _extract(row, "IMPRESSIONS",   "impressions")

    result = []
    for name, t in by_demand.items():
        result.append({
            "name":         name,
            "revenue":      round(t["gross_revenue"], 4),
            "bid_fill_pct": round(pct(t["bids"], t["bid_requests"]), 4),
            "win_rate_pct": round(pct(t["wins"], t["bids"]), 4),
            "impressions":  int(t["impressions"]),
        })
    result.sort(key=lambda x: x["revenue"], reverse=True)
    return result


# ---------------------------------------------------------------------------
# DoD helpers
# ---------------------------------------------------------------------------

def _dod_pct(today_val: float, yesterday_val: float) -> float | None:
    if yesterday_val <= 0:
        return None
    return round((today_val - yesterday_val) / yesterday_val * 100, 2)


def _format_dod(dod: float | None) -> str:
    if dod is None:
        return "n/a"
    arrow = "▲" if dod >= 0 else "▼"
    sign  = "+" if dod >= 0 else ""
    return f"{arrow} {sign}{dod:.1f}% DoD"


# ---------------------------------------------------------------------------
# Formatting helpers
# ---------------------------------------------------------------------------

def _fmt_opps(n: float) -> str:
    """Format large opportunity counts with B/M suffixes."""
    n = float(n)
    if n >= 1_000_000_000:
        return f"{n / 1_000_000_000:.1f}B"
    if n >= 1_000_000:
        return f"{n / 1_000_000:.1f}M"
    if n >= 1_000:
        return f"{n / 1_000:.1f}K"
    return str(int(n))


def _next_monday(from_date: date) -> str:
    """Return the next Monday on or after from_date as 'Mon Apr 20'."""
    days_ahead = 7 - from_date.weekday()  # Monday is 0
    if days_ahead == 7:
        days_ahead = 0
    next_mon = from_date + timedelta(days=days_ahead)
    return next_mon.strftime("%a %b %-d")


# ---------------------------------------------------------------------------
# Slack Block Kit builders
# ---------------------------------------------------------------------------

def _supplier_section_text(
    label: str,
    totals: dict,
    by_demand: list[dict],
    dod: float | None,
    max_demand: int = 5,
) -> str:
    """Build the mrkdwn text for one supplier section."""
    dod_str = _format_dod(dod)
    lines = [
        f"*{label}*  {dod_str}",
        (
            f"Revenue: {fmt_usd(totals['gross_revenue'])}  |  "
            f"Margin: {totals['margin_pct']:.1f}%  |  "
            f"eCPM: {fmt_usd(totals['ecpm'])}"
        ),
        (
            f"Opps: {_fmt_opps(totals['opportunities'])}  "
            f"\u2192  Bid Fill: {totals['bid_fill_pct']:.2f}%  "
            f"\u2192  Win Rate: {totals['win_rate_pct']:.1f}%  "
            f"\u2192  Imps: {fmt_n(totals['impressions'])}"
        ),
    ]
    if by_demand:
        lines.append("\nTop demand:")
        for d in by_demand[:max_demand]:
            lines.append(
                f"\u2022 {d['name']}: {fmt_usd(d['revenue'])} | "
                f"bfr {d['bid_fill_pct']:.1f}% | "
                f"wr {d['win_rate_pct']:.1f}%"
            )
    return "\n".join(lines)


def _build_blocks(
    date_str: str,
    pn_totals: dict,
    pn_demand: list[dict],
    as_totals: dict,
    as_demand: list[dict],
    pn_dod: float | None,
    as_dod: float | None,
    first_snapshot: str | None,
    actions_applied: list,
) -> list:
    today_date = date.fromisoformat(date_str)
    pilot_week = "n/a"
    if first_snapshot:
        try:
            first_dt = date.fromisoformat(first_snapshot)
            week_num = ((today_date - first_dt).days // 7) + 1
            pilot_week = str(week_num)
            first_label = first_dt.strftime("%b %-d")
        except ValueError:
            first_label = first_snapshot
    else:
        first_label = "unknown"

    next_mon = _next_monday(today_date)
    changes_str = ", ".join(str(a) for a in actions_applied) if actions_applied else "none"

    blocks = [
        # Header
        {
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": f":bar_chart: Pilot Snapshot \u2014 PubNative & AppStock \u2014 {date_str}",
                "emoji": True,
            },
        },
        {"type": "divider"},
        # PubNative section
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": _supplier_section_text(
                    "PubNative - In-App Magnite",
                    pn_totals,
                    pn_demand,
                    pn_dod,
                ),
            },
        },
        {"type": "divider"},
        # AppStock section
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": _supplier_section_text(
                    "AppStock",
                    as_totals,
                    as_demand,
                    as_dod,
                ),
            },
        },
        {"type": "divider"},
    ]

    # Recent changes block (only if actions were applied yesterday)
    if actions_applied:
        change_lines = [f":wrench: *Changes applied {date_str}:*"]
        for action in actions_applied:
            change_lines.append(f"\u2022 {action}")
        blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": "\n".join(change_lines),
            },
        })
        blocks.append({"type": "divider"})

    # Footer context block
    blocks.append({
        "type": "context",
        "elements": [
            {
                "type": "mrkdwn",
                "text": (
                    f"Pilot week {pilot_week} (started {first_label})"
                    f"  \u00b7  Changes applied: {changes_str}"
                    f"  \u00b7  Next review: {next_mon}"
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
    Execute the pilot snapshot agent.

    Fetches yesterday's data, computes per-supplier metrics, appends to the
    monthly JSON log, and posts a Slack summary.  Skips if already ran today.
    """
    from core.slack import send_blocks

    now_et    = datetime.now(ET)
    today_str = now_et.strftime("%Y-%m-%d")
    yest_str  = yesterday()

    # ── 1. Daily dedup ───────────────────────────────────────────────────────
    if _already_ran(today_str):
        print(f"[pilot_snapshot] Already ran for {today_str} — skipping.")
        return

    print(f"[pilot_snapshot] Fetching data for {yest_str} …")

    # ── 2. Fetch data ────────────────────────────────────────────────────────
    try:
        demand_rows = ll_report.fetch_publisher_demand(yest_str, yest_str)
    except Exception as exc:
        print(f"[pilot_snapshot] fetch_publisher_demand failed: {exc}")
        demand_rows = []

    try:
        pub_rows = ll_report.fetch_publisher(yest_str, yest_str)
    except Exception as exc:
        print(f"[pilot_snapshot] fetch_publisher failed: {exc}")
        pub_rows = []

    # ── 3. Filter to pilot publishers ────────────────────────────────────────
    pn_demand_rows = _filter_rows(demand_rows, "pubnative")
    as_demand_rows = _filter_rows(demand_rows, "appstock")

    pn_pub_rows    = _filter_rows(pub_rows, "pubnative")
    as_pub_rows    = _filter_rows(pub_rows, "appstock")

    # ── 4. Compute totals ────────────────────────────────────────────────────
    pn_totals = _compute_supplier_totals(pn_pub_rows)
    as_totals = _compute_supplier_totals(as_pub_rows)

    pn_demand = _compute_demand_breakdown(pn_demand_rows)
    as_demand = _compute_demand_breakdown(as_demand_rows)

    print(
        f"[pilot_snapshot] PubNative: {fmt_usd(pn_totals['gross_revenue'])}  |  "
        f"AppStock: {fmt_usd(as_totals['gross_revenue'])}"
    )

    # ── 5. Load yesterday's entry for DoD ────────────────────────────────────
    prev_date  = (date.fromisoformat(yest_str) - timedelta(days=1)).isoformat()
    prev_entry = _find_entry(prev_date)

    pn_dod: float | None = None
    as_dod: float | None = None
    if prev_entry:
        pn_prev = prev_entry.get("pubnative", {})
        as_prev = prev_entry.get("appstock",  {})
        pn_dod  = _dod_pct(pn_totals["gross_revenue"], sf(pn_prev.get("gross_revenue", 0)))
        as_dod  = _dod_pct(as_totals["gross_revenue"], sf(as_prev.get("gross_revenue", 0)))

    # ── 6. Pull any actions that were applied yesterday ──────────────────────
    yest_entry      = _find_entry(yest_str)
    actions_applied = (yest_entry or {}).get("actions_applied", [])

    # ── 7. Build and write log entry ─────────────────────────────────────────
    log_entry = {
        "date":       yest_str,
        "pubnative":  {**pn_totals, "by_demand": pn_demand},
        "appstock":   {**as_totals, "by_demand": as_demand},
        "changes": {
            "pubnative_revenue_dod": pn_dod,
            "appstock_revenue_dod":  as_dod,
        },
        "actions_applied": actions_applied,
    }
    _append_log(log_entry)
    print(f"[pilot_snapshot] Log entry written for {yest_str}.")

    # ── 8. Post Slack summary ────────────────────────────────────────────────
    first_snapshot = _first_snapshot_date()
    blocks = _build_blocks(
        date_str       = yest_str,
        pn_totals      = pn_totals,
        pn_demand      = pn_demand,
        as_totals      = as_totals,
        as_demand      = as_demand,
        pn_dod         = pn_dod,
        as_dod         = as_dod,
        first_snapshot = first_snapshot,
        actions_applied= actions_applied,
    )
    fallback = (
        f"Pilot Snapshot {yest_str}: "
        f"PubNative {fmt_usd(pn_totals['gross_revenue'])} "
        f"({_format_dod(pn_dod)}) | "
        f"AppStock {fmt_usd(as_totals['gross_revenue'])} "
        f"({_format_dod(as_dod)})"
    )
    send_blocks(blocks=blocks, text=fallback)

    # ── 9. Mark as done ──────────────────────────────────────────────────────
    _mark_ran(today_str)
    print(f"[pilot_snapshot] Done — Slack posted, state saved for {today_str}.")


if __name__ == "__main__":
    run()
