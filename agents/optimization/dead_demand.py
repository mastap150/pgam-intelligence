"""
agents/optimization/dead_demand.py

Weekly agent (Mondays at 08:00 ET) that scans the last 7 days for
publisher × demand combinations that are configured and receiving
opportunities but generating ZERO revenue.  These are "dead seats" —
wasted bid requests that could be replaced with better demand.

Seat categories
---------------
  DEAD SEAT   — demand is bidding but never winning:
                  OPPORTUNITIES > 500,000
                  BIDS > 0
                  GROSS_REVENUE == 0  OR  (IMPRESSIONS == 0 AND WINS == 0)
                Floor is above their clearing price.

  GHOST SEAT  — demand never responds at all:
                  OPPORTUNITIES > 500,000
                  BID_REQUESTS > 0   (we're sending requests)
                  BIDS == 0          (demand never responds)
                Wrong format / geo / seat not actually active.

Sorting / capping
-----------------
Publishers are sorted by total OPPORTUNITIES wasted (dead + ghost combined),
highest first.  At most 10 publishers appear in the Slack message.

Deduplication
-------------
Weekly dedup via STATE_FILE = /tmp/pgam_dead_demand_state.json.
One post per ISO week (year + week number).
"""

import json
import os
from datetime import datetime

import pytz

from core.api import yesterday, n_days_ago, sf, fmt_usd, fmt_n
from core.ll_report import report_pub_demand
from core.slack import send_blocks

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
ALERT_KEY         = "dead_demand_weekly"
STATE_FILE        = "/tmp/pgam_dead_demand_state.json"
MIN_OPPORTUNITIES = 500_000   # ignore trivially small seat pairs
MAX_PUBS_IN_SLACK = 10

ET = pytz.timezone("US/Eastern")


# ---------------------------------------------------------------------------
# State / deduplication (weekly, keyed by ISO year-week)
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


def _week_key() -> str:
    """Return ISO year-week string, e.g. '2026-W15'."""
    now = datetime.now(ET)
    iso = now.isocalendar()
    return f"{iso[0]}-W{iso[1]:02d}"


def _already_sent_this_week() -> bool:
    week = _week_key()
    state = _load_state()
    return ALERT_KEY in state.get(week, [])


def _mark_sent() -> None:
    week = _week_key()
    state = _load_state()
    # Retain only the current week to keep file small
    state = {k: v for k, v in state.items() if k == week}
    sent_week = state.setdefault(week, [])
    if ALERT_KEY not in sent_week:
        sent_week.append(ALERT_KEY)
    _save_state(state)


# ---------------------------------------------------------------------------
# Seat classification
# ---------------------------------------------------------------------------

def _classify_row(row: dict) -> str | None:
    """
    Return 'DEAD', 'GHOST', or None for a pub × demand row.

    None means the row does not qualify as either problem type.
    """
    opps  = sf(row.get("OPPORTUNITIES", 0))
    breqs = sf(row.get("BID_REQUESTS",  0))
    bids  = sf(row.get("BIDS",          0))
    wins  = sf(row.get("WINS",          0))
    imps  = sf(row.get("IMPRESSIONS",   0))
    rev   = sf(row.get("GROSS_REVENUE", 0))

    if opps <= MIN_OPPORTUNITIES:
        return None

    # Ghost: requests sent, demand never responds
    if breqs > 0 and bids == 0:
        return "GHOST"

    # Dead: demand responds (bids) but never wins / generates revenue
    if bids > 0 and (rev == 0.0 or (imps == 0 and wins == 0)):
        return "DEAD"

    return None


def _build_seat_entry(row: dict, seat_type: str) -> dict:
    """Package a row into a structured seat dict."""
    return {
        "demand_id":    str(row.get("DEMAND_ID",   "")),
        "demand_name":  str(row.get("DEMAND_NAME") or row.get("DEMAND_ID") or "Unknown"),
        "seat_type":    seat_type,
        "OPPORTUNITIES": sf(row.get("OPPORTUNITIES", 0)),
        "BID_REQUESTS":  sf(row.get("BID_REQUESTS",  0)),
        "BIDS":          sf(row.get("BIDS",          0)),
        "WINS":          sf(row.get("WINS",          0)),
        "IMPRESSIONS":   sf(row.get("IMPRESSIONS",   0)),
        "GROSS_REVENUE": sf(row.get("GROSS_REVENUE", 0)),
    }


# ---------------------------------------------------------------------------
# Aggregation by publisher
# ---------------------------------------------------------------------------

def _group_by_publisher(rows: list[dict]) -> dict[str, dict]:
    """
    Walk all rows, classify each one, and group dead/ghost seats under
    their publisher.

    Returns a dict keyed by PUBLISHER_ID:
        {
          "pub_id":     str,
          "pub_name":   str,
          "dead_seats": list[dict],
          "ghost_seats": list[dict],
          "total_wasted_opps": float,   # dead + ghost opportunities combined
        }
    """
    pubs: dict[str, dict] = {}

    for row in rows:
        seat_type = _classify_row(row)
        if seat_type is None:
            continue

        pub_id   = str(row.get("PUBLISHER_ID", ""))
        pub_name = str(row.get("PUBLISHER_NAME") or pub_id or "Unknown")

        if pub_id not in pubs:
            pubs[pub_id] = {
                "pub_id":             pub_id,
                "pub_name":           pub_name,
                "dead_seats":         [],
                "ghost_seats":        [],
                "total_wasted_opps":  0.0,
            }

        entry = _build_seat_entry(row, seat_type)
        opps  = entry["OPPORTUNITIES"]

        if seat_type == "DEAD":
            pubs[pub_id]["dead_seats"].append(entry)
        else:
            pubs[pub_id]["ghost_seats"].append(entry)

        pubs[pub_id]["total_wasted_opps"] += opps

    return pubs


# ---------------------------------------------------------------------------
# Slack Block Kit builder
# ---------------------------------------------------------------------------

def _dead_seat_line(seat: dict, pub_name: str) -> str:
    opps  = seat["OPPORTUNITIES"]
    bids  = seat["BIDS"]
    name  = seat["demand_name"]
    return (
        f"• *{name}*: {fmt_n(opps)} opps, {fmt_n(bids)} bids, 0 wins "
        f"-> floor likely above their max CPM "
        f"-> *Execute:* LL UI -> Publishers -> {pub_name} "
        f"-> Demand Rules -> {name} -> lower floor or remove minimum"
    )


def _ghost_seat_line(seat: dict, pub_name: str) -> str:
    opps  = seat["OPPORTUNITIES"]
    breqs = seat["BID_REQUESTS"]
    name  = seat["demand_name"]
    return (
        f"• *{name}*: {fmt_n(opps)} opps sent, 0 bids "
        f"-> not active for this format/geo "
        f"-> *Execute:* Contact demand partner or remove seat to reduce wasted QPS"
    )


def _pub_section_text(pub: dict) -> str:
    n_dead  = len(pub["dead_seats"])
    n_ghost = len(pub["ghost_seats"])
    lines   = [
        f"*{pub['pub_name']}*  —  "
        f"{n_dead} dead seat{'s' if n_dead != 1 else ''}, "
        f"{n_ghost} ghost seat{'s' if n_ghost != 1 else ''}  |  "
        f"Wasted opps: {fmt_n(pub['total_wasted_opps'])}"
    ]

    # Dead seats (sorted by opportunities descending)
    for seat in sorted(pub["dead_seats"], key=lambda s: -s["OPPORTUNITIES"]):
        lines.append(_dead_seat_line(seat, pub["pub_name"]))

    # Ghost seats (sorted by opportunities descending)
    for seat in sorted(pub["ghost_seats"], key=lambda s: -s["OPPORTUNITIES"]):
        lines.append(_ghost_seat_line(seat, pub["pub_name"]))

    return "\n".join(lines)


def _build_blocks(
    pubs_sorted: list[dict],
    date_label: str,
    now_label: str,
    total_dead: int,
    total_ghost: int,
    total_pubs: int,
) -> list:
    blocks = [
        {
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": f":ghost:  Dead Demand Audit — Week of {date_label}",
                "emoji": True,
            },
        },
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": (
                    f"{total_dead} dead seat{'s' if total_dead != 1 else ''} "
                    f"(bidding, never winning) + "
                    f"{total_ghost} ghost seat{'s' if total_ghost != 1 else ''} "
                    f"(never bidding) across {total_pubs} publisher{'s' if total_pubs != 1 else ''} "
                    f"— 7-day window"
                ),
            },
        },
        {"type": "divider"},
    ]

    for pub in pubs_sorted[:MAX_PUBS_IN_SLACK]:
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": _pub_section_text(pub)},
        })
        blocks.append({"type": "divider"})

    blocks.append({
        "type": "context",
        "elements": [
            {
                "type": "mrkdwn",
                "text": (
                    "Removing/fixing these seats recovers QPS capacity and can "
                    "improve fill rate for active demand."
                ),
            }
        ],
    })
    blocks.append({
        "type": "context",
        "elements": [
            {
                "type": "mrkdwn",
                "text": f"PGAM Intelligence · Dead Demand Agent · {now_label}",
            }
        ],
    })

    return blocks


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def run():
    """
    Execute the dead demand audit. Designed to be called by a scheduler
    or run directly: `python -m agents.optimization.dead_demand`.

    Runs on Mondays only (weekday == 0 in ET timezone).
    """
    now_et     = datetime.now(ET)
    date_label = now_et.strftime("%A, %B %-d")
    now_label  = now_et.strftime("%H:%M ET")

    # ── 0. Monday guard ──────────────────────────────────────────────────────
    if now_et.weekday() != 0:
        print("[dead_demand] Not Monday — skipping.")
        return

    # ── 1. Weekly deduplication check ────────────────────────────────────────
    if _already_sent_this_week():
        print("[dead_demand] Report already sent this week — skipping.")
        return

    # ── 2. Fetch last 7 days of publisher × demand data ──────────────────────
    start_date = n_days_ago(7)
    end_date   = yesterday()

    print(f"[dead_demand] Fetching pub x demand data {start_date} → {end_date}…")
    try:
        rows = report_pub_demand(start_date, end_date)
    except Exception as exc:
        print(f"[dead_demand] API fetch failed: {exc}")
        return

    if not rows:
        print("[dead_demand] No data returned from API — aborting.")
        return

    print(f"[dead_demand] {len(rows)} pub x demand rows received.")

    # ── 3. Classify and group by publisher ───────────────────────────────────
    pubs = _group_by_publisher(rows)

    if not pubs:
        print("[dead_demand] No dead/ghost seats found — no alert needed.")
        _mark_sent()
        return

    # ── 4. Compute summary counts ────────────────────────────────────────────
    total_dead  = sum(len(p["dead_seats"])  for p in pubs.values())
    total_ghost = sum(len(p["ghost_seats"]) for p in pubs.values())
    total_pubs  = len(pubs)

    print(
        f"[dead_demand] {total_pubs} publishers | "
        f"{total_dead} dead seats | "
        f"{total_ghost} ghost seats"
    )

    # ── 5. Sort publishers by wasted opportunities (highest first) ────────────
    pubs_sorted = sorted(
        pubs.values(),
        key=lambda p: p["total_wasted_opps"],
        reverse=True,
    )

    # ── 6. Build and post Slack message ──────────────────────────────────────
    blocks = _build_blocks(
        pubs_sorted=pubs_sorted,
        date_label=date_label,
        now_label=now_label,
        total_dead=total_dead,
        total_ghost=total_ghost,
        total_pubs=total_pubs,
    )

    fallback = (
        f"Dead Demand Audit: {total_dead} dead seat(s) + {total_ghost} ghost seat(s) "
        f"across {total_pubs} publisher(s) — 7-day window."
    )

    send_blocks(blocks=blocks, text=fallback)
    _mark_sent()
    print(
        f"[dead_demand] Report sent — {total_pubs} publishers, "
        f"{total_dead} dead, {total_ghost} ghost seats."
    )


if __name__ == "__main__":
    run()
