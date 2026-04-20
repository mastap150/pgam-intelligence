"""
scripts/floor_optimizer.py

Dynamic floor optimizer for AppStock and PubNative — runs every 2 hours.

How it works
------------
Every 2 hours, for each active pilot publisher × demand group:
  1. Fetch today's partial stats (win rate, eCPM) from the LL API
  2. Compare to yesterday's full-day stats for the same publisher
  3. Decide direction:
       - Win rate < WIN_RATE_LOOSEN (75 %) of yesterday → loosen floor by STEP_PCT
       - Win rate > WIN_RATE_TIGHTEN (125 %) AND eCPM ≥ yesterday → tighten by STEP_PCT
       - Otherwise → hold
  4. Apply the change across all individual seats in the group via pilot_actions
  5. Post a Slack summary of all moves

Why this captures EU/APAC traffic
----------------------------------
European traffic peaks 8 AM – 5 PM CET (2–11 AM ET). Asian traffic peaks
roughly 9 AM – 6 PM CST (8 PM – 5 AM ET).  These regions bid at lower CPMs
than US daytime, so win rates drop overnight when floors are calibrated for US
traffic.  The optimizer detects the drop and loosens floors to capture this
volume, then tightens again when US prime traffic arrives.

Data lag note
-------------
The LL stats API has ~2 h of lag. "Today" queries return cumulative data from
midnight to ~2 h ago. Win rate is a ratio (wins/bids) so it's comparable
across partial and full days without normalisation.

Safety limits
-------------
MAX_DAILY_DRIFT_PCT  — floor cannot move more than ±25 % from its base value
                        within a single calendar day (cumulative, all moves)
STEP_PCT             — each individual move is ≤ 8 %
MIN_FLOOR            — absolute minimum (from pilot_actions)
MAX_FLOOR            — absolute ceiling ($10)

Reset
-----
At 08:00 ET the optimizer restores every floor to its base value and wipes the
daily drift counter, ready for a fresh US trading day.

State file: logs/floor_optimizer_state.json
"""

import json
import os
import sys
from datetime import datetime, timedelta, timezone

_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

from core.api import fetch as ll_fetch
import core.ll_mgmt as ll_mgmt
import core.floor_ledger as floor_ledger
import core.slack as slack
from scripts.pilot_actions import PILOT_SUPPLIER_IDS

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

MAX_DAILY_DRIFT_PCT = 25.0   # max cumulative drift from base per calendar day
STEP_PCT            = 8.0    # each move is this % of the current floor
WIN_RATE_LOOSEN     = 0.75   # today win_rate < 75 % of yesterday → loosen
WIN_RATE_TIGHTEN    = 1.25   # today win_rate > 125 % of yesterday → tighten
MIN_FLOOR           = 0.10   # absolute minimum — never write below this
MAX_FLOOR           = 10.0   # absolute ceiling
RESET_HOUR_ET       = 8      # restore base floors at this ET hour

# Demand IDs already written during this run — prevents duplicate writes to
# the same demand when multiple publisher groups share seats (demand-level
# writes are global, so repeating is wasted work).
_SEEN_DEMAND_IDS_THIS_RUN: set[int] = set()

ET_TZ = None
try:
    from zoneinfo import ZoneInfo
    ET_TZ = ZoneInfo("America/New_York")
except Exception:
    pass

# ---------------------------------------------------------------------------
# Pilot demand groups
# Each entry maps:
#   (publisher_name, demand_group_label, stats_demand_match, base_floor, [seat_names])
#
# stats_demand_match — substring to identify this demand partner in LL stats
# seat_names         — exact demand names used in apply_floor_change()
# ---------------------------------------------------------------------------

PILOT_GROUPS = [
    {
        "publisher":          "AppStock",
        "group":              "Unruly",
        "stats_match":        "unruly",
        "base_floor":         0.40,
        "seats": [
            "Unruly AppStock",
            "Unruly AppStock oRTB 2.6",
            "Unruly AppStock Prebid Server",
            "Unruly AppStock Prebid Server oRTB2.6",
        ],
    },
    {
        "publisher":          "AppStock",
        "group":              "Magnite",
        "stats_match":        "magnite",
        "base_floor":         0.15,
        "seats": [
            "Magnite - AppStock - In App US",
            "Magnite - AppStock - In App oRTB 2.6 US",
            "Magnite - AppStock - In App Prebid Server US",
            "Magnite - AppStock - In App Prebid Server oRTB 2.6 US",
        ],
    },
    # PubNative — Magnite (all sub-publishers share the same seats)
    {
        "publisher":          "PubNative - Display In-App US Interstitial",
        "group":              "Magnite",
        "stats_match":        "magnite",
        "base_floor":         1.00,
        "seats": [
            "Magnite - Pubnative - In App US",
            "Magnite - Pubnative - In App oRTB 2.6 US",
            "Magnite - PubNative - In App Prebid Server US",
            "Magnite - PubNative - In App Prebid Server oRTB 2.6 US",
        ],
    },
    {
        "publisher":          "PubNative - In-App Magnite",
        "group":              "Magnite",
        "stats_match":        "magnite",
        "base_floor":         1.00,
        "seats": [
            "Magnite - Pubnative - In App US",
            "Magnite - Pubnative - In App oRTB 2.6 US",
            "Magnite - PubNative - In App Prebid Server US",
            "Magnite - PubNative - In App Prebid Server oRTB 2.6 US",
        ],
    },
    # PubNative — Sovrn (floor already at MIN_FLOOR; only loosen/hold, never raise above $0.10 for Sovrn)
    {
        "publisher":          "PubNative - Display In-App EU",
        "group":              "Sovrn",
        "stats_match":        "sovrn",
        "base_floor":         0.10,
        "seats": [
            "Sovrn PubNative_300x250",
            "Sovrn PubNative_300x250 oRTB 2.6",
            "Sovrn - PubNative_300x600",
            "Sovrn - PubNative_320x50",
            "Sovrn - PubNative_320x50 oRTB 2.6",
            "Sovrn - PubNative_728x90 oRTB 2.6",
        ],
    },
    {
        "publisher":          "PubNative - Display In-App APAC",
        "group":              "Sovrn",
        "stats_match":        "sovrn",
        "base_floor":         0.10,
        "seats": [
            "Sovrn PubNative_300x250",
            "Sovrn PubNative_300x250 oRTB 2.6",
            "Sovrn - PubNative_300x600",
            "Sovrn - PubNative_320x50",
            "Sovrn - PubNative_320x50 oRTB 2.6",
            "Sovrn - PubNative_728x90 oRTB 2.6",
        ],
    },
    {
        "publisher":          "PubNative - Display In-App US Interstitial",
        "group":              "Sovrn",
        "stats_match":        "sovrn",
        "base_floor":         0.10,
        "seats": [
            "Sovrn PubNative_300x250",
            "Sovrn PubNative_300x250 oRTB 2.6",
            "Sovrn - PubNative_300x600",
            "Sovrn - PubNative_320x50",
            "Sovrn - PubNative_320x50 oRTB 2.6",
            "Sovrn - PubNative_728x90 oRTB 2.6",
        ],
    },
    {
        "publisher":          "PubNative - In-App Magnite",
        "group":              "Sovrn",
        "stats_match":        "sovrn",
        "base_floor":         0.10,
        "seats": [
            "Sovrn PubNative_300x250",
            "Sovrn PubNative_300x250 oRTB 2.6",
            "Sovrn - PubNative_300x600",
            "Sovrn - PubNative_320x50",
            "Sovrn - PubNative_320x50 oRTB 2.6",
            "Sovrn - PubNative_728x90 oRTB 2.6",
        ],
    },
]

# ---------------------------------------------------------------------------
# State
# ---------------------------------------------------------------------------

LOG_DIR    = os.path.join(_REPO_ROOT, "logs")
STATE_FILE = os.path.normpath(os.path.join(LOG_DIR, "floor_optimizer_state.json"))


def _load_state() -> dict:
    if not os.path.exists(STATE_FILE):
        return {"date": "", "positions": {}}
    try:
        with open(STATE_FILE) as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError):
        return {"date": "", "positions": {}}


def _save_state(state: dict):
    os.makedirs(LOG_DIR, exist_ok=True)
    with open(STATE_FILE, "w") as f:
        json.dump(state, f, indent=2)


def _position_key(publisher: str, group: str) -> str:
    return f"{publisher}|{group}"


def _today_str() -> str:
    return datetime.now().strftime("%Y-%m-%d")


def _yesterday_str() -> str:
    return (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")


def _now_et_str() -> str:
    try:
        return datetime.now(ET_TZ).strftime("%Y-%m-%d %H:%M ET")
    except Exception:
        return datetime.now().strftime("%Y-%m-%d %H:%M")


def _et_hour() -> int:
    """Return the current hour in ET (0-23)."""
    try:
        return datetime.now(ET_TZ).hour
    except Exception:
        return datetime.now().hour


# ---------------------------------------------------------------------------
# Stats helpers
# ---------------------------------------------------------------------------

def _fetch_publisher_stats(publisher_name: str, date_str: str) -> dict:
    """
    Fetch win rate and eCPM for a publisher on a given date.
    Returns {win_rate, ecpm, revenue, wins, bids} or empty dict.

    Uses root PubNative name for the stats API.
    """
    pub_lower = (
        "pubnative"
        if "pubnative" in publisher_name.lower()
        else publisher_name.lower()
    )

    rows = ll_fetch(
        "DATE,PUBLISHER",
        "GROSS_REVENUE,WINS,BIDS,GROSS_ECPM",
        date_str, date_str,
    )

    matched = [
        r for r in rows
        if pub_lower in (r.get("PUBLISHER_NAME") or r.get("PUBLISHER") or "").lower()
    ]

    if not matched:
        return {}

    revenue = sum(float(r.get("GROSS_REVENUE", 0) or 0) for r in matched)
    wins    = sum(float(r.get("WINS", 0) or 0) for r in matched)
    bids    = sum(float(r.get("BIDS", 0) or 0) for r in matched)
    ecpm_vals = [
        float(r.get("GROSS_ECPM", 0) or 0)
        for r in matched
        if float(r.get("GROSS_ECPM", 0) or 0) > 0
    ]
    ecpm = sum(ecpm_vals) / len(ecpm_vals) if ecpm_vals else 0.0

    return {
        "win_rate": (wins / bids) if bids > 0 else 0.0,
        "ecpm":     ecpm,
        "revenue":  revenue,
        "wins":     wins,
        "bids":     bids,
    }


# ---------------------------------------------------------------------------
# Demand-level write path (the only one that actually sticks)
# ---------------------------------------------------------------------------
# PUT /v1/publishers/{id} with nested minBidFloor returns 200 OK but silently
# drops the change — see core.ll_mgmt.set_demand_floor docstring. We write at
# the demand level and verify the live value before reporting success.

def _apply_floor_via_demand(
    *,
    publisher_name: str,
    demand_name: str,
    new_floor: float,
    reason: str,
) -> dict | None:
    """Write new_floor via set_demand_floor(verify=True). On a confirmed live
    change, post the Slack alert and append to floor_ledger. Returns the
    ll_mgmt result dict on success, or None if the write was skipped/failed.
    """
    if demand_name is None:
        return None

    publisher = ll_mgmt.get_publisher_by_name(publisher_name)
    if publisher is None:
        print(f"[floor_optimizer] publisher not found: {publisher_name!r}")
        return None
    pub_id      = publisher["id"]
    pub_display = publisher.get("name", publisher_name)
    sup_id      = (publisher.get("supplier")
                   or publisher.get("supplier_id")
                   or publisher.get("supplierId"))
    if sup_id not in PILOT_SUPPLIER_IDS:
        print(f"[floor_optimizer] {pub_display} supplier_id={sup_id} not in pilot — skip")
        return None

    demand = ll_mgmt.get_demand_by_name(demand_name)
    if demand is None:
        print(f"[floor_optimizer] demand not found: {demand_name!r}")
        return None
    demand_id      = demand["id"]
    demand_display = demand.get("name", demand_name)

    if demand_id in _SEEN_DEMAND_IDS_THIS_RUN:
        return None
    _SEEN_DEMAND_IDS_THIS_RUN.add(demand_id)

    if new_floor < MIN_FLOOR:
        print(f"[floor_optimizer] new_floor=${new_floor:.4f} < MIN_FLOOR=${MIN_FLOOR:.2f} — skip")
        return None

    try:
        result = ll_mgmt.set_demand_floor(
            demand_id,
            new_floor,
            verify=True,
            allow_multi_pub=True,
        )
    except Exception as exc:
        print(f"[floor_optimizer] set_demand_floor FAILED "
              f"{pub_display}/{demand_display}: {exc}")
        return None

    if result.get("dry_run"):
        return result
    if result.get("no_change"):
        return result

    # Verified live. Record ledger + post Slack.
    old_floor = result.get("old_floor") or 0.0
    # set_demand_floor may have clamped up (e.g. 9 Dots contract); use the
    # final landed value from the ledger/Slack perspective.
    landed_floor = result.get("new_floor", new_floor)

    floor_ledger.record(
        publisher_id=pub_id,
        publisher_name=pub_display,
        demand_id=demand_id,
        demand_name=demand_display,
        old_floor=old_floor,
        new_floor=landed_floor,
        actor="floor_optimizer",
        reason=reason,
    )

    change_pct = (abs((landed_floor - old_floor) / old_floor) * 100.0) if old_floor else 0.0
    direction  = "−" if landed_floor < old_floor else "+"
    text = (
        f":white_check_mark: *Pilot Action Applied*\n"
        f"Publisher: {pub_display} (supplier {sup_id})\n"
        f"Demand: {demand_display}\n"
        f"Change: Floor ${old_floor:.2f} → ${landed_floor:.2f}  "
        f"({direction}{change_pct:.0f}%)\n"
        f"Reason: {reason}\n"
        f"Time: {_now_et_str()}\n"
        f"Mode: LIVE"
    )
    blocks = [{"type": "section", "text": {"type": "mrkdwn", "text": text}}]
    slack.send_blocks(blocks, text=f"Pilot floor change: {pub_display} / {demand_display}")

    return result


# ---------------------------------------------------------------------------
# Daily reset
# ---------------------------------------------------------------------------

def _reset_to_base_floors(state: dict) -> list[dict]:
    """
    Restore every position to its base floor and clear daily drift.
    Called at RESET_HOUR_ET (08:00 ET) each day.
    """
    moves = []
    for g in PILOT_GROUPS:
        key       = _position_key(g["publisher"], g["group"])
        pos       = state["positions"].get(key, {})
        cur_floor = pos.get("current_floor", g["base_floor"])
        base      = g["base_floor"]

        if abs(cur_floor - base) < 0.001:
            continue  # already at base

        # Restore to base for each seat (demand-level write)
        for seat in g["seats"]:
            _apply_floor_via_demand(
                publisher_name=g["publisher"],
                demand_name=seat,
                new_floor=base,
                reason=f"Daily reset to base floor at {RESET_HOUR_ET}:00 ET",
            )

        state["positions"][key] = {
            "base_floor":           base,
            "current_floor":        base,
            "cumulative_change_pct": 0.0,
            "last_adjusted":        _now_et_str(),
        }
        moves.append({
            "publisher": g["publisher"],
            "group":     g["group"],
            "action":    "reset",
            "from":      cur_floor,
            "to":        base,
        })
        print(f"[floor_optimizer] RESET {g['publisher']} / {g['group']}  ${cur_floor:.3f} → ${base:.3f}")

    return moves


# ---------------------------------------------------------------------------
# Per-group optimisation step
# ---------------------------------------------------------------------------

def _optimise_group(g: dict, state: dict, today_stats: dict, yest_stats: dict) -> dict | None:
    """
    Decide whether to raise, lower, or hold the floor for a group.
    Returns a move dict if a change is made, else None.
    """
    key = _position_key(g["publisher"], g["group"])
    pos = state["positions"].setdefault(key, {
        "base_floor":           g["base_floor"],
        "current_floor":        g["base_floor"],
        "cumulative_change_pct": 0.0,
        "last_adjusted":        None,
    })

    cur_floor  = pos.get("current_floor",         g["base_floor"])
    base_floor = pos.get("base_floor",             g["base_floor"])
    cum_pct    = pos.get("cumulative_change_pct",  0.0)

    if not today_stats or not yest_stats:
        print(f"[floor_optimizer] {g['publisher']} / {g['group']} — no stats, skip")
        return None

    today_wr  = today_stats.get("win_rate", 0.0)
    yest_wr   = yest_stats.get("win_rate", 0.0)
    today_ecpm = today_stats.get("ecpm", 0.0)
    yest_ecpm  = yest_stats.get("ecpm", 0.0)

    if yest_wr < 0.0001:
        print(f"[floor_optimizer] {g['publisher']} / {g['group']} — yesterday win_rate ~0, skip")
        return None

    wr_ratio   = today_wr  / yest_wr
    ecpm_ratio = (today_ecpm / yest_ecpm) if yest_ecpm > 0.0001 else 1.0

    direction = "hold"
    if wr_ratio < WIN_RATE_LOOSEN:
        direction = "loosen"
    elif wr_ratio > WIN_RATE_TIGHTEN and ecpm_ratio >= 1.0:
        direction = "tighten"

    if direction == "hold":
        print(
            f"[floor_optimizer] {g['publisher']} / {g['group']}  "
            f"wr_ratio={wr_ratio:.2f}  ecpm_ratio={ecpm_ratio:.2f}  → hold"
        )
        return None

    # Compute new floor
    step        = STEP_PCT / 100.0
    new_floor   = cur_floor * (1 - step) if direction == "loosen" else cur_floor * (1 + step)
    new_floor   = max(MIN_FLOOR, min(MAX_FLOOR, round(new_floor, 4)))

    # Check cumulative daily drift cap
    new_cum_pct = (new_floor - base_floor) / base_floor * 100.0 if base_floor > 0 else 0.0
    if abs(new_cum_pct) > MAX_DAILY_DRIFT_PCT:
        print(
            f"[floor_optimizer] {g['publisher']} / {g['group']}  "
            f"daily drift cap reached ({new_cum_pct:.1f}% vs ±{MAX_DAILY_DRIFT_PCT}%) — hold"
        )
        return None

    if abs(new_floor - cur_floor) < 0.001:
        return None  # no meaningful change

    # Apply to all seats via the demand-level write path. Every successful
    # write is verified live, ledger-logged, and Slack-announced inside the
    # helper — we only update local state if at least one seat landed.
    reason_text = (
        f"Auto-optimizer: win_rate {wr_ratio:.2f}x yesterday, "
        f"eCPM {ecpm_ratio:.2f}x → {direction}"
    )
    landed_any = False
    for seat in g["seats"]:
        result = _apply_floor_via_demand(
            publisher_name=g["publisher"],
            demand_name=seat,
            new_floor=new_floor,
            reason=reason_text,
        )
        if result and (result.get("verified") or result.get("dry_run") or result.get("no_change")):
            landed_any = True

    if not landed_any:
        return None  # nothing landed — don't update state or summary

    pos["current_floor"]        = new_floor
    pos["cumulative_change_pct"] = new_cum_pct
    pos["last_adjusted"]         = _now_et_str()

    print(
        f"[floor_optimizer] {direction.upper()}  {g['publisher']} / {g['group']}  "
        f"${cur_floor:.3f} → ${new_floor:.3f}  "
        f"(wr {wr_ratio:.2f}x  ecpm {ecpm_ratio:.2f}x  cum {new_cum_pct:+.1f}%)"
    )

    return {
        "publisher": g["publisher"],
        "group":     g["group"],
        "direction": direction,
        "from":      cur_floor,
        "to":        new_floor,
        "wr_ratio":  round(wr_ratio, 3),
        "ecpm_ratio": round(ecpm_ratio, 3),
        "cum_pct":   round(new_cum_pct, 1),
    }


# ---------------------------------------------------------------------------
# Slack summary
# ---------------------------------------------------------------------------

def _slack_summary(moves: list[dict], resets: list[dict]):
    if not moves and not resets:
        return

    lines = [f":robot_face: *Floor Optimizer — {_now_et_str()}*", ""]

    if resets:
        lines.append("*Daily resets (08:00 ET):*")
        for r in resets:
            lines.append(f"  • {r['publisher']} / {r['group']}  ${r['from']:.3f} → ${r['to']:.3f} (base)")
        lines.append("")

    if moves:
        lines.append("*Hourly adjustments:*")
        for m in moves:
            arrow = ":arrow_down_small:" if m["direction"] == "loosen" else ":arrow_up_small:"
            lines.append(
                f"  {arrow} {m['publisher']} / {m['group']}  "
                f"${m['from']:.3f} → ${m['to']:.3f}  "
                f"(wr {m['wr_ratio']:.2f}x  eCPM {m['ecpm_ratio']:.2f}x  "
                f"drift {m['cum_pct']:+.1f}%)"
            )

    blocks = [{"type": "section", "text": {"type": "mrkdwn", "text": "\n".join(lines)}}]
    slack.send_blocks(blocks, text="Floor optimizer update")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def run():
    """Called by the scheduler every 2 hours."""
    print(f"[floor_optimizer] Starting run  {_now_et_str()}")
    _SEEN_DEMAND_IDS_THIS_RUN.clear()
    state  = _load_state()
    today  = _today_str()
    et_hr  = _et_hour()

    # ── Daily state reset at start of new day ──────────────────────────────
    if state.get("date") != today:
        print(f"[floor_optimizer] New day — resetting state for {today}")
        state["date"]      = today
        state["positions"] = {}

    # ── 08:00 ET: restore base floors ──────────────────────────────────────
    resets = []
    if et_hr == RESET_HOUR_ET:
        resets = _reset_to_base_floors(state)
        _save_state(state)
        if resets:
            _slack_summary([], resets)
        print(f"[floor_optimizer] Reset complete ({len(resets)} groups).")

    # ── Fetch stats for today and yesterday ────────────────────────────────
    # Cache per-publisher to avoid duplicate API calls
    today_cache: dict[str, dict] = {}
    yest_cache:  dict[str, dict] = {}

    def _get_stats(publisher: str, date: str, cache: dict) -> dict:
        if publisher not in cache:
            cache[publisher] = _fetch_publisher_stats(publisher, date)
        return cache[publisher]

    # ── Optimise each group ────────────────────────────────────────────────
    moves = []
    for g in PILOT_GROUPS:
        pub          = g["publisher"]
        today_stats  = _get_stats(pub, today,             today_cache)
        yest_stats   = _get_stats(pub, _yesterday_str(),  yest_cache)
        move         = _optimise_group(g, state, today_stats, yest_stats)
        if move:
            moves.append(move)

    _save_state(state)

    if moves:
        _slack_summary(moves, [])
        print(f"[floor_optimizer] {len(moves)} adjustment(s) applied.")
    else:
        print("[floor_optimizer] No adjustments needed.")


if __name__ == "__main__":
    run()
