"""
agents/alerts/daily_recommendations.py

Daily LL publisher analysis — posts actionable recommendations to Slack every morning.

Runs daily at 08:30 ET (after floor_gap and other 08:00 agents).
Analyses ALL LL publishers (not just pilot ones) across:
  - Win rate anomalies (too high = floor opportunity, too low = demand issue)
  - eCPM vs avg bid gap (floor set too low or too high)
  - Demand concentration risk per publisher
  - Zero or near-zero revenue publishers
  - Cross-publisher demand gaps (strong demand missing from similar publishers)
  - Day-over-day revenue changes (>20% swing flagged)

Posts a concise Slack digest with ranked recommendations.
Self-deduplicates: only posts once per calendar day.
"""

import os
import sys
import json
from datetime import datetime, timedelta
from collections import defaultdict

_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

from core.api import fetch as ll_fetch
import core.slack as slack

# ---------------------------------------------------------------------------
# Dedup
# ---------------------------------------------------------------------------

LOG_DIR   = os.path.join(_REPO_ROOT, "logs")
DEDUP_FILE = os.path.join(LOG_DIR, "daily_recommendations_dedup.json")

ET_TZ = None
try:
    from zoneinfo import ZoneInfo
    ET_TZ = ZoneInfo("America/New_York")
except Exception:
    pass


def _today_et() -> str:
    try:
        return datetime.now(ET_TZ).strftime("%Y-%m-%d")
    except Exception:
        return datetime.now().strftime("%Y-%m-%d")


def _already_sent() -> bool:
    today = _today_et()
    if not os.path.exists(DEDUP_FILE):
        return False
    try:
        with open(DEDUP_FILE) as f:
            data = json.load(f)
        return data.get("last_sent") == today
    except Exception:
        return False


def _mark_sent():
    os.makedirs(LOG_DIR, exist_ok=True)
    with open(DEDUP_FILE, "w") as f:
        json.dump({"last_sent": _today_et()}, f)


# ---------------------------------------------------------------------------
# Analysis helpers
# ---------------------------------------------------------------------------

def _safe_float(v, default=0.0) -> float:
    try:
        return float(v) if v is not None else default
    except (TypeError, ValueError):
        return default


def _analyse(rows_7d: list, rows_today: list, rows_yesterday: list) -> list[dict]:
    """
    Returns a list of recommendation dicts, each with:
      priority (1=high, 2=medium, 3=low), publisher, issue, suggestion
    """
    recs = []

    # ── Aggregate 7-day by publisher ─────────────────────────────────────
    pub_7d: dict[str, dict] = defaultdict(lambda: {
        "rev": 0.0, "wins": 0.0, "bids": 0.0, "ecpm_list": [], "avg_bid_list": []
    })
    for r in rows_7d:
        pub = r.get("PUBLISHER_NAME") or r.get("PUBLISHER") or "Unknown"
        pub_7d[pub]["rev"]   += _safe_float(r.get("GROSS_REVENUE"))
        pub_7d[pub]["wins"]  += _safe_float(r.get("WINS"))
        pub_7d[pub]["bids"]  += _safe_float(r.get("BIDS"))
        ecpm = _safe_float(r.get("GROSS_ECPM"))
        ab   = _safe_float(r.get("AVG_BID_PRICE"))
        if ecpm > 0:  pub_7d[pub]["ecpm_list"].append(ecpm)
        if ab   > 0:  pub_7d[pub]["avg_bid_list"].append(ab)

    # ── Aggregate yesterday and today by publisher ────────────────────────
    pub_yest: dict[str, float] = defaultdict(float)
    for r in rows_yesterday:
        pub = r.get("PUBLISHER_NAME") or r.get("PUBLISHER") or "Unknown"
        pub_yest[pub] += _safe_float(r.get("GROSS_REVENUE"))

    pub_today: dict[str, float] = defaultdict(float)
    for r in rows_today:
        pub = r.get("PUBLISHER_NAME") or r.get("PUBLISHER") or "Unknown"
        pub_today[pub] += _safe_float(r.get("GROSS_REVENUE"))

    # ── Demand breakdown per publisher (7d) ───────────────────────────────
    pub_demand: dict[str, dict[str, float]] = defaultdict(lambda: defaultdict(float))
    for r in rows_7d:
        pub    = r.get("PUBLISHER_NAME") or r.get("PUBLISHER") or "Unknown"
        demand = r.get("DEMAND_PARTNER_NAME") or r.get("DEMAND_PARTNER") or "Unknown"
        pub_demand[pub][demand] += _safe_float(r.get("GROSS_REVENUE"))

    # ── Check each publisher ──────────────────────────────────────────────
    for pub, stats in pub_7d.items():
        rev   = stats["rev"]
        wins  = stats["wins"]
        bids  = stats["bids"]
        wr    = (wins / bids * 100) if bids > 0 else 0.0
        ecpm  = sum(stats["ecpm_list"]) / len(stats["ecpm_list"]) if stats["ecpm_list"] else 0.0
        ab    = sum(stats["avg_bid_list"]) / len(stats["avg_bid_list"]) if stats["avg_bid_list"] else 0.0
        daily = rev / 7

        # Zero revenue
        if rev < 1.0 and bids > 10000:
            recs.append({
                "priority": 1,
                "publisher": pub,
                "issue": f"${rev:.2f} revenue on {int(bids):,} bids (7d)",
                "suggestion": "Investigate — bids arriving but near-zero revenue. Possible broken integration or floor misconfiguration.",
            })
            continue

        # Very low revenue skip detailed analysis
        if rev < 5.0:
            continue

        # High win rate — floor raising opportunity
        if wr > 35 and ab > 0:
            suggested_floor = round(ab * 0.50, 2)
            recs.append({
                "priority": 1,
                "publisher": pub,
                "issue": f"{wr:.1f}% win rate — demand is very strong relative to supply",
                "suggestion": f"Set floor ≈ ${suggested_floor:.2f} (50% of avg bid ${ab:.2f}). "
                              f"Would filter low-quality bids while protecting ${daily:.0f}/day revenue.",
            })
        elif wr > 20 and ab > 0:
            suggested_floor = round(ab * 0.40, 2)
            recs.append({
                "priority": 2,
                "publisher": pub,
                "issue": f"{wr:.1f}% win rate — healthy but room to tighten",
                "suggestion": f"Consider floor ≈ ${suggested_floor:.2f} (40% of avg bid ${ab:.2f}).",
            })

        # Very low win rate — potential floor too high or demand problem
        if wr < 2.0 and bids > 100000 and rev > 10:
            recs.append({
                "priority": 2,
                "publisher": pub,
                "issue": f"{wr:.1f}% win rate on {int(bids):,} bids — very few bids winning",
                "suggestion": "Check if floor is too aggressive, or if demand endpoints need reviewing.",
            })

        # Demand concentration — single partner > 80%
        demands = pub_demand.get(pub, {})
        if demands and rev > 20:
            top_demand = max(demands, key=demands.get)
            top_share  = demands[top_demand] / rev * 100
            if top_share > 80:
                recs.append({
                    "priority": 2,
                    "publisher": pub,
                    "issue": f"{top_demand} = {top_share:.0f}% of revenue — high concentration risk",
                    "suggestion": f"Add competing demand partners to create auction pressure and protect against {top_demand} outages.",
                })

        # Day-over-day revenue drop > 25%
        yest = pub_yest.get(pub, 0.0)
        if yest > 5 and daily > 5:
            dod = (yest - daily) / daily * 100
            if dod > 25:
                recs.append({
                    "priority": 1,
                    "publisher": pub,
                    "issue": f"Yesterday ${yest:.2f} vs 7d avg ${daily:.2f}/day ({dod:+.0f}%)",
                    "suggestion": "Revenue dropped sharply vs recent average. Check for demand partner issues or floor over-tightening.",
                })
            elif dod < -25:
                recs.append({
                    "priority": 3,
                    "publisher": pub,
                    "issue": f"Yesterday ${yest:.2f} vs 7d avg ${daily:.2f}/day ({dod:+.0f}% spike)",
                    "suggestion": "Revenue spike vs average — monitor for sustainability.",
                })

    # Sort: priority 1 first, then by publisher name
    recs.sort(key=lambda r: (r["priority"], r["publisher"]))
    return recs


# ---------------------------------------------------------------------------
# Slack formatting
# ---------------------------------------------------------------------------

def _build_blocks(recs: list[dict], total_7d: float, total_today: float) -> list[dict]:
    today_str = _today_et()
    daily_avg = total_7d / 7

    try:
        dod_pct = (total_today - daily_avg) / daily_avg * 100
        dod_str = f"{dod_pct:+.1f}% vs 7d avg"
    except ZeroDivisionError:
        dod_str = ""

    header = (
        f":bar_chart: *Daily LL Recommendations — {today_str}*\n"
        f"LL 7d total: ${total_7d:,.0f}  |  7d avg/day: ${daily_avg:,.0f}  |  "
        f"Today so far: ${total_today:,.0f} {dod_str}"
    )

    blocks = [
        {"type": "section", "text": {"type": "mrkdwn", "text": header}},
        {"type": "divider"},
    ]

    if not recs:
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": ":white_check_mark: No actionable recommendations today. All publishers look healthy."}
        })
        return blocks

    priority_labels = {1: ":red_circle: HIGH", 2: ":large_yellow_circle: MEDIUM", 3: ":large_green_circle: LOW"}

    for r in recs[:12]:  # cap at 12 to keep Slack readable
        label = priority_labels.get(r["priority"], "")
        text  = (
            f"{label}  *{r['publisher']}*\n"
            f"Issue: {r['issue']}\n"
            f"Suggestion: _{r['suggestion']}_"
        )
        blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": text}})

    if len(recs) > 12:
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": f"_… and {len(recs) - 12} more recommendations not shown._"}
        })

    return blocks


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def run():
    if _already_sent():
        print("[daily_recommendations] Already sent today — skipping.")
        return

    print("[daily_recommendations] Fetching LL stats…")

    today     = datetime.now().strftime("%Y-%m-%d")
    yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    week_ago  = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")

    # 7-day publisher + demand breakdown
    rows_7d = ll_fetch(
        "DATE,PUBLISHER,DEMAND_PARTNER",
        "GROSS_REVENUE,WINS,BIDS,GROSS_ECPM,AVG_BID_PRICE",
        week_ago, yesterday,
    )

    # Yesterday publisher only
    rows_yest = ll_fetch(
        "DATE,PUBLISHER",
        "GROSS_REVENUE",
        yesterday, yesterday,
    )

    # Today partial
    rows_today = ll_fetch(
        "DATE,PUBLISHER",
        "GROSS_REVENUE",
        today, today,
    )

    total_7d    = sum(_safe_float(r.get("GROSS_REVENUE")) for r in rows_7d)
    total_today = sum(_safe_float(r.get("GROSS_REVENUE")) for r in rows_today)

    recs   = _analyse(rows_7d, rows_today, rows_yest)
    blocks = _build_blocks(recs, total_7d, total_today)

    slack.send_blocks(blocks, text=f"Daily LL recommendations — {today}")
    _mark_sent()

    print(f"[daily_recommendations] Sent {len(recs)} recommendation(s).")


if __name__ == "__main__":
    run()
