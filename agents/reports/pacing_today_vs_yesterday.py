"""
agents/reports/pacing_today_vs_yesterday.py

Real-time supplier-level pacing comparison: today-so-far vs yesterday-at-same-hour
vs yesterday-full-day. Matches the supplier aggregation in the LL UI dashboard
so the user can see daily revenue trajectory without screenshotting.

Output is a Slack-formatted table per supplier showing:
  - Yesterday at this hour (UTC)
  - Today so far
  - Pace ratio (today / same-hour-yesterday)
  - Yesterday's final total
  - Today's linear projection (current rate × hours_remaining)
  - 🟢/⚪/🔴 status indicator

Runs every 2 hours during the day. Idempotent — same hour multiple times is fine.
"""
from __future__ import annotations

import gzip
import json
from collections import defaultdict
from datetime import datetime, timedelta, timezone, date
from pathlib import Path

from core import ll_mgmt, slack

DATA_DIR = Path(__file__).parent.parent.parent / "data"
HOURLY_PATH = DATA_DIR / "hourly_pub_demand.json.gz"

# Don't post for tiny suppliers — noise reduction
MIN_DAILY_GROSS = 50.0


def _load_supplier_map() -> dict[int, str]:
    """Fetch supplier_id → name. Returns {} on failure (safe degrade)."""
    try:
        sup = ll_mgmt._get("/v1/suppliers")
        if isinstance(sup, dict):
            sup = sup.get("items", [])
        return {s["id"]: s.get("name", f"<sup{s['id']}>") for s in sup}
    except Exception as e:
        print(f"[pacing] supplier fetch failed: {e}")
        return {}


def _load_pub_to_supplier() -> dict[int, int]:
    """Fetch publisher_id → supplier_id map. Cached in memory."""
    pubs = ll_mgmt.get_publishers(include_archived=False)
    out = {}
    for p in pubs:
        try:
            full = ll_mgmt.get_publisher(p["id"])
            sup_id = full.get("supplier")
            if sup_id is not None:
                out[p["id"]] = int(sup_id)
        except Exception:
            continue
    return out


def _aggregate(hourly: list[dict], pub_to_sup: dict[int, int],
              today_iso: str, yest_iso: str, current_hour: int) -> dict:
    """Returns nested dict: {supplier_id: {today_now, today_imp, yest_full, yest_now, yest_imp_full}}"""
    out: dict = defaultdict(lambda: {
        "today_gross": 0.0, "today_imp": 0, "today_payout": 0.0,
        "yest_full_gross": 0.0, "yest_full_imp": 0, "yest_full_payout": 0.0,
        "yest_now_gross": 0.0, "yest_now_imp": 0,
    })
    for r in hourly:
        pid = int(r.get("PUBLISHER_ID", 0) or 0)
        sup = pub_to_sup.get(pid)
        if sup is None:
            continue
        d = str(r.get("DATE", ""))
        h = int(r.get("HOUR", 0) or 0)
        g = float(r.get("GROSS_REVENUE", 0) or 0)
        p = float(r.get("PUB_PAYOUT", 0) or 0)
        i = int(r.get("IMPRESSIONS", 0) or 0)
        if d == today_iso:
            out[sup]["today_gross"] += g
            out[sup]["today_imp"] += i
            out[sup]["today_payout"] += p
        elif d == yest_iso:
            out[sup]["yest_full_gross"] += g
            out[sup]["yest_full_imp"] += i
            out[sup]["yest_full_payout"] += p
            if h <= current_hour:
                out[sup]["yest_now_gross"] += g
                out[sup]["yest_now_imp"] += i
    return out


def run() -> dict:
    now = datetime.now(timezone.utc)
    today_iso = now.date().isoformat()
    yest_iso = (now.date() - timedelta(days=1)).isoformat()
    current_hour = now.hour
    hours_remaining = 23 - current_hour

    if not HOURLY_PATH.exists():
        return {"skipped": True, "reason": "no hourly data"}
    with gzip.open(HOURLY_PATH, "rt") as f:
        hourly = json.load(f)

    sup_names = _load_supplier_map()
    pub_to_sup = _load_pub_to_supplier()

    agg = _aggregate(hourly, pub_to_sup, today_iso, yest_iso, current_hour)

    # Build per-supplier rows
    rows = []
    grand = {
        "today_gross": 0.0, "today_payout": 0.0,
        "yest_full_gross": 0.0, "yest_full_payout": 0.0,
        "yest_now_gross": 0.0,
    }
    for sup_id, a in agg.items():
        if a["yest_full_gross"] < MIN_DAILY_GROSS and a["today_gross"] < MIN_DAILY_GROSS:
            continue
        # Pace ratio: today / yesterday-at-same-hour
        pace = (a["today_gross"] / a["yest_now_gross"]) if a["yest_now_gross"] > 0.01 else None
        # Linear projection for rest-of-day
        if current_hour > 0:
            projected = a["today_gross"] * (24 / (current_hour + 1))
        else:
            projected = a["today_gross"] * 24
        # vs yesterday final
        vs_yest_pct = ((projected - a["yest_full_gross"]) / a["yest_full_gross"] * 100) if a["yest_full_gross"] else 0
        # Margin
        today_mgn = ((a["today_gross"] - a["today_payout"]) / a["today_gross"] * 100) if a["today_gross"] else 0
        yest_mgn = ((a["yest_full_gross"] - a["yest_full_payout"]) / a["yest_full_gross"] * 100) if a["yest_full_gross"] else 0

        rows.append({
            "sup_id": sup_id,
            "name": sup_names.get(sup_id, f"<sup{sup_id}>"),
            "today_gross": a["today_gross"],
            "yest_now_gross": a["yest_now_gross"],
            "yest_full_gross": a["yest_full_gross"],
            "pace": pace,
            "projected": projected,
            "vs_yest_pct": vs_yest_pct,
            "today_mgn": today_mgn,
            "yest_mgn": yest_mgn,
        })
        grand["today_gross"] += a["today_gross"]
        grand["today_payout"] += a["today_payout"]
        grand["yest_full_gross"] += a["yest_full_gross"]
        grand["yest_full_payout"] += a["yest_full_payout"]
        grand["yest_now_gross"] += a["yest_now_gross"]

    # Sort by yesterday-full size (descending) so the top revenue suppliers lead
    rows.sort(key=lambda r: -max(r["yest_full_gross"], r["today_gross"]))

    # Build Slack message
    grand_pace = (grand["today_gross"] / grand["yest_now_gross"]) if grand["yest_now_gross"] > 0.01 else None
    grand_projected = grand["today_gross"] * (24 / (current_hour + 1)) if current_hour >= 0 else grand["today_gross"]
    grand_vs_yest = ((grand_projected - grand["yest_full_gross"]) / grand["yest_full_gross"] * 100) if grand["yest_full_gross"] else 0
    grand_today_mgn = (grand["today_gross"] - grand["today_payout"]) / grand["today_gross"] * 100 if grand["today_gross"] else 0

    parts = [f":bar_chart: *Pacing — today vs yesterday ({now.strftime('%H:%M UTC')}, hour {current_hour+1}/24)*"]
    pace_str = f"{grand_pace:.2f}x" if grand_pace else "n/a"
    parts.append(f"*Portfolio: today ${grand['today_gross']:,.0f} @ hour {current_hour+1}*  |  yest_same_hour ${grand['yest_now_gross']:,.0f} ({pace_str})  |  yest_final ${grand['yest_full_gross']:,.0f}  |  projected ${grand_projected:,.0f} ({grand_vs_yest:+.0f}%)  |  today margin {grand_today_mgn:.1f}%")
    parts.append("```")
    parts.append(f"{'Supplier':<22} {'Today':>9} {'YestSame':>9} {'Pace':>5} {'YestFinal':>10} {'Proj':>10} {'vs Yest':>8} {'Mgn%':>5}")
    parts.append("-" * 86)
    for r in rows[:18]:
        pace_s = f"{r['pace']:.2f}x" if r['pace'] is not None else "  n/a"
        status = "🟢" if r['vs_yest_pct'] > 5 else ("🔴" if r['vs_yest_pct'] < -10 else "⚪")
        parts.append(f"{r['name'][:22]:<22} ${r['today_gross']:>7,.0f} ${r['yest_now_gross']:>7,.0f} {pace_s:>5} ${r['yest_full_gross']:>8,.0f} ${r['projected']:>8,.0f} {r['vs_yest_pct']:>+6.0f}% {status} {r['today_mgn']:>4.0f}%")
    parts.append("```")
    msg = "\n".join(parts)

    print(msg)
    try:
        slack.send_text(msg)
    except Exception as e:
        print(f"[pacing] Slack post failed: {e}")

    return {
        "ran_at": now.isoformat(),
        "supplier_count": len(rows),
        "today_gross": grand["today_gross"],
        "yest_full_gross": grand["yest_full_gross"],
        "portfolio_pace": grand_pace,
        "portfolio_projected": grand_projected,
        "portfolio_vs_yest_pct": grand_vs_yest,
    }


if __name__ == "__main__":
    print(json.dumps(run(), indent=2, default=str))
