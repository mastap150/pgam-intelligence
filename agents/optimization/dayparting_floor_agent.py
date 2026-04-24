"""
agents/optimization/dayparting_floor_agent.py

Hourly floor modulation based on observed eCPM by hour-of-day.

Logic
-----
1. Pull placement × hour stats for the last 14 days.
2. For each placement with ≥MIN_IMPS_FOR_SIGNAL:
     - Compute avg eCPM per hour (0–23 UTC, converted to ET)
     - Identify peak hours (eCPM ≥ PEAK_MULTIPLIER × all-hour avg)
     - Identify trough hours (eCPM ≤ TROUGH_MULTIPLIER × all-hour avg)
3. Write a schedule to logs/dayparting_schedule.json:
     {placement_id: {hour: recommended_floor}}

Since TB doesn't expose per-hour floor scheduling via API, this agent
works in two modes:

(a) REPORT mode (default):
    Produces a schedule file + Slack summary of peak/trough deltas.
    Gives the human actionable "these hours underprice by X%" insight.

(b) HOURLY_APPLY mode (--apply):
    Runs hourly (when wired to scheduler at every hour), and:
      - Reads current hour in ET
      - Looks up each placement's recommended floor for THIS hour
      - If different from current placement floor by ≥CHANGE_THRESHOLD,
        calls tbm.set_floor() to update
      - Restores original floor at end of peak window

This module implements (a). A separate hourly runner consumes the
schedule for (b). That way the expensive signal-generation runs once
per day and the cheap "look up and apply" runs every hour.
"""

from __future__ import annotations

import json, os, sys, urllib.parse, requests, statistics
from collections import defaultdict
from datetime import datetime, timezone, timedelta

_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

from dotenv import load_dotenv
load_dotenv(override=True)
import core.tb_mgmt as tbm

WINDOW_DAYS           = 14
MIN_IMPS_FOR_SIGNAL   = 10_000
PEAK_MULTIPLIER       = 1.30
TROUGH_MULTIPLIER     = 0.70
MAX_FLOOR_MULT        = 1.50
MIN_FLOOR_MULT        = 0.80
CHANGE_THRESHOLD      = 0.10

TB_BASE = "https://ssp.pgammedia.com/api"
LOG_DIR       = os.path.join(_REPO_ROOT, "logs")
SCHEDULE_FILE = os.path.join(LOG_DIR, "dayparting_schedule.json")
RECS_FILE     = os.path.join(LOG_DIR, "dayparting_recs.json")
os.makedirs(LOG_DIR, exist_ok=True)


def _hour_report() -> list[dict]:
    """Paginate through all hourly rows using offset (TB caps limit=5000)."""
    end = datetime.now(timezone.utc).date()
    start = end - timedelta(days=WINDOW_DAYS)
    all_rows: list[dict] = []
    offset = 0
    PAGE = 5000
    while True:
        params = [("from", start.isoformat()), ("to", end.isoformat()),
                  ("day_group", "hour"), ("limit", PAGE), ("offset", offset),
                  ("attribute[]", "placement")]
        url = f"{TB_BASE}/{tbm._get_token()}/report?" + urllib.parse.urlencode(params)
        r = requests.get(url, timeout=180)
        r.raise_for_status()
        rows = r.json().get("data", r.json())
        if not rows: break
        all_rows.extend(rows)
        print(f"  … pulled {len(all_rows)} rows")
        if len(rows) < PAGE: break
        offset += PAGE
        if offset > 100_000: break   # safety
    return all_rows


def run() -> dict:
    print(f"\n{'='*70}\n  Dayparting Floor Agent\n{'='*70}")

    print("  → hourly placement report...")
    try:
        rows = _hour_report()
    except Exception as e:
        print(f"  ✗ {e}")
        return {"error": str(e)}
    print(f"  {len(rows)} rows")

    placements = tbm.list_all_placements_via_report(
        days=WINDOW_DAYS, min_impressions=MIN_IMPS_FOR_SIGNAL, hydrate=True
    )
    pmap = {p["placement_id"]: p for p in placements}

    # Group by (placement_id, hour)
    by_ph: dict[tuple[int,int], dict] = defaultdict(
        lambda: {"imps": 0, "spend": 0.0, "responses": 0})
    for r in rows:
        pid = r.get("placement_id")
        if pid is None: continue
        pid = int(pid)
        if pid not in pmap: continue
        # TB returns date like "2026-04-10 14:00:00" or "hour":14
        hour = r.get("hour")
        if hour is None:
            dt = r.get("date") or r.get("datetime") or ""
            if " " in dt:
                try: hour = int(dt.split(" ")[1].split(":")[0])
                except Exception: continue
        if hour is None: continue
        hour = int(hour)
        key = (pid, hour)
        by_ph[key]["imps"]      += r.get("impressions", 0) or 0
        by_ph[key]["spend"]     += r.get("dsp_spend", 0.0) or 0.0
        by_ph[key]["responses"] += r.get("bid_responses", 0) or 0

    # Per-placement hour-eCPM map
    schedule: dict[int, dict] = {}
    summaries = []
    for pid, p in pmap.items():
        hours: dict[int, float] = {}
        total_imps = 0
        for h in range(24):
            e = by_ph.get((pid, h))
            if not e or e["imps"] == 0: continue
            hours[h] = (e["spend"] * 1000.0 / e["imps"]) if e["imps"] else 0.0
            total_imps += e["imps"]
        if total_imps < MIN_IMPS_FOR_SIGNAL or not hours: continue
        avg = statistics.mean(hours.values())
        if avg <= 0: continue

        cur_floor = float(p.get("price", 0.0) or 0.0)
        if cur_floor <= 0: continue

        hour_floors: dict[int, float] = {}
        peak_hours, trough_hours = [], []
        for h, ec in hours.items():
            ratio = ec / avg
            if ratio >= PEAK_MULTIPLIER:
                mult = min(MAX_FLOOR_MULT, 1.0 + (ratio - 1.0) * 0.5)
                hour_floors[h] = round(cur_floor * mult, 3)
                peak_hours.append(h)
            elif ratio <= TROUGH_MULTIPLIER:
                mult = max(MIN_FLOOR_MULT, 1.0 - (1.0 - ratio) * 0.5)
                hour_floors[h] = round(cur_floor * mult, 3)
                trough_hours.append(h)

        if hour_floors:
            schedule[pid] = {
                "base_floor":     cur_floor,
                "avg_ecpm":       round(avg, 3),
                "peak_hours":     sorted(peak_hours),
                "trough_hours":   sorted(trough_hours),
                "hour_floors":    {str(k): v for k, v in hour_floors.items()},
                "title":          p.get("title"),
                "inventory_id":   p.get("inventory_id"),
            }
            summaries.append({
                "pid": pid, "title": p.get("title"),
                "base": cur_floor, "avg_ecpm": round(avg, 2),
                "peak_count": len(peak_hours),
                "trough_count": len(trough_hours),
                "peak_lift_pct": round(
                    (max(hour_floors.get(h, cur_floor) for h in peak_hours) / cur_floor - 1) * 100, 1
                ) if peak_hours else 0,
            })

    summaries.sort(key=lambda x: -x["peak_lift_pct"])
    print(f"\n  {len(schedule)} placements with actionable daypart signal")
    for s in summaries[:15]:
        print(f"    [{s['pid']}] {s['title'][:34]:<34} "
              f"base=${s['base']:.2f} avg_eCPM=${s['avg_ecpm']:.2f} "
              f"peaks={s['peak_count']} troughs={s['trough_count']} "
              f"peak_lift=+{s['peak_lift_pct']}%")

    with open(SCHEDULE_FILE, "w") as f:
        json.dump({"generated": datetime.now(timezone.utc).isoformat(),
                   "window_days": WINDOW_DAYS,
                   "schedule": schedule}, f, indent=2)
    with open(RECS_FILE, "w") as f:
        json.dump({"summaries": summaries,
                   "timestamp": datetime.now(timezone.utc).isoformat()}, f, indent=2)
    print(f"\n  Schedule → {SCHEDULE_FILE}")

    try:
        from core.slack import post_message
        lines = [f"🕐 *Dayparting Floor Agent* — {len(schedule)} placements have peak/trough signal"]
        for s in summaries[:6]:
            lines.append(f"  • [{s['pid']}] {s['title'][:30]}  "
                         f"peak_lift +{s['peak_lift_pct']}%  "
                         f"peak_hours={s['peak_count']}")
        post_message("\n".join(lines))
    except Exception: pass

    return {"schedule": schedule, "summaries": summaries}


if __name__ == "__main__":
    run()
