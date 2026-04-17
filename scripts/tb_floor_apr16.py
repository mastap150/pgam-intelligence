"""
scripts/tb_floor_apr16.py
~~~~~~~~~~~~~~~~~~~~~~~~~
TB Management API — first floor calibration (Apr 16 2026).

Context
-------
TB platform total revenue last 7 days (Apr 9-15): $7,590/week
Most top placements have floors set at $0.01–$0.05 which is well below actual
clearing prices. This script raises floors to ~50% of observed pub_eCPM to
filter bottom-quality bids without material volume loss.

Methodology
-----------
- pub_eCPM = publisher_revenue / wins * 1000  (from TB report, Apr 9-15)
- New floor = ~50-60% of pub_eCPM (conservative — well below clearing price)
- Only apply where gap > $0.03 (meaningful floor headroom)
- Skip placements where floor >= pub_eCPM (already at or above clearing)

Expected impact
---------------
- 3-8% eCPM improvement on affected placements
- Minimal volume loss (floors still well below avg bid)
- TB has is_optimal_price flag which can automate this further

Run with dry_run=True first (default), then set dry_run=False to apply.
"""

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import requests
import urllib.parse
from core.tb_api import get_token
import core.tb_mgmt as tbm

DRY_RUN = True   # ← change to False to apply

TB_BASE = "https://ssp.pgammedia.com/api"


def get_report(start_date: str, end_date: str, limit: int = 1000) -> list[dict]:
    """Pull all placement stats for the date range."""
    token = get_token()
    all_rows = []
    for page in range(1, 10):
        params = [
            ("from", start_date), ("to", end_date),
            ("day_group", "total"), ("limit", limit), ("page", page),
            ("attribute[]", "placement"), ("attribute[]", "ad_format"),
        ]
        url = f"{TB_BASE}/{token}/report?" + urllib.parse.urlencode(params)
        r = requests.get(url, timeout=60)
        r.raise_for_status()
        data = r.json()
        rows = data.get("data", data) if isinstance(data, dict) else data
        all_rows.extend(rows)
        if page >= (data.get("totalPages", 1) if isinstance(data, dict) else 1):
            break
    return all_rows


def compute_pub_ecpm(rows: list) -> dict:
    """Return {placement_id: {"pub_ecpm": float, "revenue": float, "wins": int, "bids": int}}."""
    from collections import defaultdict
    stats = defaultdict(lambda: {"revenue": 0.0, "wins": 0, "bids": 0})
    for row in rows:
        pid = row.get("placement") or row.get("placement_id")
        if not pid:
            continue
        stats[int(pid)]["revenue"] += float(row.get("publisher_revenue", 0) or 0)
        stats[int(pid)]["wins"]    += int(row.get("wins", 0) or 0)
        stats[int(pid)]["bids"]    += int(row.get("bid_requests", 0) or 0)
    for pid, d in stats.items():
        d["pub_ecpm"] = (d["revenue"] / d["wins"] * 1000) if d["wins"] > 0 else 0.0
        d["win_rate"] = (d["wins"] / d["bids"] * 100) if d["bids"] > 0 else 0.0
    return dict(stats)


def recommend_floor(current_floor: float, pub_ecpm: float) -> float | None:
    """
    Return a new floor if improvement is warranted, else None.

    Rules:
    - If pub_ecpm < current_floor → skip (already above clearing)
    - If gap (pub_ecpm - current_floor) < $0.02 → skip (insufficient headroom)
    - New floor = max(current_floor, pub_ecpm * 0.50)
    - Round to 2 decimal places
    - Cap at $1.00 (safety — never set excessive floors via automation)
    """
    if pub_ecpm <= 0:
        return None
    gap = pub_ecpm - current_floor
    if gap < 0.02:
        return None  # floor already at or above clearing, or too little headroom
    new_floor = round(min(pub_ecpm * 0.50, 1.00), 3)
    if new_floor <= current_floor:
        return None  # no improvement
    return new_floor


# ---------------------------------------------------------------------------
# Pre-computed floor recommendations based on Apr 9-15 data
# (placement_id → new_floor)
# Generated from: pub_ecpm * 0.50, floored at current + $0.02
# ---------------------------------------------------------------------------

FLOOR_CHANGES = {
    # Rule: only RAISE floors. New floor = pub_eCPM * 0.50, skipping if result < current floor.
    # Formula column: [current_floor → pub_eCPM → 50% target]

    # --- RoughMaps (inv 107) ---
    # rough_ros_mobile_300x250_3:    $0.03 → $0.091 → raise to $0.046
    1067: 0.046,
    # rough_ros_mobile_320x50_anchor:$0.01 → $0.088 → raise to $0.044
    1069: 0.044,
    # rough_ros_mobile_320x50_2:     $0.01 → $0.085 → raise to $0.042
    1068: 0.042,
    # rough_ros_desktop_728x90_1:    $0.03 → $0.122 → raise to $0.061
    1076: 0.061,
    # rough_ros_desktop_300x250_2:   $0.03 → $0.140 → raise to $0.070
    1071: 0.070,
    # NOTE: 1077 (rough_ros_desktop_728x90_2): floor=$0.10 > 50% of $0.171=$0.085 → skip

    # --- Modrinth (inv 544) ---
    # Modrinth 300x250: $0.01 → $0.053 → raise to $0.026
    3327: 0.026,
    # Modrinth Video:   $0.05 → $0.139 → raise to $0.069
    3328: 0.069,

    # --- booksandbao.com (inv 1802) ---
    # booksandbao.com_300x250:   $0.02 → $0.074 → raise to $0.037
    23587: 0.037,
    # booksandbao.com_320x50:    $0.02 → $0.103 → raise to $0.051
    23589: 0.051,

    # NOTE: GeeksForGeeks floors ($0.15) are already > 50% of their pub_eCPMs → skip
    # NOTE: outdoorrevival.com floors ($0.10) are > 50% of pub_eCPMs → skip

    # --- outdoorrevival.com (inv 955) secondary placement ---
    # or_secondary-P5: $0.02 → $0.246 → raise to $0.123
    7870: 0.123,

    # --- macworld.com (inv 1232) ---
    # macworld.com_300x600: $0.02 → $0.101 → raise to $0.050
    12294: 0.050,
    # macworld.com_250x600: $0.02 → $0.091 → raise to $0.045
    12295: 0.045,
    # macworld.com_300x250: $0.02 → $0.080 → raise to $0.040
    12291: 0.040,
    # macworld.com_160x600: $0.02 → $0.085 → raise to $0.042
    12296: 0.042,
    # macworld.com_120x600: $0.02 → $0.084 → raise to $0.042
    12297: 0.042,

    # --- 365scores.com (inv 855) ---
    # 365scores.com_300x50: $0.02 → $0.067 → raise to $0.033
    6787: 0.033,
    # NOTE: 6785 (365scores 320x50): floor=$0.05 > 50% of $0.081=$0.040 → skip
}

# ---------------------------------------------------------------------------
# Dexerto video placements — dexerto.com PreRoll had $0.12 floor (too low)
# dexerto.com is main EN site — video should have a floor of at least $1.00
# (matches charlieintel.com, dexerto.es, dexerto.fr video floors of $3.00)
# ---------------------------------------------------------------------------

DEXERTO_VIDEO_FLOORS = {
    # DEX_PreRollOMP_Desktop (inv=66, dexerto.com): currently $0.12 → $1.00
    434: 1.00,
    # DEX_PreRollOMP_Mobile (inv=66, dexerto.com): currently $0.12 → $1.00
    444: 1.00,
}


def run_calibration(dry_run: bool = True) -> None:
    print(f"\n{'='*65}")
    print(f"TB Floor Calibration — Apr 16 2026  {'[DRY RUN]' if dry_run else '[LIVE]'}")
    print(f"{'='*65}\n")

    token = get_token()
    print(f"Token: {token[:16]}...")

    total_applied = 0

    # 1. Main floor improvements
    print(f"\n--- Part 1: Floor improvements on top-revenue placements ({len(FLOOR_CHANGES)} placements) ---")
    for pid, new_floor in sorted(FLOOR_CHANGES.items()):
        try:
            r = tbm.set_floor(pid, price=new_floor, dry_run=dry_run)
            if r.get("applied") or dry_run:
                total_applied += 1
        except Exception as e:
            print(f"  ✗ placement_id={pid}  error: {e}")

    # 2. Dexerto video
    print(f"\n--- Part 2: Dexerto video floor fix ({len(DEXERTO_VIDEO_FLOORS)} placements) ---")
    for pid, new_floor in DEXERTO_VIDEO_FLOORS.items():
        try:
            r = tbm.set_floor(pid, price=new_floor, dry_run=dry_run)
            if r.get("applied") or dry_run:
                total_applied += 1
        except Exception as e:
            print(f"  ✗ placement_id={pid}  error: {e}")

    print(f"\n{'='*65}")
    print(f"{'DRY RUN' if dry_run else 'APPLIED'}: {total_applied} placement floors updated")
    if dry_run:
        print("Set DRY_RUN = False at top of script to apply changes.")
    print(f"{'='*65}")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--live", action="store_true", help="Apply changes (default: dry run)")
    args = parser.parse_args()
    run_calibration(dry_run=not args.live)
