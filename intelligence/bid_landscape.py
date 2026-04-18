"""
Bid-landscape table — per-(publisher, demand, hour_of_week) observed funnel
under each floor regime it's been run at.

Joins the hourly collector store with the floor ledger:

    for each (publisher_id, demand_id, hour_of_week, floor_regime):
        bids, wins, revenue, impressions, win_rate, clear_ecpm, sample_hours

`floor_regime` is the floor value in effect during each hour, derived by
playing the ledger forward in time per (pub, demand). This lets Tranche 2
(bandit / portfolio optimizer) fit a price-response curve per tuple.

Also exposes:
    - `hourly_percentiles(pub, demand)` — eCPM p20/p40/p60/p80 by hour_of_week
      (for dayparting floor suggestions without running optimizer)
    - `summary(top_n=20)` — human-readable table of biggest buckets

Usage:
    python -m intelligence.bid_landscape                    # build + write
    python -m intelligence.bid_landscape --summary
    python -m intelligence.bid_landscape --percentiles 290115332 604
"""
from __future__ import annotations

import argparse
import gzip
import json
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from statistics import median
from typing import Iterable

from core import floor_ledger

DATA_DIR = Path(__file__).parent.parent / "data"
HOURLY_PATH = DATA_DIR / "hourly_pub_demand.json.gz"
LANDSCAPE_PATH = DATA_DIR / "bid_landscape.json.gz"
PERCENTILES_PATH = DATA_DIR / "hourly_percentiles.json.gz"


def _read_gz_json(path: Path) -> list[dict]:
    with gzip.open(path, "rt") as f:
        return json.load(f)


def _write_gz_json(path: Path, obj) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(path, "wt") as f:
        json.dump(obj, f)


def _hour_of_week(date_str: str, hour: int) -> int:
    """0..167  where 0 = Mon 00:00 UTC."""
    d = datetime.fromisoformat(str(date_str)).replace(tzinfo=timezone.utc)
    return d.weekday() * 24 + int(hour)


def _build_floor_timeline() -> dict[tuple[int, int], list[tuple[str, float | None]]]:
    """
    (publisher_id, demand_id) -> sorted [(ts_utc, new_floor), …]
    for applied, non-dry_run changes only.
    """
    timeline: dict[tuple[int, int], list[tuple[str, float | None]]] = defaultdict(list)
    for r in floor_ledger.read_all():
        if not r.get("applied") or r.get("dry_run"):
            continue
        timeline[(r["publisher_id"], r["demand_id"])].append(
            (r["ts_utc"], r["new_floor"])
        )
    for k in timeline:
        timeline[k].sort(key=lambda x: x[0])
    return timeline


def _floor_at(
    timeline: dict[tuple[int, int], list[tuple[str, float | None]]],
    pub_id: int,
    demand_id: int,
    when_iso: str,
) -> float | None | str:
    """Floor in effect for (pub, demand) at the given UTC timestamp.
    Returns 'unknown' if the ledger has no record before `when_iso`."""
    entries = timeline.get((pub_id, demand_id))
    if not entries:
        return "unknown"
    last = "unknown"
    for ts, val in entries:
        if ts <= when_iso:
            last = val
        else:
            break
    return last


def _floor_regime_key(floor: float | None | str) -> str:
    if floor == "unknown":
        return "unknown"
    if floor is None:
        return "null"
    return f"{float(floor):.2f}"


def build() -> dict:
    """Aggregate hourly rows × ledger → bucketed landscape stats."""
    hourly = _read_gz_json(HOURLY_PATH)
    timeline = _build_floor_timeline()

    # bucket: (pub_id, demand_id, hour_of_week, floor_regime) -> aggregates
    buckets: dict[tuple, dict] = {}
    for row in hourly:
        pub_id = int(row.get("PUBLISHER_ID", 0))
        demand_id = int(row.get("DEMAND_ID", 0))
        if pub_id == 0 or demand_id == 0:
            continue
        hod = int(row.get("HOUR", 0))
        date_str = str(row.get("DATE", ""))
        if not date_str:
            continue
        how = _hour_of_week(date_str, hod)
        # approximate hour timestamp (middle of hour)
        hour_iso = f"{date_str}T{hod:02d}:30:00Z"
        floor = _floor_at(timeline, pub_id, demand_id, hour_iso)
        regime = _floor_regime_key(floor)

        key = (pub_id, demand_id, how, regime)
        bucket = buckets.setdefault(key, {
            "publisher_id": pub_id,
            "publisher_name": row.get("PUBLISHER_NAME", ""),
            "demand_id": demand_id,
            "demand_name": row.get("DEMAND_NAME", ""),
            "hour_of_week": how,
            "floor_regime": regime,
            "bids": 0.0, "wins": 0.0, "impressions": 0.0,
            "revenue": 0.0, "pub_payout": 0.0,
            "sample_hours": 0,
            "hourly_ecpms": [],   # per-hour clear eCPM samples for percentiles
        })
        bids = float(row.get("BIDS", 0) or 0)
        wins = float(row.get("WINS", 0) or 0)
        imps = float(row.get("IMPRESSIONS", 0) or 0)
        rev = float(row.get("GROSS_REVENUE", 0) or 0)
        payout = float(row.get("PUB_PAYOUT", 0) or 0)

        bucket["bids"] += bids
        bucket["wins"] += wins
        bucket["impressions"] += imps
        bucket["revenue"] += rev
        bucket["pub_payout"] += payout
        bucket["sample_hours"] += 1
        if wins > 0:
            bucket["hourly_ecpms"].append(rev / wins * 1000.0)

    # finalize — add derived metrics
    out = []
    for b in buckets.values():
        bids = b["bids"]
        wins = b["wins"]
        b["win_rate"] = (wins / bids) if bids > 0 else 0.0
        b["clear_ecpm"] = (b["revenue"] / wins * 1000.0) if wins > 0 else 0.0
        b["rpm_per_bid"] = (b["revenue"] / bids * 1000.0) if bids > 0 else 0.0
        out.append(b)

    _write_gz_json(LANDSCAPE_PATH, out)
    return {
        "buckets": len(out),
        "distinct_tuples": len({(b["publisher_id"], b["demand_id"]) for b in out}),
        "regimes_seen": sorted({b["floor_regime"] for b in out}),
    }


# ────────────────────────────────────────────────────────────────────────────
# Hour-of-week percentiles — drives dayparting floor proposals
# ────────────────────────────────────────────────────────────────────────────

def _percentile(values: list[float], p: float) -> float:
    if not values:
        return 0.0
    s = sorted(values)
    k = (len(s) - 1) * p
    lo, hi = int(k), min(int(k) + 1, len(s) - 1)
    return s[lo] + (s[hi] - s[lo]) * (k - lo)


def build_percentiles() -> dict:
    """Per (pub, demand, hour_of_week), clear-eCPM p20/p40/p60/p80 across all
    regimes — used as the floor suggestion baseline (floor = p40).
    Independent of ledger to avoid circularity."""
    hourly = _read_gz_json(HOURLY_PATH)
    grouped: dict[tuple, list[float]] = defaultdict(list)
    meta: dict[tuple, dict] = {}
    for row in hourly:
        pub_id = int(row.get("PUBLISHER_ID", 0))
        demand_id = int(row.get("DEMAND_ID", 0))
        if pub_id == 0 or demand_id == 0:
            continue
        wins = float(row.get("WINS", 0) or 0)
        rev = float(row.get("GROSS_REVENUE", 0) or 0)
        if wins <= 0:
            continue
        ecpm = rev / wins * 1000.0
        how = _hour_of_week(str(row.get("DATE", "")), int(row.get("HOUR", 0)))
        key = (pub_id, demand_id, how)
        grouped[key].append(ecpm)
        meta.setdefault(key, {
            "publisher_id": pub_id,
            "publisher_name": row.get("PUBLISHER_NAME", ""),
            "demand_id": demand_id,
            "demand_name": row.get("DEMAND_NAME", ""),
            "hour_of_week": how,
        })

    out = []
    for key, samples in grouped.items():
        if len(samples) < 3:  # need min sample
            continue
        m = meta[key]
        m.update({
            "samples": len(samples),
            "p20": round(_percentile(samples, 0.20), 4),
            "p40": round(_percentile(samples, 0.40), 4),
            "p60": round(_percentile(samples, 0.60), 4),
            "p80": round(_percentile(samples, 0.80), 4),
            "median": round(median(samples), 4),
        })
        out.append(m)

    _write_gz_json(PERCENTILES_PATH, out)
    return {"rows": len(out), "tuples": len({(r["publisher_id"], r["demand_id"]) for r in out})}


# ────────────────────────────────────────────────────────────────────────────
# CLI
# ────────────────────────────────────────────────────────────────────────────

def run() -> dict:
    """Scheduler entry point: rebuild landscape + percentiles."""
    return {"landscape": build(), "percentiles": build_percentiles()}


def summary(top_n: int = 20) -> None:
    if not LANDSCAPE_PATH.exists():
        print("No landscape yet. Run: python -m intelligence.bid_landscape")
        return
    rows = _read_gz_json(LANDSCAPE_PATH)
    # collapse to (pub, demand) revenue totals
    totals: dict[tuple[int, int], dict] = {}
    for r in rows:
        k = (r["publisher_id"], r["demand_id"])
        t = totals.setdefault(k, {
            "publisher_name": r["publisher_name"], "demand_name": r["demand_name"],
            "bids": 0.0, "wins": 0.0, "revenue": 0.0, "regimes": set(),
        })
        t["bids"] += r["bids"]
        t["wins"] += r["wins"]
        t["revenue"] += r["revenue"]
        t["regimes"].add(r["floor_regime"])
    ranked = sorted(totals.items(), key=lambda kv: -kv[1]["revenue"])[:top_n]
    print(f"\n{'Pub':<32} {'Demand':<38} {'Bids':>10} {'WR%':>6} {'Rev':>8}  Regimes")
    print("-" * 120)
    for (pid, did), t in ranked:
        wr = (t["wins"] / t["bids"] * 100) if t["bids"] else 0
        print(f"{t['publisher_name'][:30]:<32} {t['demand_name'][:36]:<38} "
              f"{t['bids']:>10,.0f} {wr:>5.2f}% ${t['revenue']:>7,.0f}  "
              f"{','.join(sorted(t['regimes']))}")


def show_percentiles(pub_id: int, demand_id: int) -> None:
    if not PERCENTILES_PATH.exists():
        print("No percentiles yet. Run: python -m intelligence.bid_landscape")
        return
    rows = _read_gz_json(PERCENTILES_PATH)
    matches = [r for r in rows if r["publisher_id"] == pub_id and r["demand_id"] == demand_id]
    if not matches:
        print(f"No data for pub={pub_id} demand={demand_id}")
        return
    matches.sort(key=lambda r: r["hour_of_week"])
    name = f"{matches[0]['publisher_name']} / {matches[0]['demand_name']}"
    print(f"\n{name}")
    print(f"{'DOW':<4} {'Hr':>3} {'n':>4} {'p20':>7} {'p40':>7} {'p60':>7} {'p80':>7}")
    dows = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
    for r in matches:
        how = r["hour_of_week"]
        print(f"{dows[how // 24]:<4} {how % 24:>3} {r['samples']:>4} "
              f"{r['p20']:>7.3f} {r['p40']:>7.3f} {r['p60']:>7.3f} {r['p80']:>7.3f}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--summary", action="store_true")
    ap.add_argument("--percentiles", nargs=2, type=int, metavar=("PUB", "DEMAND"),
                    help="show hour-of-week percentile table for a tuple")
    args = ap.parse_args()

    if args.summary:
        summary()
        return
    if args.percentiles:
        show_percentiles(*args.percentiles)
        return

    state = build()
    pstate = build_percentiles()
    print(json.dumps({"landscape": state, "percentiles": pstate}, indent=2))


if __name__ == "__main__":
    main()
