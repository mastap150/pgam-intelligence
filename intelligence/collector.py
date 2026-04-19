"""
Hourly metrics collector — Tranche 1 of the ML optimizer.

Pulls the last 30d of (date, hour, publisher, demand) funnel metrics from the
LL POST reporting API (which exposes HOUR + DEMAND_ID, unlike the GET stats
endpoint) and persists a gzip-JSON rolling store. Also pulls a (date, publisher,
demand, country) daily snapshot for geo-aware models.

No automated actions. Instrumentation only.

Usage:
    python -m intelligence.collector              # pull + write
    python -m intelligence.collector --summary    # print summary of current store
"""
from __future__ import annotations

import argparse
import gzip
import json
import os
import tempfile
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from core.ll_report import report

DATA_DIR = Path(__file__).parent.parent / "data"
HOURLY_PATH = DATA_DIR / "hourly_pub_demand.json.gz"
DAILY_GEO_PATH = DATA_DIR / "daily_pub_demand_country.json.gz"
COLLECTOR_STATE = DATA_DIR / "collector_state.json"

HOURLY_DIMS = ["DATE", "HOUR", "PUBLISHER_ID", "PUBLISHER_NAME", "DEMAND_ID", "DEMAND_NAME"]
GEO_DIMS = ["DATE", "PUBLISHER_ID", "DEMAND_ID", "COUNTRY"]
METRICS = ["OPPORTUNITIES", "BIDS", "WINS", "IMPRESSIONS",
           "GROSS_REVENUE", "PUB_PAYOUT", "GROSS_ECPM"]

RETAIN_DAYS_HOURLY = 14
RETAIN_DAYS_GEO = 14
# Trimmed from 30d → 14d on 2026-04-18. The Render worker was being killed
# mid-run every hour. Chunking doesn't help because the LL POST /report
# endpoint ignores startDate/endDate and returns all-time data regardless
# (see core.ll_report module docstring). Smaller retention → smaller final
# filtered-and-written payload, which is the part we actually control.


def _today() -> date:
    return datetime.now(timezone.utc).date()


def _write_atomic_gz(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=str(path.parent), suffix=".tmp.gz")
    os.close(fd)
    with gzip.open(tmp, "wt") as f:
        json.dump(rows, f)
    os.replace(tmp, path)


def _read_gz(path: Path) -> list[dict]:
    if not path.exists():
        return []
    with gzip.open(path, "rt") as f:
        return json.load(f)


def _filter_recent(rows: list[dict], days: int) -> list[dict]:
    cutoff = (_today() - timedelta(days=days)).isoformat()
    return [r for r in rows if str(r.get("DATE", "")) >= cutoff]


def collect_hourly() -> dict:
    """Pull rolling {RETAIN_DAYS_HOURLY}d of hourly pub×demand funnel metrics.

    Progress is printed with flush=True so we can see where the process hangs
    or dies mid-run on Render — without this the whole thing is silent until
    completion."""
    end = _today().isoformat()
    start = (_today() - timedelta(days=RETAIN_DAYS_HOURLY)).isoformat()
    print(f"[collector] hourly: calling report() for {start}..{end}", flush=True)
    rows = report(HOURLY_DIMS, METRICS, start, end)
    print(f"[collector] hourly: got {len(rows):,} rows, filtering…", flush=True)
    rows = _filter_recent(rows, RETAIN_DAYS_HOURLY)
    rows = [r for r in rows if float(r.get("BIDS", 0)) > 0]
    print(f"[collector] hourly: writing {len(rows):,} rows to {HOURLY_PATH.name}", flush=True)
    _write_atomic_gz(HOURLY_PATH, rows)
    print(f"[collector] hourly: done", flush=True)
    return {
        "rows": len(rows),
        "distinct_publishers": len({r.get("PUBLISHER_ID") for r in rows}),
        "distinct_demands": len({r.get("DEMAND_ID") for r in rows}),
        "date_range": [
            min((r.get("DATE", "") for r in rows), default=""),
            max((r.get("DATE", "") for r in rows), default=""),
        ],
    }


def collect_geo() -> dict:
    """Pull rolling {RETAIN_DAYS_GEO}d of daily pub×demand×country funnel metrics."""
    end = _today().isoformat()
    start = (_today() - timedelta(days=RETAIN_DAYS_GEO)).isoformat()
    print(f"[collector] geo: calling report() for {start}..{end}", flush=True)
    rows = report(GEO_DIMS, METRICS, start, end)
    print(f"[collector] geo: got {len(rows):,} rows, filtering…", flush=True)
    rows = _filter_recent(rows, RETAIN_DAYS_GEO)
    rows = [r for r in rows if float(r.get("BIDS", 0)) > 0]
    print(f"[collector] geo: writing {len(rows):,} rows to {DAILY_GEO_PATH.name}", flush=True)
    _write_atomic_gz(DAILY_GEO_PATH, rows)
    print(f"[collector] geo: done", flush=True)
    return {
        "rows": len(rows),
        "distinct_countries": len({r.get("COUNTRY") for r in rows}),
        "date_range": [
            min((r.get("DATE", "") for r in rows), default=""),
            max((r.get("DATE", "") for r in rows), default=""),
        ],
    }


def run() -> dict:
    started = datetime.now(timezone.utc).isoformat()
    print(f"[collector] run() started at {started}", flush=True)
    hourly = collect_hourly()
    geo = collect_geo()
    state = {
        "last_run_utc": started,
        "hourly": hourly,
        "geo": geo,
    }
    COLLECTOR_STATE.parent.mkdir(parents=True, exist_ok=True)
    COLLECTOR_STATE.write_text(json.dumps(state, indent=2))
    print(f"[collector] run() complete: hourly_rows={hourly['rows']:,} "
          f"geo_rows={geo['rows']:,}", flush=True)
    return state


def summary() -> None:
    if not HOURLY_PATH.exists():
        print("No hourly store yet. Run: python -m intelligence.collector")
        return
    hourly = _read_gz(HOURLY_PATH)
    geo = _read_gz(DAILY_GEO_PATH) if DAILY_GEO_PATH.exists() else []
    print(f"Hourly rows:  {len(hourly):>8,}   size={HOURLY_PATH.stat().st_size/1024:.0f}kB")
    print(f"Geo rows:     {len(geo):>8,}   size={DAILY_GEO_PATH.stat().st_size/1024:.0f}kB"
          if geo else "Geo rows: (none)")
    if COLLECTOR_STATE.exists():
        print("\nLast run state:")
        print(COLLECTOR_STATE.read_text())


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--summary", action="store_true")
    args = ap.parse_args()
    if args.summary:
        summary()
        return
    state = run()
    print(json.dumps(state, indent=2))


if __name__ == "__main__":
    main()
