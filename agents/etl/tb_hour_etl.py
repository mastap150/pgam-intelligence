"""
agents/etl/tb_hour_etl.py

TB hour-of-day rollups for the daypart heatmap on the executive
dashboard and the hour panel on Geo Intelligence.

Two breakdowns:
  HOUR              → pgam_direct.tb_daily_hour            (24 rows/day)
  HOUR,COUNTRY_NAME → pgam_direct.tb_daily_country_hour    (~1.5K rows/day)

TB exposes hour-of-day via day_group=hour, not as an attribute
(probed 2026-05-12). The 'date' field comes back as
"YYYY-MM-DD HH:00:00" — we split into report_date + hour at parse.

Window: trailing 2 days. Backfill via
`python -m agents.etl.tb_hour_etl --backfill 30`.
"""

from __future__ import annotations

import argparse
import sys
import time
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Iterable

from core.tb_api import fetch_tb
from core.api import sf, n_days_ago, today
from core.neon import connect

WINDOW_DAYS = 2
METRICS = ["GROSS_REVENUE", "PUB_PAYOUT", "IMPRESSIONS"]


def _date_range(start: str, end: str) -> list[str]:
    s = datetime.strptime(start, "%Y-%m-%d").date()
    e = datetime.strptime(end, "%Y-%m-%d").date()
    out: list[str] = []
    cur = s
    while cur <= e:
        out.append(cur.isoformat())
        cur += timedelta(days=1)
    return out


def _fetch_with_retry(breakdown: str, day: str, attempts: int = 3) -> list[dict]:
    last: Exception | None = None
    for i in range(attempts):
        try:
            return fetch_tb(breakdown, METRICS, day, day)
        except Exception as exc:
            last = exc
            wait = 6 * (i + 1)
            print(f"[tb_hour_etl] {breakdown} {day} retry {i+1}/{attempts} in {wait}s: {exc}", flush=True)
            time.sleep(wait)
    raise last if last else RuntimeError("fetch failed")


def _split_timestamp(ts: str) -> tuple[str, int] | None:
    """TB returns 'YYYY-MM-DD HH:00:00' for day_group=hour."""
    if not ts or len(ts) < 13:
        return None
    try:
        return ts[:10], int(ts[11:13])
    except (ValueError, IndexError):
        return None


def _normalize_hour(rows: Iterable[dict]) -> list[dict]:
    grouped: dict[tuple, dict] = defaultdict(lambda: {
        "impressions": 0.0, "ssp_revenue": 0.0, "profit": 0.0,
    })
    for row in rows:
        ts = str(row.get("DATE") or row.get("date") or "")
        parts = _split_timestamp(ts)
        if not parts:
            continue
        d, hour = parts
        gross = sf(row.get("GROSS_REVENUE"))
        imps  = sf(row.get("IMPRESSIONS"))
        if gross <= 0 and imps <= 0:
            continue
        key = (d, hour)
        agg = grouped[key]
        agg["impressions"] += imps
        agg["ssp_revenue"] += sf(row.get("PUB_PAYOUT"))
        agg["profit"]      += sf(row.get("PROFIT") or (gross - sf(row.get("PUB_PAYOUT"))))
    return [
        {
            "report_date": k[0], "hour": k[1],
            "impressions": int(v["impressions"]),
            "ssp_revenue": round(v["ssp_revenue"], 4),
            "profit": round(v["profit"], 4),
        }
        for k, v in grouped.items()
    ]


def _normalize_country_hour(rows: Iterable[dict]) -> list[dict]:
    grouped: dict[tuple, dict] = defaultdict(lambda: {
        "impressions": 0.0, "ssp_revenue": 0.0, "profit": 0.0,
    })
    for row in rows:
        ts = str(row.get("DATE") or row.get("date") or "")
        parts = _split_timestamp(ts)
        if not parts:
            continue
        d, hour = parts
        country = (str(row.get("COUNTRY_NAME") or row.get("country") or "") or "").upper()
        if not country:
            continue
        gross = sf(row.get("GROSS_REVENUE"))
        imps  = sf(row.get("IMPRESSIONS"))
        if gross <= 0 and imps <= 0:
            continue
        key = (d, country, hour)
        agg = grouped[key]
        agg["impressions"] += imps
        agg["ssp_revenue"] += sf(row.get("PUB_PAYOUT"))
        agg["profit"]      += sf(row.get("PROFIT") or (gross - sf(row.get("PUB_PAYOUT"))))
    return [
        {
            "report_date": k[0], "country": k[1], "hour": k[2],
            "impressions": int(v["impressions"]),
            "ssp_revenue": round(v["ssp_revenue"], 4),
            "profit": round(v["profit"], 4),
        }
        for k, v in grouped.items()
    ]


_HOUR_UPSERT = """
INSERT INTO pgam_direct.tb_daily_hour
    (report_date, hour, impressions, ssp_revenue, profit, updated_at)
VALUES (%(report_date)s, %(hour)s, %(impressions)s, %(ssp_revenue)s, %(profit)s, now())
ON CONFLICT (report_date, hour) DO UPDATE
   SET impressions = EXCLUDED.impressions,
       ssp_revenue = EXCLUDED.ssp_revenue,
       profit      = EXCLUDED.profit,
       updated_at  = now()
"""

_COUNTRY_HOUR_UPSERT = """
INSERT INTO pgam_direct.tb_daily_country_hour
    (report_date, country, hour, impressions, ssp_revenue, profit, updated_at)
VALUES (%(report_date)s, %(country)s, %(hour)s, %(impressions)s,
        %(ssp_revenue)s, %(profit)s, now())
ON CONFLICT (report_date, country, hour) DO UPDATE
   SET impressions = EXCLUDED.impressions,
       ssp_revenue = EXCLUDED.ssp_revenue,
       profit      = EXCLUDED.profit,
       updated_at  = now()
"""


def _upsert(sql: str, records: list[dict]) -> int:
    if not records:
        return 0
    with connect() as conn:
        with conn.cursor() as cur:
            cur.executemany(sql, records)
        conn.commit()
    return len(records)


def run(window_days: int = WINDOW_DAYS) -> dict:
    end_date = today()
    start_date = n_days_ago(max(window_days - 1, 0))
    print(f"[tb_hour_etl] {start_date}..{end_date} ({window_days}d)", flush=True)

    days = _date_range(start_date, end_date)
    hour_records: list[dict] = []
    country_hour_records: list[dict] = []
    skipped: list[str] = []

    for d in days:
        for breakdown, normaliser, bucket, label in [
            ("HOUR",              _normalize_hour,         hour_records,         "hour"),
            ("HOUR,COUNTRY_NAME", _normalize_country_hour, country_hour_records, "country×hour"),
        ]:
            try:
                rows = _fetch_with_retry(breakdown, d)
            except Exception as exc:
                print(f"[tb_hour_etl] {label} {d} SKIP: {exc}", flush=True)
                skipped.append(f"{label}:{d}")
                continue
            bucket.extend(normaliser(rows))
            print(f"[tb_hour_etl] {label} {d} -> {len(rows)} raw rows", flush=True)

    try:
        n_h  = _upsert(_HOUR_UPSERT, hour_records)
        n_ch = _upsert(_COUNTRY_HOUR_UPSERT, country_hour_records)
    except Exception as exc:
        return {"ok": False, "error": str(exc), "skipped": skipped}

    print(f"[tb_hour_etl] DONE — {n_h} hour + {n_ch} country×hour (skipped {len(skipped)})", flush=True)
    return {"ok": True, "hour": n_h, "country_hour": n_ch, "skipped": skipped}


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--backfill", type=int, default=None,
                        help="Override window (default: 2)")
    args = parser.parse_args()
    result = run(window_days=args.backfill or WINDOW_DAYS)
    sys.exit(0 if result.get("ok") else 1)
