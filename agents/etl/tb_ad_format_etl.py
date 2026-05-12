"""
agents/etl/tb_ad_format_etl.py

Pulls TB's ad_format breakdowns into three rollups feeding the Format
section on /admin/finance/geo-intelligence:

  pgam_direct.tb_daily_ad_format            DATE,AD_FORMAT
  pgam_direct.tb_daily_ad_format_country    DATE,AD_FORMAT,COUNTRY_NAME
  pgam_direct.tb_daily_ad_format_publisher  DATE,AD_FORMAT,PUBLISHER

Discovered 2026-05-12 — TB exposes `ad_format` as an attribute on
adx-report (banner / video / native / rewarded video). Combines
cleanly with country, ssp_name, and dsp_name.

Window: trailing 2 days. Backfill via
`python -m agents.etl.tb_ad_format_etl --backfill 30`.

Per-day chunking matches tb_segments_etl — TB times out on >1 day
windows for cross-attribute breakdowns. Three queries × N days ×
~10s TB rate-limit = backfill takes a few minutes for 30 days.
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
METRICS = ["GROSS_REVENUE", "PUB_PAYOUT", "IMPRESSIONS", "BIDS"]


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
            print(f"[tb_ad_format_etl] {breakdown} {day} retry {i+1}/{attempts} in {wait}s: {exc}", flush=True)
            time.sleep(wait)
    raise last if last else RuntimeError("fetch failed")


def _normalize_format(date: str, rows: Iterable[dict]) -> list[dict]:
    """Row schema: DATE,AD_FORMAT → one row per ad_format per day."""
    grouped: dict[str, dict] = defaultdict(lambda: {
        "impressions": 0.0, "bid_requests": 0.0,
        "ssp_revenue": 0.0, "profit": 0.0,
    })
    for row in rows:
        fmt = (str(row.get("AD_FORMAT") or "") or "unknown").lower()
        gross = sf(row.get("GROSS_REVENUE"))
        if gross <= 0 and sf(row.get("IMPRESSIONS")) <= 0:
            continue
        agg = grouped[fmt]
        agg["impressions"]  += sf(row.get("IMPRESSIONS"))
        agg["bid_requests"] += sf(row.get("BIDS"))
        agg["ssp_revenue"]  += sf(row.get("PUB_PAYOUT"))   # PUB_PAYOUT in LL units = ssp_revenue in TB units
        agg["profit"]       += sf(row.get("PROFIT") or (gross - sf(row.get("PUB_PAYOUT"))))
    return [
        {
            "report_date": date, "ad_format": fmt,
            "impressions": int(v["impressions"]),
            "bid_requests": int(v["bid_requests"]),
            "ssp_revenue": round(v["ssp_revenue"], 4),
            "profit": round(v["profit"], 4),
        }
        for fmt, v in grouped.items() if fmt
    ]


def _normalize_format_country(date: str, rows: Iterable[dict]) -> list[dict]:
    grouped: dict[tuple, dict] = defaultdict(lambda: {
        "impressions": 0.0, "ssp_revenue": 0.0, "profit": 0.0,
    })
    for row in rows:
        fmt = (str(row.get("AD_FORMAT") or "") or "unknown").lower()
        country = (str(row.get("COUNTRY_NAME") or "") or "").upper()
        if not fmt or not country:
            continue
        gross = sf(row.get("GROSS_REVENUE"))
        if gross <= 0 and sf(row.get("IMPRESSIONS")) <= 0:
            continue
        key = (fmt, country)
        agg = grouped[key]
        agg["impressions"] += sf(row.get("IMPRESSIONS"))
        agg["ssp_revenue"] += sf(row.get("PUB_PAYOUT"))
        agg["profit"]      += sf(row.get("PROFIT") or (gross - sf(row.get("PUB_PAYOUT"))))
    return [
        {
            "report_date": date, "ad_format": k[0], "country": k[1],
            "impressions": int(v["impressions"]),
            "ssp_revenue": round(v["ssp_revenue"], 4),
            "profit": round(v["profit"], 4),
        }
        for k, v in grouped.items()
    ]


def _normalize_format_publisher(date: str, rows: Iterable[dict]) -> list[dict]:
    grouped: dict[tuple, dict] = defaultdict(lambda: {
        "impressions": 0.0, "ssp_revenue": 0.0, "profit": 0.0,
    })
    for row in rows:
        fmt = (str(row.get("AD_FORMAT") or "") or "unknown").lower()
        pub = str(row.get("PUBLISHER_NAME") or row.get("ssp_name") or "").strip()
        if not fmt or not pub:
            continue
        gross = sf(row.get("GROSS_REVENUE"))
        if gross <= 0 and sf(row.get("IMPRESSIONS")) <= 0:
            continue
        key = (fmt, pub)
        agg = grouped[key]
        agg["impressions"] += sf(row.get("IMPRESSIONS"))
        agg["ssp_revenue"] += sf(row.get("PUB_PAYOUT"))
        agg["profit"]      += sf(row.get("PROFIT") or (gross - sf(row.get("PUB_PAYOUT"))))
    return [
        {
            "report_date": date, "ad_format": k[0], "publisher_name": k[1],
            "impressions": int(v["impressions"]),
            "ssp_revenue": round(v["ssp_revenue"], 4),
            "profit": round(v["profit"], 4),
        }
        for k, v in grouped.items()
    ]


_FORMAT_UPSERT = """
INSERT INTO pgam_direct.tb_daily_ad_format
    (report_date, ad_format, impressions, bid_requests, ssp_revenue, profit, updated_at)
VALUES (%(report_date)s, %(ad_format)s, %(impressions)s, %(bid_requests)s,
        %(ssp_revenue)s, %(profit)s, now())
ON CONFLICT (report_date, ad_format) DO UPDATE
   SET impressions  = EXCLUDED.impressions,
       bid_requests = EXCLUDED.bid_requests,
       ssp_revenue  = EXCLUDED.ssp_revenue,
       profit       = EXCLUDED.profit,
       updated_at   = now()
"""

_FORMAT_COUNTRY_UPSERT = """
INSERT INTO pgam_direct.tb_daily_ad_format_country
    (report_date, ad_format, country, impressions, ssp_revenue, profit, updated_at)
VALUES (%(report_date)s, %(ad_format)s, %(country)s,
        %(impressions)s, %(ssp_revenue)s, %(profit)s, now())
ON CONFLICT (report_date, ad_format, country) DO UPDATE
   SET impressions = EXCLUDED.impressions,
       ssp_revenue = EXCLUDED.ssp_revenue,
       profit      = EXCLUDED.profit,
       updated_at  = now()
"""

_FORMAT_PUB_UPSERT = """
INSERT INTO pgam_direct.tb_daily_ad_format_publisher
    (report_date, ad_format, publisher_name, impressions, ssp_revenue, profit, updated_at)
VALUES (%(report_date)s, %(ad_format)s, %(publisher_name)s,
        %(impressions)s, %(ssp_revenue)s, %(profit)s, now())
ON CONFLICT (report_date, ad_format, publisher_name) DO UPDATE
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
    print(f"[tb_ad_format_etl] {start_date}..{end_date} ({window_days}d)", flush=True)

    days = _date_range(start_date, end_date)
    fmt_records: list[dict] = []
    fmt_country_records: list[dict] = []
    fmt_pub_records: list[dict] = []
    skipped: list[str] = []

    for d in days:
        # Three calls per day. TB allows one concurrent query per user,
        # so they run sequentially via the lock in core/tb_api.py.
        for breakdown, normaliser, bucket, label in [
            ("DATE,AD_FORMAT",            _normalize_format,           fmt_records,         "format"),
            ("AD_FORMAT,COUNTRY_NAME",    _normalize_format_country,   fmt_country_records, "fmt×country"),
            ("AD_FORMAT,PUBLISHER",       _normalize_format_publisher, fmt_pub_records,     "fmt×pub"),
        ]:
            try:
                rows = _fetch_with_retry(breakdown, d)
            except Exception as exc:
                print(f"[tb_ad_format_etl] {label} {d} SKIP: {exc}", flush=True)
                skipped.append(f"{label}:{d}")
                continue
            bucket.extend(normaliser(d, rows))
            print(f"[tb_ad_format_etl] {label} {d} -> {len(rows)} raw rows", flush=True)

    try:
        n_f  = _upsert(_FORMAT_UPSERT, fmt_records)
        n_fc = _upsert(_FORMAT_COUNTRY_UPSERT, fmt_country_records)
        n_fp = _upsert(_FORMAT_PUB_UPSERT, fmt_pub_records)
    except Exception as exc:
        return {"ok": False, "error": str(exc), "skipped": skipped}

    print(f"[tb_ad_format_etl] DONE — {n_f} format + {n_fc} fmt×country + {n_fp} fmt×pub (skipped {len(skipped)})", flush=True)
    return {
        "ok": True,
        "format": n_f, "format_country": n_fc, "format_publisher": n_fp,
        "skipped": skipped,
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--backfill", type=int, default=None,
                        help="Override window (default: 2)")
    args = parser.parse_args()
    result = run(window_days=args.backfill or WINDOW_DAYS)
    sys.exit(0 if result.get("ok") else 1)
