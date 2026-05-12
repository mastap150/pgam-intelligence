"""
agents/etl/ll_geo_segments_etl.py

Geo Intelligence ETL — LL-only country × {device_type, OS, hour}
cross-cuts that power /admin/finance/geo-intelligence.

Three breakdowns, all LL via stats.ortb.net:

  DATE,COUNTRY,DEVICE_TYPE  → ll_daily_country_device  (~850 rows/day)
  DATE,COUNTRY,OS           → ll_daily_country_os      (~1300 rows/day)
  DATE,COUNTRY,HOUR         → ll_daily_country_hour    (~5600 rows/day)

The hour one is the heaviest. All three return in <2s for a single
day on stats.ortb.net so no chunking is needed.

TB does NOT expose hour, state, or DMA via their adx-report API
(probed 2026-05-11). Geo Intelligence joins TB country totals (from
the existing tb_daily_publisher_country) at query time instead.

Window: trailing 2 days hourly. Backfill via
`python -m agents.etl.ll_geo_segments_etl --backfill 30`.
"""

import argparse
import sys
from collections import defaultdict
from typing import Iterable

from core.api import fetch, sf, n_days_ago, today
from core.neon import connect

WINDOW_DAYS = 2
METRICS = ["GROSS_REVENUE", "PUB_PAYOUT", "IMPRESSIONS"]


def _normalize_country_device(rows: Iterable[dict]) -> list[dict]:
    grouped: dict[tuple, dict] = defaultdict(lambda: {
        "impressions": 0.0, "gross_revenue": 0.0, "pub_payout": 0.0,
    })
    for row in rows:
        date = str(row.get("DATE") or "")
        country = (str(row.get("COUNTRY") or "") or "").upper()
        dt = (str(row.get("DEVICE_TYPE") or "") or "UNKNOWN").upper()
        if not date or not country:
            continue
        gross = sf(row.get("GROSS_REVENUE"))
        imps  = sf(row.get("IMPRESSIONS"))
        if gross <= 0 and imps <= 0:
            continue
        key = (date, country, dt)
        agg = grouped[key]
        agg["impressions"]   += imps
        agg["gross_revenue"] += gross
        agg["pub_payout"]    += sf(row.get("PUB_PAYOUT"))
    return [
        {
            "report_date": k[0], "country": k[1], "device_type": k[2],
            "impressions": int(v["impressions"]),
            "gross_revenue": round(v["gross_revenue"], 4),
            "pub_payout": round(v["pub_payout"], 4),
        }
        for k, v in grouped.items()
    ]


def _normalize_country_os(rows: Iterable[dict]) -> list[dict]:
    grouped: dict[tuple, dict] = defaultdict(lambda: {
        "impressions": 0.0, "gross_revenue": 0.0, "pub_payout": 0.0,
    })
    for row in rows:
        date = str(row.get("DATE") or "")
        country = (str(row.get("COUNTRY") or "") or "").upper()
        osv = (str(row.get("OS") or "") or "unknown").lower()
        if not date or not country:
            continue
        gross = sf(row.get("GROSS_REVENUE"))
        imps  = sf(row.get("IMPRESSIONS"))
        if gross <= 0 and imps <= 0:
            continue
        key = (date, country, osv)
        agg = grouped[key]
        agg["impressions"]   += imps
        agg["gross_revenue"] += gross
        agg["pub_payout"]    += sf(row.get("PUB_PAYOUT"))
    return [
        {
            "report_date": k[0], "country": k[1], "os": k[2],
            "impressions": int(v["impressions"]),
            "gross_revenue": round(v["gross_revenue"], 4),
            "pub_payout": round(v["pub_payout"], 4),
        }
        for k, v in grouped.items()
    ]


def _normalize_country_hour(rows: Iterable[dict]) -> list[dict]:
    grouped: dict[tuple, dict] = defaultdict(lambda: {
        "impressions": 0.0, "gross_revenue": 0.0, "pub_payout": 0.0,
    })
    for row in rows:
        date = str(row.get("DATE") or "")
        country = (str(row.get("COUNTRY") or "") or "").upper()
        hour_raw = row.get("HOUR")
        if not date or not country or hour_raw is None:
            continue
        try:
            h = int(hour_raw)
        except (TypeError, ValueError):
            continue
        if h < 0 or h > 23:
            continue
        gross = sf(row.get("GROSS_REVENUE"))
        imps  = sf(row.get("IMPRESSIONS"))
        if gross <= 0 and imps <= 0:
            continue
        key = (date, country, h)
        agg = grouped[key]
        agg["impressions"]   += imps
        agg["gross_revenue"] += gross
        agg["pub_payout"]    += sf(row.get("PUB_PAYOUT"))
    return [
        {
            "report_date": k[0], "country": k[1], "hour": k[2],
            "impressions": int(v["impressions"]),
            "gross_revenue": round(v["gross_revenue"], 4),
            "pub_payout": round(v["pub_payout"], 4),
        }
        for k, v in grouped.items()
    ]


_COUNTRY_DEVICE_UPSERT = """
INSERT INTO pgam_direct.ll_daily_country_device
    (report_date, country, device_type, impressions, gross_revenue, pub_payout, updated_at)
VALUES (%(report_date)s, %(country)s, %(device_type)s,
        %(impressions)s, %(gross_revenue)s, %(pub_payout)s, now())
ON CONFLICT (report_date, country, device_type) DO UPDATE
   SET impressions   = EXCLUDED.impressions,
       gross_revenue = EXCLUDED.gross_revenue,
       pub_payout    = EXCLUDED.pub_payout,
       updated_at    = now()
"""

_COUNTRY_OS_UPSERT = """
INSERT INTO pgam_direct.ll_daily_country_os
    (report_date, country, os, impressions, gross_revenue, pub_payout, updated_at)
VALUES (%(report_date)s, %(country)s, %(os)s,
        %(impressions)s, %(gross_revenue)s, %(pub_payout)s, now())
ON CONFLICT (report_date, country, os) DO UPDATE
   SET impressions   = EXCLUDED.impressions,
       gross_revenue = EXCLUDED.gross_revenue,
       pub_payout    = EXCLUDED.pub_payout,
       updated_at    = now()
"""

_COUNTRY_HOUR_UPSERT = """
INSERT INTO pgam_direct.ll_daily_country_hour
    (report_date, country, hour, impressions, gross_revenue, pub_payout, updated_at)
VALUES (%(report_date)s, %(country)s, %(hour)s,
        %(impressions)s, %(gross_revenue)s, %(pub_payout)s, now())
ON CONFLICT (report_date, country, hour) DO UPDATE
   SET impressions   = EXCLUDED.impressions,
       gross_revenue = EXCLUDED.gross_revenue,
       pub_payout    = EXCLUDED.pub_payout,
       updated_at    = now()
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
    print(f"[ll_geo_segments_etl] {start_date}..{end_date} ({window_days}d)", flush=True)

    try:
        dev_rows = fetch("DATE,COUNTRY,DEVICE_TYPE", METRICS, start_date, end_date)
    except Exception as exc:
        return {"ok": False, "error": f"country_device: {exc}"}
    dev_records = _normalize_country_device(dev_rows)
    print(f"[ll_geo_segments_etl] country×device: {len(dev_rows)} -> {len(dev_records)} non-zero", flush=True)

    try:
        os_rows = fetch("DATE,COUNTRY,OS", METRICS, start_date, end_date)
    except Exception as exc:
        return {"ok": False, "error": f"country_os: {exc}"}
    os_records = _normalize_country_os(os_rows)
    print(f"[ll_geo_segments_etl] country×os: {len(os_rows)} -> {len(os_records)} non-zero", flush=True)

    try:
        hr_rows = fetch("DATE,COUNTRY,HOUR", METRICS, start_date, end_date)
    except Exception as exc:
        return {"ok": False, "error": f"country_hour: {exc}"}
    hr_records = _normalize_country_hour(hr_rows)
    print(f"[ll_geo_segments_etl] country×hour: {len(hr_rows)} -> {len(hr_records)} non-zero", flush=True)

    try:
        n_dev = _upsert(_COUNTRY_DEVICE_UPSERT, dev_records)
        n_os  = _upsert(_COUNTRY_OS_UPSERT, os_records)
        n_hr  = _upsert(_COUNTRY_HOUR_UPSERT, hr_records)
    except Exception as exc:
        return {"ok": False, "error": str(exc)}

    print(f"[ll_geo_segments_etl] DONE — {n_dev} country×device + {n_os} country×os + {n_hr} country×hour", flush=True)
    return {"ok": True, "country_device": n_dev, "country_os": n_os, "country_hour": n_hr}


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--backfill", type=int, default=None,
                        help="Override window: pull this many trailing days")
    args = parser.parse_args()
    result = run(window_days=args.backfill or WINDOW_DAYS)
    sys.exit(0 if result.get("ok") else 1)
