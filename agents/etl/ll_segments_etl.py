"""
agents/etl/ll_segments_etl.py

Hourly ETL that lands four small LL "segment" rollups for the
Executive Dashboard:

  pgam_direct.ll_daily_device_os         Device & OS section
  pgam_direct.ll_daily_hour              Daypart heatmap
  pgam_direct.ll_daily_publisher_funnel  Funnel by supply brand
  pgam_direct.ll_daily_publisher_country Country → publisher drill-down

All breakdowns are tiny (~30-300 rows/day) and return in under a
second on a single multi-day window — no chunking needed.

Window: trailing 2 days hourly. Backfill via
`python -m agents.etl.ll_segments_etl --backfill 30`.
"""

import argparse
import sys
from collections import defaultdict
from typing import Iterable

from core.api import fetch, sf, n_days_ago, today
from core.neon import connect

WINDOW_DAYS = 2
DEVICE_OS_METRICS = ["GROSS_REVENUE", "PUB_PAYOUT", "IMPRESSIONS"]
HOUR_METRICS      = ["GROSS_REVENUE", "PUB_PAYOUT", "IMPRESSIONS"]
FUNNEL_METRICS    = ["OPPORTUNITIES", "BID_REQUESTS", "BIDS", "WINS", "IMPRESSIONS", "GROSS_REVENUE"]
PUB_COUNTRY_METRICS = ["GROSS_REVENUE", "PUB_PAYOUT", "IMPRESSIONS"]


def _normalize_device_os(rows: Iterable[dict]) -> list[dict]:
    grouped: dict[tuple, dict] = defaultdict(lambda: {
        "impressions": 0.0, "gross_revenue": 0.0, "pub_payout": 0.0,
    })
    for row in rows:
        date = str(row.get("DATE") or "")
        dt = (str(row.get("DEVICE_TYPE") or "") or "UNKNOWN").upper()
        osv = (str(row.get("OS") or "") or "unknown").lower()
        if not date or dt == "UNKNOWN" and osv == "unknown":
            continue
        gross = sf(row.get("GROSS_REVENUE"))
        if gross <= 0:
            continue
        key = (date, dt, osv)
        agg = grouped[key]
        agg["impressions"]   += sf(row.get("IMPRESSIONS"))
        agg["gross_revenue"] += gross
        agg["pub_payout"]    += sf(row.get("PUB_PAYOUT"))
    return [
        {"report_date": k[0], "device_type": k[1], "os": k[2],
         "impressions": int(v["impressions"]),
         "gross_revenue": round(v["gross_revenue"], 4),
         "pub_payout": round(v["pub_payout"], 4)}
        for k, v in grouped.items()
    ]


def _normalize_hour(rows: Iterable[dict]) -> list[dict]:
    out: list[dict] = []
    for row in rows:
        date = str(row.get("DATE") or "")
        hour = row.get("HOUR")
        if not date or hour is None:
            continue
        try:
            h = int(hour)
        except (TypeError, ValueError):
            continue
        if not (0 <= h <= 23):
            continue
        gross = sf(row.get("GROSS_REVENUE"))
        out.append({
            "report_date":   date,
            "hour":          h,
            "impressions":   int(sf(row.get("IMPRESSIONS"))),
            "gross_revenue": round(gross, 4),
            "pub_payout":    round(sf(row.get("PUB_PAYOUT")), 4),
        })
    return out


def _normalize_funnel(rows: Iterable[dict]) -> list[dict]:
    grouped: dict[tuple, dict] = defaultdict(lambda: {
        "opportunities": 0.0, "bid_requests": 0.0, "bids": 0.0,
        "wins": 0.0, "impressions": 0.0, "gross_revenue": 0.0,
    })
    meta: dict[tuple, str] = {}
    for row in rows:
        date = str(row.get("DATE") or "")
        pid = str(row.get("PUBLISHER_ID") or row.get("PUBLISHER") or "")
        pname = str(row.get("PUBLISHER_NAME") or row.get("PUBLISHER") or "") or pid
        if not (date and pid):
            continue
        key = (date, pid)
        agg = grouped[key]
        agg["opportunities"] += sf(row.get("OPPORTUNITIES"))
        agg["bid_requests"]  += sf(row.get("BID_REQUESTS"))
        agg["bids"]          += sf(row.get("BIDS"))
        agg["wins"]          += sf(row.get("WINS"))
        agg["impressions"]   += sf(row.get("IMPRESSIONS"))
        agg["gross_revenue"] += sf(row.get("GROSS_REVENUE"))
        meta[key] = pname
    return [
        {"report_date": k[0], "publisher_id": k[1], "publisher_name": meta[k],
         "opportunities": int(v["opportunities"]),
         "bid_requests":  int(v["bid_requests"]),
         "bids":          int(v["bids"]),
         "wins":          int(v["wins"]),
         "impressions":   int(v["impressions"]),
         "gross_revenue": round(v["gross_revenue"], 4)}
        for k, v in grouped.items()
    ]


_DEVICE_OS_UPSERT = """
INSERT INTO pgam_direct.ll_daily_device_os
  (report_date, device_type, os, impressions, gross_revenue, pub_payout, updated_at)
VALUES
  (%(report_date)s, %(device_type)s, %(os)s,
   %(impressions)s, %(gross_revenue)s, %(pub_payout)s, now())
ON CONFLICT (report_date, device_type, os) DO UPDATE SET
  impressions   = EXCLUDED.impressions,
  gross_revenue = EXCLUDED.gross_revenue,
  pub_payout    = EXCLUDED.pub_payout,
  updated_at    = now();
"""

_HOUR_UPSERT = """
INSERT INTO pgam_direct.ll_daily_hour
  (report_date, hour, impressions, gross_revenue, pub_payout, updated_at)
VALUES
  (%(report_date)s, %(hour)s, %(impressions)s,
   %(gross_revenue)s, %(pub_payout)s, now())
ON CONFLICT (report_date, hour) DO UPDATE SET
  impressions   = EXCLUDED.impressions,
  gross_revenue = EXCLUDED.gross_revenue,
  pub_payout    = EXCLUDED.pub_payout,
  updated_at    = now();
"""

_FUNNEL_UPSERT = """
INSERT INTO pgam_direct.ll_daily_publisher_funnel
  (report_date, publisher_id, publisher_name,
   opportunities, bid_requests, bids, wins, impressions,
   gross_revenue, updated_at)
VALUES
  (%(report_date)s, %(publisher_id)s, %(publisher_name)s,
   %(opportunities)s, %(bid_requests)s, %(bids)s, %(wins)s, %(impressions)s,
   %(gross_revenue)s, now())
ON CONFLICT (report_date, publisher_id) DO UPDATE SET
  publisher_name = EXCLUDED.publisher_name,
  opportunities  = EXCLUDED.opportunities,
  bid_requests   = EXCLUDED.bid_requests,
  bids           = EXCLUDED.bids,
  wins           = EXCLUDED.wins,
  impressions    = EXCLUDED.impressions,
  gross_revenue  = EXCLUDED.gross_revenue,
  updated_at     = now();
"""

_PUB_COUNTRY_UPSERT = """
INSERT INTO pgam_direct.ll_daily_publisher_country
  (report_date, publisher_id, publisher_name, country,
   impressions, gross_revenue, pub_payout, updated_at)
VALUES
  (%(report_date)s, %(publisher_id)s, %(publisher_name)s, %(country)s,
   %(impressions)s, %(gross_revenue)s, %(pub_payout)s, now())
ON CONFLICT (report_date, publisher_id, country) DO UPDATE SET
  publisher_name = EXCLUDED.publisher_name,
  impressions    = EXCLUDED.impressions,
  gross_revenue  = EXCLUDED.gross_revenue,
  pub_payout     = EXCLUDED.pub_payout,
  updated_at     = now();
"""


def _normalize_pub_country(rows: Iterable[dict]) -> list[dict]:
    grouped: dict[tuple, dict] = defaultdict(lambda: {
        "impressions": 0.0, "gross_revenue": 0.0, "pub_payout": 0.0,
    })
    meta: dict[tuple, str] = {}
    for row in rows:
        date = str(row.get("DATE") or "")
        pid = str(row.get("PUBLISHER_ID") or row.get("PUBLISHER") or "")
        pname = str(row.get("PUBLISHER_NAME") or row.get("PUBLISHER") or "") or pid
        country = str(row.get("COUNTRY") or "").strip().upper()
        if not (date and pid and country):
            continue
        gross = sf(row.get("GROSS_REVENUE"))
        if gross <= 0:
            continue
        key = (date, pid, country)
        agg = grouped[key]
        agg["impressions"]   += sf(row.get("IMPRESSIONS"))
        agg["gross_revenue"] += gross
        agg["pub_payout"]    += sf(row.get("PUB_PAYOUT"))
        meta[key] = pname
    return [
        {"report_date": k[0], "publisher_id": k[1], "publisher_name": meta[k],
         "country": k[2],
         "impressions": int(v["impressions"]),
         "gross_revenue": round(v["gross_revenue"], 4),
         "pub_payout": round(v["pub_payout"], 4)}
        for k, v in grouped.items()
    ]


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
    print(f"[ll_segments_etl] {start_date}..{end_date} ({window_days}d)", flush=True)

    try:
        dev_rows = fetch("DATE,DEVICE_TYPE,OS", DEVICE_OS_METRICS, start_date, end_date)
    except Exception as exc:
        return {"ok": False, "error": f"device_os: {exc}"}
    dev_records = _normalize_device_os(dev_rows)
    print(f"[ll_segments_etl] device_os: {len(dev_rows)} -> {len(dev_records)} non-zero", flush=True)

    try:
        hr_rows = fetch("DATE,HOUR", HOUR_METRICS, start_date, end_date)
    except Exception as exc:
        return {"ok": False, "error": f"hour: {exc}"}
    hr_records = _normalize_hour(hr_rows)
    print(f"[ll_segments_etl] hour: {len(hr_rows)} -> {len(hr_records)}", flush=True)

    try:
        fun_rows = fetch("DATE,PUBLISHER", FUNNEL_METRICS, start_date, end_date)
    except Exception as exc:
        return {"ok": False, "error": f"funnel: {exc}"}
    fun_records = _normalize_funnel(fun_rows)
    print(f"[ll_segments_etl] funnel: {len(fun_rows)} -> {len(fun_records)}", flush=True)

    try:
        pc_rows = fetch("DATE,PUBLISHER,COUNTRY", PUB_COUNTRY_METRICS, start_date, end_date)
    except Exception as exc:
        return {"ok": False, "error": f"pub_country: {exc}"}
    pc_records = _normalize_pub_country(pc_rows)
    print(f"[ll_segments_etl] pub_country: {len(pc_rows)} -> {len(pc_records)} non-zero", flush=True)

    try:
        n_dev = _upsert(_DEVICE_OS_UPSERT, dev_records)
        n_hr  = _upsert(_HOUR_UPSERT, hr_records)
        n_fun = _upsert(_FUNNEL_UPSERT, fun_records)
        n_pc  = _upsert(_PUB_COUNTRY_UPSERT, pc_records)
    except Exception as exc:
        return {"ok": False, "error": str(exc)}

    print(f"[ll_segments_etl] DONE — {n_dev} device_os + {n_hr} hour + {n_fun} funnel + {n_pc} pub_country", flush=True)
    return {"ok": True, "device_os": n_dev, "hour": n_hr, "funnel": n_fun, "pub_country": n_pc}


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--backfill", type=int, default=None)
    args = parser.parse_args()
    result = run(window_days=args.backfill or WINDOW_DAYS)
    sys.exit(0 if result.get("ok") else 1)
