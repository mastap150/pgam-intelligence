"""
agents/etl/tb_segments_etl.py

Hourly ETL that lands richer per-publisher TB rollups now that we
know the TB stats endpoint accepts more attribute combos than
core/tb_api.py originally mapped:

  pgam_direct.tb_daily_publisher_demand_revenue   PUBLISHER,DEMAND_PARTNER
  pgam_direct.tb_daily_publisher_country          PUBLISHER,COUNTRY_NAME
  pgam_direct.tb_daily_os                         OS

Three calls per day. Total runtime ~30s for the trailing 2 days
(default window) and ~15 min for the 30-day backfill.

TB doesn't expose a per-day grain on these multi-attr breakdowns
(day_group=total drops the date), so we issue one fetch per
target_date and stamp the date externally.
"""

import argparse
import sys
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Iterable

from core.api import sf, n_days_ago, today
from core.tb_api import fetch_tb
from core.neon import connect

WINDOW_DAYS = 2
METRICS = ["GROSS_REVENUE", "PUB_PAYOUT", "IMPRESSIONS", "WINS", "BIDS"]


def _date_range(start: str, end: str) -> list[str]:
    s = datetime.strptime(start, "%Y-%m-%d").date()
    e = datetime.strptime(end, "%Y-%m-%d").date()
    out: list[str] = []
    cur = s
    while cur <= e:
        out.append(cur.isoformat())
        cur += timedelta(days=1)
    return out


def _normalize_pub_demand(date: str, rows: Iterable[dict]) -> list[dict]:
    grouped: dict[tuple, dict] = defaultdict(lambda: {
        "impressions": 0.0, "bids": 0.0, "wins": 0.0,
        "gross_revenue": 0.0, "pub_payout": 0.0,
    })
    meta: dict[tuple, dict] = {}
    for row in rows:
        pub_id   = str(row.get("PUBLISHER") or row.get("ssp_id") or "")
        pub_name = str(row.get("PUBLISHER_NAME") or row.get("ssp_name") or "") or pub_id
        dmd_id   = str(row.get("DEMAND_PARTNER") or row.get("dsp_id") or "")
        dmd_name = str(row.get("DEMAND_PARTNER_NAME") or row.get("dsp_name") or "") or dmd_id
        if not (pub_id and dmd_id):
            continue
        gross = sf(row.get("GROSS_REVENUE"))
        if gross <= 0:
            continue
        key = (date, pub_id, dmd_id)
        agg = grouped[key]
        agg["impressions"]   += sf(row.get("IMPRESSIONS"))
        agg["bids"]          += sf(row.get("BIDS"))
        agg["wins"]          += sf(row.get("WINS"))
        agg["gross_revenue"] += gross
        agg["pub_payout"]    += sf(row.get("PUB_PAYOUT"))
        meta[key] = {"publisher_name": pub_name, "demand_name": dmd_name}
    return [
        {"report_date": k[0], "publisher_id": k[1],
         "publisher_name": meta[k]["publisher_name"],
         "demand_id": k[2], "demand_name": meta[k]["demand_name"],
         "impressions": int(v["impressions"]),
         "bids": int(v["bids"]), "wins": int(v["wins"]),
         "gross_revenue": round(v["gross_revenue"], 4),
         "pub_payout": round(v["pub_payout"], 4)}
        for k, v in grouped.items()
    ]


def _normalize_pub_country(date: str, rows: Iterable[dict]) -> list[dict]:
    grouped: dict[tuple, dict] = defaultdict(lambda: {
        "impressions": 0.0, "gross_revenue": 0.0, "pub_payout": 0.0,
    })
    meta: dict[tuple, str] = {}
    for row in rows:
        pub_id   = str(row.get("PUBLISHER") or row.get("ssp_id") or "")
        pub_name = str(row.get("PUBLISHER_NAME") or row.get("ssp_name") or "") or pub_id
        country  = str(row.get("COUNTRY_NAME") or row.get("country") or "").strip().upper()
        if not (pub_id and country):
            continue
        gross = sf(row.get("GROSS_REVENUE"))
        if gross <= 0:
            continue
        key = (date, pub_id, country)
        agg = grouped[key]
        agg["impressions"]   += sf(row.get("IMPRESSIONS"))
        agg["gross_revenue"] += gross
        agg["pub_payout"]    += sf(row.get("PUB_PAYOUT"))
        meta[key] = pub_name
    return [
        {"report_date": k[0], "publisher_id": k[1], "publisher_name": meta[k],
         "country": k[2],
         "impressions": int(v["impressions"]),
         "gross_revenue": round(v["gross_revenue"], 4),
         "pub_payout": round(v["pub_payout"], 4)}
        for k, v in grouped.items()
    ]


def _normalize_os(date: str, rows: Iterable[dict]) -> list[dict]:
    out: list[dict] = []
    for row in rows:
        osv = str(row.get("os") or row.get("OS") or "").strip().lower()
        if not osv:
            continue
        gross = sf(row.get("GROSS_REVENUE"))
        if gross <= 0:
            continue
        out.append({
            "report_date": date, "os": osv,
            "impressions": int(sf(row.get("IMPRESSIONS"))),
            "gross_revenue": round(gross, 4),
            "pub_payout": round(sf(row.get("PUB_PAYOUT")), 4),
        })
    return out


_PUB_DEMAND_UPSERT = """
INSERT INTO pgam_direct.tb_daily_publisher_demand_revenue
  (report_date, publisher_id, publisher_name, demand_id, demand_name,
   impressions, bids, wins, gross_revenue, pub_payout, updated_at)
VALUES
  (%(report_date)s, %(publisher_id)s, %(publisher_name)s,
   %(demand_id)s, %(demand_name)s,
   %(impressions)s, %(bids)s, %(wins)s,
   %(gross_revenue)s, %(pub_payout)s, now())
ON CONFLICT (report_date, publisher_id, demand_id) DO UPDATE SET
  publisher_name = EXCLUDED.publisher_name,
  demand_name    = EXCLUDED.demand_name,
  impressions    = EXCLUDED.impressions,
  bids           = EXCLUDED.bids,
  wins           = EXCLUDED.wins,
  gross_revenue  = EXCLUDED.gross_revenue,
  pub_payout     = EXCLUDED.pub_payout,
  updated_at     = now();
"""

_PUB_COUNTRY_UPSERT = """
INSERT INTO pgam_direct.tb_daily_publisher_country
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

_OS_UPSERT = """
INSERT INTO pgam_direct.tb_daily_os
  (report_date, os, impressions, gross_revenue, pub_payout, updated_at)
VALUES
  (%(report_date)s, %(os)s, %(impressions)s,
   %(gross_revenue)s, %(pub_payout)s, now())
ON CONFLICT (report_date, os) DO UPDATE SET
  impressions   = EXCLUDED.impressions,
  gross_revenue = EXCLUDED.gross_revenue,
  pub_payout    = EXCLUDED.pub_payout,
  updated_at    = now();
"""


def _upsert_chunked(sql: str, records: list[dict], chunk_size: int = 5000) -> int:
    if not records:
        return 0
    n = 0
    with connect() as conn:
        for i in range(0, len(records), chunk_size):
            chunk = records[i:i + chunk_size]
            with conn.cursor() as cur:
                cur.executemany(sql, chunk)
            conn.commit()
            n += len(chunk)
    return n


def run(window_days: int = WINDOW_DAYS) -> dict:
    end_date = today()
    start_date = n_days_ago(max(window_days - 1, 0))
    days = _date_range(start_date, end_date)
    print(f"[tb_segments_etl] {start_date}..{end_date} ({len(days)}d)", flush=True)

    pd_records: list[dict] = []
    pc_records: list[dict] = []
    os_records: list[dict] = []

    for d in days:
        # 1. PUBLISHER × DEMAND_PARTNER
        try:
            rows = fetch_tb("PUBLISHER,DEMAND_PARTNER", METRICS, d, d)
            recs = _normalize_pub_demand(d, rows)
            pd_records.extend(recs)
            print(f"[tb_segments_etl]   {d} pub×dmd: {len(rows)} -> {len(recs)} non-zero", flush=True)
        except Exception as exc:
            print(f"[tb_segments_etl]   {d} pub×dmd FAILED: {exc}", flush=True)

        # 2. PUBLISHER × COUNTRY
        try:
            rows = fetch_tb("PUBLISHER,COUNTRY_NAME", METRICS, d, d)
            recs = _normalize_pub_country(d, rows)
            pc_records.extend(recs)
            print(f"[tb_segments_etl]   {d} pub×cty: {len(rows)} -> {len(recs)} non-zero", flush=True)
        except Exception as exc:
            print(f"[tb_segments_etl]   {d} pub×cty FAILED: {exc}", flush=True)

        # 3. OS
        try:
            rows = fetch_tb("OS", METRICS, d, d)
            recs = _normalize_os(d, rows)
            os_records.extend(recs)
            print(f"[tb_segments_etl]   {d} os: {len(rows)} -> {len(recs)} non-zero", flush=True)
        except Exception as exc:
            print(f"[tb_segments_etl]   {d} os FAILED: {exc}", flush=True)

    try:
        n_pd = _upsert_chunked(_PUB_DEMAND_UPSERT, pd_records)
        n_pc = _upsert_chunked(_PUB_COUNTRY_UPSERT, pc_records)
        n_os = _upsert_chunked(_OS_UPSERT, os_records)
    except Exception as exc:
        return {"ok": False, "error": str(exc)}

    print(f"[tb_segments_etl] DONE — {n_pd} pub×dmd + {n_pc} pub×cty + {n_os} os", flush=True)
    return {"ok": True, "pub_demand": n_pd, "pub_country": n_pc, "os": n_os}


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--backfill", type=int, default=None)
    args = parser.parse_args()
    result = run(window_days=args.backfill or WINDOW_DAYS)
    sys.exit(0 if result.get("ok") else 1)
