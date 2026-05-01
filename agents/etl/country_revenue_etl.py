"""
agents/etl/country_revenue_etl.py

Hourly ETL that lands daily country revenue from LL and TB into Neon
for the Executive Dashboard's Geography section.

  pgam_direct.ll_daily_country_revenue   (PK date, country)
  pgam_direct.tb_daily_country_revenue   (PK date, country)

LL exposes a DATE,COUNTRY breakdown that returns ~240 rows/day in
~1s. TB only gives a country breakdown WITHOUT a date dim, so we
issue one TB call per day.

LL emits ISO-3166-1 alpha-2 ("US", "GB"); TB emits alpha-3 ("USA",
"GBR"). Stored as-emitted; the read side normalises.

Window:
  - Hourly: trailing 2 days
  - Backfill: `python -m agents.etl.country_revenue_etl --backfill 30`
"""

import argparse
import sys
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Iterable

from core.api import fetch, sf, n_days_ago, today
from core.tb_api import fetch_tb
from core.neon import connect

WINDOW_DAYS = 2
LL_METRICS = ["GROSS_REVENUE", "PUB_PAYOUT", "IMPRESSIONS"]
TB_METRICS = ["GROSS_REVENUE", "PUB_PAYOUT", "IMPRESSIONS"]


def _date_range(start: str, end: str) -> list[str]:
    s = datetime.strptime(start, "%Y-%m-%d").date()
    e = datetime.strptime(end, "%Y-%m-%d").date()
    out: list[str] = []
    cur = s
    while cur <= e:
        out.append(cur.isoformat())
        cur += timedelta(days=1)
    return out


def _normalize_ll(rows: Iterable[dict]) -> list[dict]:
    grouped: dict[tuple, dict] = defaultdict(lambda: {
        "impressions": 0.0, "gross_revenue": 0.0, "pub_payout": 0.0,
    })
    for row in rows:
        date = str(row.get("DATE") or row.get("date") or "")
        country = str(row.get("COUNTRY") or row.get("country") or "").strip().upper()
        if not (date and country):
            continue
        gross = sf(row.get("GROSS_REVENUE"))
        if gross <= 0:
            continue
        key = (date, country)
        agg = grouped[key]
        agg["impressions"]   += sf(row.get("IMPRESSIONS"))
        agg["gross_revenue"] += gross
        agg["pub_payout"]    += sf(row.get("PUB_PAYOUT"))
    return [
        {
            "report_date":   k[0],
            "country":       k[1],
            "impressions":   int(v["impressions"]),
            "gross_revenue": round(v["gross_revenue"], 4),
            "pub_payout":    round(v["pub_payout"], 4),
        }
        for k, v in grouped.items()
    ]


def _normalize_tb(date: str, rows: Iterable[dict]) -> list[dict]:
    """TB country rows for a single day. The breakdown is COUNTRY_NAME
    alone (TB doesn't support DATE,COUNTRY together) so we attach the
    date externally and dedupe."""
    grouped: dict[tuple, dict] = defaultdict(lambda: {
        "impressions": 0.0, "gross_revenue": 0.0, "pub_payout": 0.0,
    })
    for row in rows:
        country = str(row.get("COUNTRY_NAME") or row.get("COUNTRY") or "").strip().upper()
        if not country:
            continue
        gross = sf(row.get("GROSS_REVENUE"))
        if gross <= 0:
            continue
        key = (date, country)
        agg = grouped[key]
        agg["impressions"]   += sf(row.get("IMPRESSIONS"))
        agg["gross_revenue"] += gross
        agg["pub_payout"]    += sf(row.get("PUB_PAYOUT"))
    return [
        {
            "report_date":   k[0],
            "country":       k[1],
            "impressions":   int(v["impressions"]),
            "gross_revenue": round(v["gross_revenue"], 4),
            "pub_payout":    round(v["pub_payout"], 4),
        }
        for k, v in grouped.items()
    ]


_LL_UPSERT = """
INSERT INTO pgam_direct.ll_daily_country_revenue
  (report_date, country, impressions, gross_revenue, pub_payout, updated_at)
VALUES
  (%(report_date)s, %(country)s, %(impressions)s,
   %(gross_revenue)s, %(pub_payout)s, now())
ON CONFLICT (report_date, country) DO UPDATE SET
  impressions   = EXCLUDED.impressions,
  gross_revenue = EXCLUDED.gross_revenue,
  pub_payout    = EXCLUDED.pub_payout,
  updated_at    = now();
"""

_TB_UPSERT = """
INSERT INTO pgam_direct.tb_daily_country_revenue
  (report_date, country, impressions, gross_revenue, pub_payout, updated_at)
VALUES
  (%(report_date)s, %(country)s, %(impressions)s,
   %(gross_revenue)s, %(pub_payout)s, now())
ON CONFLICT (report_date, country) DO UPDATE SET
  impressions   = EXCLUDED.impressions,
  gross_revenue = EXCLUDED.gross_revenue,
  pub_payout    = EXCLUDED.pub_payout,
  updated_at    = now();
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
    days = _date_range(start_date, end_date)
    print(f"[country_revenue_etl] {start_date}..{end_date} ({len(days)}d)", flush=True)

    # 1. LL — single multi-day fetch
    try:
        ll_rows = fetch("DATE,COUNTRY", LL_METRICS, start_date, end_date)
    except Exception as exc:
        print(f"[country_revenue_etl] LL fetch failed: {exc}", flush=True)
        return {"ok": False, "error": f"ll: {exc}"}
    ll_records = _normalize_ll(ll_rows)
    print(f"[country_revenue_etl] LL: {len(ll_rows)} raw -> {len(ll_records)} non-zero", flush=True)

    # 2. TB — one call per day (no DATE,COUNTRY composite available)
    tb_records: list[dict] = []
    for d in days:
        try:
            tb_rows = fetch_tb("COUNTRY_NAME", TB_METRICS, d, d)
        except Exception as exc:
            print(f"[country_revenue_etl] TB {d} failed: {exc}", flush=True)
            continue
        recs = _normalize_tb(d, tb_rows)
        tb_records.extend(recs)
        print(f"[country_revenue_etl]   TB {d} -> {len(tb_rows)} raw / {len(recs)} non-zero", flush=True)

    # 3. UPSERT both
    try:
        n_ll = _upsert(_LL_UPSERT, ll_records)
        n_tb = _upsert(_TB_UPSERT, tb_records)
    except Exception as exc:
        print(f"[country_revenue_etl] UPSERT failed: {exc}", flush=True)
        return {"ok": False, "error": str(exc)}

    print(f"[country_revenue_etl] DONE — {n_ll} LL + {n_tb} TB", flush=True)
    return {"ok": True, "ll_rows": n_ll, "tb_rows": n_tb}


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--backfill", type=int, default=None)
    args = parser.parse_args()
    result = run(window_days=args.backfill or WINDOW_DAYS)
    sys.exit(0 if result.get("ok") else 1)
