"""
agents/etl/ll_4dim_etl.py

Hourly ETL that lands LL per-publisher × per-(domain|bundle) ×
per-demand-partner daily rollups into Neon. Powers the Executive
Dashboard's deep drill-down: pick a domain or app under a brand,
see which DSPs paid for THAT specific inventory.

Two API calls per run, both ~5–8s for a single day:
  1. DATE,PUBLISHER,DOMAIN,DEMAND_PARTNER
       → pgam_direct.ll_daily_publisher_domain_demand
  2. DATE,PUBLISHER,BUNDLE,DEMAND_PARTNER
       → pgam_direct.ll_daily_publisher_bundle_demand

Filtering: same as ll_dimensions_etl — drop gross_revenue<=0 rows
to keep persisted tables meaningful (LL emits ~97% zero-rev rows
on the 4-dim breakdowns; the survivors are what operators care
about).

Window: trailing WINDOW_DAYS = 2 (today + yesterday) hourly.
Backfill: `python -m agents.etl.ll_4dim_etl --backfill 30`.
"""

import argparse
import sys
import time
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Iterable

from core.api import fetch, sf, n_days_ago, today
from core.neon import connect


def _fetch_with_retry(breakdown: str, start: str, end: str, attempts: int = 3) -> list[dict]:
    """fetch() with retry-on-timeout. The 4-dim breakdown is heavy
    (~170k rows/day) and LL occasionally times out at the 30s default
    read deadline; a simple retry-with-short-backoff usually succeeds
    on the second attempt. Re-raises after `attempts` failures."""
    last_exc: Exception | None = None
    for i in range(attempts):
        try:
            return fetch(breakdown, METRICS, start, end)
        except Exception as exc:
            last_exc = exc
            wait = 2 ** i
            print(f"[ll_4dim_etl]   {breakdown} {start}..{end} retry {i+1}/{attempts} in {wait}s: {exc}", flush=True)
            time.sleep(wait)
    raise last_exc if last_exc else RuntimeError("fetch failed without exception")

METRICS = ["GROSS_REVENUE", "PUB_PAYOUT", "IMPRESSIONS"]
WINDOW_DAYS = 2
CHUNK_DAYS = 1   # multi-dim fetches scale linearly with days; chunk per day

_DATE_KEYS     = ("DATE", "date")
_PUB_ID_KEYS   = ("PUBLISHER_ID", "publisher_id")
_PUB_NAME_KEYS = ("PUBLISHER_NAME", "PUBLISHER", "publisher_name")
_DOM_KEYS      = ("DOMAIN", "domain")
_BUN_KEYS      = ("BUNDLE", "bundle")
_DMD_ID_KEYS   = ("DEMAND_PARTNER_ID", "DEMAND_ID", "demand_partner_id", "demand_id")
_DMD_NAME_KEYS = ("DEMAND_PARTNER_NAME", "DEMAND_NAME", "demand_partner_name", "demand_name")


def _first(row: dict, keys: tuple) -> str:
    for k in keys:
        v = row.get(k)
        if v not in (None, ""):
            return str(v)
    return ""


def _date_chunks(start: str, end: str, chunk_days: int) -> list[tuple[str, str]]:
    s = datetime.strptime(start, "%Y-%m-%d").date()
    e = datetime.strptime(end, "%Y-%m-%d").date()
    out: list[tuple[str, str]] = []
    cur = s
    while cur <= e:
        chunk_end = min(cur + timedelta(days=chunk_days - 1), e)
        out.append((cur.isoformat(), chunk_end.isoformat()))
        cur = chunk_end + timedelta(days=1)
    return out


def _fetch_chunked(breakdown: str, start: str, end: str) -> list[dict]:
    """Pull a date range in 1-day chunks. Per-chunk retries on timeout —
    the 4-dim API call is ~10MB of JSON and read-times-out on busy days."""
    all_rows: list[dict] = []
    for cs, ce in _date_chunks(start, end, CHUNK_DAYS):
        chunk = _fetch_with_retry(breakdown, cs, ce)
        all_rows.extend(chunk)
        nz = sum(1 for r in chunk if sf(r.get("GROSS_REVENUE")) > 0)
        print(f"[ll_4dim_etl]   {breakdown} {cs}..{ce} -> {len(chunk)} rows ({nz} non-zero)", flush=True)
    return all_rows


def _normalize(rows: Iterable[dict], dim: str) -> list[dict]:
    """Aggregate by (date, publisher_id, dim_value, demand_id), filter zeros."""
    if dim == "domain":
        dim_keys, dim_col = _DOM_KEYS, "domain"
    else:
        dim_keys, dim_col = _BUN_KEYS, "bundle"

    grouped: dict[tuple, dict] = defaultdict(lambda: {
        "impressions": 0.0, "gross_revenue": 0.0, "pub_payout": 0.0,
    })
    meta: dict[tuple, dict] = {}

    for row in rows:
        report_date    = _first(row, _DATE_KEYS)
        publisher_id   = _first(row, _PUB_ID_KEYS)
        publisher_name = _first(row, _PUB_NAME_KEYS) or publisher_id
        dim_value      = _first(row, dim_keys)
        demand_id      = _first(row, _DMD_ID_KEYS)
        demand_name    = _first(row, _DMD_NAME_KEYS) or demand_id
        if not (report_date and publisher_id and dim_value and demand_id):
            continue
        gross = sf(row.get("GROSS_REVENUE"))
        if gross <= 0:
            continue
        key = (report_date, publisher_id, dim_value, demand_id)
        agg = grouped[key]
        agg["impressions"]   += sf(row.get("IMPRESSIONS"))
        agg["gross_revenue"] += gross
        agg["pub_payout"]    += sf(row.get("PUB_PAYOUT"))
        meta[key] = {"publisher_name": publisher_name, "demand_name": demand_name}

    return [
        {
            "report_date":    k[0],
            "publisher_id":   k[1],
            "publisher_name": meta[k]["publisher_name"],
            dim_col:          k[2],
            "demand_id":      k[3],
            "demand_name":    meta[k]["demand_name"],
            "impressions":    int(v["impressions"]),
            "gross_revenue":  round(v["gross_revenue"], 4),
            "pub_payout":     round(v["pub_payout"], 4),
        }
        for k, v in grouped.items()
    ]


_DOMAIN_UPSERT = """
INSERT INTO pgam_direct.ll_daily_publisher_domain_demand
  (report_date, publisher_id, publisher_name, domain, demand_id, demand_name,
   impressions, gross_revenue, pub_payout, updated_at)
VALUES
  (%(report_date)s, %(publisher_id)s, %(publisher_name)s, %(domain)s,
   %(demand_id)s, %(demand_name)s,
   %(impressions)s, %(gross_revenue)s, %(pub_payout)s, now())
ON CONFLICT (report_date, publisher_id, domain, demand_id) DO UPDATE SET
  publisher_name = EXCLUDED.publisher_name,
  demand_name    = EXCLUDED.demand_name,
  impressions    = EXCLUDED.impressions,
  gross_revenue  = EXCLUDED.gross_revenue,
  pub_payout     = EXCLUDED.pub_payout,
  updated_at     = now();
"""

_BUNDLE_UPSERT = """
INSERT INTO pgam_direct.ll_daily_publisher_bundle_demand
  (report_date, publisher_id, publisher_name, bundle, demand_id, demand_name,
   impressions, gross_revenue, pub_payout, updated_at)
VALUES
  (%(report_date)s, %(publisher_id)s, %(publisher_name)s, %(bundle)s,
   %(demand_id)s, %(demand_name)s,
   %(impressions)s, %(gross_revenue)s, %(pub_payout)s, now())
ON CONFLICT (report_date, publisher_id, bundle, demand_id) DO UPDATE SET
  publisher_name = EXCLUDED.publisher_name,
  demand_name    = EXCLUDED.demand_name,
  impressions    = EXCLUDED.impressions,
  gross_revenue  = EXCLUDED.gross_revenue,
  pub_payout     = EXCLUDED.pub_payout,
  updated_at     = now();
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
            print(f"[ll_4dim_etl]   committed {n}/{len(records)}", flush=True)
    return n


def run(window_days: int = WINDOW_DAYS) -> dict:
    end_date = today()
    start_date = n_days_ago(max(window_days - 1, 0))

    print(
        f"[ll_4dim_etl] Fetching {start_date}..{end_date} ({window_days}d, chunk={CHUNK_DAYS}d)",
        flush=True,
    )

    try:
        dom_rows = _fetch_chunked("DATE,PUBLISHER,DOMAIN,DEMAND_PARTNER", start_date, end_date)
    except Exception as exc:
        print(f"[ll_4dim_etl] DOMAIN fetch failed: {exc}", flush=True)
        return {"ok": False, "error": f"domain fetch: {exc}"}
    dom_records = _normalize(dom_rows, "domain")
    print(f"[ll_4dim_etl] DOMAIN total: {len(dom_rows)} raw -> {len(dom_records)} non-zero unique", flush=True)

    try:
        bun_rows = _fetch_chunked("DATE,PUBLISHER,BUNDLE,DEMAND_PARTNER", start_date, end_date)
    except Exception as exc:
        print(f"[ll_4dim_etl] BUNDLE fetch failed: {exc}", flush=True)
        return {"ok": False, "error": f"bundle fetch: {exc}"}
    bun_records = _normalize(bun_rows, "bundle")
    print(f"[ll_4dim_etl] BUNDLE total: {len(bun_rows)} raw -> {len(bun_records)} non-zero unique", flush=True)

    try:
        n_dom = _upsert_chunked(_DOMAIN_UPSERT, dom_records)
        n_bun = _upsert_chunked(_BUNDLE_UPSERT, bun_records)
    except Exception as exc:
        print(f"[ll_4dim_etl] Neon UPSERT failed: {exc}", flush=True)
        return {"ok": False, "error": str(exc)}

    print(f"[ll_4dim_etl] DONE — upserted {n_dom} domain×demand + {n_bun} bundle×demand rows", flush=True)
    return {"ok": True, "domain_rows": n_dom, "bundle_rows": n_bun}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Land LL 4-dim per-publisher × dim × demand rollups")
    parser.add_argument("--backfill", type=int, default=None,
                        help="Override window: pull this many trailing days (default: 2)")
    args = parser.parse_args()
    result = run(window_days=args.backfill or WINDOW_DAYS)
    sys.exit(0 if result.get("ok") else 1)
