"""
agents/etl/ll_dimensions_etl.py

Hourly ETL that lands LL per-publisher × per-(domain|bundle) daily
rollups into Neon. Powers the Executive Dashboard's drill-down panel:
pick a supply brand (Smaato, Illumin, ...) → see which domains and
apps are monetising under that brand.

Two API calls per run:
  1. DATE,PUBLISHER,DOMAIN — landed in pgam_direct.ll_daily_publisher_domain
  2. DATE,PUBLISHER,BUNDLE — landed in pgam_direct.ll_daily_publisher_bundle

Both calls return ~120k–280k rows for a single day, but the LL API is
fast (~4s/call). ~88% of those rows have zero revenue — we filter
them out at the ETL layer to keep the table sizes reasonable. Only
rows where gross_revenue > 0 are persisted.

Window:
  - Hourly run: trailing WINDOW_DAYS = 2 (today + yesterday)
  - Backfill:   `python -m agents.etl.ll_dimensions_etl --backfill 30`
"""

import argparse
import sys
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Iterable

from core.api import fetch, sf, n_days_ago, today, yesterday
from core.neon import connect


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
    """Pull a date range in CHUNK_DAYS slices; concatenate. Single-day
    LL multi-dim fetches return in ~4s so a 30-day backfill runs in
    ~2 minutes per breakdown."""
    all_rows: list[dict] = []
    for cs, ce in _date_chunks(start, end, CHUNK_DAYS):
        chunk = fetch(breakdown, METRICS, cs, ce)
        all_rows.extend(chunk)
        print(f"[ll_dimensions_etl]   {breakdown} {cs}..{ce} -> {len(chunk)} rows")
    return all_rows

METRICS = ["GROSS_REVENUE", "PUB_PAYOUT", "IMPRESSIONS"]
WINDOW_DAYS = 2
# Single-day fetches return ~120k–280k rows in ~4s. Multi-day fetches
# fan out linearly and time out around 5–7 days. Chunk per day.
CHUNK_DAYS = 1

_DATE_KEYS = ("DATE", "date")
_PUB_ID_KEYS    = ("PUBLISHER_ID", "publisher_id")
_PUB_NAME_KEYS  = ("PUBLISHER_NAME", "PUBLISHER", "publisher_name", "publisher")
_DOMAIN_KEYS    = ("DOMAIN", "domain")
_BUNDLE_KEYS    = ("BUNDLE", "bundle")


def _first(row: dict, keys: tuple) -> str:
    for k in keys:
        v = row.get(k)
        if v not in (None, ""):
            return str(v)
    return ""


def _normalize(rows: Iterable[dict], dim: str) -> list[dict]:
    """Aggregate by (date, publisher_id, dim_value), filter zeros.

    `dim` is "domain" or "bundle"; controls which key we pluck for the
    third dimension. Filters out rows where gross_revenue==0 — LL emits
    a long tail of empty rows we don't need to persist.
    """
    if dim == "domain":
        dim_keys, dim_col = _DOMAIN_KEYS, "domain"
    else:
        dim_keys, dim_col = _BUNDLE_KEYS, "bundle"

    grouped: dict[tuple, dict] = defaultdict(lambda: {
        "impressions": 0.0, "gross_revenue": 0.0, "pub_payout": 0.0,
    })
    meta: dict[tuple, str] = {}

    for row in rows:
        report_date    = _first(row, _DATE_KEYS)
        publisher_id   = _first(row, _PUB_ID_KEYS)
        publisher_name = _first(row, _PUB_NAME_KEYS) or publisher_id
        dim_value      = _first(row, dim_keys)
        if not (report_date and publisher_id and dim_value):
            continue
        gross = sf(row.get("GROSS_REVENUE"))
        if gross <= 0:
            # 88% of LL's domain/bundle rows are zero-revenue. Drop them
            # at the ETL layer so the persisted tables only contain
            # meaningful inventory.
            continue
        key = (report_date, publisher_id, dim_value)
        agg = grouped[key]
        agg["impressions"]   += sf(row.get("IMPRESSIONS"))
        agg["gross_revenue"] += gross
        agg["pub_payout"]    += sf(row.get("PUB_PAYOUT"))
        meta[key] = publisher_name

    return [
        {
            "report_date":    k[0],
            "publisher_id":   k[1],
            "publisher_name": meta[k],
            dim_col:          k[2],
            "impressions":    int(v["impressions"]),
            "gross_revenue":  round(v["gross_revenue"], 4),
            "pub_payout":     round(v["pub_payout"], 4),
        }
        for k, v in grouped.items()
    ]


_DOMAIN_UPSERT = """
INSERT INTO pgam_direct.ll_daily_publisher_domain
  (report_date, publisher_id, publisher_name, domain,
   impressions, gross_revenue, pub_payout, updated_at)
VALUES
  (%(report_date)s, %(publisher_id)s, %(publisher_name)s, %(domain)s,
   %(impressions)s, %(gross_revenue)s, %(pub_payout)s, now())
ON CONFLICT (report_date, publisher_id, domain) DO UPDATE SET
  publisher_name = EXCLUDED.publisher_name,
  impressions    = EXCLUDED.impressions,
  gross_revenue  = EXCLUDED.gross_revenue,
  pub_payout     = EXCLUDED.pub_payout,
  updated_at     = now();
"""

_BUNDLE_UPSERT = """
INSERT INTO pgam_direct.ll_daily_publisher_bundle
  (report_date, publisher_id, publisher_name, bundle,
   impressions, gross_revenue, pub_payout, updated_at)
VALUES
  (%(report_date)s, %(publisher_id)s, %(publisher_name)s, %(bundle)s,
   %(impressions)s, %(gross_revenue)s, %(pub_payout)s, now())
ON CONFLICT (report_date, publisher_id, bundle) DO UPDATE SET
  publisher_name = EXCLUDED.publisher_name,
  impressions    = EXCLUDED.impressions,
  gross_revenue  = EXCLUDED.gross_revenue,
  pub_payout     = EXCLUDED.pub_payout,
  updated_at     = now();
"""


def _upsert_chunked(sql: str, records: list[dict], chunk_size: int = 5000) -> int:
    """UPSERT in chunks so we don't hold a single 100k-row transaction.

    Each chunk commits independently — if the run fails midway, the
    completed chunks stay landed and the next hourly run picks up
    where we left off.
    """
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

    print(
        f"[ll_dimensions_etl] Fetching LL DATE,PUBLISHER,DOMAIN + "
        f"DATE,PUBLISHER,BUNDLE for {start_date}..{end_date} ({window_days}d)"
    )

    # 1. DOMAIN
    try:
        dom_rows = _fetch_chunked("DATE,PUBLISHER,DOMAIN", start_date, end_date)
    except Exception as exc:
        print(f"[ll_dimensions_etl] DOMAIN fetch failed: {exc}")
        return {"ok": False, "error": f"domain fetch: {exc}"}
    dom_records = _normalize(dom_rows, "domain")
    print(f"[ll_dimensions_etl] DOMAIN: {len(dom_rows)} raw -> {len(dom_records)} non-zero unique")

    # 2. BUNDLE
    try:
        bun_rows = _fetch_chunked("DATE,PUBLISHER,BUNDLE", start_date, end_date)
    except Exception as exc:
        print(f"[ll_dimensions_etl] BUNDLE fetch failed: {exc}")
        return {"ok": False, "error": f"bundle fetch: {exc}"}
    bun_records = _normalize(bun_rows, "bundle")
    print(f"[ll_dimensions_etl] BUNDLE: {len(bun_rows)} raw -> {len(bun_records)} non-zero unique")

    # 3. UPSERT both
    try:
        n_dom = _upsert_chunked(_DOMAIN_UPSERT, dom_records)
        n_bun = _upsert_chunked(_BUNDLE_UPSERT, bun_records)
    except Exception as exc:
        print(f"[ll_dimensions_etl] Neon UPSERT failed: {exc}")
        return {"ok": False, "error": str(exc)}

    print(f"[ll_dimensions_etl] Upserted {n_dom} domain rows + {n_bun} bundle rows.")
    return {"ok": True, "domain_rows": n_dom, "bundle_rows": n_bun}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Land LL per-publisher dimensions")
    parser.add_argument(
        "--backfill", type=int, default=None,
        help="Override window: pull this many trailing days (default: 2)",
    )
    args = parser.parse_args()
    result = run(window_days=args.backfill or WINDOW_DAYS)
    sys.exit(0 if result.get("ok") else 1)
