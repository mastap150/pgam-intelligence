"""
agents/etl/tb_revenue_etl.py

Hourly ETL that lands TB (TrueBlue / "Direct" in Domo) daily revenue
into Neon for the Executive Dashboard at
admin.pgammedia.com/admin/executive-dashboard.

Why two destination tables (publisher + demand)
-----------------------------------------------
TB's stats endpoint at ssp.pgammedia.com times out on the two-dim
DATE,PUBLISHER,DEMAND_PARTNER breakdown (the cell count is
~publishers × ~demands × days). Single-dim breakdowns return in ~17s.
So this ETL pulls DATE,PUBLISHER and DATE,DEMAND_PARTNER separately
and lands each in its own table:

  pgam_direct.tb_daily_publisher_revenue  (PK date, publisher_id)
  pgam_direct.tb_daily_demand_revenue     (PK date, demand_id)

Totals reconcile across the two — same `report_date` SUM(gross_revenue)
should match. The dashboard uses the publisher table for top-line
tiles and joins each table for its corresponding rollup view.

Schedule: every 60 minutes (in scheduler.py). UPSERTs the trailing
WINDOW_DAYS = 2 by default (today + yesterday). Backfill via
`python -m agents.etl.tb_revenue_etl --backfill 30`.
"""

import argparse
import sys
from collections import defaultdict
from datetime import date, datetime, timedelta
from typing import Iterable

from core.api import sf, n_days_ago, today, yesterday
from core.tb_api import fetch_tb
from core.neon import connect

METRICS = ["GROSS_REVENUE", "PUB_PAYOUT", "IMPRESSIONS", "WINS", "BIDS"]
WINDOW_DAYS = 2
# TB stats endpoint has two hard ceilings:
#   1. 30s read timeout (hardcoded in core/tb_api.py)
#   2. limit=1000 rows per response — DATE,DEMAND_PARTNER returns ~438
#      rows for a single day, so anything above 2 days clips silently.
# Chunk by 1 day to stay under both. Trade-off: backfill of N days costs
# 2N round-trips at ~17s each. 30-day backfill ≈ 17 min — acceptable for
# a one-off; hourly runs only do today + yesterday (2 chunks × 2 = 4 hits).
CHUNK_DAYS = 1

_DATE_KEYS = ("DATE", "date")
_PUB_ID_KEYS   = ("PUBLISHER", "PUBLISHER_ID", "publisher")
_PUB_NAME_KEYS = ("PUBLISHER_NAME", "publisher_name")
_DMD_ID_KEYS   = ("DEMAND_PARTNER", "DEMAND_PARTNER_ID", "demand_partner")
_DMD_NAME_KEYS = ("DEMAND_PARTNER_NAME", "demand_partner_name")


def _first(row: dict, keys: tuple) -> str:
    for k in keys:
        v = row.get(k)
        if v not in (None, ""):
            return str(v)
    return ""


def _aggregate(rows: Iterable[dict], dim: str) -> list[dict]:
    """Collapse rows keyed by (report_date, dim_id) into UPSERT-ready records.

    `dim` is "publisher" or "demand"; controls which ID/name keys we
    pluck out and which fields land in the output dict.
    """
    if dim == "publisher":
        id_keys, name_keys, id_col, name_col = _PUB_ID_KEYS, _PUB_NAME_KEYS, "publisher_id", "publisher_name"
    else:
        id_keys, name_keys, id_col, name_col = _DMD_ID_KEYS, _DMD_NAME_KEYS, "demand_id", "demand_name"

    grouped: dict[tuple, dict] = defaultdict(lambda: {
        "impressions": 0.0, "bids": 0.0, "wins": 0.0,
        "gross_revenue": 0.0, "pub_payout": 0.0,
    })
    meta: dict[tuple, str] = {}

    for row in rows:
        report_date = _first(row, _DATE_KEYS)
        dim_id      = _first(row, id_keys)
        dim_name    = _first(row, name_keys) or dim_id
        # Skip totals rows where the dim wasn't broken out (TB occasionally
        # returns a grand-total row alongside the dimensioned ones).
        if not (report_date and dim_id):
            continue
        key = (report_date, dim_id)
        agg = grouped[key]
        agg["impressions"]   += sf(row.get("IMPRESSIONS"))
        agg["bids"]          += sf(row.get("BIDS"))
        agg["wins"]          += sf(row.get("WINS"))
        agg["gross_revenue"] += sf(row.get("GROSS_REVENUE"))
        agg["pub_payout"]    += sf(row.get("PUB_PAYOUT"))
        meta[key] = dim_name

    return [
        {
            "report_date":  k[0],
            id_col:         k[1],
            name_col:       meta[k],
            "impressions":  int(v["impressions"]),
            "bids":         int(v["bids"]),
            "wins":         int(v["wins"]),
            "gross_revenue":round(v["gross_revenue"], 4),
            "pub_payout":   round(v["pub_payout"], 4),
        }
        for k, v in grouped.items()
    ]


_PUB_UPSERT = """
INSERT INTO pgam_direct.tb_daily_publisher_revenue
  (report_date, publisher_id, publisher_name,
   impressions, bids, wins, gross_revenue, pub_payout, updated_at)
VALUES
  (%(report_date)s, %(publisher_id)s, %(publisher_name)s,
   %(impressions)s, %(bids)s, %(wins)s,
   %(gross_revenue)s, %(pub_payout)s, now())
ON CONFLICT (report_date, publisher_id) DO UPDATE SET
  publisher_name = EXCLUDED.publisher_name,
  impressions    = EXCLUDED.impressions,
  bids           = EXCLUDED.bids,
  wins           = EXCLUDED.wins,
  gross_revenue  = EXCLUDED.gross_revenue,
  pub_payout     = EXCLUDED.pub_payout,
  updated_at     = now();
"""

_DMD_UPSERT = """
INSERT INTO pgam_direct.tb_daily_demand_revenue
  (report_date, demand_id, demand_name,
   impressions, bids, wins, gross_revenue, pub_payout, updated_at)
VALUES
  (%(report_date)s, %(demand_id)s, %(demand_name)s,
   %(impressions)s, %(bids)s, %(wins)s,
   %(gross_revenue)s, %(pub_payout)s, now())
ON CONFLICT (report_date, demand_id) DO UPDATE SET
  demand_name    = EXCLUDED.demand_name,
  impressions    = EXCLUDED.impressions,
  bids           = EXCLUDED.bids,
  wins           = EXCLUDED.wins,
  gross_revenue  = EXCLUDED.gross_revenue,
  pub_payout     = EXCLUDED.pub_payout,
  updated_at     = now();
"""


def _upsert(sql: str, records: list[dict]) -> int:
    if not records:
        return 0
    with connect() as conn:
        with conn.cursor() as cur:
            cur.executemany(sql, records)
        conn.commit()
    return len(records)


def _date_chunks(start: str, end: str, chunk_days: int) -> list[tuple[str, str]]:
    """Yield (chunk_start, chunk_end) ISO pairs covering [start, end]
    inclusive in `chunk_days` slices. Inclusive on both sides — the API
    treats endDate as inclusive."""
    s = datetime.strptime(start, "%Y-%m-%d").date()
    e = datetime.strptime(end, "%Y-%m-%d").date()
    out: list[tuple[str, str]] = []
    cur = s
    while cur <= e:
        chunk_end = min(cur + timedelta(days=chunk_days - 1), e)
        out.append((cur.isoformat(), chunk_end.isoformat()))
        cur = chunk_end + timedelta(days=1)
    return out


def _fetch_chunked(breakdown: str, start: str, end: str) -> tuple[list[dict], list[str]]:
    """Fetch a date range in CHUNK_DAYS slices. Returns (rows, skipped_dates).

    Each chunk retries once on read-timeout. On the second failure we log
    and skip — partial-success > all-or-nothing. Caller can re-run the
    agent later to fill the skipped slices.
    """
    all_rows: list[dict] = []
    skipped: list[str] = []
    for cs, ce in _date_chunks(start, end, CHUNK_DAYS):
        chunk: list[dict] | None = None
        for attempt in (1, 2):
            try:
                chunk = fetch_tb(breakdown, METRICS, cs, ce)
                break
            except Exception as exc:
                if attempt == 2:
                    print(f"[tb_revenue_etl]   {breakdown} {cs}..{ce} SKIP after retry: {exc}")
                    skipped.append(cs)
                else:
                    print(f"[tb_revenue_etl]   {breakdown} {cs}..{ce} retry 1: {exc}")
        if chunk is not None:
            all_rows.extend(chunk)
            print(f"[tb_revenue_etl]   {breakdown} {cs}..{ce} -> {len(chunk)} rows")
    return all_rows, skipped


def run(window_days: int = WINDOW_DAYS) -> dict:
    """Pull TB DATE,PUBLISHER and DATE,DEMAND_PARTNER breakdowns and UPSERT.

    For windows longer than CHUNK_DAYS, fetches are split into smaller
    sub-ranges to stay under the TB API's timeout.
    """
    end_date = today()
    start_date = n_days_ago(max(window_days - 1, 0))
    print(
        f"[tb_revenue_etl] Fetching TB {start_date}..{end_date} "
        f"({window_days}d window, chunk={CHUNK_DAYS}d)"
    )

    # 1. Publisher breakdown
    pub_rows, pub_skipped = _fetch_chunked("DATE,PUBLISHER", start_date, end_date)
    pub_records = _aggregate(pub_rows, "publisher")
    print(f"[tb_revenue_etl] DATE,PUBLISHER total: {len(pub_rows)} rows -> {len(pub_records)} unique"
          + (f" ({len(pub_skipped)} dates skipped)" if pub_skipped else ""))

    # 2. Demand breakdown
    dmd_rows, dmd_skipped = _fetch_chunked("DATE,DEMAND_PARTNER", start_date, end_date)
    dmd_records = _aggregate(dmd_rows, "demand")
    print(f"[tb_revenue_etl] DATE,DEMAND_PARTNER total: {len(dmd_rows)} rows -> {len(dmd_records)} unique"
          + (f" ({len(dmd_skipped)} dates skipped)" if dmd_skipped else ""))

    # 3. UPSERT both tables
    try:
        n_pub = _upsert(_PUB_UPSERT, pub_records)
        n_dmd = _upsert(_DMD_UPSERT, dmd_records)
    except Exception as exc:
        print(f"[tb_revenue_etl] Neon UPSERT failed: {exc}")
        return {"ok": False, "error": str(exc)}

    yest = yesterday()
    yest_pub_revenue = sum(r["gross_revenue"] for r in pub_records if r["report_date"] == yest)
    print(
        f"[tb_revenue_etl] Upserted {n_pub} publisher rows + {n_dmd} demand rows. "
        f"Yesterday ({yest}) gross_revenue=${yest_pub_revenue:,.2f}"
    )
    return {
        "ok": True,
        "publisher_rows": n_pub,
        "demand_rows": n_dmd,
        "yesterday_gross_revenue": yest_pub_revenue,
        "skipped_publisher_dates": pub_skipped,
        "skipped_demand_dates": dmd_skipped,
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Land TB daily revenue into Neon")
    parser.add_argument(
        "--backfill", type=int, default=None,
        help="Override window: pull this many trailing days (default: 2 = today + yesterday)",
    )
    args = parser.parse_args()
    result = run(window_days=args.backfill or WINDOW_DAYS)
    sys.exit(0 if result.get("ok") else 1)
