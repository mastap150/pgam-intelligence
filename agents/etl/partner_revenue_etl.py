"""
agents/etl/partner_revenue_etl.py

Hourly ETL that lands Limelight (LL) daily revenue totals into Neon
(`pgam_direct.ll_daily_partner_revenue`) for the Partner Revenue
Dashboard at admin.pgammedia.com/admin/partner-revenue.

Why this exists
---------------
The dashboard merges two demand sources: TB (TrueBlue/Prebid, already
in `pgam_direct.financial_events` because pgam-direct's bidder writes
there in real time) and LL (Limelight, a separate platform fronted by
stats.ortb.net). LL has no presence in Neon, so we ETL its daily
publisher x demand rollup here.

Cadence + window
----------------
- Scheduler fires `run()` every 60 minutes (see scheduler.py).
- Each run UPSERTs the trailing `WINDOW_DAYS` (default 2 — today +
  yesterday). Yesterday is included because LL's intraday counters
  reset after 8 PM ET; the morning re-pull is what lands the final
  number for the prior day.
- For initial load / gap recovery, call `run(window_days=30)` once
  (or `python -m agents.etl.partner_revenue_etl --backfill 30`).

Schema (PK = report_date, publisher_id, demand_id)
-------------------------------------------------
report_date, publisher_id, publisher_name, demand_id, demand_name,
impressions, bids, wins, gross_revenue, pub_payout, updated_at.

WINS event-drop bug: core.api.fetch already runs _patch_zero_wins_rows
to backfill WINS = IMPRESSIONS for affected adapters (Start.IO et al).
We persist the patched value because every downstream consumer of the
table is treating WINS as a count of paid auctions, and a 0 there
would skew win-rate / eCPM.
"""

import argparse
import sys
from collections import defaultdict
from typing import Iterable

from core.api import fetch, sf, n_days_ago, today, yesterday
from core.neon import connect

BREAKDOWN = "DATE,PUBLISHER,DEMAND_PARTNER"
METRICS = [
    "GROSS_REVENUE",
    "PUB_PAYOUT",
    "IMPRESSIONS",
    "WINS",
    "BIDS",
]
WINDOW_DAYS = 2  # incremental: today + yesterday

# Possible LL field aliases (the API has been seen returning either
# casing/underscore variant depending on the dimension combination)
_PUB_ID_KEYS    = ("PUBLISHER_ID", "PUBLISHER", "publisher_id", "publisher")
_PUB_NAME_KEYS  = ("PUBLISHER_NAME", "publisher_name")
_DMD_ID_KEYS    = ("DEMAND_PARTNER_ID", "DEMAND_ID", "DEMAND_PARTNER", "demand_id")
_DMD_NAME_KEYS  = ("DEMAND_PARTNER_NAME", "DEMAND_NAME", "demand_partner_name", "demand_name")
_DATE_KEYS      = ("DATE", "date")


def _first(row: dict, keys: tuple) -> str:
    for k in keys:
        v = row.get(k)
        if v not in (None, ""):
            return str(v)
    return ""


def _normalize(rows: Iterable[dict]) -> list[dict]:
    """Collapse LL rows into UPSERT-ready records keyed by (date, pub, demand).

    LL occasionally splits the same logical row across multiple records
    (different sub-dimensions we didn't ask for); aggregating defensively
    means we always have one row per PK before sending to Postgres.
    """
    grouped: dict[tuple, dict] = defaultdict(lambda: {
        "impressions": 0.0, "bids": 0.0, "wins": 0.0,
        "gross_revenue": 0.0, "pub_payout": 0.0,
    })
    meta: dict[tuple, dict] = {}

    for row in rows:
        report_date    = _first(row, _DATE_KEYS)
        publisher_id   = _first(row, _PUB_ID_KEYS)
        publisher_name = _first(row, _PUB_NAME_KEYS) or publisher_id
        demand_id      = _first(row, _DMD_ID_KEYS)
        demand_name    = _first(row, _DMD_NAME_KEYS) or demand_id

        # A row missing any of date/publisher/demand can't be UPSERTed
        # (composite PK would be ambiguous). Skip silently — typically
        # an API "totals" row.
        if not (report_date and publisher_id and demand_id):
            continue

        key = (report_date, publisher_id, demand_id)
        agg = grouped[key]
        agg["impressions"]   += sf(row.get("IMPRESSIONS"))
        agg["bids"]          += sf(row.get("BIDS"))
        agg["wins"]          += sf(row.get("WINS"))
        agg["gross_revenue"] += sf(row.get("GROSS_REVENUE"))
        agg["pub_payout"]    += sf(row.get("PUB_PAYOUT"))

        meta[key] = {
            "publisher_name": publisher_name,
            "demand_name":    demand_name,
        }

    return [
        {
            "report_date":    k[0],
            "publisher_id":   k[1],
            "publisher_name": meta[k]["publisher_name"],
            "demand_id":      k[2],
            "demand_name":    meta[k]["demand_name"],
            "impressions":    int(v["impressions"]),
            "bids":           int(v["bids"]),
            "wins":           int(v["wins"]),
            "gross_revenue":  round(v["gross_revenue"], 4),
            "pub_payout":     round(v["pub_payout"], 4),
        }
        for k, v in grouped.items()
    ]


_UPSERT_SQL = """
INSERT INTO pgam_direct.ll_daily_partner_revenue
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


def _upsert(records: list[dict]) -> int:
    if not records:
        return 0
    with connect() as conn:
        with conn.cursor() as cur:
            cur.executemany(_UPSERT_SQL, records)
        conn.commit()
    return len(records)


def run(window_days: int = WINDOW_DAYS) -> dict:
    """Pull `window_days` of LL daily data and UPSERT into Neon.

    Returns a small status dict — useful for the scheduler log.
    """
    end_date = today()
    start_date = n_days_ago(max(window_days - 1, 0))

    print(
        f"[partner_revenue_etl] Fetching LL {BREAKDOWN} "
        f"{start_date}..{end_date} ({window_days}d window)"
    )
    try:
        rows = fetch(BREAKDOWN, METRICS, start_date, end_date)
    except Exception as exc:
        print(f"[partner_revenue_etl] LL fetch failed: {exc}")
        return {"ok": False, "error": str(exc)}

    records = _normalize(rows)
    print(f"[partner_revenue_etl] Normalized {len(rows)} rows -> {len(records)} unique (date,pub,demand)")

    try:
        n = _upsert(records)
    except Exception as exc:
        print(f"[partner_revenue_etl] Neon UPSERT failed: {exc}")
        return {"ok": False, "error": str(exc), "records": len(records)}

    # One-line summary to mirror other agents' log style
    yest = yesterday()
    yest_revenue = sum(r["gross_revenue"] for r in records if r["report_date"] == yest)
    print(
        f"[partner_revenue_etl] Upserted {n} rows. "
        f"Yesterday ({yest}) gross_revenue=${yest_revenue:,.2f}"
    )
    return {"ok": True, "rows_upserted": n, "yesterday_gross_revenue": yest_revenue}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Land LL daily revenue into Neon")
    parser.add_argument(
        "--backfill", type=int, default=None,
        help="Override window: pull this many trailing days (default: 2 = today + yesterday)",
    )
    args = parser.parse_args()
    result = run(window_days=args.backfill or WINDOW_DAYS)
    sys.exit(0 if result.get("ok") else 1)
