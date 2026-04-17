"""
scripts/diagnose_startio_wins.py

Probe the LL API with several dimension/filter combinations to locate where
Start.IO wins drop to zero. Run:
    python3 scripts/diagnose_startio_wins.py
"""
import os
import sys
from datetime import date, timedelta

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from dotenv import load_dotenv
load_dotenv(dotenv_path=os.path.join(_ROOT, ".env"), override=True)

import core.ll_report as llr
from core.api import fetch


def _days(n):
    end = date.today() - timedelta(days=1)
    start = end - timedelta(days=n - 1)
    return start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d")


def _find_startio(rows, name_field, id_field=None):
    out = []
    for r in rows:
        name = str(r.get(name_field, "")).lower()
        if "start" in name and ("io" in name or "app" in name):
            out.append(r)
    return out


def main():
    start, end = _days(7)
    print(f"\n=== Date range: {start} → {end} ===\n")

    # -------------------------------------------------------------------
    # Probe 1: GET /v1/stats — DEMAND_PARTNER breakdown (date-accurate)
    # -------------------------------------------------------------------
    print("### Probe 1: GET /v1/stats, breakdown=DEMAND_PARTNER")
    try:
        rows = fetch(
            "DEMAND_PARTNER",
            "GROSS_REVENUE,PUB_PAYOUT,IMPRESSIONS,WINS,BIDS,BID_REQUESTS,OPPORTUNITIES",
            start, end,
        )
        print(f"  total rows: {len(rows)}")
        hits = _find_startio(rows, "DEMAND_PARTNER_NAME") or _find_startio(rows, "DEMAND_PARTNER")
        if not hits:
            print("  NO Start.IO rows found — check dimension keys:")
            if rows:
                print(f"  sample keys: {list(rows[0].keys())}")
        for r in hits:
            print(f"  {r}")
    except Exception as e:
        print(f"  ERROR: {e}")

    # -------------------------------------------------------------------
    # Probe 2: GET /v1/stats — PUBLISHER,DEMAND_PARTNER
    # -------------------------------------------------------------------
    print("\n### Probe 2: GET /v1/stats, breakdown=PUBLISHER,DEMAND_PARTNER")
    try:
        rows = fetch(
            "PUBLISHER,DEMAND_PARTNER",
            "GROSS_REVENUE,PUB_PAYOUT,IMPRESSIONS,WINS,BIDS,BID_REQUESTS,OPPORTUNITIES",
            start, end,
        )
        print(f"  total rows: {len(rows)}")
        hits = _find_startio(rows, "DEMAND_PARTNER_NAME") or _find_startio(rows, "DEMAND_PARTNER")
        print(f"  Start.IO-matching rows: {len(hits)}")
        total_wins = sum(float(r.get("WINS", 0) or 0) for r in hits)
        total_rev = sum(float(r.get("GROSS_REVENUE", 0) or 0) for r in hits)
        print(f"  Σ WINS: {total_wins:,.0f}   Σ GROSS_REVENUE: ${total_rev:,.2f}")
        for r in hits[:10]:
            print(f"  {r.get('PUBLISHER_NAME', '')!r:40s}  "
                  f"{r.get('DEMAND_PARTNER_NAME', r.get('DEMAND_PARTNER', ''))!r:30s}  "
                  f"wins={r.get('WINS')}  rev=${r.get('GROSS_REVENUE')}")
    except Exception as e:
        print(f"  ERROR: {e}")

    # -------------------------------------------------------------------
    # Probe 3: POST /v1/report — DEMAND_ID / DEMAND_NAME (no date filter)
    # -------------------------------------------------------------------
    print("\n### Probe 3: POST /v1/report, dims=[DEMAND_ID,DEMAND_NAME] (all-time)")
    try:
        rows = llr.report(
            ["DEMAND_ID", "DEMAND_NAME"],
            llr.FUNNEL_METRICS,
            start, end,
        )
        print(f"  total rows: {len(rows)}")
        hits = _find_startio(rows, "DEMAND_NAME")
        print(f"  Start.IO-matching rows: {len(hits)}")
        for r in hits[:10]:
            print(f"  id={r.get('DEMAND_ID')!r:8s}  name={r.get('DEMAND_NAME')!r:40s}  "
                  f"wins={r.get('WINS')}  bids={r.get('BIDS')}  rev=${r.get('GROSS_REVENUE')}")
    except Exception as e:
        print(f"  ERROR: {e}")

    # -------------------------------------------------------------------
    # Probe 4: POST /v1/report — PUBLISHER x DEMAND_NAME, filter LIKE Start
    # -------------------------------------------------------------------
    print("\n### Probe 4: POST /v1/report, filter DEMAND_NAME LIKE Start")
    try:
        rows = llr.report(
            ["PUBLISHER_NAME", "DEMAND_ID", "DEMAND_NAME"],
            llr.FUNNEL_METRICS,
            start, end,
            filters=[{"dimension": "DEMAND_NAME", "type": "LIKE", "value": "Start"}],
        )
        print(f"  total rows: {len(rows)}")
        for r in rows[:20]:
            print(f"  pub={r.get('PUBLISHER_NAME')!r:35s}  "
                  f"dem={r.get('DEMAND_NAME')!r:30s}  "
                  f"wins={r.get('WINS')}  bids={r.get('BIDS')}  "
                  f"imps={r.get('IMPRESSIONS')}  rev=${r.get('GROSS_REVENUE')}")
    except Exception as e:
        print(f"  ERROR: {e}")

    # -------------------------------------------------------------------
    # Probe 5: compare wins across dimension granularities for one pub
    # -------------------------------------------------------------------
    print("\n### Probe 5: sum WINS by dim granularity (cross-check)")
    try:
        dims_sets = [
            ["DEMAND_NAME"],
            ["PUBLISHER_NAME", "DEMAND_NAME"],
            ["PUBLISHER_NAME", "DEMAND_NAME", "COUNTRY"],
            ["PUBLISHER_NAME", "DEMAND_NAME", "BUNDLE"],
        ]
        for dims in dims_sets:
            rows = llr.report(dims, ["WINS", "BIDS", "IMPRESSIONS", "GROSS_REVENUE"], start, end,
                              filters=[{"dimension": "DEMAND_NAME", "type": "LIKE", "value": "Start"}])
            total_wins = sum(float(r.get("WINS", 0) or 0) for r in rows)
            total_bids = sum(float(r.get("BIDS", 0) or 0) for r in rows)
            total_imps = sum(float(r.get("IMPRESSIONS", 0) or 0) for r in rows)
            total_rev = sum(float(r.get("GROSS_REVENUE", 0) or 0) for r in rows)
            print(f"  dims={dims}  rows={len(rows):4d}  "
                  f"wins={total_wins:>10,.0f}  bids={total_bids:>12,.0f}  "
                  f"imps={total_imps:>10,.0f}  rev=${total_rev:>10,.2f}")
    except Exception as e:
        print(f"  ERROR: {e}")


if __name__ == "__main__":
    main()
