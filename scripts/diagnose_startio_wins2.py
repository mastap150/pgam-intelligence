"""
Follow-up probe — which metrics ARE populated for Start.IO entries?
If WINS=0 but IMPRESSIONS>0 we can derive win-rate from imps.
If BOTH are 0 but revenue>0, only revenue/ecpm are reliable.
"""
import os, sys
from datetime import date, timedelta

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)
from dotenv import load_dotenv
load_dotenv(dotenv_path=os.path.join(_ROOT, ".env"), override=True)

import core.ll_report as llr
from core.api import fetch


def _sf(v):
    try: return float(v)
    except: return 0.0


def main():
    # POST /v1/report is all-time regardless of date — that's fine for diagnosis
    end = (date.today() - timedelta(days=1)).strftime("%Y-%m-%d")
    start = (date.today() - timedelta(days=7)).strftime("%Y-%m-%d")

    print("### All Start.IO demand rows — full metric set (POST /v1/report is all-time)")
    rows = llr.report(
        ["DEMAND_ID", "DEMAND_NAME"],
        llr.FUNNEL_METRICS,
        start, end,
    )
    startio = [r for r in rows if "start.io" in str(r.get("DEMAND_NAME", "")).lower()
               or "startio" in str(r.get("DEMAND_NAME", "")).lower()]
    print(f"Found {len(startio)} Start.IO demand entries (all-time).\n")

    hdr = f"{'DEMAND_NAME':<45} {'BIDS':>10} {'WINS':>8} {'IMPS':>10} {'REV':>10} {'PUB_PAY':>10}"
    print(hdr); print("-" * len(hdr))
    agg = dict(BIDS=0, WINS=0, IMPRESSIONS=0, GROSS_REVENUE=0, PUB_PAYOUT=0,
               BID_REQUESTS=0, OPPORTUNITIES=0)
    for r in sorted(startio, key=lambda x: -_sf(x.get("GROSS_REVENUE", 0))):
        print(f"{str(r.get('DEMAND_NAME',''))[:44]:<45} "
              f"{_sf(r.get('BIDS')):>10,.0f} "
              f"{_sf(r.get('WINS')):>8,.0f} "
              f"{_sf(r.get('IMPRESSIONS')):>10,.0f} "
              f"${_sf(r.get('GROSS_REVENUE')):>9,.2f} "
              f"${_sf(r.get('PUB_PAYOUT')):>9,.2f}")
        for k in agg: agg[k] += _sf(r.get(k, 0))

    print("-" * len(hdr))
    print(f"{'TOTAL (all Start.IO entries, all-time)':<45} "
          f"{agg['BIDS']:>10,.0f} {agg['WINS']:>8,.0f} "
          f"{agg['IMPRESSIONS']:>10,.0f} ${agg['GROSS_REVENUE']:>9,.2f} "
          f"${agg['PUB_PAYOUT']:>9,.2f}")
    print(f"bid_requests={agg['BID_REQUESTS']:,.0f}  opportunities={agg['OPPORTUNITIES']:,.0f}")

    # Compare: sanity-check a healthy demand source (say Magnite-Magnite)
    print("\n### Control: top 5 non-Start.IO demand entries by revenue")
    non = [r for r in rows if "start" not in str(r.get("DEMAND_NAME","")).lower()]
    top5 = sorted(non, key=lambda x: -_sf(x.get("GROSS_REVENUE", 0)))[:5]
    print(hdr); print("-" * len(hdr))
    for r in top5:
        print(f"{str(r.get('DEMAND_NAME',''))[:44]:<45} "
              f"{_sf(r.get('BIDS')):>10,.0f} "
              f"{_sf(r.get('WINS')):>8,.0f} "
              f"{_sf(r.get('IMPRESSIONS')):>10,.0f} "
              f"${_sf(r.get('GROSS_REVENUE')):>9,.2f} "
              f"${_sf(r.get('PUB_PAYOUT')):>9,.2f}")

    # Check: how many demand entries globally have WINS==0 AND rev>0?
    print("\n### Global scan: entries with WINS=0 but GROSS_REVENUE > $1")
    weird = [r for r in rows
             if _sf(r.get("WINS", 0)) == 0 and _sf(r.get("GROSS_REVENUE", 0)) > 1]
    weird.sort(key=lambda x: -_sf(x.get("GROSS_REVENUE", 0)))
    print(f"Count: {len(weird)} (out of {len(rows)} total demand entries)")
    total_missing_rev = sum(_sf(r.get("GROSS_REVENUE", 0)) for r in weird)
    print(f"Total revenue on these 'zero-win' entries: ${total_missing_rev:,.2f}")
    startio_missing = sum(_sf(r.get("GROSS_REVENUE", 0)) for r in weird
                          if "start" in str(r.get("DEMAND_NAME","")).lower())
    print(f"  of which Start.IO: ${startio_missing:,.2f}")
    print("\nTop 15 zero-win-but-has-revenue entries:")
    for r in weird[:15]:
        print(f"  {str(r.get('DEMAND_NAME',''))[:50]:<50}  "
              f"bids={_sf(r.get('BIDS')):>10,.0f}  "
              f"rev=${_sf(r.get('GROSS_REVENUE')):>8,.2f}")


if __name__ == "__main__":
    main()
