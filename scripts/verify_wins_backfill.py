"""
scripts/verify_wins_backfill.py

Verify the WINS backfill workaround in core.ll_report._sanitize_rows().

Pulls a fresh report, counts how many rows got patched, validates that
healthy rows are untouched, and prints the new aggregate WINS for Start.IO.

Run:
    python3 scripts/verify_wins_backfill.py
"""
import os, sys
from datetime import date, timedelta

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)
from dotenv import load_dotenv
load_dotenv(dotenv_path=os.path.join(_ROOT, ".env"), override=True)

import core.ll_report as llr


def _sf(v):
    try: return float(v)
    except: return 0.0


def main():
    end = (date.today() - timedelta(days=1)).strftime("%Y-%m-%d")
    start = (date.today() - timedelta(days=7)).strftime("%Y-%m-%d")

    rows = llr.report(
        ["DEMAND_ID", "DEMAND_NAME"],
        llr.FUNNEL_METRICS,
        start, end,
    )

    patched = [r for r in rows if r.get("_WINS_BACKFILLED")]
    healthy = [r for r in rows if not r.get("_WINS_BACKFILLED")]

    print(f"Total demand rows: {len(rows)}")
    print(f"  Patched (WINS backfilled from IMPRESSIONS): {len(patched)}")
    print(f"  Healthy (untouched):                        {len(healthy)}")

    # Sanity: every healthy row should have WINS >= IMPRESSIONS (the rule)
    violations = [r for r in healthy
                  if _sf(r.get("WINS")) < _sf(r.get("IMPRESSIONS"))
                  and _sf(r.get("IMPRESSIONS")) > 0]
    print(f"  Healthy-row sanity violations (WINS < IMPS): {len(violations)}")
    if violations:
        print("    First 3:")
        for v in violations[:3]:
            print(f"    {v.get('DEMAND_NAME')!r}  WINS={v.get('WINS')}  IMPS={v.get('IMPRESSIONS')}")

    # Start.IO aggregate after patch
    startio = [r for r in rows if "start" in str(r.get("DEMAND_NAME","")).lower()]
    s_wins = sum(_sf(r.get("WINS")) for r in startio)
    s_bids = sum(_sf(r.get("BIDS")) for r in startio)
    s_imps = sum(_sf(r.get("IMPRESSIONS")) for r in startio)
    s_rev = sum(_sf(r.get("GROSS_REVENUE")) for r in startio)
    s_patched = sum(1 for r in startio if r.get("_WINS_BACKFILLED"))
    wr = (s_wins / s_bids * 100) if s_bids else 0.0
    print(f"\nStart.IO aggregate (after patch):")
    print(f"  entries:        {len(startio)}  ({s_patched} patched)")
    print(f"  bids:           {s_bids:>14,.0f}")
    print(f"  wins (post-fix):{s_wins:>14,.0f}    ← was 0 before")
    print(f"  impressions:    {s_imps:>14,.0f}")
    print(f"  revenue:        ${s_rev:>13,.2f}")
    print(f"  win_rate:       {wr:>14.3f}%   ← downstream agents will see this")

    # Show top 5 patched non-Start.IO rows so we know what else got fixed
    other_patched = [r for r in patched
                     if "start" not in str(r.get("DEMAND_NAME","")).lower()]
    other_patched.sort(key=lambda r: -_sf(r.get("GROSS_REVENUE", 0)))
    print(f"\nOther partners that benefited from the fix ({len(other_patched)} entries):")
    for r in other_patched[:8]:
        print(f"  {str(r.get('DEMAND_NAME',''))[:50]:<50}  "
              f"wins+={_sf(r.get('WINS')):>10,.0f}  "
              f"rev=${_sf(r.get('GROSS_REVENUE')):>8,.2f}")


if __name__ == "__main__":
    main()
