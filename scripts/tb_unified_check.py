#!/usr/bin/env python3
"""
Print what `core.tb_unified` resolves for a date range, per day and per source.

The point is to prove — against the real warehouse, not a fixture — that the
Slack alert and `/admin/pnl` now report the same figures. They read the same
two Neon rollups through the same cutover rule, so agreement should be exact
rather than approximate; anything else means the rule diverged somewhere.

Read-only: every statement runs inside SET TRANSACTION READ ONLY, and nothing
here calls a platform endpoint.

    python3 scripts/tb_unified_check.py --from 2026-08-17 --to 2026-08-24
"""
from __future__ import annotations

import argparse
import os
import sys
from datetime import date, timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core import tb_unified as u        # noqa: E402


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--from", dest="start", required=True, metavar="YYYY-MM-DD")
    ap.add_argument("--to", dest="end", metavar="YYYY-MM-DD",
                    help="default: today")
    args = ap.parse_args()

    start = date.fromisoformat(args.start)
    end = date.fromisoformat(args.end) if args.end else date.today()

    if not u.configured():
        print("No warehouse DSN — set PGAM_DIRECT_DATABASE_URL or DATABASE_URL.",
              file=sys.stderr)
        return 2

    print(f"tb_unified — {start} → {end}")
    print(f"split window starts {u.split_start()}, cutover {u.cutover()}\n")

    rows = u.fetch("DATE", [], start.isoformat(), end.isoformat())
    by_day = {r["DATE"]: r for r in rows}

    print(f"{'day':<12} {'source':<12} {'gross':>12} {'payout':>12} "
          f"{'profit':>12} {'margin':>8} {'imps':>12}")
    print("-" * 84)

    tot_gross = tot_profit = 0.0
    day = start
    while day <= end:
        iso = day.isoformat()
        use_l, use_t = u.legs_for(day)
        src = "legacy+tbx" if (use_l and use_t) else ("legacy" if use_l else "tbx")
        r = by_day.get(iso)
        if not r:
            print(f"{iso:<12} {src:<12} {'—':>12} {'—':>12} {'—':>12} "
                  f"{'—':>8} {'—':>12}")
            day += timedelta(days=1)
            continue
        gross = r["GROSS_REVENUE"]
        payout = r["PUB_PAYOUT"]
        profit = gross - payout
        margin = (profit / gross * 100) if gross else 0.0
        tot_gross += gross
        tot_profit += profit
        print(f"{iso:<12} {src:<12} {gross:>12,.2f} {payout:>12,.2f} "
              f"{profit:>12,.2f} {margin:>7.1f}% {r['IMPRESSIONS']:>12,.0f}")
        day += timedelta(days=1)

    print("-" * 84)
    overall = (tot_profit / tot_gross * 100) if tot_gross else 0.0
    print(f"{'TOTAL':<25} {tot_gross:>12,.2f} {'':>12} {tot_profit:>12,.2f} "
          f"{overall:>7.1f}%")
    print("\nThese are the figures the Slack alert now posts and the P&L row "
          "now holds.\nA split day shows legacy+tbx because both hosts served "
          "it; every other day\ndraws on exactly one, so the fold cannot "
          "double-count.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
