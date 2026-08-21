#!/usr/bin/env python3
"""Attribute a revenue change to the partners, DSPs, geos and devices behind it.

Built for the 2026-08-11 eCPM cliff, but written to be reusable: this is the
query a regression alert has to be able to answer. "Revenue is down 43%" is not
actionable; "these three endpoints account for 80% of it" is.

Method: split the window at a pivot date, aggregate each dimension either side,
and rank by absolute dollar delta. The share-of-change column is the useful one
— it says how much of the total move each row explains, so you can stop reading
once it adds up.

Read-only: SELECTs inside a READ ONLY transaction.

Usage
-----
    # 14 days either side of the pivot
    python3 scripts/tb_whatchanged.py --pivot 2026-08-11 --days 14

    # explicit windows
    python3 scripts/tb_whatchanged.py \
        --before-from 2026-07-20 --before-to 2026-08-02 \
        --after-from  2026-08-05 --after-to  2026-08-18
"""

from __future__ import annotations

import argparse
import os
import sys
from datetime import date, datetime, timedelta

sys.path.insert(0, __file__.rsplit("/scripts/", 1)[0])


def heading(text: str) -> None:
    print(f"\n{'═' * 78}\n{text}\n{'═' * 78}")


def fmt(v: object) -> str:
    if v is None:
        return "-"
    if isinstance(v, float):
        return f"{v:,.2f}"
    if isinstance(v, int):
        return f"{v:,}"
    t = str(v)
    return t if len(t) <= 30 else t[:29] + "…"


def table(rows: list[tuple], cols: list[str], limit: int = 20) -> None:
    if not rows:
        print("  (no rows)")
        return
    w = [max(len(cols[i]), *(len(fmt(r[i])) for r in rows[:limit])) for i in range(len(cols))]
    print("  " + "  ".join(c.ljust(w[i]) for i, c in enumerate(cols)))
    print("  " + "  ".join("─" * x for x in w))
    for r in rows[:limit]:
        print("  " + "  ".join(fmt(r[i]).ljust(w[i]) for i in range(len(cols))))
    if len(rows) > limit:
        print(f"  … {len(rows) - limit:,} more")


# One query shape, parameterised by dimension — every table here has the same
# report_date / <label> / impressions / gross_revenue columns.
_DIFF = """
WITH before AS (
    SELECT {label} AS k,
           sum(impressions)::bigint AS imps,
           sum({revenue})::numeric  AS gross
    FROM pgam_direct.{table}
    WHERE report_date BETWEEN %(bf)s AND %(bt)s
    GROUP BY {label}
), after AS (
    SELECT {label} AS k,
           sum(impressions)::bigint AS imps,
           sum({revenue})::numeric  AS gross
    FROM pgam_direct.{table}
    WHERE report_date BETWEEN %(af)s AND %(at)s
    GROUP BY {label}
)
SELECT COALESCE(b.k, a.k),
       COALESCE(b.gross, 0)::numeric(14,2),
       COALESCE(a.gross, 0)::numeric(14,2),
       (COALESCE(a.gross, 0) - COALESCE(b.gross, 0))::numeric(14,2),
       CASE WHEN COALESCE(b.gross, 0) > 0
            THEN round(100.0 * (COALESCE(a.gross, 0) - b.gross) / b.gross, 1) END,
       COALESCE(b.imps, 0), COALESCE(a.imps, 0),
       CASE WHEN COALESCE(b.imps, 0) > 0
            THEN round((1000.0 * b.gross / b.imps)::numeric, 3) END,
       CASE WHEN COALESCE(a.imps, 0) > 0
            THEN round((1000.0 * a.gross / a.imps)::numeric, 3) END
FROM before b FULL OUTER JOIN after a ON a.k = b.k
ORDER BY (COALESCE(a.gross, 0) - COALESCE(b.gross, 0)) ASC
"""


def diff(conn, title: str, tbl: str, label: str, revenue: str,
         windows: dict, limit: int = 15) -> None:
    heading(title)
    sql = _DIFF.format(table=tbl, label=label, revenue=revenue)
    try:
        with conn.cursor() as cur:
            cur.execute(sql, windows)
            rows = cur.fetchall()
    except Exception as exc:
        conn.rollback()
        print(f"  ✗ {str(exc).splitlines()[0][:200]}")
        return
    if not rows:
        print("  (no rows)")
        return

    total_delta = sum(float(r[3] or 0) for r in rows)
    print(f"  net change across all rows: ${total_delta:,.2f}")

    def annotate(subset):
        out = []
        for k, gb, ga, d, pct, ib, ia, eb, ea in subset:
            share = (100 * float(d) / total_delta) if total_delta else 0
            out.append((k, float(gb), float(ga), float(d), pct,
                        int(ib), int(ia), eb, ea, round(share, 1)))
        return out

    cols = ["name", "gross_before", "gross_after", "delta", "chg_%",
            "imps_before", "imps_after", "eCPM_before", "eCPM_after", "share_%"]

    losers = [r for r in rows if float(r[3] or 0) < 0]
    print(f"\n  ↓ biggest decliners ({len(losers)} rows fell)")
    table(annotate(losers[:limit]), cols, limit=limit)

    gainers = sorted([r for r in rows if float(r[3] or 0) > 0],
                     key=lambda r: -float(r[3]))
    if gainers:
        print(f"\n  ↑ biggest gainers ({len(gainers)} rows rose)")
        table(annotate(gainers[:8]), cols, limit=8)

    # How concentrated is the move? If three rows explain most of it, that is
    # the answer; if it takes thirty, the cause is systemic.
    if total_delta < 0 and losers:
        run = 0.0
        for n, r in enumerate(losers, 1):
            run += float(r[3])
            if run <= 0.8 * total_delta:
                print(f"\n  {n} row(s) explain 80% of the decline.")
                break
        else:
            print(f"\n  no small subset explains 80% — the decline is spread "
                  f"across {len(losers)} rows, which points at something "
                  f"systemic rather than one endpoint dying.")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--pivot", help="split date (YYYY-MM-DD)")
    ap.add_argument("--days", type=int, default=14,
                    help="days either side of the pivot (default 14)")
    ap.add_argument("--before-from")
    ap.add_argument("--before-to")
    ap.add_argument("--after-from")
    ap.add_argument("--after-to")
    ap.add_argument("--limit", type=int, default=15)
    a = ap.parse_args()

    if a.before_from and a.before_to and a.after_from and a.after_to:
        bf, bt, af, at = a.before_from, a.before_to, a.after_from, a.after_to
    elif a.pivot:
        piv = datetime.strptime(a.pivot, "%Y-%m-%d").date()
        bf = (piv - timedelta(days=a.days)).isoformat()
        bt = (piv - timedelta(days=1)).isoformat()
        af = piv.isoformat()
        at = min(piv + timedelta(days=a.days - 1), date.today() - timedelta(days=1)).isoformat()
    else:
        ap.error("give --pivot, or all four explicit window dates")

    if not (os.environ.get("PGAM_DIRECT_DATABASE_URL") or os.environ.get("DATABASE_URL")):
        print("No Neon DSN set.", file=sys.stderr)
        return 2

    from core.neon import connect

    print("Revenue change attribution")
    print(f"  before  {bf} → {bt}")
    print(f"  after   {af} → {at}")
    print("  ranked by absolute gross delta; share_% is how much of the total")
    print("  move each row explains")

    w = {"bf": bf, "bt": bt, "af": af, "at": at}
    conn = connect()
    try:
        with conn.cursor() as cur:
            cur.execute("SET TRANSACTION READ ONLY")
        diff(conn, "BY PARTNER", "tb_daily_publisher_revenue",
             "publisher_name", "gross_revenue", w, a.limit)
        diff(conn, "BY DEMAND SOURCE", "tb_daily_demand_revenue",
             "demand_name", "gross_revenue", w, a.limit)
        diff(conn, "BY COUNTRY", "tb_daily_country_revenue",
             "country", "gross_revenue", w, a.limit)
        diff(conn, "BY OS / DEVICE", "tb_daily_os",
             "os", "gross_revenue", w, a.limit)
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
