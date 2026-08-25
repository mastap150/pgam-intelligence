#!/usr/bin/env python3
"""
Which supply stopped arriving at the Teqblaze cutover?

Impressions fell from ~9.4M/day on the legacy host to ~6.6M/day on TBX and
stayed there (`docs/tb-data-workflow-integration.md` §12). That is the largest
single component of the profit drop — 41% of it — and unlike the price and
margin components it has a recoverable cause: supply that was earning before
2026-08-21 and is earning nothing after it.

This names those publishers. It is deliberately NOT a revenue reconciliation:
`scripts/tbx_recon.py` already asks "do the two hosts agree about the days
they both served", which is a question about correctness. This asks "what did
one host carry that the other never picked up", which is a question about
inventory, and the two want opposite treatment of a missing row. In a recon a
one-sided partner is noise to exclude before comparing; here it is the entire
finding.

Joining
-------
On NAME, lowercased and trimmed — never on id. Teqblaze confirmed *placement*
ids are unchanged and explicitly did not cover publisher or supply-source ids
(§8.1.10d), and `tbx_recon`'s own demand check exists to measure that. A
publisher whose id was reassigned would show up here as one partner vanishing
and another appearing, which is exactly the false alarm this report must not
raise.

Names are not guaranteed identical across hosts either, so a name that matches
nothing is reported as *unmatched*, not as *lost*. The difference matters: the
first is a question for whoever knows the roster, the second is money. Where
the two hosts spell a partner differently this will overstate the gap, and
that is the safe direction — a false positive costs someone a look, a false
negative costs the inventory.

The window
----------
Two windows, both settled, either side of the cutover:

    before   [cutover - days, cutover)      legacy, the roster that was earning
    after    [cutover, today)               TBX, the roster that is earning now

`--days` sizes both. The default of 4 is not arbitrary: the legacy leg stops
on 2026-08-20 and 2026-08-17 is the first day with meaningful legacy data in
Neon, so a longer "before" window silently averages in days that do not exist
and understates every publisher's baseline.

A partner is GONE when it earned on the before-window and has no row at all
after. It is QUIET when it still has rows but impressions fell more than
`--quiet-pct` — a partner reduced to a trickle is the same problem arriving
more slowly, and it will not show up in any absence check.

Read-only: every statement runs inside `SET TRANSACTION READ ONLY`, matching
`tbx_recon.py` and `tb_freshness.py`.

Exit codes:
    0  nothing lost
    1  supply is missing (so a scheduled run fails visibly until it is fixed)
    2  could not run — missing DSN or table

Usage:
    python3 scripts/tbx_supply_gap.py
    python3 scripts/tbx_supply_gap.py --days 4 --quiet-pct 60
    python3 scripts/tbx_supply_gap.py --cutover 2026-08-21 --json
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import date, timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

LEGACY_SUPPLY = "tb_daily_publisher_revenue"
TBX_SUPPLY = "tbx_daily_supply_revenue"

CUTOVER_DEFAULT = "2026-08-21"

# Below this, a partner's "before" revenue is too small for its disappearance
# to be worth anyone's time. The report would otherwise be dominated by test
# endpoints and long-dead publishers earning cents.
MIN_BEFORE_REVENUE = 1.0

_HDR = "=" * 78


def _q(conn, sql: str, params=None) -> list[tuple]:
    with conn.cursor() as cur:
        cur.execute(sql, params or ())
        return cur.fetchall()


def _exists(conn, table: str) -> bool:
    return bool(_q(conn, """
        SELECT 1 FROM information_schema.tables
        WHERE table_schema = 'pgam_direct' AND table_name = %s
    """, (table,)))


def windows(cutover: date, days: int, today: date) -> tuple[list[date], list[date]]:
    """The settled days either side of the cutover.

    The 'after' window ends yesterday, never today: today is partial on the
    platform's US/Eastern clock, and comparing a part-day against a full one
    manufactures a gap out of nothing. That is the same mistake §12 records
    against 2026-08-24.
    """
    before = [cutover - timedelta(days=n) for n in range(days, 0, -1)]
    last_settled = today - timedelta(days=1)
    after = [d for d in (cutover + timedelta(days=n) for n in range(days))
             if d <= last_settled]
    return before, after


def collect(conn, table: str, id_col: str, name_col: str,
            days: list[date]) -> dict[str, dict]:
    """Per-partner totals over `days`, keyed on the normalised name."""
    if not days:
        return {}
    rows = _q(conn, f"""
        SELECT lower(trim({name_col})) AS key,
               min({name_col})         AS name,
               min({id_col})           AS id,
               sum(impressions)        AS imps,
               sum(gross_revenue)      AS gross,
               count(DISTINCT report_date) AS active_days
        FROM pgam_direct.{table}
        WHERE report_date = ANY(%(days)s) AND {name_col} IS NOT NULL
        GROUP BY 1
    """, {"days": days})
    return {
        key: {"name": name, "id": pid, "imps": int(imps or 0),
              "gross": float(gross or 0), "active_days": int(active or 0)}
        for key, name, pid, imps, gross, active in rows
    }


def classify(before: dict[str, dict], after: dict[str, dict],
             quiet_pct: float, n_before: int, n_after: int) -> dict:
    """Split the before-roster into gone / quiet / carried, plus new arrivals.

    Rates, not totals. The two windows can be different lengths — the 'after'
    one is clipped to settled days — and comparing a 4-day sum against a
    2-day sum would report every surviving partner as halved.
    """
    gone, quiet, carried = [], [], []

    for key, b in before.items():
        if b["gross"] < MIN_BEFORE_REVENUE:
            continue
        b_rate = b["imps"] / n_before if n_before else 0.0
        a = after.get(key)

        if a is None:
            gone.append({**b, "key": key, "before_imps_per_day": b_rate,
                         "before_gross_per_day": b["gross"] / n_before})
            continue

        a_rate = a["imps"] / n_after if n_after else 0.0
        drop = (b_rate - a_rate) / b_rate if b_rate else 0.0
        rec = {"key": key, "name": b["name"],
               "legacy_id": b["id"], "tbx_id": a["id"],
               "before_imps_per_day": b_rate, "after_imps_per_day": a_rate,
               "drop_pct": drop * 100,
               "before_gross_per_day": b["gross"] / n_before,
               "after_gross_per_day": a["gross"] / n_after if n_after else 0.0}
        (quiet if drop * 100 >= quiet_pct else carried).append(rec)

    arrived = [{**v, "key": k} for k, v in after.items() if k not in before]

    gone.sort(key=lambda r: r["before_gross_per_day"], reverse=True)
    quiet.sort(key=lambda r: r["before_gross_per_day"] - r["after_gross_per_day"],
               reverse=True)
    arrived.sort(key=lambda r: r["gross"], reverse=True)
    return {"gone": gone, "quiet": quiet, "carried": carried, "arrived": arrived}


def render(res: dict, before_days: list[date], after_days: list[date],
           quiet_pct: float) -> None:
    n_before, n_after = len(before_days), len(after_days)
    print(_HDR)
    print("SUPPLY THAT DID NOT SURVIVE THE CUTOVER")
    print(_HDR)
    print(f"  before  {before_days[0]} → {before_days[-1]}  "
          f"({n_before}d, legacy {LEGACY_SUPPLY})")
    print(f"  after   {after_days[0]} → {after_days[-1]}  "
          f"({n_after}d, TBX {TBX_SUPPLY})")
    print(f"  matched on name, lowercased — never on id (see module docstring)")
    print()

    lost_rev = sum(r["before_gross_per_day"] for r in res["gone"])
    faded_rev = sum(r["before_gross_per_day"] - r["after_gross_per_day"]
                    for r in res["quiet"])

    if res["gone"]:
        print(f"  GONE — earned before the cutover, no rows at all after "
              f"({len(res['gone'])})")
        print(f"    {'publisher':<34} {'legacy id':>10} {'imps/day':>12} "
              f"{'gross/day':>11}")
        print(f"    {'-' * 34} {'-' * 10} {'-' * 12} {'-' * 11}")
        for r in res["gone"]:
            print(f"    {str(r['name'])[:34]:<34} {str(r['id']):>10} "
                  f"{r['before_imps_per_day']:>12,.0f} "
                  f"{r['before_gross_per_day']:>11,.2f}")
        print(f"    {'':<34} {'':>10} {'':>12} {'-' * 11}")
        print(f"    {'lost gross per day':<34} {'':>10} {'':>12} "
              f"{lost_rev:>11,.2f}")
        print()
    else:
        print("  GONE — none. Every publisher earning before the cutover has\n"
              "         rows after it.\n")

    if res["quiet"]:
        print(f"  QUIET — still present, impressions down more than "
              f"{quiet_pct:.0f}% ({len(res['quiet'])})")
        print(f"    {'publisher':<30} {'imps/day before':>16} "
              f"{'after':>12} {'drop':>7} {'gross/day lost':>15}")
        print(f"    {'-' * 30} {'-' * 16} {'-' * 12} {'-' * 7} {'-' * 15}")
        for r in res["quiet"]:
            print(f"    {str(r['name'])[:30]:<30} "
                  f"{r['before_imps_per_day']:>16,.0f} "
                  f"{r['after_imps_per_day']:>12,.0f} "
                  f"{r['drop_pct']:>6.0f}% "
                  f"{r['before_gross_per_day'] - r['after_gross_per_day']:>15,.2f}")
        print()

    if res["arrived"]:
        print(f"  NEW ON TBX — no legacy row under this name ({len(res['arrived'])})")
        print("    Most likely a renamed publisher rather than new inventory.\n"
              "    Check these against the GONE list before chasing anything.")
        for r in res["arrived"][:10]:
            print(f"    {str(r['name'])[:40]:<40} {r['imps']:>12,} imps  "
                  f"{r['gross']:>10,.2f}")
        if len(res["arrived"]) > 10:
            print(f"    … and {len(res['arrived']) - 10} more")
        print()

    print(_HDR)
    print(f"  carried over cleanly : {len(res['carried'])}")
    print(f"  gone                 : {len(res['gone'])}  "
          f"(${lost_rev:,.2f}/day)")
    print(f"  quiet                : {len(res['quiet'])}  "
          f"(${faded_rev:,.2f}/day)")
    print(f"  TOTAL AT RISK        : ${lost_rev + faded_rev:,.2f}/day  "
          f"(${(lost_rev + faded_rev) * 30:,.0f}/month)")
    print(_HDR)

    if res["gone"] or res["quiet"]:
        print("\n  These are gross, not profit. At the legacy margin of ~30.6%")
        print(f"  the profit at risk is ~${(lost_rev + faded_rev) * 0.306:,.2f}/day.")
        print("\n  Next: confirm on the platform whether each publisher above is")
        print("  configured and active on api.pgammedia.com. A publisher that")
        print("  exists there but sends nothing is a connection problem; one")
        print("  that does not exist was never migrated.")


def main() -> int:
    p = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--cutover", default=os.environ.get("TB_TBX_CUTOVER",
                                                       CUTOVER_DEFAULT),
                   help=f"first TBX-only day (default {CUTOVER_DEFAULT})")
    p.add_argument("--days", type=int, default=4,
                   help="window length either side of the cutover (default 4)")
    p.add_argument("--quiet-pct", type=float, default=60.0,
                   help="impression drop that counts as QUIET (default 60)")
    p.add_argument("--json", action="store_true",
                   help="emit the finding as JSON instead of a table")
    args = p.parse_args()

    try:
        cutover = date.fromisoformat(args.cutover)
    except ValueError:
        print(f"--cutover {args.cutover!r} is not YYYY-MM-DD", file=sys.stderr)
        return 2

    dsn = (os.environ.get("PGAM_DIRECT_DATABASE_URL")
           or os.environ.get("DATABASE_URL") or "").strip()
    if not dsn:
        print("Set PGAM_DIRECT_DATABASE_URL (or DATABASE_URL).", file=sys.stderr)
        return 2

    before_days, after_days = windows(cutover, args.days, date.today())
    if not after_days:
        print(f"No settled days after {cutover} yet — nothing to compare.",
              file=sys.stderr)
        return 2

    import psycopg2
    conn = psycopg2.connect(dsn)
    try:
        with conn.cursor() as cur:
            cur.execute("SET TRANSACTION READ ONLY")
        for t in (LEGACY_SUPPLY, TBX_SUPPLY):
            if not _exists(conn, t):
                print(f"pgam_direct.{t} does not exist.", file=sys.stderr)
                return 2

        before = collect(conn, LEGACY_SUPPLY, "publisher_id", "publisher_name",
                         before_days)
        after = collect(conn, TBX_SUPPLY, "supply_id", "supply_name", after_days)
    finally:
        conn.close()

    res = classify(before, after, args.quiet_pct,
                   len(before_days), len(after_days))

    if args.json:
        print(json.dumps({
            "cutover": cutover.isoformat(),
            "before": [d.isoformat() for d in before_days],
            "after": [d.isoformat() for d in after_days],
            **res,
        }, indent=2, default=str))
    else:
        render(res, before_days, after_days, args.quiet_pct)

    return 1 if (res["gone"] or res["quiet"]) else 0


if __name__ == "__main__":
    raise SystemExit(main())
