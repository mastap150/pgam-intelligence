#!/usr/bin/env python3
"""
Do the two platforms agree about our revenue?

This is tranche 1 step 2 of `docs/teqblaze-new-platform.md` §7 — the cheapest
correctness test available on the migration, and the one that gates everything
downstream. It compares what the legacy platform (`ssp.pgammedia.com`, ETL'd
into `pgam_direct.tb_daily_*`) and the new one (`api.pgammedia.com`, ETL'd into
`pgam_direct.tbx_daily_*`) say about the same settled window.

Teqblaze confirmed on 2026-08-20 (§8.1.10c) that the old ClickHouse was
transferred wholesale and the reports should match. That makes this a
**confirmation test with a stated expected answer** — exact agreement — rather
than an open question, and it means a divergence is escalatable rather than
something for us to model around.

Three outcomes, all informative, and this script names which one it found:

  AGREEMENT           → the new platform is trustworthy for revenue. This is
                        the result that lets PGAM consider confirming the
                        legacy shutdown (§1) — not before.
  CONSTANT OFFSET     → likely a fee or margin applied at a different stage.
                        Contradicts (c), so it is also a question back to them.
                        The implied ratio is printed so the conversation starts
                        from a number.
  ROW-LEVEL DIVERGENCE → one host is wrong about our revenue. Rule out the two
                        traps below first; if it survives that, escalate rather
                        than working around it.

The two traps, and the second is the one that will bite
-------------------------------------------------------
1. **Timezone.** Both legs must have been pulled in the same zone or the daily
   buckets disagree for reasons that have nothing to do with the data. Both
   ETLs use ET (`TBX_TIMEZONE=US/Eastern`, and the legacy convention), so this
   script asserts nothing and simply reminds you — if either leg's timezone was
   ever changed, every number below is noise.

2. **Join keys.** Section 3 does NOT join demand rows on `demand_id`. Teqblaze
   confirmed *placement* IDs are unchanged and excluded *inventory* IDs;
   `demand_id` and `publisher_id` were not covered either way (§8.1.10d).
   Joining on a silently reassigned ID manufactures row-level divergence out of
   a perfectly matching dataset — and because report parity is now a vendor
   commitment, that failure mode means escalating our own join error to the
   vendor. So section 3 joins on **name** and then reports whether the IDs agree
   as a *finding*, which is the answer to §8.1.10d.

What this cannot check
----------------------
Placement grain — the grain with the only actual ID commitment behind it. The
new platform's ETL writes `tbx_daily_placement_revenue`, but there is no
legacy placement table in Neon (the legacy ETL lands publisher, demand,
country, os, hour and ad_format). Reconciling at placement level therefore
needs a live pull from the legacy API, not a warehouse query, and is out of
scope here. Section 3's name-keyed demand check is the substitute.

Strictly read-only: every statement runs inside `SET TRANSACTION READ ONLY`,
so a mistake here cannot write.

Usage:
    python3 scripts/tbx_recon.py                    # 7 settled days, ending yesterday
    python3 scripts/tbx_recon.py --days 14
    python3 scripts/tbx_recon.py --end 2026-08-20 --days 7
"""

from __future__ import annotations

import argparse
import os
import statistics
import sys
from datetime import date, datetime, timedelta, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Supply-side pair: both are aggregates of the whole marketplace at day grain,
# so their totals are directly comparable even though the entity grain differs
# (legacy keys on publisher, TBX on supply source).
LEGACY_SUPPLY = "tb_daily_publisher_revenue"
TBX_SUPPLY = "tbx_daily_supply_revenue"

# Demand-side pair, used for the name-keyed ID-stability check.
LEGACY_DEMAND = "tb_daily_demand_revenue"
TBX_DEMAND = "tbx_daily_demand_revenue"

METRICS = ("impressions", "gross_revenue", "pub_payout")

# Relative tolerance below which two numbers count as equal. 0.1% absorbs
# rounding in numeric(14,4) and nothing else — it is deliberately far tighter
# than any real fee would be.
EPSILON = 0.001

# Coefficient of variation below which a set of per-day ratios counts as "the
# same ratio every day", i.e. a constant offset rather than noise.
CONSTANT_CV = 0.01

_HDR = "=" * 78


def _q(conn, sql: str, params: tuple | dict | None = None) -> list[tuple]:
    with conn.cursor() as cur:
        cur.execute(sql, params or ())
        return cur.fetchall()


def _exists(conn, table: str) -> bool:
    return bool(_q(conn, """
        SELECT 1 FROM information_schema.tables
        WHERE table_schema = 'pgam_direct' AND table_name = %s
    """, (table,)))


def _classify(pairs: list[tuple[float, float]]) -> tuple[str, str]:
    """
    Name the relationship between two series. Returns (verdict, detail).

    `pairs` is [(legacy, tbx)] per day. Days where the legacy side is zero are
    skipped for the ratio test — a ratio against zero says nothing.

    A single comparable day can never be classified as a constant offset: one
    ratio is consistent with anything. That case reports divergence, which is
    the conservative direction — it sends you to look rather than to conclude.
    """
    usable = [(a, b) for a, b in pairs if a not in (0, None) and b is not None]
    if not usable:
        return "NO DATA", "nothing comparable in this window"

    deltas = [abs(b - a) / a for a, b in usable]
    if max(deltas) <= EPSILON:
        return "AGREEMENT", f"largest daily gap {max(deltas):.4%}"

    ratios = [b / a for a, b in usable]
    mean = statistics.fmean(ratios)
    if len(ratios) > 1 and mean:
        cv = statistics.stdev(ratios) / abs(mean)
        if cv < CONSTANT_CV:
            direction = "higher" if mean > 1 else "lower"
            return ("CONSTANT OFFSET",
                    f"TBX is consistently {abs(1 - mean):.2%} {direction} "
                    f"(ratio {mean:.4f}, cv {cv:.4%})")

    return ("ROW-LEVEL DIVERGENCE",
            f"daily gaps range {min(deltas):.2%} → {max(deltas):.2%}")


def coverage(conn, days: list[date]) -> list[date]:
    """
    Which days both legs actually hold. Returns the comparable subset.

    A day present in only one leg is not a divergence, it is a coverage gap —
    and comparing a full day against a partial one is the most common way to
    invent a crisis. So they are excluded here and reported separately.
    """
    print(_HDR)
    print("1. COVERAGE — which days can be compared at all?")
    print(_HDR)

    for table in (LEGACY_SUPPLY, TBX_SUPPLY, LEGACY_DEMAND, TBX_DEMAND):
        if not _exists(conn, table):
            print(f"  {table:<38} absent from Neon")
            continue
        rows = _q(conn, f"SELECT min(report_date), max(report_date), count(*) "
                        f"FROM pgam_direct.{table}")
        lo, hi, cnt = rows[0]
        span = f"{lo} → {hi}" if lo else "empty"
        print(f"  {table:<38} {span:<26} {cnt:>9,} rows")

    if not (_exists(conn, LEGACY_SUPPLY) and _exists(conn, TBX_SUPPLY)):
        print("\n  Cannot reconcile: one of the supply tables does not exist.")
        return []

    legacy_days = {r[0] for r in _q(
        conn, f"SELECT DISTINCT report_date FROM pgam_direct.{LEGACY_SUPPLY} "
              f"WHERE report_date = ANY(%s)", (days,))}
    tbx_days = {r[0] for r in _q(
        conn, f"SELECT DISTINCT report_date FROM pgam_direct.{TBX_SUPPLY} "
              f"WHERE report_date = ANY(%s)", (days,))}

    both = sorted(legacy_days & tbx_days)
    legacy_only = sorted(legacy_days - tbx_days)
    tbx_only = sorted(tbx_days - legacy_days)

    print(f"\n  window          {days[0]} → {days[-1]}  ({len(days)} days)")
    print(f"  in both legs    {len(both)} days")
    if legacy_only:
        print(f"  legacy only     {', '.join(d.isoformat() for d in legacy_only)}")
    if tbx_only:
        print(f"  TBX only        {', '.join(d.isoformat() for d in tbx_only)}")
    if not both:
        print("\n  Nothing to compare. If TBX is empty, the ETL has not run with")
        print("  credentials yet — check `python3 scripts/tb_freshness.py` and the")
        print("  Render worker log for [tbx_revenue_etl].")
    return both


def totals(conn, days: list[date]) -> dict[str, tuple[str, str]]:
    """Day-by-day totals on both legs, and a verdict per metric."""
    print()
    print(_HDR)
    print("2. DAY TOTALS — legacy vs new, whole marketplace")
    print(_HDR)
    print(f"  legacy  pgam_direct.{LEGACY_SUPPLY}")
    print(f"  new     pgam_direct.{TBX_SUPPLY}")
    print("  expected: exact agreement (Teqblaze, 2026-08-20 — same ClickHouse)\n")

    cols = ", ".join(f"sum({m})" for m in METRICS)
    legacy = {r[0]: r[1:] for r in _q(
        conn, f"SELECT report_date, {cols} FROM pgam_direct.{LEGACY_SUPPLY} "
              f"WHERE report_date = ANY(%s) GROUP BY report_date", (days,))}
    tbx = {r[0]: r[1:] for r in _q(
        conn, f"SELECT report_date, {cols} FROM pgam_direct.{TBX_SUPPLY} "
              f"WHERE report_date = ANY(%s) GROUP BY report_date", (days,))}

    series: dict[str, list[tuple[float, float]]] = {m: [] for m in METRICS}

    for idx, metric in enumerate(METRICS):
        money = metric != "impressions"
        print(f"  {metric}")
        print(f"    {'date':<12} {'legacy':>16} {'new (TBX)':>16} {'delta':>14} {'':>9}")
        print(f"    {'-' * 12} {'-' * 16} {'-' * 16} {'-' * 14} {'-' * 9}")
        for day in days:
            a = float(legacy.get(day, (0,) * len(METRICS))[idx] or 0)
            b = float(tbx.get(day, (0,) * len(METRICS))[idx] or 0)
            series[metric].append((a, b))
            gap = b - a
            pct = (gap / a) if a else None
            fmt = "{:>16,.2f}" if money else "{:>16,.0f}"
            mark = "" if (pct is not None and abs(pct) <= EPSILON) else "  ←"
            pct_s = f"{pct:>13.3%}" if pct is not None else f"{'n/a':>13}"
            print(f"    {day.isoformat():<12} " + fmt.format(a) + " " +
                  fmt.format(b) + f" {pct_s}{mark}")
        print()

    verdicts = {m: _classify(series[m]) for m in METRICS}
    print("  verdict per metric")
    for metric, (verdict, detail) in verdicts.items():
        print(f"    {metric:<16} {verdict:<22} {detail}")
    return verdicts


def demand_keys(conn, days: list[date]) -> None:
    """
    Name-keyed demand comparison, which measures whether `demand_id` moved.

    This is the empirical answer to §8.1.10d. Joining on the name and then
    comparing the IDs is the only way to tell "the platforms disagree about
    revenue" apart from "the platforms agree but renumbered the partners" —
    and those two have completely different consequences for the ETL.
    """
    print()
    print(_HDR)
    print("3. DEMAND PARTNERS — do the IDs still line up? (§8.1.10d)")
    print(_HDR)

    if not (_exists(conn, LEGACY_DEMAND) and _exists(conn, TBX_DEMAND)):
        print("  One of the demand tables is absent — skipping.")
        return

    print("  joined on NAME, never on id — see the module docstring, trap 2\n")

    rows = _q(conn, f"""
        WITH l AS (
            SELECT lower(trim(demand_name)) AS key, demand_name,
                   min(demand_id) AS id,
                   sum(impressions) AS imps, sum(gross_revenue) AS gross
            FROM pgam_direct.{LEGACY_DEMAND}
            WHERE report_date = ANY(%(days)s) AND demand_name IS NOT NULL
            GROUP BY 1, 2
        ), t AS (
            SELECT lower(trim(demand_name)) AS key, demand_name,
                   min(demand_id) AS id,
                   sum(impressions) AS imps, sum(gross_revenue) AS gross
            FROM pgam_direct.{TBX_DEMAND}
            WHERE report_date = ANY(%(days)s) AND demand_name IS NOT NULL
            GROUP BY 1, 2
        )
        SELECT coalesce(l.demand_name, t.demand_name),
               l.id, t.id, l.imps, t.imps, l.gross, t.gross
        FROM l FULL OUTER JOIN t ON l.key = t.key
        ORDER BY coalesce(t.gross, l.gross, 0) DESC
        LIMIT 40
    """, {"days": days})

    if not rows:
        print("  No named demand rows in this window on either leg.")
        return

    print(f"  {'partner':<28} {'legacy id':>10} {'new id':>8} {'id?':>5} "
          f"{'imps Δ':>10} {'gross Δ':>10}")
    print(f"  {'-' * 28} {'-' * 10} {'-' * 8} {'-' * 5} {'-' * 10} {'-' * 10}")

    same_id = moved_id = only_one = 0
    for name, lid, tid, limp, timp, lgross, tgross in rows:
        if lid is None or tid is None:
            only_one += 1
            side = "legacy only" if tid is None else "TBX only"
            print(f"  {str(name)[:28]:<28} {str(lid or '—'):>10} "
                  f"{str(tid or '—'):>8} {side:>5}")
            continue
        if int(lid) == int(tid):
            same_id += 1
            flag = "same"
        else:
            moved_id += 1
            flag = "MOVED"
        imp_d = (f"{(float(timp or 0) - float(limp or 0)) / float(limp):.2%}"
                 if limp else "n/a")
        gr_d = (f"{(float(tgross or 0) - float(lgross or 0)) / float(lgross):.2%}"
                if lgross else "n/a")
        print(f"  {str(name)[:28]:<28} {int(lid):>10} {int(tid):>8} "
              f"{flag:>5} {imp_d:>10} {gr_d:>10}")

    print(f"\n  matched by name: {same_id + moved_id}   "
          f"same id: {same_id}   MOVED id: {moved_id}   "
          f"present on one leg only: {only_one}")
    if moved_id:
        print("\n  → demand_id is NOT stable across the two hosts. Every join in")
        print("    pgam_direct.tb_daily_demand_revenue that assumes it is will")
        print("    match the wrong rows SILENTLY. This is the §8.1.10d answer and")
        print("    it is worth sending to Teqblaze.")
    elif same_id:
        print("\n  → demand_id looks stable for every partner matched by name.")
        print("    Good, but it is evidence, not a commitment — Teqblaze still")
        print("    has not confirmed it (§8.1.10d).")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--days", type=int, default=7,
                        help="window length in days (default 7)")
    parser.add_argument("--end", help="last day of the window (YYYY-MM-DD), "
                                      "default yesterday — today is never settled")
    args = parser.parse_args()

    if not (os.environ.get("PGAM_DIRECT_DATABASE_URL") or os.environ.get("DATABASE_URL")):
        print("PGAM_DIRECT_DATABASE_URL / DATABASE_URL not set — nothing to read.",
              file=sys.stderr)
        return 2
    if args.days < 1:
        print("--days must be at least 1", file=sys.stderr)
        return 2

    try:
        end = (date.fromisoformat(args.end) if args.end
               else datetime.now(timezone.utc).date() - timedelta(days=1))
    except ValueError:
        print(f"--end must be YYYY-MM-DD, got {args.end!r}", file=sys.stderr)
        return 2

    days = [end - timedelta(days=n) for n in range(args.days - 1, -1, -1)]

    from core.neon import connect

    print("Legacy TB vs new TBX — do they agree about our revenue?")
    print(f"  window   {days[0]} → {days[-1]}")
    print(f"  mode     READ ONLY")
    print(f"  timezone both ETLs are ET; if either was ever repointed, every")
    print(f"           number below is noise (docstring, trap 1)")
    print()

    conn = connect()
    try:
        with conn.cursor() as cur:
            cur.execute("SET TRANSACTION READ ONLY")
        comparable = coverage(conn, days)
        if not comparable:
            return 1
        verdicts = totals(conn, comparable)
        demand_keys(conn, comparable)
    finally:
        conn.close()

    print()
    print(_HDR)
    print("4. WHAT THIS MEANS")
    print(_HDR)
    kinds = {v for v, _ in verdicts.values()}
    if kinds == {"AGREEMENT"}:
        print("  Every metric agrees. The new platform is trustworthy for revenue,")
        print("  the migration is a porting exercise, and PGAM can consider")
        print("  confirming the legacy shutdown to Teqblaze (§1) — a decision, not")
        print("  a formality: those legacy tables are the only independent check on")
        print("  TBX's numbers we will ever have.")
        return 0
    if "ROW-LEVEL DIVERGENCE" in kinds:
        print("  Row-level divergence. Rule out the two traps FIRST — timezone on")
        print("  both legs, and whether a partial day slipped into the window.")
        print("  If it survives that, one host is wrong about our revenue: stop,")
        print("  do not work around it, and escalate. Teqblaze has committed to")
        print("  these matching (§8.1.10c), so a real gap is their bug to explain.")
        return 1
    print("  A constant offset, which usually means a fee or margin applied at a")
    print("  different stage. Find it before anything downstream inherits it — and")
    print("  because it contradicts the report-parity commitment, it is also a")
    print("  question back to Teqblaze with the ratio above attached.")
    return 1


if __name__ == "__main__":
    sys.exit(main())
