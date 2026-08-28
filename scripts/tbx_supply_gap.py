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
The two tables spell the same entity differently, and the first version of
this script did not know that: it matched nothing, and reported 30 publishers
gone and $7,241/day at risk. Both figures were artifacts of the failed join.

The legacy report appends the id to the display name, and the legacy ETL
stores that whole string in BOTH columns:

    legacy   publisher_name = "Smaato - Display Stirista Premium #190"
             publisher_id   = "Smaato - Display Stirista Premium #190"
    TBX      supply_name    = "Smaato - Display Stirista Premium"
             supply_id      = 190

This is the same vendor convention `agents/etl/tbx_revenue_etl._entity`
already parses on the TBX side — the id in a trailing `#NNNN`. Strip it and
the rosters match. Only a TRAILING `#NNNN` counts, so a `#` inside a real
partner name is left alone.

So the key is the suffix-stripped, lowercased name. The extracted number is
then compared against TBX's `supply_id` and reported as a *finding* rather
than used to match — matching on an id that may have been reassigned would
manufacture a gap out of a matching dataset, and Teqblaze explicitly did not
commit to supply-side id stability (§8.1.10d). Measured 2026-08-25, every
matched supply source kept its id, which is the empirical answer to that
question.

A name that matches nothing is reported as *unmatched*, not as *lost*: the
first is a question for whoever knows the roster, the second is money. Where
the hosts genuinely spell a partner differently this overstates the gap, and
that is the safe direction.

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
import re
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

# The vendor appends the entity id to its display name — "Foo Bar #190". Only
# a trailing match counts, so a '#' inside a real partner name survives. Same
# rule as agents/etl/tbx_revenue_etl._entity, which parses it on the TBX side.
_TRAILING_ID = re.compile(r"\s*#(-?\d+)\s*$")


def split_name_id(raw: str) -> tuple[str, int | None]:
    """('Smaato - Display Stirista Premium #190') -> ('smaato - ...', 190)."""
    text = (raw or "").strip()
    m = _TRAILING_ID.search(text)
    if not m:
        return text.lower(), None
    return text[:m.start()].strip().lower(), int(m.group(1))


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
    """Per-partner totals over `days`, keyed on the suffix-stripped name.

    Aggregated in Python rather than SQL because the key needs the `#NNNN`
    stripped first, and two raw names can collapse onto one key once it is.
    """
    if not days:
        return {}
    rows = _q(conn, f"""
        SELECT {name_col}, {id_col}, impressions, gross_revenue
        FROM pgam_direct.{table}
        WHERE report_date = ANY(%(days)s) AND {name_col} IS NOT NULL
    """, {"days": days})

    out: dict[str, dict] = {}
    for name, raw_id, imps, gross in rows:
        key, embedded = split_name_id(str(name))
        if not key:
            continue
        # The legacy ETL stores the suffixed name in its id column too, so an
        # id is only usable when it is actually a number. Otherwise fall back
        # to the one carried in the name.
        try:
            pid = int(raw_id)
        except (TypeError, ValueError):
            pid = embedded
        e = out.setdefault(key, {"name": None, "id": pid, "imps": 0,
                                 "gross": 0.0})
        if e["name"] is None:
            e["name"] = str(name)[:_TRAILING_ID.search(str(name)).start()].strip() \
                if _TRAILING_ID.search(str(name)) else str(name)
        if e["id"] is None:
            e["id"] = pid
        e["imps"] += int(imps or 0)
        e["gross"] += float(gross or 0)
    return out


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

    same = [r for r in res["carried"] + res["quiet"]
            if r["legacy_id"] is not None and r["tbx_id"] is not None
            and int(r["legacy_id"]) == int(r["tbx_id"])]
    moved = [r for r in res["carried"] + res["quiet"]
             if r["legacy_id"] is not None and r["tbx_id"] is not None
             and int(r["legacy_id"]) != int(r["tbx_id"])]
    if same or moved:
        print(f"  supply ids across the hosts: {len(same)} same, "
              f"{len(moved)} MOVED   (§8.1.10d — never used to match)")
        for r in moved[:10]:
            print(f"    {str(r['name'])[:40]:<40} "
                  f"legacy {r['legacy_id']} → TBX {r['tbx_id']}")
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


def rosters(conn, before_days: list[date], after_days: list[date],
            limit: int) -> None:
    """Print the top names on every candidate table, and nothing else.

    Exists because the first run of this script joined legacy publishers
    against TBX supply sources and matched nothing — the two are different
    entity grains, which `tbx_recon.py` says in its own docstring. The lesson
    is that the join key has to be established from the data before any
    comparison is built on it, so this mode does that and draws no conclusion.
    """
    specs = [
        ("legacy supply ", LEGACY_SUPPLY, "publisher_id", "publisher_name",
         before_days),
        ("TBX supply    ", TBX_SUPPLY, "supply_id", "supply_name", after_days),
        ("legacy demand ", "tb_daily_demand_revenue", "demand_id",
         "demand_name", before_days),
        ("TBX demand    ", "tbx_daily_demand_revenue", "demand_id",
         "demand_name", after_days),
    ]
    for label, table, id_col, name_col, days in specs:
        print(_HDR)
        print(f"{label} — pgam_direct.{table}   {days[0]} → {days[-1]}")
        print(_HDR)
        if not _exists(conn, table):
            print("  table absent\n")
            continue
        rows = _q(conn, f"""
            SELECT {name_col}, min({id_col}), sum(impressions),
                   sum(gross_revenue)
            FROM pgam_direct.{table}
            WHERE report_date = ANY(%(days)s)
            GROUP BY 1
            ORDER BY 4 DESC NULLS LAST
            LIMIT %(limit)s
        """, {"days": days, "limit": limit})
        total = _q(conn, f"""
            SELECT count(DISTINCT {name_col}) FROM pgam_direct.{table}
            WHERE report_date = ANY(%(days)s)
        """, {"days": days})[0][0]
        print(f"  {total} distinct names in window; top {len(rows)} by gross\n")
        for name, pid, imps, gross in rows:
            print(f"    {str(name)[:52]:<52} id={str(pid):<8} "
                  f"{int(imps or 0):>12,} imps  {float(gross or 0):>10,.2f}")
        print()


def trace(conn, needle: str, before_days: list[date], after_days: list[date],
          top: int) -> None:
    """Where did one source's traffic go?

    A source can vanish from this report for two very different reasons: it
    stopped sending, or TBX broke it out under names legacy never used. TBX
    carries ~387 supply sources against legacy's 29, much of the difference
    being domain-level entries (`decoist.com`), so the second is a real
    possibility and the two want opposite responses — one is a conversation
    with the publisher, the other is a naming map.

    Three things, in the order that settles it:
      1. every name on either host containing `needle`, with its volume
      2. its per-day shape, so a collapse can be told from a clean stop
      3. the largest sources with no legacy counterpart, to see whether the
         missing impressions turn up under a name nobody recognises
    """
    n = needle.lower()

    print(_HDR)
    print(f"TRACE '{needle}'")
    print(_HDR)

    for label, table, id_col, name_col, days in (
        ("legacy", LEGACY_SUPPLY, "publisher_id", "publisher_name", before_days),
        ("TBX", TBX_SUPPLY, "supply_id", "supply_name", after_days),
    ):
        rows = _q(conn, f"""
            SELECT {name_col}, report_date, impressions, gross_revenue
              FROM pgam_direct.{table}
             WHERE report_date = ANY(%(d)s)
               AND lower({name_col}) LIKE %(n)s
             ORDER BY 1, 2
        """, {"d": days, "n": f"%{n}%"})
        print(f"\n  {label}: {len(rows)} matching row(s) "
              f"{days[0]} → {days[-1]}")
        if not rows:
            print(f"    no name on this host contains '{needle}'")
            continue
        for name, d, imps, gross in rows:
            print(f"    {str(name)[:44]:<44} {d}  {int(imps or 0):>12,} imps  "
                  f"{float(gross or 0):>9,.2f}")

    # Anything on TBX with no legacy counterpart at all, biggest first. If the
    # traffic moved rather than stopped, it is in this list.
    before = collect(conn, LEGACY_SUPPLY, "publisher_id", "publisher_name",
                     before_days)
    after = collect(conn, TBX_SUPPLY, "supply_id", "supply_name", after_days)
    fresh = [(k, v) for k, v in after.items() if k not in before]
    fresh.sort(key=lambda kv: kv[1]["imps"], reverse=True)

    n_after = len(after_days)
    total_fresh = sum(v["imps"] for _, v in fresh)
    print(f"\n  {len(fresh)} TBX source(s) with no legacy name, "
          f"{total_fresh:,} imps total "
          f"({total_fresh / n_after:,.0f}/day):\n")
    print(f"    {'supply source':<44} {'imps/day':>12} {'gross/day':>11}")
    print(f"    {'-' * 44} {'-' * 12} {'-' * 11}")
    for _, v in fresh[:top]:
        print(f"    {str(v['name'])[:44]:<44} "
              f"{v['imps'] / n_after:>12,.0f} {v['gross'] / n_after:>11,.2f}")
    if len(fresh) > top:
        rest = sum(v["imps"] for _, v in fresh[top:]) / n_after
        print(f"    {'… and ' + str(len(fresh) - top) + ' smaller':<44} "
              f"{rest:>12,.0f}")


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
    p.add_argument("--rosters", type=int, metavar="N",
                   help="print the top N names on each candidate table and "
                        "exit — use this to establish the join key before "
                        "trusting any comparison built on it")
    p.add_argument("--trace", metavar="NAME",
                   help="follow one source across the cutover: matching names "
                        "on both hosts, its per-day shape, and the largest "
                        "TBX sources with no legacy counterpart")
    p.add_argument("--top", type=int, default=25,
                   help="rows in the no-legacy-counterpart list (default 25)")
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

        if args.trace:
            trace(conn, args.trace, before_days, after_days, args.top)
            return 0

        if args.rosters:
            rosters(conn, before_days, after_days, args.rosters)
            return 0

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
