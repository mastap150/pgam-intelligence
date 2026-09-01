#!/usr/bin/env python3
"""
Platform revenue by month or quarter — the marketplace only, not the books.

This answers "what did the *platform* earn this year?", which is a different
question from what QuickBooks reports. The books carry revenue this warehouse
never sees (OTTA among it), so a P&L total and a platform total are not
expected to match and should not be reconciled to each other. Everything here
comes from `pgam_direct` and nothing else.

Three legs live in that schema, and the whole difficulty is that two of them
are the same marketplace:

    ll_daily_partner_revenue      LL          publisher x demand grain
    tb_daily_publisher_revenue    TB legacy   ssp.pgammedia.com
    tbx_daily_supply_revenue      TBX         api.pgammedia.com

**TB legacy and TBX report the same marketplace, and the rule for combining
them is not this script's to invent.** `core/tb_unified` already owns it, and
already serves the Slack alert and `/admin/pnl`. Its rule has three phases,
not two:

    day <  TB_SPLIT_START     -> legacy only
    day in [SPLIT, CUTOVER)   -> legacy + TBX, summed
    day >= TB_TBX_CUTOVER     -> TBX only

That middle phase is the part a naive cutover gets wrong. During the split
each host reports only what actually flowed through it, so the two are
complementary rather than two readings of one number — picking one side there
drops real revenue (on 2026-08-20 that is $7,505.66 against $2,605.46, a
$4,900 hole). Outside the split window they do report the same impressions and
summing would double-count every one.

So the TB leg here is `tb_unified.fetch()`, verbatim. Three implementations of
one rule is how a P&L and a Slack alert come to disagree about revenue, which
is the exact problem that module was written to end; a fourth would reopen it.
This script adds LL and the period arithmetic, and nothing else.

Excluding a partner
-------------------
`--exclude PATTERN` (case-insensitive substring) drops a counterparty from
every figure. The catch is that a marketplace has two sides and a partner can
sit on both — OTTA is named on supply sources *and* demand endpoints — so
"exclude OTTA" is not one number until you say which side you mean.

What each leg can honestly do, at the best grain it has:

    LL    ll_daily_partner_revenue is publisher x demand, so a row is dropped
          when EITHER side matches. This is the exact answer.
    TB    the supply and demand rollups are separate and TBX has no pair
          table, so the exact answer is not available post-cutover. Both
          one-sided views are computed and reported, and the headline uses
          whichever removes MORE revenue — the conservative reading.

The two TB views differ by however much of the partner's supply was bought by
someone else (or vice versa). That gap is printed, not hidden: it is the size
of the question the data cannot answer.

Revenue is defined as the platform defines it:

    gross_revenue   what the advertiser paid   (dsp price)
    pub_payout      what the publisher is owed (ssp price)
    gross_profit    gross_revenue - pub_payout

**This script writes nothing.** The connection runs inside SET TRANSACTION
READ ONLY.

Exit codes carry the verdict, matching tbx_recon.py and tbx_pnl_check.py:
    0  a report was produced over days that have data
    1  nothing to report, or a leg is missing entirely — look at it
    2  misconfigured (no DSN, bad argument)

Usage:
    python3 scripts/platform_revenue.py                      # YTD by month
    python3 scripts/platform_revenue.py --grain quarter
    python3 scripts/platform_revenue.py --from 2026-01-01 --to 2026-08-31
    python3 scripts/platform_revenue.py --exclude OTTA
    python3 scripts/platform_revenue.py --json

Requires PGAM_DIRECT_DATABASE_URL (or DATABASE_URL).
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import date, datetime, timedelta, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core import tb_unified as u        # noqa: E402  the one owner of the TB rule

LL_TABLE = "ll_daily_partner_revenue"
TB_TABLE = "tb_daily_publisher_revenue"      # diagnostics only — see tb_leg()
TBX_TABLE = "tbx_daily_supply_revenue"       # diagnostics only

_HDR = "=" * 92


# ---------------------------------------------------------------------------
# db
# ---------------------------------------------------------------------------

def _read_only(conn) -> None:
    with conn.cursor() as cur:
        cur.execute("SET TRANSACTION READ ONLY")


def _table_exists(conn, table: str) -> bool:
    with conn.cursor() as cur:
        cur.execute("""
            SELECT 1 FROM information_schema.tables
            WHERE table_schema = 'pgam_direct' AND table_name = %s
        """, (table,))
        return cur.fetchone() is not None


def daily(conn, table: str, start: date, end: date,
          exclude: str | None = None,
          name_cols: tuple[str, ...] = ()) -> dict[date, dict]:
    """Per-day totals for one source table. {} when the table is absent.

    `exclude` drops rows where any of `name_cols` contains the pattern. On the
    LL table that is publisher_name and demand_name together, which is an
    either-side match at pair grain — the exact exclusion.
    """
    if not _table_exists(conn, table):
        return {}
    where = "report_date BETWEEN %s AND %s"
    params: list = [start, end]
    if exclude and name_cols:
        clauses = " OR ".join(f"coalesce({c},'') ILIKE %s" for c in name_cols)
        where += f" AND NOT ({clauses})"
        params += [f"%{exclude}%"] * len(name_cols)
    with conn.cursor() as cur:
        cur.execute(f"""
            SELECT report_date,
                   sum(gross_revenue)::float8,
                   sum(pub_payout)::float8,
                   sum(impressions)::bigint
            FROM pgam_direct.{table}
            WHERE {where}
            GROUP BY report_date
        """, tuple(params))
        return {
            r[0]: {"gross": r[1] or 0.0,
                   "payout": r[2] or 0.0,
                   "imps": int(r[3] or 0)}
            for r in cur.fetchall()
        }


# ---------------------------------------------------------------------------
# periods and the stitch — pure, so the tests can reach them
# ---------------------------------------------------------------------------

def period_key(day: date, grain: str) -> str:
    if grain == "quarter":
        return f"{day.year}-Q{(day.month - 1) // 3 + 1}"
    return f"{day.year}-{day.month:02d}"


def tb_leg(start: date, end: date, exclude: str | None = None,
           side: str = "supply") -> tuple[dict[date, dict], dict[date, str]]:
    """The TB marketplace per day, straight out of `core.tb_unified`.

    No rule is applied here beyond reshaping its rows. `legs_for` supplies the
    origin label from the same module, so the report cannot describe a seam
    the numbers were not actually built with.

    With `exclude`, the entity breakdown is read instead of the date one and
    matching counterparties are dropped before the per-day fold. `side` picks
    which roster the pattern is matched against; tb_unified applies the same
    cutover rule either way.
    """
    breakdown = "PUBLISHER" if exclude else "DATE"
    needle = (exclude or "").lower()
    out: dict[date, dict] = {}
    origin: dict[date, str] = {}
    for row in u.fetch(breakdown, [], start.isoformat(), end.isoformat(), side=side):
        if exclude and needle in (row.get("PUBLISHER") or "").lower():
            continue
        day = date.fromisoformat(row["DATE"])
        slot = out.setdefault(day, {"gross": 0.0, "payout": 0.0, "imps": 0})
        slot["gross"] += row["GROSS_REVENUE"]
        slot["payout"] += row["PUB_PAYOUT"]
        slot["imps"] += int(row["IMPRESSIONS"])
        use_legacy, use_tbx = u.legs_for(day)
        origin[day] = ("legacy+tbx" if use_legacy and use_tbx
                       else "tbx" if use_tbx else "legacy")
    return out, origin


def heavier(a: dict[date, dict], b: dict[date, dict]) -> tuple[dict[date, dict], str]:
    """Of two one-sided exclusions, the one that removes more revenue.

    Neither is wrong; they answer slightly different questions and only a pair
    table could settle it. Taking the larger removal is the conservative
    choice — it cannot overstate what is left.
    """
    ga = sum(v["gross"] for v in a.values())
    gb = sum(v["gross"] for v in b.values())
    return (a, "supply") if ga <= gb else (b, "demand")


def roll_up(series: dict[date, dict], grain: str) -> dict[str, dict]:
    """Daily rows -> period totals, with day counts."""
    periods: dict[str, dict] = {}
    for day, v in series.items():
        p = periods.setdefault(period_key(day, grain),
                               {"gross": 0.0, "payout": 0.0, "imps": 0, "days": 0})
        p["gross"] += v["gross"]
        p["payout"] += v["payout"]
        p["imps"] += v["imps"]
        p["days"] += 1
    for p in periods.values():
        p["profit"] = p["gross"] - p["payout"]
        p["margin"] = (p["profit"] / p["gross"] * 100) if p["gross"] else 0.0
        p["ecpm"] = (p["gross"] / p["imps"] * 1000) if p["imps"] else 0.0
    return periods


def overlaps(tb: dict[date, dict], tbx: dict[date, dict]) -> list[date]:
    """Days both hosts reported. Not an error — but never additive."""
    return sorted(set(tb) & set(tbx))


# ---------------------------------------------------------------------------
# output
# ---------------------------------------------------------------------------

def _row(label: str, p: dict) -> str:
    return (f"  {label:<10} {p['gross']:>14,.2f} {p['payout']:>14,.2f} "
            f"{p['profit']:>14,.2f} {p['margin']:>7.1f}% {p['imps']:>15,} "
            f"{p['ecpm']:>8.3f} {p['days']:>5}")


def _table(title: str, periods: dict[str, dict], keys: list[str]) -> None:
    print(f"\n{title}")
    print(f"  {'period':<10} {'gross':>14} {'pub payout':>14} {'gross profit':>14} "
          f"{'margin':>8} {'impressions':>15} {'eCPM':>8} {'days':>5}")
    print(f"  {'-' * 10} {'-' * 14} {'-' * 14} {'-' * 14} {'-' * 8} "
          f"{'-' * 15} {'-' * 8} {'-' * 5}")
    if not keys:
        print("  (no data in range)")
        return
    tot = {"gross": 0.0, "payout": 0.0, "imps": 0, "days": 0}
    for k in keys:
        p = periods.get(k)
        if not p:
            continue
        print(_row(k, p))
        for f in ("gross", "payout", "imps", "days"):
            tot[f] += p[f]
    tot["profit"] = tot["gross"] - tot["payout"]
    tot["margin"] = (tot["profit"] / tot["gross"] * 100) if tot["gross"] else 0.0
    tot["ecpm"] = (tot["gross"] / tot["imps"] * 1000) if tot["imps"] else 0.0
    print(f"  {'-' * 10} {'-' * 14} {'-' * 14} {'-' * 14} {'-' * 8} "
          f"{'-' * 15} {'-' * 8} {'-' * 5}")
    print(_row("TOTAL", tot))


def report(ll: dict, tb: dict, tbx: dict, tb_series: dict, origin: dict,
           start: date, end: date, grain: str, as_json: bool,
           exclude: str | None = None, excl: dict | None = None) -> int:

    platform: dict[date, dict] = {}
    for day in sorted(set(ll) | set(tb_series)):
        a, b = ll.get(day), tb_series.get(day)
        platform[day] = {
            "gross": (a["gross"] if a else 0.0) + (b["gross"] if b else 0.0),
            "payout": (a["payout"] if a else 0.0) + (b["payout"] if b else 0.0),
            "imps": (a["imps"] if a else 0) + (b["imps"] if b else 0),
        }

    legs = {
        "ll": roll_up(ll, grain),
        "tb_legacy": roll_up(tb, grain),
        "tbx": roll_up(tbx, grain),
        "tb_marketplace": roll_up(tb_series, grain),
        "platform_total": roll_up(platform, grain),
    }
    keys = sorted({k for leg in legs.values() for k in leg})

    if as_json:
        print(json.dumps({
            "range": {"from": start.isoformat(), "to": end.isoformat()},
            "grain": grain,
            "split_start": u.split_start().isoformat(),
            "cutover": u.cutover().isoformat(),
            "periods": keys,
            "exclude": exclude,
            "exclusion": (excl or {}).get("meta"),
            "legs": legs,
            "stitch": {
                "overlap_days": [d.isoformat() for d in overlaps(tb, tbx)],
                "origin": {d.isoformat(): o for d, o in sorted(origin.items())},
                "rule": "core.tb_unified",
            },
        }, indent=2, default=str))
        return 0 if keys else 1

    # ---- 1. what is actually there -----------------------------------------
    print(_HDR)
    print("1. COVERAGE")
    print(_HDR)
    print("  A period is only as complete as the days behind it. Partial")
    print("  months read as a decline if you do not check this first.\n")
    for name, src in (("LL", ll), ("TB legacy", tb), ("TBX", tbx)):
        if not src:
            print(f"  {name:<12} no rows in range (table absent, or the ETL "
                  f"has not run over it)")
            continue
        days = sorted(src)
        span = (end - start).days + 1
        print(f"  {name:<12} {len(days):>4} of {span} days   "
              f"{days[0]} → {days[-1]}")

    dup = overlaps(tb, tbx)
    split = sorted(d for d, o in origin.items() if o == "legacy+tbx")
    print()
    print(f"  Rule       core.tb_unified — legacy before {u.split_start()}, "
          f"both summed")
    print(f"             through the split window, TBX from {u.cutover()} on.")
    if dup:
        print(f"  Both hosts hold rows for {len(dup)} day(s): {dup[0]} → {dup[-1]}.")
        print("  Outside the split window those are the SAME impressions and are")
        print("  never added; inside it they are complementary and are.")
    if split:
        print(f"  Split days summed from both hosts: "
              f"{', '.join(d.isoformat() for d in split)}")

    # ---- 2. the legs -------------------------------------------------------
    print()
    print(_HDR)
    print("2. EACH LEG ON ITS OWN")
    print(_HDR)
    _table("LL  (ll_daily_partner_revenue)", legs["ll"], keys)
    _table(f"TB legacy  ({TB_TABLE})", legs["tb_legacy"], keys)
    _table(f"TBX  ({TBX_TABLE})", legs["tbx"], keys)
    print("\n  These two are shown for coverage only. Do not add them together —")
    print("  section 3 combines them through core.tb_unified instead.")

    # ---- 2b. the exclusion --------------------------------------------------
    if excl:
        m = excl["meta"]
        print()
        print(_HDR)
        print(f"2b. EXCLUDING '{exclude}'")
        print(_HDR)
        print("  A marketplace has two sides and a partner can sit on both, so")
        print("  this is not one number until you say which side you mean.\n")
        print(f"  LL   pair grain (publisher x demand) — either side matches.")
        print(f"       This is exact.")
        print(f"         kept {m['ll_kept']:>14,.2f}   removed {m['ll_removed']:>12,.2f}"
              f"   ({m['ll_pct']:.1f}% of LL)")
        print(f"\n  TB   supply and demand rollups are separate and TBX has no pair")
        print(f"       table, so the exact answer does not exist post-cutover.")
        print(f"         supply-side kept {m['tb_supply']:>14,.2f}"
              f"   removed {m['tb_supply_removed']:>12,.2f}")
        print(f"         demand-side kept {m['tb_demand']:>14,.2f}"
              f"   removed {m['tb_demand_removed']:>12,.2f}")
        print(f"         the two differ by {abs(m['tb_supply']-m['tb_demand']):,.2f} —"
              f" that gap is the size of")
        print(f"         the question the data cannot answer.")
        print(f"\n  Headline uses the {m['tb_side']} side: it removes more, so it cannot")
        print(f"  overstate what is left.")

    # ---- 3. the platform ---------------------------------------------------
    print()
    print(_HDR)
    print("3. THE PLATFORM")
    print(_HDR)
    print("  TB marketplace as core.tb_unified resolves it — the same figures")
    print("  the Slack alert posts and the P&L row holds.")
    _table("TB marketplace (core.tb_unified)", legs["tb_marketplace"], keys)
    _table("PLATFORM TOTAL  (LL + TB marketplace)", legs["platform_total"], keys)

    print("\n  Platform only. This excludes every revenue stream the")
    print("  marketplace warehouse does not carry, so it will not agree with")
    print("  the P&L and is not meant to.")

    return 0 if keys else 1


# ---------------------------------------------------------------------------

def main() -> int:
    ap = argparse.ArgumentParser(
        description="Platform revenue by month or quarter, from pgam_direct only.")
    ap.add_argument("--from", dest="start", help="first day (YYYY-MM-DD), default Jan 1 this year")
    ap.add_argument("--to", dest="end", help="last day (YYYY-MM-DD), default yesterday")
    ap.add_argument("--grain", choices=("month", "quarter"), default="month")
    ap.add_argument("--exclude", metavar="PATTERN",
                    help="drop a counterparty (case-insensitive substring), e.g. OTTA")
    ap.add_argument("--json", action="store_true", help="machine-readable output")
    args = ap.parse_args()

    dsn = os.environ.get("PGAM_DIRECT_DATABASE_URL") or os.environ.get("DATABASE_URL")
    if not dsn:
        print("Not set: PGAM_DIRECT_DATABASE_URL. This report reads the "
              "pgam_direct marketplace warehouse.", file=sys.stderr)
        return 2

    today = datetime.now(timezone.utc).date()
    try:
        end = date.fromisoformat(args.end) if args.end else today - timedelta(days=1)
        start = date.fromisoformat(args.start) if args.start else date(end.year, 1, 1)
    except ValueError as exc:
        print(f"Bad date: {exc}. Use YYYY-MM-DD.", file=sys.stderr)
        return 2
    if start > end:
        print(f"--from {start} is after --to {end}.", file=sys.stderr)
        return 2

    import psycopg

    if not args.json:
        print("Platform revenue — marketplace warehouse only, not the books")
        print(f"  range    {start} → {end}   ({args.grain})")
        print(f"  source   pgam_direct: {LL_TABLE}, {TB_TABLE}, {TBX_TABLE}")
        print(f"  TB rule  core.tb_unified — split {u.split_start()}, "
              f"cutover {u.cutover()}")
        if args.exclude:
            print(f"  exclude  '{args.exclude}' — see section 2b for what that "
                  f"means on each leg")
        print( "  mode     READ ONLY — this script writes nothing")
        print()

    conn = psycopg.connect(dsn, autocommit=False)
    try:
        _read_only(conn)
        ll_all = daily(conn, LL_TABLE, start, end)
        tb = daily(conn, TB_TABLE, start, end)
        tbx = daily(conn, TBX_TABLE, start, end)
        ll = (daily(conn, LL_TABLE, start, end, args.exclude,
                    ("publisher_name", "demand_name"))
              if args.exclude else ll_all)
    finally:
        conn.close()

    # Opens its own connection, by design: the rule lives in one place and
    # this script does not reach around it.
    excl = None
    if args.exclude:
        sup, origin = tb_leg(start, end, args.exclude, "supply")
        dem, _ = tb_leg(start, end, args.exclude, "demand")
        tb_series, side = heavier(sup, dem)
        g = lambda d: sum(v["gross"] for v in d.values())
        tb_full, _ = tb_leg(start, end)
        excl = {"meta": {
            "pattern": args.exclude,
            "ll_kept": g(ll), "ll_removed": g(ll_all) - g(ll),
            "ll_pct": (100 * (g(ll_all) - g(ll)) / g(ll_all)) if g(ll_all) else 0.0,
            "tb_supply": g(sup), "tb_supply_removed": g(tb_full) - g(sup),
            "tb_demand": g(dem), "tb_demand_removed": g(tb_full) - g(dem),
            "tb_side": side,
        }}
    else:
        tb_series, origin = tb_leg(start, end)

    if not (ll or tb_series):
        print("No rows in any of the three tables over this range.", file=sys.stderr)
        return 1

    return report(ll, tb, tbx, tb_series, origin, start, end, args.grain,
                  args.json, args.exclude, excl)


if __name__ == "__main__":
    sys.exit(main())
