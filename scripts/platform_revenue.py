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

**TB legacy and TBX report the same impressions.** Summing them double-counts
every one — the trap the migration notes call out repeatedly and the reason
the ETL keeps `tbx_daily_*` separate from `tb_daily_*`
(migrations/2026_08_21_tbx_daily_revenue.sql, docs/teqblaze-new-platform.md
§7.4). So this script never adds them. It *stitches* them into one TB
marketplace series at a cutover date — legacy before, TBX from the cutover on
— and prints, per period, how many days came from each side and where they
overlap, so the seam is auditable rather than assumed. Legacy ran full through
2026-08-20 and TBX full from 2026-08-20, which is the default cutover.

    Platform total = LL + stitched TB marketplace

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
    python3 scripts/platform_revenue.py --cutover 2026-08-20
    python3 scripts/platform_revenue.py --json

Requires PGAM_DIRECT_DATABASE_URL (or DATABASE_URL).
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import date, datetime, timedelta, timezone

LL_TABLE = "ll_daily_partner_revenue"
TB_TABLE = "tb_daily_publisher_revenue"
TBX_TABLE = "tbx_daily_supply_revenue"

# Legacy ran full through the 20th, TBX full from the 20th
# (docs/tb-data-workflow-integration.md §12). The 20th is served by both, so
# the cutover is the first day TBX owns.
DEFAULT_CUTOVER = date(2026, 8, 20)

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


def daily(conn, table: str, start: date, end: date) -> dict[date, dict]:
    """Per-day totals for one source table. {} when the table is absent."""
    if not _table_exists(conn, table):
        return {}
    with conn.cursor() as cur:
        cur.execute(f"""
            SELECT report_date,
                   sum(gross_revenue)::float8,
                   sum(pub_payout)::float8,
                   sum(impressions)::bigint
            FROM pgam_direct.{table}
            WHERE report_date BETWEEN %s AND %s
            GROUP BY report_date
        """, (start, end))
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


def stitch_tb(tb: dict[date, dict], tbx: dict[date, dict],
              cutover: date) -> tuple[dict[date, dict], dict[date, str]]:
    """One TB marketplace series, never the sum of the two.

    Before the cutover the legacy host is authoritative; from the cutover on,
    TBX is. Where the authoritative side has no row for a day, the other side
    fills it rather than dropping the day — a gap would understate the period,
    which is the more misleading error. Every day's origin is returned so the
    caller can show its work.
    """
    out: dict[date, dict] = {}
    origin: dict[date, str] = {}
    for day in sorted(set(tb) | set(tbx)):
        primary, fallback = ((tbx, tb), ("tbx", "tb")) if day >= cutover \
            else ((tb, tbx), ("tb", "tbx"))
        if day in primary[0]:
            out[day] = primary[0][day]
            origin[day] = fallback[0]
        else:
            out[day] = primary[1][day]
            origin[day] = fallback[1] + "*"      # * = filled from the other host
    return out, origin


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


def report(ll: dict, tb: dict, tbx: dict, start: date, end: date,
           grain: str, cutover: date, as_json: bool) -> int:
    tb_series, origin = stitch_tb(tb, tbx, cutover)

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
            "cutover": cutover.isoformat(),
            "periods": keys,
            "legs": legs,
            "stitch": {
                "overlap_days": [d.isoformat() for d in overlaps(tb, tbx)],
                "origin": {d.isoformat(): o for d, o in sorted(origin.items())},
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
    print()
    if dup:
        print(f"  TB legacy and TBX both reported {len(dup)} day(s): "
              f"{dup[0]} → {dup[-1]}.")
        print("  These are the SAME impressions on two hosts. They are never")
        print("  added — the stitch below takes one side per day.")
    else:
        print("  No day has rows on both TB hosts, so the stitch is unambiguous.")

    filled = [d for d, o in origin.items() if o.endswith("*")]
    if filled:
        print(f"\n  {len(filled)} day(s) fell back to the non-authoritative host "
              f"because the")
        print(f"  authoritative one had no row: "
              f"{', '.join(d.isoformat() for d in filled[:6])}"
              f"{' …' if len(filled) > 6 else ''}")

    # ---- 2. the legs -------------------------------------------------------
    print()
    print(_HDR)
    print("2. EACH LEG ON ITS OWN")
    print(_HDR)
    _table("LL  (ll_daily_partner_revenue)", legs["ll"], keys)
    _table(f"TB legacy  ({TB_TABLE})", legs["tb_legacy"], keys)
    _table(f"TBX  ({TBX_TABLE})", legs["tbx"], keys)
    print("\n  TB legacy and TBX are the same marketplace. Do not add these two")
    print("  tables together — section 3 stitches them instead.")

    # ---- 3. the platform ---------------------------------------------------
    print()
    print(_HDR)
    print("3. THE PLATFORM")
    print(_HDR)
    print(f"  TB marketplace = legacy before {cutover}, TBX from {cutover} on.")
    _table("TB marketplace (stitched)", legs["tb_marketplace"], keys)
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
    ap.add_argument("--cutover", default=DEFAULT_CUTOVER.isoformat(),
                    help=f"first day TBX is authoritative (default {DEFAULT_CUTOVER})")
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
        cutover = date.fromisoformat(args.cutover)
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
        print(f"  cutover  {cutover}  (first day TBX is authoritative)")
        print( "  mode     READ ONLY — this script writes nothing")
        print()

    conn = psycopg.connect(dsn, autocommit=False)
    try:
        _read_only(conn)
        ll = daily(conn, LL_TABLE, start, end)
        tb = daily(conn, TB_TABLE, start, end)
        tbx = daily(conn, TBX_TABLE, start, end)
    finally:
        conn.close()

    if not (ll or tb or tbx):
        print("No rows in any of the three tables over this range.", file=sys.stderr)
        return 1

    return report(ll, tb, tbx, start, end, args.grain, cutover, args.json)


if __name__ == "__main__":
    sys.exit(main())
