#!/usr/bin/env python3
"""
How current is the legacy TB data in Neon, and what has landed for today?

`tb_headroom.py` deliberately reports on windows ending yesterday, because a
part-written day makes every rate look wrong — a day that is 6 hours old has
6 hours of impressions against a full day's worth of nothing else. That is the
right default for analysis and the wrong one for the question "what is today
doing", so this script answers that one separately and says plainly which it
is giving you.

Three things, in order of how often they matter:

1. **Is the ETL current?** Per table: the newest `report_date` present, and how
   many days behind today that is. A table that has silently stopped writing is
   the failure that makes every downstream report confidently wrong, and it
   looks identical to a quiet marketplace unless you check the write date.

2. **What has today booked so far?** Gross, payout, impressions for today, if
   today exists at all. Explicitly labelled partial — a part-day total compared
   against a full-day total is the most common way to invent a crisis.

3. **Same-time-yesterday comparison.** A partial day is only interpretable
   against the same slice of a previous day, so where the hourly table exists
   this compares today's booked hours against the same hours yesterday and the
   trailing 7-day mean for those hours. That is the only honest way to say
   whether today is up or down while it is still running.

Strictly read-only: SELECTs inside a `SET TRANSACTION READ ONLY` transaction,
so a mistaken query cannot write.

Usage:
    python3 scripts/tb_freshness.py
    python3 scripts/tb_freshness.py --date 2026-08-21
"""

from __future__ import annotations

import argparse
import os
import sys
from datetime import date, datetime, timedelta, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Every table the TB ETLs write, with the columns worth totalling. A table
# missing from Neon is reported, not fatal: this script's whole job is telling
# you what is and is not there.
TABLES: dict[str, tuple[str, ...]] = {
    "tb_daily_publisher_revenue": ("impressions", "gross_revenue", "pub_payout"),
    "tb_daily_demand_revenue": ("impressions", "gross_revenue", "pub_payout"),
    "tb_daily_publisher_demand_revenue": ("impressions", "gross_revenue", "pub_payout"),
    "tb_daily_publisher_country": ("impressions", "gross_revenue", "pub_payout"),
    "tb_daily_country_revenue": ("impressions", "gross_revenue", "pub_payout"),
    "tb_daily_os": ("impressions", "gross_revenue", "pub_payout"),
    "tb_daily_hour": ("impressions", "gross_revenue", "pub_payout"),
    "tb_daily_country_hour": ("impressions", "gross_revenue", "pub_payout"),
    "tb_daily_ad_format": ("impressions", "gross_revenue", "pub_payout"),
}

# The new platform's tables, written by agents/etl/tbx_revenue_etl.py. Listed
# separately so the report says which LEG is stale rather than just which
# table: with both platforms live, "TB is current but TBX is empty" and "TBX is
# current but TB has stopped" are completely different problems, and during the
# migration either one can happen on its own. Absent is the normal state until
# TBX_EMAIL / TBX_PASSWORD are set in Render.
TBX_TABLES: tuple[str, ...] = (
    "tbx_daily_supply_revenue",
    "tbx_daily_demand_revenue",
    "tbx_daily_placement_revenue",
)

_HDR = "=" * 78


def _q(conn, sql: str, params: tuple | dict | None = None) -> list[tuple]:
    with conn.cursor() as cur:
        cur.execute(sql, params or ())
        return cur.fetchall()


def _exists(conn, table: str) -> bool:
    rows = _q(conn, """
        SELECT 1 FROM information_schema.tables
        WHERE table_schema = 'pgam_direct' AND table_name = %s
    """, (table,))
    return bool(rows)


def _has_column(conn, table: str, column: str) -> bool:
    rows = _q(conn, """
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'pgam_direct'
          AND table_name = %s AND column_name = %s
    """, (table, column))
    return bool(rows)


def freshness(conn, today: date) -> dict[str, date | None]:
    """Newest report_date per table, and how stale that is."""
    print(_HDR)
    print("1. ETL FRESHNESS — is the data current?")
    print(_HDR)
    print(f"  today is {today.isoformat()}\n")
    print(f"  {'table':<38} {'latest':<12} {'lag':<7} {'rows@latest':>11}")
    print(f"  {'-' * 38} {'-' * 12} {'-' * 7} {'-' * 11}")

    latest: dict[str, date | None] = {}
    for table in TABLES:
        if not _exists(conn, table):
            print(f"  {table:<38} {'—':<12} {'—':<7} {'absent':>11}")
            latest[table] = None
            continue
        rows = _q(conn, f"SELECT max(report_date) FROM pgam_direct.{table}")
        top = rows[0][0] if rows else None
        if top is None:
            print(f"  {table:<38} {'—':<12} {'—':<7} {'empty':>11}")
            latest[table] = None
            continue
        cnt = _q(conn, f"SELECT count(*) FROM pgam_direct.{table} "
                       f"WHERE report_date = %s", (top,))[0][0]
        lag = (today - top).days
        flag = "" if lag <= 1 else "  ← STALE"
        print(f"  {table:<38} {top.isoformat():<12} {str(lag) + 'd':<7} {cnt:>11,}{flag}")
        latest[table] = top

    # --- new platform leg -------------------------------------------------
    print()
    print("  new platform (TBX) — written by agents/etl/tbx_revenue_etl.py")
    any_tbx = False
    for table in TBX_TABLES:
        if not _exists(conn, table):
            print(f"  {table:<38} {'—':<12} {'—':<7} {'not created':>11}")
            continue
        rows = _q(conn, f"SELECT max(report_date) FROM pgam_direct.{table}")
        top = rows[0][0] if rows else None
        if top is None:
            print(f"  {table:<38} {'—':<12} {'—':<7} {'empty':>11}")
            continue
        any_tbx = True
        cnt = _q(conn, f"SELECT count(*) FROM pgam_direct.{table} "
                       f"WHERE report_date = %s", (top,))[0][0]
        lag = (today - top).days
        gross = _q(conn, f"SELECT sum(gross_revenue) FROM pgam_direct.{table} "
                         f"WHERE report_date = %s", (top,))[0][0] or 0
        print(f"  {table:<38} {top.isoformat():<12} {str(lag) + 'd':<7} {cnt:>11,}"
              f"   gross ${float(gross):,.2f}")
        latest[table] = top

    if not any_tbx:
        print()
        print("  TBX leg is not yet producing. Expected until TBX_EMAIL and")
        print("  TBX_PASSWORD are set on the Render worker — note the TBX_")
        print("  prefix: TB_EMAIL / TB_PASSWORD are the LEGACY host's and")
        print("  cannot serve this leg.")

    stale = [t for t, d in latest.items() if d is not None and (today - d).days > 1]
    print()
    if stale:
        print(f"  {len(stale)} table(s) more than a day behind. A stale table reads")
        print("  downstream as a quiet marketplace, not as a broken pipeline —")
        print("  check the Render worker's logs before interpreting any total below.")
    else:
        present = [t for t, d in latest.items() if d is not None]
        print(f"  all {len(present)} present table(s) are current to within a day.")
    return latest


def today_so_far(conn, target: date, latest: dict[str, date | None]) -> bool:
    """Totals for `target`, clearly marked partial. Returns whether any exist."""
    print()
    print(_HDR)
    print(f"2. {target.isoformat()} — WHAT HAS LANDED")
    print(_HDR)

    table = "tb_daily_publisher_revenue"
    if latest.get(table) is None:
        print(f"  {table} is absent or empty — cannot answer.")
        return False

    rows = _q(conn, f"""
        SELECT count(*), sum(impressions), sum(gross_revenue), sum(pub_payout)
        FROM pgam_direct.{table} WHERE report_date = %s
    """, (target,))
    n, imps, gross, payout = rows[0]

    if not n:
        newest = latest[table]
        print(f"  No rows for {target.isoformat()}.")
        print(f"  Newest date in {table} is {newest.isoformat()}.")
        print()
        print("  This is the expected state for a date the daily ETL has not")
        print("  reached yet. It is NOT evidence of zero revenue — the platform")
        print("  has the day, our copy does not. Read it on the platform, or")
        print("  wait for the ETL.")
        return False

    imps = int(imps or 0)
    gross = float(gross or 0.0)
    payout = float(payout or 0.0)
    profit = gross - payout
    ecpm = (gross / imps * 1000) if imps else 0.0
    margin = (profit / gross * 100) if gross else 0.0

    print(f"  rows          {n:,} publisher row(s)")
    print(f"  impressions   {imps:,}")
    print(f"  gross         ${gross:,.2f}")
    print(f"  payout        ${payout:,.2f}")
    print(f"  profit        ${profit:,.2f}")
    print(f"  eCPM          ${ecpm:.3f}")
    print(f"  margin        {margin:.2f}%")
    print()
    print("  PARTIAL if the ETL has not finished this day. Do not compare this")
    print("  against a full day's total — see the same-hours comparison below.")
    return True


def same_hours(conn, target: date) -> None:
    """
    Compare today's booked hours against the same hours on previous days.

    A part-day total means nothing next to a whole-day total. Restricting both
    sides to the hours today has actually booked is the only comparison that
    survives being made at 2pm.
    """
    print()
    print(_HDR)
    print("3. LIKE-FOR-LIKE — today's hours vs the same hours before")
    print(_HDR)

    table = "tb_daily_hour"
    if not _exists(conn, table):
        print(f"  pgam_direct.{table} is absent — no hourly grain, so a partial")
        print("  day cannot be compared honestly. Skipping rather than guessing.")
        return
    if not _has_column(conn, table, "hour"):
        print(f"  {table} has no `hour` column — schema differs from expected.")
        return

    hours = _q(conn, f"""
        SELECT min(hour), max(hour), count(DISTINCT hour)
        FROM pgam_direct.{table} WHERE report_date = %s
    """, (target,))
    if not hours or hours[0][2] in (None, 0):
        print(f"  No hourly rows for {target.isoformat()} yet — nothing to compare.")
        return
    lo, hi, n_hours = hours[0]
    print(f"  {target.isoformat()} has {n_hours} hour(s) booked (hour {lo}–{hi}).")
    print(f"  Comparing hours {lo}–{hi} only, on each day.\n")

    rows = _q(conn, f"""
        SELECT report_date,
               sum(impressions)::bigint,
               sum(gross_revenue)::numeric
        FROM pgam_direct.{table}
        WHERE report_date BETWEEN %s AND %s
          AND hour BETWEEN %s AND %s
        GROUP BY report_date
        ORDER BY report_date DESC
    """, (target - timedelta(days=8), target, lo, hi))

    if not rows:
        print("  No comparable history.")
        return

    print(f"  {'date':<12} {'impressions':>13} {'gross':>12} {'eCPM':>8}   {'vs today':>9}")
    print(f"  {'-' * 12} {'-' * 13} {'-' * 12} {'-' * 8}   {'-' * 9}")

    today_gross = None
    prior: list[float] = []
    for d, imps, gross in rows:
        imps = int(imps or 0)
        gross = float(gross or 0.0)
        ecpm = (gross / imps * 1000) if imps else 0.0
        if d == target:
            today_gross = gross
            delta = "—"
        else:
            prior.append(gross)
            delta = f"{(today_gross - gross) / gross * 100:+.1f}%" \
                if today_gross is not None and gross else "—"
        mark = "  ← today" if d == target else ""
        print(f"  {d.isoformat():<12} {imps:>13,} {gross:>12,.2f} {ecpm:>8.3f}   {delta:>9}{mark}")

    if today_gross is not None and prior:
        mean = sum(prior) / len(prior)
        print()
        if mean:
            diff = (today_gross - mean) / mean * 100
            print(f"  Today's booked hours: ${today_gross:,.2f} vs a {len(prior)}-day mean of "
                  f"${mean:,.2f} for the same hours — {diff:+.1f}%.")
        if len(prior) < 3:
            print("  Only a short baseline available; treat the direction as weak.")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--date", help="date to inspect (YYYY-MM-DD), default today UTC")
    args = parser.parse_args()

    if not (os.environ.get("PGAM_DIRECT_DATABASE_URL") or os.environ.get("DATABASE_URL")):
        print("PGAM_DIRECT_DATABASE_URL / DATABASE_URL not set — nothing to read.",
              file=sys.stderr)
        return 2

    today = datetime.now(timezone.utc).date()
    try:
        target = date.fromisoformat(args.date) if args.date else today
    except ValueError:
        print(f"--date must be YYYY-MM-DD, got {args.date!r}", file=sys.stderr)
        return 2

    from core.neon import connect

    print("Legacy TB data freshness in Neon")
    print(f"  source   pgam_direct.tb_daily_*")
    print(f"  mode     READ ONLY")
    print()

    conn = connect()
    try:
        with conn.cursor() as cur:
            cur.execute("SET TRANSACTION READ ONLY")
        latest = freshness(conn, today)
        today_so_far(conn, target, latest)
        same_hours(conn, target)
    finally:
        conn.close()

    print()
    print("  Note: this reads the LEGACY platform's data as ETL'd into Neon. The")
    print("  new platform (api.pgammedia.com) may already have the day; nothing")
    print("  here can see it until TBX credentials exist.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
