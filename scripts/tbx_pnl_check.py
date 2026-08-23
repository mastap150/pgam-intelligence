#!/usr/bin/env python3
"""
Would the new platform's numbers change the P&L? Read-only.

The P&L at `admin.pgammedia.com/admin/pnl` models each day's profit from a TB
row it defines as:

    TB Gross = DSP Spend (advertiser-paid through Teqblaze)
    TB GP    = the "Profit" column on the TB report

Those live in `finance.daily_pnl_inputs.tb_gross_usd` / `tb_gross_profit_usd`
(a *different* Neon database from `pgam_direct` — `FINANCE_DATABASE_URL`), and
they are written by `pnl_sync.py` in the **pgam-recon** repo, daily at 10:15
UTC over a trailing 7 days, with a Vercel cron watchdog that re-fires the
workflow whenever `tb_gross_usd` comes back NULL.

The new platform reports **the same marketplace** that row already counts. So
this script does not propose an additional revenue stream — summing TBX
alongside TB would double-count every impression. It answers the narrower,
prior question: *if that row were sourced from TBX instead, would the numbers
move?*

    TB Gross  ←  sum(tbx_daily_supply_revenue.gross_revenue)     (dsp_price_sum)
    TB GP     ←  sum(gross_revenue - pub_payout)                 (dsp - ssp)

**This script writes nothing.** Both connections run inside
`SET TRANSACTION READ ONLY`. Changing where the P&L sources its TB row is a
decision for a human, and the point of this report is to make that decision
from evidence rather than from confidence.

Read it alongside `scripts/tbx_recon.py`, which asks the other half of the
question — whether TBX and the legacy platform agree with *each other*. That
one is the trust test; this one is the impact test. A disagreement here with
agreement there means the P&L row is stale or hand-entered, not that TBX is
wrong.

Three things it reports, in the order that matters:

1. **Gaps TBX could fill.** Days where the P&L holds NULL or zero for TB but
   TBX has a number. These are the strongest argument for the change and they
   carry no risk of contradicting anything — there is nothing there to
   contradict.
2. **Days where both exist, and by how much they differ.** A settled day that
   disagrees is a question, not a licence to overwrite.
3. **A verdict**, reusing `tbx_recon`'s classifier so both reports speak the
   same language.

Usage:
    python3 scripts/tbx_pnl_check.py                 # 7 settled days
    python3 scripts/tbx_pnl_check.py --days 30
    python3 scripts/tbx_pnl_check.py --end 2026-08-20 --days 14

Requires both `PGAM_DIRECT_DATABASE_URL` and `FINANCE_DATABASE_URL`.
"""

from __future__ import annotations

import argparse
import os
import sys
from datetime import date, datetime, timedelta, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from scripts.tbx_recon import _classify, EPSILON  # noqa: E402  same verdicts, same language

TBX_SUPPLY = "tbx_daily_supply_revenue"
PNL_TABLE = "finance.daily_pnl_inputs"

_HDR = "=" * 78


def _rows(conn, sql: str, params: tuple | dict | None = None) -> list[tuple]:
    with conn.cursor() as cur:
        cur.execute(sql, params or ())
        return cur.fetchall()


def _read_only(conn) -> None:
    with conn.cursor() as cur:
        cur.execute("SET TRANSACTION READ ONLY")


def tbx_side(conn, days: list[date]) -> dict[date, tuple[float, float]]:
    """What TBX says TB Gross and TB GP were, per day."""
    exists = _rows(conn, """
        SELECT 1 FROM information_schema.tables
        WHERE table_schema = 'pgam_direct' AND table_name = %s
    """, (TBX_SUPPLY,))
    if not exists:
        print(f"  pgam_direct.{TBX_SUPPLY} does not exist — the TBX ETL has "
              f"never run. Nothing to compare.")
        return {}

    rows = _rows(conn, f"""
        SELECT report_date,
               sum(gross_revenue)::float8,
               sum(gross_revenue - pub_payout)::float8
        FROM pgam_direct.{TBX_SUPPLY}
        WHERE report_date = ANY(%s)
        GROUP BY report_date
    """, (days,))
    return {r[0]: (r[1] or 0.0, r[2] or 0.0) for r in rows}


def pnl_side(conn, days: list[date]) -> dict[date, tuple[float | None, float | None]]:
    """What the P&L currently holds for TB, per day. None means NULL."""
    rows = _rows(conn, f"""
        SELECT target_date,
               tb_gross_usd::float8,
               tb_gross_profit_usd::float8
        FROM {PNL_TABLE}
        WHERE target_date = ANY(%s)
    """, (days,))
    return {r[0]: (r[1], r[2]) for r in rows}


def report(tbx: dict, pnl: dict, days: list[date]) -> int:
    missing_rows = [d for d in days if d not in pnl]
    fillable: list[date] = []
    comparable: list[date] = []

    for day in days:
        if day not in tbx:
            continue
        cur_gross = pnl.get(day, (None, None))[0]
        if cur_gross in (None, 0) or cur_gross == 0.0:
            fillable.append(day)
        else:
            comparable.append(day)

    # --- 1. gaps ----------------------------------------------------------
    print(_HDR)
    print("1. GAPS TBX COULD FILL")
    print(_HDR)
    print("  Days where the P&L has no TB number but TBX does. Nothing to")
    print("  contradict here — this is the safe half of the change.\n")
    if not fillable:
        print("  None. Every day with TBX data already has a TB number in the P&L.")
    else:
        print(f"  {'date':<12} {'TBX gross':>14} {'TBX GP':>14}  P&L currently")
        print(f"  {'-' * 12} {'-' * 14} {'-' * 14}  {'-' * 14}")
        for day in fillable:
            g, p = tbx[day]
            state = "row absent" if day not in pnl else "NULL / zero"
            print(f"  {day.isoformat():<12} {g:>14,.2f} {p:>14,.2f}  {state}")
    if missing_rows:
        print(f"\n  ({len(missing_rows)} day(s) have no daily_pnl_inputs row at all: "
              f"{', '.join(d.isoformat() for d in missing_rows[:5])}"
              f"{' …' if len(missing_rows) > 5 else ''})")

    # --- 2. disagreements -------------------------------------------------
    print()
    print(_HDR)
    print("2. DAYS WHERE BOTH EXIST")
    print(_HDR)
    if not comparable:
        print("  No day has both a TBX number and a non-zero P&L TB number.")
        print("  Nothing to compare — come back once the ETL has run over a")
        print("  window the P&L also covers.")
        return 0

    print(f"  {'date':<12} {'P&L gross':>13} {'TBX gross':>13} {'Δ':>9}"
          f" {'P&L GP':>13} {'TBX GP':>13} {'Δ':>9}")
    print(f"  {'-' * 12} {'-' * 13} {'-' * 13} {'-' * 9}"
          f" {'-' * 13} {'-' * 13} {'-' * 9}")

    gross_pairs: list[tuple[float, float]] = []
    gp_pairs: list[tuple[float, float]] = []

    for day in comparable:
        tg, tp = tbx[day]
        pg, pp = pnl[day]
        gross_pairs.append((pg, tg))
        gd = (tg - pg) / pg if pg else None
        gd_s = f"{gd:>8.2%}" if gd is not None else f"{'n/a':>8}"

        if pp not in (None, 0):
            gp_pairs.append((pp, tp))
            pd_ = (tp - pp) / pp
            pp_s, pd_s = f"{pp:>13,.2f}", f"{pd_:>8.2%}"
        else:
            pp_s, pd_s = f"{'—':>13}", f"{'n/a':>8}"

        mark = "" if (gd is not None and abs(gd) <= EPSILON) else " ←"
        print(f"  {day.isoformat():<12} {pg:>13,.2f} {tg:>13,.2f} {gd_s}"
              f" {pp_s} {tp:>13,.2f} {pd_s}{mark}")

    # --- 3. verdict -------------------------------------------------------
    print()
    print(_HDR)
    print("3. VERDICT")
    print(_HDR)
    verdicts = {}
    for label, pairs in (("TB Gross", gross_pairs), ("TB GP", gp_pairs)):
        verdict, detail = _classify(pairs)
        verdicts[label] = verdict
        print(f"  {label:<10} {verdict:<22} {detail}")

    kinds = set(verdicts.values()) - {"NO DATA"}
    print()
    if kinds == {"AGREEMENT"}:
        print("  TBX reproduces what the P&L already holds. Sourcing the TB row")
        print("  from TBX would change no number that is currently there, and")
        print("  would fill the gaps in section 1. That is the case for making")
        print("  the change — but confirm scripts/tbx_recon.py agrees first: this")
        print("  says TBX matches the P&L, not that TBX matches the legacy host.")
    elif not kinds:
        print("  Nothing comparable. Re-run once both sides cover the same days.")
    else:
        print("  TBX and the P&L disagree on days that already have numbers. Do")
        print("  NOT repoint the source on this evidence. Three things to rule")
        print("  out first, in this order:")
        print("    • Is the P&L row hand-entered or stale? It is inline-editable,")
        print("      so a human value can sit there indefinitely.")
        print("    • Do TBX and the legacy platform agree? scripts/tbx_recon.py.")
        print("      If they disagree there too, the problem is upstream of the P&L.")
        print("    • Same timezone on both? The P&L books ET; so does the TBX ETL")
        print("      unless TBX_TIMEZONE was changed.")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--days", type=int, default=7, help="window length (default 7)")
    parser.add_argument("--end", help="last day (YYYY-MM-DD), default yesterday")
    args = parser.parse_args()

    direct_dsn = (os.environ.get("PGAM_DIRECT_DATABASE_URL")
                  or os.environ.get("DATABASE_URL"))
    finance_dsn = os.environ.get("FINANCE_DATABASE_URL")
    missing = [name for name, val in
               (("PGAM_DIRECT_DATABASE_URL", direct_dsn),
                ("FINANCE_DATABASE_URL", finance_dsn)) if not val]
    if missing:
        print(f"Not set: {', '.join(missing)}. This report reads two separate "
              f"Neon databases — pgam_direct for TBX, finance for the P&L.",
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

    import psycopg

    print("Would TBX change the P&L's TB row?")
    print(f"  window   {days[0]} → {days[-1]}")
    print(f"  TBX      pgam_direct.{TBX_SUPPLY}")
    print(f"  P&L      {PNL_TABLE}  (written by pnl_sync.py in pgam-recon)")
    print(f"  mode     READ ONLY — this script writes nothing")
    print()

    direct = psycopg.connect(direct_dsn, autocommit=False)
    finance = psycopg.connect(finance_dsn, autocommit=False)
    try:
        _read_only(direct)
        _read_only(finance)
        tbx = tbx_side(direct, days)
        if not tbx:
            return 1
        pnl = pnl_side(finance, days)
    finally:
        direct.close()
        finance.close()

    return report(tbx, pnl, days)


if __name__ == "__main__":
    sys.exit(main())
