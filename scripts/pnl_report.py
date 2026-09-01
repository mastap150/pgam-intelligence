#!/usr/bin/env python3
"""
The P&L behind `admin.pgammedia.com/admin/pnl`, by month or quarter. Read-only.

The admin page is a *render* of two tables in the finance Neon database. This
script reads those tables directly, so the same numbers come back without a
browser, a login, or the SSO wall in front of the dashboard:

    finance.daily_pnl_inputs   one row per day — the P&L's own inputs, written
                               by pnl_sync.py in the pgam-recon repo (daily at
                               10:15 UTC over a trailing 7 days). Inline-
                               editable in the dashboard, so a row whose
                               `source` starts with 'manual' is a human's
                               number and pnl_sync will not overwrite it.

    finance.ssp_recon_daily    one row per (day, demand partner) — what the
                               partner's own dashboard said (`ssp_dash_net`)
                               against what PGAM's platforms said
                               (`pgam_ssp_dash`). This is the discrepancy
                               report, per partner, per day.

WHY THIS IS NOT `platform_revenue.py`. That script reads `pgam_direct`, the
marketplace warehouse: what flowed through Teqblaze and LoopMe. This one reads
the books' inputs, which carry streams the warehouse never sees — FreeWheel
above all. FreeWheel is a demand partner whose spend is reported to PGAM but
never transits the PGAM platforms, so it appears in `fw_gross_usd` and in
`ssp_recon_daily` and nowhere in `pgam_direct`. The two reports answer
different questions and are not expected to agree; the gap between them is
itself a number worth reading, and section 5 prints it.

The finance database also reaches back further than the warehouse does, which
is the practical reason to run this: `pgam_direct` starts in late March, so Q1
exists here and only here.

**This script writes nothing.** The connection runs inside
`SET TRANSACTION READ ONLY`.

Sections, in the order a CFO reads them:

  1. Revenue and profit by period, from daily_pnl_inputs.
  2. Where the money comes from — each input line's share of gross.
  3. Coverage. A period is only as good as the days behind it: missing rows,
     NULL columns and hand-entered days are all listed, because a sum over a
     column that is NULL half the month is a smaller number, not a true one.
  4. Demand partners, from ssp_recon_daily: partner revenue and the
     partner-vs-PGAM discrepancy, YTD and by period.
  5. Cross-checks between the two tables, and against the marketplace.

Usage:
    python3 scripts/pnl_report.py                          # YTD by month
    python3 scripts/pnl_report.py --grain quarter
    python3 scripts/pnl_report.py --from 2026-01-01 --to 2026-08-31
    python3 scripts/pnl_report.py --exclude-partner sovrn  # drop a partner
    python3 scripts/pnl_report.py --json                   # machine-readable

Exit codes match the other Neon reports: 0 a report was produced, 1 there was
nothing to report, 2 misconfigured.

Requires `FINANCE_DATABASE_URL`.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone

PNL_TABLE = "finance.daily_pnl_inputs"
RECON_TABLE = "finance.ssp_recon_daily"

_HDR = "=" * 78

# The columns of daily_pnl_inputs, in reading order, with what each one is.
# `gross` marks the lines that add up to top-line revenue; the rest are
# components, payouts and adjustments that must not be summed into it.
COLUMNS: list[tuple[str, str, bool]] = [
    # (column,                         label,                     is_gross)
    ("tb_gross_usd",                   "Teqblaze gross",          True),
    ("ll_gross_usd",                   "LoopMe gross",            True),
    ("fw_gross_usd",                   "FreeWheel gross",         True),
    ("pmp_gross_usd",                  "PMP gross",               True),
    ("tb_gross_profit_usd",            "Teqblaze GP",             False),
    ("ll_net_usd",                     "LoopMe net",              False),
    ("ll_profit_usd",                  "LoopMe profit",           False),
    ("pmp_net_usd",                    "PMP net",                 False),
    ("ll_pub_payout_usd",              "LoopMe publisher payout", False),
    ("ll_demand_fee_usd",              "LoopMe demand fee",       False),
    ("ll_platform_fee_usd",            "LoopMe platform fee",     False),
    ("entrepreneur_publisher_payout_usd", "Entrepreneur payout",  False),
    ("excess_discrepancy_usd",         "Excess discrepancy",      False),
]

GROSS_COLS = [c for c, _, g in COLUMNS if g]
# Profit lines that stack into gross profit. FreeWheel and PMP contribute
# their own margin lines (pmp_net); FW carries no profit column at all, which
# section 5 says out loud rather than quietly treating as zero margin.
PROFIT_COLS = ["tb_gross_profit_usd", "ll_profit_usd", "pmp_net_usd"]


# --------------------------------------------------------------------------
# periods
# --------------------------------------------------------------------------

def period_key(day: date, grain: str) -> str:
    if grain == "quarter":
        return f"{day.year}-Q{(day.month - 1) // 3 + 1}"
    return f"{day.year}-{day.month:02d}"


def period_days(key: str, grain: str) -> tuple[date, date]:
    """First and last calendar day of a period key."""
    if grain == "quarter":
        year, q = key.split("-Q")
        year, q = int(year), int(q)
        first = date(year, (q - 1) * 3 + 1, 1)
        last_month = (q - 1) * 3 + 3
    else:
        year, month = (int(x) for x in key.split("-"))
        first = date(year, month, 1)
        last_month = month
    if last_month == 12:
        last = date(first.year, 12, 31)
    else:
        last = date(first.year, last_month + 1, 1) - timedelta(days=1)
    return first, last


def periods_between(start: date, end: date, grain: str) -> list[str]:
    keys: list[str] = []
    day = start
    while day <= end:
        key = period_key(day, grain)
        if key not in keys:
            keys.append(key)
        day += timedelta(days=1)
    return keys


def days_in_range(key: str, grain: str, start: date, end: date) -> int:
    """Calendar days of a period that fall inside the requested window."""
    first, last = period_days(key, grain)
    lo, hi = max(first, start), min(last, end)
    return (hi - lo).days + 1 if hi >= lo else 0


# --------------------------------------------------------------------------
# reading
# --------------------------------------------------------------------------

def _rows(conn, sql: str, params: tuple | None = None) -> list[tuple]:
    with conn.cursor() as cur:
        cur.execute(sql, params or ())
        return cur.fetchall()


def _read_only(conn) -> None:
    with conn.cursor() as cur:
        cur.execute("SET TRANSACTION READ ONLY")


def existing_columns(conn) -> list[str]:
    """Which of the columns we know about this database actually has.

    daily_pnl_inputs grew by ALTER over time — the LL fee breakdown and the
    entrepreneur payout are later additions. Selecting a column that is not
    there fails the whole query, so ask first and report the absence as a
    finding instead of a crash.
    """
    schema, table = PNL_TABLE.split(".")
    have = {r[0] for r in _rows(conn, """
        SELECT column_name FROM information_schema.columns
        WHERE table_schema = %s AND table_name = %s
    """, (schema, table))}
    return [c for c, _, _ in COLUMNS if c in have]


def pnl_rows(conn, cols: list[str], start: date, end: date) -> list[dict]:
    select = ", ".join(f"{c}::float8" for c in cols)
    rows = _rows(conn, f"""
        SELECT target_date, coalesce(source, ''), {select}
        FROM {PNL_TABLE}
        WHERE target_date BETWEEN %s AND %s
        ORDER BY target_date
    """, (start, end))
    out = []
    for r in rows:
        rec = {"date": r[0], "source": r[1]}
        for i, col in enumerate(cols):
            rec[col] = r[2 + i]
        out.append(rec)
    return out


def recon_rows(conn, start: date, end: date,
               exclude: str | None = None) -> list[tuple]:
    """(date, partner_key, sheet name, ssp_dash_net, pgam_ssp_dash) per day."""
    schema, table = RECON_TABLE.split(".")
    exists = _rows(conn, """
        SELECT 1 FROM information_schema.tables
        WHERE table_schema = %s AND table_name = %s
    """, (schema, table))
    if not exists:
        return []
    where = "target_date BETWEEN %s AND %s"
    params: list = [start, end]
    if exclude:
        where += (" AND coalesce(partner_key,'') NOT ILIKE %s"
                  " AND coalesce(partner_sheet_name,'') NOT ILIKE %s")
        params += [f"%{exclude}%"] * 2
    return _rows(conn, f"""
        SELECT target_date, coalesce(partner_key, ''),
               coalesce(partner_sheet_name, ''),
               ssp_dash_net::float8, pgam_ssp_dash::float8
        FROM {RECON_TABLE}
        WHERE {where}
        ORDER BY target_date
    """, tuple(params))


# --------------------------------------------------------------------------
# rolling up
# --------------------------------------------------------------------------

def roll_pnl(rows: list[dict], cols: list[str], grain: str) -> dict[str, dict]:
    """Sum each column by period, and count what was missing while summing.

    NULL and present-but-zero are different facts and are counted separately:
    a NULL is an absent number, a zero is a number. Both make a sum smaller,
    only one of them makes it wrong.
    """
    out: dict[str, dict] = {}
    for row in rows:
        key = period_key(row["date"], grain)
        slot = out.setdefault(key, {
            "sums": dict.fromkeys(cols, 0.0),
            "nulls": dict.fromkeys(cols, 0),
            "days": 0,
            "manual_days": 0,
            "dates": set(),
        })
        slot["days"] += 1
        slot["dates"].add(row["date"])
        if row["source"].lower().startswith("manual"):
            slot["manual_days"] += 1
        for col in cols:
            val = row[col]
            if val is None:
                slot["nulls"][col] += 1
            else:
                slot["sums"][col] += val
    return out


def roll_recon(rows: list[tuple], grain: str) -> tuple[dict, dict]:
    """By period, and by partner: (ssp_dash_net, pgam_ssp_dash) totals."""
    by_period: dict[str, dict] = {}
    by_partner: dict[str, dict] = {}
    for day, key, sheet, ssp, pgam in rows:
        pkey = period_key(day, grain)
        name = sheet or key
        slot = by_period.setdefault(pkey, {"ssp": 0.0, "pgam": 0.0, "rows": 0})
        slot["ssp"] += ssp or 0.0
        slot["pgam"] += pgam or 0.0
        slot["rows"] += 1
        pslot = by_partner.setdefault(name, {
            "key": key, "ssp": 0.0, "pgam": 0.0, "rows": 0,
            "periods": defaultdict(float),
        })
        pslot["ssp"] += ssp or 0.0
        pslot["pgam"] += pgam or 0.0
        pslot["rows"] += 1
        pslot["periods"][pkey] += ssp or 0.0
    return by_period, by_partner


def gross_of(sums: dict, cols: list[str]) -> float:
    return sum(sums.get(c, 0.0) for c in cols if c in sums)


# --------------------------------------------------------------------------
# reporting
# --------------------------------------------------------------------------

def _money(x: float | None, width: int = 14) -> str:
    return f"{'—':>{width}}" if x is None else f"{x:>{width},.2f}"


def report(pnl: dict, recon_period: dict, recon_partner: dict,
           cols: list[str], grain: str, start: date, end: date,
           missing_cols: list[str], top: int) -> int:
    keys = periods_between(start, end, grain)
    have = [k for k in keys if k in pnl]

    print(_HDR)
    print("1. REVENUE AND PROFIT BY PERIOD")
    print(_HDR)
    if not have:
        print(f"  No rows in {PNL_TABLE} between {start} and {end}.")
        print("  Either the range predates the table or pnl_sync has not run"
              " over it.")
        return 1

    gross_present = [c for c in GROSS_COLS if c in cols]
    profit_present = [c for c in PROFIT_COLS if c in cols]

    print(f"  Gross  = {' + '.join(gross_present)}")
    print(f"  GP     = {' + '.join(profit_present)}")
    print()
    head = (f"  {'period':<9} {'days':>5} {'gross':>15} {'gross profit':>15}"
            f" {'margin':>8} {'gross/day':>13}")
    print(head)
    print(f"  {'-' * 9} {'-' * 5} {'-' * 15} {'-' * 15} {'-' * 8} {'-' * 13}")

    tot_gross = tot_gp = 0.0
    tot_days = 0
    period_out: list[dict] = []
    for key in have:
        slot = pnl[key]
        g = gross_of(slot["sums"], gross_present)
        p = gross_of(slot["sums"], profit_present)
        n = slot["days"]
        tot_gross += g
        tot_gp += p
        tot_days += n
        margin = f"{p / g:>7.1%}" if g else f"{'n/a':>7}"
        print(f"  {key:<9} {n:>5} {g:>15,.2f} {p:>15,.2f} {margin}"
              f" {g / n if n else 0:>13,.2f}")
        period_out.append({"period": key, "days": n, "gross": g,
                           "gross_profit": p,
                           "margin": (p / g) if g else None})

    print(f"  {'-' * 9} {'-' * 5} {'-' * 15} {'-' * 15} {'-' * 8} {'-' * 13}")
    tmargin = f"{tot_gp / tot_gross:>7.1%}" if tot_gross else f"{'n/a':>7}"
    print(f"  {'TOTAL':<9} {tot_days:>5} {tot_gross:>15,.2f} {tot_gp:>15,.2f}"
          f" {tmargin} {tot_gross / tot_days if tot_days else 0:>13,.2f}")

    # --- 2. composition ---------------------------------------------------
    print()
    print(_HDR)
    print("2. WHERE IT COMES FROM")
    print(_HDR)
    print("  Every input line, summed over the whole window. The four gross")
    print("  lines add to the total above; the rest are components of them.\n")
    print(f"  {'line':<26} {'total':>15} {'% of gross':>11} {'per day':>12}")
    print(f"  {'-' * 26} {'-' * 15} {'-' * 11} {'-' * 12}")
    totals: dict[str, float] = {}
    for col, label, is_gross in COLUMNS:
        if col not in cols:
            continue
        val = sum(pnl[k]["sums"][col] for k in have)
        totals[col] = val
        share = f"{val / tot_gross:>10.1%}" if (is_gross and tot_gross) else f"{'':>10}"
        mark = " " if is_gross else "·"
        print(f" {mark}{label:<26} {val:>15,.2f} {share} "
              f"{val / tot_days if tot_days else 0:>11,.2f}")
    print(f"\n  (lines marked · are components or payouts, not top-line revenue)")
    if missing_cols:
        print(f"\n  Not in this database: {', '.join(missing_cols)}")
        print("  Those are later ALTERs in pgam-recon's pnl_sync.py; if you")
        print("  expected them, that repo's migration has not been applied here.")

    # --- 3. coverage ------------------------------------------------------
    print()
    print(_HDR)
    print("3. COVERAGE — HOW MUCH TO TRUST EACH PERIOD")
    print(_HDR)
    print("  A period's sum is only over the days that carry a number. A month")
    print("  with 12 NULL FreeWheel days is not a month with less FreeWheel.\n")
    print(f"  {'period':<9} {'rows':>5} {'cal':>5} {'manual':>7}  worst-covered columns")
    print(f"  {'-' * 9} {'-' * 5} {'-' * 5} {'-' * 7}  {'-' * 40}")
    coverage_flag = False
    for key in have:
        slot = pnl[key]
        cal = days_in_range(key, grain, start, end)
        gaps = sorted(((n, c) for c, n in slot["nulls"].items() if n),
                      reverse=True)[:3]
        gap_s = ", ".join(f"{c.replace('_usd', '')} {n}✗" for n, c in gaps) or "complete"
        if gaps or slot["days"] < cal:
            coverage_flag = True
        short = "" if slot["days"] == cal else f"  ← {cal - slot['days']} day(s) with no row"
        print(f"  {key:<9} {slot['days']:>5} {cal:>5} {slot['manual_days']:>7}  "
              f"{gap_s}{short}")
    if not coverage_flag:
        print("\n  Every day has a row and every column a number.")

    # --- 4. demand partners ----------------------------------------------
    print()
    print(_HDR)
    print("4. DEMAND PARTNERS")
    print(_HDR)
    if not recon_partner:
        print(f"  Nothing in {RECON_TABLE} over this range.")
    else:
        print("  `partner` is what the partner's own dashboard reported;")
        print("  `PGAM` is what PGAM's platforms recorded for the same days.")
        print("  A partner that never disagrees is one PGAM does not clear —")
        print("  FreeWheel is the case in point.\n")
        ranked = sorted(recon_partner.items(), key=lambda kv: -kv[1]["ssp"])
        p_ssp = sum(v["ssp"] for v in recon_partner.values())
        p_pgam = sum(v["pgam"] for v in recon_partner.values())
        print(f"  {'partner':<24} {'partner $':>14} {'PGAM $':>14}"
              f" {'diff':>12} {'diff %':>8} {'share':>7}")
        print(f"  {'-' * 24} {'-' * 14} {'-' * 14} {'-' * 12} {'-' * 8} {'-' * 7}")
        shown = ranked if top <= 0 else ranked[:top]
        for name, v in shown:
            diff = v["ssp"] - v["pgam"]
            dpct = f"{diff / v['ssp']:>7.1%}" if v["ssp"] else f"{'n/a':>7}"
            share = f"{v['ssp'] / p_ssp:>6.1%}" if p_ssp else f"{'':>6}"
            print(f"  {name[:24]:<24} {v['ssp']:>14,.2f} {v['pgam']:>14,.2f}"
                  f" {diff:>12,.2f} {dpct} {share}")
        if top > 0 and len(ranked) > top:
            rest = ranked[top:]
            r_ssp = sum(v["ssp"] for _, v in rest)
            r_pgam = sum(v["pgam"] for _, v in rest)
            print(f"  {f'+ {len(rest)} more':<24} {r_ssp:>14,.2f} {r_pgam:>14,.2f}"
                  f" {r_ssp - r_pgam:>12,.2f}")
        print(f"  {'-' * 24} {'-' * 14} {'-' * 14} {'-' * 12} {'-' * 8} {'-' * 7}")
        tdiff = p_ssp - p_pgam
        tdpct = f"{tdiff / p_ssp:>7.1%}" if p_ssp else f"{'n/a':>7}"
        print(f"  {'TOTAL':<24} {p_ssp:>14,.2f} {p_pgam:>14,.2f}"
              f" {tdiff:>12,.2f} {tdpct}")

        print()
        print("  By period:")
        print(f"  {'period':<9} {'partner $':>15} {'PGAM $':>15} {'diff':>13} {'diff %':>8}")
        print(f"  {'-' * 9} {'-' * 15} {'-' * 15} {'-' * 13} {'-' * 8}")
        for key in keys:
            if key not in recon_period:
                continue
            v = recon_period[key]
            d = v["ssp"] - v["pgam"]
            dp = f"{d / v['ssp']:>7.1%}" if v["ssp"] else f"{'n/a':>7}"
            print(f"  {key:<9} {v['ssp']:>15,.2f} {v['pgam']:>15,.2f} {d:>13,.2f} {dp}")

    # --- 5. cross-checks --------------------------------------------------
    print()
    print(_HDR)
    print("5. CROSS-CHECKS")
    print(_HDR)

    fw_total = totals.get("fw_gross_usd")
    fw_recon = next((v for name, v in recon_partner.items()
                     if "free" in name.lower() or "free" in v["key"].lower()),
                    None)
    if fw_total is not None and fw_recon:
        d = fw_total - fw_recon["ssp"]
        rel = f"{d / fw_recon['ssp']:.2%}" if fw_recon["ssp"] else "n/a"
        print(f"  FreeWheel, both ways — the P&L's fw_gross_usd is copied from")
        print(f"  ssp_recon_daily.ssp_dash_net, so these must agree. They are a")
        print(f"  check on the copy, not on FreeWheel.")
        print(f"    daily_pnl_inputs.fw_gross_usd   {fw_total:>15,.2f}")
        print(f"    ssp_recon_daily (freewheel)     {fw_recon['ssp']:>15,.2f}")
        print(f"    difference                      {d:>15,.2f}   ({rel})")
        if abs(d) > max(1.0, 0.005 * (fw_recon["ssp"] or 1)):
            print("    ← they do not agree. pnl_sync skips a day whose row is")
            print("      hand-edited (source 'manual…'), and section 3 counts those.")
        if fw_recon["pgam"] == 0:
            print("    FreeWheel's PGAM-side figure is zero over the whole window:")
            print("    its spend never transits the PGAM platforms, which is why")
            print("    platform_revenue.py cannot see it and this report can.")
    elif fw_total is not None:
        print(f"  FreeWheel gross (P&L): {fw_total:,.2f} — no matching partner")
        print(f"  row found in {RECON_TABLE}.")
    else:
        print("  No fw_gross_usd column, so FreeWheel cannot be checked here.")

    print()
    if recon_partner:
        p_ssp = sum(v["ssp"] for v in recon_partner.values())
        d = tot_gross - p_ssp
        print("  Books' gross vs the partner-reported book:")
        print(f"    daily_pnl_inputs gross          {tot_gross:>15,.2f}")
        print(f"    ssp_recon_daily partner total   {p_ssp:>15,.2f}")
        print(f"    difference                      {d:>15,.2f}")
        print("    These count different things and are not expected to match:")
        print("    the recon table is one row per demand partner PGAM reconciles,")
        print("    the P&L adds PMP and the platform's own gross on top. Read the")
        print("    difference as scope, and only worry when it moves sharply.")

    print()
    print("  Against the marketplace: run scripts/platform_revenue.py over the")
    print("  same window. It reads pgam_direct and will come back lower by")
    print("  roughly FreeWheel plus anything else that does not transit the")
    print("  platforms. That gap is the point of running both.")

    return 0


# --------------------------------------------------------------------------

def as_json(pnl: dict, recon_period: dict, recon_partner: dict,
            cols: list[str], grain: str, start: date, end: date) -> dict:
    keys = [k for k in periods_between(start, end, grain) if k in pnl]
    gross_present = [c for c in GROSS_COLS if c in cols]
    profit_present = [c for c in PROFIT_COLS if c in cols]
    return {
        "generated_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "source": {"pnl": PNL_TABLE, "recon": RECON_TABLE},
        "window": {"from": start.isoformat(), "to": end.isoformat(),
                   "grain": grain},
        "columns": cols,
        "periods": [{
            "period": k,
            "days_with_rows": pnl[k]["days"],
            "calendar_days": days_in_range(k, grain, start, end),
            "manual_days": pnl[k]["manual_days"],
            "gross": gross_of(pnl[k]["sums"], gross_present),
            "gross_profit": gross_of(pnl[k]["sums"], profit_present),
            "lines": {c: pnl[k]["sums"][c] for c in cols},
            "nulls": {c: n for c, n in pnl[k]["nulls"].items() if n},
            "recon": ({"partner": recon_period[k]["ssp"],
                       "pgam": recon_period[k]["pgam"]}
                      if k in recon_period else None),
        } for k in keys],
        "partners": [{
            "name": name, "partner_key": v["key"],
            "partner_reported": v["ssp"], "pgam_reported": v["pgam"],
            "difference": v["ssp"] - v["pgam"],
            "by_period": dict(v["periods"]),
        } for name, v in sorted(recon_partner.items(), key=lambda kv: -kv[1]["ssp"])],
    }


def main() -> int:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--grain", choices=("month", "quarter"), default="month")
    parser.add_argument("--from", dest="start",
                        help="first day YYYY-MM-DD (default: 1 Jan of --to's year)")
    parser.add_argument("--to", dest="end",
                        help="last day YYYY-MM-DD (default: yesterday)")
    parser.add_argument("--exclude-partner", dest="exclude",
                        help="drop a demand partner from section 4 "
                             "(case-insensitive substring; the P&L columns in "
                             "sections 1-3 are unaffected — they are already "
                             "aggregated)")
    parser.add_argument("--top", type=int, default=25,
                        help="partners to list individually (0 = all)")
    parser.add_argument("--json", action="store_true",
                        help="emit JSON instead of the report")
    args = parser.parse_args()

    dsn = os.environ.get("FINANCE_DATABASE_URL")
    if not dsn:
        print("FINANCE_DATABASE_URL is not set. This report reads the finance "
              "Neon database — a different one from pgam_direct, which is what "
              "PGAM_DIRECT_DATABASE_URL points at. Set it and re-run.",
              file=sys.stderr)
        return 2

    try:
        end = (date.fromisoformat(args.end) if args.end
               else datetime.now(timezone.utc).date() - timedelta(days=1))
        start = (date.fromisoformat(args.start) if args.start
                 else date(end.year, 1, 1))
    except ValueError as exc:
        print(f"--from/--to must be YYYY-MM-DD: {exc}", file=sys.stderr)
        return 2
    if start > end:
        print(f"--from {start} is after --to {end}", file=sys.stderr)
        return 2

    import psycopg

    conn = psycopg.connect(dsn, autocommit=False)
    try:
        _read_only(conn)
        cols = existing_columns(conn)
        if not cols:
            print(f"{PNL_TABLE} has none of the expected columns — is this the "
                  f"finance database?", file=sys.stderr)
            return 2
        rows = pnl_rows(conn, cols, start, end)
        recon = recon_rows(conn, start, end, args.exclude)
    finally:
        conn.close()

    missing_cols = [c for c, _, _ in COLUMNS if c not in cols]
    pnl = roll_pnl(rows, cols, args.grain)
    recon_period, recon_partner = roll_recon(recon, args.grain)

    if args.json:
        if not pnl:
            print(json.dumps({"error": "no rows", "window": {
                "from": start.isoformat(), "to": end.isoformat()}}, indent=2))
            return 1
        print(json.dumps(as_json(pnl, recon_period, recon_partner, cols,
                                 args.grain, start, end), indent=2, default=str))
        return 0

    print("PGAM P&L — the finance database behind admin.pgammedia.com/admin/pnl")
    print(f"  window   {start} → {end}   ({args.grain})")
    print(f"  inputs   {PNL_TABLE}")
    print(f"  partners {RECON_TABLE}"
          + (f"   (excluding '{args.exclude}')" if args.exclude else ""))
    print(f"  mode     READ ONLY — this script writes nothing")
    print()

    return report(pnl, recon_period, recon_partner, cols, args.grain,
                  start, end, missing_cols, args.top)


if __name__ == "__main__":
    sys.exit(main())
