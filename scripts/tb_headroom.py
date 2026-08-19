#!/usr/bin/env python3
"""Read the real marketplace data and quantify optimisation headroom per lever.

Why this exists
---------------
The new Teqblaze platform (`api.pgammedia.com`) is where the write levers live,
but we have no credentials for it yet. The *legacy* platform is the same
marketplace (per Priyesh, 2026-08-19), and its data is already in Neon
`pgam_direct.tb_daily_*`, written by this repo's own ETLs. So we can size the
opportunity now and point each finding at the TBX lever that would act on it —
without waiting on a credential.

Read `docs/teqblaze-new-platform.md` §4 for the lever inventory this maps onto.

Two caveats worth carrying into any decision made from this output:

* **Legacy IDs, not TBX IDs.** A `publisher_id` here does not address anything
  on the new platform. Findings are directional — "this country is underpriced",
  "this pair never wins" — not ready-to-apply payloads. The ID mapping is an
  open question with Teqblaze (`docs/…` §8.1.10b).
* **Margin here is derived**, as `(gross_revenue - pub_payout) / gross_revenue`.
  The new platform computes `margin` itself. If the two disagree, the platform's
  definition wins and this needs revisiting.

Strictly read-only: every statement is a SELECT, inside a `READ ONLY`
transaction, so it cannot write even if a query were wrong.

Usage
-----
    python3 scripts/tb_headroom.py --days 30
    python3 scripts/tb_headroom.py --date-from 2026-07-01 --date-to 2026-07-31
"""

from __future__ import annotations

import argparse
import os
import sys
from datetime import date, timedelta

sys.path.insert(0, __file__.rsplit("/scripts/", 1)[0])

OK, FAIL = "✓", "✗"
_notes: list[str] = []


# ---------------------------------------------------------------------------
# Output helpers
# ---------------------------------------------------------------------------

def heading(text: str) -> None:
    print(f"\n{'═' * 78}\n{text}\n{'═' * 78}")


def section(text: str) -> None:
    print(f"\n── {text} " + "─" * max(0, 74 - len(text)))


def lever(text: str) -> None:
    """Name the TBX lever a finding maps to. The point of the whole exercise."""
    print(f"\n  ► LEVER: {text}")


def fmt(value: object) -> str:
    if value is None:
        return "-"
    if isinstance(value, float):
        return f"{value:,.2f}"
    if isinstance(value, int):
        return f"{value:,}"
    text = str(value)
    return text if len(text) <= 30 else text[:29] + "…"


def table(rows: list[tuple], columns: list[str], limit: int = 20) -> None:
    if not rows:
        print("  (no rows)")
        return
    widths = [
        max(len(columns[i]), *(len(fmt(r[i])) for r in rows[:limit]))
        for i in range(len(columns))
    ]
    print("  " + "  ".join(c.ljust(widths[i]) for i, c in enumerate(columns)))
    print("  " + "  ".join("─" * w for w in widths))
    for row in rows[:limit]:
        print("  " + "  ".join(fmt(row[i]).ljust(widths[i]) for i in range(len(columns))))
    if len(rows) > limit:
        print(f"  … {len(rows) - limit:,} more")


# ---------------------------------------------------------------------------
# Query plumbing
# ---------------------------------------------------------------------------

def run(conn, label: str, sql: str, params: dict) -> list[tuple] | None:
    """Execute one SELECT. A missing table is a finding, not a crash."""
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            return cur.fetchall()
    except Exception as exc:
        conn.rollback()
        detail = str(exc).strip().split("\n")[0]
        print(f"  {FAIL} {label}: {detail[:200]}")
        _notes.append(f"{label}: {detail[:160]}")
        return None


# ---------------------------------------------------------------------------
# Sections
# ---------------------------------------------------------------------------

def coverage(conn, df: str, dt: str) -> dict[str, int]:
    heading("0. WHAT DATA ACTUALLY EXISTS")
    print("  Everything below is only as good as this. A table with no rows in")
    print("  the window means that lever cannot be sized yet, not that it is fine.")
    tables = [
        "tb_daily_publisher_revenue", "tb_daily_demand_revenue",
        "tb_daily_publisher_demand_revenue", "tb_daily_publisher_country",
        "tb_daily_country_revenue", "tb_daily_os", "tb_daily_hour",
        "tb_daily_country_hour",
    ]
    counts: dict[str, int] = {}
    rows = []
    for name in tables:
        got = run(conn, name,
                  f"SELECT count(*), min(report_date), max(report_date) "
                  f"FROM pgam_direct.{name} "
                  f"WHERE report_date BETWEEN %(df)s AND %(dt)s",
                  {"df": df, "dt": dt})
        if got is None:
            counts[name] = 0
            rows.append((name, "MISSING", "-", "-"))
            continue
        n, lo, hi = got[0]
        counts[name] = int(n or 0)
        rows.append((name, int(n or 0), str(lo or "-"), str(hi or "-")))
    table(rows, ["table", "rows_in_window", "first_date", "last_date"], limit=20)
    return counts


def marketplace_shape(conn, df: str, dt: str) -> None:
    heading("1. MARKETPLACE SHAPE")
    section("daily totals")
    rows = run(conn, "daily totals", """
        SELECT report_date,
               sum(impressions)::bigint,
               round(sum(gross_revenue)::numeric, 2),
               round(sum(pub_payout)::numeric, 2),
               round((sum(gross_revenue) - sum(pub_payout))::numeric, 2),
               CASE WHEN sum(gross_revenue) > 0
                    THEN round((100.0 * (sum(gross_revenue) - sum(pub_payout))
                                / sum(gross_revenue))::numeric, 2) END,
               CASE WHEN sum(impressions) > 0
                    THEN round((1000.0 * sum(gross_revenue)
                                / sum(impressions))::numeric, 3) END
        FROM pgam_direct.tb_daily_publisher_revenue
        WHERE report_date BETWEEN %(df)s AND %(dt)s
        GROUP BY report_date ORDER BY report_date
    """, {"df": df, "dt": dt})
    if rows:
        table(rows, ["date", "imps", "gross", "payout", "profit", "margin%", "eCPM"],
              limit=40)
        gross = sum(float(r[2] or 0) for r in rows)
        profit = sum(float(r[4] or 0) for r in rows)
        print(f"\n  window total: gross ${gross:,.2f}  profit ${profit:,.2f}  "
              f"blended margin {100 * profit / gross if gross else 0:,.2f}%")


def demand_side(conn, df: str, dt: str) -> None:
    heading("2. DEMAND SIDE — where the money comes from, and at what margin")
    section("per demand source")
    rows = run(conn, "per demand", """
        SELECT demand_name,
               sum(impressions)::bigint,
               sum(bids)::bigint,
               sum(wins)::bigint,
               round(sum(gross_revenue)::numeric, 2),
               CASE WHEN sum(gross_revenue) > 0
                    THEN round((100.0 * (sum(gross_revenue) - sum(pub_payout))
                                / sum(gross_revenue))::numeric, 2) END,
               CASE WHEN sum(impressions) > 0
                    THEN round((1000.0 * sum(gross_revenue)
                                / sum(impressions))::numeric, 3) END,
               CASE WHEN sum(bids) > 0
                    THEN round((100.0 * sum(wins) / sum(bids))::numeric, 3) END,
               CASE WHEN sum(bids) > 0
                    THEN round((100.0 * sum(impressions) / sum(bids))::numeric, 3) END
        FROM pgam_direct.tb_daily_demand_revenue
        WHERE report_date BETWEEN %(df)s AND %(dt)s
        GROUP BY demand_name
        HAVING sum(gross_revenue) > 0
        ORDER BY sum(gross_revenue) DESC
    """, {"df": df, "dt": dt})
    if rows:
        table(rows, ["demand_source", "imps", "bids", "wins", "gross",
                     "margin%", "eCPM", "win%", "fill%"], limit=30)
        total = sum(float(r[4] or 0) for r in rows)
        top3 = sum(float(r[4] or 0) for r in rows[:3])
        print(f"\n  concentration: top 3 of {len(rows)} sources = "
              f"{100 * top3 / total if total else 0:,.1f}% of gross")
        lever("`margin_type` fixed/adaptive/range + `margin_min`/`margin_max` per DSP; "
              "`target_srcpm` where a source underprices; `spend_limit` to cap a "
              "concentrated one. Adaptive margin is the lever with no legacy "
              "equivalent — a fixed margin on a source whose eCPM swings is "
              "leaving money on both sides.")

    section("demand sources bidding but rarely winning")
    rows = run(conn, "low win rate", """
        SELECT demand_name,
               sum(bids)::bigint,
               sum(wins)::bigint,
               CASE WHEN sum(bids) > 0
                    THEN round((100.0 * sum(wins) / sum(bids))::numeric, 4) END,
               round(sum(gross_revenue)::numeric, 2)
        FROM pgam_direct.tb_daily_demand_revenue
        WHERE report_date BETWEEN %(df)s AND %(dt)s
        GROUP BY demand_name
        HAVING sum(bids) > 100000 AND sum(wins) < 0.001 * sum(bids)
        ORDER BY sum(bids) DESC
    """, {"df": df, "dt": dt})
    if rows is not None:
        table(rows, ["demand_source", "bids", "wins", "win%", "gross"], limit=20)
        if rows:
            print("\n  These consume QPS and return almost nothing. Either their")
            print("  price is far below our floor, or the traffic doesn't match")
            print("  what they buy.")
            lever("`qps_limit` to stop spending capacity on them, or "
                  "`geo_settings.bid_floor` / `banner_filter` / `video_filter` to "
                  "send them only what they actually bid on. Confirm against "
                  "`bids-overview` drop reasons on the new platform first — that "
                  "names the cause, which this can only infer.")


def geo(conn, df: str, dt: str) -> None:
    heading("3. GEO — the standout lever on the new platform")
    print("  `geo_settings.bid_floor[]` sets a per-country floor on a DEMAND")
    print("  source. It prices demand without touching publisher-side floors, so")
    print("  it carries no contract exposure — which is what makes it the safest")
    print("  first writer. This section sizes where it would pay.")

    section("per country")
    rows = run(conn, "per country", """
        SELECT country,
               sum(impressions)::bigint,
               round(sum(gross_revenue)::numeric, 2),
               CASE WHEN sum(impressions) > 0
                    THEN round((1000.0 * sum(gross_revenue)
                                / sum(impressions))::numeric, 3) END,
               CASE WHEN sum(gross_revenue) > 0
                    THEN round((100.0 * (sum(gross_revenue) - sum(pub_payout))
                                / sum(gross_revenue))::numeric, 2) END
        FROM pgam_direct.tb_daily_country_revenue
        WHERE report_date BETWEEN %(df)s AND %(dt)s
        GROUP BY country
        HAVING sum(impressions) > 0
        ORDER BY sum(gross_revenue) DESC
    """, {"df": df, "dt": dt})
    if rows:
        table(rows, ["country", "imps", "gross", "eCPM", "margin%"], limit=30)

        # The interesting cases are the extremes of eCPM at material volume:
        # high eCPM says we could ask for more, low eCPM at high volume says
        # we are clearing inventory too cheaply.
        priced = [r for r in rows if r[3] is not None and (r[1] or 0) > 0]
        if priced:
            total_imps = sum(int(r[1]) for r in priced)
            floor_imps = max(int(0.001 * total_imps), 1000)
            material = [r for r in priced if int(r[1]) >= floor_imps]
            if material:
                ranked = sorted(material, key=lambda r: float(r[3]), reverse=True)
                print(f"\n  countries with ≥{floor_imps:,} imps ({len(material)} of "
                      f"{len(priced)}), by eCPM:")
                print("\n  highest eCPM — ask for more:")
                table(ranked[:8], ["country", "imps", "gross", "eCPM", "margin%"], limit=8)
                print("\n  lowest eCPM at material volume — clearing too cheaply:")
                table(ranked[-8:], ["country", "imps", "gross", "eCPM", "margin%"], limit=8)
                spread = float(ranked[0][3]) / float(ranked[-1][3]) if float(ranked[-1][3]) else 0
                print(f"\n  eCPM spread across material countries: {spread:,.1f}×")
                print("  A single global floor cannot serve both ends of that spread.")
        lever("`geo_settings.bid_floor[]` per demand source — raise it in the "
              "high-eCPM countries to capture more, and in the low-eCPM ones to "
              "stop clearing cheap. `geo_settings.blacklist[]` for countries that "
              "never clear at any price. `geo_settings.qps[]` to stop paying for "
              "request volume from geos that don't convert.")


def pairs(conn, df: str, dt: str) -> None:
    heading("4. SUPPLY × DEMAND ROUTING")
    section("pairs with material volume and thin margin")
    rows = run(conn, "thin margin pairs", """
        SELECT publisher_name, demand_name,
               sum(impressions)::bigint,
               round(sum(gross_revenue)::numeric, 2),
               round((sum(gross_revenue) - sum(pub_payout))::numeric, 2),
               CASE WHEN sum(gross_revenue) > 0
                    THEN round((100.0 * (sum(gross_revenue) - sum(pub_payout))
                                / sum(gross_revenue))::numeric, 2) END
        FROM pgam_direct.tb_daily_publisher_demand_revenue
        WHERE report_date BETWEEN %(df)s AND %(dt)s
        GROUP BY publisher_name, demand_name
        HAVING sum(impressions) > 10000
           AND sum(gross_revenue) > 0
           AND (sum(gross_revenue) - sum(pub_payout)) / sum(gross_revenue) < 0.05
        ORDER BY sum(impressions) DESC
    """, {"df": df, "dt": dt})
    if rows is not None:
        table(rows, ["publisher", "demand_source", "imps", "gross", "profit",
                     "margin%"], limit=25)
        if rows:
            imps = sum(int(r[2] or 0) for r in rows)
            profit = sum(float(r[4] or 0) for r in rows)
            print(f"\n  {len(rows)} pairs, {imps:,} imps, ${profit:,.2f} profit — "
                  f"volume we carry for almost no margin.")
            lever("`is_allowed_sources` + `demand_sources[]` on the supply source "
                  "(or `supply_sources[]` on the demand source) to stop routing "
                  "that pair, or per-placement `margin_type`/`margin_min` to price "
                  "it properly. Check for a contract floor before touching the "
                  "publisher side.")

    section("pairs bidding with zero wins")
    rows = run(conn, "zero win pairs", """
        SELECT publisher_name, demand_name,
               sum(bids)::bigint, sum(wins)::bigint, sum(impressions)::bigint
        FROM pgam_direct.tb_daily_publisher_demand_revenue
        WHERE report_date BETWEEN %(df)s AND %(dt)s
        GROUP BY publisher_name, demand_name
        HAVING sum(bids) > 50000 AND sum(wins) = 0
        ORDER BY sum(bids) DESC
    """, {"df": df, "dt": dt})
    if rows is not None:
        table(rows, ["publisher", "demand_source", "bids", "wins", "imps"], limit=25)
        if rows:
            wasted = sum(int(r[2] or 0) for r in rows)
            print(f"\n  {wasted:,} bid requests across {len(rows)} pairs that never "
                  f"won once. Pure QPS cost.")
            lever("Same routing lever, plus `qps_limit`. On the new platform, "
                  "`bids-overview` names *why* each was dropped — worth reading "
                  "before cutting, in case the fix is a filter rather than a block.")


def dayparting(conn, df: str, dt: str) -> None:
    heading("5. HOUR OF DAY")
    rows = run(conn, "by hour", """
        SELECT hour,
               sum(impressions)::bigint,
               round(sum(ssp_revenue)::numeric, 2),
               round(sum(profit)::numeric, 2),
               CASE WHEN sum(impressions) > 0
                    THEN round((1000.0 * sum(ssp_revenue)
                                / sum(impressions))::numeric, 3) END
        FROM pgam_direct.tb_daily_hour
        WHERE report_date BETWEEN %(df)s AND %(dt)s
        GROUP BY hour ORDER BY hour
    """, {"df": df, "dt": dt})
    if rows:
        table(rows, ["hour", "imps", "ssp_revenue", "profit", "eCPM"], limit=24)
        priced = [r for r in rows if r[4] is not None]
        if len(priced) > 2:
            best = max(priced, key=lambda r: float(r[4]))
            worst = min(priced, key=lambda r: float(r[4]))
            ratio = float(best[4]) / float(worst[4]) if float(worst[4]) else 0
            print(f"\n  best hour {best[0]} at eCPM {float(best[4]):,.3f}, "
                  f"worst hour {worst[0]} at {float(worst[4]):,.3f} — {ratio:,.1f}× spread")
            lever("Per-hour floor rotation. `intelligence/dayparting.py` already "
                  "exists for the legacy platform and is default-off "
                  "(`PGAM_DAYPARTING_ENABLED=0`). A spread this size is the "
                  "argument for turning it on — but note the platform's own "
                  "`is_smart_floor` may already be moving floors, and two "
                  "controllers on one floor is the April thrash again.")


def device(conn, df: str, dt: str) -> None:
    heading("6. OS / DEVICE")
    rows = run(conn, "by os", """
        SELECT os,
               sum(impressions)::bigint,
               round(sum(gross_revenue)::numeric, 2),
               CASE WHEN sum(impressions) > 0
                    THEN round((1000.0 * sum(gross_revenue)
                                / sum(impressions))::numeric, 3) END,
               CASE WHEN sum(gross_revenue) > 0
                    THEN round((100.0 * (sum(gross_revenue) - sum(pub_payout))
                                / sum(gross_revenue))::numeric, 2) END
        FROM pgam_direct.tb_daily_os
        WHERE report_date BETWEEN %(df)s AND %(dt)s
        GROUP BY os ORDER BY sum(gross_revenue) DESC
    """, {"df": df, "dt": dt})
    if rows:
        table(rows, ["os", "imps", "gross", "eCPM", "margin%"], limit=20)
        lever("`traffic_type_*` and `ad_format_*` gating per source, and "
              "`banner_filter[]` / `video_filter[]` sizes. The new platform also "
              "breaks reports down by `traffic_type` (CTV / Desktop / Mobile App / "
              "Mobile Web) and `size`, which this legacy table cannot.")


# ---------------------------------------------------------------------------

def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--days", type=int, default=30)
    parser.add_argument("--date-from")
    parser.add_argument("--date-to")
    args = parser.parse_args()

    if args.date_from and args.date_to:
        df, dt = args.date_from, args.date_to
    else:
        end = date.today() - timedelta(days=1)
        df = (end - timedelta(days=max(args.days - 1, 0))).isoformat()
        dt = end.isoformat()

    if not (os.environ.get("PGAM_DIRECT_DATABASE_URL") or os.environ.get("DATABASE_URL")):
        print("PGAM_DIRECT_DATABASE_URL / DATABASE_URL not set — nothing to read.",
              file=sys.stderr)
        return 2

    from core.neon import connect

    print("Marketplace optimisation headroom, from the legacy TB tables in Neon")
    print(f"  window   {df} → {dt}")
    print("  source   pgam_direct.tb_daily_*  (legacy platform = same marketplace)")
    print("  levers   docs/teqblaze-new-platform.md §4")
    print("  mode     READ ONLY")

    conn = connect()
    try:
        # Belt and braces: even a mistaken query cannot write.
        with conn.cursor() as cur:
            cur.execute("SET TRANSACTION READ ONLY")

        counts = coverage(conn, df, dt)
        if not any(counts.values()):
            print("\nNo rows in any tb_daily_* table for this window. Either the "
                  "window predates the ETL, or the TB ETLs have stopped. Widen "
                  "--days before concluding anything.", file=sys.stderr)
            return 1

        if counts.get("tb_daily_publisher_revenue"):
            marketplace_shape(conn, df, dt)
        if counts.get("tb_daily_demand_revenue"):
            demand_side(conn, df, dt)
        if counts.get("tb_daily_country_revenue"):
            geo(conn, df, dt)
        if counts.get("tb_daily_publisher_demand_revenue"):
            pairs(conn, df, dt)
        if counts.get("tb_daily_hour"):
            dayparting(conn, df, dt)
        if counts.get("tb_daily_os"):
            device(conn, df, dt)

        heading("NOTES")
        if _notes:
            for note in _notes:
                print(f"  • {note}")
        else:
            print("  every query answered")
        print("\n  Reminder: these are LEGACY ids. Findings are directional; they")
        print("  do not address entities on the new platform until the ID mapping")
        print("  exists (docs/teqblaze-new-platform.md §8.1.10b).")
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
