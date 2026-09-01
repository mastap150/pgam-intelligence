#!/usr/bin/env python3
"""
Offline checks for the P&L rollup. No credentials, no database.

The risks in this report are not in the SQL, they are in the accounting:

  * Summing a component into the top line. `daily_pnl_inputs` mixes gross
    lines with profit lines, payouts and an adjustment, all in the same row.
    Adding `ll_pub_payout_usd` to gross would inflate revenue by a payout;
    adding `tb_gross_profit_usd` would double-count margin. GROSS_COLS and
    PROFIT_COLS must stay disjoint, and neither may reach a payout or the
    discrepancy line.
  * Treating NULL as zero. Every column here is nullable and pnl_sync fills
    them on different schedules, so a month can be short a week of FreeWheel
    and still look like a complete month. A NULL must be counted, not summed.
  * Period boundaries. A quarter off by one moves money between quarters.

`psycopg` is never imported: the rollup functions take plain rows.
"""

from __future__ import annotations

import sys
from datetime import date

sys.path.insert(0, __file__.rsplit("/tests/", 1)[0])

from scripts import pnl_report as pr   # noqa: E402

PASS = FAIL = 0


def check(label: str, ok: bool, detail: str = "") -> None:
    global PASS, FAIL
    if ok:
        PASS += 1
        print(f"  ✓ {label}")
    else:
        FAIL += 1
        print(f"  ✗ {label}" + (f"  — {detail}" if detail else ""))


def d(day: str) -> date:
    return date.fromisoformat(day)


ALL = [c for c, _, _ in pr.COLUMNS]


def rec(day: str, source: str = "auto-x", **vals) -> dict:
    row = {"date": d(day), "source": source}
    for col in ALL:
        row[col] = vals.get(col)
    return row


# ---------------------------------------------------------------------------
# what counts as revenue
# ---------------------------------------------------------------------------

def test_gross_and_profit_are_disjoint() -> None:
    print("\nthe top line is only the top line")
    overlap = set(pr.GROSS_COLS) & set(pr.PROFIT_COLS)
    check("no column is both gross and profit", not overlap, str(overlap))

    payout_ish = {"ll_pub_payout_usd", "entrepreneur_publisher_payout_usd",
                  "ll_demand_fee_usd", "ll_platform_fee_usd",
                  "excess_discrepancy_usd"}
    check("no payout, fee or adjustment is counted as gross",
          not (payout_ish & set(pr.GROSS_COLS)),
          str(payout_ish & set(pr.GROSS_COLS)))
    check("no payout, fee or adjustment is counted as profit",
          not (payout_ish & set(pr.PROFIT_COLS)),
          str(payout_ish & set(pr.PROFIT_COLS)))

    declared = {c for c, _, g in pr.COLUMNS if g}
    check("GROSS_COLS is exactly the lines COLUMNS marks gross",
          declared == set(pr.GROSS_COLS))
    check("FreeWheel is one of them — the reason this report exists",
          "fw_gross_usd" in pr.GROSS_COLS)


def test_gross_of_ignores_absent_columns() -> None:
    print("\na column this database lacks contributes nothing")
    sums = {"tb_gross_usd": 100.0, "fw_gross_usd": 25.0}
    check("sums only what is present",
          pr.gross_of(sums, ["tb_gross_usd", "fw_gross_usd", "pmp_gross_usd"])
          == 125.0)
    check("an empty selection is zero, not an error",
          pr.gross_of(sums, []) == 0.0)


# ---------------------------------------------------------------------------
# NULL is not zero
# ---------------------------------------------------------------------------

def test_nulls_counted_not_summed() -> None:
    print("\nNULL is an absent number, zero is a number")
    rows = [
        rec("2026-05-01", tb_gross_usd=100.0, fw_gross_usd=10.0),
        rec("2026-05-02", tb_gross_usd=200.0, fw_gross_usd=None),
        rec("2026-05-03", tb_gross_usd=300.0, fw_gross_usd=0.0),
    ]
    out = pr.roll_pnl(rows, ALL, "month")["2026-05"]
    check("the NULL day is not summed", out["sums"]["fw_gross_usd"] == 10.0)
    check("the NULL day is counted", out["nulls"]["fw_gross_usd"] == 1,
          str(out["nulls"]["fw_gross_usd"]))
    check("a zero is not counted as a NULL",
          out["nulls"]["fw_gross_usd"] == 1)
    check("days counts rows, not non-NULLs", out["days"] == 3)
    check("a column NULL on every day sums to zero and flags 3",
          out["sums"]["pmp_gross_usd"] == 0.0
          and out["nulls"]["pmp_gross_usd"] == 3)


def test_manual_days_flagged() -> None:
    print("\na hand-entered day is a human's number")
    rows = [
        rec("2026-05-01", source="auto-tb", tb_gross_usd=1.0),
        rec("2026-05-02", source="manual-priyesh", tb_gross_usd=2.0),
        rec("2026-05-03", source="MANUAL", tb_gross_usd=3.0),
        rec("2026-05-04", source="", tb_gross_usd=4.0),
    ]
    out = pr.roll_pnl(rows, ALL, "month")["2026-05"]
    check("counts both spellings, case-insensitively",
          out["manual_days"] == 2, str(out["manual_days"]))
    check("still sums them — a manual number is a number",
          out["sums"]["tb_gross_usd"] == 10.0)


# ---------------------------------------------------------------------------
# periods
# ---------------------------------------------------------------------------

def test_period_keys() -> None:
    print("\nperiod boundaries")
    check("March is Q1", pr.period_key(d("2026-03-31"), "quarter") == "2026-Q1")
    check("April is Q2", pr.period_key(d("2026-04-01"), "quarter") == "2026-Q2")
    check("December is Q4", pr.period_key(d("2026-12-31"), "quarter") == "2026-Q4")
    check("month keys are zero-padded",
          pr.period_key(d("2026-09-05"), "month") == "2026-09")

    check("Q1 spans Jan 1 – Mar 31",
          pr.period_days("2026-Q1", "quarter") == (d("2026-01-01"), d("2026-03-31")))
    check("Q4 spans Oct 1 – Dec 31",
          pr.period_days("2026-Q4", "quarter") == (d("2026-10-01"), d("2026-12-31")))
    check("February 2026 ends on the 28th",
          pr.period_days("2026-02", "month")[1] == d("2026-02-28"))
    check("a leap February ends on the 29th",
          pr.period_days("2024-02", "month")[1] == d("2024-02-29"))
    check("December does not roll into next year",
          pr.period_days("2026-12", "month")[1] == d("2026-12-31"))


def test_periods_between() -> None:
    print("\nthe periods a window touches")
    months = pr.periods_between(d("2026-01-15"), d("2026-04-02"), "month")
    check("a partial first and last month are both included",
          months == ["2026-01", "2026-02", "2026-03", "2026-04"], str(months))
    quarters = pr.periods_between(d("2026-03-31"), d("2026-04-01"), "quarter")
    check("one day either side of a quarter boundary gives two quarters",
          quarters == ["2026-Q1", "2026-Q2"], str(quarters))
    check("order is chronological, no duplicates",
          pr.periods_between(d("2026-01-01"), d("2026-01-31"), "month")
          == ["2026-01"])


def test_days_in_range_clips() -> None:
    print("\ncalendar days are clipped to the window, not the period")
    check("a full month inside the window counts its own length",
          pr.days_in_range("2026-05", "month", d("2026-01-01"), d("2026-12-31")) == 31)
    check("a month the window enters halfway counts only the tail",
          pr.days_in_range("2026-05", "month", d("2026-05-20"), d("2026-12-31")) == 12)
    check("a month the window leaves halfway counts only the head",
          pr.days_in_range("2026-05", "month", d("2026-01-01"), d("2026-05-10")) == 10)
    check("Q1 clipped to a March-only window",
          pr.days_in_range("2026-Q1", "quarter", d("2026-03-01"), d("2026-03-31")) == 31)
    check("a period wholly outside the window is zero, not negative",
          pr.days_in_range("2026-05", "month", d("2026-07-01"), d("2026-08-01")) == 0)


# ---------------------------------------------------------------------------
# the partner side
# ---------------------------------------------------------------------------

def test_roll_recon() -> None:
    print("\ndemand partners roll up by period and by partner")
    rows = [
        (d("2026-04-01"), "freewheel", "Freehweel", 100.0, 0.0),
        (d("2026-04-02"), "freewheel", "Freehweel", 150.0, 0.0),
        (d("2026-05-01"), "freewheel", "Freehweel", 200.0, 0.0),
        (d("2026-04-01"), "loopme", "LoopMe", 80.0, 75.0),
        (d("2026-04-02"), "loopme", "LoopMe", None, 20.0),
    ]
    by_period, by_partner = pr.roll_recon(rows, "month")
    check("April totals both partners",
          round(by_period["2026-04"]["ssp"], 2) == 330.0,
          str(by_period["2026-04"]["ssp"]))
    check("a NULL partner figure adds nothing",
          round(by_period["2026-04"]["pgam"], 2) == 95.0,
          str(by_period["2026-04"]["pgam"]))
    check("FreeWheel is 450 across both months",
          by_partner["Freehweel"]["ssp"] == 450.0)
    check("FreeWheel's PGAM side is zero — it never transits the platforms",
          by_partner["Freehweel"]["pgam"] == 0.0)
    check("per-partner period split is kept",
          dict(by_partner["Freehweel"]["periods"])
          == {"2026-04": 250.0, "2026-05": 200.0})
    check("the sheet name is the display name, the key is kept alongside",
          by_partner["Freehweel"]["key"] == "freewheel")

    q_period, _ = pr.roll_recon(rows, "quarter")
    check("at quarter grain both months land in Q2",
          round(q_period["2026-Q2"]["ssp"], 2) == 530.0, str(q_period))


def test_roll_recon_falls_back_to_key() -> None:
    print("\na partner with no sheet name is still named")
    rows = [(d("2026-04-01"), "zmaticoo", "", 10.0, 9.0)]
    _, by_partner = pr.roll_recon(rows, "month")
    check("falls back to partner_key", "zmaticoo" in by_partner, str(list(by_partner)))


# ---------------------------------------------------------------------------
# the report reads only what it was given
# ---------------------------------------------------------------------------

def test_report_runs_on_a_thin_database() -> None:
    print("\na database missing the later ALTERs still reports")
    cols = ["tb_gross_usd", "tb_gross_profit_usd", "fw_gross_usd"]
    rows = [rec("2026-05-01", tb_gross_usd=1000.0, tb_gross_profit_usd=300.0,
                fw_gross_usd=500.0)]
    pnl = pr.roll_pnl(rows, cols, "month")
    code = pr.report(pnl, {}, {}, cols, "month",
                     d("2026-05-01"), d("2026-05-31"),
                     [c for c, _, _ in pr.COLUMNS if c not in cols], 25)
    check("returns 0 — a report was produced", code == 0, str(code))
    check("gross is the two gross lines present, not the profit line",
          pr.gross_of(pnl["2026-05"]["sums"],
                      [c for c in pr.GROSS_COLS if c in cols]) == 1500.0)


def test_report_says_so_when_empty() -> None:
    print("\nan empty window is a 1, not a silent 0")
    code = pr.report({}, {}, {}, ALL, "month",
                     d("2026-01-01"), d("2026-01-31"), [], 25)
    check("returns 1", code == 1, str(code))


def test_json_shape() -> None:
    print("\nthe JSON carries what a spreadsheet needs")
    rows = [rec("2026-04-01", tb_gross_usd=10.0, fw_gross_usd=5.0),
            rec("2026-04-02", tb_gross_usd=20.0, fw_gross_usd=None)]
    pnl = pr.roll_pnl(rows, ALL, "month")
    recon_p, recon_partner = pr.roll_recon(
        [(d("2026-04-01"), "freewheel", "Freehweel", 5.0, 0.0)], "month")
    blob = pr.as_json(pnl, recon_p, recon_partner, ALL, "month",
                      d("2026-04-01"), d("2026-04-30"))
    p = blob["periods"][0]
    check("gross sums both days, skipping the NULL",
          p["gross"] == 35.0, str(p["gross"]))
    check("calendar days are reported next to rows held",
          p["calendar_days"] == 30 and p["days_with_rows"] == 2)
    check("NULLs are reported, and only the non-zero counts",
          p["nulls"].get("fw_gross_usd") == 1
          and all(v for v in p["nulls"].values()))
    check("every line is present so nothing has to be re-derived",
          set(p["lines"]) == set(ALL))
    check("partners are ranked and carry their period split",
          blob["partners"][0]["name"] == "Freehweel"
          and blob["partners"][0]["by_period"] == {"2026-04": 5.0})


def test_no_writes_anywhere() -> None:
    print("\nread-only means read-only")
    src = open(__file__.rsplit("/tests/", 1)[0]
               + "/scripts/pnl_report.py").read()
    # Literal case: every SQL keyword in this repo's queries is uppercase,
    # while prose like "drop a demand partner" is not — so a case-sensitive
    # search finds the statements without tripping over the docstring.
    for stmt in ("INSERT INTO", "UPDATE ", "DELETE FROM", "DROP TABLE",
                 "DROP SCHEMA", "ALTER TABLE", "CREATE TABLE", "CREATE INDEX",
                 "TRUNCATE", "GRANT ", "COMMIT"):
        check(f"no {stmt.strip()} statement", stmt not in src, stmt)
    check("the connection is set read-only", "SET TRANSACTION READ ONLY" in src)


def main() -> int:
    print("=" * 70)
    print("pnl_report — offline checks")
    print("=" * 70)
    test_gross_and_profit_are_disjoint()
    test_gross_of_ignores_absent_columns()
    test_nulls_counted_not_summed()
    test_manual_days_flagged()
    test_period_keys()
    test_periods_between()
    test_days_in_range_clips()
    test_roll_recon()
    test_roll_recon_falls_back_to_key()
    test_report_runs_on_a_thin_database()
    test_report_says_so_when_empty()
    test_json_shape()
    test_no_writes_anywhere()
    print("\n" + "=" * 70)
    print(f"{PASS} passed, {FAIL} failed")
    return 1 if FAIL else 0


if __name__ == "__main__":
    sys.exit(main())
