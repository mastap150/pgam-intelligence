#!/usr/bin/env python3
"""
Offline checks for the platform revenue rollup.

The whole risk in this report is one mistake — adding TB legacy to TBX and
double-counting the marketplace. So the tests are weighted there: the stitch
must pick exactly one side per day, must never sum, and must say which side it
picked. Period bucketing is checked because a quarter boundary off by one
silently moves money between quarters.

No credentials, no database.
"""

from __future__ import annotations

import sys
from datetime import date

sys.path.insert(0, __file__.rsplit("/tests/", 1)[0])

from scripts import platform_revenue as pr   # noqa: E402

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


def row(gross, payout=0.0, imps=0):
    return {"gross": gross, "payout": payout, "imps": imps}


CUT = d("2026-08-20")


# ---------------------------------------------------------------------------
# the stitch — the double-count guard
# ---------------------------------------------------------------------------

def test_stitch_never_sums() -> None:
    print("\nthe stitch never adds the two hosts")
    tb = {d("2026-08-19"): row(100.0), d("2026-08-20"): row(200.0)}
    tbx = {d("2026-08-19"): row(999.0), d("2026-08-20"): row(300.0)}
    out, origin = pr.stitch_tb(tb, tbx, CUT)
    check("an overlap day yields one value, not the sum",
          out[d("2026-08-20")]["gross"] == 300.0,
          f"got {out[d('2026-08-20')]['gross']}")
    check("before the cutover legacy wins",
          out[d("2026-08-19")]["gross"] == 100.0)
    check("from the cutover TBX wins",
          origin[d("2026-08-20")] == "tbx")
    total = sum(v["gross"] for v in out.values())
    check("stitched total is not the sum of both tables",
          total == 400.0 and total != 100.0 + 200.0 + 999.0 + 300.0,
          f"got {total}")


def test_cutover_is_inclusive() -> None:
    print("\nthe cutover day belongs to TBX")
    tb = {CUT: row(10.0)}
    tbx = {CUT: row(20.0)}
    out, origin = pr.stitch_tb(tb, tbx, CUT)
    check("day == cutover takes TBX", out[CUT]["gross"] == 20.0)
    check("and is labelled tbx", origin[CUT] == "tbx")


def test_fallback_fills_gaps_and_is_marked() -> None:
    print("\na missing authoritative row falls back, and says so")
    # after the cutover but TBX has no row: legacy fills rather than dropping
    tb = {d("2026-08-25"): row(50.0)}
    tbx: dict = {}
    out, origin = pr.stitch_tb(tb, tbx, CUT)
    check("the day survives", out[d("2026-08-25")]["gross"] == 50.0)
    check("and is flagged as a fallback", origin[d("2026-08-25")] == "tb*")

    # before the cutover but legacy has no row
    out2, origin2 = pr.stitch_tb({}, {d("2026-03-02"): row(7.0)}, CUT)
    check("works in the other direction too", origin2[d("2026-03-02")] == "tbx*")


def test_dropping_a_day_would_understate() -> None:
    print("\nno day is silently dropped")
    tb = {d("2026-08-18"): row(5.0)}
    tbx = {d("2026-08-22"): row(9.0)}
    out, _ = pr.stitch_tb(tb, tbx, CUT)
    check("both days present", sorted(out) == [d("2026-08-18"), d("2026-08-22")])
    check("totals add across days (not across hosts)",
          sum(v["gross"] for v in out.values()) == 14.0)


def test_overlaps_reported() -> None:
    print("\noverlap days are surfaced")
    tb = {d("2026-08-19"): row(1), d("2026-08-20"): row(1)}
    tbx = {d("2026-08-20"): row(1), d("2026-08-21"): row(1)}
    check("only the shared day is listed",
          pr.overlaps(tb, tbx) == [d("2026-08-20")])
    check("no overlap reads empty", pr.overlaps(tb, {}) == [])


# ---------------------------------------------------------------------------
# periods
# ---------------------------------------------------------------------------

def test_period_key() -> None:
    print("\nperiod bucketing")
    check("month", pr.period_key(d("2026-03-09"), "month") == "2026-03")
    check("Q1 ends 31 Mar", pr.period_key(d("2026-03-31"), "quarter") == "2026-Q1")
    check("Q2 starts 1 Apr", pr.period_key(d("2026-04-01"), "quarter") == "2026-Q2")
    check("Q3 boundary", pr.period_key(d("2026-07-01"), "quarter") == "2026-Q3")
    check("Q4 boundary", pr.period_key(d("2026-10-01"), "quarter") == "2026-Q4")
    check("year carries", pr.period_key(d("2025-12-31"), "quarter") == "2025-Q4")


def test_roll_up_maths() -> None:
    print("\nroll-up arithmetic")
    series = {
        d("2026-01-05"): {"gross": 100.0, "payout": 70.0, "imps": 1_000_000},
        d("2026-01-06"): {"gross": 300.0, "payout": 210.0, "imps": 3_000_000},
        d("2026-02-01"): {"gross": 50.0, "payout": 25.0, "imps": 500_000},
    }
    out = pr.roll_up(series, "month")
    jan = out["2026-01"]
    check("gross sums", jan["gross"] == 400.0)
    check("profit is gross minus payout", jan["profit"] == 120.0)
    check("margin is a percentage", abs(jan["margin"] - 30.0) < 1e-9,
          f"got {jan['margin']}")
    check("eCPM is gross per thousand imps", abs(jan["ecpm"] - 0.1) < 1e-9,
          f"got {jan['ecpm']}")
    check("day count carried", jan["days"] == 2)
    check("february is its own bucket", out["2026-02"]["gross"] == 50.0)

    q = pr.roll_up(series, "quarter")
    check("all three land in Q1", q["2026-Q1"]["days"] == 3)
    check("quarter gross is the sum", q["2026-Q1"]["gross"] == 450.0)


def test_zero_safe() -> None:
    print("\nzeros do not divide")
    out = pr.roll_up({d("2026-05-01"): {"gross": 0.0, "payout": 0.0, "imps": 0}},
                     "month")
    check("no ZeroDivisionError on margin", out["2026-05"]["margin"] == 0.0)
    check("no ZeroDivisionError on eCPM", out["2026-05"]["ecpm"] == 0.0)


def main() -> int:
    print("=" * 70)
    print("platform_revenue — offline checks")
    print("=" * 70)
    test_stitch_never_sums()
    test_cutover_is_inclusive()
    test_fallback_fills_gaps_and_is_marked()
    test_dropping_a_day_would_understate()
    test_overlaps_reported()
    test_period_key()
    test_roll_up_maths()
    test_zero_safe()
    print("\n" + "=" * 70)
    print(f"{PASS} passed, {FAIL} failed")
    return 1 if FAIL else 0


if __name__ == "__main__":
    sys.exit(main())
