"""
tests/test_hasib_month_report.py

Standalone, no-network/no-DB tests for scripts/hasib_month_report.py.

Covers the parts that are easy to get silently wrong and impossible to
check by eye once the report is only ever delivered to Slack: month
boundary arithmetic (including the December and January rollovers), the
"which month is settled yet" default, and the report body itself.

Run:
    python tests/test_hasib_month_report.py

Exits non-zero on any failure. No pytest dependency, matching the rest of
tests/.
"""

from __future__ import annotations

import sys
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from scripts.hasib_month_report import (  # noqa: E402
    PAYOUT_MULTIPLIER,
    default_month,
    est_payout,
    format_report,
    month_bounds,
    previous_month,
)
from scripts.msn_lane_performance import Cohort  # noqa: E402

failures: list[str] = []


def check(label: str, got, want) -> None:
    if got != want:
        failures.append(f"{label}: got {got!r}, want {want!r}")


def check_true(label: str, cond: bool) -> None:
    if not cond:
        failures.append(label)


# ── month_bounds ───────────────────────────────────────────────────────────
check("month_bounds mid-year", month_bounds("2026-08"), (date(2026, 8, 1), date(2026, 9, 1)))
check("month_bounds December rolls the year",
      month_bounds("2026-12"), (date(2026, 12, 1), date(2027, 1, 1)))
check("month_bounds January", month_bounds("2026-01"), (date(2026, 1, 1), date(2026, 2, 1)))

for bad in ("2026-13", "august", "2026", ""):
    try:
        month_bounds(bad)
        failures.append(f"month_bounds accepted invalid input {bad!r}")
    except SystemExit:
        pass

# ── previous_month ─────────────────────────────────────────────────────────
check("previous_month mid-year", previous_month("2026-08"), "2026-07")
check("previous_month January rolls back a year", previous_month("2026-01"), "2025-12")

# ── default_month ──────────────────────────────────────────────────────────
# Inside the 5-day settle window at the start of September, August is not
# trustworthy yet, so the default steps back to July.
check("default_month on the 1st skips the unsettled month",
      default_month(date(2026, 9, 1)), "2026-07")
check("default_month past the settle window uses last month",
      default_month(date(2026, 9, 10)), "2026-08")
check("default_month on the 6th uses last month",
      default_month(date(2026, 9, 6)), "2026-08")
check("default_month in January reaches back across the year",
      default_month(date(2026, 1, 2)), "2025-11")

# ── est_payout ─────────────────────────────────────────────────────────────
c = Cohort("x")
c.total_pvs = 10_000
check("est_payout applies CPM and payout multiplier",
      round(est_payout(c), 2), round(10_000 * 4.0 / 1000.0 * PAYOUT_MULTIPLIER, 2))
check("payout multiplier matches the trigger check", PAYOUT_MULTIPLIER, 1.6)


# ── format_report ──────────────────────────────────────────────────────────
def make(articles: int, on_msn: int, total_pvs: int, ge_1k: int = 0, max_pv: int = 0) -> Cohort:
    ch = Cohort("c")
    ch.articles, ch.on_msn, ch.total_pvs = articles, on_msn, total_pvs
    ch.ge_1k, ch.max_pv = ge_1k, max_pv
    return ch


cur = {
    "rotation": make(400, 300, 600_000, ge_1k=120, max_pv=40_000),
    "hasib": make(150, 100, 150_000, ge_1k=20, max_pv=9_000),
    "fighter-angle": make(0, 0, 0),
}
prev = {
    "rotation": make(300, 200, 300_000),
    "hasib": make(155, 110, 200_000),
    "fighter-angle": make(0, 0, 0),
}

report = format_report("2026-08", cur, "2026-07", prev, None)

check_true("report names the month", "2026-08" in report)
check_true("report carries Hasib article count", "150" in report)
check_true("report states the total-views ratio", "4.0x" in report)
check_true("report reports Hasib cost at $11/article", "$1,650" in report)
check_true("report shows per-ingested views for both cohorts",
           "2,000" in report and "1,500" in report)
check_true("report shows AI views up vs prior month", "+100%" in report)
check_true("report shows Hasib views down vs prior month", "-25%" in report)
check_true("clean window carries no staleness warning", "UNDER-count" not in report)

stale_report = format_report("2026-08", cur, "2026-07", prev, 2)
check_true("stale window warns", "UNDER-count" in stale_report)
check_true("stale window says how long to wait", "3d" in stale_report)

# A zero-denominator month must not raise — Hasib was cut, or the ETL broke.
zero = {"rotation": make(400, 300, 600_000), "hasib": make(0, 0, 0),
        "fighter-angle": make(0, 0, 0)}
zero_report = format_report("2026-09", zero, "2026-08", prev, None)
check_true("zero Hasib views does not divide by zero", "vs Hasib 0" in zero_report)
check_true("zero Hasib articles reports $0 cost", "$0" in zero_report)

if failures:
    print(f"FAIL — {len(failures)} problem(s):")
    for f in failures:
        print(f"  - {f}")
    sys.exit(1)

print("ok — hasib_month_report: month math, payout, and report body all pass")
