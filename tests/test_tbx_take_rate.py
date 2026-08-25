"""Checks for scripts/tbx_take_rate.py — offline, no database."""
from __future__ import annotations

import os
import sys
from datetime import date, datetime

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(ROOT, "scripts"))

import tbx_take_rate as t  # noqa: E402

CHECKS = []


def check(name):
    def deco(fn):
        CHECKS.append((name, fn))
        return fn
    return deco


TARGET = date(2026, 8, 24)


def series(name, days):
    """days: [(date, gross, payout)] -> the shape assess() takes."""
    return {name: days}


def steady(margin_pct, gross, n, end=TARGET, start_offset=1):
    """n days ending the day before `end`, all at the same margin."""
    return [(end - __import__("datetime").timedelta(days=i), gross,
             gross * (1 - margin_pct / 100))
            for i in range(start_offset, start_offset + n)]


@check("margin is gross minus payout over gross")
def _():
    assert abs(t.margin(100.0, 70.0) - 30.0) < 1e-9


@check("margin of a day with no gross is None, not a divide-by-zero")
def _():
    assert t.margin(0.0, 0.0) is None


@check("the settled day is yesterday once the ET close has passed")
def _():
    # 13:00 UTC on the 25th — well past the 04:00/05:00 close of the 24th.
    assert t.latest_settled(datetime(2026, 8, 25, 13, 0)) == date(2026, 8, 24)


@check("just after midnight UTC the day before yesterday is still the latest")
def _():
    # 01:27 UTC on the 25th — the 24th has NOT closed in ET yet. This is the
    # exact reading that made a partial day look like a complete one.
    assert t.latest_settled(datetime(2026, 8, 25, 1, 27)) == date(2026, 8, 23)


@check("a source holding its own margin is not flagged")
def _():
    s = series("Steady", steady(30.0, 1000.0, 10) + [(TARGET, 1000.0, 700.0)])
    [r] = t.assess(s, TARGET, 3.0, 25.0)
    assert not r["flagged"], r
    assert abs(r["delta_points"]) < 1e-9


@check("a source that drops past the threshold is flagged")
def _():
    s = series("Slipped", steady(30.0, 1000.0, 10) + [(TARGET, 1000.0, 780.0)])
    [r] = t.assess(s, TARGET, 3.0, 25.0)
    assert r["flagged"]
    assert abs(r["delta_points"] - (-8.0)) < 1e-9
    assert abs(r["profit_delta_usd"] - (-80.0)) < 1e-9


@check("a low margin that has always been low is not flagged")
def _():
    # 12% forever. Below the book average, but this source's normal.
    s = series("AlwaysThin", steady(12.0, 1000.0, 10) + [(TARGET, 1000.0, 880.0)])
    [r] = t.assess(s, TARGET, 3.0, 25.0)
    assert not r["flagged"], r


@check("a high margin that fell is flagged even while above the book average")
def _():
    # 35% → 25%. Still a good margin; still a 10-point loss.
    s = series("WasRich", steady(35.0, 1000.0, 10) + [(TARGET, 1000.0, 750.0)])
    [r] = t.assess(s, TARGET, 3.0, 25.0)
    assert r["flagged"]
    assert abs(r["delta_points"] - (-10.0)) < 1e-9


@check("days below the revenue floor cannot set the baseline")
def _():
    import datetime as _dt
    # Nine thin days at a wild margin, plus three real ones at 30%.
    thin = [(TARGET - _dt.timedelta(days=i), 3.0, 0.3) for i in range(4, 13)]
    real = steady(30.0, 1000.0, 3)
    s = series("Mixed", thin + real + [(TARGET, 1000.0, 700.0)])
    [r] = t.assess(s, TARGET, 3.0, 25.0)
    assert r["history_days"] == 3, r["history_days"]
    assert not r["flagged"], r


@check("a thin target day is dropped rather than compared")
def _():
    s = series("ThinToday", steady(30.0, 1000.0, 10) + [(TARGET, 3.0, 0.3)])
    assert t.assess(s, TARGET, 3.0, 25.0) == []


@check("fewer than three comparable days is not assessed")
def _():
    s = series("Newborn", steady(30.0, 1000.0, 2) + [(TARGET, 1000.0, 500.0)])
    assert t.assess(s, TARGET, 3.0, 25.0) == []


@check("a source with no row on the target day is skipped, not treated as zero")
def _():
    s = series("Absent", steady(30.0, 1000.0, 10))
    assert t.assess(s, TARGET, 3.0, 25.0) == []


@check("the dollar impact scales with gross, not with the point move")
def _():
    small = series("Small", steady(30.0, 100.0, 10) + [(TARGET, 100.0, 78.0)])
    big = series("Big", steady(30.0, 10_000.0, 10) + [(TARGET, 10_000.0, 7800.0)])
    [rs] = t.assess(small, TARGET, 3.0, 25.0)
    [rb] = t.assess(big, TARGET, 3.0, 25.0)
    assert abs(rs["delta_points"] - rb["delta_points"]) < 1e-9
    assert abs(rb["profit_delta_usd"]) > abs(rs["profit_delta_usd"]) * 99


@check("results are ordered worst-money-first")
def _():
    s = {}
    s.update(series("Tiny", steady(30.0, 100.0, 10) + [(TARGET, 100.0, 78.0)]))
    s.update(series("Huge", steady(30.0, 9000.0, 10) + [(TARGET, 9000.0, 7020.0)]))
    s.update(series("Rose", steady(30.0, 1000.0, 10) + [(TARGET, 1000.0, 620.0)]))
    got = [r["name"] for r in t.assess(s, TARGET, 3.0, 25.0)]
    assert got[0] == "Huge", got
    assert got[-1] == "Rose", got


@check("a margin that improved is reported, with a positive delta")
def _():
    s = series("Improved", steady(20.0, 1000.0, 10) + [(TARGET, 1000.0, 700.0)])
    [r] = t.assess(s, TARGET, 3.0, 25.0)
    assert r["flagged"]
    assert r["delta_points"] > 0
    assert r["profit_delta_usd"] > 0


def main() -> int:
    failed = 0
    for name, fn in CHECKS:
        try:
            fn()
            print(f"  ✓ {name}")
        except AssertionError as e:
            failed += 1
            print(f"  ✗ {name}\n      {e}")
    print(f"\n{len(CHECKS)} checks, {failed} failed")
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
