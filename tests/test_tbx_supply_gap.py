"""Checks for scripts/tbx_supply_gap.py — offline, no database."""
from __future__ import annotations

import os
import sys
from datetime import date

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(ROOT, "scripts"))

import tbx_supply_gap as g  # noqa: E402

CHECKS = []


def check(name):
    def deco(fn):
        CHECKS.append((name, fn))
        return fn
    return deco


CUT = date(2026, 8, 21)


@check("the before window is the days up to but not including the cutover")
def _():
    before, _ = g.windows(CUT, 4, date(2026, 8, 25))
    assert before == [date(2026, 8, 17), date(2026, 8, 18),
                      date(2026, 8, 19), date(2026, 8, 20)], before


@check("the after window starts on the cutover and never includes today")
def _():
    _, after = g.windows(CUT, 4, date(2026, 8, 25))
    assert after == [date(2026, 8, 21), date(2026, 8, 22),
                     date(2026, 8, 23), date(2026, 8, 24)], after


@check("a partial after-window is clipped to settled days, not padded")
def _():
    # Two days after the cutover: only the 21st and 22nd have closed.
    _, after = g.windows(CUT, 4, date(2026, 8, 23))
    assert after == [date(2026, 8, 21), date(2026, 8, 22)], after


@check("a publisher with no rows after the cutover is GONE")
def _():
    before = {"acme": {"name": "Acme", "id": 7, "imps": 4_000_000,
                       "gross": 400.0, "active_days": 4}}
    res = g.classify(before, {}, 60.0, 4, 4)
    assert [r["name"] for r in res["gone"]] == ["Acme"]
    assert res["gone"][0]["before_gross_per_day"] == 100.0


@check("a publisher below the revenue floor is not reported as GONE")
def _():
    before = {"tiny": {"name": "Tiny", "id": 1, "imps": 10,
                       "gross": 0.4, "active_days": 1}}
    res = g.classify(before, {}, 60.0, 4, 4)
    assert res["gone"] == [], res["gone"]


@check("unequal window lengths compare rates, not totals")
def _():
    # Same impressions per day on both sides, but the after window is half as
    # long. Comparing sums would call this a 50% collapse.
    before = {"acme": {"name": "Acme", "id": 7, "imps": 4_000_000,
                       "gross": 400.0, "active_days": 4}}
    after = {"acme": {"name": "Acme", "id": 9, "imps": 2_000_000,
                      "gross": 200.0, "active_days": 2}}
    res = g.classify(before, after, 60.0, 4, 2)
    assert res["gone"] == []
    assert res["quiet"] == [], res["quiet"]
    assert len(res["carried"]) == 1
    assert abs(res["carried"][0]["drop_pct"]) < 1e-9


@check("a partner reduced to a trickle is QUIET, not carried")
def _():
    before = {"acme": {"name": "Acme", "id": 7, "imps": 4_000_000,
                       "gross": 400.0, "active_days": 4}}
    after = {"acme": {"name": "Acme", "id": 9, "imps": 400_000,
                      "gross": 40.0, "active_days": 4}}
    res = g.classify(before, after, 60.0, 4, 4)
    assert [r["name"] for r in res["quiet"]] == ["Acme"]
    assert abs(res["quiet"][0]["drop_pct"] - 90.0) < 1e-9


@check("the quiet threshold is a boundary, not a strict inequality")
def _():
    before = {"a": {"name": "A", "id": 1, "imps": 1000, "gross": 100.0,
                    "active_days": 1}}
    after = {"a": {"name": "A", "id": 1, "imps": 400, "gross": 40.0,
                   "active_days": 1}}
    res = g.classify(before, after, 60.0, 1, 1)   # exactly 60% down
    assert len(res["quiet"]) == 1, res


@check("a renamed publisher shows on both GONE and NEW, never silently dropped")
def _():
    before = {"acme media": {"name": "Acme Media", "id": 7, "imps": 4_000_000,
                             "gross": 400.0, "active_days": 4}}
    after = {"acme media ltd": {"name": "Acme Media Ltd", "id": 9,
                                "imps": 4_000_000, "gross": 400.0,
                                "active_days": 4}}
    res = g.classify(before, after, 60.0, 4, 4)
    assert [r["name"] for r in res["gone"]] == ["Acme Media"]
    assert [r["name"] for r in res["arrived"]] == ["Acme Media Ltd"]


@check("ids are never used to match — only reported")
def _():
    # Same name, completely different ids: still one carried partner.
    before = {"acme": {"name": "Acme", "id": 7, "imps": 1000, "gross": 100.0,
                       "active_days": 1}}
    after = {"acme": {"name": "Acme", "id": 99999, "imps": 1000,
                      "gross": 100.0, "active_days": 1}}
    res = g.classify(before, after, 60.0, 1, 1)
    assert len(res["carried"]) == 1
    assert res["carried"][0]["legacy_id"] == 7
    assert res["carried"][0]["tbx_id"] == 99999


@check("GONE is ordered by the revenue at stake, biggest first")
def _():
    before = {
        "small": {"name": "Small", "id": 1, "imps": 100, "gross": 10.0,
                  "active_days": 1},
        "big": {"name": "Big", "id": 2, "imps": 10_000, "gross": 1000.0,
                "active_days": 1},
        "mid": {"name": "Mid", "id": 3, "imps": 1000, "gross": 100.0,
                "active_days": 1},
    }
    res = g.classify(before, {}, 60.0, 1, 1)
    assert [r["name"] for r in res["gone"]] == ["Big", "Mid", "Small"]


@check("a publisher that grew is carried, with a negative drop")
def _():
    before = {"acme": {"name": "Acme", "id": 1, "imps": 1000, "gross": 100.0,
                       "active_days": 1}}
    after = {"acme": {"name": "Acme", "id": 1, "imps": 2000, "gross": 200.0,
                      "active_days": 1}}
    res = g.classify(before, after, 60.0, 1, 1)
    assert len(res["carried"]) == 1
    assert res["carried"][0]["drop_pct"] < 0


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
