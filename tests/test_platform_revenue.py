#!/usr/bin/env python3
"""
Offline checks for the platform revenue rollup.

The risk here is not the arithmetic, it is ownership. Combining TB legacy with
TBX is a rule with three phases and a split window where the two ARE summed,
and `core/tb_unified` owns it for every consumer. An earlier draft of this
script reimplemented it as a plain two-phase cutover, which silently dropped
the legacy half of every split day — $4,900 on 2026-08-20 alone.

So the checks are weighted on delegation: the TB leg must come out of
`tb_unified.fetch()` unchanged, the origin label must come from
`tb_unified.legs_for()` rather than being re-derived here, and a split day
must survive at its summed value. Period bucketing is checked because a
quarter boundary off by one silently moves money between quarters.

No credentials, no database — `fetch` is stubbed.
"""

from __future__ import annotations

import sys
from datetime import date

sys.path.insert(0, __file__.rsplit("/tests/", 1)[0])

from core import tb_unified as u             # noqa: E402
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
# delegation — the TB rule is not ours
# ---------------------------------------------------------------------------

def stub_fetch(rows):
    """Stand in for tb_unified.fetch, which needs a database."""
    def _f(breakdown, metrics, start, end, side="supply"):
        return rows
    return _f


def urow(day, gross, payout=0.0, imps=0):
    return {"DATE": day, "PUBLISHER": None, "GROSS_REVENUE": gross,
            "PUB_PAYOUT": payout, "IMPRESSIONS": float(imps),
            "BIDS": 0.0, "WINS": 0.0}


def with_stub(rows, fn):
    real = u.fetch
    u.fetch = stub_fetch(rows)
    try:
        return fn()
    finally:
        u.fetch = real


def test_tb_leg_passes_through() -> None:
    print("\nthe TB leg is tb_unified's answer, unchanged")
    rows = [urow("2026-08-19", 8011.13, 5546.89, 8713946),
            urow("2026-08-20", 7505.66, 5389.68, 9689743)]
    out, origin = with_stub(rows, lambda: pr.tb_leg(d("2026-08-19"), d("2026-08-20")))
    check("gross carried verbatim", out[d("2026-08-20")]["gross"] == 7505.66,
          f"got {out[d('2026-08-20')]['gross']}")
    check("payout carried verbatim", out[d("2026-08-20")]["payout"] == 5389.68)
    check("impressions become an int", out[d("2026-08-20")]["imps"] == 9689743)
    check("every returned day is kept", len(out) == 2)


def test_split_day_keeps_both_hosts() -> None:
    print("\na split day survives at its summed value")
    # 2026-08-20 is inside tb_unified's split window, where legacy and TBX are
    # complementary. A two-phase cutover would return TBX's 2,605.46 here.
    out, origin = with_stub([urow("2026-08-20", 7505.66, 5389.68, 9689743)],
                            lambda: pr.tb_leg(d("2026-08-20"), d("2026-08-20")))
    check("value is the summed one, not one host's",
          out[d("2026-08-20")]["gross"] == 7505.66 != 2605.46)
    check("and it is labelled as both", origin[d("2026-08-20")] == "legacy+tbx",
          f"got {origin[d('2026-08-20')]!r}")


def test_origin_labels_come_from_legs_for() -> None:
    print("\norigin labels are tb_unified's, not re-derived")
    rows = [urow("2026-08-19", 1.0), urow("2026-08-20", 1.0), urow("2026-08-21", 1.0)]
    _, origin = with_stub(rows, lambda: pr.tb_leg(d("2026-08-19"), d("2026-08-21")))
    for day, expect in (("2026-08-19", "legacy"),
                        ("2026-08-20", "legacy+tbx"),
                        ("2026-08-21", "tbx")):
        legacy_leg, tbx_leg = u.legs_for(d(day))
        want = ("legacy+tbx" if legacy_leg and tbx_leg
                else "tbx" if tbx_leg else "legacy")
        check(f"{day} labelled {expect}",
              origin[d(day)] == expect == want, f"got {origin[d(day)]!r}")


def test_no_local_rule() -> None:
    print("\nthe module defines no cutover of its own")
    src = open(pr.__file__).read()
    check("no stitch_tb function survives", "def stitch_tb" not in src)
    check("no DEFAULT_CUTOVER constant", "DEFAULT_CUTOVER" not in src)
    check("no --cutover flag to drift from tb_unified's env vars",
          '"--cutover"' not in src)
    check("tb_unified is imported", "from core import tb_unified" in src)


def test_overlaps_reported() -> None:
    print("\noverlap days are still surfaced for coverage")
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


# ---------------------------------------------------------------------------
# exclusion
# ---------------------------------------------------------------------------

def test_exclude_filters_by_entity() -> None:
    print("\nexclusion drops matching counterparties, case-insensitively")
    rows=[urow("2026-08-25",100.0,60.0,1000), urow("2026-08-25",25.0,15.0,250),
          urow("2026-08-25",7.0,4.0,70)]
    rows[0]["PUBLISHER"]="Illumin Display and Video"
    rows[1]["PUBLISHER"]="Illumin - Video Unruly OTTA"
    rows[2]["PUBLISHER"]="otta olv rtb"          # lowercase — must still match
    out,_=with_stub(rows, lambda: pr.tb_leg(d("2026-08-25"), d("2026-08-25"), "OTTA"))
    check("only the non-matching row survives", out[d("2026-08-25")]["gross"]==100.0,
          out[d("2026-08-25")]["gross"])
    check("lowercase name is matched too", out[d("2026-08-25")]["imps"]==1000)


def test_exclude_folds_remaining_entities() -> None:
    print("\nsurviving entities are folded back to one row per day")
    rows=[urow("2026-08-25",100.0,60.0,1000), urow("2026-08-25",50.0,30.0,500)]
    rows[0]["PUBLISHER"]="A"; rows[1]["PUBLISHER"]="B"
    out,_=with_stub(rows, lambda: pr.tb_leg(d("2026-08-25"), d("2026-08-25"), "ZZZ"))
    check("both kept and summed", out[d("2026-08-25")]["gross"]==150.0)
    check("payout summed", out[d("2026-08-25")]["payout"]==90.0)
    check("impressions summed", out[d("2026-08-25")]["imps"]==1500)


def test_exclude_switches_breakdown() -> None:
    print("\nexcluding reads the entity breakdown, not the date one")
    seen={}
    real=u.fetch
    def spy(breakdown,metrics,start,end,side="supply"):
        seen["breakdown"]=breakdown; seen["side"]=side; return []
    u.fetch=spy
    try:
        pr.tb_leg(d("2026-08-25"), d("2026-08-25"))
        check("no exclusion -> DATE breakdown", seen["breakdown"]=="DATE", seen)
        pr.tb_leg(d("2026-08-25"), d("2026-08-25"), "OTTA", "demand")
        check("exclusion -> PUBLISHER breakdown", seen["breakdown"]=="PUBLISHER", seen)
        check("side is passed through", seen["side"]=="demand", seen)
    finally:
        u.fetch=real


def test_heavier_is_conservative() -> None:
    print("\nthe headline takes whichever side removes more")
    sup={d("2026-08-25"):row(80.0)}     # removed more -> less left
    dem={d("2026-08-25"):row(95.0)}
    keep,side=pr.heavier(sup,dem)
    check("keeps the smaller remainder", keep is sup and side=="supply",
          (side, sum(v["gross"] for v in keep.values())))
    keep2,side2=pr.heavier(dem,sup)
    check("orientation does not matter", keep2 is sup and side2=="demand", side2)


def test_tb_unified_has_both_sides() -> None:
    print("\ntb_unified owns both sides, so no second rule exists")
    check("supply and demand both declared", set(u.SIDES)=={"supply","demand"}, set(u.SIDES))
    check("demand uses the demand tables",
          u.SIDES["demand"][0].endswith("tb_daily_demand_revenue") and
          u.SIDES["demand"][2].endswith("tbx_daily_demand_revenue"), u.SIDES["demand"])
    try:
        u.fetch("DATE",[],"2026-08-01","2026-08-02",side="sideways"); bad=False
    except ValueError: bad=True
    except Exception: bad=False
    check("side is validated before any query", bad)


def main() -> int:
    print("=" * 70)
    print("platform_revenue — offline checks")
    print("=" * 70)
    test_tb_leg_passes_through()
    test_split_day_keeps_both_hosts()
    test_origin_labels_come_from_legs_for()
    test_no_local_rule()
    test_overlaps_reported()
    test_period_key()
    test_roll_up_maths()
    test_zero_safe()
    test_exclude_filters_by_entity()
    test_exclude_folds_remaining_entities()
    test_exclude_switches_breakdown()
    test_heavier_is_conservative()
    test_tb_unified_has_both_sides()
    print("\n" + "=" * 70)
    print(f"{PASS} passed, {FAIL} failed")
    return 1 if FAIL else 0


if __name__ == "__main__":
    sys.exit(main())
