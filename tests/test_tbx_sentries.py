#!/usr/bin/env python3
"""
Offline checks for the two scheduled sentries.

Both post to a Slack channel people are expected to act on, so the tests are
weighted at the two ways a sentry destroys its own value: crying wolf every
day until nobody reads it, and staying quiet through the one event it exists
to catch.

No credentials, no platform call.
"""

from __future__ import annotations

import sys
from datetime import date, date as _date

sys.path.insert(0, __file__.rsplit("/tests/", 1)[0])

from scripts import tbx_margin_sentry as m   # noqa: E402
from scripts import tbx_nowin_watch as w     # noqa: E402
from scripts import tbx_trim as t_trim       # noqa: E402

PASS = FAIL = 0


def check(label: str, ok: bool, detail: str = "") -> None:
    global PASS, FAIL
    if ok:
        PASS += 1
        print(f"  ✓ {label}")
    else:
        FAIL += 1
        print(f"  ✗ {label}" + (f"  — {detail}" if detail else ""))


# ---------------------------------------------------------------------------
# margin sentry
# ---------------------------------------------------------------------------

def src(sid, name, take, gross, profit=0.0):
    return {"id": sid, "name": name, "take_rate": take,
            "gross_day": gross, "profit_day": profit}


def test_floor_and_ceiling() -> None:
    print("\nreading a band out of a config")
    check("fixed uses margin_min as the floor",
          m.floor_for({"margin_type": "fixed", "margin_min": 10, "margin_max": 0}) == 10.0)
    # The trap: margin_max comes back 0 on a fixed source. Read as a ceiling,
    # every fixed source is permanently "above band".
    check("fixed has NO ceiling — margin_max 0 is not one",
          m.ceiling_for({"margin_type": "fixed", "margin_min": 10, "margin_max": 0}) is None)
    check("range floor", m.floor_for({"margin_type": "range", "margin_min": 5,
                                      "margin_max": 30}) == 5.0)
    check("range ceiling", m.ceiling_for({"margin_type": "range", "margin_min": 5,
                                          "margin_max": 30}) == 30.0)
    check("adaptive is treated as a band too",
          m.floor_for({"margin_type": "adaptive", "margin_min": 5,
                       "margin_max": 95}) == 5.0)
    check("an unknown margin_type yields no floor rather than a guess",
          m.floor_for({"margin_type": "weird", "margin_min": 9}) is None)
    check("a missing margin_type yields no floor",
          m.floor_for({}) is None)
    check("a zero margin_max on a range is not a ceiling either",
          m.ceiling_for({"margin_type": "range", "margin_min": 5,
                         "margin_max": 0}) is None)


def test_below_band() -> None:
    print("\nbelow the floor is the alerting direction")
    cfg = {
        1: {"margin_type": "range", "margin_min": 20, "margin_max": 40},
        2: {"margin_type": "fixed", "margin_min": 10, "margin_max": 0},
        3: {"margin_type": "range", "margin_min": 5, "margin_max": 30},
    }
    rows = [
        src(1, "Way Under", 7.6, 1000.0),   # 12.4 points under a 20% floor
        src(2, "On Target", 10.4, 500.0),   # inside tolerance of a fixed 10
        src(3, "Comfortable", 19.0, 800.0),  # inside a 5-30 band
    ]
    got = {r["id"]: r for r in m.assess(rows, cfg, tolerance=1.0, min_gross_day=10.0)}
    check("a source under its floor is flagged BELOW",
          got[1]["verdict"] == "BELOW", str(got[1]["verdict"]))
    check("a fixed source within tolerance is not flagged",
          got[2]["verdict"] is None, str(got[2]["verdict"]))
    check("a source inside its band is not flagged",
          got[3]["verdict"] is None, str(got[3]["verdict"]))
    # 12.4 points of $1000/day = $124/day
    check("the shortfall is points x gross, per day",
          abs(got[1]["shortfall_day"] - 124.0) < 0.01, str(got[1]["shortfall_day"]))
    check("an unflagged source carries no shortfall to sum",
          got[3]["shortfall_day"] == 0.0)


def test_above_band_is_not_a_shortfall() -> None:
    print("\nabove the ceiling is informational, never a shortfall")
    cfg = {9: {"margin_type": "range", "margin_min": 12, "margin_max": 20}}
    got = m.assess([src(9, "Over", 28.2, 400.0)], cfg,
                   tolerance=1.0, min_gross_day=10.0)[0]
    check("above the ceiling is flagged ABOVE", got["verdict"] == "ABOVE")
    # This is the one that would corrupt the headline number: an above-band
    # row has a NEGATIVE gap, so summing it into the shortfall would net off
    # against real below-band money and understate the total.
    check("an above-band row contributes ZERO to the shortfall total",
          got["shortfall_day"] == 0.0, str(got["shortfall_day"]))


def test_noise_floors() -> None:
    print("\nwhat the sentry refuses to have an opinion on")
    cfg = {
        1: {"margin_type": "fixed", "margin_min": 10, "margin_max": 0},
        2: {"margin_type": "fixed", "margin_min": 10, "margin_max": 0},
    }
    tiny = m.assess([src(1, "Tiny", 0.5, 3.0)], cfg, 1.0, min_gross_day=10.0)
    check("a source under the revenue floor gets no verdict at all",
          tiny == [], str(tiny))

    no_cfg = m.assess([src(99, "No Config", 2.0, 900.0)], cfg, 1.0, 10.0)
    check("a source whose config could not be read is skipped, not flagged",
          no_cfg == [], str(no_cfg))

    no_take = m.assess([{"id": 2, "name": "No Revenue", "take_rate": None,
                         "gross_day": 900.0}], cfg, 1.0, 10.0)
    check("a source with no take rate is skipped rather than read as 0%",
          no_take == [], str(no_take))

    # Tolerance exists because a blended 7-day average never lands exactly on
    # a fixed number; without it the sentry fires on rounding forever.
    edge = m.assess([src(1, "Just Under", 9.5, 900.0)], cfg, 1.0, 10.0)[0]
    check("half a point under a fixed 10 is inside tolerance",
          edge["verdict"] is None, str(edge["verdict"]))
    past = m.assess([src(1, "Past It", 8.5, 900.0)], cfg, 1.0, 10.0)[0]
    check("1.5 points under is past tolerance", past["verdict"] == "BELOW")


def test_margin_slack_silence() -> None:
    print("\nthe margin sentry stays quiet when there is nothing to say")
    posted = []
    real = m.to_slack.__globals__.get("send_blocks")
    import core.slack as slack
    orig = slack.send_blocks
    slack.send_blocks = lambda blocks, text="": posted.append(text)
    try:
        m.to_slack([], [{"id": 1}], 7)
        check("no post when nothing is below floor", posted == [], str(posted))
        m.to_slack([{"id": 1, "name": "X", "realised": 5.0, "floor": 20.0,
                     "margin_type": "range", "gross_day": 100.0,
                     "shortfall_day": 15.0, "is_smart_floor": False}], [], 7)
        check("posts when something is below floor", len(posted) == 1, str(posted))
        check("the headline carries the dollar figure",
              "$15.00" in posted[0], posted[0] if posted else "")
    finally:
        slack.send_blocks = orig


# ---------------------------------------------------------------------------
# no-win watch
# ---------------------------------------------------------------------------

def test_nowin_assess() -> None:
    print("\nno-win watch: a single win is the whole signal")
    names = {1: "Dormant", 2: "Woke Up", 3: "Never Seen"}
    series = {
        1: [{"day": "2026-08-25", "requests": 1e6, "wins": 0, "imps": 0, "spend": 0.0},
            {"day": "2026-08-26", "requests": 1e6, "wins": 0, "imps": 0, "spend": 0.0}],
        2: [{"day": "2026-08-25", "requests": 1e6, "wins": 0, "imps": 0, "spend": 0.0},
            {"day": "2026-08-26", "requests": 1e6, "wins": 40, "imps": 38, "spend": 2.5}],
        3: [],
    }
    got = {a["id"]: a for a in w.assess(series, names)}
    check("a dormant endpoint does not read as woken", got[1]["woke"] is False)
    check("one winning day wakes the endpoint", got[2]["woke"] is True)
    check("the winning day is named, not just counted",
          got[2]["winning_days"] == ["2026-08-26"], str(got[2]["winning_days"]))
    check("requests are summed across days", got[1]["requests"] == 2e6)
    # An endpoint absent from every day is NOT the same as one with zero
    # wins: the platform drops all-zero rows, so absence may mean already off.
    check("an endpoint with no rows is distinguishable by days_present",
          got[3]["days_present"] == 0 and got[3]["woke"] is False)
    check("a woken endpoint sorts to the top",
          w.assess(series, names)[0]["id"] == 2)


def test_nowin_slack_silence() -> None:
    print("\nno-win watch: silence is the steady state")
    posted = []
    import core.slack as slack
    orig = slack.send_blocks
    slack.send_blocks = lambda blocks, text="": posted.append(text)
    try:
        quiet = [{"id": 1, "name": "Dormant", "wins": 0, "spend": 0.0,
                  "winning_days": [], "woke": False, "days_present": 14}]
        w.to_slack([], quiet, 14)
        check("no post while everything is still at zero", posted == [], str(posted))

        woke = [{"id": 2, "name": "Woke Up", "wins": 40, "spend": 2.5,
                 "winning_days": ["2026-08-26"], "woke": True, "days_present": 14}]
        w.to_slack(woke, quiet + woke, 14)
        check("posts the moment one wins", len(posted) == 1, str(posted))
    finally:
        slack.send_blocks = orig


def test_nowin_pull_filters() -> None:
    print("\nno-win watch: the pull is per-day and id-filtered")
    calls = []
    served = {
        "2026-08-25": [
            {"date": "2026-08-25", "demand_source": "Illumin - RON #1553",
             "requests_sum": "100", "wins_sum": "0", "imps_sum": "0",
             "dsp_price_sum": "0"},
            # An unwatched partner on the same day must not be collected.
            {"date": "2026-08-25", "demand_source": "Someone Else #4242",
             "requests_sum": "999", "wins_sum": "50", "imps_sum": "50",
             "dsp_price_sum": "9.9"},
            # An off-window row: single-day requests have been seen to carry
            # neighbouring days, so every row's date is checked.
            {"date": "2026-08-24", "demand_source": "Illumin - RON #1553",
             "requests_sum": "8888", "wins_sum": "77", "imps_sum": "70",
             "dsp_price_sum": "7.7"},
        ],
    }

    def fake_report(df, dt, attributes=None, metrics=None, **kw):
        calls.append((df, dt))
        return served.get(df, []), {}

    real = w.tbx.report
    w.tbx.report = fake_report
    try:
        series, missing = w.pull_per_day(date(2026, 8, 25), 2, {1553})
    finally:
        w.tbx.report = real

    check("one request per day, never a multi-day window",
          calls == [("2026-08-25", "2026-08-25"), ("2026-08-26", "2026-08-26")],
          str(calls))
    check("only the watched id is collected", set(series) == {1553}, str(set(series)))
    check("the off-window row is discarded",
          len(series[1553]) == 1, str(series[1553]))
    check("and its wins are NOT counted — that would fake a wake-up",
          series[1553][0]["wins"] == 0, str(series[1553][0]))
    check("a day with no watched endpoint is recorded as missing",
          missing == ["2026-08-26"], str(missing))


def test_side_option() -> None:
    """--side picks the entity, and demand is the writable one."""
    print("\nsupply vs demand side")
    args = m.build_parser().parse_args([])
    check("supply is the default side", args.side == "supply", args.side)
    check("demand is selectable",
          m.build_parser().parse_args(["--side", "demand"]).side == "demand")

    # The pull must key rows off the grain it was asked for. Reading
    # "supply_source" out of a demand row would silently yield no ids and a
    # clean-looking empty report.
    served = {"2026-08-25": [
        {"date": "2026-08-25", "demand_source": "Some DSP #501",
         "imps_sum": "100", "dsp_price_sum": "100", "ssp_price_sum": "80"},
    ]}
    real = t_trim.tbx.report
    t_trim.tbx.report = lambda df, dt, **kw: (served.get(df, []), {})
    try:
        rows, days = m.pull_take_rates(_date(2026, 8, 25), 1, "demand_source")
    finally:
        t_trim.tbx.report = real
    check("a demand row is keyed off demand_source", rows and rows[0]["id"] == 501,
          str(rows))
    check("and its take rate is (dsp - ssp) / dsp",
          rows and abs(rows[0]["take_rate"] - 20.0) < 1e-9, str(rows))


def test_parsers() -> None:
    print("\nargument surfaces")
    import re
    from pathlib import Path
    for mod, label in ((m, "margin sentry"), (w, "no-win watch")):
        args = mod.build_parser().parse_args([])
        src_text = Path(mod.__file__).read_text()
        used = set(re.findall(r"\bargs\.([a-z_]+)", src_text))
        missing = sorted(u for u in used if not hasattr(args, u))
        check(f"{label}: every args.X is defined by the parser",
              not missing, f"missing: {missing}")
        check(f"{label}: --slack defaults off", args.slack is False)
    check("margin sentry does not alert on above-band by default",
          m.build_parser().parse_args([]).include_above is False)
    check("the built-in watch list is the nine Illumin endpoints",
          len(w.DEFAULT_WATCH) == 9, str(len(w.DEFAULT_WATCH)))


def main() -> int:
    print("=" * 70)
    print("TBX sentries — offline checks")
    print("=" * 70)
    test_floor_and_ceiling()
    test_below_band()
    test_above_band_is_not_a_shortfall()
    test_noise_floors()
    test_margin_slack_silence()
    test_nowin_assess()
    test_nowin_slack_silence()
    test_nowin_pull_filters()
    test_side_option()
    test_parsers()
    print("\n" + "=" * 70)
    print(f"{PASS} passed, {FAIL} failed")
    return 1 if FAIL else 0


if __name__ == "__main__":
    sys.exit(main())
