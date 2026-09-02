#!/usr/bin/env python3
"""Offline checks for tbx_net_margin — the arithmetic and the rails.

No network. The fee identity is the whole point of the script, so it is
asserted directly rather than inferred from a rendered table.
"""

from __future__ import annotations

import io
import os
import sys
from contextlib import redirect_stdout
from datetime import date, datetime, timedelta, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from scripts import tbx_net_margin as nm     # noqa: E402

PASS = FAIL = 0


def check(label: str, ok: bool, detail: str = "") -> None:
    global PASS, FAIL
    if ok:
        PASS += 1
        print(f"  ✓ {label}")
    else:
        FAIL += 1
        print(f"  ✗ {label}   {detail}")


def args_for(**over):
    args = nm.build_parser().parse_args([])
    for k, v in over.items():
        setattr(args, k, v)
    return args


def src(name, sid, gross, payout, imps=1000.0):
    return {f"{name}#{sid}": {"name": name, "id": sid, "gross": gross,
                              "payout": payout, "imps": imps}}


# ---------------------------------------------------------------- arithmetic

def test_gross_fee_comes_straight_off_the_take_rate() -> None:
    print("\na gross-based fee subtracts percentage points from the take rate")
    for take in (30.0, 20.0, 12.0, 8.0, 5.3):
        got = nm.net_rate(take, 8.0, "gross")
        check(f"take {take}% -> net {take - 8.0:.1f}%",
              abs(got - (take - 8.0)) < 1e-9, str(got))
    check("a zero take rate nets the fee as a loss",
          abs(nm.net_rate(0.0, 8.0, "gross") + 8.0) < 1e-9)
    check("no gross means no opinion", nm.net_rate(None, 8.0, "gross") is None)


def test_retention_is_a_fraction_of_the_spread_not_the_gross() -> None:
    """The counterintuitive half: a flat fee does not hurt everyone equally."""
    print("\nthe fee eats a share of the SPREAD, and thin spreads lose most")
    check("30% take keeps 73% of its margin",
          abs(nm.retention(30.0, 8.0, "gross") - 73.3333) < 0.01,
          str(nm.retention(30.0, 8.0, "gross")))
    check("12% take keeps only 33%",
          abs(nm.retention(12.0, 8.0, "gross") - 33.3333) < 0.01,
          str(nm.retention(12.0, 8.0, "gross")))
    check("10% take keeps 20%",
          abs(nm.retention(10.0, 8.0, "gross") - 20.0) < 1e-9,
          str(nm.retention(10.0, 8.0, "gross")))
    check("8% take keeps nothing",
          abs(nm.retention(8.0, 8.0, "gross")) < 1e-9)
    check("below the fee, retention goes negative",
          nm.retention(5.0, 8.0, "gross") < 0)


def test_margin_base_behaves_completely_differently() -> None:
    print("\na margin-based fee is flat, and cannot push anything underwater")
    check("20% take -> 18.4% net", abs(nm.net_rate(20.0, 8.0, "margin") - 18.4) < 1e-9)
    check("5% take -> 4.6% net", abs(nm.net_rate(5.0, 8.0, "margin") - 4.6) < 1e-9)
    check("retention is flat at 92% regardless of take",
          nm.retention(30.0, 8.0, "margin") == nm.retention(5.0, 8.0, "margin") == 92.0)
    check("a thin but positive source stays positive",
          nm.net_rate(1.0, 8.0, "margin") > 0)


def test_fee_dollars_follow_the_base() -> None:
    print("\nthe fee base changes the dollar amount, not just the rate")
    check("gross base bills on gross",
          abs(nm.fee_on(1000.0, 200.0, 8.0, "gross") - 80.0) < 1e-9)
    check("margin base bills on the spread",
          abs(nm.fee_on(1000.0, 200.0, 8.0, "margin") - 16.0) < 1e-9)


def test_take_rate() -> None:
    print("\ntake rate")
    check("200 of 1000", abs(nm.take_of(1000.0, 800.0) - 20.0) < 1e-9)
    check("no gross is None, not zero", nm.take_of(0.0, 0.0) is None)
    check("payout above gross is negative", nm.take_of(100.0, 130.0) < 0)


# --------------------------------------------------------------------- rails

def test_underwater_sources_are_the_exit_code() -> None:
    print("\na source below the fee is reported and sets the exit code")
    sources = {}
    sources.update(src("Healthy", 1, 1000.0, 700.0))     # 30% take -> 22% net
    sources.update(src("Thin", 2, 1000.0, 947.0))        # 5.3% take -> -2.7%
    rows = nm.assess(sources, 1, args_for())
    by = {r["name"]: r for r in rows}
    check("healthy nets 22%", abs(by["Healthy"]["net_pct"] - 22.0) < 1e-9,
          str(by["Healthy"]["net_pct"]))
    check("thin nets -2.7%", abs(by["Thin"]["net_pct"] + 2.7) < 1e-9,
          str(by["Thin"]["net_pct"]))
    check("worst first", rows[0]["name"] == "Thin", str([r["name"] for r in rows]))

    with redirect_stdout(io.StringIO()):
        under = nm.render_sources(rows, args_for(), 1, True)
    check("only the thin one is flagged underwater",
          [r["name"] for r in under] == ["Thin"], str(under))


def test_min_gross_keeps_rounding_noise_out() -> None:
    print("\na source with almost no gross does not get a verdict")
    sources = src("Dust", 9, 0.40, 0.10)
    check("dropped below --min-gross",
          nm.assess(sources, 1, args_for(min_gross=1.0)) == [])
    check("kept when the floor is lowered",
          len(nm.assess(sources, 1, args_for(min_gross=0.1))) == 1)


def test_per_day_totals_and_partial_labelling() -> None:
    print("\neach day is labelled settled or partial")
    sources = {}
    sources.update(src("A", 1, 600.0, 480.0))
    sources.update(src("B", 2, 400.0, 320.0))
    line = nm.day_line("2026-09-01", sources, args_for(), settled=False)
    check("gross sums", line["gross"] == 1000.0, str(line["gross"]))
    check("take is 20%", abs(line["take_pct"] - 20.0) < 1e-9)
    check("fee is 8% of gross", abs(line["fee"] - 80.0) < 1e-9)
    check("net is margin minus fee", abs(line["net"] - 120.0) < 1e-9)
    check("net% is 12", abs(line["net_pct"] - 12.0) < 1e-9)
    check("carries the partial flag", line["settled"] is False)

    with redirect_stdout(io.StringIO()) as buf:
        nm.render_days([line], args_for())
    check("PARTIAL is printed", "PARTIAL" in buf.getvalue())


def test_aggregate_sums_across_days() -> None:
    print("\nthe window aggregate adds days together")
    d1 = src("A", 1, 100.0, 80.0)
    d2 = src("A", 1, 300.0, 240.0)
    agg = nm.aggregate([d1, d2])
    check("gross added", agg["A#1"]["gross"] == 400.0, str(agg))
    rows = nm.assess(agg, 2, args_for())
    check("per-day gross divides by the day count",
          rows[0]["gross_day"] == 200.0, str(rows[0]["gross_day"]))


def test_a_partial_day_does_not_drive_the_aggregate() -> None:
    """The rail that keeps a half-counted day from inventing a problem."""
    print("\nwhen a settled day exists, partial days are excluded from it")
    settled = date(2026, 8, 31)
    calls = []

    def fake_pull(day):
        calls.append(day)
        # The settled day is healthy; the partial day looks terrible because
        # it is only a third counted.
        if day == "2026-08-31":
            return src("A", 1, 900.0, 630.0)          # 30% take
        return src("A", 1, 30.0, 29.0)                # 3.3% take, partial

    real_pull, real_conf = nm.pull_day, nm.tbx.configured
    real_settled = nm.trim.latest_settled
    nm.pull_day = fake_pull
    nm.tbx.configured = lambda: True
    nm.trim.latest_settled = lambda now: settled
    try:
        with redirect_stdout(io.StringIO()) as buf:
            rc = nm.main(["--date", "2026-08-31", "--date", "2026-09-01"])
    finally:
        nm.pull_day, nm.tbx.configured = real_pull, real_conf
        nm.trim.latest_settled = real_settled

    out = buf.getvalue()
    check("both days were pulled", calls == ["2026-08-31", "2026-09-01"], str(calls))
    check("the unsettled day is named as not settled",
          "NOT settled" in out, out[:200])
    check("both days appear in the per-day table",
          "2026-08-31" in out and "2026-09-01" in out)
    check("the aggregate used 1 settled day, not 2",
          "across 1 settled day(s)" in out,
          [l for l in out.splitlines() if "across" in l])
    check("so the healthy 30% take is what is assessed",
          "  30.0%" in out or " 30.0%" in out,
          [l for l in out.splitlines() if "A #1" in l])
    check("nothing underwater, exit 0", rc == 0, str(rc))


def test_all_partial_says_so_loudly() -> None:
    print("\nwith no settled day in range, the report says the numbers will move")
    real_pull, real_conf = nm.pull_day, nm.tbx.configured
    real_settled = nm.trim.latest_settled
    nm.pull_day = lambda day: src("A", 1, 100.0, 80.0)
    nm.tbx.configured = lambda: True
    nm.trim.latest_settled = lambda now: date(2026, 8, 31)
    try:
        with redirect_stdout(io.StringIO()) as buf:
            nm.main(["--date", "2026-09-01", "--date", "2026-09-02"])
    finally:
        nm.pull_day, nm.tbx.configured = real_pull, real_conf
        nm.trim.latest_settled = real_settled
    out = buf.getvalue()
    check("warns that no settled day is present", "No settled day" in out)
    check("the adjustment section is marked directional",
          "directional only" in out)
    check("the source table says PARTIAL", "PARTIAL — not final" in out)


def test_a_failed_day_is_not_a_day_of_zero_revenue() -> None:
    print("\na day whose query failed is dropped, not counted as $0")
    real_pull, real_conf = nm.pull_day, nm.tbx.configured
    real_settled = nm.trim.latest_settled
    nm.pull_day = lambda day: None if day == "2026-08-30" else src("A", 1, 100.0, 70.0)
    nm.tbx.configured = lambda: True
    nm.trim.latest_settled = lambda now: date(2026, 8, 31)
    try:
        with redirect_stdout(io.StringIO()) as buf:
            rc = nm.main(["--date", "2026-08-30", "--date", "2026-08-31"])
    finally:
        nm.pull_day, nm.tbx.configured = real_pull, real_conf
        nm.trim.latest_settled = real_settled
    out = buf.getvalue()
    check("the failed day is absent from the table",
          "2026-08-30" not in out.split("per day")[1], out[:400])
    check("the aggregate divides by 1, not 2",
          "across 1 settled day(s)" in out)
    check("and the take rate is not halved",
          "  30.0%" in out, [l for l in out.splitlines() if "A #1" in l])
    check("exit 0", rc == 0, str(rc))


def test_no_measurable_day_refuses() -> None:
    print("\nnothing measurable refuses to report")
    real_pull, real_conf = nm.pull_day, nm.tbx.configured
    nm.pull_day = lambda day: None
    nm.tbx.configured = lambda: True
    try:
        with redirect_stdout(io.StringIO()):
            rc = nm.main(["--date", "2026-08-31"])
    finally:
        nm.pull_day, nm.tbx.configured = real_pull, real_conf
    check("exits 2", rc == 2, str(rc))


def test_no_credentials_refuses_before_anything_else() -> None:
    print("\nno credentials, no run")
    real_conf = nm.tbx.configured
    nm.tbx.configured = lambda: False
    try:
        with redirect_stdout(io.StringIO()):
            rc = nm.main([])
    finally:
        nm.tbx.configured = real_conf
    check("exits 2", rc == 2, str(rc))


def test_adjustment_target_needs_target_plus_fee() -> None:
    print("\nthe required take rate is the target plus the fee")
    rows = nm.assess(src("Thin", 2, 1000.0, 880.0), 1,          # 12% take
                     args_for())
    with redirect_stdout(io.StringIO()) as buf:
        nm.render_adjustment(rows, args_for(target_net=10.0, fee_pct=8.0), True)
    out = buf.getvalue()
    check("names an 18.0% required take rate", "18.0%" in out, out[:300])
    check("sizes the gap at 6 points", " 6.0 " in out,
          [l for l in out.splitlines() if "Thin" in l])
    check("and $60/day on $1,000 gross", "60.00" in out,
          [l for l in out.splitlines() if "Thin" in l])


def test_parser_surface() -> None:
    """Every args.X the module reads must exist on the namespace."""
    print("\nargument surface")
    import re
    source = open(nm.__file__).read()
    used = set(re.findall(r"args\.([a-z_]+)", source))
    ns = vars(nm.build_parser().parse_args([]))
    missing = sorted(used - set(ns))
    check("every args.X is defined by the parser", not missing, str(missing))
    check("the fee defaults to 8%", ns["fee_pct"] == 8.0)
    check("charged on gross by default", ns["fee_base"] == "gross")


def main() -> int:
    print("=" * 70)
    print("tbx_net_margin — offline checks")
    print("=" * 70)
    test_gross_fee_comes_straight_off_the_take_rate()
    test_retention_is_a_fraction_of_the_spread_not_the_gross()
    test_margin_base_behaves_completely_differently()
    test_fee_dollars_follow_the_base()
    test_take_rate()
    test_underwater_sources_are_the_exit_code()
    test_min_gross_keeps_rounding_noise_out()
    test_per_day_totals_and_partial_labelling()
    test_aggregate_sums_across_days()
    test_a_partial_day_does_not_drive_the_aggregate()
    test_all_partial_says_so_loudly()
    test_a_failed_day_is_not_a_day_of_zero_revenue()
    test_no_measurable_day_refuses()
    test_no_credentials_refuses_before_anything_else()
    test_adjustment_target_needs_target_plus_fee()
    test_parser_surface()
    print("\n" + "=" * 70)
    print(f"{PASS} passed, {FAIL} failed")
    return 1 if FAIL else 0


if __name__ == "__main__":
    sys.exit(main())
