#!/usr/bin/env python3
"""
Offline checks for scripts/tbx_trim.py.

The report's whole value is its verdicts, and a verdict that is wrong in a
plausible-looking way is worse than no report: it sends someone to cut a
source that was earning, or tells them a margin is fine when it is not. So
the bucket rules and the band comparison are pinned here, with no credentials
and no platform call.
"""

from __future__ import annotations

import sys
from datetime import datetime, timezone

sys.path.insert(0, __file__.rsplit("/tests/", 1)[0])

from scripts import tbx_trim as t        # noqa: E402

PASS = FAIL = 0


def check(label: str, ok: bool, detail: str = "") -> None:
    global PASS, FAIL
    if ok:
        PASS += 1
        print(f"  ✓ {label}")
    else:
        FAIL += 1
        print(f"  ✗ {label}" + (f"  — {detail}" if detail else ""))


class Args:
    """The subset of the parsed namespace the assessors read."""
    min_requests_day = 10_000
    min_revenue_day = 1.0
    hungry_multiple = 5.0
    timeout_pct = 20.0


def supply_row(name, requests, imps, dsp, ssp, fill=0.0):
    return {"supply_source": name, "ssp_requests_sum": str(requests),
            "imps_sum": str(imps), "dsp_price_sum": str(dsp),
            "ssp_price_sum": str(ssp), "supply_fill_rate": str(fill)}


def demand_row(name, requests, wins, imps, dsp, timeout=0.0):
    return {"demand_source": name, "requests_sum": str(requests),
            "responses_sum": str(requests), "wins_sum": str(wins),
            "imps_sum": str(imps), "dsp_price_sum": str(dsp),
            "timeout_rate": str(timeout)}


def test_split_name_id() -> None:
    print("\nthe #NNNN name convention")
    check("trailing id is split off",
          t.split_name_id("Advetisi - Zmaticoo #264") == ("Advetisi - Zmaticoo", 264))
    check("no suffix yields no id",
          t.split_name_id("Plain Name") == ("Plain Name", None))
    check("a '#' inside the name is not an id",
          t.split_name_id("Chan #1 Media") == ("Chan #1 Media", None))
    check("only the trailing group counts",
          t.split_name_id("Chan #1 Media #77") == ("Chan #1 Media", 77))
    check("case is preserved — this name is displayed, not joined on",
          t.split_name_id("Illumin - Video #65")[0] == "Illumin - Video")
    check("empty input is survivable", t.split_name_id("") == ("", None))


def test_per_dollar() -> None:
    print("\nrequests per revenue dollar")
    check("ordinary case", t.per_dollar(1000, 10) == 100.0)
    check("no revenue is None, NOT zero", t.per_dollar(1000, 0) is None)
    check("negative revenue is also None", t.per_dollar(1000, -5) is None)
    # The reason it must not be 0.0: a zero would sort as the *most*
    # efficient source in an ascending ranking, which is backwards.
    values = [t.per_dollar(1000, 10), t.per_dollar(1000, 0)]
    check("a no-revenue source cannot masquerade as efficient",
          values[1] is None and values[0] == 100.0)


def test_settled_day() -> None:
    print("\nUS/Eastern settlement")
    # 02:00 UTC on the 28th is still the 27th in New York, so the last
    # settled day is the 26th, not the 27th.
    early = datetime(2026, 8, 28, 2, 0, tzinfo=timezone.utc)
    check("before the ET close, yesterday is not yet settled",
          t.latest_settled(early).isoformat() == "2026-08-26",
          t.latest_settled(early).isoformat())
    late = datetime(2026, 8, 28, 12, 0, tzinfo=timezone.utc)
    check("after the close, yesterday is settled",
          t.latest_settled(late).isoformat() == "2026-08-27",
          t.latest_settled(late).isoformat())


def test_supply_buckets() -> None:
    print("\nsupply buckets")
    days = 7
    rows = [
        # requests but never an impression -> DEAD
        supply_row("Dead Weight #1", 7_000_000, 0, 0, 0),
        # earns pennies for real volume -> NEAR-DEAD
        supply_row("Pennies #2", 3_500_000, 700, 3.5, 3.0),
        # payout exceeds gross -> LOSS
        supply_row("Underwater #3", 700_000, 70_000, 700, 900),
        # healthy, sets the median
        supply_row("Healthy #4", 700_000, 70_000, 7_000, 5_000),
        supply_row("Healthy Two #5", 1_400_000, 140_000, 14_000, 10_000),
        # earns, but needs a huge number of requests per dollar -> HUNGRY
        supply_row("Thirsty #6", 7_000_000, 7_000, 700, 500),
        # below the size floor -> no opinion, even though it earns nothing
        supply_row("Tiny #7", 700, 0, 0, 0),
    ]
    assessed, book_median = t.assess_supply(rows, days, Args())
    got = {a["name"]: a["bucket"] for a in assessed}

    check("zero impressions reads as DEAD", got["Dead Weight"] == "DEAD", str(got))
    check("sub-dollar revenue reads as NEAR-DEAD", got["Pennies"] == "NEAR-DEAD", str(got))
    check("payout above gross reads as LOSS", got["Underwater"] == "LOSS", str(got))
    check("a healthy source is not flagged", got["Healthy"] is None, str(got))
    check("an inefficient earner reads as HUNGRY", got["Thirsty"] == "HUNGRY", str(got))
    check("a source under the size floor gets no verdict",
          got["Tiny"] is None, str(got))

    # DEAD outranks HUNGRY: both fit Dead Weight, and DEAD is the stronger
    # statement to put in front of an operator.
    check("the strongest applicable bucket wins",
          got["Dead Weight"] == "DEAD")

    check("the median is computed over earning sources only",
          book_median is not None and book_median > 0, str(book_median))
    # Healthy: 700k/7000 = 100. Healthy Two: 1.4M/14000 = 100.
    # Underwater: 700k/700 = 1000. Thirsty: 7M/700 = 10000. Pennies: 1M.
    # Median of [100, 100, 1000, 10000, 1_000_000] is 1000.
    check("median matches the hand calculation", book_median == 1000.0,
          str(book_median))

    per_day = {a["name"]: a["gross_day"] for a in assessed}
    check("revenue is reported per day, not per window",
          abs(per_day["Healthy"] - 1000.0) < 1e-6, str(per_day["Healthy"]))


def test_demand_buckets() -> None:
    print("\ndemand buckets")
    rows = [
        demand_row("Never Wins #10", 7_000_000, 0, 0, 0),
        demand_row("Slow Poke #11", 7_000_000, 70_000, 70_000, 7_000, timeout=35.0),
        demand_row("Good Buyer #12", 7_000_000, 70_000, 70_000, 7_000, timeout=2.0),
        demand_row("Too Small #13", 700, 0, 0, 0),
    ]
    assessed = t.assess_demand(rows, 7, Args())
    got = {a["name"]: a["bucket"] for a in assessed}
    check("zero wins reads as NO-WIN", got["Never Wins"] == "NO-WIN", str(got))
    check("a heavy timeout rate is flagged", got["Slow Poke"] == "TIMEOUT", str(got))
    check("a good buyer is left alone", got["Good Buyer"] is None, str(got))
    check("below the size floor gets no verdict", got["Too Small"] is None, str(got))
    # NO-WIN must beat TIMEOUT for a source that is both, same reasoning as
    # DEAD beating HUNGRY.
    both = t.assess_demand([demand_row("Both #14", 7_000_000, 0, 0, 0, timeout=90.0)],
                           7, Args())
    check("no wins outranks a timeout verdict", both[0]["bucket"] == "NO-WIN")


def test_band_verdict() -> None:
    print("\nrealised take rate against the configured band")
    rng = {"margin_type": "range", "margin_min": 5, "margin_max": 30}
    check("inside a range is in band", "in band" in t.band_verdict(18.0, rng))
    check("under a range is flagged", t.band_verdict(3.0, rng).startswith("BELOW BAND"))
    check("over a range is flagged", t.band_verdict(44.0, rng).startswith("above band"))
    check("exactly on the lower edge is in band", "in band" in t.band_verdict(5.0, rng))

    adaptive = {"margin_type": "adaptive", "margin_min": 5, "margin_max": 95}
    check("adaptive uses the same comparison",
          "in band" in t.band_verdict(50.0, adaptive))

    # The trap: a `fixed` source returns margin_max 0. Treating that as an
    # upper bound would put every fixed source permanently above band.
    fixed = {"margin_type": "fixed", "margin_min": 2, "margin_max": 0}
    check("a fixed source realising its number is on target",
          t.band_verdict(2.0, fixed).startswith("on target"), t.band_verdict(2.0, fixed))
    check("margin_max 0 on a fixed source is not read as a ceiling",
          not t.band_verdict(2.0, fixed).startswith("above"),
          t.band_verdict(2.0, fixed))
    check("a fixed source missing its number is flagged",
          t.band_verdict(9.0, fixed).startswith("OFF TARGET"))
    check("a point of slack is allowed on fixed",
          t.band_verdict(2.6, fixed).startswith("on target"))

    check("no revenue yields no verdict rather than a false one",
          t.band_verdict(None, rng) == "no revenue")
    check("an unknown margin_type is reported, not guessed",
          "unknown margin_type" in t.band_verdict(10.0, {"margin_type": "weird"}))


def test_parser() -> None:
    print("\nargument surface")
    args = t.build_parser().parse_args([])
    for field in ("days", "min_requests_day", "min_revenue_day",
                  "hungry_multiple", "timeout_pct", "config_top", "no_config"):
        check(f"--{field.replace('_', '-')} has a default",
              getattr(args, field, None) is not None)

    # The failure this guards against actually happened (#132): a call site
    # read args.pinned while the add_argument defining it lived on another
    # branch, so the script crashed only when that path ran. Scrape every
    # args.X out of the module and assert the parser defines it.
    import re
    from pathlib import Path
    src = Path(t.__file__).read_text()
    used = set(re.findall(r"\bargs\.([a-z_]+)", src))
    missing = sorted(u for u in used if not hasattr(args, u))
    check("every args.X the module reads is defined by the parser",
          not missing, f"missing: {missing}")


def main() -> int:
    print("=" * 70)
    print("tbx_trim — offline checks")
    print("=" * 70)
    test_split_name_id()
    test_per_dollar()
    test_settled_day()
    test_supply_buckets()
    test_demand_buckets()
    test_band_verdict()
    test_parser()
    print("\n" + "=" * 70)
    print(f"{PASS} passed, {FAIL} failed")
    return 1 if FAIL else 0


if __name__ == "__main__":
    sys.exit(main())
