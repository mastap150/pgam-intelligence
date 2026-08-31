#!/usr/bin/env python3
"""
Offline checks for scripts/tbx_geo_waste.py.

The report's output is a list of `set_demand_geo_blacklist(id, [countries])`
calls someone will paste and run, so the tests are weighted at the two ways
that goes wrong: naming a buyer that does trade in that country, and losing
the id so the call cannot be written at all.

No credentials, no platform call.
"""

from __future__ import annotations

import sys
from datetime import date

sys.path.insert(0, __file__.rsplit("/tests/", 1)[0])

from scripts import tbx_geo_waste as g    # noqa: E402

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
    min_requests_day = 1_000_000
    max_spend_day = 1.0
    top = 30


def test_country_summary() -> None:
    print("\ncountry grain")
    rows = [
        {"country": "US", "requests_sum": "7000000", "imps_sum": "70000",
         "dsp_price_sum": "7000"},
        {"country": "BR", "requests_sum": "14000000", "imps_sum": "0",
         "dsp_price_sum": "0"},
    ]
    got = {r["name"]: r for r in g.summarise(rows, "country", 7)}
    check("per-day division uses the days that answered",
          got["US"]["requests_day"] == 1_000_000.0, str(got["US"]["requests_day"]))
    check("a country with no spend has no requests-per-dollar",
          got["BR"]["per_dollar"] is None)
    check("country is carried as its own field for grouping",
          got["BR"]["country"] == "BR", str(got["BR"]["country"]))
    check("rows sort by request volume, biggest first",
          g.summarise(rows, "country", 7)[0]["name"] == "BR")


def test_pair_id_survives() -> None:
    """The id must survive into the output or the blacklist call is unusable.

    split_name_id only matches a TRAILING #NNNN, and a pair label puts the id
    mid-string ("Some DSP #501 — BR"). pull_pairs therefore splits first and
    hands summarise the parts; this pins that contract.
    """
    print("\npair grain keeps the demand id")
    rows = [{"demand_id": 501, "country": "BR", "label": "Some DSP #501 — BR",
             "requests_sum": "7000000", "imps_sum": "0", "dsp_price_sum": "0"}]
    got = g.summarise(rows, "demand_source", 7)[0]
    check("the demand id is recovered", got["id"] == 501, str(got["id"]))
    check("the country is recovered", got["country"] == "BR", str(got["country"]))
    check("the label stays human-readable",
          got["name"] == "Some DSP #501 — BR", got["name"])

    # A composite label run through the naive path loses the id entirely,
    # which is the bug this design avoids.
    naive = g.summarise([{"demand_source": "Some DSP #501 — BR",
                          "requests_sum": "1", "imps_sum": "0",
                          "dsp_price_sum": "0"}], "demand_source", 1)[0]
    check("the naive path would have lost it — hence the pre-split",
          naive["id"] is None)


def test_waste_thresholds() -> None:
    print("\nwhat counts as waste")
    rows = g.summarise([
        # real volume, no spend -> waste
        {"demand_id": 1, "country": "BR", "label": "A #1 — BR",
         "requests_sum": "14000000", "imps_sum": "0", "dsp_price_sum": "0"},
        # real volume, real spend -> NOT waste, must never be blacklisted
        {"demand_id": 2, "country": "US", "label": "B #2 — US",
         "requests_sum": "14000000", "imps_sum": "70000", "dsp_price_sum": "7000"},
        # no spend but trivial volume -> below the bar, no opinion
        {"demand_id": 3, "country": "FJ", "label": "C #3 — FJ",
         "requests_sum": "700", "imps_sum": "0", "dsp_price_sum": "0"},
        # spends a little but under the threshold -> waste
        {"demand_id": 1, "country": "IN", "label": "A #1 — IN",
         "requests_sum": "21000000", "imps_sum": "70", "dsp_price_sum": "3.5"},
    ], "demand_source", 7)

    waste = g.render_pairs(rows, Args())
    ids = {(w["id"], w["country"]) for w in waste}
    check("a high-volume zero-spend pair is flagged", (1, "BR") in ids, str(ids))
    check("a pair that actually trades is NEVER flagged", (2, "US") not in ids, str(ids))
    check("a trivial-volume pair is left alone", (3, "FJ") not in ids, str(ids))
    check("sub-threshold spend still counts as waste", (1, "IN") in ids, str(ids))


def test_country_view_waste() -> None:
    print("\ncountry view sizes the prize")
    rows = g.summarise([
        {"country": "US", "requests_sum": "7000000", "imps_sum": "70000",
         "dsp_price_sum": "7000"},
        {"country": "BR", "requests_sum": "21000000", "imps_sum": "0",
         "dsp_price_sum": "0"},
    ], "country", 7)
    waste = g.render_country(rows, Args())
    check("the zero-revenue country is flagged",
          [w["name"] for w in waste] == ["BR"], str([w["name"] for w in waste]))
    check("the earning country is not",
          "US" not in [w["name"] for w in waste])


def test_pull_pairs_filters() -> None:
    print("\nthe pair pull is per-day and date-checked")
    calls = []
    served = {
        "2026-08-25": [
            {"date": "2026-08-25", "demand_source": "A #1", "country": "BR",
             "requests_sum": "100", "imps_sum": "0", "dsp_price_sum": "0"},
            # off-window row — must not be added, same rule as every other
            # TBX reader in this repo (§5.10)
            {"date": "2026-08-24", "demand_source": "A #1", "country": "BR",
             "requests_sum": "99999", "imps_sum": "0", "dsp_price_sum": "0"},
        ],
        "2026-08-26": [
            {"date": "2026-08-26", "demand_source": "A #1", "country": "BR",
             "requests_sum": "300", "imps_sum": "0", "dsp_price_sum": "0"},
        ],
    }

    def fake_report(df, dt, attributes=None, metrics=None, **kw):
        calls.append((df, dt))
        return served.get(df, []), {}

    real = g.tbx.report
    g.tbx.report = fake_report
    try:
        rows, days = g.pull_pairs(date(2026, 8, 25), 2)
    finally:
        g.tbx.report = real

    check("one request per day, never a multi-day window",
          calls == [("2026-08-25", "2026-08-25"), ("2026-08-26", "2026-08-26")],
          str(calls))
    check("both days counted", days == 2, str(days))
    check("one pair key across two days", len(rows) == 1, str(rows))
    check("counts summed, off-window row discarded",
          rows[0]["requests_sum"] == 400.0, str(rows[0]["requests_sum"]))
    check("the key is (demand, country), not demand alone",
          rows[0]["demand_id"] == 1 and rows[0]["country"] == "BR", str(rows[0]))

    # Two countries on one DSP must stay separate rows, or the whole report
    # collapses and the blacklist would name a country that does trade.
    g.tbx.report = lambda df, dt, **kw: ([
        {"date": df, "demand_source": "A #1", "country": "BR",
         "requests_sum": "100", "imps_sum": "0", "dsp_price_sum": "0"},
        {"date": df, "demand_source": "A #1", "country": "US",
         "requests_sum": "200", "imps_sum": "50", "dsp_price_sum": "9"},
    ], {})
    try:
        rows2, _ = g.pull_pairs(date(2026, 8, 25), 1)
    finally:
        g.tbx.report = real
    check("countries do not collapse into one row per DSP",
          len(rows2) == 2, str(rows2))


def test_a_failed_day_does_not_lose_the_good_ones() -> None:
    """The 2026-08-31 failure: day 4 timed out, days 1-3 were discarded.

    Three good days of data went in the bin and the run reported success. The
    pull must keep what answered and say what did not.
    """
    print("\na day that times out is skipped, not fatal")
    served = {
        "2026-08-25": [{"date": "2026-08-25", "demand_source": "A #1",
                        "country": "BR", "requests_sum": "100",
                        "imps_sum": "0", "dsp_price_sum": "0"}],
        "2026-08-27": [{"date": "2026-08-27", "demand_source": "A #1",
                        "country": "BR", "requests_sum": "300",
                        "imps_sum": "0", "dsp_price_sum": "0"}],
    }

    def flaky_report(df, dt, **kw):
        if df not in served:
            raise g.tbx.TbxError(f"POST /report/ unreachable: read timed out")
        return served[df], {}

    real = g.tbx.report
    g.tbx.report = flaky_report
    try:
        rows, days = g.pull_pairs(date(2026, 8, 25), 3)
    finally:
        g.tbx.report = real

    check("the run survives the failed day", days == 2, str(days))
    check("the good days are kept, not discarded",
          rows and rows[0]["requests_sum"] == 400.0,
          str(rows))
    check("per-day division uses the days that answered",
          g.summarise(rows, "demand_source", days)[0]["requests_day"] == 200.0,
          str(g.summarise(rows, "demand_source", days)[0]["requests_day"]))

    # ...but every day failing is still an empty read, not a zero.
    g.tbx.report = lambda df, dt, **kw: (_ for _ in ()).throw(
        g.tbx.TbxError("read timed out"))
    try:
        rows, days = g.pull_pairs(date(2026, 8, 25), 2)
    finally:
        g.tbx.report = real
    check("all days failing reports no coverage", days == 0, str(days))
    check("and returns nothing to report on", rows == [], str(rows))


def test_crash_exit_code_is_not_the_finding_code() -> None:
    """Exit 1 means 'found waste' and callers treat it as success.

    A Python traceback also exits 1, which is how a crashed pairs run reported
    success on 2026-08-31. The module guard must map an unreachable platform
    somewhere else.
    """
    print("\na crash is distinguishable from a finding")
    from pathlib import Path
    src = Path(g.__file__).read_text()
    guard = src.rsplit('if __name__ == "__main__":', 1)[-1]
    check("the entrypoint catches TbxError", "TbxError" in guard, guard.strip())
    check("and exits on a code callers do not treat as success",
          "sys.exit(3)" in guard, guard.strip())
    check("exit 1 is still documented as the finding code",
          "1  at least one wasteful country or pair" in src)


def test_parser() -> None:
    print("\nargument surface")
    import re
    from pathlib import Path
    args = g.build_parser().parse_args([])
    src = Path(g.__file__).read_text()
    used = set(re.findall(r"\bargs\.([a-z_]+)", src))
    missing = sorted(u for u in used if not hasattr(args, u))
    check("every args.X the module reads is defined by the parser",
          not missing, f"missing: {missing}")
    check("country is the default view", args.view == "country")


def main() -> int:
    print("=" * 70)
    print("tbx_geo_waste — offline checks")
    print("=" * 70)
    test_country_summary()
    test_pair_id_survives()
    test_waste_thresholds()
    test_country_view_waste()
    test_pull_pairs_filters()
    test_a_failed_day_does_not_lose_the_good_ones()
    test_crash_exit_code_is_not_the_finding_code()
    test_parser()
    print("\n" + "=" * 70)
    print(f"{PASS} passed, {FAIL} failed")
    return 1 if FAIL else 0


if __name__ == "__main__":
    sys.exit(main())
