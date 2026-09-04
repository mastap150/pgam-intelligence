#!/usr/bin/env python3
"""
Offline checks for scripts/tbx_dark_demand.py.

This is the first TBX script meant to run unattended and switch partners off,
so the tests are weighted almost entirely at the ways an automation turns a
measurement problem into an outage:

  * a day that failed to answer must NEVER read as a day of silence
  * a source that answered once must never be called dark
  * a source too new to judge must be left alone
  * one run must not be able to disable the whole book

No credentials, no platform call.
"""

from __future__ import annotations

import json
import sys
import tempfile
from datetime import date
from pathlib import Path

sys.path.insert(0, __file__.rsplit("/tests/", 1)[0])

from scripts import tbx_dark_demand as dd    # noqa: E402

PASS = FAIL = 0


def check(label: str, ok: bool, detail: str = "") -> None:
    global PASS, FAIL
    if ok:
        PASS += 1
        print(f"  ✓ {label}")
    else:
        FAIL += 1
        print(f"  ✗ {label}" + (f"  — {detail}" if detail else ""))


DAYS = ["2026-08-26", "2026-08-27", "2026-08-28"]


def args_for(**over):
    args = dd.build_parser().parse_args([])
    args.exclude = dict(dd.NEVER_AUTO_DISABLE)
    args.include = set()
    for k, v in over.items():
        setattr(args, k, v)
    return args


def src(did, name, per_day):
    return {did: {"name": name, "per_day": dict(per_day)}}


def all_on(seen):
    """Status map in which every measured source is still switched on."""
    return {did: True for did in seen}


def test_dark_source_is_selected() -> None:
    print("\na source that answered nothing on every day")
    seen = src(501, "Dead DSP", {d: (500_000.0, 0.0) for d in DAYS})
    targets, _, _ = dd.select(seen, DAYS, all_on(seen), args_for())
    check("it is selected", [t["id"] for t in targets] == [501], str(targets))
    check("the per-day request rate is reported",
          targets[0]["requests_day"] == 500_000.0, str(targets[0]))


def test_one_response_saves_it() -> None:
    print("\na single response anywhere in the window")
    for day in DAYS:
        per = {d: (500_000.0, 0.0) for d in DAYS}
        per[day] = (500_000.0, 1.0)          # answered exactly once
        targets, _, _ = dd.select(src(501, "Blinker", per), DAYS, {501: True}, args_for())
        check(f"one response on {day} spares it", targets == [], str(targets))


def test_a_new_source_is_not_dark() -> None:
    """Absent for part of the window means new, not silent."""
    print("\na source that appears mid-window")
    per = {DAYS[1]: (500_000.0, 0.0), DAYS[2]: (500_000.0, 0.0)}
    targets, _, _ = dd.select(src(501, "Newcomer", per), DAYS, {501: True}, args_for())
    check("it is left alone", targets == [], str(targets))


def test_low_volume_day_disqualifies() -> None:
    print("\na day with too few requests is not a fair chance to answer")
    per = {d: (500_000.0, 0.0) for d in DAYS}
    per[DAYS[1]] = (9.0, 0.0)
    targets, _, _ = dd.select(src(501, "Barely Used", per), DAYS, {501: True}, args_for())
    check("one thin day spares the source", targets == [], str(targets))


def test_exclusions() -> None:
    print("\nexclusions")
    seen = src(501, "Dead DSP", {d: (500_000.0, 0.0) for d in DAYS})
    targets, skipped, _ = dd.select(seen, DAYS, all_on(seen),
                                 args_for(exclude={501: "ask the AM first"}))
    check("an excluded source is not selected", targets == [], str(targets))
    check("the reason is carried", any("AM" in w for _, w in skipped),
          str(skipped))

    targets, _, _ = dd.select(seen, DAYS, all_on(seen), args_for(include={999}))
    check("--include filters to the named ids", targets == [], str(targets))


def test_failed_day_never_reads_as_silence() -> None:
    """The property this whole design exists for.

    A day whose query raises returns no rows. Summed over a window that is
    indistinguishable from a day on which nobody answered — and an unattended
    job acting on that would switch off the entire demand side during a
    platform outage.
    """
    print("\na day that failed is not a day of silence")
    calls = []

    def flaky(df, dt, **kw):
        calls.append(df)
        if df == "2026-08-27":
            raise dd.tbx.TbxError("read timed out")
        return [{"date": df, "demand_source": "A #1",
                 "requests_sum": "500000", "responses_sum": "0"}], {}

    real = dd.tbx.report
    dd.tbx.report = flaky
    try:
        seen, answered = dd.pull_days(date(2026, 8, 26), 3)
    finally:
        dd.tbx.report = real

    check("all three days were attempted", len(calls) == 3, str(calls))
    check("the failed day is not counted as answered",
          answered == ["2026-08-26", "2026-08-28"], str(answered))
    check("the source is still tracked on the days that worked",
          len(seen[1]["per_day"]) == 2, str(seen))


def test_partial_window_refuses_to_conclude() -> None:
    print("\nmain() refuses a window it could not fully measure")
    real_conf, real_pull = dd.tbx.configured, dd.pull_days
    dd.tbx.configured = lambda: True
    dd.pull_days = lambda s, d: (src(501, "X", {DAYS[0]: (9e5, 0.0)}), [DAYS[0]])
    try:
        rc = dd.main(["--days", "3", "--apply"])
    finally:
        dd.tbx.configured, dd.pull_days = real_conf, real_pull
    check("it exits 2 rather than reporting", rc == 2, str(rc))


def test_max_disable_caps_a_run() -> None:
    print("\none run cannot switch off the whole book")
    seen = {}
    for i in range(40):
        seen.update(src(i, f"Dead {i}",
                        {d: (1_000_000.0 - i, 0.0) for d in DAYS}))
    targets, _, _ = dd.select(seen, DAYS, all_on(seen), args_for())
    check("all 40 are detected", len(targets) == 40, str(len(targets)))
    to_cut = dd.render(targets, [], [], DAYS, args_for(max_disable=25))
    check("only 25 are handed to the writer", len(to_cut) == 25, str(len(to_cut)))
    check("the biggest are the ones cut",
          to_cut[0]["requests_day"] == 1_000_000.0, str(to_cut[0]))


def test_an_already_disabled_source_is_left_alone() -> None:
    """The defect the first live run exposed.

    The window is history. Disabling a source does not remove the requests it
    was already sent, so yesterday's cut is still dark today — and without a
    status check the job re-flags its own past work every morning, spends the
    --max-disable cap on writes that change nothing, and buries whatever is
    genuinely new underneath.
    """
    print("\nalready-disabled sources do not come back round")
    seen = {}
    seen.update(src(501, "Cut Last Night", {d: (900_000.0, 0.0) for d in DAYS}))
    seen.update(src(502, "Newly Dark", {d: (100_000.0, 0.0) for d in DAYS}))

    targets, _, already = dd.select(seen, DAYS, {501: False, 502: True},
                                    args_for())
    check("only the one still switched on is a target",
          [r["id"] for r in targets] == [502], str(targets))
    check("the one already off is reported, not dropped",
          [r["id"] for r in already] == [501], str(already))

    to_cut = dd.render(targets, [], already, DAYS, args_for())
    check("and it is not handed to the writer",
          [r["id"] for r in to_cut] == [502], str(to_cut))


def test_an_unknown_source_is_not_written_to() -> None:
    print("\na source missing from the dictionary is not guessed at")
    seen = src(501, "Ghost", {d: (900_000.0, 0.0) for d in DAYS})
    targets, _, already = dd.select(seen, DAYS, {999: True}, args_for())
    check("not selected", targets == [], str(targets))
    check("accounted for rather than silently dropped",
          [r["id"] for r in already] == [501], str(already))


def test_the_cap_is_spent_on_live_sources() -> None:
    """max_disable must budget writes that do something."""
    print("\nthe cap counts sources that are actually still on")
    seen = {}
    for i in range(40):
        seen.update(src(i, f"Dead {i}", {d: (1_000_000.0 - i, 0.0)
                                         for d in DAYS}))
    # The 20 biggest were cut yesterday.
    status = {i: i >= 20 for i in range(40)}
    targets, _, already = dd.select(seen, DAYS, status, args_for())
    check("20 already off", len(already) == 20, str(len(already)))
    to_cut = dd.render(targets, [], already, DAYS, args_for(max_disable=25))
    check("the remaining 20 all fit under the cap of 25",
          len(to_cut) == 20, str(len(to_cut)))
    check("and none of them is one that was already off",
          all(r["id"] >= 20 for r in to_cut), str([r["id"] for r in to_cut]))


def test_an_empty_status_map_refuses_to_write() -> None:
    """Same class of failure as an unmeasured day: state we could not read."""
    print("\nan unreadable demand list refuses to conclude anything")
    calls = []
    real_conf, real_pull = dd.tbx.configured, dd.pull_days
    real_status, real_set = dd.live_status, dd.tbm.set_demand_source_status
    dd.tbx.configured = lambda: True
    dd.pull_days = lambda start, days: (
        src(501, "Dead DSP", {d: (900_000.0, 0.0) for d in DAYS}), list(DAYS))
    dd.live_status = lambda: {}
    dd.tbm.set_demand_source_status = lambda *a, **k: calls.append(a)
    try:
        rc = dd.main(["--days", "3", "--apply"])
    finally:
        dd.tbx.configured, dd.pull_days = real_conf, real_pull
        dd.live_status, dd.tbm.set_demand_source_status = real_status, real_set
    check("exits 2", rc == 2, str(rc))
    check("and wrote nothing", calls == [], str(calls))


def test_dry_run_and_ledger_round_trip() -> None:
    print("\ndry run writes nothing; the ledger reverts exactly")
    seen_calls = []

    def writer(did, enabled, **kw):
        seen_calls.append((did, enabled, kw.get("dry_run")))
        return {"applied": not kw.get("dry_run")}

    real = dd.tbm.set_demand_source_status
    dd.tbm.set_demand_source_status = writer
    try:
        rows = [{"id": 501, "name": "Dead", "requests_day": 5e5}]
        dd.apply_cuts(rows, args_for(apply=False))
        check("a dry run passes dry_run=True",
              seen_calls[-1][2] is True, str(seen_calls))
        check("and asks for disable, not enable",
              seen_calls[-1][1] is False, str(seen_calls))

        entries, failures = dd.apply_cuts(rows, args_for(apply=True))
        check("an applied run records the write", entries[0]["applied"] is True,
              str(entries))
        check("no failures", failures == 0, str(failures))

        seen_calls.clear()
        with tempfile.TemporaryDirectory() as tmp:
            p = Path(tmp) / "dark-ledger.json"
            p.write_text(json.dumps({"entries": entries}))
            rc = dd.revert(str(p), args_for(apply=True))
        check("revert succeeds", rc == 0, str(rc))
        check("revert RE-ENABLES", seen_calls[-1][1] is True, str(seen_calls))
        check("and targets the same id", seen_calls[-1][0] == 501, str(seen_calls))
    finally:
        dd.tbm.set_demand_source_status = real


def test_unattended_run_announces_itself() -> None:
    """The schedule auto-applies, so a write nobody sees is the failure mode."""
    print("\nan applied run posts what it switched off")
    posted = []
    real = dd.slack
    class FakeSlack:
        @staticmethod
        def send_text(text):
            posted.append(text)
    dd.slack = FakeSlack
    try:
        entries = [{"id": 501, "name": "Dead DSP", "requests_day": 5e5,
                    "applied": True}]
        dd.announce(entries, args_for(days=3), "dark-ledger-X.json")
        check("something was posted", len(posted) == 1, str(posted))
        body = posted[0]
        check("it names the source", "Dead DSP #501" in body, body[:120])
        check("it says how many days", "3 settled days" in body, body[:160])
        check("it names the ledger for the undo",
              "dark-ledger-X.json" in body, body[-160:])

        posted.clear()
        dd.announce([], args_for(), "x.json")
        check("nothing applied means nothing posted", posted == [], str(posted))
    finally:
        dd.slack = real


def test_a_failed_post_never_loses_the_cut() -> None:
    print("\na broken webhook does not undo a write")
    real = dd.slack
    class Exploding:
        @staticmethod
        def send_text(text):
            raise RuntimeError("webhook 500")
    dd.slack = Exploding
    try:
        entries = [{"id": 1, "name": "X", "requests_day": 1.0, "applied": True}]
        dd.announce(entries, args_for(), "l.json")     # must not raise
        check("announce swallows the failure", True)
    except Exception as exc:                            # noqa: BLE001
        check("announce swallows the failure", False, str(exc))
    finally:
        dd.slack = real

    # And with no slack module at all, which is the local-dev case.
    dd_slack, dd.slack = dd.slack, None
    try:
        dd.announce([{"id": 1, "name": "X", "requests_day": 1.0,
                      "applied": True}], args_for(), "l.json")
        check("a missing slack module is not an error", True)
    finally:
        dd.slack = dd_slack


def test_parser_and_defaults() -> None:
    print("\nargument surface")
    import re
    args = dd.build_parser().parse_args([])
    src_text = Path(dd.__file__).read_text()
    used = set(re.findall(r"\bargs\.([a-z_]+)", src_text)) - {"exclude", "include"}
    missing = sorted(u for u in used if not hasattr(args, u))
    check("every args.X is defined by the parser", not missing, f"missing: {missing}")
    check("three days is the default", args.days == 3, str(args.days))
    check("applying is opt-in", args.apply is False)
    check("a cap exists by default", args.max_disable == 25, str(args.max_disable))


def main() -> int:
    print("=" * 70)
    print("tbx_dark_demand — offline checks")
    print("=" * 70)
    test_dark_source_is_selected()
    test_one_response_saves_it()
    test_a_new_source_is_not_dark()
    test_low_volume_day_disqualifies()
    test_exclusions()
    test_failed_day_never_reads_as_silence()
    test_partial_window_refuses_to_conclude()
    test_max_disable_caps_a_run()
    test_an_already_disabled_source_is_left_alone()
    test_an_unknown_source_is_not_written_to()
    test_the_cap_is_spent_on_live_sources()
    test_an_empty_status_map_refuses_to_write()
    test_dry_run_and_ledger_round_trip()
    test_unattended_run_announces_itself()
    test_a_failed_post_never_loses_the_cut()
    test_parser_and_defaults()
    print("\n" + "=" * 70)
    print(f"{PASS} passed, {FAIL} failed")
    return 1 if FAIL else 0


if __name__ == "__main__":
    sys.exit(main())
