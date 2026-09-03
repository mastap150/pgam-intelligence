#!/usr/bin/env python3
"""Offline checks for tbx_connection_margin — the arithmetic and the rails."""

from __future__ import annotations

import io
import os
import sys
from contextlib import redirect_stdout

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from scripts import tbx_connection_margin as cm     # noqa: E402

PASS = FAIL = 0
DAYS = ["2026-08-30", "2026-08-31", "2026-09-01"]


def check(label, ok, detail=""):
    global PASS, FAIL
    if ok:
        PASS += 1; print(f"  ✓ {label}")
    else:
        FAIL += 1; print(f"  ✗ {label}   {detail}")


def args_for(**over):
    a = cm.build_parser().parse_args([])
    a.include = set()
    for k, v in over.items():
        setattr(a, k, v)
    return a


def pair(sid, did, gross, take_pct, sname="Pub", dname="Buyer"):
    return {(sid, did): {"sname": sname, "dname": dname, "gross": gross,
                         "payout": gross * (1 - take_pct / 100.0), "imps": 1.0}}


def test_proposal_raises_the_floor_by_exactly_the_gap():
    print("\nthe additive proposal is floor + gap")
    p = cm.propose(6.9, {"type": "range", "min": 5.0, "max": 20.0}, 30.0)
    check("supply's effective share is take minus demand floor",
          abs(p["s_eff"] - 1.9) < 1e-9, str(p))
    check("new floor = 5 + (30 - 6.9) = 28.1", abs(p["additive"] - 28.1) < 1e-9, str(p))
    check("compound proposal is a little higher, not lower",
          p["compound"] > p["additive"], str(p))
    check("a range band is marked as an estimate", p["exact"] is False)
    p2 = cm.propose(34.7, {"type": "fixed", "min": 20.0, "max": 0.0}, 30.0)
    check("fixed band is exact", p2["exact"] is True)
    check("above target proposes a LOWER floor", p2["additive"] < 20.0, str(p2))


def test_fixed_band_collapses_max_to_min():
    print("\nband parsing")
    b = cm.band({"margin_type": "fixed", "margin_min": 20, "margin_max": 0})
    check("fixed: max == min", b["max"] == b["min"] == 20.0, str(b))
    b = cm.band({"margin_type": "range", "margin_min": "5", "margin_max": "30"})
    check("strings coerce", b["min"] == 5.0 and b["max"] == 30.0, str(b))
    check("unknown type is None", cm.band({})["type"] is None)


def test_fanout_detects_one_to_many():
    print("\na demand source selling on two supply sources is not 1:1")
    pairs = {}
    pairs.update(pair(1, 900, 300.0, 20))
    pairs.update(pair(2, 900, 300.0, 20))
    pairs.update(pair(3, 901, 300.0, 20))
    pairs.update(pair(4, 901, 0.3, 20))            # immaterial gross
    fan = cm.fanout(pairs, min_gross_day=5.0, n_days=3)
    check("900 fans out to two", fan[900] == [1, 2], str(fan))
    check("901's tiny second leg is ignored", fan[901] == [3], str(fan))


def test_plan_refuses_what_it_must():
    print("\napply plan rails")
    pairs = {}
    pairs.update(pair(10, 500, 300.0, 6.9, dname="Thin"))         # target
    pairs.update(pair(11, 501, 300.0, 6.9, dname="Fanout"))
    pairs.update(pair(12, 501, 300.0, 6.9, dname="Fanout"))
    pairs.update(pair(13, 502, 300.0, 34.0, dname="Rich"))
    pairs.update(pair(14, 503, 300.0, 6.9, dname="NoCfg"))
    s_cfg = {i: {"type": "range", "min": 5.0, "max": 30.0} for i in (10, 11, 12, 13, 14)}
    d_cfg = {500: {"type": "range", "min": 5.0, "max": 20.0},
             501: {"type": "range", "min": 5.0, "max": 20.0},
             502: {"type": "fixed", "min": 20.0, "max": 20.0}}
    fan = cm.fanout(pairs, 5.0, 3)
    a = args_for(include={500, 501, 502, 503})
    rows = cm.assess(pairs, DAYS, s_cfg, d_cfg, fan, a)
    todo, refused = cm.plan_writes(rows, a)
    ids = [w["did"] for w in todo]
    check("only the thin 1:1 connection is written", ids == [500], str(ids))
    check("fan-out is refused", any("501" in r and "1:1" in r for r in refused), str(refused))
    check("at-target is refused", any("502" in r and "target" in r for r in refused), str(refused))
    check("no config is refused", any("503" in r and "config" in r for r in refused), str(refused))
    w = todo[0]
    check("the raise is capped at --max-raise-pp",
          abs(w["margin_min"] - 20.0) < 1e-9 and "capped" in w["note"], str(w))
    check("max is lifted to at least the new floor", w["margin_max"] >= w["margin_min"], str(w))
    check("before is recorded for the revert", w["before"] == d_cfg[500], str(w))

    a2 = args_for(include={500}, max_raise_pp=50.0)
    todo2, _ = cm.plan_writes(cm.assess(pairs, DAYS, s_cfg, d_cfg, fan, a2), a2)
    check("uncapped: 5 + 23.1 = 28.1", abs(todo2[0]["margin_min"] - 28.1) < 1e-9, str(todo2))


def test_three_defects_from_the_2026_09_03_rollout():
    """Each of these cost a write on the live platform before it was a test."""
    print("\nfixed sends one number; max stays strictly above min; adaptive is refused")
    pairs = {}
    pairs.update(pair(10, 500, 300.0, 10.0, dname="FixedZero"))   # #35
    pairs.update(pair(11, 501, 300.0, 19.3, dname="TightMax"))    # #2408
    pairs.update(pair(12, 502, 300.0, 6.9, dname="Adaptive"))     # #1986
    s_cfg = {i: {"type": "fixed", "min": 10.0, "max": 10.0} for i in (10, 11, 12)}
    d_cfg = {500: {"type": "fixed", "min": 0.0, "max": 0.0},
             501: {"type": "range", "min": 15.0, "max": 25.0},
             502: {"type": "adaptive", "min": 5.0, "max": 95.0}}
    fan = cm.fanout(pairs, 5.0, 3)
    a = args_for(include={500, 501, 502}, max_raise_pp=50.0)
    todo, refused = cm.plan_writes(cm.assess(pairs, DAYS, s_cfg, d_cfg, fan, a), a)
    by = {w["did"]: w for w in todo}
    check("fixed band: margin_max is None (not sent)",
          500 in by and by[500]["margin_max"] is None, str(by.get(500)))
    check("fixed band: floor still proposed", by[500]["margin_min"] == 20.0, str(by[500]))
    w = by[501]
    check("proposal 15 + (30 - 19.3) = 25.7 clears the 25 ceiling",
          abs(w["margin_min"] - 25.7) < 1e-9, str(w))
    check("so the ceiling is lifted STRICTLY above the floor, not to it",
          w["margin_max"] > w["margin_min"], str(w))
    check("by the headroom constant",
          abs(w["margin_max"] - (25.7 + cm.MAX_HEADROOM_PP)) < 1e-9, str(w))
    check("adaptive band is refused, not written", 502 not in by, str(by))
    check("and the refusal names why", any("adaptive" in r and "502" in r for r in refused),
          str(refused))

    # a ceiling already above the floor is left alone
    d_cfg[501] = {"type": "range", "min": 15.0, "max": 40.0}
    todo, _ = cm.plan_writes(cm.assess(pairs, DAYS, s_cfg, d_cfg, fan, a), a)
    check("ceiling above the new floor is untouched",
          {w["did"]: w for w in todo}[501]["margin_max"] == 40.0, str(todo))

    # the write path honours a None max
    seen = []
    real = cm.tbm.set_demand_economics
    cm.tbm.set_demand_economics = lambda did, **kw: seen.append((did, kw)) or {"applied": False}
    try:
        cm.apply_writes([by[500]], args_for(apply=False))
    finally:
        cm.tbm.set_demand_economics = real
    check("fixed: margin_max is not in the call", "margin_max" not in seen[0][1], str(seen))
    check("fixed: margin_min is", seen[0][1]["margin_min"] == 20.0, str(seen))


def test_max_apply_caps_the_run():
    print("\n--max-apply")
    pairs = {}
    for i in range(6):
        pairs.update(pair(100 + i, 600 + i, 300.0, 6.9))
    cfg_d = {600 + i: {"type": "range", "min": 5.0, "max": 20.0} for i in range(6)}
    cfg_s = {100 + i: {"type": "range", "min": 5.0, "max": 30.0} for i in range(6)}
    a = args_for(include=set(cfg_d), max_apply=3)
    rows = cm.assess(pairs, DAYS, cfg_s, cfg_d, cm.fanout(pairs, 5.0, 3), a)
    todo, _ = cm.plan_writes(rows, a)
    check("three, not six", len(todo) == 3, str(len(todo)))


def test_apply_needs_include_and_never_writes_book_wide():
    print("\n--apply without --include is refused before measuring")
    calls = []
    real_conf, real_pull = cm.tbx.configured, cm.pull_pairs
    cm.tbx.configured = lambda: True
    cm.pull_pairs = lambda s, d: calls.append("pulled") or ({}, [])
    try:
        with redirect_stdout(io.StringIO()):
            rc = cm.main(["--apply"])
    finally:
        cm.tbx.configured, cm.pull_pairs = real_conf, real_pull
    check("exit 1", rc == 1, str(rc))
    check("nothing was pulled", calls == [], str(calls))


def test_partial_window_refuses():
    print("\na partial window refuses to conclude")
    real_conf, real_pull = cm.tbx.configured, cm.pull_pairs
    cm.tbx.configured = lambda: True
    cm.pull_pairs = lambda s, d: (pair(1, 2, 100.0, 10), [DAYS[0]])
    try:
        with redirect_stdout(io.StringIO()):
            rc = cm.main(["--days", "3"])
    finally:
        cm.tbx.configured, cm.pull_pairs = real_conf, real_pull
    check("exit 2", rc == 2, str(rc))


def test_dry_run_writes_nothing_and_revert_restores_before():
    print("\ndry run passes dry_run=True; revert sends the recorded band back")
    seen = []
    real = cm.tbm.set_demand_economics
    cm.tbm.set_demand_economics = lambda did, **kw: seen.append((did, kw)) or {"applied": not kw["dry_run"]}
    try:
        todo = [{"did": 500, "dname": "Thin", "sid": 10, "sname": "Pub", "take": 6.9,
                 "gross_day": 100.0, "before": {"type": "range", "min": 5.0, "max": 20.0},
                 "margin_min": 20.0, "margin_max": 20.0, "note": ""}]
        entries, fails = cm.apply_writes(todo, args_for(apply=False))
        check("dry_run=True passed", seen[0][1]["dry_run"] is True, str(seen))
        check("floor and max both sent", seen[0][1]["margin_min"] == 20.0
              and seen[0][1]["margin_max"] == 20.0, str(seen))
        check("no failures", fails == 0)
        import json, tempfile
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "margin-ledger.json")
            with open(path, "w") as fh:
                json.dump({"entries": [{**todo[0], "applied": True}]}, fh)
            seen.clear()
            with redirect_stdout(io.StringIO()):
                rc = cm.revert(path, args_for(apply=True))
        check("revert ok", rc == 0, str(rc))
        check("revert restores min 5 / max 20",
              seen[0][1]["margin_min"] == 5.0 and seen[0][1]["margin_max"] == 20.0, str(seen))
    finally:
        cm.tbm.set_demand_economics = real


def test_parser_surface():
    print("\nargument surface")
    import re
    used = set(re.findall(r"args\.([a-z_]+)", open(cm.__file__).read()))
    ns = vars(cm.build_parser().parse_args([]))
    missing = sorted(used - set(ns))
    check("every args.X is defined", not missing, str(missing))
    check("target defaults to 30", ns["target"] == 30.0)
    check("apply is opt-in", ns["apply"] is False)
    check("a per-run cap exists", ns["max_apply"] == 3)
    check("a per-step raise cap exists", ns["max_raise_pp"] == 15.0)


def main():
    print("=" * 70 + "\ntbx_connection_margin — offline checks\n" + "=" * 70)
    test_proposal_raises_the_floor_by_exactly_the_gap()
    test_fixed_band_collapses_max_to_min()
    test_fanout_detects_one_to_many()
    test_plan_refuses_what_it_must()
    test_three_defects_from_the_2026_09_03_rollout()
    test_max_apply_caps_the_run()
    test_apply_needs_include_and_never_writes_book_wide()
    test_partial_window_refuses()
    test_dry_run_writes_nothing_and_revert_restores_before()
    test_parser_surface()
    print("\n" + "=" * 70 + f"\n{PASS} passed, {FAIL} failed")
    return 1 if FAIL else 0


if __name__ == "__main__":
    sys.exit(main())
