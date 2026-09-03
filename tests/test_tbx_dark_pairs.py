#!/usr/bin/env python3
"""Offline checks for tbx_dark_pairs — selection rails, list edits, revert."""
from __future__ import annotations
import io, os, sys, json, tempfile
from contextlib import redirect_stdout
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from scripts import tbx_dark_pairs as dp   # noqa: E402

PASS = FAIL = 0
def check(l, ok, d=""):
    global PASS, FAIL
    if ok: PASS += 1; print(f"  ✓ {l}")
    else:  FAIL += 1; print(f"  ✗ {l}   {d}")

DAYS = ["2026-09-01", "2026-09-02", "2026-09-03"]

def args_for(**o):
    a = dp.build_parser().parse_args([]); a.include = set(); a.exclude = set()
    for k, v in o.items(): setattr(a, k, v)
    return a

def pair(sid, did, reqs, resps, days=DAYS):
    return {"sname": f"S{sid}", "dname": f"D{did}",
            "per_day": {d: (float(r), float(s)) for d, r, s in zip(days, reqs, resps)}}


def test_select_rails():
    print("\nselection rails")
    seen = {
        (6, 566):  pair(6, 566, [266000, 266000, 266000], [0, 0, 0]),      # dark, demand alive elsewhere
        (6, 999):  pair(6, 999, [20000, 20000, 20000], [0, 0, 5]),         # answered once
        (6, 111):  pair(6, 111, [20000, 9000, 20000], [0, 0, 0]),          # thin day
        (6, 222):  pair(6, 222, [20000, 20000], [0, 0], DAYS[1:]),         # new mid-window
        (6, 333):  pair(6, 333, [20000, 20000, 20000], [0, 0, 0]),         # demand dark everywhere
        (7, 566):  pair(7, 566, [50000, 50000, 50000], [10, 10, 10]),      # 566 answers on supply 7
        (8, 444):  pair(8, 444, [20000, 20000, 20000], [0, 0, 0]),         # already blocked
        (9, 555):  pair(9, 555, [20000, 20000, 20000], [0, 0, 0]),         # allowlist, removable
        (10, 777): pair(10, 777, [20000, 20000, 20000], [0, 0, 0]),        # allowlist, last entry
        (11, 888): pair(11, 888, [20000, 20000, 20000], [0, 0, 0]),        # allowlist w/o it
        (12, 123): pair(12, 123, [20000, 20000, 20000], [0, 0, 0]),        # demand off
    }
    # give every candidate demand some responses elsewhere except 333
    for did in (444, 555, 777, 888, 123):
        seen[(99, did)] = pair(99, did, [100, 100, 100], [1, 1, 1])
    dresp = dp.demand_responses(seen)
    status = {d: True for d in (566, 999, 111, 222, 333, 444, 555, 777, 888)}
    status[123] = False
    lists = {6: {"is_allowed": False, "demand_sources": [1, 2]},
             8: {"is_allowed": False, "demand_sources": [444]},
             9: {"is_allowed": True, "demand_sources": [555, 556]},
             10: {"is_allowed": True, "demand_sources": [777]},
             11: {"is_allowed": True, "demand_sources": [1]},
             12: {"is_allowed": False, "demand_sources": []}}
    real = dp.partner_freeze.is_frozen
    dp.partner_freeze.is_frozen = lambda **k: False
    try:
        targets, held = dp.select(seen, DAYS, dresp, status, lists, args_for())
    finally:
        dp.partner_freeze.is_frozen = real
    ids = {(t["sid"], t["did"]): t["mode"] for t in targets}
    check("dark pair on blocklist supply → block", ids.get((6, 566)) == "block", str(ids))
    check("dark pair on allowlist supply → allow (remove)", ids.get((9, 555)) == "allow", str(ids))
    check("answered once → not a target", (6, 999) not in ids)
    check("thin day → not a target", (6, 111) not in ids)
    check("new mid-window → not a target", (6, 222) not in ids)
    why = {(r["sid"], r["did"]): w for r, w in held}
    check("demand dark everywhere → held for dark_demand", "dark_demand" in why.get((6, 333), ""), str(why))
    check("already blocked → held", "already" in why.get((8, 444), ""), str(why))
    check("last allowlist entry → refused", "empty" in why.get((10, 777), ""), str(why))
    check("allowlist not containing it → refused", "not governing" in why.get((11, 888), ""), str(why))
    check("demand source off → held", "already off" in why.get((12, 123), ""), str(why))
    check("biggest first", targets[0]["did"] == 566)
    # frozen partner
    dp.partner_freeze.is_frozen = lambda **k: k.get("demand_id") == 566
    try:
        targets2, held2 = dp.select(seen, DAYS, dresp, status, lists, args_for())
    finally:
        dp.partner_freeze.is_frozen = real
    check("frozen partner held", any(r["did"] == 566 and "frozen" in w for r, w in held2), str(held2))
    # exclude / include
    dp.partner_freeze.is_frozen = lambda **k: False
    try:
        t3, h3 = dp.select(seen, DAYS, dresp, status, lists, args_for(exclude={6}))
        check("exclude by supply id", all(t["sid"] != 6 for t in t3) and any("excluded" in w for _, w in h3))
        t4, _ = dp.select(seen, DAYS, dresp, status, lists, args_for(include={555}))
        check("include narrows to that demand", [t["did"] for t in t4] == [555], str(t4))
    finally:
        dp.partner_freeze.is_frozen = real
    return targets, lists


def test_plan_apply_revert(targets, lists):
    print("\nplan groups per supply; apply/revert replace the full list")
    lists = dict(lists)
    lists[6] = {"is_allowed": False, "demand_sources": [1, 2]}
    two = [t for t in targets if t["sid"] in (6, 9)]
    two.append({"sid": 6, "sname": "S6", "did": 567, "dname": "D567", "requests_day": 1.0, "mode": "block"})
    writes = dp.plan_writes(two, lists)
    w6 = [w for w in writes if w["sid"] == 6][0]
    w9 = [w for w in writes if w["sid"] == 9][0]
    check("blocklist gains both dids, keeps existing", w6["after"] == [1, 2, 566, 567], str(w6))
    check("allowlist drops the did, keeps the rest", w9["after"] == [556], str(w9))
    check("one write per supply", len(writes) == 2)
    seen = []
    real = dp.tbm.set_supply_allowed_demand
    dp.tbm.set_supply_allowed_demand = lambda sid, ids, **kw: seen.append((sid, ids, kw)) or {"applied": not kw["dry_run"], "verify_ok": True}
    try:
        entries, fails = dp.apply(writes, args_for(apply=False))
        check("dry_run passed", all(s[2]["dry_run"] for s in seen))
        s6 = [s for s in seen if s[0] == 6][0]
        check("mode preserved on write (blocklist)", s6[2]["is_allowed"] is False and s6[1] == [1, 2, 566, 567], str(s6))
        with tempfile.TemporaryDirectory() as tmp:
            p = os.path.join(tmp, "l.json")
            json.dump({"entries": [{**e, "applied": True} for e in entries]}, open(p, "w"))
            seen.clear()
            with redirect_stdout(io.StringIO()):
                rc = dp.revert(p, args_for(apply=True))
        check("revert ok", rc == 0)
        r6 = [s for s in seen if s[0] == 6][0]
        r9 = [s for s in seen if s[0] == 9][0]
        check("revert restores blocklist exactly", r6[1] == [1, 2] and r6[2]["is_allowed"] is False, str(r6))
        check("revert restores allowlist exactly", r9[1] == [555, 556] and r9[2]["is_allowed"] is True, str(r9))
    finally:
        dp.tbm.set_supply_allowed_demand = real


def test_helpers_and_gates():
    print("\nhelpers and gates")
    seen = {(1, 5): pair(1, 5, [1, 1, 1], [0, 2, 0]), (2, 5): pair(2, 5, [1, 1, 1], [3, 0, 0])}
    check("demand_responses sums across supplies", dp.demand_responses(seen)[5] == 5.0)
    real = dp.tbm.get_supply_source
    dp.tbm.get_supply_source = lambda sid: {"name": "x", "is_allowed_sources": True,
                                           "demand_sources": [{"id": 3}, 4, "5", "junk"]}
    try:
        out = dp.supply_lists({1})
        check("supply_lists normalises ints/dicts/strings", out[1]["demand_sources"] == [3, 4, 5] and out[1]["is_allowed"] is True, str(out))
    finally:
        dp.tbm.get_supply_source = real
    import re
    used = set(re.findall(r"args\.([a-z_]+)", open(dp.__file__).read()))
    ns = vars(dp.build_parser().parse_args([]))
    check("every args.X defined", not (used - set(ns)), str(sorted(used - set(ns))))
    check("defaults: 3 days, 10k req/day, cap 10", ns["days"] == 3 and ns["min_requests_day"] == 10000 and ns["max_pause"] == 10)


def main():
    print("=" * 70 + "\ntbx_dark_pairs — offline checks\n" + "=" * 70)
    targets, lists = test_select_rails()
    test_plan_apply_revert(targets, lists)
    test_helpers_and_gates()
    print("\n" + "=" * 70 + f"\n{PASS} passed, {FAIL} failed")
    return 1 if FAIL else 0

if __name__ == "__main__":
    sys.exit(main())
