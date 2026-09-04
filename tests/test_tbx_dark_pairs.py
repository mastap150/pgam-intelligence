#!/usr/bin/env python3
"""Offline checks for tbx_dark_pairs — selection rails, side choice, list edits, revert."""
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

def L(allow, ids, companies=(), company=None):
    return {"is_allowed": allow, "ids": list(ids), "companies": list(companies),
            "company_id": company, "name": "x"}


def test_choose_side():
    print("\nchoose_side — blocklists first, allowlists only without a company allow")
    S_block = L(False, [1]); D_any = L(True, [9], company=50)
    check("supply blocklist → add there", dp.choose_side(9, 7, S_block, D_any) == ("supply_block", ""))
    check("already in supply blocklist → alert", dp.choose_side(9, 1, S_block, D_any)[1].startswith(dp.ALERT))
    S_allow = L(True, [7, 8], companies=[], company=60); D_block = L(False, [3])
    check("demand blocklist beats supply allowlist", dp.choose_side(9, 7, S_allow, D_block) == ("demand_block", ""))
    check("already in demand blocklist → alert", dp.choose_side(3, 7, S_allow, D_block)[1].startswith(dp.ALERT))
    D_allow = L(True, [9, 10], companies=[], company=50)
    check("supply allowlist contains did, no company allow → remove there",
          dp.choose_side(9, 7, S_allow, D_allow) == ("supply_allow", ""))
    S_co = L(True, [7, 8], companies=[50], company=60)
    check("supply allowlist but company allowed → fall to demand allowlist",
          dp.choose_side(9, 7, S_co, D_allow) == ("demand_allow", ""))
    D_co = L(True, [9, 10], companies=[60], company=50)
    m, why = dp.choose_side(9, 7, S_co, D_co)
    check("both company-allowed → alert, no write", m is None and "company #50" in why and "company #60" in why, why)
    S_last = L(True, [7], companies=[], company=60)
    m, why = dp.choose_side(9, 7, S_last, D_co)
    check("would empty supply allowlist → refused", m is None and "empty the supply" in why, why)
    S_absent = L(True, [8], companies=[], company=60)
    m, why = dp.choose_side(9, 7, S_absent, D_co)
    check("id absent from allowlist yet traffic → alert", m is None and "not in the supply allowlist" in why, why)


def test_select_rails():
    print("\nselection rails")
    seen = {
        (6, 566): pair(6, 566, [266000, 266000, 266000], [0, 0, 0]),   # dark, alive elsewhere
        (6, 999): pair(6, 999, [20000, 20000, 20000], [0, 0, 5]),      # answered once
        (6, 111): pair(6, 111, [20000, 9000, 20000], [0, 0, 0]),       # thin day
        (6, 222): pair(6, 222, [20000, 20000], [0, 0], DAYS[1:]),      # new mid-window
        (6, 333): pair(6, 333, [20000, 20000, 20000], [0, 0, 0]),      # demand dark everywhere
        (7, 566): pair(7, 566, [50000, 50000, 50000], [10, 10, 10]),   # 566 answers on 7
        (12, 123): pair(12, 123, [20000, 20000, 20000], [0, 0, 0]),    # demand off
        (13, 444): pair(13, 444, [20000, 20000, 20000], [0, 0, 0]),    # both allow + company → alert
    }
    for did in (999, 111, 222, 123, 444):
        seen[(99, did)] = pair(99, did, [100, 100, 100], [1, 1, 1])
    dresp = dp.demand_responses(seen)
    status = {d: True for d in (566, 999, 111, 222, 333, 444)}; status[123] = False
    slists = {6: L(False, [1, 2]), 12: L(False, []), 13: L(True, [444], companies=[5], company=1)}
    dlists = {566: L(True, [6, 7], company=9), 333: L(True, [6], company=9),
              123: L(True, [12], company=9), 444: L(True, [13, 14], companies=[1], company=5)}
    real = dp.partner_freeze.is_frozen
    dp.partner_freeze.is_frozen = lambda **k: False
    try:
        targets, held = dp.select(seen, DAYS, dresp, status, slists, dlists, args_for())
    finally:
        dp.partner_freeze.is_frozen = real
    modes = {(t["sid"], t["did"]): t["mode"] for t in targets}
    check("dark pair on blocklist supply → supply_block", modes.get((6, 566)) == "supply_block", str(modes))
    check("answered once / thin day / new → not targets", not ({(6, 999), (6, 111), (6, 222)} & set(modes)))
    why = {(r["sid"], r["did"]): w for r, w in held}
    check("demand dark everywhere → dark_demand's case", "dark_demand" in why.get((6, 333), ""), str(why))
    check("demand off → held", "already off" in why.get((12, 123), ""), str(why))
    check("both sides company-allowed → ALERT", why.get((13, 444), "").startswith(dp.ALERT), str(why))
    dp.partner_freeze.is_frozen = lambda **k: k.get("demand_id") == 566
    try:
        _, held2 = dp.select(seen, DAYS, dresp, status, slists, dlists, args_for())
    finally:
        dp.partner_freeze.is_frozen = real
    check("frozen partner held", any(r["did"] == 566 and "frozen" in w for r, w in held2))
    dp.partner_freeze.is_frozen = lambda **k: False
    try:
        t3, h3 = dp.select(seen, DAYS, dresp, status, slists, dlists, args_for(exclude={6}))
        check("exclude by supply id", all(t["sid"] != 6 for t in t3) and any("excluded" in w for _, w in h3))
        t4, _ = dp.select(seen, DAYS, dresp, status, slists, dlists, args_for(include={566}))
        check("include narrows", [t["did"] for t in t4] == [566], str(t4))
    finally:
        dp.partner_freeze.is_frozen = real


def test_plan_apply_revert():
    print("\nplan groups per (side, entity); apply routes by side; revert restores exactly")
    slists = {6: L(False, [1, 2]), 9: L(True, [555, 556], company=1)}
    dlists = {566: L(True, [6], company=9), 567: L(True, [6], company=9),
              555: L(True, [9, 10], company=9), 777: L(False, [1], company=9)}
    targets = [
        {"sid": 6, "sname": "S6", "did": 566, "dname": "D566", "requests_day": 5.0, "mode": "supply_block"},
        {"sid": 6, "sname": "S6", "did": 567, "dname": "D567", "requests_day": 1.0, "mode": "supply_block"},
        {"sid": 9, "sname": "S9", "did": 555, "dname": "D555", "requests_day": 3.0, "mode": "supply_allow"},
        {"sid": 9, "sname": "S9", "did": 777, "dname": "D777", "requests_day": 2.0, "mode": "demand_block"},
    ]
    writes = dp.plan_writes(targets, slists, dlists)
    by = {(w["side"], w["id"]): w for w in writes}
    check("three writes: supply 6, supply 9, demand 777", set(by) == {("supply", 6), ("supply", 9), ("demand", 777)}, str(by.keys()))
    check("blocklist gains both dids, keeps existing", by[("supply", 6)]["after"] == [1, 2, 566, 567])
    check("allowlist drops the did, keeps the rest", by[("supply", 9)]["after"] == [556])
    check("demand blocklist gains the sid", by[("demand", 777)]["after"] == [1, 9])
    seen = []
    rs, rdm = dp.tbm.set_supply_allowed_demand, dp.tbm.set_demand_allowed_supply
    dp.tbm.set_supply_allowed_demand = lambda i, ids, **kw: seen.append(("s", i, ids, kw)) or {"applied": not kw["dry_run"], "verify_ok": True}
    dp.tbm.set_demand_allowed_supply = lambda i, ids, **kw: seen.append(("d", i, ids, kw)) or {"applied": not kw["dry_run"], "verify_ok": True}
    try:
        entries, fails = dp.apply(writes, args_for(apply=False))
        check("all dry_run", all(s[3]["dry_run"] for s in seen))
        check("supply and demand writers both used", {s[0] for s in seen} == {"s", "d"})
        s6 = [s for s in seen if s[1] == 6][0]
        check("blocklist mode preserved on write", s6[3]["is_allowed"] is False and s6[2] == [1, 2, 566, 567])
        with tempfile.TemporaryDirectory() as tmp:
            p = os.path.join(tmp, "l.json")
            json.dump({"entries": [{**e, "applied": True} for e in entries]}, open(p, "w"))
            seen.clear()
            with redirect_stdout(io.StringIO()):
                rc = dp.revert(p, args_for(apply=True))
        check("revert ok", rc == 0)
        r6 = [s for s in seen if s[1] == 6][0]; r9 = [s for s in seen if s[1] == 9][0]
        r777 = [s for s in seen if s[0] == "d"][0]
        check("revert restores supply blocklist exactly", r6[2] == [1, 2] and r6[3]["is_allowed"] is False)
        check("revert restores supply allowlist exactly", r9[2] == [555, 556] and r9[3]["is_allowed"] is True)
        check("revert restores demand blocklist exactly", r777[2] == [1] and r777[3]["is_allowed"] is False)
    finally:
        dp.tbm.set_supply_allowed_demand, dp.tbm.set_demand_allowed_supply = rs, rdm


def test_helpers_and_gates():
    print("\nhelpers and gates")
    seen = {(1, 5): pair(1, 5, [1, 1, 1], [0, 2, 0]), (2, 5): pair(2, 5, [1, 1, 1], [3, 0, 0])}
    check("demand_responses sums across supplies", dp.demand_responses(seen)[5] == 5.0)
    real = dp.tbm.get_supply_source
    dp.tbm.get_supply_source = lambda sid: {"name": "x", "is_allowed_sources": True, "company_id": "7",
                                           "demand_sources": [{"id": 3}, 4, "5", "junk"], "companies": [8]}
    try:
        out = dp.supply_lists({1})[1]
        check("lists normalise ints/dicts/strings + company", out["ids"] == [3, 4, 5] and out["companies"] == [8] and out["company_id"] == 7, str(out))
    finally:
        dp.tbm.get_supply_source = real
    import re
    used = set(re.findall(r"args\.([a-z_]+)", open(dp.__file__).read()))
    ns = vars(dp.build_parser().parse_args([]))
    check("every args.X defined", not (used - set(ns)), str(sorted(used - set(ns))))
    check("defaults: 3 days, 10k req/day, cap 10", ns["days"] == 3 and ns["min_requests_day"] == 10000 and ns["max_pause"] == 10)


def main():
    print("=" * 70 + "\ntbx_dark_pairs — offline checks\n" + "=" * 70)
    test_choose_side(); test_select_rails(); test_plan_apply_revert(); test_helpers_and_gates()
    print("\n" + "=" * 70 + f"\n{PASS} passed, {FAIL} failed")
    return 1 if FAIL else 0

if __name__ == "__main__":
    sys.exit(main())
