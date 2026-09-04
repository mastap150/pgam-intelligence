#!/usr/bin/env python3
"""Offline checks for tbx_margin_autopilot — the decision rule and its rails."""
from __future__ import annotations
import io, os, sys, json, tempfile
from contextlib import redirect_stdout
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from scripts import tbx_margin_autopilot as ap   # noqa: E402

PASS = FAIL = 0
def check(l, ok, d=""):
    global PASS, FAIL
    if ok: PASS += 1; print(f"  ✓ {l}")
    else:  FAIL += 1; print(f"  ✗ {l}   {d}")

def cfg(**o):
    c = dict(ap.DEFAULTS); c["exclude_supply"] = set(); c["exclude_demand"] = set()
    c.update(o); return c

def band(t, lo, hi=None):
    return {"type": t, "min": lo, "max": lo if t == "fixed" else hi}

def row(sid, did, take, gross, s_b, d_b, one=True, peers=None):
    return {"sid": sid, "sname": f"S{sid}", "did": did, "dname": f"D{did}",
            "gross_day": gross, "take": take, "s_band": s_b, "d_band": d_b,
            "one_to_one": one, "peers": peers or []}

def args_for(**o):
    a = ap.build_parser().parse_args([])
    for k, v in o.items(): setattr(a, k, v)
    return a


def test_maths():
    print("\nmaths")
    check("compound 10 × 5 = 14.5", abs(ap.compound(10, 5) - 14.5) < 1e-9)
    check("floor for 30 given 5 = 26.3", abs(ap.floor_for_target(5, 30) - 26.315789) < 1e-3)
    check("adaptive effective floor is 0", ap.effective_floor(band("adaptive", 5, 95)) == 0.0)
    check("fixed effective floor is its value", ap.effective_floor(band("fixed", 10)) == 10.0)


def test_config():
    print("\nconfig")
    with tempfile.TemporaryDirectory() as tmp:
        p = os.path.join(tmp, "c.json")
        json.dump({"trigger_pct": 22, "exclude_supply": ["310"], "_comment": "x"}, open(p, "w"))
        c = ap.load_config(p)
        check("file overrides defaults", c["trigger_pct"] == 22 and c["target_pct"] == 30.0)
        check("exclusions coerced to int set", c["exclude_supply"] == {310})
        check("underscore keys ignored", "_comment" not in c)
        json.dump({"trigger_pct": 35}, open(p, "w"))
        try:
            ap.load_config(p); check("trigger >= target rejected", False)
        except ValueError:
            check("trigger >= target rejected", True)
    real = ap.DEFAULT_CONFIG
    check("shipped config loads and is enabled", ap.load_config(real)["enabled"] is True)


def test_one_to_one_demand_raise():
    print("\n1:1 connection → demand floor, compound, capped")
    r = row(194, 2050, 18.0, 100, band("range", 10, 30), band("range", 12, 35))
    todo, alerts, notes = ap.decide([r], cfg(max_raise_pp=15))
    check("one demand write", len(todo) == 1 and todo[0]["kind"] == "demand", str(todo))
    w = todo[0]
    # floor for 30 given supply 10 = 22.2
    check("floor lands compound on target", abs(w["margin_min"] - 22.2) < 0.11, str(w))
    check("projected ≈ 30", abs(w["projected"] - 30) < 0.5, str(w["projected"]))
    check("max kept (35 > 22.2)", w["margin_max"] == 35)
    r2 = row(1, 2, 10.0, 100, band("fixed", 2), band("range", 5, 40))
    todo, _, _ = ap.decide([r2], cfg(max_raise_pp=10))
    check("step capped at max_raise_pp (5 → 15)", todo and todo[0]["margin_min"] == 15.0, str(todo))
    check("step note present", "stepped" in todo[0]["note"])
    r3 = row(1, 2, 10.0, 100, band("fixed", 2), band("adaptive", 5, 95))
    todo, _, _ = ap.decide([r3], cfg())
    check("adaptive converted to range", todo and todo[0]["margin_type"] == "range", str(todo))
    r4 = row(1, 2, 10.0, 100, band("fixed", 2), band("fixed", 5))
    todo, _, _ = ap.decide([r4], cfg())
    check("fixed band sends min only", todo and todo[0]["margin_max"] is None)


def test_not_honoured_alerts():
    print("\nbelow the implied floor → hold, in-flight, or not-honoured alert; never a write")
    # configured floors imply 1-(.9)(.78)=29.8 but realised 12
    r = row(1, 2, 12.0, 100, band("range", 10, 30), band("range", 22, 35))
    todo, alerts, notes = ap.decide([r], cfg())
    check("no partial read → held one day, no write, no alert", not todo and not alerts and any("holding one day" in n for n in notes), str(notes))
    todo, alerts, notes = ap.decide([r], cfg(), partial={(1, 2): 29.5})
    check("partial ≥ trigger → in flight; held", not todo and not alerts and any("in flight" in n for n in notes), str(notes))
    # implied 1-(.9)(.88)=20.8; realised 12 is below; partial 18.5 is consistent with the floors
    r2 = row(1, 2, 12.0, 100, band("fixed", 10), band("range", 12, 35))
    todo, alerts, notes = ap.decide([r2], cfg(), partial={(1, 2): 18.5})
    check("partial consistent with floors → band changed since; held", not todo and not alerts and any("band changed" in n for n in notes), str(notes))
    todo, alerts, notes = ap.decide([r], cfg(), partial={(1, 2): 12.4})
    check("partial also below → not-honoured alert", not todo and alerts and "not applying" in alerts[0], str(alerts))
    ok = row(1, 3, 18.0, 100, band("range", 10, 30), band("range", 5, 35))   # implied 14.5, realised 18 → raise
    todo, alerts, notes = ap.decide([ok], cfg(), partial={(1, 3): 31.0})
    check("partial ≥ trigger → in flight, held", not todo and any("in flight" in n for n in notes), str(notes))
    todo, alerts, notes = ap.decide([ok], cfg(), partial={(1, 3): 17.0})
    check("partial still low → raise proceeds", len(todo) == 1)
    s = band("fixed", 10)
    legs = [row(65, 1758, 15.0, 200, s, band("range", 7, 35), one=False, peers=[65, 189]),
            row(65, 1932, 14.0, 300, s, band("range", 7, 35), one=False, peers=[65, 189])]
    real = ap.partner_freeze.is_frozen
    ap.partner_freeze.is_frozen = lambda **k: False
    try:
        todo, _, notes = ap.decide(legs, cfg(), partial={(65, 1758): 28.0, (65, 1932): 27.0})
        check("fan-out: partial weighted ≥ trigger → in flight, held", not todo and any("in flight" in n for n in notes), str(notes))
    finally:
        ap.partner_freeze.is_frozen = real


def test_fanout_supply_raise():
    print("\nfan-out → supply band, weighted, overshoot-trimmed, frozen-aware")
    s = band("fixed", 10)
    # supply fixed 10 × demand floor 7 implies 16.3%; realised 15/14 is within
    # the not-honoured gap, so these are genuine "floors too low" legs.
    legs = [row(65, 1758, 15.0, 200, s, band("range", 7, 35), one=False, peers=[65, 189]),
            row(65, 1932, 14.0, 300, s, band("range", 7, 35), one=False, peers=[65, 189])]
    real = ap.partner_freeze.is_frozen
    ap.partner_freeze.is_frozen = lambda **k: False
    try:
        todo, alerts, notes = ap.decide(legs, cfg())
        check("one supply write for the source", len(todo) == 1 and todo[0]["kind"] == "supply", str(todo))
        w = todo[0]
        # floor for 30 given d=7 → 24.7; step cap 10 → 20
        check("stepped to cur+10 = 20", w["margin_min"] == 20.0, str(w))
        check("covers both legs", sorted(w["legs"]) == [1758, 1932])
        # weighted take above trigger → held
        mixed = [row(65, 1758, 40.0, 900, s, band("range", 7, 35), one=False, peers=[65, 189]),
                 row(65, 1932, 15.0, 100, s, band("range", 7, 35), one=False, peers=[65, 189])]
        todo, alerts, notes = ap.decide(mixed, cfg())
        check("weighted take ≥ trigger → held with a note", not todo and any("supply-weighted" in n for n in notes), str(notes))
        # overshoot cap: one (healthy) leg has demand floor 30; the supply raise
        # for the cold leg is trimmed so that hot leg's projection stays ≤ cap
        hot = [row(65, 1, 38.0, 100, s, band("range", 30, 40), one=False, peers=[65, 189]),
               row(65, 2, 15.0, 400, s, band("range", 7, 35), one=False, peers=[65, 189])]
        todo, _, _ = ap.decide(hot, cfg(max_raise_pp=40, overshoot_cap_pct=40))
        lim = ap.floor_for_target(30, 40)   # 14.3
        check("overshoot cap trims the supply raise", todo and abs(todo[0]["margin_min"] - round(lim, 1)) < 0.11, str(todo))
        # adaptive supply converts
        ad = [row(23, 1, 12.0, 100, band("adaptive", 5, 95), band("range", 7, 35), one=False, peers=[23, 6])]
        todo, _, _ = ap.decide(ad, cfg())
        check("adaptive supply converted to range", todo and todo[0]["margin_type"] == "range", str(todo))
    finally:
        ap.partner_freeze.is_frozen = real
    ap.partner_freeze.is_frozen = lambda **k: "1932" in str(k.get("demand_id"))
    try:
        todo, alerts, notes = ap.decide(legs, cfg())
        check("frozen partner on a leg refuses the supply raise", not todo and any("frozen" in n for n in notes), str(notes))
    finally:
        ap.partner_freeze.is_frozen = real


def test_rails():
    print("\nrails: exclusions, max_writes, ordering by gross")
    rows = [row(i, 100 + i, 10.0, 1000 - i, band("fixed", 2), band("range", 5, 40)) for i in range(6)]
    todo, _, notes = ap.decide(rows, cfg(max_writes=4))
    check("max_writes caps the run", len(todo) == 4)
    check("biggest gross first", [w["did"] for w in todo] == [100, 101, 102, 103])
    todo, _, notes = ap.decide(rows, cfg(exclude_demand={100}, exclude_supply={1}))
    check("exclusions honoured", all(w["did"] not in (100, 101) for w in todo) and len([n for n in notes if "excluded" in n]) == 2, str(notes))
    todo, _, _ = ap.decide(rows, cfg(max_writes=0))
    check("max_writes 0 writes nothing", not todo)
    above = [row(1, 2, 26.0, 100, band("fixed", 2), band("range", 5, 40))]
    todo, alerts, notes = ap.decide(above, cfg())
    check("above trigger is left alone", not todo and not alerts and not notes)


def test_apply_and_revert():
    print("\napply routes by kind; revert restores type/min/max")
    seen = []
    rd, rs = ap.tbm.set_demand_economics, ap.tbm.set_supply_margin
    ap.tbm.set_demand_economics = lambda did, **kw: seen.append(("d", did, kw)) or {"applied": not kw["dry_run"], "verify_ok": True}
    ap.tbm.set_supply_margin = lambda sid, **kw: seen.append(("s", sid, kw)) or {"applied": not kw["dry_run"], "verify_ok": True}
    try:
        r1 = row(194, 2050, 18.0, 100, band("range", 10, 30), band("adaptive", 5, 95))
        r2 = row(65, 1758, 14.0, 200, band("fixed", 10), band("range", 7, 35), one=False, peers=[65, 189])
        real = ap.partner_freeze.is_frozen
        ap.partner_freeze.is_frozen = lambda **k: False
        try:
            todo, _, _ = ap.decide([r1, r2], cfg())
        finally:
            ap.partner_freeze.is_frozen = real
        entries, fails = ap.apply(todo, args_for(apply=False))
        kinds = sorted(s[0] for s in seen)
        check("one demand and one supply call", kinds == ["d", "s"], str(seen))
        check("all dry_run", all(s[2]["dry_run"] for s in seen))
        d = [s for s in seen if s[0] == "d"][0]
        check("demand call passes demand_name and range conversion", d[2]["demand_name"] == "D2050" and d[2]["margin_type"] == "range", str(d))
        s = [s for s in seen if s[0] == "s"][0]
        check("supply fixed band sends min only", "margin_max" not in s[2], str(s))
        with tempfile.TemporaryDirectory() as tmp:
            p = os.path.join(tmp, "l.json")
            json.dump({"entries": [{**e, "applied": True} for e in entries]}, open(p, "w"))
            seen.clear()
            with redirect_stdout(io.StringIO()):
                rc = ap.revert(p, args_for(apply=True))
        check("revert ok", rc == 0)
        d = [s for s in seen if s[0] == "d"][0]
        check("revert restores demand adaptive 5–95", (d[2]["margin_type"], d[2]["margin_min"], d[2]["margin_max"]) == ("adaptive", 5, 95), str(d))
        s = [s for s in seen if s[0] == "s"][0]
        check("revert restores supply fixed 10 (min only)", s[2]["margin_type"] == "fixed" and s[2]["margin_min"] == 10 and "margin_max" not in s[2], str(s))
    finally:
        ap.tbm.set_demand_economics, ap.tbm.set_supply_margin = rd, rs


def test_main_gates():
    print("\nmain gates")
    import re
    used = set(re.findall(r"args\.([a-z_]+)", open(ap.__file__).read()))
    ns = vars(ap.build_parser().parse_args([]))
    check("every args.X defined", not (used - set(ns)), str(sorted(used - set(ns))))
    real_conf = ap.tbx.configured
    ap.tbx.configured = lambda: True
    try:
        with tempfile.TemporaryDirectory() as tmp:
            p = os.path.join(tmp, "c.json")
            json.dump({"enabled": False}, open(p, "w"))
            with redirect_stdout(io.StringIO()):
                check("disabled config exits 0 without touching the platform", ap.main(["--config", p]) == 0)
    finally:
        ap.tbx.configured = real_conf
    txt = ap.summary(["2026-09-04"], [], [], [], ["x"], [], cfg(), args_for(apply=True))
    check("summary carries alerts", "Alerts" in txt and "APPLIED" in txt)


def main():
    print("=" * 70 + "\ntbx_margin_autopilot — offline checks\n" + "=" * 70)
    test_maths(); test_config(); test_one_to_one_demand_raise(); test_not_honoured_alerts()
    test_fanout_supply_raise(); test_rails(); test_apply_and_revert(); test_main_gates()
    print("\n" + "=" * 70 + f"\n{PASS} passed, {FAIL} failed")
    return 1 if FAIL else 0

if __name__ == "__main__":
    sys.exit(main())
