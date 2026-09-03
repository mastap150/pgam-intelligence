#!/usr/bin/env python3
"""Offline checks for tbx_dynamic_margin."""
from __future__ import annotations
import io, os, sys, json, tempfile
from contextlib import redirect_stdout
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from scripts import tbx_dynamic_margin as dm   # noqa: E402

PASS = FAIL = 0
def check(l, ok, d=""):
    global PASS, FAIL
    if ok: PASS += 1; print(f"  ✓ {l}")
    else:  FAIL += 1; print(f"  ✗ {l}   {d}")

def args_for(**o):
    a = dm.build_parser().parse_args([]); a.include = set()
    for k, v in o.items(): setattr(a, k, v)
    return a

def ent(sid, typ, dyn=False, pct=0.0, smart=True, has=True):
    src = {"type": typ, "is_smart_floor": smart}
    if has: src.update({"is_dynamic_margin": dyn, "dynamic_margin": str(pct)})
    return {"id": sid, "name": f"S{sid}", "margin_type": "range",
            "margin_min": 5, "margin_max": 30, "source": src}

def test_shape_and_type():
    print("\nshape parsing and the indirect test")
    s = dm.shape(ent(196, "indirect_suppliers"))
    check("dynamic fields detected", s["has_dynamic_fields"] is True)
    check("dynamic_margin coerces from string", s["dynamic_margin"] == 0.0)
    check("indirect by field presence", dm.is_indirect(s))
    d = dm.shape(ent(1503, "direct_inventory", has=False))
    check("direct inventory has no dynamic fields", d["has_dynamic_fields"] is False)
    check("and is not indirect", not dm.is_indirect(d))

def test_plan_rails():
    print("\nplan refuses direct inventory and already-set; caps at one")
    rows = [dm.shape(ent(196, "indirect_suppliers")),
            dm.shape(ent(1503, "direct_inventory", has=False)),
            dm.shape(ent(65, "indirect_suppliers", dyn=True, pct=30.0)),
            dm.shape(ent(264, "indirect_suppliers"))]
    todo, refused = dm.plan(rows, args_for(include={196, 1503, 65, 264}, set=30.0))
    check("only one written (max-apply 1)", len(todo) == 1, str([t["id"] for t in todo]))
    check("direct inventory refused", any("1503" in r and "indirect" in r for r in refused), str(refused))
    check("already at 30 refused", any("65" in r and "already" in r for r in refused), str(refused))
    todo2, _ = dm.plan(rows, args_for(include={196, 264}, set=30.0, max_apply=5))
    check("cap lifts when asked", len(todo2) == 2)

def test_apply_and_revert():
    print("\napply passes both fields; revert restores the exact prior pair")
    seen = []
    real = dm.tbm.set_supply_source_fields
    dm.tbm.set_supply_source_fields = lambda sid, **kw: seen.append((sid, kw)) or {"applied": not kw["dry_run"], "verify_ok": True}
    try:
        s = dm.shape(ent(196, "indirect_suppliers"))
        entries, fails = dm.apply([s], args_for(apply=False, set=30.0))
        check("dry_run=True", seen[0][1]["dry_run"] is True)
        check("is_dynamic_margin=True sent", seen[0][1]["is_dynamic_margin"] is True)
        check("dynamic_margin=30 sent", seen[0][1]["dynamic_margin"] == 30.0)
        check("before recorded as off/0", entries[0]["before"] == {"is_dynamic_margin": False, "dynamic_margin": 0.0}, str(entries))
        with tempfile.TemporaryDirectory() as tmp:
            p = os.path.join(tmp, "l.json")
            json.dump({"entries": [{**entries[0], "applied": True}]}, open(p, "w"))
            seen.clear()
            with redirect_stdout(io.StringIO()):
                rc = dm.revert(p, args_for(apply=True))
        check("revert ok", rc == 0)
        check("revert turns it back off at 0", seen[0][1]["is_dynamic_margin"] is False and seen[0][1]["dynamic_margin"] == 0.0, str(seen))
    finally:
        dm.tbm.set_supply_source_fields = real

def test_main_gates():
    print("\nmain gates")
    real_conf = dm.tbx.configured
    dm.tbx.configured = lambda: True
    try:
        with redirect_stdout(io.StringIO()):
            check("--apply without --include exits 1", dm.main(["--apply"]) == 1)
            check("nothing to read exits 2", dm.main([]) == 2)
    finally:
        dm.tbx.configured = real_conf
    import re
    used = set(re.findall(r"args\.([a-z_]+)", open(dm.__file__).read()))
    ns = vars(dm.build_parser().parse_args([]))
    check("every args.X defined", not (used - set(ns)), str(sorted(used - set(ns))))
    check("max-apply defaults to 1", ns["max_apply"] == 1)

def main():
    print("=" * 70 + "\ntbx_dynamic_margin — offline checks\n" + "=" * 70)
    test_shape_and_type(); test_plan_rails(); test_apply_and_revert(); test_main_gates()
    print("\n" + "=" * 70 + f"\n{PASS} passed, {FAIL} failed")
    return 1 if FAIL else 0

if __name__ == "__main__":
    sys.exit(main())
