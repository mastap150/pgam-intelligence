#!/usr/bin/env python3
"""
Offline checks for scripts/tbx_type_probe.py.

The probe exists to explain a 422, so its one job is to not mis-attribute
blame: a field that genuinely matches its type must never be listed, and a
null must be distinguishable from a wrong-typed value, because those want
different fixes.

No credentials, no platform call.
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, __file__.rsplit("/tests/", 1)[0])

from scripts import tbx_type_probe as tp    # noqa: E402

PASS = FAIL = 0


def check(label: str, ok: bool, detail: str = "") -> None:
    global PASS, FAIL
    if ok:
        PASS += 1
        print(f"  ✓ {label}")
    else:
        FAIL += 1
        print(f"  ✗ {label}" + (f"  — {detail}" if detail else ""))


TYPES = {
    "vcr_optimization": "number",
    "target_srcpm": "string",
    "is_vcr_optimization": "boolean",
    "seat": "array",
    "geo_settings": "object",
    "id": "integer",
}


def test_the_two_observed_failures() -> None:
    """The exact shapes the live 422s named."""
    print("\nthe fields the platform actually rejected")
    got = tp.mismatches({"vcr_optimization": "", "target_srcpm": None}, TYPES)
    by = {f: (want, kind) for f, want, kind, _ in got}
    check("a string in a number field is flagged",
          by.get("vcr_optimization") == ("number", "str"), str(by))
    check("a null in a string field is flagged",
          by.get("target_srcpm") == ("string", "null"), str(by))
    check("null is reported as null, not as its Python type",
          all(k != "NoneType" for _, _, k, _ in got), str(got))


def test_correct_values_are_never_flagged() -> None:
    print("\na conforming entity produces no findings")
    clean = {"vcr_optimization": 2.34, "target_srcpm": "target",
             "is_vcr_optimization": True, "seat": [], "geo_settings": {},
             "id": 501}
    check("nothing is flagged", tp.mismatches(clean, TYPES) == [],
          str(tp.mismatches(clean, TYPES)))
    check("an int satisfies a number field",
          tp.mismatches({"vcr_optimization": 2}, TYPES) == [])


def test_bool_in_a_number_field() -> None:
    """True is an int in Python; the probe must not let that pass silently."""
    print("\na boolean sitting in a numeric field")
    got = tp.mismatches({"vcr_optimization": True}, TYPES)
    check("it is flagged, not swallowed by isinstance(bool, int)",
          [(f, k) for f, _, k, _ in got] == [("vcr_optimization", "boolean")],
          str(got))


def test_absent_fields_are_not_findings() -> None:
    print("\nabsence is not a mismatch")
    check("a field the entity omits is skipped",
          tp.mismatches({}, TYPES) == [], str(tp.mismatches({}, TYPES)))
    check("a field the spec does not declare is ignored",
          tp.mismatches({"mystery": object()}, TYPES) == [])


def test_example_value_is_carried() -> None:
    print("\nthe offending value is reported, not just its type")
    got = tp.mismatches({"vcr_optimization": "abc"}, TYPES)
    check("the value comes back for the report", got[0][3] == "abc", str(got))


def test_never_imports_a_write() -> None:
    print("\nread-only by construction")
    src = Path(tp.__file__).read_text()
    for bad in ("set_demand_", "set_supply_", "_apply_update", "--apply"):
        check(f"no {bad}", bad not in src.split('"""', 2)[2],
              "found in code body")


def test_parser() -> None:
    print("\nargument surface")
    import re
    args = tp.build_parser().parse_args([])
    src = Path(tp.__file__).read_text()
    used = set(re.findall(r"\bargs\.([a-z_]+)", src))
    missing = sorted(u for u in used if not hasattr(args, u))
    check("every args.X is defined by the parser", not missing, f"missing: {missing}")
    check("demand is the default side", args.side == "demand")


def main() -> int:
    print("=" * 70)
    print("tbx_type_probe — offline checks")
    print("=" * 70)
    test_the_two_observed_failures()
    test_correct_values_are_never_flagged()
    test_bool_in_a_number_field()
    test_absent_fields_are_not_findings()
    test_example_value_is_carried()
    test_never_imports_a_write()
    test_parser()
    print("\n" + "=" * 70)
    print(f"{PASS} passed, {FAIL} failed")
    return 1 if FAIL else 0


if __name__ == "__main__":
    sys.exit(main())
