#!/usr/bin/env python3
"""
Which live field values do not match the type their own spec declares?

Why this exists
---------------
On 2026-08-31 the first live geo-blacklist apply attempted 50 demand-source
writes and the platform refused all 50:

    HTTP 422 The Advanced Setting -> VCR Optimization value must be a number.
             (and 2 more errors)
    HTTP 422 The Advanced Setting -> Target sRCPM must be a string.
             (and 4 more errors)

`core/tbx_mgmt.py` writes by read-modify-write: GET the whole entity, deep-
merge a sparse change, POST the whole thing back. That is only sound if an
entity is round-trippable through its own update endpoint — and these are not.
The vendored spec is no help: `DemandSourceRequest` and `DemandSourceResource`
declare **identical** types for every field, including `vcr_optimization`
(number) and `target_srcpm` (string enum). So the mismatch is between the
live data and the spec both halves share, not between the two halves.

That blocks every demand writer, not just the geo one — `set_demand_economics`,
`set_demand_geo_bid_floors`, `set_demand_source_status` all go through the same
path.

Why a probe rather than a fix
-----------------------------
The obvious repair is to coerce each offending field before POSTing. But the
error text is truncated ("and 2 more errors"), so the full set of offenders is
unknown, and the right coercion is not guessable: a numeric field arriving as
`null` might want `0`, might want the key dropped so the platform re-applies
its default, and those are different trading outcomes on a live buyer. Dropping
`vcr_optimization` could switch VCR optimisation off.

So: measure first. This prints every field whose live value contradicts its
spec-declared type, with the actual value, so the coercion is chosen from
evidence.

Read-only. It never writes and imports no write path.

Exit codes:
    0  every sampled entity round-trips cleanly
    1  at least one type mismatch found
    2  credentials absent
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from collections import Counter, defaultdict

sys.path.insert(0, __file__.rsplit("/scripts/", 1)[0])

from core import tbx_api as tbx          # noqa: E402
from core import tbx_mgmt as tbm         # noqa: E402

_HDR = "=" * 78

# JSON type name -> the Python types the platform should accept for it.
# bool is excluded from "number" deliberately: in Python `True` is an int, and
# a boolean sitting in a numeric field is exactly the kind of thing worth
# seeing rather than silently passing.
_OK: dict[str, tuple] = {
    "string": (str,),
    "number": (int, float),
    "integer": (int,),
    "boolean": (bool,),
    "array": (list,),
    "object": (dict,),
}


def declared_types(schema_name: str) -> dict[str, str]:
    """{field: json type} from one schema in the vendored spec."""
    try:
        with open(tbm._SPEC_PATH) as handle:
            spec = json.load(handle)
    except (OSError, ValueError) as exc:
        print(f"could not read the vendored spec: {exc}", file=sys.stderr)
        return {}
    props = (spec.get("components", {}).get("schemas", {})
             .get(schema_name, {}).get("properties") or {})
    return {name: p["type"] for name, p in props.items()
            if isinstance(p, dict) and isinstance(p.get("type"), str)}


def mismatches(entity: dict, types: dict[str, str]) -> list[tuple[str, str, str, object]]:
    """Fields whose live value contradicts the declared type.

    A null is reported separately from a wrong-typed value: the platform may
    well accept null for an optional field, and conflating the two would
    inflate the list with fields that are not actually the problem.
    """
    out = []
    for field, want in types.items():
        if field not in entity:
            continue
        value = entity[field]
        if value is None:
            out.append((field, want, "null", value))
            continue
        allowed = _OK.get(want)
        if allowed is None:
            continue
        if want == "number" and isinstance(value, bool):
            out.append((field, want, "boolean", value))
            continue
        if not isinstance(value, allowed):
            out.append((field, want, type(value).__name__, value))
    return out


def probe(kind: str, ids: list[int], limit: int) -> int:
    schema = tbm._READ_SCHEMA[kind]
    types = declared_types(schema)
    if not types:
        print(f"no declared types for {schema} — cannot compare.", file=sys.stderr)
        return 2

    getter = (tbm.get_demand_source if kind == "demand_source"
              else tbm.get_supply_source)
    print(f"\n{_HDR}\n{kind}: live values vs {schema}\n{_HDR}")
    print(f"  {len(types)} fields carry a declared scalar type")

    by_field: dict[str, Counter] = defaultdict(Counter)
    samples: dict[str, object] = {}
    seen = 0

    for eid in ids[:limit]:
        try:
            entity = getter(eid)
        except Exception as exc:                       # noqa: BLE001
            print(f"  ✗ {eid}: {exc}", file=sys.stderr)
            continue
        if not isinstance(entity, dict):
            continue
        seen += 1
        bad = mismatches(entity, types)
        print(f"  {eid}: {len(bad)} mismatch(es)", flush=True)
        for field, want, got, value in bad:
            by_field[field][f"declared {want}, got {got}"] += 1
            samples.setdefault(f"{field}|{got}", value)

    if not seen:
        print("  nothing readable — not reporting on an empty sample.",
              file=sys.stderr)
        return 2

    print(f"\n  across {seen} {kind}(s):")
    if not by_field:
        print("    every field matched its declared type")
        return 0

    print(f"\n  {'field':<32} {'n':>4}  problem / example value")
    print(f"  {'-'*32} {'----':>4}  {'-'*36}")
    for field in sorted(by_field, key=lambda f: -sum(by_field[f].values())):
        for problem, count in by_field[field].most_common():
            got = problem.rsplit("got ", 1)[-1]
            example = samples.get(f"{field}|{got}", "?")
            print(f"  {field:<32} {count:>4}  {problem} — {example!r}")

    print(f"\n  These are the fields a read-modify-write POSTs back unchanged, "
          f"so any\n  one of them can 422 an update that has nothing to do "
          f"with it.")
    print(f"\n  Nothing was changed. This probe never writes.")
    return 1


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Live field values that contradict their declared type.")
    p.add_argument("--side", choices=("demand", "supply", "both"),
                   default="demand")
    p.add_argument("--ids", default="",
                   help="specific ids to sample, comma-separated. Default: "
                        "the first --limit from the dictionary.")
    p.add_argument("--limit", type=int, default=8,
                   help="how many entities to sample per side (default 8)")
    return p


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if not tbx.configured():
        print("TBX_EMAIL / TBX_PASSWORD are not set — nothing to read.",
              file=sys.stderr)
        return 2

    explicit = [int(x) for x in args.ids.replace(",", " ").split()] if args.ids else []
    rc = 0
    for kind, dict_name in (("demand_source", "demand-sources"),
                            ("supply_source", "supply-sources")):
        if args.side != "both" and not kind.startswith(args.side):
            continue
        ids = explicit or [row["id"] for row in tbx.dictionary(dict_name)
                           if row.get("id") is not None]
        rc = max(rc, probe(kind, ids, args.limit))
    return rc


if __name__ == "__main__":
    try:
        sys.exit(main())
    except tbx.TbxError as exc:
        print(f"\nplatform unreachable: {exc}", file=sys.stderr)
        sys.exit(3)
