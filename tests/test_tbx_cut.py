#!/usr/bin/env python3
"""
Offline checks for scripts/tbx_cut.py.

This is the one script in the TBX set that can switch off a live partner, so
the tests are weighted towards what must NEVER happen: cutting an earner,
cutting something on the conversation-first list, or writing when a gate is
shut. No credentials, no platform call.
"""

from __future__ import annotations

import json
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, __file__.rsplit("/tests/", 1)[0])

from scripts import tbx_cut as c         # noqa: E402

PASS = FAIL = 0


def check(label: str, ok: bool, detail: str = "") -> None:
    global PASS, FAIL
    if ok:
        PASS += 1
        print(f"  ✓ {label}")
    else:
        FAIL += 1
        print(f"  ✗ {label}" + (f"  — {detail}" if detail else ""))


def args_for(**over):
    args = c.build_parser().parse_args([])
    args.exclude = dict(c.EXCLUDE_BY_DEFAULT)
    args.include = set()
    args.buckets = ["DEAD", "NEAR-DEAD", "NO-WIN"]
    for k, v in over.items():
        setattr(args, k, v)
    return args


def entry(name, eid, bucket, requests, gross=0.0, spend=0.0):
    return {"name": name, "id": eid, "bucket": bucket,
            "requests_day": requests, "gross_day": gross, "spend_day": spend}


def test_revenue_rail() -> None:
    print("\nthe revenue rail is an absolute refusal")
    rows = [
        entry("Dead One", 501, "DEAD", 1_000_000, gross=0.0),
        entry("Earner", 502, "DEAD", 1_000_000, gross=25.0),
        entry("Just Under", 503, "DEAD", 1_000_000, gross=0.99),
        entry("Just Over", 504, "DEAD", 1_000_000, gross=1.01),
    ]
    targets, skipped = c.select(rows, "supply_source", args_for())
    ids = {t["id"] for t in targets}
    check("a zero-revenue source is selected", 501 in ids, str(ids))
    check("a $25/day earner is never cut", 502 not in ids, str(ids))
    check("just under the rail passes", 503 in ids, str(ids))
    check("just over the rail is refused", 504 not in ids, str(ids))
    reasons = {e["id"]: why for e, why in skipped}
    check("the refusal says why", "above the" in reasons.get(502, ""),
          reasons.get(502, ""))

    # The rail must beat an explicit --include: a mistyped id cannot take out
    # a live earner just because someone named it.
    targets, _ = c.select(rows, "supply_source", args_for(include={502}))
    check("--include cannot override the revenue rail",
          not any(t["id"] == 502 for t in targets))


def test_default_excludes() -> None:
    print("\nthe conversation-first list")
    rows = [
        entry("Illumin Endpoint3 - RON", 1549, "NO-WIN", 233_000_000, spend=0.0),
        entry("Dexerto Display", 6, "NEAR-DEAD", 1_106_000_000, gross=0.5),
        entry("Ordinary Dead", 777, "DEAD", 50_000, gross=0.0),
    ]
    targets, skipped = c.select(rows, "demand_source", args_for())
    ids = {t["id"] for t in targets}
    check("an Illumin RON endpoint is skipped by default", 1549 not in ids, str(ids))
    check("Dexerto Display is skipped by default", 6 not in ids, str(ids))
    check("an ordinary dead source is still selected", 777 in ids, str(ids))
    reasons = {e["id"]: why for e, why in skipped}
    check("the skip names the conversation, not just 'excluded'",
          "Illumin" in reasons.get(1549, ""), reasons.get(1549, ""))

    # --include must not smuggle an excluded entity through: exclusion is
    # about a decision that has not been made, not about ranking.
    targets, _ = c.select(rows, "demand_source", args_for(include={1549}))
    check("--include does not override an exclusion",
          not any(t["id"] == 1549 for t in targets))

    cleared = args_for(exclude={})
    targets, _ = c.select(rows, "demand_source", cleared)
    check("--clear-default-excludes does let them through",
          {1549, 6, 777} <= {t["id"] for t in targets})


def test_bucket_filter() -> None:
    print("\nbucket selection")
    rows = [
        entry("Dead", 601, "DEAD", 100_000, gross=0.0),
        entry("Hungry", 602, "HUNGRY", 900_000_000, gross=0.5),
        entry("Unflagged", 603, None, 100_000, gross=0.0),
    ]
    targets, _ = c.select(rows, "supply_source", args_for(buckets=["DEAD"]))
    ids = {t["id"] for t in targets}
    check("only the named bucket is selected", ids == {601}, str(ids))
    check("an unflagged source is never selected", 603 not in ids)

    targets, _ = c.select(rows, "supply_source", args_for(buckets=["HUNGRY"]))
    check("HUNGRY is selectable but only when asked for",
          {t["id"] for t in targets} == {602})


def test_missing_id() -> None:
    print("\nan unparseable name")
    rows = [{"name": "No Id Here", "id": None, "bucket": "DEAD",
             "requests_day": 500_000, "gross_day": 0.0}]
    targets, skipped = c.select(rows, "supply_source", args_for())
    check("an entity with no id is never written to", not targets)
    check("and is reported rather than dropped silently", len(skipped) == 1)


def test_no_default_buckets() -> None:
    print("\nbuckets must be chosen")
    args = c.build_parser().parse_args([])
    check("--buckets has no default", args.buckets == "")
    check("--apply defaults off", args.apply is False)
    check("--max-revenue-day defaults to a dollar", args.max_revenue_day == 1.0)


def test_parse_ids() -> None:
    print("\nid parsing")
    check("comma separated", c.parse_ids("1,2,3") == {1, 2, 3})
    check("space separated", c.parse_ids("1 2 3") == {1, 2, 3})
    check("mixed and padded", c.parse_ids(" 1, 2  3 ") == {1, 2, 3})
    check("empty is empty", c.parse_ids("") == set())
    check("None is empty", c.parse_ids(None) == set())


def test_revert_roundtrip() -> None:
    print("\nrevert reads a ledger and re-enables exactly it")
    calls = []

    def fake_supply(eid, active, **kw):
        calls.append(("supply", eid, active))
        return {"entity_id": eid, "applied": True}

    def fake_demand(eid, active, **kw):
        calls.append(("demand", eid, active))
        return {"entity_id": eid, "applied": True}

    ledger = {"entries": [
        {"kind": "supply_source", "id": 11, "name": "A", "applied": True},
        {"kind": "demand_source", "id": 22, "name": "B", "applied": True},
        # A dry-run row must not be re-enabled — it was never disabled.
        {"kind": "supply_source", "id": 33, "name": "C", "applied": False},
    ]}
    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / "led.json"
        path.write_text(json.dumps(ledger))
        real_s, real_d = c.tbm.set_supply_source_status, c.tbm.set_demand_source_status
        c.tbm.set_supply_source_status = fake_supply
        c.tbm.set_demand_source_status = fake_demand
        try:
            rc = c.revert(str(path), args_for(apply=True))
        finally:
            c.tbm.set_supply_source_status = real_s
            c.tbm.set_demand_source_status = real_d

    check("revert succeeds", rc == 0, str(rc))
    check("both applied entries are re-enabled",
          ("supply", 11, True) in calls and ("demand", 22, True) in calls, str(calls))
    check("a row that was only a dry run is NOT re-enabled",
          not any(cl[1] == 33 for cl in calls), str(calls))
    check("revert only ever enables, never disables",
          all(cl[2] is True for cl in calls), str(calls))


def test_parser_surface() -> None:
    print("\nargument surface")
    import re
    args = c.build_parser().parse_args([])
    args.exclude, args.include = {}, set()
    src = Path(c.__file__).read_text()
    used = set(re.findall(r"\bargs\.([a-z_]+)", src))
    missing = sorted(u for u in used if not hasattr(args, u))
    check("every args.X the module reads is defined by the parser",
          not missing, f"missing: {missing}")


def main() -> int:
    print("=" * 70)
    print("tbx_cut — offline checks")
    print("=" * 70)
    test_revenue_rail()
    test_default_excludes()
    test_bucket_filter()
    test_missing_id()
    test_no_default_buckets()
    test_parse_ids()
    test_revert_roundtrip()
    test_parser_surface()
    print("\n" + "=" * 70)
    print(f"{PASS} passed, {FAIL} failed")
    return 1 if FAIL else 0


if __name__ == "__main__":
    sys.exit(main())
