#!/usr/bin/env python3
"""
Offline checks for scripts/tbx_geo_cut.py and the blacklist merge it rests on.

This script edits a live buyer's trading rules, so the tests are weighted at
what must NEVER happen: blocking a country the buyer actually trades in,
blocking a country the report could not name, and — the one that would be
invisible until a partner complained — silently dropping a blacklist somebody
set by hand.

No credentials, no platform call.
"""

from __future__ import annotations

import json
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, __file__.rsplit("/tests/", 1)[0])

from core import tbx_mgmt as tbm         # noqa: E402
from scripts import tbx_geo_cut as gc    # noqa: E402

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
    args = gc.build_parser().parse_args([])
    args.exclude = dict(gc.EXCLUDE_BY_DEFAULT)
    args.include = set()
    for key, value in over.items():
        setattr(args, key, value)
    return args


def pair(did, country, requests, imps=0.0, spend=0.0, name=None):
    """A row shaped like geo.summarise output."""
    return {
        "id": did,
        "country": country,
        "name": name or f"Buyer {did} #{did} — {country}",
        "requests_day": requests,
        "imps_day": imps,
        "spend_day": spend,
        "requests": requests, "imps": imps, "spend": spend,
        "per_dollar": None,
    }


# ---------------------------------------------------------------------------
# The merge. This is the half that can destroy state nobody asked it to touch.
# ---------------------------------------------------------------------------

def with_fake_platform(current: dict, fn):
    """Run `fn` with tbx_mgmt's read and write both stubbed."""
    sent = {}

    def fake_get(_id):
        return current

    def fake_apply(kind, entity_id, changes, **kw):
        sent["kind"] = kind
        sent["id"] = entity_id
        sent["changes"] = changes
        sent["reason"] = kw.get("reason")
        return {"applied": True, "verify_ok": True}

    real_get, real_apply = tbm.get_demand_source, tbm._apply_update
    tbm.get_demand_source = fake_get
    tbm._apply_update = fake_apply
    try:
        return fn(), sent
    finally:
        tbm.get_demand_source = real_get
        tbm._apply_update = real_apply


def test_blacklist_merges() -> None:
    print("\nset_demand_geo_blacklist merges instead of clobbering")
    current = {"geo_settings": {"blacklist": [{"country_id": 7, "value": 0},
                                              {"country_id": 9, "value": 0}]}}

    result, sent = with_fake_platform(
        current, lambda: tbm.set_demand_geo_blacklist(501, [9, 42], dry_run=True))
    got = [row["country_id"] for row in sent["changes"]["geo_settings"]["blacklist"]]

    check("a country a human blocked earlier survives the write",
          7 in got, str(got))
    check("the new country is added", 42 in got, str(got))
    check("an already-blocked country is not duplicated",
          got.count(9) == 1, str(got))
    check("the union is exactly what is sent", got == [7, 9, 42], str(got))
    check("only the country genuinely new is reported as added",
          result["added"] == [42], str(result["added"]))
    check("a merge never reports a removal", result["removed"] == [],
          str(result["removed"]))
    check("the prior list is returned so a ledger can record it",
          result["blacklist_before"] == [7, 9], str(result["blacklist_before"]))


def test_blacklist_replace_is_opt_in() -> None:
    print("\nreplace=True is the only way to drop an entry")
    current = {"geo_settings": {"blacklist": [{"country_id": 7, "value": 0},
                                              {"country_id": 9, "value": 0}]}}
    result, sent = with_fake_platform(
        current,
        lambda: tbm.set_demand_geo_blacklist(501, [9], replace=True, dry_run=True))
    got = [row["country_id"] for row in sent["changes"]["geo_settings"]["blacklist"]]
    check("replace sends exactly what was asked for", got == [9], str(got))
    check("the dropped country is reported", result["removed"] == [7],
          str(result["removed"]))


def test_blacklist_handles_empty_current() -> None:
    print("\na buyer with no blacklist at all")
    for current in ({}, {"geo_settings": None}, {"geo_settings": {}},
                    {"geo_settings": {"blacklist": None}}):
        result, sent = with_fake_platform(
            current, lambda: tbm.set_demand_geo_blacklist(501, [3], dry_run=True))
        got = [r["country_id"] for r in sent["changes"]["geo_settings"]["blacklist"]]
        check(f"{current} → writes just the new list", got == [3], str(got))


# ---------------------------------------------------------------------------
# The rails
# ---------------------------------------------------------------------------

def test_trading_pair_is_never_blocked() -> None:
    print("\na pair that trades is never blacklisted")
    rows = [
        pair(501, "BR", 5_000_000),                       # dead: block
        pair(501, "US", 5_000_000, imps=70_000, spend=90.0),   # earns: never
        pair(501, "IN", 5_000_000, imps=40, spend=0.02),  # trades a little
        pair(501, "MX", 5_000_000, spend=0.50),           # spends, no imps
    ]
    targets, skipped = gc.select(rows, args_for())
    blocked = set(targets.get(501, {}).get("countries", []))
    check("the zero-everything country is blocked", "BR" in blocked, str(blocked))
    check("the earning country is NEVER blocked", "US" not in blocked, str(blocked))
    check("a country with impressions is not blocked — that wants a floor",
          "IN" not in blocked, str(blocked))
    check("spend above the rail is not blocked", "MX" not in blocked, str(blocked))
    reasons = " ".join(why for _, why in skipped)
    check("the skip reasons say why", "floor" in reasons and "rail" in reasons,
          reasons)


def test_spend_rail_beats_include() -> None:
    print("\nthe spend rail beats an explicit --include")
    rows = [pair(501, "US", 5_000_000, imps=70_000, spend=90.0)]
    targets, _ = gc.select(rows, args_for(include={501}))
    check("naming the buyer does not unlock an earning country",
          501 not in targets, str(targets))


def test_dead_buyer_is_a_source_decision() -> None:
    print("\na buyer that spends nowhere is not a geo problem")
    rows = [pair(777, "BR", 5_000_000), pair(777, "IN", 5_000_000)]
    targets, skipped = gc.select(rows, args_for())
    check("no country is blacklisted", 777 not in targets, str(targets))
    check("the skip points at tbx_cut instead",
          any("NO-WIN" in why for _, why in skipped),
          str([w for _, w in skipped]))

    # ...but the same buyer earning anywhere makes its dead geos actionable.
    rows.append(pair(777, "US", 5_000_000, imps=70_000, spend=90.0))
    targets, _ = gc.select(rows, args_for())
    check("once it earns somewhere, its dead geos are blockable",
          sorted(targets[777]["countries"]) == ["BR", "IN"],
          str(targets.get(777)))


def test_unattributed_country_is_refused() -> None:
    print("\nan unnamed country is never blacklisted")
    rows = [pair(501, "(none)", 9_000_000), pair(501, "US", 1, imps=1, spend=90.0),
            pair(501, "BR", 5_000_000)]
    targets, skipped = gc.select(rows, args_for())
    check("'(none)' is not in the blacklist",
          "(none)" not in targets.get(501, {}).get("countries", []),
          str(targets.get(501)))
    check("it is reported rather than dropped silently",
          any("unattributed" in why for _, why in skipped),
          str([w for _, w in skipped]))


def test_volume_bar_and_grouping() -> None:
    print("\nvolume bar, and one call per buyer")
    rows = [
        pair(501, "BR", 5_000_000), pair(501, "RU", 3_000_000),
        pair(501, "FJ", 900),                                   # below the bar
        pair(501, "US", 1, imps=1, spend=90.0),                 # keeps 501 alive
        pair(502, "BR", 4_000_000), pair(502, "US", 1, imps=1, spend=90.0),
    ]
    targets, _ = gc.select(rows, args_for())
    check("a trivial-volume pair is left alone",
          "FJ" not in targets[501]["countries"], str(targets[501]["countries"]))
    check("countries group under one buyer",
          targets[501]["countries"] == ["BR", "RU"], str(targets[501]["countries"]))
    check("two buyers stay separate", sorted(targets) == [501, 502], str(sorted(targets)))
    check("the buyer name loses the country suffix",
          " — " not in targets[501]["name"], targets[501]["name"])
    check("and its trailing id, so the display does not print it twice",
          targets[501]["name"] == "Buyer 501", targets[501]["name"])
    check("volumes add up per buyer",
          targets[501]["requests_day"] == 8_000_000,
          str(targets[501]["requests_day"]))


def test_exclusions() -> None:
    print("\nexclusions")
    rows = [pair(501, "BR", 5_000_000), pair(501, "US", 1, imps=1, spend=90.0)]
    targets, skipped = gc.select(rows, args_for(exclude={501: "ask the AM first"}))
    check("an excluded buyer is skipped", 501 not in targets, str(targets))
    check("the reason is carried through",
          any("AM" in why for _, why in skipped), str([w for _, w in skipped]))


# ---------------------------------------------------------------------------
# The write
# ---------------------------------------------------------------------------

def run_apply(targets, args, resolver, writer):
    real_resolve, real_write = gc.tbx.country_ids, gc.tbm.set_demand_geo_blacklist
    gc.tbx.country_ids = resolver
    gc.tbm.set_demand_geo_blacklist = writer
    try:
        return gc.apply_blacklists(targets, args)
    finally:
        gc.tbx.country_ids = real_resolve
        gc.tbm.set_demand_geo_blacklist = real_write


def test_dry_run_never_writes() -> None:
    print("\na dry run passes dry_run=True on every call")
    calls = []

    def writer(did, cids, **kw):
        calls.append(kw.get("dry_run"))
        return {"applied": False, "added": cids, "blacklist_before": []}

    rows = [pair(501, "BR", 5_000_000), pair(501, "US", 1, imps=1, spend=90.0)]
    targets, _ = gc.select(rows, args_for())
    entries, failures = run_apply(targets, args_for(apply=False),
                                  lambda c: [1] * len(c), writer)
    check("dry_run=True was passed", calls == [True], str(calls))
    check("no failure is recorded for a dry run", failures == 0, str(failures))
    check("the ledger entry is marked unapplied",
          entries and entries[0]["applied"] is False, str(entries))


def test_partial_country_resolution_refuses_the_buyer() -> None:
    print("\nan unresolvable country name aborts that buyer")
    calls = []

    def writer(did, cids, **kw):
        calls.append(did)
        return {"applied": True, "added": cids, "blacklist_before": []}

    rows = [pair(501, "BR", 5_000_000), pair(501, "ZZ", 5_000_000),
            pair(501, "US", 1, imps=1, spend=90.0)]
    targets, _ = gc.select(rows, args_for())
    # Resolver drops "ZZ", mimicking tbx_api.country_ids' behaviour.
    entries, failures = run_apply(
        targets, args_for(apply=True),
        lambda codes: [1 for c in codes if c != "ZZ"], writer)
    check("nothing was written for the buyer", calls == [], str(calls))
    check("it counts as a failure", failures == 1, str(failures))
    check("no ledger entry claims a write", entries == [], str(entries))


def test_ledger_round_trip() -> None:
    print("\nthe ledger reverts to the exact prior list")
    def writer(did, cids, **kw):
        return {"applied": True, "added": cids,
                "blacklist_before": [7], "blacklist_after": [7] + list(cids)}

    rows = [pair(501, "BR", 5_000_000), pair(501, "US", 1, imps=1, spend=90.0)]
    targets, _ = gc.select(rows, args_for())
    entries, _ = run_apply(targets, args_for(apply=True), lambda c: [42], writer)
    check("the prior list is in the ledger",
          entries[0]["blacklist_before"] == [7], str(entries[0]))

    seen = {}

    def revert_writer(did, cids, **kw):
        seen["ids"] = list(cids)
        seen["replace"] = kw.get("replace")
        return {"applied": True}

    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / "geo-ledger.json"
        path.write_text(json.dumps({"entries": entries}))
        real = gc.tbm.set_demand_geo_blacklist
        gc.tbm.set_demand_geo_blacklist = revert_writer
        try:
            rc = gc.revert(str(path), args_for(apply=True))
        finally:
            gc.tbm.set_demand_geo_blacklist = real

    check("revert succeeds", rc == 0, str(rc))
    check("it restores the prior list, not an empty one",
          seen.get("ids") == [7], str(seen))
    check("and it uses replace=True, or the additions would survive",
          seen.get("replace") is True, str(seen))


def test_not_set_is_unattributed() -> None:
    """The live dry run's first real bug.

    "Not set" is the platform's own label for a row it could not attribute to
    a country. The guessed UNATTRIBUTED set did not contain it, so the filter
    never fired; the country-id resolver caught it downstream and refused the
    whole buyer, taking 62 valid countries with it on Zeta - RON Desktop.
    """
    print("\n'Not set' is a non-country, whatever its casing")
    for label in ("Not set", "not set", "  NOT SET  ", "(none)", "None", "-"):
        rows = [pair(501, label, 5_000_000),
                pair(501, "BR", 5_000_000),
                pair(501, "US", 1, imps=1, spend=90.0)]
        targets, skipped = gc.select(rows, args_for())
        blocked = targets.get(501, {}).get("countries", [])
        check(f"{label!r} is not blacklisted", label not in blocked, str(blocked))
        check(f"{label!r} does not cost the buyer its real countries",
              blocked == ["BR"], str(blocked))


def test_country_cap_skips_a_broken_integration() -> None:
    """63 dead countries on one buyer is not a geo problem."""
    print("\na buyer with too many dead countries is a source decision")
    many = [pair(501, f"C{i}", 5_000_000) for i in range(30)]
    many.append(pair(501, "US", 1, imps=1, spend=90.0))
    targets, skipped = gc.select(many, args_for())
    check("the buyer is skipped entirely", 501 not in targets, str(list(targets)))
    check("the skip explains and points at NO-WIN",
          any("broken integration" in why and "NO-WIN" in why
              for _, why in skipped), str([w for _, w in skipped][:1]))

    # Just under the cap is still ordinary geo waste.
    few = [pair(502, f"C{i}", 5_000_000) for i in range(25)]
    few.append(pair(502, "US", 1, imps=1, spend=90.0))
    targets, _ = gc.select(few, args_for())
    check("a buyer at the cap is still actionable",
          len(targets.get(502, {}).get("countries", [])) == 25,
          str(len(targets.get(502, {}).get("countries", []))))

    # And the rail can be turned off deliberately.
    targets, _ = gc.select(many, args_for(max_countries_per_buyer=0))
    check("0 disables the rail", 501 in targets, str(list(targets)))


def test_write_normalisation_matches_the_measured_offenders() -> None:
    """The four fields tbx_type_probe found on 8 live demand sources.

    TBX serves demand sources it will not accept back, which is why all 50
    writes 422'd on 2026-08-31. Each repair must represent absence, never a
    value we chose: vcr_optimization: 0 is a VCR target of zero, not an unset
    one, and target_srcpm "default" vs "target" is a live trading setting.
    """
    print("\nthe payload is repaired to what the write schema accepts")
    live = {
        "id": 501,
        "target_srcpm_value": None,      # 8/8 declared number, is null
        "vcr_optimization": None,        # 8/8 declared number, is null
        "geo_settings": [],              # 7/8 declared object, is []
        "target_srcpm": None,            # 1/8 declared string, is null
        "name": "Real Buyer",            # untouched
        "margin_min": 7,                 # untouched
    }
    out, notes = tbm._normalise_for_write(live, "demand_source")

    for field in ("target_srcpm_value", "vcr_optimization", "target_srcpm"):
        check(f"{field}: the null is dropped, not coerced",
              field not in out, str(out.get(field)))
    check("geo_settings: [] becomes {}", out["geo_settings"] == {},
          str(out["geo_settings"]))
    check("no value is invented for a dropped field",
          all(v is not None for v in out.values()), str(out))
    check("fields that were fine are untouched",
          out["name"] == "Real Buyer" and out["margin_min"] == 7, str(out))
    check("the repairs are reported so they can be logged",
          len(notes) == 4, str(notes))
    check("the input is not mutated", live["geo_settings"] == [], str(live))

    # A populated geo_settings must survive — only an EMPTY list is the PHP
    # empty-associative-array tell. A non-empty list is real data.
    out2, _ = tbm._normalise_for_write(
        {"geo_settings": [{"country_id": 7}]}, "demand_source")
    check("a non-empty list is left alone",
          out2["geo_settings"] == [{"country_id": 7}], str(out2))

    # Supply had zero mismatches; normalisation must stay a no-op there.
    out3, notes3 = tbm._normalise_for_write(
        {"id": 4, "name": "Pub", "margin_min": 5}, "supply_source")
    check("a clean supply source is unchanged", notes3 == [], str(notes3))


def test_normalisation_does_not_trip_the_key_loss_guard() -> None:
    """Dropping a null is a repair; _assert_no_key_loss must not see it.

    The guard compares the merged payload against the body it merged onto, so
    normalising before the merge is what keeps a deliberate drop from reading
    as the field-blanking failure the guard exists to catch.
    """
    print("\nthe repair does not read as field-blanking")
    body, _ = tbm._normalise_for_write(
        {"id": 1, "vcr_optimization": None, "geo_settings": []}, "demand_source")
    merged = tbm._deep_merge(body, {"geo_settings": {"blacklist": [1]}})
    check("no key loss is reported",
          tbm._assert_no_key_loss(body, merged) in ([], None),
          str(tbm._assert_no_key_loss(body, merged)))
    check("the blacklist landed on a dict, not an empty list",
          merged["geo_settings"] == {"blacklist": [1]}, str(merged))


def test_partial_window_cannot_drive_a_write() -> None:
    """A pair that only traded on a day that timed out looks dead.

    pull_pairs now keeps the days that answered rather than losing the run,
    which is right for a report and dangerous for a blacklist.
    """
    print("\na partial window blocks --apply but not a dry run")
    rows = [pair(501, "BR", 5_000_000), pair(501, "US", 1, imps=1, spend=90.0)]

    def fake_measure(args):
        return rows, 3                      # 3 of 7 days answered

    calls = []
    real_measure, real_conf = gc.measure, gc.tbx.configured
    real_write, real_resolve = gc.tbm.set_demand_geo_blacklist, gc.tbx.country_ids
    gc.measure = fake_measure
    # Without this main() returns 2 at the credential check, which is the same
    # code the coverage gate uses — the refusal has to be the reason.
    gc.tbx.configured = lambda: True
    gc.tbx.country_ids = lambda codes: [1] * len(codes)
    gc.tbm.set_demand_geo_blacklist = lambda *a, **k: calls.append(a) or {
        "applied": True, "added": [], "blacklist_before": []}
    try:
        rc_apply = gc.main(["--apply"])
        refused_writes = list(calls)     # must be empty; the later two are not
        rc_dry = gc.main([])
        # --ledger into a temp dir: without it this writes a real-looking
        # geo-ledger-*.json into the repo root, and two of those were
        # committed by mistake in #141.
        with tempfile.TemporaryDirectory() as tmp:
            rc_override = gc.main(["--apply", "--min-days-with-data", "3",
                                   "--ledger", str(Path(tmp) / "ledger.json")])
    finally:
        gc.measure, gc.tbx.configured = real_measure, real_conf
        gc.tbm.set_demand_geo_blacklist = real_write
        gc.tbx.country_ids = real_resolve

    check("--apply is refused on a partial window", rc_apply == 2, str(rc_apply))
    check("and it refused before reaching the writer",
          refused_writes == [], str(refused_writes))
    check("a dry run still reports", rc_dry in (0, 1), str(rc_dry))
    check("an explicit lower bar is honoured", rc_override != 2, str(rc_override))


def test_crash_exit_code_is_not_the_refusal_code() -> None:
    print("\na crash is distinguishable from a refused write")
    src = Path(gc.__file__).read_text()
    guard = src.rsplit('if __name__ == "__main__":', 1)[-1]
    check("the entrypoint catches TbxError", "TbxError" in guard, guard.strip())
    check("and exits 3, not 1", "sys.exit(3)" in guard, guard.strip())


def test_parser() -> None:
    print("\nargument surface")
    import re
    args = gc.build_parser().parse_args([])
    src = Path(gc.__file__).read_text()
    used = set(re.findall(r"\bargs\.([a-z_]+)", src))
    # set by main() rather than the parser
    used -= {"exclude", "include"}
    missing = sorted(u for u in used if not hasattr(args, u))
    check("every args.X the module reads is defined by the parser",
          not missing, f"missing: {missing}")
    check("the default forbids blocking anything with impressions",
          args.max_imps_day == 0.0, str(args.max_imps_day))
    check("applying is opt-in", args.apply is False)


def main() -> int:
    print("=" * 70)
    print("tbx_geo_cut — offline checks")
    print("=" * 70)
    test_blacklist_merges()
    test_blacklist_replace_is_opt_in()
    test_blacklist_handles_empty_current()
    test_trading_pair_is_never_blocked()
    test_spend_rail_beats_include()
    test_dead_buyer_is_a_source_decision()
    test_unattributed_country_is_refused()
    test_volume_bar_and_grouping()
    test_exclusions()
    test_dry_run_never_writes()
    test_partial_country_resolution_refuses_the_buyer()
    test_ledger_round_trip()
    test_not_set_is_unattributed()
    test_country_cap_skips_a_broken_integration()
    test_write_normalisation_matches_the_measured_offenders()
    test_normalisation_does_not_trip_the_key_loss_guard()
    test_partial_window_cannot_drive_a_write()
    test_crash_exit_code_is_not_the_refusal_code()
    test_parser()
    print("\n" + "=" * 70)
    print(f"{PASS} passed, {FAIL} failed")
    return 1 if FAIL else 0


if __name__ == "__main__":
    sys.exit(main())
