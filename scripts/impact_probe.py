#!/usr/bin/env python3
"""Read-only capability probe for the impact.com API.

Exists for the same reason scripts/tbx_probe.py does, and more urgently:
`core/impact_api.py` and `agents/etl/impact_revenue_etl.py` were written
WITHOUT a live account. api.impact.com is unreachable from this repo's cloud
sessions (the egress proxy 403s it) and no IMPACT_* credential existed anywhere
when they were written, 2026-08-26.

So the transport is standard and probably right; the per-account FIELD NAMES
are the part that needs confirming before any number from this leg is quoted
at anyone. This script confirms them, on a machine that has the credentials,
without writing anything.

Run this BEFORE trusting the ETL's output.

Usage
-----
    # auth only — cheapest possible check that the credentials work
    python3 scripts/impact_probe.py

    # the important one: what an action actually looks like, and which of
    # core.impact_api.ACTION_FIELDS resolve against it
    python3 scripts/impact_probe.py --actions --days 30

    # what reports this account can run, AND what their rows look like.
    # /Actions has no clicks or impressions, so this is the run that unblocks
    # the EPC / conversion-rate grain. Prints a paste-ready summary at the end.
    python3 scripts/impact_probe.py --reports --days 30

    # everything, saved for review
    python3 scripts/impact_probe.py --actions --reports --json /tmp/impact.json

Nothing here writes to impact.com or to Neon.
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import date, timedelta

sys.path.insert(0, __file__.rsplit("/scripts/", 1)[0])

from core import impact_api as imp          # noqa: E402
from core.impact_api import ImpactError     # noqa: E402

OK, FAIL, SKIP = "✓", "✗", "–"

# Fields whose absence is not cosmetic. Everything else being missing costs a
# column; these being missing costs the ETL its ability to key, date, or value
# a row at all.
CRITICAL = ("action_id", "event_date", "payout", "state")


def _probe(results: dict, label: str, fn, *args, **kwargs):
    """Run one read, record the outcome, never raise."""
    try:
        value = fn(*args, **kwargs)
    except ImpactError as exc:
        print(f"  {FAIL} {label:28} {exc}")
        results[label] = {"ok": False, "error": str(exc)[:500],
                          "status": getattr(exc, "status", None)}
        return None
    except Exception as exc:
        print(f"  {FAIL} {label:28} {type(exc).__name__}: {exc}")
        results[label] = {"ok": False, "error": f"{type(exc).__name__}: {exc}"[:500]}
        return None

    count = len(value) if isinstance(value, (list, tuple)) else 1
    print(f"  {OK} {label:28} {count} record(s)")
    results[label] = {"ok": True, "count": count}
    return value


def check_field_mapping(rows: list[dict]) -> dict:
    """
    For each logical field, report which vendor spelling actually resolved.

    This is the whole point of the probe. A field that resolves via no
    candidate is a column the ETL will silently write as NULL — or, for the
    critical four, a row it will drop entirely.
    """
    print("\nField mapping (core.impact_api.ACTION_FIELDS vs live rows)")
    print("-" * 70)
    report: dict[str, dict] = {}
    sample_n = len(rows)

    for logical, candidates in imp.ACTION_FIELDS.items():
        hits: dict[str, int] = {}
        for row in rows:
            for key in candidates:
                if key in row and row[key] not in (None, ""):
                    hits[key] = hits.get(key, 0) + 1
                    break
        resolved = sum(hits.values())
        mark = OK if resolved else (FAIL if logical in CRITICAL else SKIP)
        via = ", ".join(f"{k} ({v})" for k, v in sorted(hits.items(),
                                                       key=lambda kv: -kv[1]))
        note = ""
        if not resolved:
            note = ("  <-- CRITICAL: the ETL cannot use these rows"
                    if logical in CRITICAL else "  (column will be NULL)")
        print(f"  {mark} {logical:18} {resolved}/{sample_n:<5} {via}{note}")
        report[logical] = {"resolved": resolved, "of": sample_n, "via": hits,
                           "critical": logical in CRITICAL}

    # Vendor keys nothing in ACTION_FIELDS claims. Often the interesting ones:
    # account-specific custom fields, and any renamed standard field.
    known = {k for cands in imp.ACTION_FIELDS.values() for k in cands}
    seen: dict[str, int] = {}
    for row in rows:
        for key in row:
            if key not in known:
                seen[key] = seen.get(key, 0) + 1
    if seen:
        print("\nVendor keys NOT mapped by ACTION_FIELDS "
              "(candidates for new columns):")
        for key, n in sorted(seen.items(), key=lambda kv: -kv[1]):
            print(f"    {key}  ({n}/{sample_n})")
        report["_unmapped"] = seen

    missing_critical = [f for f in CRITICAL if not report[f]["resolved"]]
    if missing_critical:
        print(f"\n{FAIL} {len(missing_critical)} CRITICAL field(s) unresolved: "
              f"{', '.join(missing_critical)}")
        print("    Add the real vendor spelling to ACTION_FIELDS in "
              "core/impact_api.py before running the ETL — every row will "
              "otherwise be dropped, and the run will look merely empty.")
    else:
        print(f"\n{OK} all critical fields resolve — the ETL can key, date, "
              f"and value these rows.")
    report["_missing_critical"] = missing_critical
    return report


def check_states(rows: list[dict]) -> dict:
    """
    Which lifecycle states this account actually uses.

    The views in migrations/2026_08_26_impact_affiliate.sql filter on the
    literal strings PENDING / APPROVED / REVERSED / LOCKED. A state this
    account spells differently would fall out of every one of those filters —
    counted in actions_total, absent from every payout column.
    """
    seen: dict[str, int] = {}
    for row in rows:
        state = imp.action_field(row, "state")
        key = str(state).upper() if state else "(none)"
        seen[key] = seen.get(key, 0) + 1
    print("\nAction states present:")
    unexpected = []
    for state, n in sorted(seen.items(), key=lambda kv: -kv[1]):
        known = state in imp.ACTION_STATES
        if not known and state != "(none)":
            unexpected.append(state)
        print(f"  {OK if known else FAIL} {state:12} {n}")
    if unexpected:
        print(f"  {FAIL} {', '.join(unexpected)} not in ACTION_STATES — the "
              f"views' payout_* columns will not count these rows. Add them "
              f"to the migration's FILTER clauses.")
    return {"states": seen, "unexpected": unexpected}


def check_subid_coverage(rows: list[dict]) -> dict:
    """
    How much revenue can be attributed to a PGAM property.

    SubId1 is set by the tracking link, not by impact.com. If PGAM's links
    don't carry it, impact_daily_property_revenue is one big '(unset)' bucket
    and the per-site question cannot be answered from this data at all. Better
    to learn that here than from an empty dashboard panel.
    """
    total = len(rows)
    with_sub = 0
    values: dict[str, int] = {}
    for row in rows:
        sub = imp.action_field(row, "sub_id1")
        if sub:
            with_sub += 1
            values[str(sub)] = values.get(str(sub), 0) + 1
    pct = (100.0 * with_sub / total) if total else 0.0
    print(f"\nSubId1 coverage: {with_sub}/{total} ({pct:.0f}%) actions carry a "
          f"property tag")
    for val, n in sorted(values.items(), key=lambda kv: -kv[1])[:15]:
        print(f"    {val}  ({n})")
    if total and pct < 50:
        print(f"  {FAIL} Under half the actions are attributable to a site. "
              f"Per-property revenue will be mostly '(unset)' until the "
              f"tracking links set SubId1.")
    return {"total": total, "with_sub_id1": with_sub, "pct": round(pct, 1),
            "values": values}


def check_currencies(rows: list[dict]) -> dict:
    """More than one payout currency means no total may be summed naively."""
    seen: dict[str, int] = {}
    for row in rows:
        cur = (imp.action_field(row, "payout_currency")
               or imp.action_field(row, "currency") or "UNKNOWN")
        seen[str(cur)] = seen.get(str(cur), 0) + 1
    print(f"\nPayout currencies: {', '.join(f'{k} ({v})' for k, v in seen.items())}")
    if len(seen) > 1:
        print("  NOTE: multi-currency account. The views keep currency in the "
              "grain and this repo has no FX source — any dashboard summing "
              "across currencies is wrong.")
    return seen


def check_modification_sweep(days: int) -> dict:
    """
    Whether the reversal pass works on this account.

    ModificationDateStart is not part of every account's accepted parameter
    set. If it 4xxs here, the ETL's hourly reversal sweep is dead and the
    daily deep pass is the only thing catching reversals — which is fine, but
    it must not then be shortened.
    """
    since = (date.today() - timedelta(days=days)).isoformat()
    print(f"\nModification sweep (reversal catcher), since {since}")
    try:
        rows = imp.actions_modified_since(since)
    except ImpactError as exc:
        print(f"  {FAIL} unsupported or rejected — {exc}")
        print("      The ETL degrades to a warning and relies on the daily "
              "--deep pass. Do not narrow DEEP_WINDOW_DAYS while this fails.")
        return {"ok": False, "error": str(exc)[:500]}
    print(f"  {OK} {len(rows)} action(s) modified in the window")
    return {"ok": True, "count": len(rows)}


# How many candidate performance reports to actually run. Each one costs up to
# len(REPORT_DATE_PARAM_CANDIDATES) requests against a metered API, so this is
# bounded — and when it bites, the number dropped is printed rather than
# silently truncated.
MAX_SHAPED = 5


def audit_reports(args) -> dict:
    """
    List the report catalog, then run the likely performance reports to learn
    their real parameter names and row shape.

    This exists because /Actions carries conversions and payout but NOT clicks
    or impressions, so EPC and conversion rate need a second grain — and that
    grain cannot be written until three unknowns are settled: which report id,
    which date-parameter spelling it takes, and what its rows are called. All
    three are account-specific. One run of this answers all three, so the
    follow-up does not need a second trip to the account.
    """
    out: dict = {}
    print("\nReport catalog (ids are account-specific — nothing in this repo "
          "hardcodes one)")
    try:
        catalog = imp.report_catalog()
    except ImpactError as exc:
        print(f"  {FAIL} could not list reports — {exc}")
        return {"ok": False, "error": str(exc)[:500]}

    starred: list[dict] = []
    for rep in catalog:
        perf = imp.looks_like_perf_report(rep)
        if perf:
            starred.append(rep)
        print(f"  {'*' if perf else ' '} {str(rep.get('Id', '?')):34} "
              f"{rep.get('Name', '')}")
    print(f"\n  {len(catalog)} report(s), {len(starred)} marked (*) as likely "
          f"to carry clicks/impressions")
    out["catalog"] = [{"id": r.get("Id"), "name": r.get("Name"),
                       "perf_candidate": imp.looks_like_perf_report(r)}
                      for r in catalog]

    targets = [r for r in starred if r.get("Id")]
    if args.report_shape:
        wanted = {t.strip() for t in args.report_shape.split(",") if t.strip()}
        targets = [{"Id": rid} for rid in wanted]
        print(f"\nShaping the {len(targets)} report(s) named on the command line")
    elif len(targets) > MAX_SHAPED:
        print(f"\n  Shaping only the first {MAX_SHAPED} of {len(targets)} "
              f"candidates (each costs up to "
              f"{len(imp.REPORT_DATE_PARAM_CANDIDATES)} requests). "
              f"DROPPED: {', '.join(str(r.get('Id')) for r in targets[MAX_SHAPED:])}"
              f" — pass --report-shape to inspect those.")
        targets = targets[:MAX_SHAPED]

    end = date.today()
    start = end - timedelta(days=max(args.days - 1, 0))
    shapes: dict = {}

    for rep in targets:
        rid = str(rep.get("Id"))
        print(f"\n{'-' * 70}\nReport {rid}  ({start}..{end})")

        meta = None
        try:
            meta = imp.report_metadata(rid)
        except ImpactError as exc:
            print(f"  metadata: error — {exc}")
        if meta:
            # If the account documents its own parameters, that beats guessing
            # from the candidate list below.
            keys = sorted(meta) if isinstance(meta, dict) else []
            print(f"  {OK} metadata exposed, keys: {keys}")
        else:
            print(f"  {SKIP} no metadata endpoint — falling back to trying "
                  f"parameter spellings")

        findings = imp.try_report(rid, start.isoformat(), end.isoformat())
        accepted = [f for f in findings if f["ok"]]
        with_rows = [f for f in accepted if f["rows"]]

        for f in findings:
            if not f["ok"]:
                print(f"  {FAIL} {f['label']:26} {f.get('status') or ''} "
                      f"{f['error'][:90]}")
            else:
                print(f"  {OK} {f['label']:26} {f['rows']} row(s)")

        winner = with_rows[0] if with_rows else (accepted[0] if accepted else None)
        if winner and winner.get("row_keys"):
            print(f"\n  ROW KEYS: {winner['row_keys']}")
            print(f"  SAMPLE:   "
                  f"{json.dumps(winner['sample'], default=str)[:900]}")
        elif winner:
            print(f"\n  accepted with no rows — envelope keys: "
                  f"{winner.get('envelope_keys')}")
            print("  Widen --days, or this report has no data for the window.")
        else:
            print("\n  no parameter spelling was accepted for this report.")

        shapes[rid] = {"metadata_keys": sorted(meta) if isinstance(meta, dict) else None,
                       "attempts": findings,
                       "winner": winner.get("label") if winner else None,
                       "row_keys": winner.get("row_keys") if winner else []}

    out["shapes"] = shapes

    # The handoff. Everything a follow-up needs to write the clicks grain, in
    # one block, so it can be pasted into a fresh session rather than
    # re-derived from the scrollback above.
    #
    # Only reports that both accepted a parameter spelling AND returned rows
    # go in it. A report that answered nothing is not a finding you can build
    # from, and mixing it in here turns the one block that should be pure
    # signal back into something that needs reading around.
    usable = {rid: sh for rid, sh in shapes.items()
              if sh["winner"] and sh["row_keys"]}
    unusable = {rid: sh for rid, sh in shapes.items() if rid not in usable}

    if usable:
        print(f"\n{'=' * 70}")
        print("PASTE THIS into a fresh session to get the clicks/impressions "
              "grain built:")
        print("=" * 70)
        for rid, shape in usable.items():
            print(f"  report_id   : {rid}")
            print(f"  date params : {shape['winner']}")
            print(f"  row keys    : {shape['row_keys']}")
            print()
        print("(ids and column names only — no credentials in the above.)")
    elif shapes:
        print(f"\n{'=' * 70}")
        print(f"{FAIL} None of the {len(shapes)} report(s) inspected returned "
              f"usable rows, so the clicks/impressions grain still has "
              f"nothing to build on.")
        print("    Next: widen --days (the window may simply be empty), or "
              "pass --report-shape with ids from the catalog above that this "
              "run did not flag.")

    if unusable:
        print(f"\n  not usable from this run: " + ", ".join(
            f"{rid} ({'no rows' if sh['winner'] else 'no accepted params'})"
            for rid, sh in unusable.items()))

    out["usable"] = list(usable)
    return out


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Read-only probe of the impact.com API for PGAM's account")
    parser.add_argument("--days", type=int, default=30,
                        help="trailing days of actions to sample (default 30)")
    parser.add_argument("--actions", action="store_true",
                        help="pull actions and audit the field mapping "
                             "(the check that matters)")
    parser.add_argument("--reports", action="store_true",
                        help="list the report catalog AND run the likely "
                             "performance reports to learn their real "
                             "parameter names and row shape — this is what "
                             "the clicks/impressions grain is waiting on")
    parser.add_argument("--report-shape", metavar="ID[,ID]",
                        help="shape these specific report ids instead of the "
                             "auto-detected candidates")
    parser.add_argument("--json", metavar="PATH",
                        help="write findings as JSON")
    args = parser.parse_args()

    results: dict = {"account_type": imp.IMPACT_ACCOUNT_TYPE,
                     "base": imp.IMPACT_BASE}

    print(f"impact.com probe — {imp.IMPACT_BASE} "
          f"/{imp.IMPACT_ACCOUNT_TYPE}/…")
    if not imp.configured():
        missing = ", ".join(imp.missing_env())
        print(f"\n{FAIL} not configured — missing {missing}")
        print("    Both come from the impact.com UI: Settings → API Access "
              "(Account SID + Auth Token).")
        print("    Export them and re-run:")
        print("        export IMPACT_ACCOUNT_SID=...")
        print("        export IMPACT_AUTH_TOKEN=...")
        return 2

    print("\nAuth + basic reads")
    conn = imp.test_connection()
    if not conn.get("ok"):
        print(f"  {FAIL} {conn.get('error')}")
        results["connection"] = conn
        if args.json:
            _dump(args.json, results)
        return 1
    print(f"  {OK} authenticated — {conn['campaigns']} campaign(s): "
          f"{', '.join(str(s) for s in conn['sample'] if s)}")
    results["connection"] = conn

    if args.reports:
        results["reports"] = audit_reports(args)

    if args.actions:
        end = date.today()
        start = end - timedelta(days=max(args.days - 1, 0))
        print(f"\nActions {start}..{end}")
        rows = _probe(results, "actions", imp.actions,
                      date_start=start.isoformat(), date_end=end.isoformat())
        if rows:
            results["field_mapping"] = check_field_mapping(rows)
            results["states"] = check_states(rows)
            results["sub_id1"] = check_subid_coverage(rows)
            results["currencies"] = check_currencies(rows)
            print("\nOne full raw action, for reference:")
            print(json.dumps(rows[0], indent=2, default=str)[:2000])
        elif rows is not None:
            print(f"  {SKIP} no actions in the window — widen --days, or this "
                  f"account genuinely has no conversions yet. Either way the "
                  f"field mapping is UNVERIFIED.")
        results["modification_sweep"] = check_modification_sweep(args.days)

    if args.json:
        _dump(args.json, results)

    fm = results.get("field_mapping") or {}
    if fm.get("_missing_critical"):
        return 1
    return 0


def _dump(path: str, results: dict) -> None:
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(results, fh, indent=2, default=str)
    print(f"\nwrote {path}")


if __name__ == "__main__":
    raise SystemExit(main())
