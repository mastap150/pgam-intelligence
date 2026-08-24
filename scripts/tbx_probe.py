#!/usr/bin/env python3
"""Read-only capability probe for the new Teqblaze platform (api.pgammedia.com).

Exists because this integration was written from the OpenAPI spec
(`docs/api/teqblaze-openapi.json`), not against a live account. Nothing in
`core/tbx_api.py` or `core/tbx_mgmt.py` has been exercised against real
credentials. This script does that, one module at a time, and reports exactly
which surfaces answer.

Run it first, on a machine that has TBX_EMAIL / TBX_PASSWORD, before wiring
any agent to this platform.

Usage
-----
    # auth + one call per module (safe, read-only)
    python3 scripts/tbx_probe.py

    # add a week of report/analytics reads
    python3 scripts/tbx_probe.py --days 7 --reports

    # dump one entity's config and the exact payload an update would send.
    # This is the check that must pass before TBX_ALLOW_WRITES is ever set.
    python3 scripts/tbx_probe.py --diff-shape supply:22
    python3 scripts/tbx_probe.py --diff-shape demand:91

    # write findings to a file for review
    python3 scripts/tbx_probe.py --reports --json /tmp/tbx_probe.json

Nothing here writes. `--diff-shape` builds an update payload and prints it
without sending it.
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import date, timedelta

sys.path.insert(0, __file__.rsplit("/scripts/", 1)[0])

from core import tbx_api as tbx          # noqa: E402
from core import tbx_mgmt as tbm         # noqa: E402
from core.tbx_api import TbxError        # noqa: E402

OK, FAIL, SKIP = "✓", "✗", "–"


def _probe(results: dict, label: str, fn, *args, **kwargs) -> object:
    """Run one read, record shape + outcome, never raise."""
    try:
        value = fn(*args, **kwargs)
    except TbxError as exc:
        print(f"  {FAIL} {label:32} {exc}")
        results[label] = {"ok": False, "error": str(exc)[:500],
                          "status": getattr(exc, "status", None)}
        return None
    except Exception as exc:  # unexpected: shape mismatch, bad assumption here
        print(f"  {FAIL} {label:32} {type(exc).__name__}: {exc}")
        results[label] = {"ok": False, "error": f"{type(exc).__name__}: {exc}"[:500]}
        return None

    if isinstance(value, list):
        summary = f"{len(value)} rows"
        sample_keys = sorted(value[0].keys()) if value and isinstance(value[0], dict) else []
    elif isinstance(value, tuple):
        rows = value[0] if value else []
        summary = f"{len(rows)} rows + totals"
        sample_keys = sorted(rows[0].keys()) if rows and isinstance(rows[0], dict) else []
    elif isinstance(value, dict):
        summary = f"dict[{len(value)}]"
        sample_keys = sorted(value.keys())[:20]
    else:
        summary = type(value).__name__
        sample_keys = []

    print(f"  {OK} {label:32} {summary}")
    if sample_keys:
        print(f"      keys: {', '.join(str(k) for k in sample_keys[:14])}")
    results[label] = {"ok": True, "summary": summary, "keys": sample_keys}
    return value


def probe_entities(results: dict) -> None:
    print("\n── Entities ──────────────────────────────────────────────")
    _probe(results, "permissions", tbx.permissions)
    supply = _probe(results, "supply-sources", tbm.list_supply_sources)
    demand = _probe(results, "demand-sources", tbm.list_demand_sources)
    _probe(results, "companies", tbm.list_companies)
    _probe(results, "filter-lists", tbm.list_filter_lists)
    _probe(results, "deals", tbm.list_deals)
    _probe(results, "alerts", tbm.list_alerts)
    _probe(results, "scheduled-reports", tbm.list_scheduled_reports)
    _probe(results, "supply qps-capacity", tbm.supply_qps_capacity)
    _probe(results, "demand qps-capacity", tbm.demand_qps_capacity)

    if supply:
        sid = supply[0].get("id")
        if sid is not None:
            _probe(results, f"supply {sid} config", tbm.get_supply_source, int(sid))
            _probe(results, f"supply {sid} placements", tbm.list_placements, int(sid))
            _probe(results, f"supply {sid} ads-txt", tbm.get_supply_ads_txt, int(sid))
    if demand:
        did = demand[0].get("id")
        if did is not None:
            _probe(results, f"demand {did} config", tbm.get_demand_source, int(did))


def probe_dictionaries(results: dict) -> None:
    print("\n── Dictionaries ──────────────────────────────────────────")
    for dict_type in ("countries", "supply-sources", "demand-sources",
                      "failure-reasons", "ivt-reasons", "seats", "scanners",
                      "presets", "banner-sizes"):
        _probe(results, f"dict:{dict_type}", tbx.dictionary, dict_type)


def probe_reports(results: dict, days: int) -> None:
    date_to = date.today() - timedelta(days=1)
    date_from = date_to - timedelta(days=max(days - 1, 0))
    df, dt = date_from.isoformat(), date_to.isoformat()
    print(f"\n── Analytics ({df} → {dt}) ───────────────────────")

    _probe(results, "report columns-list", tbx.report_columns)
    _probe(results, "report by source", tbx.report, df, dt,
           attributes=["date", "supply_source"],
           metrics=["imps_sum", "ssp_price_sum", "dsp_price_sum", "profit", "margin"])
    _probe(results, "report by demand×country", tbx.report, df, dt,
           attributes=["demand_source", "country"],
           metrics=["requests_sum", "responses_sum", "imps_sum",
                    "demand_win_rate", "timeout_rate", "dsp_price_sum"])
    _probe(results, "report by inventory_key", tbx.report, df, dt,
           attributes=["inventory_key"],
           metrics=["imps_sum", "dsp_price_sum", "margin"],
           sort=[{"field": "dsp_price_sum", "direction": "desc"}])
    _probe(results, "bids-overview incoming", tbx.bids_overview, "incoming",
           date_from=df, date_to=dt)
    _probe(results, "bids-overview outgoing", tbx.bids_overview, "outgoing",
           date_from=df, date_to=dt)
    _probe(results, "human settings", tbx.human_report_settings)
    _probe(results, "human risk-metrics", tbx.human_report, "risk-metrics",
           date_from=df, date_to=dt, attributes=["inventory_key"],
           metrics=["requests_sum", "mfa_rate", "sivt_rate", "givt_rate"])
    _probe(results, "schain-utilisation", tbx.schain_utilisation, df, dt,
           attributes=["supply_source", "in_complete", "out_complete"],
           metrics=["imps_sum", "dsp_price_sum"])
    _probe(results, "sellers-validation", tbx.sellers_validation, df, dt,
           attributes=["inventory_key", "seller_domain",
                       "sellers_verification_attr", "adstxt_verification_attr"],
           metrics=["imps_sum", "dsp_price_sum"])
    _probe(results, "ads-txt-verification", tbx.ads_txt_verification)
    _probe(results, "scanner-stats prebid", tbx.scanner_statistics, "prebid",
           date_from=df, date_to=dt,
           attributes=["supply_source"], metrics=["requests_sum", "blocked_rate"])
    _probe(results, "discrepancy-report", tbx.discrepancy_report, df, dt)
    _probe(results, "traffic-logger", tbx.traffic_logger, per_page=5, max_pages=1)


def diff_shape(spec: str) -> int:
    """
    Print an entity's current config beside the payload an update would send.

    The write path is read-modify-write against endpoints that replace the
    whole object, so the round trip has to be lossless. Any key listed under
    "DROPPED" would be blanked by a live update — if that list holds anything
    beyond the known read-only fields, do not enable writes.
    """
    try:
        kind_token, raw_id = spec.split(":", 1)
        entity_id = int(raw_id)
    except ValueError:
        print(f"--diff-shape wants supply:<id> or demand:<id>, got {spec!r}", file=sys.stderr)
        return 2

    kind = {"supply": "supply_source", "demand": "demand_source"}.get(kind_token)
    if kind is None:
        print(f"unknown entity kind {kind_token!r}; use 'supply' or 'demand'", file=sys.stderr)
        return 2

    current = (tbm.get_supply_source(entity_id) if kind == "supply_source"
               else tbm.get_demand_source(entity_id))
    if not current:
        print(f"GET returned nothing for {kind} {entity_id}", file=sys.stderr)
        return 1

    payload = tbm._strip_read_only(current, kind)

    print(f"\n── {kind} {entity_id}: GET response ──────────────────────")
    print(json.dumps(current, indent=2, default=str)[:8000])

    print(f"\n── payload an update would POST ─────────────────────────")
    print(json.dumps(payload, indent=2, default=str)[:8000])

    dropped = sorted(set(current) - set(payload))
    expected = set(tbm._READ_ONLY_FIELDS.get(kind, ()))
    print(f"\nDROPPED by _strip_read_only: {dropped}")
    print(f"  expected read-only fields:  {sorted(expected)}")

    verdicts: list[str] = []

    # Direction 1 — did we drop something we did not mean to? Only ever fires
    # if _strip_read_only grows a bug: it pops exactly `expected`, so on
    # today's code this is a regression guard, not a discovery.
    unexpected = [k for k in dropped if k not in expected]
    if unexpected:
        print(f"  {FAIL} UNEXPECTED drops: {unexpected} — do NOT enable writes")
        verdicts.append("unexpected drops")

    # Direction 2 — is the account returning fields the write schema does not
    # accept? This is the one that finds real problems, because it compares the
    # LIVE response against the spec rather than against our own strip list.
    # It is how `uuid` on a demand source was caught. Anything here has to be
    # added to _READ_ONLY_FIELDS (or the spec re-vendored) before writes.
    accepted = tbm.write_schema_fields(kind)
    if not accepted:
        print(f"  {SKIP} write schema for {kind} unreadable — cannot check "
              f"payload keys against the spec")
    else:
        undeclared = tbm.unknown_write_keys(payload, kind)
        print(f"\n  payload keys vs {tbm._WRITE_SCHEMA[kind]}: "
              f"{len(payload)} sent, {len(accepted)} accepted by the schema")
        if undeclared:
            print(f"  {FAIL} NOT ACCEPTED by the write schema: {undeclared}")
            print(f"      → add to core.tbx_mgmt._READ_ONLY_FIELDS[{kind!r}], "
                  f"or re-vendor docs/api/teqblaze-openapi.json if the "
                  f"platform has moved on. Do NOT enable writes first.")
            verdicts.append("undeclared payload keys")
        else:
            print(f"  {OK} every payload key is declared by the write schema")

        # Informational: schema fields the account did not return. Harmless
        # for a full-replace update (we cannot send what we were not given)
        # but worth seeing, because it says which levers this account lacks.
        absent = sorted(accepted - set(payload))
        if absent:
            print(f"  {SKIP} not returned by this account: {absent}")

    if verdicts:
        print(f"\n  {FAIL} round trip is NOT lossless: {', '.join(verdicts)}")
        return 1
    print(f"\n  {OK} round trip looks lossless")
    return 0


def reachability(date_from: str, date_to: str | None) -> int:
    """
    How far back will this platform actually serve?

    The one question a backfill depends on, asked in the cheapest possible
    way: one single-day report per day, smallest useful attribute+metric set,
    and a check of the `date` on every row that comes back.

    Why it needs asking at all. A multi-day request is answered `200` with
    only the most recent ~5 days in it — no error, no flag, no short-window
    marker (reference A5.1). What was never established is what that window
    is anchored to:

      * anchored to `date_to`  -> any historical day is reachable one day at
                                  a time, and a backfill of any age works.
      * anchored to *today*    -> days older than the window are gone for
                                  good, and a backfill has a deadline.

    Both readings fit the original observation, which was made on a query
    ending at the present day. A single-day request per day distinguishes
    them: under the first reading every day answers, under the second only
    the recent ones do.

    Reads nothing but reports and writes nothing anywhere.
    """
    from datetime import date as _date, timedelta as _td

    try:
        start = _date.fromisoformat(date_from)
        end = _date.fromisoformat(date_to) if date_to else (
            _date.today() - _td(days=1))
    except ValueError as exc:
        print(f"bad date: {exc}", file=sys.stderr)
        return 2
    if start > end:
        print(f"--reach-from {start} is after {end}", file=sys.stderr)
        return 2

    span = (end - start).days + 1
    print(f"\n── Day reachability ──────────────────────────────────────")
    print(f"  {start} → {end} ({span} day(s)), one request each\n")

    reachable: list[str] = []
    empty: list[str] = []
    mismatched: list[str] = []
    failed: list[str] = []

    day = start
    while day <= end:
        iso = day.isoformat()
        try:
            rows, totals = tbx.report(
                iso, iso,
                attributes=["date"],
                metrics=["imps_sum", "dsp_price_sum"],
            )
        except Exception as exc:                       # noqa: BLE001
            failed.append(iso)
            print(f"  {iso}  ERROR  {type(exc).__name__}: {str(exc)[:90]}")
            day += _td(days=1)
            continue

        own = [r for r in rows
               if str(r.get("date") or r.get("report_date") or "")[:10] == iso]
        other = len(rows) - len(own)
        imps = totals.get("imps_sum") if isinstance(totals, dict) else None
        gross = totals.get("dsp_price_sum") if isinstance(totals, dict) else None

        if own:
            reachable.append(iso)
            print(f"  {iso}  OK     imps={imps or '?'}  gross={gross or '?'}")
        elif other:
            # The platform answered a single-day request with other dates.
            # That is the anchored-to-today reading showing itself.
            mismatched.append(iso)
            print(f"  {iso}  DRIFT  {other} row(s) came back for other dates "
                  f"— the window is NOT anchored to date_to")
        else:
            empty.append(iso)
            print(f"  {iso}  EMPTY  no rows")

        day += _td(days=1)

    print(f"\n  reachable {len(reachable)} · empty {len(empty)} · "
          f"drifted {len(mismatched)} · failed {len(failed)}")

    # An empty day is genuinely ambiguous: the platform drops all-zero rows,
    # so "no revenue that day" and "too old to serve" look identical. What
    # disambiguates is the shape across the range, not any single day.
    if mismatched:
        print("\n  VERDICT: the truncation window is anchored to TODAY.\n"
              "  Days older than it cannot be backfilled from this platform "
              "at all. Whatever is missing needs recovering now, and anything\n"
              "  older than the window is already unrecoverable here.")
        return 1
    if reachable and empty and reachable[0] > empty[-1]:
        print("\n  VERDICT: likely anchored to TODAY — the older end of the "
              "range is empty and the recent end is not.\n"
              "  Confirm against a day you know had revenue before treating "
              "an empty day as a dead one.")
        return 1
    if reachable:
        print("\n  VERDICT: historical days ARE reachable one at a time. The "
              "window is anchored to date_to, not to today,\n"
              "  so a backfill of any age works as long as it is chunked by "
              "day. Record this in teqblaze-new-platform.md §8.1.")
        return 0
    print("\n  VERDICT: nothing came back for any day. Either the account "
          "has no data in this range, or reporting is\n"
          "  scoped away from it. Check --dictionaries and the account's "
          "permissions before concluding anything about truncation.")
    return 1


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--days", type=int, default=3,
                        help="lookback window for analytics probes (default 3)")
    parser.add_argument("--reports", action="store_true",
                        help="also probe every analytics surface (slower)")
    parser.add_argument("--dictionaries", action="store_true",
                        help="also probe the lookup tables")
    parser.add_argument("--diff-shape", metavar="supply:22|demand:91",
                        help="dump one entity's read→write payload diff and exit")
    parser.add_argument("--reach-from", metavar="YYYY-MM-DD",
                        help="probe day-by-day reachability from this date and "
                             "exit — answers whether a backfill of that age is "
                             "possible at all")
    parser.add_argument("--reach-to", metavar="YYYY-MM-DD",
                        help="end of the reachability probe (default: yesterday)")
    parser.add_argument("--json", metavar="PATH", help="write results as JSON")
    parser.add_argument("--login", action="store_true",
                        help="prompt for the TBX password on this terminal instead of "
                             "reading TBX_PASSWORD from the environment")
    parser.add_argument("--email", help="TBX email to use with --login "
                                       "(default: $TBX_EMAIL, else prompted)")
    args = parser.parse_args()

    print(f"Teqblaze new-platform probe — {tbx.TBX_BASE}")

    if args.login:
        try:
            tbx.prompt_for_credentials(args.email)
        except tbx.TbxAuthError as exc:
            print(f"\n{exc}", file=sys.stderr)
            return 2

    if not tbx.configured():
        print("\nTBX_EMAIL / TBX_PASSWORD are not set in this environment.\n"
              "  • Render: pgam-intelligence-scheduler → Environment\n"
              "  • local:  a .env alongside the repo\n"
              "  • cloud Claude sessions: claude.ai/code environment settings\n"
              "  • one-off, nothing persisted: re-run with --login\n"
              "Nothing to probe without them.", file=sys.stderr)
        return 2

    if not tbx.test_connection():
        print("\nAuth failed — see the error above. A 401/403 here is real "
              "signal: rotated password, locked account, or IP allowlist.",
              file=sys.stderr)
        return 1

    if args.diff_shape:
        return diff_shape(args.diff_shape)

    if args.reach_from:
        return reachability(args.reach_from, args.reach_to)

    results: dict = {"base": tbx.TBX_BASE}
    probe_entities(results)
    if args.dictionaries or args.reports:
        probe_dictionaries(results)
    if args.reports:
        probe_reports(results, args.days)

    checks = [v for v in results.values() if isinstance(v, dict) and "ok" in v]
    passed = sum(1 for v in checks if v["ok"])
    print(f"\n── Summary ───────────────────────────────────────────────")
    print(f"  {passed}/{len(checks)} surfaces answered")
    failures = [k for k, v in results.items()
                if isinstance(v, dict) and v.get("ok") is False]
    if failures:
        print(f"  {FAIL} no answer from: {', '.join(failures)}")
        print("  A 403 usually means the account lacks that module, not that "
              "the endpoint is wrong — check GET /permissions.")

    if args.json:
        with open(args.json, "w") as handle:
            json.dump(results, handle, indent=2, default=str)
        print(f"  wrote {args.json}")

    return 0 if passed else 1


if __name__ == "__main__":
    sys.exit(main())
