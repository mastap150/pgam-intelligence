#!/usr/bin/env python3
"""Pull every readable surface of the new Teqblaze platform for analysis.

Companion to `scripts/tbx_probe.py`. The probe answers "does this endpoint
answer at all"; this pulls the actual data and renders it for analysis.

Read-only by construction: it imports nothing from the write path and never
sets TBX_ALLOW_WRITES. Every surface is attempted independently, so a module
the account lacks (403) is recorded as a finding rather than failing the run.

Two output channels, because they have different jobs:

  * **stdout** — a bounded digest: capability matrix, totals, and top-N tables
    per surface. This is what lands in a GitHub Actions job log, which is how
    a Claude Code session reads results back when its sandbox is blocked from
    reaching the platform.
  * **--outdir** — full JSON per surface, for a human or a larger analysis.
    Contains commercial revenue data; treat the artifact accordingly.

Usage
-----
    # last 7 complete days, digest to stdout + JSON to a directory
    python3 scripts/tbx_pull.py --days 7 --outdir /tmp/tbx

    # explicit window, more rows in the log digest
    python3 scripts/tbx_pull.py --date-from 2026-08-01 --date-to 2026-08-18 \
        --log-rows 25 --outdir /tmp/tbx

    # skip the per-source placement walk (one call per supply source)
    python3 scripts/tbx_pull.py --days 3 --max-sources 0

Exit codes: 0 if authentication worked (individual surface failures are
reported, not fatal), 1 if auth failed, 2 if credentials are absent.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import date, timedelta

sys.path.insert(0, __file__.rsplit("/scripts/", 1)[0])

from core import tbx_api as tbx          # noqa: E402
from core import tbx_mgmt as tbm         # noqa: E402

OK, FAIL = "✓", "✗"

# Populated as we go: {surface_name: {"ok", "rows"/"error", ...}}
RESULTS: dict[str, dict] = {}
DATA: dict[str, object] = {}


# ---------------------------------------------------------------------------
# Output helpers
# ---------------------------------------------------------------------------

def heading(text: str) -> None:
    print(f"\n{'═' * 78}\n{text}\n{'═' * 78}")


def section(text: str) -> None:
    print(f"\n── {text} " + "─" * max(0, 74 - len(text)))


def _fmt(value: object) -> str:
    if value is None:
        return "-"
    if isinstance(value, bool):
        return "yes" if value else "no"
    if isinstance(value, float):
        return f"{value:,.2f}"
    if isinstance(value, int):
        return f"{value:,}"
    text = str(value)
    return text if len(text) <= 28 else text[:27] + "…"


def table(rows: list[dict], columns: list[str], limit: int = 15) -> None:
    """Print a fixed-column table. Columns absent from the data are skipped."""
    if not rows:
        print("  (no rows)")
        return
    present = [c for c in columns if any(c in r for r in rows)]
    if not present:
        present = sorted(rows[0].keys())[:8]

    widths = {c: max(len(c), *(len(_fmt(r.get(c))) for r in rows[:limit])) for c in present}
    header = "  " + "  ".join(c.ljust(widths[c]) for c in present)
    print(header)
    print("  " + "  ".join("─" * widths[c] for c in present))
    for row in rows[:limit]:
        print("  " + "  ".join(_fmt(row.get(c)).ljust(widths[c]) for c in present))
    if len(rows) > limit:
        print(f"  … {len(rows) - limit:,} more row(s) — full set in the JSON artifact")


def totals_line(totals: dict) -> None:
    """One-line render of a report's `total` block."""
    if not totals:
        print("  (no totals block returned)")
        return
    interesting = [
        "requests_sum", "responses_sum", "imps_sum", "ssp_price_sum",
        "dsp_price_sum", "profit", "margin", "supply_fill_rate",
        "demand_win_rate", "timeout_rate", "render_rate",
    ]
    parts = [f"{k}={_fmt(totals[k])}" for k in interesting if k in totals]
    print("  TOTALS  " + ("  ".join(parts) if parts else json.dumps(totals, default=str)[:400]))


def record(name: str, value: object, error: str | None = None) -> object:
    """Register a surface's outcome for the capability matrix and the artifact."""
    if error is not None:
        RESULTS[name] = {"ok": False, "error": error[:600]}
        return None
    if isinstance(value, tuple):          # report(): (rows, totals)
        rows, totals = value
        RESULTS[name] = {"ok": True, "rows": len(rows), "has_totals": bool(totals)}
        DATA[name] = {"rows": rows, "totals": totals}
    elif isinstance(value, list):
        RESULTS[name] = {"ok": True, "rows": len(value)}
        DATA[name] = value
    else:
        RESULTS[name] = {"ok": True, "rows": None}
        DATA[name] = value
    return value


def pull(name: str, fn, *args, **kwargs) -> object:
    """Run one read. Never raises — a failure is a finding."""
    try:
        return record(name, fn(*args, **kwargs))
    except Exception as exc:
        detail = f"{type(exc).__name__}: {exc}"
        status = getattr(exc, "status", None)
        hint = ""
        if status == 403:
            hint = "  ← account likely lacks this module; check GET /permissions"
        elif status == 422:
            hint = "  ← payload rejected; the attribute/metric combination may be unsupported"
        print(f"  {FAIL} {name}: {detail[:220]}{hint}")
        record(name, None, error=detail)
        return None


# ---------------------------------------------------------------------------
# Surfaces
# ---------------------------------------------------------------------------

def pull_account() -> None:
    heading("ACCOUNT & VOCABULARY")

    section("permissions")
    perms = pull("permissions", tbx.permissions)
    if perms:
        entries = perms.get("data", perms) if isinstance(perms, dict) else perms
        print(f"  {OK} {len(entries) if hasattr(entries, '__len__') else '?'} permission entries")
        print(f"  {json.dumps(entries, default=str)[:1200]}")

    section("report columns-list (authoritative vocabulary)")
    cols = pull("report_columns", tbx.report_columns)
    if cols:
        data = cols.get("data", {}) if isinstance(cols, dict) else {}
        live_attrs = set((data.get("attributes") or {}).keys()) if isinstance(data.get("attributes"), dict) else set()
        live_metrics = set((data.get("metrics") or {}).keys()) if isinstance(data.get("metrics"), dict) else set()
        print(f"  live: {len(live_attrs)} attributes, {len(live_metrics)} metrics")
        if live_attrs:
            extra = sorted(live_attrs - set(tbx.REPORT_ATTRIBUTES))
            missing = sorted(set(tbx.REPORT_ATTRIBUTES) - live_attrs)
            print(f"  attributes in account but not in our constants: {extra or 'none'}")
            print(f"  attributes in our constants but not in account: {missing or 'none'}")
        if live_metrics:
            extra = sorted(live_metrics - set(tbx.REPORT_METRICS))
            missing = sorted(set(tbx.REPORT_METRICS) - live_metrics)
            print(f"  metrics in account but not in our constants:    {extra or 'none'}")
            print(f"  metrics in our constants but not in account:    {missing or 'none'}")


def pull_entities(max_sources: int) -> None:
    heading("ENTITIES")

    section("supply sources")
    supply = pull("supply_sources", tbm.list_supply_sources,
                  sort=[{"field": "id", "direction": "asc"}]) or []
    table(supply, ["id", "name", "company_name", "type", "integration_type",
                   "placements", "incoming_qps", "qps", "bid_qps", "win_rate",
                   "supply_fill_rate", "srpm", "revenue_yesterday",
                   "revenue_today", "status"], limit=40)

    section("demand sources")
    demand = pull("demand_sources", tbm.list_demand_sources,
                  sort=[{"field": "id", "direction": "asc"}]) or []
    table(demand, ["id", "demand_name", "company_name", "type", "qps_limit",
                   "qps", "bid_qps", "spend_limit", "spend_yesterday",
                   "spend_today", "srpm", "demand_fill_rate", "status"], limit=40)

    section("companies")
    table(pull("companies", tbm.list_companies) or [],
          ["id", "name", "type", "status"], limit=30)

    section("QPS capacity")
    for label, fn in (("supply_qps_capacity", tbm.supply_qps_capacity),
                      ("demand_qps_capacity", tbm.demand_qps_capacity)):
        value = pull(label, fn)
        if value is not None:
            print(f"  {label}: {json.dumps(value, default=str)[:400]}")

    section("filter lists")
    lists = pull("filter_lists", tbm.list_filter_lists) or []
    table(lists, ["id", "name", "record_type", "type", "filtering_node",
                  "is_all_ssp", "is_all_dsp", "status"], limit=30)
    # Value counts matter: an enabled-but-empty block list is a silent no-op.
    for flist in lists[:15]:
        fid = flist.get("id")
        if fid is None:
            continue
        values = pull(f"filter_list_{fid}_values", tbm.get_filter_list_values, int(fid))
        if values is not None:
            print(f"    list {fid} '{flist.get('name')}' "
                  f"({flist.get('type')}/{flist.get('record_type')}): {len(values):,} values"
                  + ("   ← ENABLED BUT EMPTY" if flist.get("status") and not values else ""))

    section("deals")
    table(pull("deals", tbm.list_deals) or [],
          ["id", "name", "type", "cpm", "auction_type", "status"], limit=30)

    section("alerts")
    table(pull("alerts", tbm.list_alerts) or [],
          ["id", "name", "period", "channel", "recipients", "status"], limit=30)

    section("scheduled reports")
    table(pull("scheduled_reports", tbm.list_scheduled_reports) or [],
          ["id", "name", "preset_id", "interval", "timezone"], limit=30)

    if max_sources and supply:
        section(f"placements (first {max_sources} supply sources)")
        all_placements: list[dict] = []
        for src in supply[:max_sources]:
            sid = src.get("id")
            if sid is None:
                continue
            rows = pull(f"placements_supply_{sid}", tbm.list_placements, int(sid))
            for row in (rows or []):
                all_placements.append({**row, "supply_source_id": sid,
                                       "supply_source_name": src.get("name")})
        record("placements_all", all_placements)
        print(f"  {len(all_placements):,} placements across {min(len(supply), max_sources)} sources")
        table(sorted(all_placements, key=lambda r: -(r.get("floor_price") or 0)),
              ["supply_source_id", "supply_source_name", "id", "name", "ad_format",
               "type", "floor_price", "margin_status", "margin_type", "margin_min",
               "margin_max", "size", "status"], limit=40)

        floors = [p.get("floor_price") or 0 for p in all_placements]
        if floors:
            zero = sum(1 for f in floors if not f)
            print(f"\n  floor spread: min ${min(floors):.4f}  max ${max(floors):.4f}  "
                  f"mean ${sum(floors)/len(floors):.4f}  at-zero {zero}/{len(floors)}")
            print("  (a zero floor means no price protection on that placement)")


def pull_reports(df: str, dt: str, log_rows: int) -> None:
    heading(f"REPORT CUTS  ({df} → {dt}, {tbx.DEFAULT_TZ})")

    money = ["imps_sum", "ssp_price_sum", "dsp_price_sum", "profit", "margin"]
    funnel = ["ssp_requests_sum", "requests_sum", "responses_sum", "wins_sum",
              "imps_sum", "supply_fill_rate", "demand_win_rate",
              "demand_bid_rate", "render_rate", "timeout_rate"]
    pricing = ["imps_sum", "avg_supply_bid_floor", "avg_supply_bid_price",
               "avg_demand_bid_floor", "avg_demand_bid_price", "supply_ecpm",
               "demand_ecpm", "margin"]

    cuts: list[tuple[str, list[str], list[str], list[dict] | None]] = [
        ("by_date",              ["date"],                                money + funnel, [{"field": "date", "direction": "asc"}]),
        ("by_supply_source",     ["supply_source"],                       money + ["supply_fill_rate", "supply_win_rate"], [{"field": "dsp_price_sum", "direction": "desc"}]),
        ("by_demand_source",     ["demand_source"],                       money + ["demand_win_rate", "demand_bid_rate", "timeout_rate", "render_rate"], [{"field": "dsp_price_sum", "direction": "desc"}]),
        ("by_supply_x_demand",   ["supply_source", "demand_source"],      money, [{"field": "dsp_price_sum", "direction": "desc"}]),
        ("by_country",           ["country"],                             money + ["demand_win_rate"], [{"field": "dsp_price_sum", "direction": "desc"}]),
        ("by_inventory_key",     ["inventory_key"],                       money, [{"field": "dsp_price_sum", "direction": "desc"}]),
        ("by_placement",         ["placement"],                           money + ["avg_supply_bid_floor", "supply_fill_rate"], [{"field": "dsp_price_sum", "direction": "desc"}]),
        ("by_traffic_x_format",  ["traffic_type", "ad_format"],           money + ["vcr"], [{"field": "dsp_price_sum", "direction": "desc"}]),
        ("by_size",              ["size"],                                money, [{"field": "dsp_price_sum", "direction": "desc"}]),
        ("by_os",                ["os"],                                  money, [{"field": "dsp_price_sum", "direction": "desc"}]),
        ("by_seat",              ["seat"],                                money, [{"field": "dsp_price_sum", "direction": "desc"}]),
        ("by_crid",              ["crid"],                                ["imps_sum", "dsp_price_sum", "ctr"], [{"field": "dsp_price_sum", "direction": "desc"}]),
        ("by_publisher",         ["publisher"],                           money, [{"field": "dsp_price_sum", "direction": "desc"}]),
        ("by_region",            ["region"],                              money, [{"field": "dsp_price_sum", "direction": "desc"}]),
        ("by_deal",              ["supply_deal", "demand_deal"],          money, [{"field": "dsp_price_sum", "direction": "desc"}]),
        ("by_integration_type",  ["supply_integration_type"],             money, [{"field": "dsp_price_sum", "direction": "desc"}]),
        ("pricing_by_supply",    ["supply_source"],                       pricing, [{"field": "imps_sum", "direction": "desc"}]),
        ("funnel_by_demand",     ["demand_source"],                       funnel, [{"field": "requests_sum", "direction": "desc"}]),
        ("sync_by_demand",       ["demand_source"],                       ["requests_sum", "cookie_requests_sum", "dsp_sync_rate", "imps_sum", "dsp_price_sum"], [{"field": "dsp_price_sum", "direction": "desc"}]),
        ("video_by_placement",   ["placement"],                           ["imps_sum", "vcr", "video_events_complete_sum", "dsp_price_sum"], [{"field": "dsp_price_sum", "direction": "desc"}]),
    ]

    for name, attrs, metrics, sort in cuts:
        section(name)
        result = pull(f"report_{name}", tbx.report, df, dt,
                      attributes=attrs, metrics=metrics, sort=sort)
        if result:
            rows, totals = result
            totals_line(totals)
            table(rows, attrs + metrics, limit=log_rows)

    # Hour-of-day, for dayparting. Kept to the last day so the cut stays small.
    section("hourly (last day, dayparting)")
    result = pull("report_hourly", tbx.report, dt, dt,
                  attributes=["date"], metrics=money + ["supply_fill_rate"],
                  date_granularity="hour",
                  sort=[{"field": "date", "direction": "asc"}])
    if result:
        rows, totals = result
        totals_line(totals)
        table(rows, ["date"] + money + ["supply_fill_rate"], limit=24)

    # Metric filters behave like SQL HAVING — prove it works and surface the
    # slices an optimiser would actually target.
    section("HAVING-style filters: material volume, thin margin")
    result = pull("report_thin_margin", tbx.report, df, dt,
                  attributes=["supply_source", "demand_source"],
                  metrics=money,
                  filters={"imps_sum": {"operator": ">", "value": "1000"},
                           "margin": {"operator": "<", "value": "10"}},
                  sort=[{"field": "dsp_price_sum", "direction": "desc"}])
    if result:
        rows, totals = result
        print(f"  {len(rows)} supply×demand pairs over 1k imps with margin under 10%")
        totals_line(totals)
        table(rows, ["supply_source", "demand_source"] + money, limit=log_rows)


def pull_diagnostics(df: str, dt: str, log_rows: int) -> None:
    heading(f"DIAGNOSTICS  ({df} → {dt})")

    section("bids overview — drop reasons")
    for kind in ("incoming", "outgoing", "responses"):
        rows = pull(f"bids_overview_{kind}", tbx.bids_overview, kind,
                    date_from=df, date_to=dt,
                    sort=[{"field": "dropped_count", "direction": "desc"}])
        if rows is not None:
            print(f"\n  {kind}:")
            table(rows, ["date", "supply_source", "placement", "demand_source",
                         "total_count", "valid_count", "dropped_count", "drop_rate"],
                  limit=log_rows)
        details = pull(f"bids_overview_details_{kind}", tbx.bids_overview_details,
                       kind, {"date_from": df, "date_to": dt})
        if details:
            data = details.get("data", details) if isinstance(details, dict) else details
            print(f"  {kind} named drop reasons:")
            if isinstance(data, list):
                table(data, ["reason", "dropped_count"], limit=25)
            else:
                print(f"    {json.dumps(data, default=str)[:600]}")

    section("failure-reasons dictionary")
    table(pull("dict_failure_reasons", tbx.failure_reasons) or [],
          ["id", "name", "key"], limit=40)

    section("scanner settings — which third-party scanners are configured")
    settings = pull("scanner_settings", tbx.scanner_settings)
    if settings is not None:
        rows = settings.get("data", settings) if isinstance(settings, dict) else settings
        if isinstance(rows, list):
            table(rows, ["id", "scanner_id", "name", "key", "type", "status"], limit=30)
            enabled = [r for r in rows if r.get("status")]
            names = ", ".join(f"{r.get('name')}/{r.get('type')}" for r in enabled)
            print(f"  {len(enabled)}/{len(rows)} enabled  ({names or 'none'})")
        else:
            print(f"  {json.dumps(rows, default=str)[:600]}")
    print("  NB: HUMAN is not in this list — it is a separate module. See the")
    print("      human-report section for its volume and charge figures.")

    section("scanner statistics")
    for kind, metrics in (("prebid", ["requests_sum", "blocked_sum", "blocked_rate"]),
                          ("postbid", ["scan_attempts", "scans"])):
        rows = pull(f"scanner_{kind}", tbx.scanner_statistics, kind,
                    date_from=df, date_to=dt,
                    attributes=["supply_source", "scanner_name"], metrics=metrics)
        if rows is not None:
            print(f"\n  {kind}:")
            table(rows, ["supply_source", "scanner_name"] + metrics, limit=log_rows)

    section("traffic logger (sample)")
    rows = pull("traffic_logger", tbx.traffic_logger, per_page=5, max_pages=1)
    if rows:
        print(f"  {len(rows)} samples; keys: {sorted(rows[0].keys())}")


def pull_quality(df: str, dt: str, log_rows: int) -> None:
    heading(f"TRAFFIC QUALITY & SUPPLY CHAIN  ({df} → {dt})")

    section("HUMAN integration settings")
    settings = pull("human_settings", tbx.human_report_settings)
    if settings is not None:
        print(f"  {json.dumps(settings, default=str)[:600]}")
        print("  (if this is empty/disabled, the IVT feed is not live on our account)")

    section("HUMAN risk metrics by inventory")
    risk = ["requests_sum", "mfa_sum", "mfa_rate", "sivt_sum", "sivt_rate",
            "givt_sum", "givt_rate"]
    rows = pull("human_risk_by_inventory", tbx.human_report, "risk-metrics",
                date_from=df, date_to=dt,
                attributes=["inventory_key"], metrics=risk)
    if rows is not None:
        worst = sorted(rows, key=lambda r: -(float(r.get("sivt_rate") or 0)
                                             + float(r.get("mfa_rate") or 0)))
        table(worst, ["inventory_key"] + risk, limit=log_rows)

    section("HUMAN risk metrics by date")
    rows = pull("human_risk_by_date", tbx.human_report, "risk-metrics",
                date_from=df, date_to=dt, attributes=["date"], metrics=risk)
    if rows is not None:
        table(rows, ["date"] + risk, limit=31)

    section("HUMAN traffic report (what HUMAN bills)")
    rows = pull("human_traffic", tbx.human_report, "traffic-report",
                date_from=df, date_to=dt, attributes=["date"],
                metrics=["impressions_sum", "charge_amount_sum"])
    if rows is not None:
        table(rows, ["date", "impressions_sum", "charge_amount_sum"], limit=31)

    section("schain utilisation (live traffic posture)")
    rows = pull("schain_utilisation", tbx.schain_utilisation, df, dt,
                attributes=["supply_source", "demand_source", "in_complete",
                            "out_complete", "in_nodes_attr", "out_nodes_attr",
                            "in_sellers_verified_nodes_attr",
                            "out_sellers_verified_nodes_attr",
                            "in_adstxt_verified_nodes_attr",
                            "out_adstxt_verified_nodes_attr"],
                metrics=["supply_requests", "bid_requests", "imps_sum",
                         "ssp_price_sum", "dsp_price_sum", "srpm"])
    if rows is not None:
        table(rows, ["supply_source", "demand_source", "in_complete", "out_complete",
                     "in_nodes_attr", "in_sellers_verified_nodes_attr",
                     "in_adstxt_verified_nodes_attr", "imps_sum", "dsp_price_sum"],
              limit=log_rows)
        incomplete = [r for r in rows if str(r.get("in_complete")).lower() in ("0", "false", "no")]
        if incomplete:
            spend = sum(float(r.get("dsp_price_sum") or 0) for r in incomplete)
            print(f"\n  ⚠️  {len(incomplete)} row(s) with INCOMPLETE incoming schain, "
                  f"${spend:,.2f} demand spend attached")

    section("sellers validation (platform's own crawl)")
    rows = pull("sellers_validation", tbx.sellers_validation, df, dt,
                attributes=["inventory_key", "seller_domain",
                            "sellers_verification_attr", "adstxt_verification_attr",
                            "domain_node_position", "seller_domain_node_rank_id_attr"],
                metrics=["supply_requests", "imps_sum", "dsp_price_sum", "srpm"])
    if rows is not None:
        table(rows, ["inventory_key", "seller_domain", "sellers_verification_attr",
                     "adstxt_verification_attr", "domain_node_position",
                     "imps_sum", "dsp_price_sum"], limit=log_rows)

    section("ads.txt verification")
    rows = pull("ads_txt_verification", tbx.ads_txt_verification)
    if rows is not None:
        table(rows, ["date", "company", "publisher_id", "domain", "traffic_name",
                     "crawled_domain", "ads_txt_url", "status"], limit=log_rows)


def pull_recon(df: str, dt: str, log_rows: int) -> None:
    heading(f"REVENUE RECON  ({df} → {dt})")

    section("discrepancy report — platform vs partner-reported")
    rows = pull("discrepancy_report", tbx.discrepancy_report, df, dt,
                sort=[{"field": "spend_discrepancy", "direction": "desc"}])
    if rows is not None:
        table(rows, ["date", "company", "source", "source_id", "source_type",
                     "sync_status", "impressions", "impressions_api",
                     "impressions_discrepancy", "spend", "spend_api",
                     "spend_discrepancy"], limit=log_rows)
        synced = [r for r in rows if r.get("sync_status")]
        print(f"\n  {len(synced)}/{len(rows)} rows have a working API sync")
        print("  Rows without one have no partner-side number to compare — "
              "registering their reporting URL is what unlocks automated recon.")


def pull_help_center() -> None:
    """Pull the platform's own docs, so they land in the artifact with the data.

    Specifically covers the Management API space Priyesh pointed at
    (https://ssp-new.pgammedia.com/help-center/management-api) — that UI is a
    front end over `GET /help-center/{space}`, so there is no need for anyone to
    copy a page out of a browser.
    """
    heading("PLATFORM DOCUMENTATION (help centre)")
    for space in ("management-api", "api", "general"):
        docs = pull(f"help_center_{space}", tbx.dump_help_center, space)
        if not docs:
            continue
        ids = docs.get("_article_ids") or []
        print(f"  {OK} space '{space}': {len(ids)} article(s) pulled")
        for aid in ids[:25]:
            art = docs.get(aid) or {}
            body = art.get("data", art) if isinstance(art, dict) else {}
            title = (body.get("title") or body.get("name") or "?") if isinstance(body, dict) else "?"
            print(f"      {aid:>8}  {str(title)[:70]}")
        if len(ids) > 25:
            print(f"      … {len(ids) - 25} more — full bodies in the JSON artifact")


def pull_dictionaries() -> None:
    heading("DICTIONARIES")
    for dict_type in ("countries", "regions", "supply-companies", "demand-companies",
                      "seats", "scanners", "presets", "ivt-reasons",
                      "operation-systems", "banner-sizes", "traffic-type",
                      "ad-format", "verification-list", "seller-domain-node-rank"):
        rows = pull(f"dict_{dict_type}", tbx.dictionary, dict_type)
        if rows is not None:
            preview = ", ".join(str(r.get("name") or r.get("id")) for r in rows[:8])
            print(f"  {OK} {dict_type:26} {len(rows):>5,} entries   {preview}")


# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------

def print_matrix() -> tuple[int, int]:
    heading("CAPABILITY MATRIX")
    passed = sum(1 for v in RESULTS.values() if v["ok"])
    for name, outcome in RESULTS.items():
        if outcome["ok"]:
            rows = outcome.get("rows")
            print(f"  {OK} {name:34} {'' if rows is None else f'{rows:,} rows'}")
        else:
            print(f"  {FAIL} {name:34} {outcome['error'][:150]}")
    print(f"\n  {passed}/{len(RESULTS)} surfaces returned data")

    failures = {n: o["error"] for n, o in RESULTS.items() if not o["ok"]}
    if failures:
        print("\n  Surfaces with no data — each is a finding, not necessarily a bug:")
        for name, error in failures.items():
            print(f"    • {name}: {error[:200]}")
    return passed, len(RESULTS)


def write_step_summary(passed: int, total: int, df: str, dt: str) -> None:
    """Render a short digest into the GitHub Actions run summary, when present."""
    path = os.environ.get("GITHUB_STEP_SUMMARY")
    if not path:
        return
    lines = [
        "# Teqblaze new-platform data pull",
        "",
        f"- Host: `{tbx.TBX_BASE}`",
        f"- Window: `{df}` → `{dt}` ({tbx.DEFAULT_TZ})",
        f"- Surfaces returning data: **{passed}/{total}**",
        "",
        "| Surface | Result |",
        "| --- | --- |",
    ]
    for name, outcome in RESULTS.items():
        if outcome["ok"]:
            rows = outcome.get("rows")
            lines.append(f"| `{name}` | ✓ {'' if rows is None else f'{rows:,} rows'} |")
        else:
            lines.append(f"| `{name}` | ✗ {outcome['error'][:120].replace('|', '/')} |")
    lines += ["", "Full JSON per surface is attached as a run artifact.",
              "The complete digest is in the job log."]
    try:
        with open(path, "a") as handle:
            handle.write("\n".join(lines) + "\n")
    except OSError as exc:
        print(f"  (could not write step summary: {exc})")


def write_outdir(outdir: str, df: str, dt: str) -> None:
    os.makedirs(outdir, exist_ok=True)
    for name, payload in DATA.items():
        with open(os.path.join(outdir, f"{name}.json"), "w") as handle:
            json.dump(payload, handle, indent=2, default=str)
    with open(os.path.join(outdir, "_manifest.json"), "w") as handle:
        json.dump({"base": tbx.TBX_BASE, "timezone": tbx.DEFAULT_TZ,
                   "date_from": df, "date_to": dt, "surfaces": RESULTS},
                  handle, indent=2, default=str)
    print(f"\n  wrote {len(DATA) + 1} JSON file(s) to {outdir}")


# ---------------------------------------------------------------------------

def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--days", type=int, default=7,
                        help="lookback window ending yesterday (default 7)")
    parser.add_argument("--date-from", help="explicit start date (YYYY-MM-DD)")
    parser.add_argument("--date-to", help="explicit end date (YYYY-MM-DD)")
    parser.add_argument("--log-rows", type=int, default=15,
                        help="rows per table in the stdout digest (default 15)")
    parser.add_argument("--max-sources", type=int, default=25,
                        help="supply sources to walk for placements; 0 to skip (default 25)")
    parser.add_argument("--outdir", help="directory for full per-surface JSON")
    parser.add_argument("--skip", default="",
                        help="comma-separated groups to skip: account, entities, "
                             "reports, diagnostics, quality, recon, dictionaries, docs")
    parser.add_argument("--login", action="store_true",
                        help="prompt for the TBX password on this terminal instead of "
                             "reading TBX_PASSWORD from the environment. Nothing is "
                             "written to disk but the short-lived JWT; use this for a "
                             "one-off pull rather than provisioning a secret.")
    parser.add_argument("--email", help="TBX email to use with --login "
                                       "(default: $TBX_EMAIL, else prompted)")
    args = parser.parse_args()

    if args.date_from and args.date_to:
        df, dt = args.date_from, args.date_to
    else:
        end = date.today() - timedelta(days=1)
        start = end - timedelta(days=max(args.days - 1, 0))
        df, dt = start.isoformat(), end.isoformat()

    skip = {s.strip() for s in args.skip.split(",") if s.strip()}

    print(f"Teqblaze new-platform data pull")
    print(f"  host     {tbx.TBX_BASE}")
    print(f"  window   {df} → {dt}  ({tbx.DEFAULT_TZ})")
    print(f"  mode     READ-ONLY (write path not imported for any mutation)")

    if args.login:
        try:
            tbx.prompt_for_credentials(args.email)
        except tbx.TbxAuthError as exc:
            print(f"\n{exc}", file=sys.stderr)
            return 2
    if not tbx.configured():
        print("\nTBX_EMAIL / TBX_PASSWORD are not set. Nothing to pull.\n"
              "Either set them in the environment (or a local .env), or re-run "
              "with --login to be prompted for the password on this terminal.",
              file=sys.stderr)
        return 2
    if not tbx.test_connection():
        print("\nAuthentication failed — a 401/403 here is real signal: rotated "
              "password, locked account, or IP allowlist.", file=sys.stderr)
        return 1

    if "account" not in skip:
        pull_account()
    if "entities" not in skip:
        pull_entities(args.max_sources)
    if "reports" not in skip:
        pull_reports(df, dt, args.log_rows)
    if "diagnostics" not in skip:
        pull_diagnostics(df, dt, args.log_rows)
    if "quality" not in skip:
        pull_quality(df, dt, args.log_rows)
    if "recon" not in skip:
        pull_recon(df, dt, args.log_rows)
    if "dictionaries" not in skip:
        pull_dictionaries()
    if "docs" not in skip:
        pull_help_center()

    passed, total = print_matrix()
    if args.outdir:
        write_outdir(args.outdir, df, dt)
    write_step_summary(passed, total, df, dt)

    # Surface failures are findings, not build failures — the account may
    # simply not license a module. Only auth failure is fatal, handled above.
    return 0


if __name__ == "__main__":
    sys.exit(main())
