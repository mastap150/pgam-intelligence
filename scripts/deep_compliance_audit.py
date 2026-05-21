#!/usr/bin/env python3
"""
scripts/deep_compliance_audit.py

One-shot deep supply-path audit on the top revenue-driving inventory
across ALL active SSPs (PubMatic, Magnite, Smaato, Unruly, Sovrn, Zeta,
LoopMe, TripleLift, Sharethrough, AppNexus). For each entity (app
bundle or domain) it:

  1. Pulls per-entity per-SSP revenue from LL stats (BUNDLE,DEMAND_PARTNER
     and DOMAIN,DEMAND_PARTNER over trailing 7d)
  2. Classifies each demand_name to a registry SSP via ssp_registry
  3. Ranks entities by combined revenue across the registry SSPs
  4. For each top entity:
     a. If a bundle, resolves the developer domain via the new
        play_store_resolver (heuristic + Play Store HTML)
     b. Fetches ads.txt (or app-ads.txt with ads.txt fallback)
     c. Cross-checks the pgamssp.com DIRECT line and seller_id against
        PGAM's sellers.json domain mapping
     d. Validates the canonical RESELLER line for every SSP observed
        monetizing that entity in trailing 7d
  5. Scores 0..100 per entity
  6. Emits a ranked report + posts a Slack digest to
     COMPLIANCE_SLACK_WEBHOOK if set (else SLACK_WEBHOOK).

This script is a one-shot operational audit — the daily run via
agents/compliance/runner.py (Phase 5.1) does the same checks across
the full entity universe and persists findings to Neon.

Run:
    python3 scripts/deep_compliance_audit.py [--top N] [--no-slack]
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

# Make repo root importable when run from scripts/
_REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_REPO_ROOT))

from dotenv import load_dotenv  # noqa: E402

load_dotenv(dotenv_path=_REPO_ROOT / ".env", override=True)

import os  # noqa: E402

import requests  # noqa: E402

from core.api import fetch, n_days_ago, sf, today  # noqa: E402
from agents.compliance.crawlers.adstxt import (  # noqa: E402
    fetch_adstxt, fetch_adstxt_merged, fetch_adstxt_with_fallback,
)
from agents.compliance.crawlers.sellersjson import (  # noqa: E402
    fetch_pgam_sellers_json,
)
from agents.compliance.ssp_registry import (  # noqa: E402
    PHASE_2_SSP_EXPECTATIONS,
    classify_demand_name,
)
from agents.enrichment.play_store_resolver import resolve_bundle  # noqa: E402

LOOKBACK_DAYS = 7

# ─── Scoring ────────────────────────────────────────────────────────────────
# Severity weights for the per-entity score (max 100).
W_PGAM_MISSING        = 30   # no pgamssp.com line at all
W_PGAM_WRONG_SELLER   = 20   # line present, seller_id doesn't match sellers.json
W_PGAM_UNKNOWN_SEAT   = 25   # line present, seller_id not in PGAM registry at all
W_PGAM_WRONG_TYPE     = 15   # right seller_id but not DIRECT
W_SSP_MISSING         = 10   # per missing SSP RESELLER line (capped)
W_SSP_WRONG_SELLER    =  8   # per SSP wrong seller_id
W_FILE_UNREACHABLE    = 25   # ads.txt + app-ads.txt both unreachable
W_BUNDLE_UNRESOLVED   =  5   # couldn't find dev_domain (info-level)


def _norm(s: str | None) -> str:
    return (s or "").strip().lower()


def _classify_kind(value: str) -> str:
    """app vs domain."""
    if value.startswith("com.") or value.replace(".", "").isdigit():
        return "app"
    # numeric iOS App Store IDs
    if value.isdigit() and len(value) >= 6:
        return "app"
    return "domain"


# ─── Pull data ──────────────────────────────────────────────────────────────


def pull_inventory_universe() -> dict[str, dict]:
    """Pull BUNDLE×DEMAND and DOMAIN×DEMAND, aggregate per-entity per-SSP."""
    end = today()
    start = n_days_ago(LOOKBACK_DAYS - 1)
    print(f"[audit] pulling LL stats {start} → {end}", flush=True)
    bundle_rows = fetch("BUNDLE,DEMAND_PARTNER",
                        ["GROSS_REVENUE", "IMPRESSIONS"], start, end)
    print(f"[audit]   bundle rows: {len(bundle_rows):,}", flush=True)
    domain_rows = fetch("DOMAIN,DEMAND_PARTNER",
                        ["GROSS_REVENUE", "IMPRESSIONS"], start, end)
    print(f"[audit]   domain rows: {len(domain_rows):,}", flush=True)

    # entity_key → {kind, value, total_rev, imps, ssps: {ssp_key: rev}}
    entities: dict[str, dict] = {}

    def _ingest(rows: list, kind: str, value_keys: tuple) -> None:
        for r in rows:
            value = None
            for k in value_keys:
                if r.get(k):
                    value = str(r[k]).strip()
                    if kind == "domain":
                        value = _norm(value)
                    break
            if not value:
                continue
            demand = (r.get("DEMAND_PARTNER_NAME") or r.get("DEMAND_PARTNER")
                      or r.get("demand_partner") or "")
            ssp = classify_demand_name(demand)
            rev = sf(r.get("GROSS_REVENUE"))
            imps = sf(r.get("IMPRESSIONS"))
            if rev <= 0:
                continue
            key = f"{'app' if kind == 'app' else 'dom'}:{value}"
            ent = entities.setdefault(key, {
                "kind": kind,
                "value": value,
                "total_rev": 0.0,
                "imps": 0,
                "ssps": defaultdict(float),
                "unclassified_demand_rev": 0.0,
            })
            ent["total_rev"] += rev
            ent["imps"] += imps
            if ssp is not None:
                ent["ssps"][ssp.ssp_key] += rev
            else:
                ent["unclassified_demand_rev"] += rev

    _ingest(bundle_rows, "app",    ("BUNDLE", "bundle"))
    _ingest(domain_rows, "domain", ("DOMAIN", "domain"))
    return entities


# ─── Per-entity validate ────────────────────────────────────────────────────


def build_pgam_registry(sellers_payload: dict) -> tuple[dict[str, str], dict[str, dict]]:
    """Return (domain → expected_seller_id, seller_id → entry)."""
    domain_to_seat: dict[str, str] = {}
    seat_to_entry: dict[str, dict] = {}
    for s in (sellers_payload.get("sellers") or []):
        sid = str(s.get("seller_id") or "").strip()
        if not sid:
            continue
        seat_to_entry[sid] = {
            "name":        s.get("name"),
            "domain":      _norm(s.get("domain") or ""),
            "seller_type": (s.get("seller_type") or "").upper().strip(),
        }
        dom = _norm(s.get("domain") or "")
        if dom and s.get("seller_type") in ("PUBLISHER", "BOTH"):
            domain_to_seat[dom] = sid
    return domain_to_seat, seat_to_entry


def audit_entity(
    entity: dict,
    *,
    domain_to_seat: dict[str, str],
    seat_to_entry: dict[str, dict],
) -> dict[str, Any]:
    """Run the full compliance audit for one entity. Returns a result dict."""
    kind = entity["kind"]
    value = entity["value"]
    key = f"{'app' if kind == 'app' else 'dom'}:{value}"

    findings: list[dict] = []
    audit_host = None
    audit_method = None
    use_app_ads = False

    if kind == "domain":
        audit_host = value
        audit_method = "domain_direct"
    else:
        rr = resolve_bundle(value)
        audit_host = rr.dev_domain
        audit_method = rr.method
        use_app_ads = True
        if not audit_host:
            findings.append({
                "severity": "info",
                "check": "bundle.dev_domain_unresolved",
                "detail": {"bundle": value},
            })

    fetch_status = None
    pgam_lines: list = []
    all_lines: list = []
    files_seen: list[str] = []
    if audit_host:
        # Use the production crawler's merged fetcher — same logic as the
        # daily agent. Tries ads.txt + app-ads.txt, falls back through
        # HTTP / browser-UA / parent-domain. A publisher counts as
        # reachable if ANY combination of those succeeds.
        af = fetch_adstxt_merged(key, audit_host)
        fetch_status = af.http_status
        all_lines = af.lines
        pgam_lines = [ln for ln in af.lines if ln.domain == "pgamssp.com"]
        files_seen = [v.strip() for v in (af.variant or "").split("+") if v.strip()]
        if af.http_status != 200:
            findings.append({
                "severity": "high",
                "check": "adstxt.file_unreachable",
                "detail": {
                    "audit_host": audit_host,
                    "http_status": af.http_status,
                    "error": af.error,
                    "tried": "ads.txt + app-ads.txt × HTTPS/HTTP/browser-UA/parent",
                },
            })

    # ── PGAM line check ──────────────────────────────────────────────────────
    expected_seat = None
    if audit_host:
        expected_seat = domain_to_seat.get(_norm(audit_host))

    if audit_host and fetch_status == 200:
        if not pgam_lines:
            findings.append({
                "severity": "critical",
                "check": "pgam.direct_missing",
                "detail": {
                    "audit_host": audit_host,
                    "expected_seller_id": expected_seat,
                },
            })
        else:
            seat_match = None
            if expected_seat:
                seat_match = next(
                    (ln for ln in pgam_lines if ln.account_id == expected_seat),
                    None,
                )
            observed_seats = [{
                "seller_id": ln.account_id,
                "relationship": ln.relationship,
                "registry": seat_to_entry.get(ln.account_id),
            } for ln in pgam_lines]
            if expected_seat and seat_match:
                if not any(ln.relationship == "DIRECT" for ln in [seat_match]):
                    findings.append({
                        "severity": "high",
                        "check": "pgam.wrong_type",
                        "detail": {"seller_id": expected_seat,
                                   "observed_relationship": seat_match.relationship},
                    })
                # else PGAM check passes
            elif expected_seat and not seat_match:
                # We have an expected seat but it's not on their ads.txt;
                # they're using a different PGAM seat (maybe intermediary)
                tiers = [s["registry"]["seller_type"]
                         if s["registry"] else "unknown"
                         for s in observed_seats]
                findings.append({
                    "severity": "critical" if "unknown" in tiers else "high",
                    "check": ("pgam.unknown_seat" if "unknown" in tiers
                              else "pgam.wrong_seat"),
                    "detail": {
                        "expected_seller_id": expected_seat,
                        "observed_seats": observed_seats,
                    },
                })
            elif not expected_seat:
                # No expected seat (entity isn't in sellers.json by domain).
                # Verify whatever's there is at least a known PGAM seat.
                unknown = [s for s in observed_seats
                           if s["registry"] is None]
                if unknown:
                    findings.append({
                        "severity": "critical",
                        "check": "pgam.unknown_seat",
                        "detail": {"observed_seats": observed_seats},
                    })
                # else: PGAM seat is in registry — provisional pass
                #       (this is the "comes-via-aggregator" case)

    # ── Per-SSP RESELLER line checks ─────────────────────────────────────────
    ssp_revenues = entity["ssps"]
    if audit_host and fetch_status == 200:
        for exp in PHASE_2_SSP_EXPECTATIONS:
            ssp_rev = ssp_revenues.get(exp.ssp_key, 0.0)
            if ssp_rev <= 0:
                continue
            ssp_lines = [ln for ln in all_lines
                         if ln.domain == exp.ads_txt_domain.lower()]
            if not ssp_lines:
                findings.append({
                    "severity": "critical",
                    "check": f"ssp.{exp.ssp_key}.reseller_missing",
                    "detail": {
                        "ssp_domain":   exp.ads_txt_domain,
                        "expected_line": (
                            f"{exp.ads_txt_domain}, {exp.account_id}, "
                            f"RESELLER" +
                            (f", {exp.cert_authority}" if exp.cert_authority else "")
                        ),
                        "ssp_revenue_7d": round(ssp_rev, 2),
                    },
                })
                continue
            matching = [ln for ln in ssp_lines if ln.account_id == exp.account_id]
            if not matching:
                findings.append({
                    "severity": "critical",
                    "check": f"ssp.{exp.ssp_key}.wrong_seller",
                    "detail": {
                        "expected_account_id": exp.account_id,
                        "observed_account_ids": sorted({
                            ln.account_id for ln in ssp_lines
                        }),
                        "ssp_revenue_7d": round(ssp_rev, 2),
                    },
                })
                continue
            if not any(ln.relationship == "RESELLER" for ln in matching):
                findings.append({
                    "severity": "high",
                    "check": f"ssp.{exp.ssp_key}.wrong_type",
                    "detail": {
                        "expected": "RESELLER",
                        "observed_relationships": sorted({
                            ln.relationship for ln in matching
                        }),
                        "ssp_revenue_7d": round(ssp_rev, 2),
                    },
                })

    # ── Score ────────────────────────────────────────────────────────────────
    score = 100
    SSP_PENALTY_CAP = 50  # Don't let SSP misses zero out the score alone
    ssp_penalty = 0
    for f in findings:
        check = f["check"]
        if check == "pgam.direct_missing":
            score -= W_PGAM_MISSING
        elif check == "pgam.wrong_seat":
            score -= W_PGAM_WRONG_SELLER
        elif check == "pgam.unknown_seat":
            score -= W_PGAM_UNKNOWN_SEAT
        elif check == "pgam.wrong_type":
            score -= W_PGAM_WRONG_TYPE
        elif check.startswith("ssp.") and check.endswith(".reseller_missing"):
            ssp_penalty += W_SSP_MISSING
        elif check.startswith("ssp.") and check.endswith(".wrong_seller"):
            ssp_penalty += W_SSP_WRONG_SELLER
        elif check == "adstxt.file_unreachable":
            score -= W_FILE_UNREACHABLE
        elif check == "bundle.dev_domain_unresolved":
            score -= W_BUNDLE_UNRESOLVED
    score -= min(ssp_penalty, SSP_PENALTY_CAP)
    score = max(0, score)

    return {
        "key":              key,
        "kind":             kind,
        "value":            value,
        "audit_host":       audit_host,
        "audit_method":     audit_method,
        "fetch_status":     fetch_status,
        "files_seen":       files_seen,
        "total_rev":        round(entity["total_rev"], 2),
        "imps":             entity["imps"],
        "ssps_observed":    {k: round(v, 2) for k, v in ssp_revenues.items()},
        "expected_seat":    expected_seat,
        "observed_pgam":    [{"sid": ln.account_id, "rel": ln.relationship}
                             for ln in pgam_lines],
        "findings":         findings,
        "score":            score,
    }


# ─── Pattern clustering ─────────────────────────────────────────────────────


def _cluster_wrong_seller_patterns(results: list[dict]) -> list[dict]:
    """Group entities by (ssp, observed_account_id) to surface shared-template
    misconfigurations.

    Example: 22 entities all declare zeta at seat=503 instead of our seat=748.
    Almost certainly one upstream operator distributing a stale ads.txt
    template. Surfacing the cluster lets one outreach fix all N publishers.

    Returns a list of cluster dicts, each:
      {"ssp": ssp_key, "observed_account_id": "503",
       "expected_account_id": "748", "entities": [{"value": ..., "rev_7d": ...}],
       "total_rev_7d": float}
    Sorted by total_rev_7d desc; only clusters with ≥3 entities are returned.
    """
    from collections import defaultdict
    by_key: dict[tuple[str, str], dict] = defaultdict(
        lambda: {"entities": [], "expected": None}
    )
    for r in results:
        for f in r["findings"]:
            if not f["check"].startswith("ssp.") or "wrong_seller" not in f["check"]:
                continue
            ssp = f["check"].split(".")[1]
            d = f.get("detail", {})
            expected = d.get("expected_account_id")
            observed_list = d.get("observed_account_ids", []) or []
            for observed in observed_list:
                key = (ssp, str(observed))
                by_key[key]["entities"].append({
                    "entity":     r["value"],
                    "kind":       r["kind"],
                    "rev_7d":     r["total_rev"],
                    "ssp_rev_7d": r["ssps_observed"].get(ssp, 0),
                })
                by_key[key]["expected"] = expected

    clusters: list[dict] = []
    for (ssp, observed), payload in by_key.items():
        ents = payload["entities"]
        if len(ents) < 3:
            continue
        clusters.append({
            "ssp": ssp,
            "observed_account_id": observed,
            "expected_account_id": payload["expected"],
            "entity_count": len(ents),
            "total_rev_7d": sum(e["rev_7d"] for e in ents),
            "ssp_rev_7d":   sum(e["ssp_rev_7d"] for e in ents),
            "entities":     sorted(ents, key=lambda x: -x["rev_7d"])[:10],
        })
    clusters.sort(key=lambda c: (-c["entity_count"], -c["total_rev_7d"]))
    return clusters


# ─── Slack render ───────────────────────────────────────────────────────────


def _slack_blocks(
    results: list[dict],
    summary: dict,
    *,
    ssp_scorecards: dict[str, list[dict]] | None = None,
    pattern_clusters: list[dict] | None = None,
) -> list[dict]:
    """Block Kit payload — header, partner rollup, top criticals, per-SSP
    scorecards, pattern clusters."""
    today_iso = date.today().isoformat()
    ssp_scorecards = ssp_scorecards or {}
    pattern_clusters = pattern_clusters or []

    header_line = (
        f":mag_right: *Compliance daily — {today_iso}*\n"
        f"_Revenue-priority audit of top {summary['top_n']} inventory across "
        f"all 10 active SSPs._"
    )
    stats = (
        f"• {summary['top_n']} entities scanned "
        f"({summary['domains']} domains · {summary['apps']} apps; "
        f"{summary['apps_resolved']} resolved, "
        f"{summary['apps_unresolved']} unresolved)\n"
        f"• ${summary['total_rev']:,.0f} combined trailing-7d revenue "
        f"({summary.get('audited_rev_coverage_pct', 0):.0f}% of "
        f"${summary.get('universe_total_rev_through_registered_ssps', 0):,.0f} "
        f"flowing through the 10 registered SSPs)\n"
        f"• {summary['critical_count']} critical · "
        f"{summary['high_count']} high · "
        f"avg score *{summary['avg_score']:.0f}/100*"
    )

    blocks: list[dict] = [
        {"type": "section",
         "text": {"type": "mrkdwn", "text": header_line}},
        {"type": "section",
         "text": {"type": "mrkdwn", "text": stats}},
        {"type": "divider"},
    ]

    # Lowest scores (max revenue impact + most issues)
    by_score = sorted(results, key=lambda r: (r["score"], -r["total_rev"]))[:10]
    if by_score:
        lines = []
        for r in by_score:
            label = (("app " if r["kind"] == "app" else "")
                     + f"`{r['value'][:36]}`")
            issues = []
            for f in r["findings"][:3]:
                issues.append(f["check"].split(".")[-1] if "." in f["check"] else f["check"])
            issue_summary = ", ".join(issues) + (
                f" +{len(r['findings']) - 3}" if len(r["findings"]) > 3 else "")
            lines.append(
                f"• *{r['score']}/100*  {label}  ·  "
                f"${r['total_rev']:,.0f}/7d  ·  _{issue_summary}_"
            )
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn",
                     "text": ":rotating_light: *Lowest-scoring revenue-driving inventory*\n"
                             + "\n".join(lines)},
        })

    # SSP gap heatmap — per SSP, count of entities with missing reseller line
    ssp_gaps = defaultdict(lambda: {"missing": 0, "wrong": 0, "rev": 0.0})
    for r in results:
        for f in r["findings"]:
            ck = f["check"]
            if not ck.startswith("ssp."):
                continue
            ssp_key = ck.split(".")[1]
            if ck.endswith("reseller_missing"):
                ssp_gaps[ssp_key]["missing"] += 1
                ssp_gaps[ssp_key]["rev"] += f["detail"].get("ssp_revenue_7d", 0)
            elif ck.endswith("wrong_seller"):
                ssp_gaps[ssp_key]["wrong"] += 1
                ssp_gaps[ssp_key]["rev"] += f["detail"].get("ssp_revenue_7d", 0)
    if ssp_gaps:
        lines = []
        for ssp_key, g in sorted(ssp_gaps.items(),
                                  key=lambda x: -x[1]["rev"]):
            lines.append(
                f"• *{ssp_key}* — {g['missing']} missing, "
                f"{g['wrong']} wrong  ·  "
                f"${g['rev']:,.0f}/7d at risk"
            )
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn",
                     "text": ":pushpin: *SSP reseller-line gaps* "
                             "(entities monetizing but not declaring)\n"
                             + "\n".join(lines)},
        })

    # Top revenue critical PGAM issues
    pgam_criticals = [
        r for r in results
        if any(f["check"].startswith("pgam.")
               and f["severity"] == "critical" for f in r["findings"])
    ]
    pgam_criticals.sort(key=lambda r: -r["total_rev"])
    if pgam_criticals:
        lines = []
        for r in pgam_criticals[:8]:
            label = (f"app `{r['value'][:34]}`" if r["kind"] == "app"
                     else f"`{r['value'][:34]}`")
            pgam_check = next((f for f in r["findings"]
                               if f["check"].startswith("pgam.")
                               and f["severity"] == "critical"), None)
            verdict = pgam_check["check"].split(".")[-1] if pgam_check else "?"
            lines.append(
                f"• {label}  ·  ${r['total_rev']:,.0f}/7d  ·  _{verdict}_"
            )
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn",
                     "text": ":warning: *PGAM line broken on revenue-driving inventory*\n"
                             + "\n".join(lines)},
        })

    # Pattern clusters — likely shared-template misconfigurations.
    # Single outreach fixes N publishers, so this is the highest-leverage section.
    if pattern_clusters:
        cluster_lines = []
        for c in pattern_clusters[:5]:
            ents = c["entities"][:3]
            ent_preview = ", ".join(f"`{e['entity'][:24]}`" for e in ents)
            extra = (f" +{c['entity_count']-3}"
                      if c["entity_count"] > 3 else "")
            cluster_lines.append(
                f"• *{c['ssp']}* — {c['entity_count']} entities share seat "
                f"`{c['observed_account_id']}` (expected `{c['expected_account_id']}`)  ·  "
                f"${c['total_rev_7d']:,.0f}/7d in affected inventory\n"
                f"     {ent_preview}{extra}"
            )
        blocks.append({"type": "divider"})
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn",
                     "text": ":dna: *Shared-template clusters* "
                             "(one outreach fixes N publishers)\n"
                             + "\n".join(cluster_lines)},
        })

    # Per-SSP scorecard sections (one section per SSP that has ≥3 entities).
    active_ssps = [(k, v) for k, v in ssp_scorecards.items() if len(v) >= 3]
    active_ssps.sort(key=lambda kv: -sum(e["ssp_rev_7d"] for e in kv[1]))
    if active_ssps:
        blocks.append({"type": "divider"})
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn",
                     "text": f":bookmark_tabs: *Per-SSP scorecards* "
                             f"(top {len(active_ssps[0][1])} entities per SSP)"},
        })
        for ssp_key, entries in active_ssps[:8]:  # cap to 8 SSPs to keep digest scannable
            total_ssp_rev = sum(e["ssp_rev_7d"] for e in entries)
            problem_entries = sum(1 for e in entries if e["verdict"] != "ok")
            verdict_emoji = {
                "ok": ":white_check_mark:",
                "missing": ":x:",
                "wrong_seller": ":warning:",
                "wrong_type": ":small_orange_diamond:",
            }
            top_lines = []
            for e in entries[:8]:
                head = f"app `{e['entity'][:28]}`" if e["kind"] == "app" else f"`{e['entity'][:28]}`"
                top_lines.append(
                    f"  {verdict_emoji.get(e['verdict'], '·')} "
                    f"{head} — ${e['ssp_rev_7d']:,.0f}/7d  _{e['verdict']}_"
                )
            blocks.append({
                "type": "section",
                "text": {"type": "mrkdwn",
                         "text": (
                             f"*{ssp_key.upper()}*  ·  {len(entries)} entities  ·  "
                             f"${total_ssp_rev:,.0f}/7d  ·  "
                             f"{problem_entries} non-compliant\n"
                             + "\n".join(top_lines)
                         )},
            })

    blocks.append({"type": "context", "elements": [
        {"type": "mrkdwn",
         "text": (":robot_face: Daily deep audit via "
                  "`scripts/deep_compliance_audit.py`. Full JSON archived "
                  "as a GH Actions artifact. To switch to the "
                  "persistence-backed daily agent (Neon-backed history, "
                  "/admin/compliance dashboard, auto-resolve), flip "
                  "`PGAM_COMPLIANCE_ENABLED=1` in Render.")}
    ]})
    return blocks


def post_to_slack(blocks: list[dict], fallback: str) -> bool:
    """POST to COMPLIANCE_SLACK_WEBHOOK if set, else SLACK_WEBHOOK."""
    url = (os.environ.get("COMPLIANCE_SLACK_WEBHOOK")
           or os.environ.get("SLACK_WEBHOOK"))
    if not url:
        print("[audit] no SLACK_WEBHOOK or COMPLIANCE_SLACK_WEBHOOK; skipping post",
              flush=True)
        return False
    try:
        resp = requests.post(
            url,
            json={"text": fallback, "blocks": blocks},
            headers={"Content-Type": "application/json"},
            timeout=10,
        )
        resp.raise_for_status()
        print(f"[audit] Slack post OK ({resp.status_code})", flush=True)
        return True
    except Exception as exc:
        print(f"[audit] Slack post FAILED: {exc}", flush=True)
        return False


# ─── Main ───────────────────────────────────────────────────────────────────


def main(argv=None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--top", type=int, default=200,
                        help="Top N entities by combined revenue (default 200)")
    parser.add_argument("--no-slack", action="store_true",
                        help="Skip the Slack post; just print + save JSON")
    parser.add_argument("--per-ssp-top", type=int, default=15,
                        help="Top N entities per SSP scorecard (default 15)")
    parser.add_argument("--out", default="/tmp/deep_compliance_audit.json")
    args = parser.parse_args(argv)

    print("[audit] start", flush=True)
    universe = pull_inventory_universe()

    # Rank by combined revenue across the registry SSPs.
    def _entity_score(e: dict) -> float:
        return sum(e["ssps"].values())
    ranked = sorted(universe.values(), key=lambda e: -_entity_score(e))
    audited_entities = [e for e in ranked if _entity_score(e) > 0]
    top = audited_entities[: args.top]
    universe_total_ssp_rev = sum(_entity_score(e) for e in audited_entities)
    audited_total_ssp_rev  = sum(_entity_score(e) for e in top)
    coverage_pct = (audited_total_ssp_rev / universe_total_ssp_rev * 100
                     if universe_total_ssp_rev > 0 else 0)
    print(f"[audit] universe={len(universe)} top={len(top)} "
          f"coverage={coverage_pct:.1f}% (${audited_total_ssp_rev:,.0f} / "
          f"${universe_total_ssp_rev:,.0f})", flush=True)

    print("[audit] fetching PGAM sellers.json", flush=True)
    payload = fetch_pgam_sellers_json()
    domain_to_seat, seat_to_entry = build_pgam_registry(payload)
    print(f"[audit]   {len(domain_to_seat)} publisher-domain → seat mappings, "
          f"{len(seat_to_entry)} total seats", flush=True)

    print(f"[audit] auditing top {len(top)} entities in parallel...",
          flush=True)
    results = []
    with ThreadPoolExecutor(max_workers=6) as pool:
        futs = {pool.submit(audit_entity, e,
                            domain_to_seat=domain_to_seat,
                            seat_to_entry=seat_to_entry): e for e in top}
        for fut in as_completed(futs):
            try:
                results.append(fut.result())
            except Exception as exc:
                e = futs[fut]
                print(f"[audit] error on {e.get('value')}: {exc}", flush=True)

    # Sort results by total revenue desc for the report
    results.sort(key=lambda r: -r["total_rev"])

    # Summary
    apps_results = [r for r in results if r["kind"] == "app"]
    apps_resolved = sum(1 for r in apps_results if r["audit_host"])
    apps_unresolved = sum(1 for r in apps_results
                          if r["audit_method"] in ("unresolved",)
                          or r["audit_host"] is None)

    critical_count = sum(
        1 for r in results for f in r["findings"] if f["severity"] == "critical"
    )
    high_count = sum(
        1 for r in results for f in r["findings"] if f["severity"] == "high"
    )

    summary = {
        "top_n":           len(results),
        "domains":         sum(1 for r in results if r["kind"] == "domain"),
        "apps":            len(apps_results),
        "apps_resolved":   apps_resolved,
        "apps_unresolved": apps_unresolved,
        "total_rev":       sum(r["total_rev"] for r in results),
        "universe_size":   len(audited_entities),
        "universe_total_rev_through_registered_ssps": round(universe_total_ssp_rev, 2),
        "audited_rev_coverage_pct": round(coverage_pct, 1),
        "critical_count":  critical_count,
        "high_count":      high_count,
        "avg_score":       (sum(r["score"] for r in results) / len(results))
                            if results else 0,
        "ran_at":          datetime.now(timezone.utc).isoformat(),
    }

    # Build per-SSP scorecards: for each SSP in the registry, the top N entities
    # by THAT SSP's revenue + their compliance status against the SSP's required line.
    ssp_scorecards: dict[str, list[dict]] = {}
    for exp in PHASE_2_SSP_EXPECTATIONS:
        entries = []
        for r in results:
            ssp_rev = r["ssps_observed"].get(exp.ssp_key, 0.0)
            if ssp_rev <= 0:
                continue
            verdict = "ok"
            for f in r["findings"]:
                if f["check"] == f"ssp.{exp.ssp_key}.reseller_missing":
                    verdict = "missing";  break
                if f["check"] == f"ssp.{exp.ssp_key}.wrong_seller":
                    verdict = "wrong_seller"; break
                if f["check"] == f"ssp.{exp.ssp_key}.wrong_type":
                    verdict = "wrong_type"; break
            entries.append({
                "entity": r["value"], "kind": r["kind"],
                "ssp_rev_7d": round(ssp_rev, 2),
                "entity_rev_7d": r["total_rev"],
                "verdict": verdict, "score": r["score"],
            })
        entries.sort(key=lambda e: -e["ssp_rev_7d"])
        ssp_scorecards[exp.ssp_key] = entries[: args.per_ssp_top]

    # Pattern clustering — detect shared-template misconfigurations. For each
    # SSP wrong_seller finding, capture the OBSERVED account_ids on each
    # entity, then group entities by the modal observed account_id. Clusters
    # of size ≥ 3 are very likely an upstream-template issue (one outreach
    # fixes N publishers).
    pattern_clusters = _cluster_wrong_seller_patterns(results)

    summary["ssp_scorecard_counts"] = {
        k: len(v) for k, v in ssp_scorecards.items()
    }
    summary["pattern_clusters"] = len(pattern_clusters)

    # Save JSON (full payload including scorecards + clusters)
    Path(args.out).write_text(json.dumps({
        "summary":          summary,
        "results":          results,
        "ssp_scorecards":   ssp_scorecards,
        "pattern_clusters": pattern_clusters,
    }, indent=2, default=str))
    print(f"[audit] saved → {args.out}", flush=True)

    # Print summary
    print("\n" + "=" * 78)
    print("DEEP COMPLIANCE AUDIT — SUMMARY")
    print("=" * 78)
    for k, v in summary.items():
        print(f"  {k:<18} {v}")
    print()
    print(f"Top 10 lowest-scoring (revenue-driving):")
    for r in sorted(results, key=lambda x: (x["score"], -x["total_rev"]))[:10]:
        kind_tag = f"{r['kind']:<6}"
        print(f"  {r['score']:>3}/100  {kind_tag} {r['value'][:50]:<50}  "
              f"${r['total_rev']:>7,.0f}/7d  "
              f"{len(r['findings'])} issue(s)")

    # Slack post
    if not args.no_slack:
        blocks = _slack_blocks(
            results, summary,
            ssp_scorecards=ssp_scorecards,
            pattern_clusters=pattern_clusters,
        )
        fallback = (f"Compliance daily: {critical_count} critical, "
                    f"{high_count} high across {len(results)} top entities "
                    f"(avg {summary['avg_score']:.0f}/100, "
                    f"{summary.get('audited_rev_coverage_pct', 0):.0f}% rev coverage).")
        post_to_slack(blocks, fallback)

    return 0


if __name__ == "__main__":
    sys.exit(main())
