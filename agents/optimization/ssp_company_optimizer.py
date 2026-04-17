"""
agents/optimization/ssp_company_optimizer.py

Optimizer for TB /ad-exchange/ SSP Company partners (Illumin, Smaato,
Dexerto, Start.IO, OC Media, PubNative, Media Lab, Daily Motion, Pijper,
Zoomer, Mission Media, RevIQ, WeBlog, …).

Why this exists (and why it's clever)
-------------------------------------
The /ad-exchange/ SSP Company aggregation is NOT exposed as a `report`
attribute in the public Management API. We probed every plausible
name (ssp, supply, company_ssp, ssp_company, partner, exchange, …) —
all rejected.

BUT `reference_dsp_list` entries are named with a consistent pattern:
    "{DSP} - {SSP_COMPANY} {format}"
    e.g. "Magnite - Smaato Display", "Pubmatic - Illumin Display",
         "AdaptMX - Start.io Video", "Unruly - WeBlog"

So we parse the catalog to build SSP_Company → [partner_ids]. Each SSP
Company maps to dozens or hundreds of DSP endpoints. Then we pull
`company_dsp` revenue from the report endpoint and aggregate spend/
revenue PER SSP Company by summing across its endpoint IDs.

This gives us an API-visible approximation of the /ad-exchange/ SSP
Company report, plus a real control surface: remove all endpoint IDs
for a dead-weight SSP Company from every inventory whitelist.

SSP Companies known from the UI screenshot
------------------------------------------
Illumin, Smaato, Native supply, Dexerto, Start.IO, OC Media Solutions,
PubNative, Media Lab RTB, Daily Motion, Pijper Publishing, Zoomer
Media, Mission Media, RevIQ, WeBlog RTB.

Classification
--------------
PRUNE         endpoints_total ≥ MIN_ENDPOINTS_PRUNE
              AND active_endpoints == 0  (no imps last window)
              AND last_spend < MIN_SPEND_PRUNE
              → remove ALL endpoint IDs from inventory whitelists

REVIEW        active_endpoints > 0 AND dsp_ecpm < LOW_ECPM
              → flag for placement floor review

EXPAND        dsp_ecpm ≥ EXPAND_ECPM AND margin ≥ EXPAND_MARGIN
              → scale up (future: add more endpoints to more inventories)

KEEP          otherwise

Safety
------
- Dry-run default. --apply required for live whitelist edits.
- MAX_SSP_PRUNES_PER_RUN caps blast radius.
- Grace period for newly-seen SSP Companies.
- Full audit log.
"""

from __future__ import annotations

import json
import os
import re
import sys
from collections import defaultdict
from datetime import datetime, timezone, timedelta

_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

from dotenv import load_dotenv
load_dotenv(override=True)

import core.tb_mgmt as tbm

# ─── Known SSP Companies (seed list from /ad-exchange/ UI) ───────────────────

KNOWN_SSP_COMPANIES = [
    "Illumin", "Smaato", "Native supply", "Dexerto", "Start.io", "Start.IO",
    "OC Media Solutions", "OC Media", "PubNative", "Media Lab RTB", "Media Lab",
    "Daily Motion", "Pijper Publishing", "Pijper", "Zoomer Media", "Zoomer",
    "Mission Media", "RevIQ", "WeBlog RTB", "WeBlog",
]

# Normalize variants to a canonical label
SSP_CANONICAL = {
    "Start.IO":            "Start.io",
    "OC Media":            "OC Media Solutions",
    "Media Lab":           "Media Lab RTB",
    "Pijper":              "Pijper Publishing",
    "Zoomer":              "Zoomer Media",
    "WeBlog":              "WeBlog RTB",
}

# Tunables
SCORING_WINDOW_DAYS     = 7
GRACE_DAYS              = 14

MIN_ENDPOINTS_PRUNE     = 3        # must have 3+ endpoints to be "real"
MIN_SPEND_PRUNE         = 5.0      # < $5 total DSP spend across all endpoints
MAX_SSP_PRUNES_PER_RUN  = 2        # conservative — big blast radius per prune

LOW_ECPM                = 0.30
EXPAND_ECPM             = 1.50
EXPAND_MARGIN           = 0.25

LOG_DIR       = os.path.join(_REPO_ROOT, "logs")
SNAPSHOT_FILE = os.path.join(LOG_DIR, "ssp_company_optimizer_snapshot.json")
ACTIONS_LOG   = os.path.join(LOG_DIR, "ssp_company_optimizer_actions.json")
RECS_FILE     = os.path.join(LOG_DIR, "ssp_company_optimizer_recs.json")
CATALOG_MAP   = os.path.join(LOG_DIR, "ssp_company_catalog_map.json")
os.makedirs(LOG_DIR, exist_ok=True)


# ─── Catalog parsing: build SSP_Company → [partner_ids] ─────────────────────

def parse_ssp_company(endpoint_name: str) -> str | None:
    """
    Parse "{DSP} - {SSP_COMPANY} {format}" pattern.

    Returns canonical SSP Company name or None if no match.
    Examples:
      "Magnite - Smaato Display"          → "Smaato"
      "AdaptMX - Illumin Display copy1"   → "Illumin"
      "Pubmatic - Pijper Publishing"      → "Pijper Publishing"
      "33Across - Start.io In App"        → "Start.io"
      "OTTA Unruly Prebid - Display RON"  → None (no SSP Company segment)
    """
    if " - " not in endpoint_name:
        return None
    # Split into DSP and tail
    tail = endpoint_name.split(" - ", 1)[1].strip()

    # Try longest-first match against known SSP Companies
    for company in sorted(KNOWN_SSP_COMPANIES, key=len, reverse=True):
        # Match at start of tail, case-insensitive, word-boundary-ish
        if re.match(rf"{re.escape(company)}\b", tail, re.IGNORECASE):
            return SSP_CANONICAL.get(company, company)
    return None


def build_company_map(catalog: dict[int, str]) -> dict[str, list[int]]:
    """SSP_Company → list of DSP endpoint IDs in catalog."""
    m: dict[str, list[int]] = defaultdict(list)
    unmatched = 0
    for pid, name in catalog.items():
        company = parse_ssp_company(name)
        if company:
            m[company].append(pid)
        else:
            unmatched += 1
    print(f"  catalog: {len(catalog)} endpoints; "
          f"{sum(len(v) for v in m.values())} mapped to "
          f"{len(m)} SSP Companies; {unmatched} unmatched")
    # Persist for debugging
    with open(CATALOG_MAP, "w") as f:
        json.dump({k: sorted(v) for k, v in m.items()}, f, indent=2)
    return dict(m)


# ─── Revenue aggregation per SSP Company ─────────────────────────────────────

def _window() -> tuple[str, str]:
    end = datetime.now(timezone.utc).date()
    start = end - timedelta(days=SCORING_WINDOW_DAYS)
    return start.isoformat(), end.isoformat()


def aggregate_by_ssp_company(
    dsp_rows: list[dict],
    company_map: dict[str, list[int]],
    catalog: dict[int, str],
) -> list[dict]:
    """
    Roll up partner_report (company_dsp rows) to SSP Company level.

    Each report row has `company_dsp` like "Pubmatic #2". We parse "#<id>"
    to get the partner ID, then look up its SSP Company from company_map.
    """
    # Reverse: partner_id → SSP_Company
    id_to_company: dict[int, str] = {}
    for company, ids in company_map.items():
        for pid in ids:
            id_to_company[pid] = company

    agg: dict[str, dict] = {}
    for row in dsp_rows:
        name = row.get("company_dsp", "")
        # Parse trailing "#NN"
        pid = None
        if "#" in name:
            try:
                pid = int(name.rsplit("#", 1)[1].strip())
            except ValueError:
                pass
        # Fallback: match by catalog name (unlikely but safe)
        if pid is None:
            for k, v in catalog.items():
                if v == name:
                    pid = k
                    break
        company = id_to_company.get(pid) if pid is not None else None
        if not company:
            continue

        a = agg.setdefault(company, {
            "ssp_company":       company,
            "active_endpoints":  0,
            "wins":              0,
            "impressions":       0,
            "bid_responses":     0,
            "publisher_revenue": 0.0,
            "dsp_spend":         0.0,
            "profit":            0.0,
            "endpoint_names":    [],
        })
        imps = row.get("impressions", 0) or 0
        a["wins"]              += row.get("wins", 0) or 0
        a["impressions"]       += imps
        a["bid_responses"]     += row.get("bid_responses", 0) or 0
        a["publisher_revenue"] += row.get("publisher_revenue", 0.0) or 0.0
        a["dsp_spend"]         += row.get("dsp_spend", 0.0) or 0.0
        a["profit"]            += row.get("profit", 0.0) or 0.0
        if imps > 0:
            a["active_endpoints"] += 1
        a["endpoint_names"].append(name)

    # Attach totals from catalog
    out = []
    for company, a in agg.items():
        a["total_endpoints"] = len(company_map.get(company, []))
        imps = a["impressions"]
        a["fill_rate"] = (imps / a["bid_responses"]) if a["bid_responses"] else 0.0
        a["dsp_ecpm"]  = (a["dsp_spend"] * 1000.0 / imps) if imps else 0.0
        a["margin"]    = (a["profit"] / a["dsp_spend"]) if a["dsp_spend"] else 0.0
        out.append(a)

    # Also add SSP Companies present in catalog but zero traffic
    for company, ids in company_map.items():
        if company not in agg:
            out.append({
                "ssp_company":       company,
                "total_endpoints":   len(ids),
                "active_endpoints":  0,
                "wins":              0,
                "impressions":       0,
                "bid_responses":     0,
                "publisher_revenue": 0.0,
                "dsp_spend":         0.0,
                "profit":            0.0,
                "fill_rate":         0.0,
                "dsp_ecpm":          0.0,
                "margin":            0.0,
                "endpoint_names":    [],
            })

    out.sort(key=lambda x: -x["dsp_spend"])
    return out


# ─── Classification ──────────────────────────────────────────────────────────

def classify(p: dict, snapshot: dict) -> str:
    name = p["ssp_company"]
    first_seen = snapshot.get("companies", {}).get(name, {}).get("first_seen")
    if first_seen:
        age = (datetime.now(timezone.utc)
               - datetime.fromisoformat(first_seen.replace("Z", "+00:00"))).days
        if age < GRACE_DAYS:
            return "KEEP"

    if (p["total_endpoints"] >= MIN_ENDPOINTS_PRUNE
            and p["active_endpoints"] == 0
            and p["dsp_spend"] < MIN_SPEND_PRUNE):
        return "PRUNE"
    if p["dsp_ecpm"] >= EXPAND_ECPM and p["margin"] >= EXPAND_MARGIN:
        return "EXPAND"
    if p["active_endpoints"] > 0 and 0 < p["dsp_ecpm"] < LOW_ECPM:
        return "REVIEW"
    return "KEEP"


# ─── Snapshot ────────────────────────────────────────────────────────────────

def load_snapshot() -> dict:
    if not os.path.exists(SNAPSHOT_FILE):
        return {"companies": {}, "last_run": None}
    with open(SNAPSHOT_FILE) as f:
        return json.load(f)


def update_snapshot(snap: dict, companies: list[dict]) -> dict:
    now = datetime.now(timezone.utc).isoformat()
    c = snap.setdefault("companies", {})
    for p in companies:
        n = p["ssp_company"]
        if n not in c:
            c[n] = {"first_seen": now}
        c[n]["last_seen"]   = now
        c[n]["last_spend"]  = p["dsp_spend"]
        c[n]["last_imps"]   = p["impressions"]
        c[n]["endpoints"]   = p["total_endpoints"]
    snap["last_run"] = now
    return snap


def save_snapshot(snap: dict) -> None:
    with open(SNAPSHOT_FILE, "w") as f:
        json.dump(snap, f, indent=2)


def append_actions_log(actions: list[dict]) -> None:
    prior = []
    if os.path.exists(ACTIONS_LOG):
        with open(ACTIONS_LOG) as f:
            try: prior = json.load(f)
            except Exception: prior = []
    prior.extend(actions)
    with open(ACTIONS_LOG, "w") as f:
        json.dump(prior, f, indent=2)


# ─── Action: prune all endpoints for an SSP Company ──────────────────────────

def apply_company_prune(
    company: str,
    endpoint_ids: list[int],
    inventories: list[dict],
    catalog: dict[int, str],
    dry_run: bool,
) -> list[dict]:
    actions = []
    id_set = set(endpoint_ids)
    for inv in inventories:
        inv_id = inv["inventory_id"]
        current = list(inv.get("inventory_dsp[white]") or [])
        to_remove = [pid for pid in current if pid in id_set]
        if not to_remove:
            continue
        new_white = [pid for pid in current if pid not in id_set]
        action = {
            "type":           "ssp_company_prune",
            "ssp_company":    company,
            "inventory_id":   inv_id,
            "inventory_name": inv.get("title"),
            "removed_ids":    to_remove,
            "removed_names":  [catalog.get(i, f"<{i}>") for i in to_remove],
            "before_count":   len(current),
            "after_count":    len(new_white),
            "dry_run":        dry_run,
            "timestamp":      datetime.now(timezone.utc).isoformat(),
        }
        if not dry_run:
            try:
                tbm.edit_inventory(inv_id, whitelist=new_white, dry_run=False)
                action["applied"] = True
            except Exception as e:
                action["applied"] = False
                action["error"]   = str(e)
        else:
            action["applied"] = False
        actions.append(action)
        print(
            f"[ssp_opt] {'DRY' if dry_run else 'APPLIED'} prune "
            f"{company} inv={inv_id} ({inv.get('title')})  -{len(to_remove)}"
        )
    return actions


# ─── Slack ───────────────────────────────────────────────────────────────────

def post_slack(recs: dict, applied: int, bootstrap: bool, dry_run: bool) -> None:
    try:
        from core.slack import post_message
    except Exception:
        return
    buckets = recs["by_class"]
    tag = "🧪 BOOTSTRAP" if bootstrap else ("🟢 SSP COMPANY" if not dry_run else "🔍 SSP COMPANY")
    lines = [
        f"{tag}  window {recs['window']['start']} → {recs['window']['end']}",
        f"Companies: {recs['total_companies']}  "
        f"| PRUNE {len(buckets['PRUNE'])}  "
        f"| REVIEW {len(buckets['REVIEW'])}  "
        f"| EXPAND {len(buckets['EXPAND'])}  "
        f"| KEEP {len(buckets['KEEP'])}",
    ]
    if buckets["PRUNE"]:
        lines.append("\n*Dead-weight SSP Companies:*")
        for p in buckets["PRUNE"][:8]:
            lines.append(
                f"  ✂️ {p['ssp_company']:<22} "
                f"endpoints={p['total_endpoints']}  "
                f"active={p['active_endpoints']}  "
                f"spend=${p['dsp_spend']:.2f}"
            )
    if buckets["EXPAND"]:
        lines.append("\n*Top performing SSP Companies:*")
        for p in sorted(buckets["EXPAND"], key=lambda x: -x["dsp_spend"])[:5]:
            lines.append(
                f"  🚀 {p['ssp_company']:<22} "
                f"eCPM=${p['dsp_ecpm']:.2f}  "
                f"margin={p['margin']*100:.0f}%  "
                f"spend=${p['dsp_spend']:.0f}"
            )
    if not dry_run and applied:
        lines.append(f"\n✅ Applied {applied} inventory whitelist removals.")
    try:
        post_message("\n".join(lines))
    except Exception:
        pass


# ─── Entry point ─────────────────────────────────────────────────────────────

def run(dry_run: bool = True) -> dict:
    print(f"\n{'='*72}")
    print(f"  SSP Company Optimizer  {'[DRY RUN]' if dry_run else '[LIVE]'}")
    print(f"  {datetime.now(timezone.utc).isoformat()}")
    print(f"{'='*72}")

    catalog = tbm.list_partners()
    company_map = build_company_map(catalog)

    start, end = _window()
    print(f"\n→ partner_report (company_dsp)  {start} → {end}")
    dsp_rows = tbm.partner_report(start, end, day_group="total", limit=1000)
    print(f"  {len(dsp_rows)} DSP rows")

    companies = aggregate_by_ssp_company(dsp_rows, company_map, catalog)

    inventories_lite = tbm.list_inventories()
    inventories = []
    for inv in inventories_lite:
        try:
            inventories.append(tbm.get_inventory(inv["inventory_id"]))
        except Exception as e:
            print(f"  ✗ inv {inv.get('inventory_id')}: {e}")

    snapshot = load_snapshot()
    bootstrap = not snapshot.get("companies")
    if bootstrap:
        print("  (first run — bootstrap)")

    by_class = {"PRUNE": [], "REVIEW": [], "EXPAND": [], "KEEP": []}
    for p in companies:
        p["classification"] = classify(p, snapshot)
        by_class[p["classification"]].append(p)

    print(
        f"\n  PRUNE:  {len(by_class['PRUNE'])}"
        f"\n  REVIEW: {len(by_class['REVIEW'])}"
        f"\n  EXPAND: {len(by_class['EXPAND'])}"
        f"\n  KEEP:   {len(by_class['KEEP'])}"
    )

    actions: list[dict] = []
    applied = 0
    if by_class["PRUNE"] and not bootstrap:
        for p in by_class["PRUNE"][:MAX_SSP_PRUNES_PER_RUN]:
            ids = company_map.get(p["ssp_company"], [])
            if ids:
                acts = apply_company_prune(
                    p["ssp_company"], ids, inventories, catalog, dry_run=dry_run
                )
                actions.extend(acts)
        applied = sum(1 for a in actions if a.get("applied"))

    # Strip endpoint_names from recs (too verbose for JSON dashboard)
    def _clean(p):
        return {k: v for k, v in p.items() if k != "endpoint_names"}

    recs = {
        "window":          {"start": start, "end": end},
        "timestamp":       datetime.now(timezone.utc).isoformat(),
        "dry_run":         dry_run,
        "bootstrap":       bootstrap,
        "total_companies": len(companies),
        "by_class":        {k: [_clean(p) for p in v] for k, v in by_class.items()},
        "applied":         applied,
    }
    with open(RECS_FILE, "w") as f:
        json.dump(recs, f, indent=2)
    print(f"\n  Recs → {RECS_FILE}")

    if actions:
        append_actions_log(actions)

    save_snapshot(update_snapshot(snapshot, companies))
    post_slack(recs, applied, bootstrap, dry_run)

    # Human summary
    print("\n  Full SSP Company scorecard (by DSP spend):")
    print(f"    {'Company':<22} {'Endpts':>7} {'Active':>7} {'Imps':>10} {'Spend $':>9} {'eCPM $':>7} {'Margin':>7} {'Class':<10}")
    for p in sorted(companies, key=lambda x: -x["dsp_spend"]):
        print(
            f"    {p['ssp_company']:<22} "
            f"{p['total_endpoints']:>7} "
            f"{p['active_endpoints']:>7} "
            f"{p['impressions']:>10,} "
            f"{p['dsp_spend']:>9.2f} "
            f"{p['dsp_ecpm']:>7.2f} "
            f"{p['margin']*100:>6.1f}% "
            f"{p['classification']:<10}"
        )

    return recs


if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true",
                    help="execute inventory whitelist removals live")
    args = ap.parse_args()
    run(dry_run=not args.apply)
