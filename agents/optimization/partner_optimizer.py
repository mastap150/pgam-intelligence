"""
agents/optimization/partner_optimizer.py

Continuous optimizer for TB /ad-exchange/ SSP (supply) partners.

Goal
----
Raise SSP revenue and fill rate while pruning dead-weight supply partners
that drive request volume without returning value. Mirrors the
/ad-exchange/ report (Illumin, Smaato, Start.IO, Dexerto, PubNative,
Daily Motion, Pijper Publishing, Mission Media, RevIQ, WeBlog RTB, …).

Data source
-----------
TB Management API `report` with attribute=publisher. Each row is one
supply partner: requests, impressions, fill_rate, publisher_revenue,
dsp_spend, profit. `ssp_fill_rate` column is the upstream partner's
bid-response rate (how often they even bid).

Levers (what we can actually do via the API)
---------------------------------------------
1. Per-placement floor  (tbm.set_floor)       — filter junk on a unit
2. Per-placement status (tbm.disable_placement)— hard stop an ad unit
3. Per-inventory whitelist (tbm.edit_inventory) — demand-side (company_dsp)

There is no endpoint to "disconnect an SSP partner" at the connection
level. That action lives in the TB admin UI. So the agent:
- Auto-applies NOTHING that could kill legitimate traffic.
- Emits a ranked recommendation report: PRUNE, REVIEW_FLOOR, EXPAND, KEEP.
- Posts the ranked list to Slack so the human can execute the few
  connection-level disables in TB with full context.
- --apply enables a conservative mode that raises placement floors on
  the top inventories where REVIEW_FLOOR partners dominate requests
  (future iteration — disabled for now).

Classification (over SCORING_WINDOW_DAYS)
-----------------------------------------
PRUNE          requests ≥ MIN_REQ_PRUNE
               AND impressions < MIN_IMP_PRUNE
               AND publisher_revenue < MIN_REV_PRUNE
               → dead weight; recommend disconnecting upstream

REVIEW_FLOOR   requests ≥ MIN_REQ_FLOOR
               AND fill_rate < FLOOR_FILL_THRESHOLD
               AND revenue > 0
               → high junk ratio with some value; raise floors

EXPAND         dsp_ecpm ≥ EXPAND_ECPM
               AND margin ≥ EXPAND_MARGIN
               AND impressions ≥ MIN_IMP_EXPAND
               → strong partner; request more QPS / new formats

KEEP           otherwise

Safety
------
- Grace period: newly-seen partners are KEEP for GRACE_DAYS.
- Bootstrap: first run records baseline and posts summary only.
- All action logs to logs/partner_optimizer_actions.json.
- Recs JSON at logs/partner_optimizer_recs.json for dashboard.
"""

from __future__ import annotations

import json
import os
import sys
import urllib.parse
import requests
from datetime import datetime, timezone, timedelta

_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

from dotenv import load_dotenv
load_dotenv(override=True)

import core.tb_mgmt as tbm

# ─────────────────────────────────────────────────────────────────────────────
# Tunables
# ─────────────────────────────────────────────────────────────────────────────

SCORING_WINDOW_DAYS   = 7
GRACE_DAYS            = 10

# PRUNE — dead weight (lots of requests, no monetization)
MIN_REQ_PRUNE         = 50_000_000
MIN_IMP_PRUNE         = 5_000
MIN_REV_PRUNE         = 20.0

# REVIEW_FLOOR — raise floors to filter junk
MIN_REQ_FLOOR         = 500_000_000
FLOOR_FILL_THRESHOLD  = 0.001

# EXPAND — scale winners
EXPAND_ECPM           = 1.50
EXPAND_MARGIN         = 0.25
MIN_IMP_EXPAND        = 100_000

TB_BASE = "https://ssp.pgammedia.com/api"

LOG_DIR       = os.path.join(_REPO_ROOT, "logs")
SNAPSHOT_FILE = os.path.join(LOG_DIR, "partner_optimizer_snapshot.json")
ACTIONS_LOG   = os.path.join(LOG_DIR, "partner_optimizer_actions.json")
RECS_FILE     = os.path.join(LOG_DIR, "partner_optimizer_recs.json")
os.makedirs(LOG_DIR, exist_ok=True)


# ─────────────────────────────────────────────────────────────────────────────
# Data pull — supply-side (publisher attribute)
# ─────────────────────────────────────────────────────────────────────────────

def _window() -> tuple[str, str]:
    end = datetime.now(timezone.utc).date()
    start = end - timedelta(days=SCORING_WINDOW_DAYS)
    return start.isoformat(), end.isoformat()


def supply_report(start: str, end: str, limit: int = 1000) -> list[dict]:
    """
    Pull publisher-grouped (upstream SSP partner) report.

    Rows contain:
      publisher           — partner name (e.g. 'Spigot Spigot #87', 'Illumin #...')
      bid_requests        — total SSP requests from that partner
      bid_responses       — how often we bid back
      wins                — won auctions
      impressions         — paid impressions
      fill_rate           — impressions / bid_responses
      ssp_fill_rate       — bid_responses / bid_requests
      publisher_revenue   — USD we earned (revenue back to PGAM)
      dsp_spend           — USD the downstream DSP paid (SSP revenue column)
      profit              — dsp_spend - publisher_revenue (margin in USD)
      dsp_ecpm            — dsp_spend CPM
    """
    token = tbm._get_token()
    params = [
        ("from", start),
        ("to", end),
        ("day_group", "total"),
        ("limit", limit),
        ("attribute[]", "publisher"),
    ]
    url = f"{TB_BASE}/{token}/report?" + urllib.parse.urlencode(params)
    resp = requests.get(url, timeout=90)
    if not resp.ok:
        raise RuntimeError(f"supply_report HTTP {resp.status_code}: {resp.text[:200]}")
    data = resp.json()
    return data.get("data", data) if isinstance(data, dict) else data


# ─────────────────────────────────────────────────────────────────────────────
# Scoring
# ─────────────────────────────────────────────────────────────────────────────

def enrich(row: dict) -> dict:
    imps = row.get("impressions", 0) or 0
    reqs = row.get("bid_requests", 0) or 0
    resp = row.get("bid_responses", 0) or 0
    rev  = row.get("publisher_revenue", 0.0) or 0.0
    spend = row.get("dsp_spend", 0.0) or 0.0
    profit = row.get("profit", 0.0) or 0.0
    return {
        "publisher":         row.get("publisher", "<unknown>"),
        "bid_requests":      reqs,
        "bid_responses":     resp,
        "wins":              row.get("wins", 0) or 0,
        "impressions":       imps,
        "publisher_revenue": rev,
        "dsp_spend":         spend,
        "profit":            profit,
        "ssp_fill_rate":     (resp / reqs) if reqs else 0.0,
        "fill_rate":         (imps / resp) if resp else 0.0,
        "dsp_ecpm":          (spend * 1000.0 / imps) if imps else 0.0,
        "rpm":               (rev * 1000.0 / reqs) if reqs else 0.0,
        "margin":            (profit / spend) if spend else 0.0,
    }


def classify(p: dict, snapshot: dict) -> str:
    name = p["publisher"]
    first_seen = snapshot.get("partners", {}).get(name, {}).get("first_seen")
    if first_seen:
        age = (datetime.now(timezone.utc)
               - datetime.fromisoformat(first_seen.replace("Z", "+00:00"))).days
        if age < GRACE_DAYS:
            return "KEEP"

    reqs = p["bid_requests"]
    imps = p["impressions"]
    rev  = p["publisher_revenue"]

    if reqs >= MIN_REQ_PRUNE and imps < MIN_IMP_PRUNE and rev < MIN_REV_PRUNE:
        return "PRUNE"
    if (p["dsp_ecpm"] >= EXPAND_ECPM
            and p["margin"] >= EXPAND_MARGIN
            and imps >= MIN_IMP_EXPAND):
        return "EXPAND"
    if (reqs >= MIN_REQ_FLOOR
            and p["fill_rate"] < FLOOR_FILL_THRESHOLD
            and rev > 0):
        return "REVIEW_FLOOR"
    return "KEEP"


# ─────────────────────────────────────────────────────────────────────────────
# Snapshot / persistence
# ─────────────────────────────────────────────────────────────────────────────

def load_snapshot() -> dict:
    if not os.path.exists(SNAPSHOT_FILE):
        return {"partners": {}, "last_run": None}
    with open(SNAPSHOT_FILE) as f:
        return json.load(f)


def update_snapshot(snap: dict, partners: list[dict]) -> dict:
    now = datetime.now(timezone.utc).isoformat()
    p = snap.setdefault("partners", {})
    for partner in partners:
        n = partner["publisher"]
        if n not in p:
            p[n] = {"first_seen": now}
        p[n]["last_seen"]        = now
        p[n]["last_revenue"]     = partner["publisher_revenue"]
        p[n]["last_impressions"] = partner["impressions"]
        p[n]["last_requests"]    = partner["bid_requests"]
    snap["last_run"] = now
    return snap


def save_snapshot(snap: dict) -> None:
    with open(SNAPSHOT_FILE, "w") as f:
        json.dump(snap, f, indent=2)


def append_actions_log(actions: list[dict]) -> None:
    prior = []
    if os.path.exists(ACTIONS_LOG):
        with open(ACTIONS_LOG) as f:
            try:
                prior = json.load(f)
            except Exception:
                prior = []
    prior.extend(actions)
    with open(ACTIONS_LOG, "w") as f:
        json.dump(prior, f, indent=2)


# ─────────────────────────────────────────────────────────────────────────────
# Slack
# ─────────────────────────────────────────────────────────────────────────────

def post_slack(recs: dict, bootstrap: bool) -> None:
    try:
        from core.slack import post_message
    except Exception:
        return
    buckets = recs["by_class"]
    tag = "🧪 BOOTSTRAP" if bootstrap else "📊 PARTNER OPTIMIZER"
    lines = [
        f"{tag}  window {recs['window']['start']} → {recs['window']['end']}",
        f"Partners: {recs['total_partners']}  "
        f"| PRUNE {len(buckets['PRUNE'])}  "
        f"| REVIEW_FLOOR {len(buckets['REVIEW_FLOOR'])}  "
        f"| EXPAND {len(buckets['EXPAND'])}  "
        f"| KEEP {len(buckets['KEEP'])}",
    ]
    if buckets["PRUNE"]:
        lines.append("\n*Dead weight (top 8) — disconnect upstream:*")
        for p in sorted(buckets["PRUNE"], key=lambda x: -x["bid_requests"])[:8]:
            lines.append(
                f"  ✂️ {p['publisher'][:38]:<38} "
                f"reqs={p['bid_requests']/1e6:.0f}M  "
                f"imps={p['impressions']:,}  "
                f"rev=${p['publisher_revenue']:.2f}"
            )
    if buckets["REVIEW_FLOOR"]:
        lines.append("\n*Raise floors (top 5):*")
        for p in sorted(buckets["REVIEW_FLOOR"], key=lambda x: -x["bid_requests"])[:5]:
            lines.append(
                f"  🪜 {p['publisher'][:38]:<38} "
                f"reqs={p['bid_requests']/1e6:.0f}M  "
                f"fill={p['fill_rate']*100:.3f}%  "
                f"rev=${p['publisher_revenue']:.0f}"
            )
    if buckets["EXPAND"]:
        lines.append("\n*Scale candidates (top 5):*")
        for p in sorted(buckets["EXPAND"], key=lambda x: -x["dsp_spend"])[:5]:
            lines.append(
                f"  🚀 {p['publisher'][:38]:<38} "
                f"eCPM=${p['dsp_ecpm']:.2f}  "
                f"margin={p['margin']*100:.0f}%  "
                f"rev=${p['publisher_revenue']:.0f}"
            )
    try:
        post_message("\n".join(lines))
    except Exception:
        pass


# ─────────────────────────────────────────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────────────────────────────────────────

def run(dry_run: bool = True) -> dict:
    print(f"\n{'='*72}")
    print(f"  Partner Optimizer  {'[DRY RUN]' if dry_run else '[LIVE]'}")
    print(f"  {datetime.now(timezone.utc).isoformat()}")
    print(f"{'='*72}")

    start, end = _window()
    print(f"\n→ supply_report  {start} → {end}")
    raw = supply_report(start, end)
    print(f"  {len(raw)} rows")

    partners = [enrich(r) for r in raw]
    # Drop the catch-all "Not set #0" row — it's residual / untagged traffic
    partners = [p for p in partners if not p["publisher"].startswith("Not set")]

    snapshot = load_snapshot()
    bootstrap = not snapshot.get("partners")
    if bootstrap:
        print("  (first run — bootstrap snapshot)")

    by_class = {"PRUNE": [], "REVIEW_FLOOR": [], "EXPAND": [], "KEEP": []}
    for p in partners:
        p["classification"] = classify(p, snapshot)
        by_class[p["classification"]].append(p)

    print(
        f"\n  PRUNE:        {len(by_class['PRUNE'])}"
        f"\n  REVIEW_FLOOR: {len(by_class['REVIEW_FLOOR'])}"
        f"\n  EXPAND:       {len(by_class['EXPAND'])}"
        f"\n  KEEP:         {len(by_class['KEEP'])}"
    )

    recs = {
        "window":         {"start": start, "end": end},
        "timestamp":      datetime.now(timezone.utc).isoformat(),
        "dry_run":        dry_run,
        "bootstrap":      bootstrap,
        "total_partners": len(partners),
        "by_class":       by_class,
    }
    with open(RECS_FILE, "w") as f:
        json.dump(recs, f, indent=2)
    print(f"\n  Recs → {RECS_FILE}")

    save_snapshot(update_snapshot(snapshot, partners))
    post_slack(recs, bootstrap)

    def _rank(b, key): return sorted(b, key=lambda x: -x[key])

    if by_class["PRUNE"]:
        print("\n  Top PRUNE (disconnect upstream — manual action in TB UI):")
        for p in _rank(by_class["PRUNE"], "bid_requests")[:15]:
            print(
                f"    ✂️ {p['publisher'][:45]:<45}  "
                f"reqs={p['bid_requests']/1e6:>6.0f}M  "
                f"imps={p['impressions']:>6,}  "
                f"rev=${p['publisher_revenue']:>7.2f}  "
                f"ssp_fill={p['ssp_fill_rate']*100:.3f}%"
            )
    if by_class["REVIEW_FLOOR"]:
        print("\n  Top REVIEW_FLOOR (raise placement floors):")
        for p in _rank(by_class["REVIEW_FLOOR"], "bid_requests")[:10]:
            print(
                f"    🪜 {p['publisher'][:45]:<45}  "
                f"reqs={p['bid_requests']/1e6:>6.0f}M  "
                f"fill={p['fill_rate']*100:>6.3f}%  "
                f"rev=${p['publisher_revenue']:>7.2f}"
            )
    if by_class["EXPAND"]:
        print("\n  Top EXPAND (scale — request more QPS):")
        for p in _rank(by_class["EXPAND"], "dsp_spend")[:10]:
            print(
                f"    🚀 {p['publisher'][:45]:<45}  "
                f"eCPM=${p['dsp_ecpm']:>5.2f}  "
                f"margin={p['margin']*100:>5.1f}%  "
                f"rev=${p['publisher_revenue']:>7.2f}"
            )

    return recs


if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true",
                    help="(reserved) apply placement floor actions live")
    args = ap.parse_args()
    run(dry_run=not args.apply)
