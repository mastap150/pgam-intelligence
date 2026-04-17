"""
agents/optimization/dsp_optimizer.py

Optimizer for TB /ad-exchange/dsp — downstream DSP endpoints (demand side).

Scope
-----
Mirrors the /ad-exchange/dsp page (Pubmatic #2, AdaptMX #17, Magnite #3,
Verve #46, Illumin Demand #29, Unruly #4, etc.). Each row is one DSP
endpoint instance; revenue/QPS attribution is per-endpoint.

Difference from partner_optimizer
---------------------------------
partner_optimizer works upstream (publisher supply) and can ONLY recommend
— the TB UI is the only control surface for disconnecting supply partners.

dsp_optimizer works downstream (demand) where we DO have an API lever:
`inventory_dsp[white]`. The agent can auto-remove dead-weight DSP endpoints
from every inventory whitelist, filtering junk demand at the SSP boundary.

Scoring (SCORING_WINDOW_DAYS)
-----------------------------
PRUNE         bid_responses ≥ MIN_RESP_PRUNE
              AND impressions < MIN_IMP_PRUNE
              AND dsp_spend < MIN_SPEND_PRUNE
              → auto-remove from all inventory whitelists (--apply)

RAISE_FLOOR   dsp_ecpm < LOW_ECPM_THRESHOLD
              AND impressions ≥ MIN_IMP_FLOOR
              → soft lever; flag for placement floor review

EXPAND        dsp_ecpm ≥ EXPAND_ECPM
              AND margin ≥ EXPAND_MARGIN
              → add to inventories where absent (--apply optional future)

KEEP          otherwise

Safety
------
- Default dry_run. --apply required for live whitelist edits.
- MAX_PRUNES_PER_RUN caps blast radius (5 by default).
- GRACE_DAYS protects new DSPs.
- Every action logged to logs/dsp_optimizer_actions.json.
"""

from __future__ import annotations

import json
import os
import sys
from datetime import datetime, timezone, timedelta

_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

from dotenv import load_dotenv
load_dotenv(override=True)

import core.tb_mgmt as tbm

# ─── Tunables ────────────────────────────────────────────────────────────────

SCORING_WINDOW_DAYS   = 7
GRACE_DAYS            = 14

MIN_RESP_PRUNE        = 5_000_000
MIN_IMP_PRUNE         = 2_000
MIN_SPEND_PRUNE       = 10.0
MAX_PRUNES_PER_RUN    = 5

LOW_ECPM_THRESHOLD    = 0.20
MIN_IMP_FLOOR         = 500_000

EXPAND_ECPM           = 1.50
EXPAND_MARGIN         = 0.25

LOG_DIR       = os.path.join(_REPO_ROOT, "logs")
SNAPSHOT_FILE = os.path.join(LOG_DIR, "dsp_optimizer_snapshot.json")
ACTIONS_LOG   = os.path.join(LOG_DIR, "dsp_optimizer_actions.json")
RECS_FILE     = os.path.join(LOG_DIR, "dsp_optimizer_recs.json")
os.makedirs(LOG_DIR, exist_ok=True)


# ─── Scoring ─────────────────────────────────────────────────────────────────

def _window() -> tuple[str, str]:
    end = datetime.now(timezone.utc).date()
    start = end - timedelta(days=SCORING_WINDOW_DAYS)
    return start.isoformat(), end.isoformat()


def enrich(row: dict) -> dict:
    imps  = row.get("impressions", 0) or 0
    resp  = row.get("bid_responses", 0) or 0
    spend = row.get("dsp_spend", 0.0) or 0.0
    prof  = row.get("profit", 0.0) or 0.0
    rev   = row.get("publisher_revenue", 0.0) or 0.0
    return {
        "company_dsp":       row.get("company_dsp", "<unknown>"),
        "bid_responses":     resp,
        "wins":              row.get("wins", 0) or 0,
        "impressions":       imps,
        "dsp_spend":         spend,
        "publisher_revenue": rev,
        "profit":            prof,
        "fill_rate":         (imps / resp) if resp else 0.0,
        "dsp_ecpm":          (spend * 1000.0 / imps) if imps else 0.0,
        "margin":            (prof / spend) if spend else 0.0,
    }


def classify(p: dict, snapshot: dict) -> str:
    name = p["company_dsp"]
    first_seen = snapshot.get("dsps", {}).get(name, {}).get("first_seen")
    if first_seen:
        age = (datetime.now(timezone.utc)
               - datetime.fromisoformat(first_seen.replace("Z", "+00:00"))).days
        if age < GRACE_DAYS:
            return "KEEP"

    if (p["bid_responses"] >= MIN_RESP_PRUNE
            and p["impressions"] < MIN_IMP_PRUNE
            and p["dsp_spend"] < MIN_SPEND_PRUNE):
        return "PRUNE"
    if (p["dsp_ecpm"] >= EXPAND_ECPM
            and p["margin"] >= EXPAND_MARGIN):
        return "EXPAND"
    if (p["impressions"] >= MIN_IMP_FLOOR
            and 0 < p["dsp_ecpm"] < LOW_ECPM_THRESHOLD):
        return "RAISE_FLOOR"
    return "KEEP"


# ─── Snapshot ────────────────────────────────────────────────────────────────

def load_snapshot() -> dict:
    if not os.path.exists(SNAPSHOT_FILE):
        return {"dsps": {}, "last_run": None}
    with open(SNAPSHOT_FILE) as f:
        return json.load(f)


def update_snapshot(snap: dict, dsps: list[dict]) -> dict:
    now = datetime.now(timezone.utc).isoformat()
    d = snap.setdefault("dsps", {})
    for p in dsps:
        n = p["company_dsp"]
        if n not in d:
            d[n] = {"first_seen": now}
        d[n]["last_seen"]    = now
        d[n]["last_spend"]   = p["dsp_spend"]
        d[n]["last_imps"]    = p["impressions"]
        d[n]["last_ecpm"]    = p["dsp_ecpm"]
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


# ─── Name → ID mapping ───────────────────────────────────────────────────────

def map_to_ids(names: list[str], catalog: dict[int, str]) -> dict[str, int]:
    """Report returns 'Pubmatic #2'; catalog key is 2 with name 'Pubmatic'.
    Parse '#<id>' suffix to resolve."""
    out = {}
    for name in names:
        # Try exact match first
        for pid, n in catalog.items():
            if n == name:
                out[name] = pid
                break
        else:
            # Parse trailing '#NN'
            if "#" in name:
                try:
                    pid = int(name.rsplit("#", 1)[1].strip())
                    if pid in catalog:
                        out[name] = pid
                except ValueError:
                    pass
    return out


# ─── Action execution ────────────────────────────────────────────────────────

def apply_prunes(
    prune_ids: list[int],
    inventories: list[dict],
    catalog: dict[int, str],
    dry_run: bool,
) -> list[dict]:
    actions = []
    for inv in inventories:
        inv_id = inv["inventory_id"]
        current = list(inv.get("inventory_dsp[white]") or [])
        to_remove = [pid for pid in prune_ids if pid in current]
        if not to_remove:
            continue
        new_white = [pid for pid in current if pid not in to_remove]
        action = {
            "type":           "dsp_prune",
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
            f"[dsp_opt] {'DRY' if dry_run else 'APPLIED'} prune "
            f"inv={inv_id} ({inv.get('title')})  -{len(to_remove)}"
        )
    return actions


# ─── Slack ───────────────────────────────────────────────────────────────────

def post_slack(recs: dict, applied: int, bootstrap: bool, dry_run: bool) -> None:
    try:
        from core.slack import post_message
    except Exception:
        return
    buckets = recs["by_class"]
    tag = "🧪 BOOTSTRAP" if bootstrap else ("🟢 DSP OPT" if not dry_run else "🔍 DSP OPT")
    lines = [
        f"{tag}  window {recs['window']['start']} → {recs['window']['end']}",
        f"DSPs: {recs['total_dsps']}  "
        f"| PRUNE {len(buckets['PRUNE'])}  "
        f"| RAISE_FLOOR {len(buckets['RAISE_FLOOR'])}  "
        f"| EXPAND {len(buckets['EXPAND'])}  "
        f"| KEEP {len(buckets['KEEP'])}",
    ]
    if buckets["PRUNE"]:
        lines.append("\n*PRUNE (auto-removed from whitelists):*" if not dry_run and applied
                     else "\n*PRUNE candidates:*")
        for p in sorted(buckets["PRUNE"], key=lambda x: -x["bid_responses"])[:8]:
            lines.append(
                f"  ✂️ {p['company_dsp'][:34]:<34} "
                f"resp={p['bid_responses']/1e6:.1f}M  "
                f"imps={p['impressions']:,}  "
                f"spend=${p['dsp_spend']:.2f}"
            )
    if buckets["EXPAND"]:
        lines.append("\n*Strong DSPs (scale up):*")
        for p in sorted(buckets["EXPAND"], key=lambda x: -x["dsp_spend"])[:5]:
            lines.append(
                f"  🚀 {p['company_dsp'][:34]:<34} "
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
    print(f"  DSP Optimizer  {'[DRY RUN]' if dry_run else '[LIVE]'}")
    print(f"  {datetime.now(timezone.utc).isoformat()}")
    print(f"{'='*72}")

    start, end = _window()
    print(f"\n→ partner_report (company_dsp)  {start} → {end}")
    raw = tbm.partner_report(start, end, day_group="total", limit=1000)
    catalog = tbm.list_partners()
    inventories_lite = tbm.list_inventories()
    inventories = []
    for inv in inventories_lite:
        try:
            inventories.append(tbm.get_inventory(inv["inventory_id"]))
        except Exception as e:
            print(f"  ✗ inv {inv.get('inventory_id')}: {e}")
    print(f"  {len(raw)} DSP rows | catalog {len(catalog)} | inv {len(inventories)}")

    dsps = [enrich(r) for r in raw if r.get("company_dsp")]

    snapshot = load_snapshot()
    bootstrap = not snapshot.get("dsps")
    if bootstrap:
        print("  (first run — bootstrap)")

    by_class = {"PRUNE": [], "RAISE_FLOOR": [], "EXPAND": [], "KEEP": []}
    for p in dsps:
        p["classification"] = classify(p, snapshot)
        by_class[p["classification"]].append(p)

    print(
        f"\n  PRUNE:       {len(by_class['PRUNE'])}"
        f"\n  RAISE_FLOOR: {len(by_class['RAISE_FLOOR'])}"
        f"\n  EXPAND:      {len(by_class['EXPAND'])}"
        f"\n  KEEP:        {len(by_class['KEEP'])}"
    )

    actions: list[dict] = []
    applied = 0
    if by_class["PRUNE"] and not bootstrap:
        top = sorted(by_class["PRUNE"], key=lambda x: -x["bid_responses"])[:MAX_PRUNES_PER_RUN]
        id_map = map_to_ids([p["company_dsp"] for p in top], catalog)
        prune_ids = list(id_map.values())
        if prune_ids:
            actions = apply_prunes(prune_ids, inventories, catalog, dry_run=dry_run)
            applied = sum(1 for a in actions if a.get("applied"))

    recs = {
        "window":      {"start": start, "end": end},
        "timestamp":   datetime.now(timezone.utc).isoformat(),
        "dry_run":     dry_run,
        "bootstrap":   bootstrap,
        "total_dsps":  len(dsps),
        "by_class":    by_class,
        "applied":     applied,
    }
    with open(RECS_FILE, "w") as f:
        json.dump(recs, f, indent=2)
    print(f"\n  Recs → {RECS_FILE}")

    if actions:
        append_actions_log(actions)

    save_snapshot(update_snapshot(snapshot, dsps))
    post_slack(recs, applied, bootstrap, dry_run)

    def _rk(b, k): return sorted(b, key=lambda x: -x[k])
    for label, bucket, key in [
        ("PRUNE",       by_class["PRUNE"],       "bid_responses"),
        ("RAISE_FLOOR", by_class["RAISE_FLOOR"], "impressions"),
        ("EXPAND",      by_class["EXPAND"],      "dsp_spend"),
    ]:
        if not bucket: continue
        print(f"\n  Top {label}:")
        for p in _rk(bucket, key)[:10]:
            print(
                f"    {p['company_dsp'][:42]:<42}  "
                f"resp={p['bid_responses']/1e6:>5.1f}M  "
                f"imps={p['impressions']:>8,}  "
                f"spend=${p['dsp_spend']:>7.2f}  "
                f"eCPM=${p['dsp_ecpm']:>4.2f}  "
                f"margin={p['margin']*100:>5.1f}%"
            )
    return recs


if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true",
                    help="execute inventory whitelist removals live")
    args = ap.parse_args()
    run(dry_run=not args.apply)
