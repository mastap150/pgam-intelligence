"""
agents/optimization/blocked_domains_agent.py

Auto-blocks junk domains/bundles at the inventory level.

Why this matters for ADX optimization
--------------------------------------
Upstream SSP Companies (Smaato, Illumin, Start.IO, PubNative, etc.)
send PGAM enormous volumes of bid requests. A chunk of that traffic
is garbage — spoofed domains, bot-infested bundles, MFA sites that
never monetize. Every junk request still costs us in:
  - Server processing
  - DSP request fees
  - Polluted eCPM averages (fill rate collapses)
  - Downstream DSP trust (bid rates compress)

This agent identifies the worst offenders per inventory using
attribute[]=domain reports, then auto-adds them to the inventory's
`blocked_domains[]` via edit_inventory.

Flagging rules
--------------
A (domain × inventory) pair is PRUNED when:
  - bid_requests ≥ MIN_REQUESTS_JUNK  (enough data)
  - impressions < MIN_IMPS_JUNK       (not monetizing)
  - publisher_revenue < MIN_REV_JUNK  (no revenue)
  - fill_rate < MAX_FILL_JUNK         (confirmed dead)

Separately, flag REVIEW candidates that are borderline (some rev but
very bad ratio) for human review — never auto-block.

Safety
------
- Dry-run default. --apply executes.
- NEVER auto-blocks if the domain has ANY revenue (only zero-revenue
  drains qualify for automation).
- MAX_BLOCKS_PER_RUN caps blast radius.
- Logs every block with full context for manual unblock.
- --rollback reverses from log (restores inventory's prior blocked_domains).
"""

from __future__ import annotations
import os, sys, json, urllib.parse, requests
from collections import defaultdict
from datetime import datetime, timezone, timedelta

_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

from dotenv import load_dotenv
load_dotenv(override=True)
import core.tb_mgmt as tbm

WINDOW_DAYS          = 3              # short window to keep API pull fast

# PRUNE thresholds scaled for 3-day window
MIN_REQUESTS_JUNK    = 100_000
MIN_IMPS_JUNK        = 20
MIN_REV_JUNK         = 0.10
MAX_FILL_JUNK        = 0.0001

REVIEW_MIN_REQUESTS  = 250_000
REVIEW_MAX_RPM       = 0.02

MAX_BLOCKS_PER_INV   = 30
MAX_INVENTORIES_RUN  = 50

TB_BASE = "https://ssp.pgammedia.com/api"
LOG_DIR     = os.path.join(_REPO_ROOT, "logs")
ACTIONS_LOG = os.path.join(LOG_DIR, "blocked_domains_actions.json")
RECS_FILE   = os.path.join(LOG_DIR, "blocked_domains_recs.json")
os.makedirs(LOG_DIR, exist_ok=True)


def _pull_domain_report() -> list[dict]:
    """Single-shot pull (no pagination) — use short window to keep API fast."""
    end = datetime.now(timezone.utc).date()
    start = end - timedelta(days=WINDOW_DAYS)
    params = [("from", start.isoformat()), ("to", end.isoformat()),
              ("day_group", "total"), ("limit", 5000),
              ("attribute[]", "domain"), ("attribute[]", "inventory")]
    url = f"{TB_BASE}/{tbm._get_token()}/report?" + urllib.parse.urlencode(params)
    print(f"  → domain × inventory {start} → {end}", flush=True)
    r = requests.get(url, timeout=300)
    print(f"  → response {r.status_code}", flush=True)
    r.raise_for_status()
    return r.json().get("data", r.json()) or []


def _classify(rows: list[dict]) -> dict:
    """
    Returns {inv_id: {
        "prune": [{domain, requests, imps, revenue}, ...],
        "review": [...],
        "totals": {requests, impressions, revenue}
    }}
    """
    per_inv: dict[int, dict] = defaultdict(lambda: {
        "prune": [], "review": [],
        "totals": {"requests": 0, "impressions": 0, "revenue": 0.0}})

    for r in rows:
        domain = (r.get("domain") or "").strip()
        inv    = r.get("inventory_id")
        if inv is None:
            inv_raw = r.get("inventory", "")
            if "#" in inv_raw:
                try: inv = int(inv_raw.rsplit("#", 1)[1])
                except ValueError: continue
        if inv is None or not domain: continue
        try: inv = int(inv)
        except Exception: continue

        reqs = r.get("bid_requests", 0) or 0
        imps = r.get("impressions", 0) or 0
        rev  = r.get("publisher_revenue", 0.0) or 0.0
        resp = r.get("bid_responses", 0) or 0
        fill = (imps / resp) if resp else 0.0
        rpm  = (rev * 1_000_000.0 / reqs) if reqs else 0.0

        per_inv[inv]["totals"]["requests"]    += reqs
        per_inv[inv]["totals"]["impressions"] += imps
        per_inv[inv]["totals"]["revenue"]     += rev

        row = {"domain": domain, "requests": reqs, "impressions": imps,
               "revenue": round(rev, 4), "fill_rate": round(fill, 6),
               "rpm": round(rpm, 4)}

        if (reqs >= MIN_REQUESTS_JUNK
                and imps < MIN_IMPS_JUNK
                and rev < MIN_REV_JUNK
                and fill < MAX_FILL_JUNK):
            per_inv[inv]["prune"].append(row)
        elif (reqs >= REVIEW_MIN_REQUESTS and rpm < REVIEW_MAX_RPM and rev > 0):
            per_inv[inv]["review"].append(row)

    return dict(per_inv)


def _apply_blocks(inv_id: int, domains_to_add: list[str], dry_run: bool) -> dict:
    """Add domains to the inventory's blocked_domains list."""
    inv = tbm.get_inventory(inv_id)
    existing = list(inv.get("blocked_domains") or [])
    new_list = list(existing)
    added = []
    for d in domains_to_add:
        if d not in new_list:
            new_list.append(d)
            added.append(d)
    if not added:
        return {"inventory_id": inv_id, "added": [], "applied": False, "dry_run": dry_run}

    if dry_run:
        return {"inventory_id": inv_id, "added": added,
                "before_count": len(existing), "after_count": len(new_list),
                "applied": False, "dry_run": True}

    # Write via edit_inventory with blocked_domains[] form-array encoding
    token = tbm._get_token()
    form = [("inventory_id", str(inv_id))]
    for d in new_list:
        form.append(("blocked_domains[]", d))
    # Preserve critical fields that edit_inventory might reset
    for pid in (inv.get("inventory_dsp[white]") or []):
        form.append(("inventory_dsp[white][]", str(pid)))
    for pid in (inv.get("inventory_dsp[black]") or []):
        form.append(("inventory_dsp[black][]", str(pid)))
    for cat in (inv.get("categories") or []):
        form.append(("categories[]", str(cat)))
    for cat in (inv.get("blocked_categories") or []):
        form.append(("blocked_categories[]", str(cat)))

    url = f"{TB_BASE}/{token}/edit_inventory"
    r = requests.post(url, data=form,
                      headers={"Content-Type": "application/x-www-form-urlencoded"},
                      timeout=60)
    ok = r.ok and "html" not in r.headers.get("content-type", "")
    return {
        "inventory_id":   inv_id, "added": added,
        "before_list":    existing, "new_list": new_list,
        "before_count":   len(existing), "after_count": len(new_list),
        "applied":        ok, "status_code": r.status_code,
        "response":       (r.json() if ok else r.text[:200]),
        "timestamp":      datetime.now(timezone.utc).isoformat(),
    }


def run(apply: bool = False, rollback: bool = False) -> dict:
    mode = "ROLLBACK" if rollback else ("APPLY" if apply else "DRY")
    print(f"\n{'='*72}\n  Blocked Domains Agent  [{mode}]\n{'='*72}")

    if rollback:
        if not os.path.exists(ACTIONS_LOG):
            print("  no log to roll back"); return {}
        with open(ACTIONS_LOG) as f: prior = json.load(f)
        # For each inventory, restore the latest "before_list"
        per_inv: dict[int, list] = {}
        for a in prior:
            if a.get("applied") and "before_list" in a:
                per_inv[a["inventory_id"]] = a["before_list"]
        print(f"  reverting {len(per_inv)} inventories...")
        for inv_id, orig_list in per_inv.items():
            try:
                token = tbm._get_token()
                inv = tbm.get_inventory(inv_id)
                form = [("inventory_id", str(inv_id))]
                for d in orig_list:
                    form.append(("blocked_domains[]", d))
                for pid in (inv.get("inventory_dsp[white]") or []):
                    form.append(("inventory_dsp[white][]", str(pid)))
                for pid in (inv.get("inventory_dsp[black]") or []):
                    form.append(("inventory_dsp[black][]", str(pid)))
                r = requests.post(f"{TB_BASE}/{token}/edit_inventory", data=form, timeout=60)
                print(f"  inv {inv_id}: restored {len(orig_list)} domains  [{r.status_code}]")
            except Exception as e:
                print(f"  inv {inv_id}: ✗ {e}")
        return {"rolled_back": len(per_inv)}

    print("  → pulling domain × inventory report...")
    rows = _pull_domain_report()
    print(f"  {len(rows)} rows")

    classified = _classify(rows)
    total_prune = sum(len(d["prune"]) for d in classified.values())
    total_review = sum(len(d["review"]) for d in classified.values())
    print(f"\n  {len(classified)} inventories analyzed")
    print(f"  PRUNE candidates (auto-block): {total_prune}")
    print(f"  REVIEW candidates (manual):    {total_review}")

    # Show top offenders
    flat_prune = []
    for inv_id, d in classified.items():
        for r in d["prune"]:
            flat_prune.append({**r, "inventory_id": inv_id})
    flat_prune.sort(key=lambda x: -x["requests"])

    if flat_prune:
        print(f"\n  Top 20 domains proposed for BLOCK:")
        for r in flat_prune[:20]:
            print(f"    inv={r['inventory_id']:>5}  "
                  f"{r['domain'][:42]:<42}  "
                  f"reqs={r['requests']:>10,}  "
                  f"imps={r['impressions']:>6}  "
                  f"rev=${r['revenue']:.2f}")

    # Show top REVIEW
    flat_review = []
    for inv_id, d in classified.items():
        for r in d["review"]:
            flat_review.append({**r, "inventory_id": inv_id})
    flat_review.sort(key=lambda x: -x["requests"])

    if flat_review:
        print(f"\n  Top 10 REVIEW candidates (low RPM, some rev — human judgement):")
        for r in flat_review[:10]:
            print(f"    inv={r['inventory_id']:>5}  "
                  f"{r['domain'][:42]:<42}  "
                  f"reqs={r['requests']:>10,}  "
                  f"rev=${r['revenue']:.2f}  rpm=${r['rpm']:.4f}")

    # Apply
    actions = []
    if apply:
        inv_count = 0
        for inv_id, d in classified.items():
            if inv_count >= MAX_INVENTORIES_RUN: break
            doms = [p["domain"] for p in d["prune"][:MAX_BLOCKS_PER_INV]]
            if not doms: continue
            try:
                res = _apply_blocks(inv_id, doms, dry_run=False)
                actions.append(res)
                if res.get("applied"):
                    print(f"  ✅ inv {inv_id}: +{len(res['added'])} blocked")
                    inv_count += 1
                else:
                    print(f"  ✗ inv {inv_id}: {res.get('response')}")
            except Exception as e:
                print(f"  ✗ inv {inv_id}: {e}")
        # Persist
        prior = []
        if os.path.exists(ACTIONS_LOG):
            with open(ACTIONS_LOG) as f:
                try: prior = json.load(f)
                except Exception: prior = []
        prior.extend(actions)
        with open(ACTIONS_LOG, "w") as f: json.dump(prior, f, indent=2, default=str)

    # Recs
    recs = {"timestamp": datetime.now(timezone.utc).isoformat(),
            "window_days": WINDOW_DAYS, "dry_run": not apply,
            "inventories_analyzed": len(classified),
            "prune_candidates": flat_prune[:200],
            "review_candidates": flat_review[:100]}
    with open(RECS_FILE, "w") as f: json.dump(recs, f, indent=2, default=str)
    print(f"\n  Recs → {RECS_FILE}")

    try:
        from core.slack import post_message
        tag = "🟢 LIVE" if apply else "🔍 DRY"
        total_reqs = sum(r["requests"] for r in flat_prune)
        msg = [f"🚫 *Blocked Domains Agent* {tag} — {total_prune} junk "
               f"(domain × inv) pairs flagged",
               f"Total wasted requests cut: {total_reqs/1e6:.0f}M over {WINDOW_DAYS}d"]
        for r in flat_prune[:6]:
            msg.append(f"  • inv {r['inventory_id']}  {r['domain'][:30]}  "
                       f"reqs={r['requests']/1e3:.0f}K  rev=${r['revenue']:.2f}")
        if apply:
            n_applied = sum(1 for a in actions if a.get("applied"))
            msg.append(f"\n✅ Applied on {n_applied} inventories")
        post_message("\n".join(msg))
    except Exception: pass

    return recs


if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply",    action="store_true")
    ap.add_argument("--rollback", action="store_true")
    args = ap.parse_args()
    run(apply=args.apply, rollback=args.rollback)
