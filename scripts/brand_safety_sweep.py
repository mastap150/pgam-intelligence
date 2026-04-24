"""
scripts/brand_safety_sweep.py

One-shot: apply standard brand-safety IAB category blocklist across all
inventories.

Why
---
Unless a publisher specifically opts into adult/gambling/illegal/MFA
categories, they should be blocked by default. This protects:
  - DSP trust scores (your inventory's IAB category exposure affects
    how major DSPs rank you)
  - Advertiser brand safety (buyers penalize inventories serving
    unsafe content)
  - Margin (these categories typically earn <$0.05 RPM anyway)

Blocklist applied
-----------------
IAB standard categories considered unsafe-by-default:
  IAB7-39  Sexuality (Adult)
  IAB9-9   Gambling (casinos, wagering)
  IAB11    Law, Government, Politics (often brand-unsafe)
  IAB14-4  Dating (mixed — optional toggle)
  IAB23    Religion & Spirituality (optional per publisher)
  IAB24    Uncategorized (proxy for MFA-like content)
  IAB25    Non-Standard Content (profanity, hate speech, violence)
  IAB26    Illegal Content (drugs, piracy, fraud)

Usage
-----
    python3 -m scripts.brand_safety_sweep                  # dry-run
    python3 -m scripts.brand_safety_sweep --apply          # apply
    python3 -m scripts.brand_safety_sweep --rollback       # revert
"""
from __future__ import annotations
import os, sys, json, urllib.parse, requests
from datetime import datetime, timezone
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from dotenv import load_dotenv; load_dotenv(override=True)
import core.tb_mgmt as tbm

# Conservative default blocklist — these never hurt legitimate revenue
DEFAULT_BLOCKLIST = [
    "IAB7-39",   # Sexuality/Adult
    "IAB9-9",    # Gambling
    "IAB24",     # Uncategorized (MFA proxy)
    "IAB25",     # Non-Standard Content
    "IAB26",     # Illegal Content
]

# Opt-in (not applied by default — uncomment to enable)
AGGRESSIVE_BLOCKLIST = [
    "IAB11",     # Law/Government/Politics
    "IAB14-4",   # Dating
    "IAB23",     # Religion
]

# Publishers who've explicitly opted IN to certain categories
EXEMPT_INVENTORIES: dict[int, list[str]] = {
    # inventory_id: [list of IAB codes to NOT block for this inventory]
    # e.g. 544: ["IAB9-9"]  # Modrinth allows gambling?
}

TB_BASE = "https://ssp.pgammedia.com/api"
LOG_FILE = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                        "logs", "brand_safety_sweep_log.json")


def _write_inventory(inv_id: int, blocked_cats: list[str], dry_run: bool) -> dict:
    inv = tbm.get_inventory(inv_id)
    existing = list(inv.get("blocked_categories") or [])
    added    = [c for c in blocked_cats if c not in existing]
    if not added:
        return {"inventory_id": inv_id, "added": [], "applied": False, "no_op": True}
    new_list = existing + added
    if dry_run:
        return {"inventory_id": inv_id, "title": inv.get("title"),
                "before_categories": existing, "added": added,
                "applied": False, "dry_run": True}

    token = tbm._get_token()
    form = [("inventory_id", str(inv_id))]
    for c in new_list:
        form.append(("blocked_categories[]", c))
    # Preserve everything else
    for pid in (inv.get("inventory_dsp[white]") or []):
        form.append(("inventory_dsp[white][]", str(pid)))
    for pid in (inv.get("inventory_dsp[black]") or []):
        form.append(("inventory_dsp[black][]", str(pid)))
    for cat in (inv.get("categories") or []):
        form.append(("categories[]", str(cat)))
    for dom in (inv.get("blocked_domains") or []):
        form.append(("blocked_domains[]", dom))

    r = requests.post(f"{TB_BASE}/{token}/edit_inventory", data=form,
                      headers={"Content-Type":"application/x-www-form-urlencoded"},
                      timeout=60)
    ok = r.ok and "html" not in r.headers.get("content-type","")
    return {"inventory_id": inv_id, "title": inv.get("title"),
            "before_categories": existing, "added": added,
            "after_categories": new_list, "applied": ok,
            "status_code": r.status_code,
            "timestamp": datetime.now(timezone.utc).isoformat()}


def _all_inventories() -> list[dict]:
    """Account-wide — scan user IDs 1-250."""
    token = tbm._get_token()
    out, seen = [], set()
    for uid in range(1, 250):
        try:
            r = requests.get(f"{TB_BASE}/{token}/list_inventory/{uid}", timeout=10)
            if r.status_code == 200 and r.content:
                data = r.json()
                if isinstance(data, list):
                    for inv in data:
                        iid = inv.get("inventory_id")
                        if iid and iid not in seen:
                            seen.add(iid); out.append(inv)
        except Exception: pass
    return out


def run(apply: bool = False, rollback: bool = False, aggressive: bool = False):
    mode = "ROLLBACK" if rollback else ("APPLY" if apply else "DRY")
    blocklist = DEFAULT_BLOCKLIST + (AGGRESSIVE_BLOCKLIST if aggressive else [])
    print(f"\n{'='*70}\n  Brand Safety Sweep  [{mode}]\n{'='*70}")
    print(f"  Blocklist ({len(blocklist)}): {', '.join(blocklist)}")

    if rollback:
        if not os.path.exists(LOG_FILE): print("  no log"); return
        with open(LOG_FILE) as f: prior = json.load(f)
        per_inv: dict[int, list] = {}
        for a in prior:
            if a.get("applied") and "before_categories" in a:
                per_inv[a["inventory_id"]] = a["before_categories"]
        print(f"  reverting {len(per_inv)} inventories...")
        for inv_id, orig in per_inv.items():
            try:
                inv = tbm.get_inventory(inv_id)
                token = tbm._get_token()
                form = [("inventory_id", str(inv_id))]
                for c in orig: form.append(("blocked_categories[]", c))
                for pid in (inv.get("inventory_dsp[white]") or []):
                    form.append(("inventory_dsp[white][]", str(pid)))
                for dom in (inv.get("blocked_domains") or []):
                    form.append(("blocked_domains[]", dom))
                r = requests.post(f"{TB_BASE}/{token}/edit_inventory", data=form, timeout=60)
                print(f"  inv {inv_id}: restored ({r.status_code})")
            except Exception as e:
                print(f"  inv {inv_id}: ✗ {e}")
        return

    inventories = _all_inventories()
    print(f"\n  {len(inventories)} inventories discovered")

    actions = []
    applied_count = 0
    for inv in inventories:
        inv_id = inv["inventory_id"]
        # Remove exempt categories
        this_blocklist = [c for c in blocklist
                          if c not in EXEMPT_INVENTORIES.get(inv_id, [])]
        try:
            res = _write_inventory(inv_id, this_blocklist, dry_run=not apply)
            actions.append(res)
            if res.get("applied"):
                applied_count += 1
                print(f"  ✅ inv {inv_id} ({res.get('title','?')[:22]}) +{len(res['added'])} cats")
            elif res.get("no_op"):
                pass  # already blocked — skip
            elif not apply:
                print(f"  🔍 inv {inv_id} ({inv.get('title','?')[:22]}) would add {res['added']}")
        except Exception as e:
            print(f"  ✗ inv {inv_id}: {e}")

    # Persist
    prior = []
    if os.path.exists(LOG_FILE):
        with open(LOG_FILE) as f:
            try: prior = json.load(f)
            except Exception: prior = []
    prior.extend(actions)
    with open(LOG_FILE,"w") as f: json.dump(prior, f, indent=2)

    print(f"\n  {'APPLIED' if apply else 'WOULD APPLY'}: {applied_count if apply else len([a for a in actions if a.get('added')])} inventories")
    print(f"  Log → {LOG_FILE}")

    try:
        from core.slack import post_message
        tag = "🟢 LIVE" if apply else "🔍 DRY"
        post_message(f"🛡️ *Brand Safety Sweep* {tag} — "
                    f"added IAB blocklist to {applied_count if apply else len([a for a in actions if a.get('added')])} inventories\n"
                    f"Categories blocked: {', '.join(blocklist)}")
    except Exception: pass


if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply",      action="store_true")
    ap.add_argument("--rollback",   action="store_true")
    ap.add_argument("--aggressive", action="store_true",
                    help="include Politics/Dating/Religion in blocklist")
    args = ap.parse_args()
    run(apply=args.apply, rollback=args.rollback, aggressive=args.aggressive)
