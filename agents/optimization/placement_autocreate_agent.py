"""
agents/optimization/placement_autocreate_agent.py

Auto-creates TB banner placements when a publisher's site starts
sending requests for a size that doesn't have a matching placement yet.

Why this exists
---------------
When a publisher adds a new ad slot to their site template (e.g. a
new 300x600 unit), TB starts receiving bid requests for that size on
their inventory. If we haven't created a matching placement, those
requests silently go unmatched and earn $0.

This agent watches the last 48h of requests for "new" (size × inventory)
pairs that aren't yet represented by a placement, then auto-creates
the matching TB placement so the new slot starts monetizing immediately.

Safety
------
- Only creates when size has ≥MIN_REQUESTS_TRIGGER bid_requests (proof
  the slot is live on the publisher side).
- Only for inventories in AUTOCREATE_ALLOWLIST (starts empty — you add
  inventory_ids here after publisher confirms readiness).
- Uses sensible defaults: status=true, is_optimal_price=true,
  price=DEFAULT_FLOOR, is_all_sizes=false.
- Dry-run default. --apply creates live.
- MAX_CREATES_PER_RUN caps blast radius.
- Full audit log for rollback via edit_placement_banner status=false.
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

WINDOW_HOURS          = 48
MIN_REQUESTS_TRIGGER  = 10_000
DEFAULT_FLOOR         = 0.05
MAX_CREATES_PER_RUN   = 10

# Allowlist: set[int] of inventory IDs cleared for auto-creation.
# Populate after each publisher confirms their dev team has added the
# ad slot HTML. Until then, an inventory stays on the recommendation
# list only.
AUTOCREATE_ALLOWLIST: set[int] = set()

TB_BASE = "https://ssp.pgammedia.com/api"
LOG_DIR     = os.path.join(_REPO_ROOT, "logs")
ACTIONS_LOG = os.path.join(LOG_DIR, "placement_autocreate_actions.json")
RECS_FILE   = os.path.join(LOG_DIR, "placement_autocreate_recs.json")
os.makedirs(LOG_DIR, exist_ok=True)


def _size_inv_recent() -> list[dict]:
    """Pull size × inventory requests for the short recent window."""
    end   = datetime.now(timezone.utc).date()
    start = end - timedelta(days=max(1, WINDOW_HOURS // 24))
    params = [("from", start.isoformat()), ("to", end.isoformat()),
              ("day_group", "total"), ("limit", 5000),
              ("attribute[]", "size"), ("attribute[]", "inventory")]
    url = f"{TB_BASE}/{tbm._get_token()}/report?" + urllib.parse.urlencode(params)
    r = requests.get(url, timeout=300)
    r.raise_for_status()
    return r.json().get("data", r.json()) or []


def _parse_size(s: str) -> tuple[int, int] | None:
    try:
        w, h = s.split("x", 1)
        return int(w), int(h)
    except Exception:
        return None


def _existing_sizes_per_inv(placements: list[dict]) -> dict[int, set[str]]:
    """For each inventory, which sizes does it ALREADY have placements for?"""
    out: dict[int, set[str]] = defaultdict(set)
    for p in placements:
        inv = p.get("inventory_id")
        if inv is None: continue
        banner = p.get("banner") or {}
        for sz in banner.get("sizes", []) or []:
            w, h = sz.get("width"), sz.get("height")
            if w and h:
                out[int(inv)].add(f"{w}x{h}")
    return out


def _create_placement_banner(
    inventory_id: int, size: str, floor: float, dry_run: bool,
) -> dict:
    parsed = _parse_size(size)
    if not parsed:
        return {"applied": False, "error": f"bad size {size}"}
    w, h = parsed
    title = f"autocreate_{inventory_id}_{size}"
    form = [
        ("inventory_id",       str(inventory_id)),
        ("title",              title),
        ("price",              str(floor)),
        ("is_optimal_price",   "true"),
        ("status",             "true"),
        ("is_all_sizes",       "false"),
        ("sizes[0][width]",    str(w)),
        ("sizes[0][height]",   str(h)),
        ("position",           "0"),
    ]
    if dry_run:
        return {"applied": False, "dry_run": True, "title": title,
                "inventory_id": inventory_id, "size": size, "floor": floor}
    url = f"{TB_BASE}/{tbm._get_token()}/create_placement_banner"
    r = requests.post(url, data=form,
                      headers={"Content-Type": "application/x-www-form-urlencoded"},
                      timeout=30)
    ok = r.ok and "html" not in r.headers.get("content-type", "")
    try: body = r.json()
    except Exception: body = r.text[:200]
    return {
        "applied":      ok,
        "status_code":  r.status_code,
        "title":        title,
        "inventory_id": inventory_id,
        "size":         size,
        "floor":        floor,
        "response":     body,
    }


def run(dry_run: bool = True) -> dict:
    print(f"\n{'='*72}\n  Placement Auto-Create Agent  "
          f"{'[DRY RUN]' if dry_run else '[LIVE]'}\n{'='*72}")

    rows = _size_inv_recent()
    placements = tbm.list_all_placements_via_report(days=14, min_impressions=0)
    existing = _existing_sizes_per_inv(placements)
    print(f"  {len(rows)} size×inv rows  |  {len(placements)} placements  "
          f"|  allowlist={len(AUTOCREATE_ALLOWLIST)} inventories")

    # Find (inv, size) with traffic but no matching placement
    candidates = []
    for r in rows:
        sz  = (r.get("size") or "").strip()
        inv = r.get("inventory_id") or r.get("inventory")
        if isinstance(inv, str) and "#" in inv:
            try: inv = int(inv.rsplit("#", 1)[1])
            except ValueError: continue
        try: inv = int(inv)
        except Exception: continue
        if not sz: continue
        if not _parse_size(sz): continue
        if sz in existing.get(inv, set()): continue
        reqs = r.get("bid_requests", 0) or 0
        if reqs < MIN_REQUESTS_TRIGGER: continue
        candidates.append({
            "inventory_id": inv, "size": sz,
            "bid_requests": reqs,
            "impressions":  r.get("impressions", 0) or 0,
            "publisher_revenue": r.get("publisher_revenue", 0.0) or 0.0,
        })
    candidates.sort(key=lambda x: -x["bid_requests"])
    print(f"\n  {len(candidates)} (inventory × size) gaps with ≥{MIN_REQUESTS_TRIGGER:,} reqs")

    # Hydrate titles
    inv_titles = {}
    for c in candidates:
        iv = c["inventory_id"]
        if iv not in inv_titles:
            try: inv_titles[iv] = tbm.get_inventory(iv).get("title", "?")
            except Exception: inv_titles[iv] = "?"
        c["inventory_title"] = inv_titles[iv]
        c["in_allowlist"]    = iv in AUTOCREATE_ALLOWLIST

    for c in candidates[:15]:
        tag = "✅ allowlisted" if c["in_allowlist"] else "⏸ awaiting allowlist"
        print(f"    inv={c['inventory_id']:>5} {c['inventory_title'][:22]:<22} "
              f"size={c['size']:<12} reqs={c['bid_requests']:>9,}  {tag}")

    # Execute on allowlisted
    actions = []
    created = 0
    for c in candidates:
        if not c["in_allowlist"]: continue
        if created >= MAX_CREATES_PER_RUN: break
        res = _create_placement_banner(
            c["inventory_id"], c["size"], DEFAULT_FLOOR, dry_run=dry_run,
        )
        res["timestamp"] = datetime.now(timezone.utc).isoformat()
        res["candidate"] = c
        actions.append(res)
        if res.get("applied"):
            created += 1
            print(f"    ✅ CREATED inv={c['inventory_id']} size={c['size']} @ ${DEFAULT_FLOOR}")
        elif not dry_run:
            print(f"    ✗ inv={c['inventory_id']} size={c['size']}: {res.get('response')}")

    # Persist
    prior = []
    if os.path.exists(ACTIONS_LOG):
        with open(ACTIONS_LOG) as f:
            try: prior = json.load(f)
            except Exception: prior = []
    prior.extend(actions)
    with open(ACTIONS_LOG, "w") as f:
        json.dump(prior, f, indent=2, default=str)

    recs = {"timestamp": datetime.now(timezone.utc).isoformat(),
            "candidates": candidates, "dry_run": dry_run,
            "created": created}
    with open(RECS_FILE, "w") as f:
        json.dump(recs, f, indent=2)
    print(f"\n  Created: {created}  |  Recs → {RECS_FILE}")

    try:
        from core.slack import post_message
        n_allow = sum(1 for c in candidates if c["in_allowlist"])
        n_wait  = sum(1 for c in candidates if not c["in_allowlist"])
        msg = [f"🧩 *Placement Auto-Create* — {len(candidates)} (inv × size) gaps detected"]
        msg.append(f"  ✅ {n_allow} allowlisted  ⏸ {n_wait} awaiting publisher confirmation")
        if created:
            msg.append(f"  🟢 Created {created} placements this run")
        for c in candidates[:5]:
            tag = "✅" if c["in_allowlist"] else "⏸"
            msg.append(f"  {tag} inv {c['inventory_id']} {c['inventory_title'][:22]}  "
                       f"{c['size']}  reqs={c['bid_requests']:,}")
        post_message("\n".join(msg))
    except Exception: pass

    return recs


if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    args = ap.parse_args()
    run(dry_run=not args.apply)
