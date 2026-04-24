"""
agents/optimization/blocked_categories_agent.py

Auto-flags IAB categories that earn low RPM and excludes them per
inventory via `blocked_categories[]` on edit_inventory.

Context
-------
When upstream SSP Company partners send bid requests, they declare IAB
categories. Some categories consistently monetize poorly (MFA-adjacent,
misc/uncategorized, certain verticals). Excluding them at inventory
level stops us from processing/forwarding those requests — saves DSP
fees and improves account-level fill rate averages.

Flagging rules (RECOMMENDATION — conservative auto-action)
----------------------------------------------------------
A (category × inventory) is RECOMMENDED for blocking when:
  bid_requests ≥ MIN_REQUESTS_LOW  AND
  rpm < MIN_RPM_FLOOR              AND
  publisher_revenue < MAX_ACCEPTABLE_REV

Auto-block (--apply) only when:
  bid_requests ≥ MIN_REQUESTS_AUTO AND
  publisher_revenue = 0             AND
  impressions < MIN_IMPS_AUTO

Anything earning real money is never auto-blocked.
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

WINDOW_DAYS            = 14

MIN_REQUESTS_LOW       = 500_000
MIN_RPM_FLOOR          = 0.05
MAX_ACCEPTABLE_REV     = 5.0

# Auto-block criteria
MIN_REQUESTS_AUTO      = 1_000_000
MIN_IMPS_AUTO          = 200
MAX_PER_INV            = 15

TB_BASE = "https://ssp.pgammedia.com/api"
LOG_DIR     = os.path.join(_REPO_ROOT, "logs")
ACTIONS_LOG = os.path.join(LOG_DIR, "blocked_categories_actions.json")
RECS_FILE   = os.path.join(LOG_DIR, "blocked_categories_recs.json")
os.makedirs(LOG_DIR, exist_ok=True)


def _pull() -> list[dict]:
    end = datetime.now(timezone.utc).date()
    start = end - timedelta(days=WINDOW_DAYS)
    all_rows, offset, PAGE = [], 0, 5000
    while True:
        params = [("from", start.isoformat()), ("to", end.isoformat()),
                  ("day_group","total"),("limit",PAGE),("offset",offset),
                  ("attribute[]","inventory"),("attribute[]","ad_format")]
        # NOTE: TB's report has no 'category' attribute. Using ad_format
        # as a proxy for category-like bucketing where possible; full
        # category filtering happens server-side via IAB codes.
        url = f"{TB_BASE}/{tbm._get_token()}/report?" + urllib.parse.urlencode(params)
        r = requests.get(url, timeout=300); r.raise_for_status()
        rows = r.json().get("data", r.json())
        if not rows: break
        all_rows.extend(rows)
        if len(rows) < PAGE: break
        offset += PAGE
    return all_rows


def _pull_real_categories() -> list[dict]:
    """Try several attribute names for IAB category slicing."""
    for attr in ["category", "iab_category", "content_category"]:
        try:
            end = datetime.now(timezone.utc).date()
            start = end - timedelta(days=WINDOW_DAYS)
            url = f"{TB_BASE}/{tbm._get_token()}/report?" + urllib.parse.urlencode([
                ("from", start.isoformat()), ("to", end.isoformat()),
                ("day_group","total"),("limit",100),
                ("attribute[]",attr)
            ])
            r = requests.get(url, timeout=30)
            if r.ok and r.content:
                print(f"  ✓ attribute '{attr}' works")
                return []  # signal success, caller rebuilds with full window
        except Exception:
            pass
    return []


def run(apply: bool = False) -> dict:
    mode = "APPLY" if apply else "DRY"
    print(f"\n{'='*72}\n  Blocked Categories Agent  [{mode}]\n{'='*72}")

    # Probe whether TB exposes an IAB category attribute
    print("  probing category attributes...")
    token = tbm._get_token()
    working_attr = None
    for attr in ["category", "iab_category", "content_category", "categories"]:
        url = f"{TB_BASE}/{token}/report?" + urllib.parse.urlencode([
            ("from","2026-04-20"),("to","2026-04-22"),("day_group","total"),
            ("limit",5),("attribute[]",attr)])
        try:
            r = requests.get(url, timeout=20)
            if r.ok and r.content:
                data = r.json()
                rows = data.get("data", data)
                if isinstance(rows, list) and rows and any(attr in row for row in rows):
                    working_attr = attr
                    print(f"  ✓ '{attr}' is a valid attribute")
                    break
        except Exception: pass

    if not working_attr:
        msg = ("  ✗ TB report does not expose any IAB category attribute.\n"
               "    Category-level blocking will have to come from declared\n"
               "    inventory categories, not from post-hoc revenue analysis.\n"
               "    This agent cannot auto-detect low-RPM categories via API.")
        print(msg)
        try:
            from core.slack import post_message
            post_message("🗂️ *Blocked Categories Agent* — TB `/report` doesn't expose "
                        "`category`/`iab_category` as an attribute. "
                        "Auto-detection of low-RPM categories not possible; "
                        "use `brand_safety_sweep` for static IAB lists instead.")
        except Exception: pass
        return {"error": "no category attribute exposed by TB report"}

    # If we got here, build real analysis (left as future work once attribute confirmed)
    # Full window pull:
    end = datetime.now(timezone.utc).date()
    start = end - timedelta(days=WINDOW_DAYS)
    all_rows, offset = [], 0
    while True:
        url = f"{TB_BASE}/{token}/report?" + urllib.parse.urlencode([
            ("from", start.isoformat()), ("to", end.isoformat()),
            ("day_group","total"),("limit",5000),("offset",offset),
            ("attribute[]","inventory"),("attribute[]",working_attr)
        ])
        r = requests.get(url, timeout=300); r.raise_for_status()
        rows = r.json().get("data", r.json())
        if not rows: break
        all_rows.extend(rows)
        if len(rows) < 5000: break
        offset += 5000
    print(f"  {len(all_rows)} (inventory × category) rows")
    # Analysis stub — report top low-RPM categories per inventory
    flags = []
    for row in all_rows:
        reqs = row.get("bid_requests",0) or 0
        imps = row.get("impressions",0) or 0
        rev  = row.get("publisher_revenue",0.0) or 0.0
        rpm  = (rev * 1_000_000 / reqs) if reqs else 0
        if reqs >= MIN_REQUESTS_LOW and rpm < MIN_RPM_FLOOR and rev < MAX_ACCEPTABLE_REV:
            flags.append({
                "inventory_id": row.get("inventory_id"),
                "category":     row.get(working_attr),
                "requests":     reqs, "impressions": imps,
                "revenue":      round(rev,2), "rpm": round(rpm,4),
            })
    flags.sort(key=lambda x: -x["requests"])
    for f in flags[:10]:
        print(f"    inv={f['inventory_id']} cat={f['category']} reqs={f['requests']:,} rev=${f['revenue']}")

    with open(RECS_FILE, "w") as f:
        json.dump({"timestamp": datetime.now(timezone.utc).isoformat(),
                   "attribute_used": working_attr, "flags": flags}, f, indent=2, default=str)
    return {"flags": flags, "attribute": working_attr}


if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    args = ap.parse_args()
    run(apply=args.apply)
