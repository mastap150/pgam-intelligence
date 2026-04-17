"""
scripts/tb_floor_optimizer.py
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Dynamic TB floor optimizer — runs every 2 hours on Render.

How it works
------------
1. Pull last 3-day placement-level stats from TB management report API
2. For each placement compute: pub_eCPM, win_rate, 7-day trend
3. Compare current floor to clearing price:
   - Floor too low  (floor < ecpm * 0.40)  → raise  by up to +25%
   - Floor too high (floor > ecpm * 0.75)  → lower  by -15%
   - Floor in range                         → hold
4. Apply changes via edit_placement_{video|banner|native}
5. Post Slack summary

Auth
----
Uses TB_MGMT_EMAIL / TB_MGMT_PASSWORD from .env.
These must be a publisher-owner or admin account with write access
to edit_placement_* endpoints. The reporting account (sagar@) is
read-only and returns 404 on edit endpoints.

To activate:
    1. Add TB_MGMT_EMAIL and TB_MGMT_PASSWORD to .env (admin credentials)
    2. Run manually: python3 scripts/tb_floor_optimizer.py --live
    3. Deploy to Render scheduled job (every 2h)

Floor bounds
------------
  MIN_FLOOR  = $0.01  (never set lower than this)
  MAX_FLOOR  = $2.00  (safety cap)
  MAX_MOVE   = 25%    (max change per cycle)
  MIN_ECPM   = $0.03  (skip placements with very low clearing — no optimization signal)
  MIN_WINS   = 10000  (skip low-volume placements — insufficient data)
"""

import os
import sys
import json
import time
import requests
import urllib.parse
from datetime import date, timedelta
from collections import defaultdict
from dotenv import load_dotenv

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

load_dotenv(override=True)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

TB_BASE        = "https://ssp.pgammedia.com/api"
TOKEN_CACHE    = "/tmp/pgam_tb_mgmt_token.json"

# Management API credentials (must have write access)
MGMT_EMAIL    = os.environ.get("TB_MGMT_EMAIL", os.environ.get("TB_EMAIL", ""))
MGMT_PASSWORD = os.environ.get("TB_MGMT_PASSWORD", os.environ.get("TB_PASSWORD", ""))

# Floor tuning parameters
MIN_FLOOR     = 0.01
MAX_FLOOR     = 2.00
MAX_MOVE_UP   = 0.25   # max +25% raise per cycle
MAX_MOVE_DOWN = 0.15   # max -15% lower per cycle
TARGET_FLOOR_RATIO_LOW  = 0.40  # raise if floor < ecpm * 0.40
TARGET_FLOOR_RATIO_HIGH = 0.75  # lower if floor > ecpm * 0.75
TARGET_FLOOR_RATIO      = 0.55  # target floor = ecpm * 0.55
MIN_ECPM      = 0.03   # skip placements clearing below this
MIN_WINS      = 10_000 # skip placements with fewer wins (3-day window)

# Placements to never touch
LOCKED_PLACEMENT_IDS: set[int] = set()


# ---------------------------------------------------------------------------
# Auth — separate token cache for management credentials
# ---------------------------------------------------------------------------

def _load_mgmt_token() -> str:
    if not os.path.exists(TOKEN_CACHE):
        return ""
    try:
        with open(TOKEN_CACHE) as f:
            data = json.load(f)
        token = data.get("token", "")
        end   = data.get("end", 0)
        if token and (end == 0 or end > time.time() + 300):
            return token
    except Exception:
        pass
    return ""


def _create_mgmt_token() -> str:
    if not MGMT_EMAIL or not MGMT_PASSWORD:
        raise ValueError(
            "TB management credentials not set.\n"
            "Add TB_MGMT_EMAIL and TB_MGMT_PASSWORD to .env\n"
            "(must be a publisher-owner or admin account — the reporting\n"
            "account sagar@pgammedia.com is read-only)"
        )
    body = urllib.parse.urlencode({
        "email":    MGMT_EMAIL,
        "password": MGMT_PASSWORD,
        "time":     0,
    }).encode()
    resp = requests.post(
        f"{TB_BASE}/create_token",
        data=body,
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        timeout=15,
    )
    resp.raise_for_status()
    data = resp.json()
    token = data.get("token", "")
    end   = data.get("end", 0)
    if not token:
        raise RuntimeError(f"Token creation failed: {data}")
    try:
        with open(TOKEN_CACHE, "w") as f:
            json.dump({"token": token, "end": end}, f)
    except OSError:
        pass
    return token


def get_mgmt_token() -> str:
    t = _load_mgmt_token()
    return t if t else _create_mgmt_token()


def test_write_access() -> bool:
    """Verify that the management token has write access to edit_placement_*."""
    token = get_mgmt_token()
    # Quick probe: POST to edit_placement_banner with no placement_id — should return 400 (bad request)
    # not 404 (not found / no access). 404 = read-only account.
    resp = requests.post(
        f"{TB_BASE}/{token}/edit_placement_banner",
        data={"placement_id": ""},
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        timeout=10,
    )
    if resp.status_code == 404:
        print("❌ Write access DENIED — account is read-only.")
        print(f"   Token from: {MGMT_EMAIL}")
        print("   Update TB_MGMT_EMAIL/TB_MGMT_PASSWORD in .env with admin credentials.")
        return False
    elif resp.status_code in (400, 422):
        print(f"✅ Write access confirmed ({resp.status_code} on empty placement_id — expected)")
        return True
    elif resp.status_code == 200:
        print("✅ Write access confirmed (200)")
        return True
    else:
        print(f"⚠️  Unexpected status {resp.status_code}: {resp.text[:100]}")
        return True  # optimistic


# ---------------------------------------------------------------------------
# Data fetching
# ---------------------------------------------------------------------------

def fetch_placement_stats(days: int = 3) -> dict[int, dict]:
    """
    Pull placement-level stats for the last N days from the management API report.
    Returns {placement_id: {pub_ecpm, revenue, wins, bids, win_rate}}.
    """
    from core.tb_api import get_token
    token = get_token()  # read-only is fine for reporting

    end_date   = date.today().isoformat()
    start_date = (date.today() - timedelta(days=days)).isoformat()

    all_rows = []
    for page in range(1, 10):
        params = [
            ("from", start_date), ("to", end_date),
            ("day_group", "total"), ("limit", 1000), ("page", page),
            ("attribute[]", "placement"), ("attribute[]", "ad_format"),
        ]
        url = f"{TB_BASE}/{token}/report?" + urllib.parse.urlencode(params)
        resp = requests.get(url, timeout=60)
        if not resp.ok:
            break
        data = resp.json()
        rows = data.get("data", data) if isinstance(data, dict) else data
        all_rows.extend(rows)
        if page >= (data.get("totalPages", 1) if isinstance(data, dict) else 1):
            break

    # Aggregate
    agg: dict[int, dict] = {}
    for row in all_rows:
        pid = row.get("placement") or row.get("placement_id")
        if not pid:
            continue
        pid = int(pid)
        rev   = float(row.get("publisher_revenue", 0) or 0)
        wins  = int(row.get("wins", 0) or 0)
        bids  = int(row.get("bid_requests", 0) or 0)
        fmt   = str(row.get("ad_format", ""))
        if pid not in agg:
            agg[pid] = {"revenue": 0.0, "wins": 0, "bids": 0, "format": fmt}
        agg[pid]["revenue"] += rev
        agg[pid]["wins"]    += wins
        agg[pid]["bids"]    += bids

    for d in agg.values():
        d["pub_ecpm"]  = (d["revenue"] / d["wins"] * 1000) if d["wins"] > 0 else 0.0
        d["win_rate"]  = (d["wins"] / d["bids"] * 100) if d["bids"] > 0 else 0.0

    return agg


def fetch_current_floors(placement_ids: list[int] | None = None) -> dict[int, dict]:
    """
    Pull current floor for every placement in placement_ids.
    Uses individual GET /placement?placement_id=N calls since list_placement
    only returns placements owned by the current user account.

    Returns {placement_id: {price, type, title, inventory_id, is_optimal_price}}.
    """
    from core.tb_api import get_token

    token = get_token()
    result = {}

    if not placement_ids:
        return result

    print(f"  Fetching floor data for {len(placement_ids)} placements...")
    for pid in placement_ids:
        try:
            resp = requests.get(
                f"{TB_BASE}/{token}/placement",
                params={"placement_id": pid},
                timeout=15,
            )
            if resp.status_code == 200:
                p = resp.json()
                if isinstance(p, dict) and p.get("placement_id"):
                    result[int(p["placement_id"])] = {
                        "price":            float(p.get("price", 0) or 0),
                        "type":             p.get("type", "banner"),
                        "title":            p.get("title", ""),
                        "inventory_id":     p.get("inventory_id"),
                        "is_optimal_price": p.get("is_optimal_price", False),
                    }
        except Exception:
            pass  # skip on error

    return result


# ---------------------------------------------------------------------------
# Floor decision engine
# ---------------------------------------------------------------------------

def compute_floor_changes(
    stats: dict[int, dict],
    floors: dict[int, dict],
) -> list[dict]:
    """
    Compare stats to current floors and return a list of recommended changes.
    Each change: {placement_id, title, type, current_floor, new_floor, reason, pub_ecpm, wins}
    """
    changes = []

    for pid, s in stats.items():
        if pid in LOCKED_PLACEMENT_IDS:
            continue
        if pid not in floors:
            continue

        f = floors[pid]
        if f.get("is_optimal_price", False):
            continue  # TB managing this one dynamically already

        current_floor = f["price"]
        pub_ecpm      = s["pub_ecpm"]
        wins          = s["wins"]

        # Skip low-signal placements
        if pub_ecpm < MIN_ECPM or wins < MIN_WINS:
            continue

        target_floor = round(pub_ecpm * TARGET_FLOOR_RATIO, 4)

        if current_floor < pub_ecpm * TARGET_FLOOR_RATIO_LOW:
            # Floor is too low — raise it
            max_raise  = round(current_floor * (1 + MAX_MOVE_UP), 4) if current_floor > 0 else target_floor
            new_floor  = min(target_floor, max_raise, MAX_FLOOR)
            new_floor  = max(new_floor, MIN_FLOOR)
            if new_floor <= current_floor:
                continue
            reason = f"floor {current_floor:.4f} < {TARGET_FLOOR_RATIO_LOW:.0%} × eCPM {pub_ecpm:.4f}"
            changes.append({
                "placement_id": pid,
                "title": f["title"],
                "type": f["type"],
                "current_floor": current_floor,
                "new_floor": round(new_floor, 4),
                "reason": reason,
                "action": "raise",
                "pub_ecpm": pub_ecpm,
                "wins": wins,
            })

        elif current_floor > pub_ecpm * TARGET_FLOOR_RATIO_HIGH:
            # Floor is too high — lower it
            min_lower = round(current_floor * (1 - MAX_MOVE_DOWN), 4)
            new_floor = max(target_floor, min_lower, MIN_FLOOR)
            if new_floor >= current_floor:
                continue
            reason = f"floor {current_floor:.4f} > {TARGET_FLOOR_RATIO_HIGH:.0%} × eCPM {pub_ecpm:.4f}"
            changes.append({
                "placement_id": pid,
                "title": f["title"],
                "type": f["type"],
                "current_floor": current_floor,
                "new_floor": round(new_floor, 4),
                "reason": reason,
                "action": "lower",
                "pub_ecpm": pub_ecpm,
                "wins": wins,
            })

    # Sort: raises first (highest eCPM gap), then lowers
    changes.sort(key=lambda x: (x["action"] != "raise", -abs(x["new_floor"] - x["current_floor"])))
    return changes


# ---------------------------------------------------------------------------
# Apply changes
# ---------------------------------------------------------------------------

def apply_changes(changes: list[dict], dry_run: bool = True) -> list[dict]:
    """Apply floor changes via management API."""
    import core.tb_mgmt as tbm

    results = []
    n_raise = sum(1 for c in changes if c["action"] == "raise")
    n_lower = sum(1 for c in changes if c["action"] == "lower")
    arrow   = {"raise": "↑", "lower": "↓"}

    print(f"\n{'DRY RUN' if dry_run else 'APPLYING'} {len(changes)} floor changes "
          f"({n_raise} raises, {n_lower} lowers)\n")

    for c in changes:
        pid       = c["placement_id"]
        new_floor = c["new_floor"]
        act       = arrow[c["action"]]
        print(
            f"  {act} [{pid}] {c['title'][:35]:<35}  "
            f"${c['current_floor']:.4f} → ${new_floor:.4f}  "
            f"eCPM=${c['pub_ecpm']:.4f}  wins={c['wins']:,}  {c['reason'][:40]}"
        )

        if not dry_run:
            try:
                r = tbm.set_floor(pid, price=new_floor, dry_run=False)
                c["applied"] = r.get("applied", False)
                c["error"]   = None
            except Exception as e:
                c["applied"] = False
                c["error"]   = str(e)
                print(f"    ✗ error: {e}")
        else:
            c["applied"] = False
            c["dry_run"] = True

        results.append(c)

    return results


# ---------------------------------------------------------------------------
# Slack summary
# ---------------------------------------------------------------------------

def post_slack_summary(results: list[dict], dry_run: bool) -> None:
    from core.slack import post_message
    mode  = "DRY RUN" if dry_run else "LIVE"
    n_ok  = sum(1 for r in results if r.get("applied"))
    n_err = sum(1 for r in results if r.get("error"))
    n_raise = sum(1 for r in results if r["action"] == "raise")
    n_lower = sum(1 for r in results if r["action"] == "lower")

    lines = [
        f"🤖 *TB Floor Optimizer* [{mode}] — {len(results)} changes ({n_raise}↑ {n_lower}↓)",
    ]
    if not dry_run:
        lines.append(f"Applied: {n_ok} ✓  Errors: {n_err} ✗")
    for r in results[:12]:
        act = "↑" if r["action"] == "raise" else "↓"
        status = "" if dry_run else ("✓" if r.get("applied") else "✗")
        lines.append(
            f"  {act} [{r['placement_id']}] {r['title'][:28]} "
            f"${r['current_floor']:.3f}→${r['new_floor']:.3f} "
            f"(eCPM=${r['pub_ecpm']:.3f}) {status}"
        )
    if len(results) > 12:
        lines.append(f"  ... and {len(results) - 12} more")

    try:
        post_message("\n".join(lines))
    except Exception:
        pass  # Slack is optional


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def run(dry_run: bool = True, days: int = 3, slack: bool = True) -> None:
    print(f"\n{'='*60}")
    print(f"TB Dynamic Floor Optimizer  {'[DRY RUN]' if dry_run else '[LIVE]'}")
    print(f"Lookback: {days} days   Min wins: {MIN_WINS:,}")
    print(f"{'='*60}\n")

    # 1. Test write access (only matters for live runs)
    if not dry_run:
        if not test_write_access():
            print("\nAborting — no write access. See instructions above.")
            return

    # 2. Fetch data
    print("Fetching placement stats...")
    stats = fetch_placement_stats(days=days)
    print(f"  {len(stats)} placements with data")

    print("Fetching current floors...")
    # Only fetch floors for placements that have enough stats data to be worth optimizing
    candidate_ids = [
        pid for pid, s in stats.items()
        if s["pub_ecpm"] >= MIN_ECPM and s["wins"] >= MIN_WINS
    ]
    print(f"  {len(candidate_ids)} candidate placements (sufficient stats)")
    floors = fetch_current_floors(placement_ids=candidate_ids)
    print(f"  {len(floors)} placements with floor info")

    # 3. Compute changes
    changes = compute_floor_changes(stats, floors)
    print(f"\n{len(changes)} floor changes recommended")

    if not changes:
        print("Nothing to do — all floors are in optimal range.")
        return

    # 4. Apply
    results = apply_changes(changes, dry_run=dry_run)

    # 5. Slack
    if slack:
        try:
            post_slack_summary(results, dry_run=dry_run)
        except Exception:
            pass

    print(f"\n{'='*60}")
    n_ok = sum(1 for r in results if r.get("applied"))
    print(f"{'DRY RUN' if dry_run else 'APPLIED'}: "
          f"{n_ok if not dry_run else len(results)} changes")
    print(f"{'='*60}")


if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument("--live",    action="store_true", help="Apply changes (default: dry run)")
    p.add_argument("--days",    type=int, default=3, help="Lookback window in days")
    p.add_argument("--no-slack", action="store_true", help="Skip Slack notification")
    args = p.parse_args()
    run(dry_run=not args.live, days=args.days, slack=not args.no_slack)
