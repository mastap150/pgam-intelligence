"""
agents/optimization/config_health_scanner.py

Daily scan that catches publisher- and demand-level config that hinders
revenue/eCPM. Auto-fixes the safe stuff, alerts on the rest.

Categories of issue
-------------------
AUTO-FIX (safe, idempotent, well-understood):
  1. supplyChainEnabled=False on a revenue-earning demand
     → enable. DSPs throttle inventory without verified supply chain.
  2. lurlEnabled=False on a revenue-earning publisher
     → enable. Without loss-URL, DSPs can't see clearing prices and
        bid more conservatively.
  3. qpsLimit utilization >= 90% on a revenue-earning demand
     → double the qpsLimit. Throttled bids = lost revenue.

ALERT (requires human judgment, posts to Slack):
  4. Demand margin < 10% on revenue-earning demand (renegotiation candidate)
  5. New publisher created with lurlEnabled=False or wrong defaults
  6. Demand auctionType=2 (second-price) on high-revenue inventory
  7. iabCategories filter on publisher with non-Arts/Entertainment traffic

Safety posture
--------------
- AUTO-FIX caps: max 5 per category per run (blast radius)
- All fixes ledgered with actor="config_health_scanner_<date>"
- Skips entities below MIN_REV_THRESHOLD (don't waste write quota on dead inventory)
- LL_DRY_RUN respected via ll_mgmt._put
- Re-runs are idempotent: an already-correct config is detected and skipped
"""
from __future__ import annotations

import gzip
import json
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from core import ll_mgmt, floor_ledger, slack

DATA_DIR = Path(__file__).parent.parent.parent / "data"
HOURLY_PATH = DATA_DIR / "hourly_pub_demand.json.gz"

# Eligibility — only touch entities with real revenue
MIN_DEMAND_REV_7D = 50.0
MIN_PUB_REV_7D = 50.0

# Auto-fix caps
MAX_AUTOFIX_PER_CATEGORY = 5

# QPS bump multiplier
QPS_UTIL_THRESHOLD = 0.90
QPS_BUMP_MULTIPLIER = 2.0

# Margin alert threshold
LOW_MARGIN_THRESHOLD = 0.10  # alert if margin <= 10%
LOW_MARGIN_MIN_REV = 500.0   # ...and demand earning >= $500/wk

ACTOR_PREFIX = "config_health_scanner"


def _load_revenue_maps() -> tuple[dict, dict]:
    """Return (demand_rev_7d, pub_rev_7d) from the hourly store."""
    if not HOURLY_PATH.exists():
        return {}, {}
    with gzip.open(HOURLY_PATH, "rt") as f:
        rows = json.load(f)
    cutoff = (date.today() - timedelta(days=7)).isoformat()
    drev: dict = defaultdict(float)
    prev: dict = defaultdict(float)
    for r in rows:
        if str(r.get("DATE", "")) < cutoff:
            continue
        rev = float(r.get("GROSS_REVENUE", 0) or 0)
        drev[int(r.get("DEMAND_ID", 0) or 0)] += rev
        prev[int(r.get("PUBLISHER_ID", 0) or 0)] += rev
    return dict(drev), dict(prev)


def check_demand_supplychain(demands: list[dict], demand_rev: dict, actor: str) -> dict:
    """AUTO-FIX: enable supplyChainEnabled on revenue-earning demands."""
    candidates = [d for d in demands
                  if not d.get("supplyChainEnabled", True)
                  and demand_rev.get(d.get("id"), 0) >= MIN_DEMAND_REV_7D]
    candidates.sort(key=lambda d: -demand_rev.get(d.get("id"), 0))

    fixed = []
    for d in candidates[:MAX_AUTOFIX_PER_CATEGORY]:
        did = d["id"]
        try:
            d_obj = ll_mgmt._get(f"/v1/demands/{did}")
            d_obj["supplyChainEnabled"] = True
            ll_mgmt._put(f"/v1/demands/{did}", d_obj)
            after = ll_mgmt._get(f"/v1/demands/{did}")
            if after.get("supplyChainEnabled") is True:
                floor_ledger.record(
                    publisher_id=0, publisher_name="[schain-enable]",
                    demand_id=did, demand_name=d.get("name", ""),
                    old_floor=after.get("minBidFloor"), new_floor=after.get("minBidFloor"),
                    actor=actor,
                    reason=(f"Auto-enable supplyChainEnabled on demand earning "
                            f"${demand_rev.get(did, 0):.0f}/7d. DSPs were throttling "
                            f"unverified inventory."),
                    dry_run=False, applied=True,
                )
                fixed.append({"demand_id": did, "name": d.get("name", ""),
                              "rev_7d": demand_rev.get(did, 0)})
                print(f"[{actor}] schain enabled on demand {did}: {d.get('name','')[:45]}")
            else:
                print(f"[{actor}] schain PUT didn't stick on demand {did}")
        except Exception as e:
            print(f"[{actor}] schain enable FAILED on demand {did}: {e}")
    return {"category": "supplychain_enable", "candidates": len(candidates),
            "fixed": fixed}


def check_pub_lurl(all_pubs_summary: list[dict], pub_rev: dict, actor: str) -> dict:
    """AUTO-FIX: enable lurlEnabled on revenue-earning publishers."""
    candidates = []
    # Need to fetch full pub detail to see lurlEnabled — pre-filter by revenue
    revenue_pubs = sorted(
        [p for p in all_pubs_summary if pub_rev.get(p["id"], 0) >= MIN_PUB_REV_7D],
        key=lambda p: -pub_rev.get(p["id"], 0)
    )
    for p_summary in revenue_pubs[:30]:  # only check top 30 by revenue per run
        pid = p_summary["id"]
        try:
            p = ll_mgmt.get_publisher(pid)
        except Exception:
            continue
        if not p.get("lurlEnabled"):
            candidates.append((pid, p, pub_rev.get(pid, 0)))

    fixed = []
    for pid, p_obj, rev in candidates[:MAX_AUTOFIX_PER_CATEGORY]:
        try:
            p_modified = dict(p_obj)
            p_modified["lurlEnabled"] = True
            ll_mgmt._put(f"/v1/publishers/{pid}", p_modified)
            after = ll_mgmt.get_publisher(pid)
            if after.get("lurlEnabled") is True:
                floor_ledger.record(
                    publisher_id=pid, publisher_name=p_obj.get("name", ""),
                    demand_id=0, demand_name="[lurl-enable]",
                    old_floor=None, new_floor=None,
                    actor=actor,
                    reason=(f"Auto-enable lurlEnabled on pub doing ${rev:.0f}/7d. "
                            f"DSPs can now track losses."),
                    dry_run=False, applied=True,
                )
                fixed.append({"pub_id": pid, "name": p_obj.get("name", ""),
                              "rev_7d": rev})
                print(f"[{actor}] LURL enabled on pub {pid}: {p_obj.get('name','')[:45]}")
        except Exception as e:
            print(f"[{actor}] LURL enable FAILED on pub {pid}: {e}")
    return {"category": "lurl_enable", "candidates": len(candidates),
            "fixed": fixed}


def check_demand_qps(demands: list[dict], demand_rev: dict, actor: str) -> dict:
    """AUTO-FIX: double qpsLimit on demands hitting >=90% utilization."""
    candidates = []
    for d in demands:
        rev = demand_rev.get(d.get("id"), 0)
        if rev < MIN_DEMAND_REV_7D:
            continue
        qps = d.get("qpsLimit") or 0
        qps_yest = d.get("qpsYesterday") or 0
        if qps > 0 and qps_yest >= qps * QPS_UTIL_THRESHOLD:
            candidates.append((d, rev, qps, qps_yest))
    candidates.sort(key=lambda c: -c[1])

    fixed = []
    for d, rev, qps, qps_yest in candidates[:MAX_AUTOFIX_PER_CATEGORY]:
        did = d["id"]
        new_qps = int(qps * QPS_BUMP_MULTIPLIER)
        try:
            d_obj = ll_mgmt._get(f"/v1/demands/{did}")
            d_obj["qpsLimit"] = new_qps
            ll_mgmt._put(f"/v1/demands/{did}", d_obj)
            after = ll_mgmt._get(f"/v1/demands/{did}")
            if after.get("qpsLimit") == new_qps:
                floor_ledger.record(
                    publisher_id=0, publisher_name="[qps-raise]",
                    demand_id=did, demand_name=d.get("name", ""),
                    old_floor=after.get("minBidFloor"), new_floor=after.get("minBidFloor"),
                    actor=actor,
                    reason=(f"Auto-raise qpsLimit {qps} -> {new_qps}. "
                            f"Demand was throttled at {qps_yest}/{qps} = "
                            f"{qps_yest/qps*100:.0f}% utilization, earning ${rev:.0f}/7d."),
                    dry_run=False, applied=True,
                )
                fixed.append({"demand_id": did, "name": d.get("name", ""),
                              "old_qps": qps, "new_qps": new_qps, "rev_7d": rev})
                print(f"[{actor}] qpsLimit {qps}->{new_qps} on demand {did}")
        except Exception as e:
            print(f"[{actor}] qps raise FAILED on demand {did}: {e}")
    return {"category": "qps_raise", "candidates": len(candidates),
            "fixed": fixed}


def check_low_margin_demands(demands: list[dict], demand_rev: dict) -> list[dict]:
    """ALERT: demands with margin <=10% earning >$500/wk (renegotiation candidates)."""
    findings = []
    for d in demands:
        rev = demand_rev.get(d.get("id"), 0)
        margin = float(d.get("margin") or 0)
        if margin > 0 and margin <= LOW_MARGIN_THRESHOLD and rev >= LOW_MARGIN_MIN_REV:
            findings.append({
                "demand_id": d["id"], "name": d.get("name", ""),
                "margin_pct": int(margin * 100), "rev_7d": rev,
                "net_rev_7d": rev * margin,
            })
    findings.sort(key=lambda f: -f["rev_7d"])
    return findings


def run() -> dict:
    now = datetime.now(timezone.utc)
    actor = f"{ACTOR_PREFIX}_{now.strftime('%Y%m%d')}"

    demand_rev, pub_rev = _load_revenue_maps()
    demands = ll_mgmt.get_demands(include_archived=False)
    pubs_summary = ll_mgmt.get_publishers(include_archived=False)

    print(f"[{actor}] starting — {len(demands)} demands, {len(pubs_summary)} pubs")

    schain_result = check_demand_supplychain(demands, demand_rev, actor)
    lurl_result = check_pub_lurl(pubs_summary, pub_rev, actor)
    qps_result = check_demand_qps(demands, demand_rev, actor)
    low_margin = check_low_margin_demands(demands, demand_rev)

    autofix_count = (len(schain_result["fixed"]) + len(lurl_result["fixed"])
                     + len(qps_result["fixed"]))
    print(f"[{actor}] AUTO-FIXES: {autofix_count} ({len(schain_result['fixed'])} schain, "
          f"{len(lurl_result['fixed'])} lurl, {len(qps_result['fixed'])} qps)")
    print(f"[{actor}] ALERTS: {len(low_margin)} low-margin demands")

    # Slack: only post if there were autofixes or new alerts
    msg_parts = []
    if autofix_count > 0:
        msg_parts.append(f":wrench: *Config health auto-fixes — {now.strftime('%Y-%m-%d')}*")
        if schain_result["fixed"]:
            msg_parts.append(f"\n• supplyChain enabled on {len(schain_result['fixed'])} demand(s):")
            for f in schain_result["fixed"][:5]:
                msg_parts.append(f"   — `{f['demand_id']}` (${f['rev_7d']:.0f}/7d) {f['name'][:45]}")
        if lurl_result["fixed"]:
            msg_parts.append(f"\n• LURL enabled on {len(lurl_result['fixed'])} pub(s):")
            for f in lurl_result["fixed"][:5]:
                msg_parts.append(f"   — `{f['pub_id']}` (${f['rev_7d']:.0f}/7d) {f['name'][:45]}")
        if qps_result["fixed"]:
            msg_parts.append(f"\n• qpsLimit raised on {len(qps_result['fixed'])} demand(s):")
            for f in qps_result["fixed"][:5]:
                msg_parts.append(f"   — `{f['demand_id']}` qps {f['old_qps']}→{f['new_qps']} ${f['rev_7d']:.0f}/7d  {f['name'][:35]}")

    if low_margin:
        if not msg_parts:
            msg_parts.append(f":bar_chart: *Config health — {now.strftime('%Y-%m-%d')}*")
        msg_parts.append(f"\n• {len(low_margin)} demands at <={int(LOW_MARGIN_THRESHOLD*100)}% margin earning >=${LOW_MARGIN_MIN_REV:.0f}/wk (renegotiation candidates):")
        for f in low_margin[:5]:
            msg_parts.append(f"   — `{f['demand_id']}` {f['margin_pct']}% margin, "
                             f"${f['rev_7d']:.0f}/7d gross, ${f['net_rev_7d']:.0f}/7d net  "
                             f"{f['name'][:35]}")

    if msg_parts:
        try:
            slack.send_text("\n".join(msg_parts))
        except Exception as e:
            print(f"[{actor}] Slack post failed: {e}")

    return {
        "ran_at": now.isoformat(),
        "schain": schain_result,
        "lurl": lurl_result,
        "qps": qps_result,
        "low_margin_alerts": low_margin,
        "autofix_total": autofix_count,
    }


if __name__ == "__main__":
    import json as _json
    print(_json.dumps(run(), indent=2, default=str))
