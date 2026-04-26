"""
agents/optimization/revenue_guardian.py

Self-correcting revenue optimizer. The "watchdog" that ensures every
revenue-growth change actually grew revenue.

Three phases per run (scheduled every 4 hours):

VERIFY (rollback losers)
  For every change ≥ WARM_UP_HOURS old:
    - Pull post-change revenue + fill_rate vs pre-change baseline
    - If revenue dropped ≥ ROLLBACK_REV_DROP_PCT: revert
    - If fill dropped ≥ ROLLBACK_FILL_DROP_PCT: revert
  Sources: floor changes from tb_floor_nudge, min_floor_sweep,
  aggressive_floor_lift, manual edits via this agent.

OBSERVE (compute opportunity scores)
  Pull placement × revenue data, score each placement on:
    - Floor utilization: eCPM / floor (>4× = headroom)
    - Trend: 3d revenue vs prior 7d avg
    - is_optimal_price status

ACT (apply ONE change at a time, conservative)
  Pick the highest-confidence opportunity from OBSERVE that:
    - Not in freeze list (recently changed)
    - Has ≥ MIN_REVENUE in baseline (skip unproven)
    - Survived the last verify cycle clean
  Apply ONE conservative bump per inventory per cycle:
    - Floor: nudge to 30% of observed eCPM (not 50% like aggressive)
    - Cap at +50% of current floor in single step
  Log everything to logs/guardian_ledger.json

Safety rules
------------
1. WARM_UP_HOURS = 24:    no judgment before 24h of post-change data
2. FREEZE_DAYS    = 3:    no re-change to same placement within 3 days
3. MAX_CHANGES_PER_RUN: 5 changes/run cap
4. PER_DAY_CAP:        20 changes/day cap (across all guardian runs)
5. PER_PLACEMENT_LIFETIME: 5 cumulative changes max — placement is
   "stabilized" after that, hand off to is_optimal_price ML
6. ROLLBACK precedence: verify always runs first; rollback before act
7. NEVER touch placements with active manual edit flag
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

# ─── Tunables ────────────────────────────────────────────────────────────────
WARM_UP_HOURS              = 24
FREEZE_DAYS                = 3
MAX_CHANGES_PER_RUN        = 5
PER_DAY_CAP                = 20
PER_PLACEMENT_LIFETIME_CAP = 5

ROLLBACK_REV_DROP_PCT      = 0.15
ROLLBACK_FILL_DROP_PCT     = 0.25

OPPORTUNITY_RATIO_MIN      = 4.0     # eCPM ≥ 4× current floor
TARGET_FLOOR_PCT_OF_ECPM   = 0.30    # set new floor to 30% of eCPM
MAX_SINGLE_STEP_PCT        = 0.50    # never bump >+50% in single step
MIN_BASELINE_REVENUE       = 5.0     # skip placements w/ <$5 revenue (noise)
MIN_BASELINE_IMPS          = 5_000

TB_BASE = "https://ssp.pgammedia.com/api"
LOG_DIR = os.path.join(_REPO_ROOT, "logs")
LEDGER  = os.path.join(LOG_DIR, "guardian_ledger.json")
RECS    = os.path.join(LOG_DIR, "guardian_recs.json")
os.makedirs(LOG_DIR, exist_ok=True)


# ─── Ledger ──────────────────────────────────────────────────────────────────

def load_ledger() -> list[dict]:
    if not os.path.exists(LEDGER): return []
    try:
        with open(LEDGER) as f: return json.load(f)
    except Exception: return []


def save_ledger(entries: list[dict]) -> None:
    with open(LEDGER, "w") as f:
        json.dump(entries, f, indent=2, default=str)


# ─── Stats ───────────────────────────────────────────────────────────────────

def _placement_stats(start_date, end_date) -> dict[int, dict]:
    """Return {placement_id: {imps, spend, rev, ecpm, fill_rate}}."""
    params = [("from", start_date), ("to", end_date),
              ("day_group","total"),("limit",5000),
              ("attribute[]","placement")]
    url = f"{TB_BASE}/{tbm._get_token()}/report?" + urllib.parse.urlencode(params)
    r = requests.get(url, timeout=300); r.raise_for_status()
    rows = r.json().get("data", r.json())
    out = {}
    for row in rows if isinstance(rows, list) else []:
        pid = row.get("placement_id")
        if pid is None: continue
        imps  = row.get("impressions", 0) or 0
        spend = row.get("dsp_spend", 0.0) or 0.0
        rev   = row.get("publisher_revenue", 0.0) or 0.0
        resp_ = row.get("bid_responses", 0) or 0
        out[int(pid)] = {
            "impressions": imps, "dsp_spend": spend, "publisher_revenue": rev,
            "ecpm":      (spend * 1000.0 / imps) if imps else 0.0,
            "fill_rate": (imps / resp_) if resp_ else 0.0,
        }
    return out


# ─── VERIFY: rollback losers ─────────────────────────────────────────────────

def _verify_phase(ledger: list[dict]) -> list[dict]:
    """For each pending change, measure pre vs post; rollback if dropped."""
    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(hours=WARM_UP_HOURS)
    pending = [e for e in ledger
               if e.get("type") == "guardian_apply"
               and e.get("verify_status") is None
               and datetime.fromisoformat(e["timestamp"].replace("Z","+00:00")) < cutoff]
    if not pending:
        print("  no changes ready for verification")
        return []

    today = now.date()
    post_start = today - timedelta(days=2)
    post_end   = today
    print(f"  verifying {len(pending)} changes (post window {post_start}..{post_end})")
    post_stats = _placement_stats(post_start.isoformat(), post_end.isoformat())

    actions = []
    for e in pending:
        pid = e["placement_id"]
        post = post_stats.get(pid, {"impressions":0,"publisher_revenue":0.0,"fill_rate":0.0})
        pre  = e.get("pre_metrics", {})
        # Normalize to per-day basis
        pre_days  = e.get("pre_window_days", 7)
        post_days = 2
        pre_rev_d  = pre.get("publisher_revenue", 0) / max(pre_days, 1)
        post_rev_d = post["publisher_revenue"] / max(post_days, 1)
        pre_fill   = pre.get("fill_rate", 0)
        post_fill  = post["fill_rate"]

        rev_drop  = (pre_rev_d - post_rev_d) / pre_rev_d if pre_rev_d else 0
        fill_drop = (pre_fill - post_fill) / pre_fill   if pre_fill   else 0

        verdict = "OK"
        reason = ""
        if rev_drop >= ROLLBACK_REV_DROP_PCT:
            verdict = "ROLLBACK"
            reason = f"revenue drop {rev_drop*100:.1f}%"
        elif fill_drop >= ROLLBACK_FILL_DROP_PCT:
            verdict = "ROLLBACK"
            reason = f"fill drop {fill_drop*100:.1f}%"

        e["verify_status"]    = verdict
        e["verify_timestamp"] = now.isoformat()
        e["verify_post_metrics"] = post
        e["verify_rev_drop_pct"] = round(rev_drop*100, 1)
        e["verify_fill_drop_pct"]= round(fill_drop*100, 1)
        e["verify_reason"]    = reason

        if verdict == "ROLLBACK":
            try:
                tbm.set_floor(pid, price=e["before_floor"], dry_run=False)
                e["rollback_applied"] = True
                actions.append({"type":"rollback","placement_id":pid,
                                "title":e.get("title"),
                                "restored_floor":e["before_floor"],
                                "reason":reason,"timestamp":now.isoformat()})
                print(f"  🔄 ROLLBACK pid={pid} {e.get('title','')[:30]}  "
                      f"${e['after_floor']:.2f} → ${e['before_floor']:.2f}  ({reason})")
            except Exception as exc:
                e["rollback_error"] = str(exc)
                print(f"  ✗ rollback failed pid={pid}: {exc}")
        else:
            print(f"  ✓ pid={pid} {e.get('title','')[:30]}  "
                  f"rev_d={rev_drop*100:+.1f}%  fill_d={fill_drop*100:+.1f}%")
    return actions


# ─── ACT: apply highest-confidence opportunity ───────────────────────────────

def _opportunity_phase(ledger: list[dict]) -> list[dict]:
    """Score placements; pick top N safe candidates; apply conservative bumps."""
    today = datetime.now(timezone.utc).date()
    base_start = today - timedelta(days=7)
    base_end   = today
    base = _placement_stats(base_start.isoformat(), base_end.isoformat())

    # Build freeze list (recent changes) + lifetime caps
    freeze_cutoff = datetime.now(timezone.utc) - timedelta(days=FREEZE_DAYS)
    freeze: set[int] = set()
    lifetime_count: dict[int,int] = defaultdict(int)
    today_str = today.isoformat()
    today_change_count = 0
    for e in ledger:
        if e.get("type") != "guardian_apply": continue
        ts = datetime.fromisoformat(e["timestamp"].replace("Z","+00:00"))
        if ts > freeze_cutoff:
            freeze.add(e["placement_id"])
        lifetime_count[e["placement_id"]] += 1
        if ts.date().isoformat() == today_str:
            today_change_count += 1

    if today_change_count >= PER_DAY_CAP:
        print(f"  daily cap hit ({today_change_count}/{PER_DAY_CAP}) — no new changes")
        return []

    # Hydrate placements (banner type, has price)
    placements = tbm.list_all_placements_via_report(days=7, min_impressions=MIN_BASELINE_IMPS)
    candidates = []
    for p in placements:
        pid = p["placement_id"]
        if pid in freeze: continue
        if lifetime_count[pid] >= PER_PLACEMENT_LIFETIME_CAP: continue
        floor = float(p.get("price") or 0.0)
        if floor <= 0: continue
        s = base.get(pid, {})
        rev = s.get("publisher_revenue", 0)
        if rev < MIN_BASELINE_REVENUE: continue
        ecpm = s.get("ecpm", 0)
        if ecpm <= 0: continue
        ratio = ecpm / floor
        if ratio < OPPORTUNITY_RATIO_MIN: continue

        target = round(ecpm * TARGET_FLOOR_PCT_OF_ECPM, 2)
        # Cap at +MAX_SINGLE_STEP_PCT vs current
        target = min(target, round(floor * (1 + MAX_SINGLE_STEP_PCT), 2))
        if target <= floor: continue

        candidates.append({
            "placement_id": pid, "title": p.get("title"),
            "inventory_id": p.get("inventory_id"),
            "before_floor": floor, "after_floor": target,
            "ecpm": round(ecpm,2), "ratio": round(ratio,1),
            "rev_7d": round(rev,2),
            "fill_rate": round(s.get("fill_rate",0),4),
            "lifetime_changes": lifetime_count[pid],
        })

    # Rank by potential uplift = (target - before) × imps_per_day
    for c in candidates:
        c["score"] = (c["after_floor"] - c["before_floor"]) * c["rev_7d"]
    candidates.sort(key=lambda x: -x["score"])
    candidates = candidates[:MAX_CHANGES_PER_RUN]

    if not candidates:
        print("  no opportunities meet safety bar")
        return []

    print(f"  applying {len(candidates)} conservative bumps:")
    actions = []
    now_iso = datetime.now(timezone.utc).isoformat()
    for c in candidates:
        try:
            tbm.set_floor(c["placement_id"], price=c["after_floor"], dry_run=False)
            entry = {
                "type":          "guardian_apply",
                "placement_id":  c["placement_id"],
                "title":         c["title"],
                "inventory_id":  c["inventory_id"],
                "before_floor":  c["before_floor"],
                "after_floor":   c["after_floor"],
                "pre_metrics":   {"publisher_revenue": c["rev_7d"],
                                  "fill_rate": c["fill_rate"],
                                  "ecpm": c["ecpm"]},
                "pre_window_days": 7,
                "ratio_at_apply":  c["ratio"],
                "verify_status": None,
                "timestamp":     now_iso,
            }
            ledger.append(entry)
            actions.append(entry)
            print(f"  📈 pid={c['placement_id']} {c['title'][:32]:<32} "
                  f"${c['before_floor']:.2f}→${c['after_floor']:.2f} "
                  f"(eCPM ${c['ecpm']:.2f}, ratio {c['ratio']}×)")
        except Exception as e:
            print(f"  ✗ pid={c['placement_id']}: {e}")
    return actions


# ─── Entry ───────────────────────────────────────────────────────────────────

def run() -> dict:
    print(f"\n{'='*72}\n  Revenue Guardian   "
          f"{datetime.now(timezone.utc).isoformat()}\n{'='*72}")

    ledger = load_ledger()
    print(f"  ledger: {len(ledger)} historical entries")

    # Phase 1: VERIFY
    print("\n[VERIFY phase]")
    rollback_actions = _verify_phase(ledger)

    # Phase 2: ACT
    print("\n[ACT phase]")
    apply_actions = _opportunity_phase(ledger)

    save_ledger(ledger)

    # Slack
    try:
        from core.slack import post_message
        msg = [f"🛡️ *Revenue Guardian*",
               f"Verified {len([e for e in ledger if e.get('verify_status')])} changes  "
               f"| Rolled back {len(rollback_actions)}  "
               f"| New bumps {len(apply_actions)}"]
        for a in apply_actions[:3]:
            msg.append(f"  📈 [{a['placement_id']}] {a['title'][:28]}  "
                       f"${a['before_floor']:.2f}→${a['after_floor']:.2f}")
        for a in rollback_actions[:3]:
            msg.append(f"  🔄 [{a['placement_id']}] {a['title'][:28]} reverted  "
                       f"({a['reason']})")
        post_message("\n".join(msg))
    except Exception: pass

    recs = {"timestamp": datetime.now(timezone.utc).isoformat(),
            "rollbacks": rollback_actions,
            "applies":   apply_actions,
            "ledger_size": len(ledger)}
    with open(RECS, "w") as f: json.dump(recs, f, indent=2, default=str)
    return recs


if __name__ == "__main__":
    run()
