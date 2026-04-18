"""
agents/optimization/tb_floor_nudge.py

Iterative TB placement floor elasticity nudging with rollback.

How it works
------------
For each placement with healthy traffic, measure the eCPM gap between
current floor and observed market price. If there's headroom
(market_ecpm ≥ current_floor × HEADROOM_MULTIPLIER), nudge the floor
up by STEP_PCT. Record the pre-change win-rate / fill / revenue. On
the NEXT run, compare post-change metrics against pre-change:

  - FILL dropped by >ROLLBACK_FILL_DROP_PCT → rollback
  - REVENUE dropped by >ROLLBACK_REV_DROP_PCT → rollback
  - Otherwise → keep (and try another nudge if still headroom)

The nudge + rollback loop converges toward each placement's true
yield-maximizing floor without destroying fill rate.

State
-----
logs/tb_floor_nudge_state.json tracks per-placement:
  baseline_floor, baseline_ecpm, baseline_fill, baseline_revenue,
  nudged_at, nudge_count, last_nudge_floor

Safety
------
- Dry-run default.
- MAX_NUDGES_PER_RUN caps how many placements change per day.
- MAX_TOTAL_INCREASE_PCT hard cap vs baseline (e.g. never >+100%).
- MIN_IMP_THRESHOLD skips low-volume placements (noise).
- Auto-rollback built in; no human required.
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

# ─── Tunables ────────────────────────────────────────────────────────────────

MEASURE_WINDOW_DAYS     = 3          # post-change look-back for rollback decision
BASELINE_WINDOW_DAYS    = 7
COOLDOWN_HOURS          = 48         # hours between nudges on same placement

STEP_PCT                = 0.10       # +10% per nudge
HEADROOM_MULTIPLIER     = 1.8        # only nudge if eCPM ≥ floor × 1.8
MIN_IMP_THRESHOLD       = 1_000      # per measurement window
MIN_FLOOR               = 0.05
MAX_FLOOR               = 10.0
MAX_TOTAL_INCREASE_PCT  = 1.00       # never >+100% vs baseline

ROLLBACK_FILL_DROP_PCT  = 0.20       # ≥20% fill drop → rollback
ROLLBACK_REV_DROP_PCT   = 0.15       # ≥15% revenue drop → rollback

MAX_NUDGES_PER_RUN      = 10

TB_BASE = "https://ssp.pgammedia.com/api"

LOG_DIR     = os.path.join(_REPO_ROOT, "logs")
STATE_FILE  = os.path.join(LOG_DIR, "tb_floor_nudge_state.json")
ACTIONS_LOG = os.path.join(LOG_DIR, "tb_floor_nudge_actions.json")
os.makedirs(LOG_DIR, exist_ok=True)


# ─── Data pull ───────────────────────────────────────────────────────────────

def placement_report(days: int) -> dict[int, dict]:
    """Return {placement_id: {imps, spend, ecpm, fill_rate, revenue}}."""
    end   = datetime.now(timezone.utc).date()
    start = end - timedelta(days=days)
    token = tbm._get_token()
    params = [
        ("from", start.isoformat()), ("to", end.isoformat()),
        ("day_group", "total"), ("limit", 1000),
        ("attribute[]", "placement"),
    ]
    url = f"{TB_BASE}/{token}/report?" + urllib.parse.urlencode(params)
    resp = requests.get(url, timeout=90)
    if not resp.ok:
        raise RuntimeError(f"placement report: {resp.status_code} {resp.text[:200]}")
    rows = resp.json().get("data", resp.json())
    out = {}
    for r in rows if isinstance(rows, list) else []:
        pid = r.get("placement_id")
        if pid is None:
            raw = r.get("placement", "")
            if "#" in raw:
                try: pid = int(raw.rsplit("#", 1)[1].strip())
                except ValueError: pass
        if pid is None:
            continue
        pid = int(pid)
        imps  = r.get("impressions", 0) or 0
        spend = r.get("dsp_spend", 0.0) or 0.0
        resp_ = r.get("bid_responses", 0) or 0
        out[pid] = {
            "impressions":  imps,
            "dsp_spend":    spend,
            "pub_revenue":  r.get("publisher_revenue", 0.0) or 0.0,
            "ecpm":         (spend * 1000.0 / imps) if imps else 0.0,
            "fill_rate":    (imps / resp_) if resp_ else 0.0,
            "bid_responses": resp_,
        }
    return out


# ─── State ───────────────────────────────────────────────────────────────────

def load_state() -> dict:
    if not os.path.exists(STATE_FILE):
        return {"placements": {}}
    with open(STATE_FILE) as f:
        return json.load(f)


def save_state(state: dict) -> None:
    with open(STATE_FILE, "w") as f:
        json.dump(state, f, indent=2)


def append_actions(actions: list[dict]) -> None:
    prior = []
    if os.path.exists(ACTIONS_LOG):
        with open(ACTIONS_LOG) as f:
            try: prior = json.load(f)
            except Exception: prior = []
    prior.extend(actions)
    with open(ACTIONS_LOG, "w") as f:
        json.dump(prior, f, indent=2)


# ─── Decision logic ──────────────────────────────────────────────────────────

def _cooldown_active(entry: dict) -> bool:
    last = entry.get("nudged_at")
    if not last: return False
    delta = datetime.now(timezone.utc) - datetime.fromisoformat(
        last.replace("Z", "+00:00")
    )
    return delta < timedelta(hours=COOLDOWN_HOURS)


def check_rollback(placement: dict, entry: dict, recent: dict) -> tuple[bool, str]:
    """True iff recent metrics warrant rollback."""
    baseline_fill = entry.get("baseline_fill", 0)
    baseline_rev  = entry.get("baseline_revenue", 0)
    if baseline_fill <= 0 or baseline_rev <= 0:
        return False, ""
    fill_now = recent.get("fill_rate", 0)
    rev_now  = recent.get("pub_revenue", 0)
    fill_drop = (baseline_fill - fill_now) / baseline_fill
    rev_drop  = (baseline_rev - rev_now) / baseline_rev
    if fill_drop >= ROLLBACK_FILL_DROP_PCT:
        return True, f"fill dropped {fill_drop*100:.1f}% (≥{ROLLBACK_FILL_DROP_PCT*100:.0f}%)"
    if rev_drop >= ROLLBACK_REV_DROP_PCT:
        return True, f"revenue dropped {rev_drop*100:.1f}% (≥{ROLLBACK_REV_DROP_PCT*100:.0f}%)"
    return False, ""


def decide_nudge(placement: dict, entry: dict, recent: dict) -> tuple[str, float | None, str]:
    """Return ('nudge'|'rollback'|'skip', new_floor, reason)."""
    pid   = placement["placement_id"]
    cur   = float(placement.get("price", 0.0) or 0.0)
    imps  = recent.get("impressions", 0)
    ecpm  = recent.get("ecpm", 0.0)

    if imps < MIN_IMP_THRESHOLD:
        return "skip", None, f"imps {imps} < {MIN_IMP_THRESHOLD}"

    # Check rollback first (only if we've nudged before)
    if entry.get("nudge_count", 0) > 0 and not _cooldown_active(entry):
        do_rb, why = check_rollback(placement, entry, recent)
        if do_rb:
            return "rollback", entry.get("baseline_floor", cur), why

    if _cooldown_active(entry):
        return "skip", None, "cooldown"

    baseline = entry.get("baseline_floor", cur)
    if baseline > 0:
        total_increase = (cur - baseline) / baseline
        if total_increase >= MAX_TOTAL_INCREASE_PCT:
            return "skip", None, f"at max total increase ({total_increase*100:.0f}%)"

    if ecpm < cur * HEADROOM_MULTIPLIER:
        return "skip", None, f"no headroom (eCPM ${ecpm:.2f} < floor ${cur:.2f} × {HEADROOM_MULTIPLIER})"

    new_floor = min(MAX_FLOOR, max(MIN_FLOOR, round(cur * (1 + STEP_PCT), 2)))
    if new_floor <= cur:
        return "skip", None, "no effective increase"
    return "nudge", new_floor, f"eCPM ${ecpm:.2f} vs floor ${cur:.2f} → +{STEP_PCT*100:.0f}%"


# ─── Entry point ─────────────────────────────────────────────────────────────

def run(dry_run: bool = True) -> dict:
    print(f"\n{'='*72}")
    print(f"  TB Floor Nudge  {'[DRY RUN]' if dry_run else '[LIVE]'}")
    print(f"{'='*72}")

    placements = tbm.list_all_placements_via_report(days=BASELINE_WINDOW_DAYS, min_impressions=MIN_IMP_THRESHOLD)
    print(f"  {len(placements)} placements account-wide (≥{MIN_IMP_THRESHOLD} imps)")

    # Recent window for decisions, baseline window for initial snapshot
    recent_stats   = placement_report(MEASURE_WINDOW_DAYS)
    baseline_stats = placement_report(BASELINE_WINDOW_DAYS)

    state = load_state()
    pstate = state.setdefault("placements", {})

    decisions = []
    nudges = 0
    for p in placements:
        pid = p["placement_id"]
        key = str(pid)
        recent = recent_stats.get(pid, {})
        base   = baseline_stats.get(pid, {})

        # Record baseline on first sight
        entry = pstate.setdefault(key, {})
        if "baseline_floor" not in entry:
            entry["baseline_floor"]   = float(p.get("price", 0.0) or 0.0)
            entry["baseline_ecpm"]    = base.get("ecpm", 0.0)
            entry["baseline_fill"]    = base.get("fill_rate", 0.0)
            entry["baseline_revenue"] = base.get("pub_revenue", 0.0)
            entry["nudge_count"]      = 0

        action, new_floor, reason = decide_nudge(p, entry, recent)
        decisions.append({
            "placement_id": pid, "title": p.get("title"),
            "current_floor": p.get("price"), "action": action,
            "new_floor": new_floor, "reason": reason,
            "imps_3d": recent.get("impressions", 0),
            "ecpm_3d": round(recent.get("ecpm", 0.0), 2),
        })

        if action in ("nudge", "rollback") and nudges < MAX_NUDGES_PER_RUN:
            print(f"  [{pid}] {action.upper():<8}  "
                  f"${p.get('price',0):.2f} → ${new_floor:.2f}  ({reason})")
            if not dry_run:
                try:
                    tbm.set_floor(pid, price=new_floor, dry_run=False)
                    entry["nudge_count"] = entry.get("nudge_count", 0) + 1
                    entry["nudged_at"]   = datetime.now(timezone.utc).isoformat()
                    entry["last_nudge_floor"] = new_floor
                    if action == "rollback":
                        entry["nudge_count"] = 0  # reset, try again after cooldown
                except Exception as e:
                    print(f"    ✗ {e}")
            nudges += 1

    # Summary counts
    by_action: dict[str, int] = {}
    for d in decisions:
        by_action[d["action"]] = by_action.get(d["action"], 0) + 1
    print(f"\n  Decisions: {by_action}")

    append_actions([d | {"timestamp": datetime.now(timezone.utc).isoformat(),
                         "dry_run": dry_run} for d in decisions])
    save_state(state)

    # Slack
    try:
        from core.slack import post_message
        nudged = [d for d in decisions if d["action"] == "nudge"]
        rolled = [d for d in decisions if d["action"] == "rollback"]
        tag = "🟢 LIVE" if not dry_run else "🔍 DRY"
        lines = [f"🪜 *TB Floor Nudge* {tag}",
                 f"Nudge: {len(nudged)}  Rollback: {len(rolled)}  "
                 f"Skip: {by_action.get('skip',0)}"]
        for d in (nudged + rolled)[:8]:
            lines.append(f"  • [{d['placement_id']}] {d['title'][:30]}  "
                         f"${d['current_floor']:.2f}→${d['new_floor']:.2f}  "
                         f"({d['reason']})")
        post_message("\n".join(lines))
    except Exception:
        pass

    return {"decisions": decisions, "applied": nudges if not dry_run else 0}


if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    args = ap.parse_args()
    run(dry_run=not args.apply)
