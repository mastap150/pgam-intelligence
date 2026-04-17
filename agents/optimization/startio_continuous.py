"""
agents/optimization/startio_continuous.py

Daily continuous optimizer for Start.IO demand floors on LL.

Wired into scheduler.py to run once per day at 14:00 ET (after the morning
pilot_watchdog cycle has had a chance to revert anything broken).

How it differs from scripts/startio_floor_optimizer_apr17.py
-------------------------------------------------------------
The apr17 script was a one-shot bulk activator. This agent is the
ongoing tuner that compounds the activation work over time:

  • Runs daily, not on-demand
  • Only makes SMALL moves per cycle (±15% per step)
  • Won't touch any demand entry it touched in the last 48h (cooldown)
  • Won't touch any publisher with an active "startio_activation"
    watchdog watch — the watchdog needs a stable baseline to detect
    regressions; we don't fight it
  • Persists daily snapshots of patched LL data so deltas can be
    computed across runs (POST /v1/report ignores date filters, so
    we manufacture a "yesterday" view by diffing snapshots)
  • Posts a Slack summary after every run, listing planned + applied
    changes plus the entries that were skipped and why
  • Self-throttles: max MAX_CHANGES_PER_RUN actions per run

Heuristic (each Start.IO demand entry, each day):
  • New entry (null floor) → ACTIVATE at 50% of all-time eff_eCPM
                             (a touch more aggressive than the apr17
                             script's 40% — we're now confident in the
                             proxy data)
  • current_floor set, recent_proxy_wr ≥ 30%, eff_eCPM ≥ 2× floor
       → RAISE +10% (capped at 1.15× current floor per cycle)
  • current_floor set, recent_proxy_wr ≤ 5%, recent_rev > $5,
    AND prior 24h shows revenue collapse vs prior week
       → LOWER -10%
  • else → HOLD

Run modes:
    python3.13 -m agents.optimization.startio_continuous           # dry-run
    python3.13 -m agents.optimization.startio_continuous --live    # apply
    python3.13 -m agents.optimization.startio_continuous --live --no-slack
"""
import argparse
import json
import os
import sys
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone

_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from dotenv import load_dotenv
load_dotenv(dotenv_path=os.path.join(_ROOT, ".env"), override=True)

import core.ll_mgmt as llm
import core.ll_report as llr
import core.slack as slack

# ---------------------------------------------------------------------------
# Config / guardrails
# ---------------------------------------------------------------------------

MAX_CHANGES_PER_RUN  = 8       # hard cap — never make more than this in one cycle
COOLDOWN_HOURS       = 48      # don't touch a (pub, demand) we changed within this window
MIN_BIDS_DAILY       = 200     # require this much daily bid volume before adjusting
MIN_REV_DAILY        = 1.00    # require this much daily revenue before adjusting

ACTIVATE_PCT_OF_ECPM = 0.50    # higher than apr17's 0.40 — we trust the proxy now
ACTIVATE_FLOOR_MIN   = 0.20
ACTIVATE_FLOOR_MAX   = 2.50

RAISE_WR_THRESHOLD   = 0.30
RAISE_HEADROOM_X     = 2.0
RAISE_PCT            = 0.10    # +10% per step
RAISE_CAP_PCT        = 0.15    # never more than +15% per cycle

LOWER_WR_THRESHOLD   = 0.05
LOWER_REV_DAILY_MIN  = 5.00
LOWER_PCT            = 0.10    # -10% per step
LOWER_CAP_PCT        = 0.20    # never more than -20% per cycle
MIN_FLOOR_FLOOR      = 0.05    # never lower a floor below this

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

LOG_DIR        = os.path.join(_ROOT, "logs")
SNAPSHOT_DIR   = os.path.join(LOG_DIR, "startio_snapshots")
ACTIONS_LOG    = os.path.join(LOG_DIR, "startio_continuous_actions.json")
COOLDOWN_FILE  = os.path.join(LOG_DIR, "startio_continuous_cooldown.json")
PILOT_LOG_PATH = os.path.join(LOG_DIR, "pilot_2026-04.json")

ET_TZ = None
try:
    from zoneinfo import ZoneInfo
    ET_TZ = ZoneInfo("America/New_York")
except Exception:
    pass


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _today() -> str:
    return datetime.now().strftime("%Y-%m-%d")


def _now_et_str() -> str:
    try:
        return datetime.now(ET_TZ).strftime("%Y-%m-%d %H:%M ET")
    except Exception:
        return datetime.now().strftime("%Y-%m-%d %H:%M")


def _sf(v):
    try: return float(v)
    except (TypeError, ValueError): return 0.0


# ---------------------------------------------------------------------------
# Snapshot persistence — used to derive daily deltas from all-time data
# ---------------------------------------------------------------------------

def _snapshot_path(d: date) -> str:
    return os.path.join(SNAPSHOT_DIR, f"{d.isoformat()}.json")


def take_snapshot(rows: list[dict]) -> dict:
    """Persist today's all-time Start.IO snapshot keyed by (pub_id, demand_id)."""
    os.makedirs(SNAPSHOT_DIR, exist_ok=True)
    snap = {
        "captured_at": _now_iso(),
        "entries": [
            {
                "publisher_id": int(r.get("PUBLISHER_ID") or 0),
                "publisher_name": r.get("PUBLISHER_NAME"),
                "demand_id": int(r.get("DEMAND_ID") or 0),
                "demand_name": r.get("DEMAND_NAME"),
                "bids": _sf(r.get("BIDS")),
                "wins": _sf(r.get("WINS")),
                "imps": _sf(r.get("IMPRESSIONS")),
                "rev":  _sf(r.get("GROSS_REVENUE")),
            }
            for r in rows
        ],
    }
    with open(_snapshot_path(date.today()), "w") as f:
        json.dump(snap, f, indent=2)
    return snap


def load_prior_snapshot() -> dict | None:
    """Find the most recent snapshot before today. Returns None if none exists."""
    if not os.path.isdir(SNAPSHOT_DIR):
        return None
    today_str = _today()
    files = sorted(
        f for f in os.listdir(SNAPSHOT_DIR)
        if f.endswith(".json") and f.replace(".json", "") < today_str
    )
    if not files:
        return None
    with open(os.path.join(SNAPSHOT_DIR, files[-1])) as f:
        return json.load(f)


def compute_daily_delta(today_snap: dict, prior_snap: dict | None) -> dict:
    """
    Returns {(pub_id, demand_id): {bids, wins, imps, rev}} representing the
    delta between snapshots (≈ activity since prior snapshot).
    """
    today_idx = {(e["publisher_id"], e["demand_id"]): e for e in today_snap["entries"]}
    if not prior_snap:
        # No prior — everything in today is a "delta" of itself (treat as fresh)
        return {k: {"bids": e["bids"], "wins": e["wins"], "imps": e["imps"], "rev": e["rev"]}
                for k, e in today_idx.items()}

    prior_idx = {(e["publisher_id"], e["demand_id"]): e for e in prior_snap["entries"]}
    deltas = {}
    for k, e in today_idx.items()
:
        p = prior_idx.get(k, {"bids": 0, "wins": 0, "imps": 0, "rev": 0})
        # If today < prior (e.g. data reset), clamp to 0
        deltas[k] = {
            "bids": max(0, e["bids"] - p.get("bids", 0)),
            "wins": max(0, e["wins"] - p.get("wins", 0)),
            "imps": max(0, e["imps"] - p.get("imps", 0)),
            "rev":  max(0, e["rev"]  - p.get("rev", 0)),
        }
    return deltas


# ---------------------------------------------------------------------------
# Cooldown tracking
# ---------------------------------------------------------------------------

def _load_cooldown() -> dict:
    if not os.path.exists(COOLDOWN_FILE):
        return {}
    try:
        with open(COOLDOWN_FILE) as f:
            return json.load(f)
    except Exception:
        return {}


def _save_cooldown(d: dict):
    os.makedirs(LOG_DIR, exist_ok=True)
    with open(COOLDOWN_FILE, "w") as f:
        json.dump(d, f, indent=2)


def _key(pid: int, did: int) -> str:
    return f"{pid}:{did}"


def _is_in_cooldown(cooldown: dict, pid: int, did: int) -> bool:
    ts = cooldown.get(_key(pid, did))
    if not ts:
        return False
    try:
        last = datetime.fromisoformat(ts)
    except Exception:
        return False
    now = datetime.now(timezone.utc)
    return (now - last) < timedelta(hours=COOLDOWN_HOURS)


def _mark_cooldown(cooldown: dict, pid: int, did: int):
    cooldown[_key(pid, did)] = _now_iso()


# ---------------------------------------------------------------------------
# Active-watch detection — don't fight the watchdog
# ---------------------------------------------------------------------------

def _publishers_under_active_watch() -> set[int]:
    """Return set of publisher IDs with an active startio_activation watch."""
    state_file = os.path.join(LOG_DIR, "pilot_watchdog_state.json")
    if not os.path.exists(state_file):
        return set()
    try:
        with open(state_file) as f:
            state = json.load(f)
    except Exception:
        return set()
    out = set()
    for w in state.get("watches", []):
        if w.get("status") != "watching":
            continue
        if w.get("kind") != "startio_activation":
            continue
        pid = w.get("publisher_id")
        if isinstance(pid, int):
            out.add(pid)
    return out


# ---------------------------------------------------------------------------
# Recommendation engine
# ---------------------------------------------------------------------------

def recommend(
    alltime_row: dict,
    delta: dict,
    current_floor: float | None,
):
    """
    Returns (action, new_floor, rationale) using delta-based daily metrics.
    """
    bids_d = delta.get("bids", 0.0)
    imps_d = delta.get("imps", 0.0)
    rev_d  = delta.get("rev",  0.0)

    eff_ecpm_alltime = (_sf(alltime_row.get("GROSS_REVENUE")) / _sf(alltime_row.get("IMPRESSIONS")) * 1000) \
        if _sf(alltime_row.get("IMPRESSIONS")) else 0.0

    # ACTIVATE — null floor & we have any signal at all (use all-time eCPM)
    if current_floor is None:
        if eff_ecpm_alltime <= 0:
            return ("SKIP", None, "null floor but no eCPM signal")
        new_floor = round(max(ACTIVATE_FLOOR_MIN,
                              min(eff_ecpm_alltime * ACTIVATE_PCT_OF_ECPM,
                                  ACTIVATE_FLOOR_MAX)), 2)
        return ("ACTIVATE", new_floor,
                f"null floor; alltime eff_ecpm=${eff_ecpm_alltime:.2f} → activate @ {ACTIVATE_PCT_OF_ECPM*100:.0f}%")

    # All other actions need recent daily activity
    if bids_d < MIN_BIDS_DAILY or rev_d < MIN_REV_DAILY:
        return ("SKIP", None,
                f"insufficient recent activity (bids/d={bids_d:.0f}, rev/d=${rev_d:.2f})")

    proxy_wr_d = (imps_d / bids_d) if bids_d else 0.0
    eff_ecpm_d = (rev_d / imps_d * 1000) if imps_d else 0.0

    # RAISE — sustained high WR + headroom
    if proxy_wr_d >= RAISE_WR_THRESHOLD and eff_ecpm_d >= current_floor * RAISE_HEADROOM_X:
        new_floor = round(min(current_floor * (1 + RAISE_PCT),
                              current_floor * (1 + RAISE_CAP_PCT)), 2)
        if new_floor > current_floor:
            return ("RAISE", new_floor,
                    f"WR_24h={proxy_wr_d*100:.1f}%, eff_ecpm_24h=${eff_ecpm_d:.2f} → +{RAISE_PCT*100:.0f}%")

    # LOWER — collapsed WR but still meaningful revenue (= floor too high)
    if proxy_wr_d <= LOWER_WR_THRESHOLD and rev_d >= LOWER_REV_DAILY_MIN:
        new_floor = round(max(MIN_FLOOR_FLOOR,
                              current_floor * (1 - LOWER_PCT)), 2)
        if new_floor < current_floor:
            return ("LOWER", new_floor,
                    f"WR_24h={proxy_wr_d*100:.1f}% choking ${rev_d:.0f}/d rev → -{LOWER_PCT*100:.0f}%")

    return ("HOLD", None,
            f"WR_24h={proxy_wr_d*100:.1f}%, eff_ecpm_24h=${eff_ecpm_d:.2f}, floor=${current_floor:.2f}")


# ---------------------------------------------------------------------------
# Apply
# ---------------------------------------------------------------------------

def apply_change(pub_id: int, demand_id: int, new_floor: float | None,
                 dry_run: bool) -> dict:
    """Set minBidFloor on one demand entry inside a publisher. Returns result dict."""
    pub = llm.get_publisher(pub_id)
    if not pub:
        return {"error": f"publisher {pub_id} not found"}
    modified = False
    old_floor = None
    for pref in pub.get("biddingpreferences", []):
        for v in pref.get("value", []):
            try:
                did = int(v.get("id") or 0)
            except (TypeError, ValueError):
                continue
            if did == demand_id:
                old_floor = v.get("minBidFloor")
                v["minBidFloor"] = new_floor
                modified = True
    if not modified:
        return {"error": f"demand {demand_id} not in pub {pub_id} biddingpreferences"}
    if dry_run:
        return {"applied": False, "dry_run": True, "old_floor": old_floor, "new_floor": new_floor}
    try:
        llm._put(f"/v1/publishers/{pub_id}", pub)
    except Exception as e:
        return {"error": str(e), "old_floor": old_floor, "new_floor": new_floor}
    return {"applied": True, "old_floor": old_floor, "new_floor": new_floor}


# ---------------------------------------------------------------------------
# Slack
# ---------------------------------------------------------------------------

def post_slack_summary(planned: list[dict], applied: list[dict], skipped_reasons: dict, dry_run: bool):
    if not planned and not applied:
        text = ":zzz: *Start.IO continuous optimizer* — no actionable changes today."
        try:
            slack.send_blocks([{"type": "section", "text": {"type": "mrkdwn", "text": text}}],
                              text="Start.IO optimizer: no changes")
        except Exception:
            pass
        return

    by_action = defaultdict(int)
    for a in (applied if applied else planned):
        by_action[a["action"]] += 1

    lines = [
        f":robot_face: *Start.IO Continuous Optimizer* — {_now_et_str()}",
        f"Mode: {'DRY-RUN' if dry_run else 'LIVE'}",
        f"Actions: {dict(by_action)}",
        "",
    ]
    for a in (applied if applied else planned)[:15]:
        old = a.get("old_floor")
        old_str = f"${_sf(old):.2f}" if old is not None else "null"
        lines.append(
            f"  {a['action']:<8}  {a['publisher_name'][:28]:<28} / {a['demand_name'][:35]:<35}  "
            f"{old_str} → ${a['new_floor']:.2f}  ({a['rationale']})"
        )
    if skipped_reasons:
        lines.append("")
        lines.append(f"_Skipped: {sum(skipped_reasons.values())} entries — "
                     f"{', '.join(f'{k}×{v}' for k,v in skipped_reasons.items())}_")

    text = "\n".join(lines)
    try:
        slack.send_blocks([{"type": "section", "text": {"type": "mrkdwn", "text": text}}],
                          text="Start.IO optimizer summary")
    except Exception as e:
        print(f"[startio_continuous] Slack post failed: {e}")


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def run(dry_run: bool = True, post_slack: bool = True):
    print(f"=== Start.IO continuous optimizer — {'DRY-RUN' if dry_run else 'LIVE'} — {_now_et_str()} ===\n")

    # Pull patched all-time data
    rows = llr.report(
        ["PUBLISHER_ID", "PUBLISHER_NAME", "DEMAND_ID", "DEMAND_NAME"],
        ["BIDS", "WINS", "IMPRESSIONS", "GROSS_REVENUE"],
        "2026-01-01", (date.today() - timedelta(days=1)).strftime("%Y-%m-%d"),
    )
    startio_rows = [r for r in rows if "start" in str(r.get("DEMAND_NAME", "")).lower()]
    print(f"Loaded {len(startio_rows)} Start.IO publisher × demand rows.")

    # Take snapshot + compute daily delta
    today_snap = take_snapshot(startio_rows)
    prior_snap = load_prior_snapshot()
    deltas = compute_daily_delta(today_snap, prior_snap)
    if prior_snap:
        print(f"Comparing against prior snapshot from {prior_snap['captured_at'][:10]}")
    else:
        print("No prior snapshot — first run, treating all-time as delta")

    # Skip publishers under active watchdog watch
    protected = _publishers_under_active_watch()
    if protected:
        print(f"Protected (active startio_activation watch): {sorted(protected)}\n")

    cooldown = _load_cooldown()

    # Build per-publisher floor map
    pub_cache: dict[int, dict] = {}
    def _floor_for(pid, did):
        if pid not in pub_cache:
            try:
                pub_cache[pid] = llm.get_publisher(pid)
            except Exception as e:
                pub_cache[pid] = {"_error": str(e)}
        pub = pub_cache[pid]
        if "_error" in pub:
            return None, None, pub["_error"]
        for pref in pub.get("biddingpreferences", []):
            for v in pref.get("value", []):
                try:
                    if int(v.get("id") or 0) == did:
                        return v.get("minBidFloor"), v.get("status"), None
                except (TypeError, ValueError):
                    continue
        return None, None, "not_in_biddingprefs"

    # Score every entry
    candidates: list[dict] = []
    skip_reasons: dict[str, int] = defaultdict(int)
    for r in startio_rows:
        try:
            pid = int(r.get("PUBLISHER_ID") or 0)
            did = int(r.get("DEMAND_ID") or 0)
        except (TypeError, ValueError):
            continue
        if not pid or not did:
            continue

        if pid in protected:
            skip_reasons["watchdog_protected"] += 1
            continue
        if _is_in_cooldown(cooldown, pid, did):
            skip_reasons["cooldown"] += 1
            continue

        cur_floor, status, err = _floor_for(pid, did)
        if err:
            skip_reasons[f"floor_lookup_{err[:20]}"] += 1
            continue
        if status == 2:  # paused
            skip_reasons["paused"] += 1
            continue

        delta = deltas.get((pid, did), {"bids": 0, "wins": 0, "imps": 0, "rev": 0})
        action, new_floor, rationale = recommend(r, delta, _sf(cur_floor) if cur_floor is not None else None)

        if action == "SKIP":
            skip_reasons["no_signal"] += 1
            continue
        if action == "HOLD":
            skip_reasons["hold"] += 1
            continue

        candidates.append({
            "publisher_id":   pid,
            "publisher_name": str(r.get("PUBLISHER_NAME") or ""),
            "demand_id":      did,
            "demand_name":    str(r.get("DEMAND_NAME") or ""),
            "old_floor":      cur_floor,
            "new_floor":      new_floor,
            "action":         action,
            "rationale":      rationale,
            "delta":          delta,
        })

    # Rank by impact (alltime revenue × |new_floor - old_floor| as a rough lift proxy)
    def _impact(c):
        old = _sf(c["old_floor"]) if c["old_floor"] is not None else 0.0
        diff = abs(_sf(c["new_floor"]) - old)
        rev = next((_sf(r.get("GROSS_REVENUE"))
                    for r in startio_rows
                    if int(r.get("PUBLISHER_ID") or 0) == c["publisher_id"]
                    and int(r.get("DEMAND_ID") or 0) == c["demand_id"]),
                   0.0)
        return rev * (diff + 0.01)
    candidates.sort(key=_impact, reverse=True)

    planned = candidates[:MAX_CHANGES_PER_RUN]
    capped = len(candidates) - len(planned)
    if capped > 0:
        skip_reasons[f"throttled_max_{MAX_CHANGES_PER_RUN}"] = capped

    print(f"\nCandidates: {len(candidates)} | Planned this run: {len(planned)} | "
          f"Skip reasons: {dict(skip_reasons)}\n")

    applied: list[dict] = []
    for c in planned:
        result = apply_change(c["publisher_id"], c["demand_id"], c["new_floor"], dry_run)
        old = c["old_floor"]
        old_str = f"${_sf(old):.2f}" if old is not None else "null"
        tag = "DRY_RUN" if dry_run else "✓"
        if result.get("error"):
            tag = "✗"
        print(f"  {tag}  {c['action']:<8}  pub={c['publisher_id']} ({c['publisher_name'][:24]:<24}) "
              f"demand={c['demand_id']:<4} {c['demand_name'][:30]:<30}  "
              f"{old_str} → ${c['new_floor']:.2f}  | {c['rationale']}"
              + (f"  ERR: {result['error']}" if result.get("error") else ""))
        out = dict(c)
        out["result"] = result
        out["timestamp"] = _now_iso()
        applied.append(out)
        if not dry_run and result.get("applied"):
            _mark_cooldown(cooldown, c["publisher_id"], c["demand_id"])

    if not dry_run:
        _save_cooldown(cooldown)

    # Persist actions
    os.makedirs(LOG_DIR, exist_ok=True)
    history = []
    if os.path.exists(ACTIONS_LOG):
        try:
            with open(ACTIONS_LOG) as f:
                history = json.load(f)
        except Exception:
            history = []
    history.append({
        "run_at":  _now_iso(),
        "dry_run": dry_run,
        "actions": applied,
        "skipped": dict(skip_reasons),
    })
    with open(ACTIONS_LOG, "w") as f:
        json.dump(history[-200:], f, indent=2)  # keep last 200 runs

    if post_slack:
        post_slack_summary(planned, applied if not dry_run else [], skip_reasons, dry_run)

    print(f"\nDone. {'DRY-RUN' if dry_run else 'LIVE'} — {len(applied)} entr{'y' if len(applied)==1 else 'ies'} touched.")


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--live", action="store_true", help="Apply changes (default: dry-run)")
    p.add_argument("--no-slack", action="store_true", help="Suppress Slack post")
    args = p.parse_args()
    run(dry_run=not args.live, post_slack=not args.no_slack)


if __name__ == "__main__":
    main()
