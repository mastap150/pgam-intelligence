"""
scripts/pilot_watchdog.py

Post-change performance monitor for the PGAM pilot program.

After floor changes are applied via pilot_actions.py, this watchdog:
  1. Captures a 7-day daily-average baseline for each publisher at time of change
  2. Runs daily at 09:30 ET for up to WATCH_DAYS (7) days per watch
  3. Compares yesterday's stats to the pre-change baseline
  4. Auto-reverts all floor changes for a publisher if:
       - Revenue drops > REV_DROP_THRESHOLD (15 %) vs daily baseline, AND
       - eCPM has NOT improved > ECPM_LIFT_THRESHOLD (10 %) — if eCPM is up,
         that means the floor is working (fewer, better bids), so we hold
  5. Posts a Slack update after every check (stable / reverted)

State file: logs/pilot_watchdog_state.json
Structure:
  {
    "watches": [
      {
        "id": "<uuid>",
        "publisher_name": "AppStock",
        "changes": [           ← list of action dicts from pilot_actions
          {demand_name, old_floor, new_floor, ...}
        ],
        "baseline": {
          "rev_daily_avg":  55.0,
          "wins_daily_avg": 50000,
          "ecpm_avg":       1.20,
          "win_rate_avg":   0.025
        },
        "applied_at":  "2026-04-14T...",
        "expires_at":  "2026-04-21T...",
        "status":      "watching",   ← watching | stable | reverted | expired
        "checks": [
          {
            "checked_at": "...",
            "date_checked": "YYYY-MM-DD",   ← yesterday
            "rev":  52.0,  "rev_delta_pct": -5.5,
            "wins": 48000, "win_delta_pct": -4.0,
            "ecpm": 1.35,  "ecpm_delta_pct": +12.5,
            "action": "hold"   ← hold | revert
          }
        ]
      }
    ]
  }
"""

import json
import os
import sys
import uuid
from datetime import datetime, timedelta, timezone

_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

from core.api import fetch as ll_fetch
import core.slack as slack
import core.ll_mgmt as llm
import core.ll_report as llr
from scripts.pilot_actions import remove_floor_change, log_action

# ---------------------------------------------------------------------------
# Thresholds
# ---------------------------------------------------------------------------

REV_DROP_THRESHOLD   = 15.0   # % revenue drop that triggers a revert
ECPM_LIFT_THRESHOLD  = 10.0   # % eCPM improvement that overrides a revert
WATCH_DAYS           = 7      # days to monitor after each change

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

LOG_DIR    = os.path.join(os.path.dirname(__file__), "..", "logs")
STATE_FILE = os.path.normpath(os.path.join(LOG_DIR, "pilot_watchdog_state.json"))

ET_TZ = None
try:
    from zoneinfo import ZoneInfo
    ET_TZ = ZoneInfo("America/New_York")
except Exception:
    pass


# ---------------------------------------------------------------------------
# State helpers
# ---------------------------------------------------------------------------

def _load_state() -> dict:
    if not os.path.exists(STATE_FILE):
        return {"watches": []}
    try:
        with open(STATE_FILE) as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError):
        return {"watches": []}


def _save_state(state: dict):
    os.makedirs(LOG_DIR, exist_ok=True)
    with open(STATE_FILE, "w") as f:
        json.dump(state, f, indent=2)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _now_et_str() -> str:
    try:
        return datetime.now(ET_TZ).strftime("%Y-%m-%d %H:%M ET")
    except Exception:
        return datetime.now().strftime("%Y-%m-%d %H:%M")


def _yesterday_str() -> str:
    return (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")


# ---------------------------------------------------------------------------
# Baseline capture
# ---------------------------------------------------------------------------

def _get_baseline(publisher_name: str) -> dict:
    """
    Fetch the last 7 days of daily stats for publisher_name and return
    daily averages for revenue, wins, and eCPM.
    """
    end   = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    start = (datetime.now() - timedelta(days=8)).strftime("%Y-%m-%d")

    rows = ll_fetch(
        "DATE,PUBLISHER",
        "GROSS_REVENUE,WINS,BIDS,GROSS_ECPM",
        start, end,
    )

    pub_lower = publisher_name.lower()
    daily = [
        r for r in rows
        if pub_lower in (r.get("PUBLISHER_NAME") or r.get("PUBLISHER") or "").lower()
    ]

    if not daily:
        return {"rev_daily_avg": 0.0, "wins_daily_avg": 0.0, "ecpm_avg": 0.0, "win_rate_avg": 0.0}

    rev_total  = sum(float(r.get("GROSS_REVENUE", 0) or 0) for r in daily)
    wins_total = sum(float(r.get("WINS", 0) or 0) for r in daily)
    bids_total = sum(float(r.get("BIDS", 0) or 0) for r in daily)
    n = len(daily)

    # eCPM: weighted average (total revenue / total impressions * 1000)
    # Use simple average of daily eCPMs as fallback
    ecpm_vals = [float(r.get("GROSS_ECPM", 0) or 0) for r in daily if float(r.get("GROSS_ECPM", 0) or 0) > 0]
    ecpm_avg  = sum(ecpm_vals) / len(ecpm_vals) if ecpm_vals else 0.0

    return {
        "rev_daily_avg":  round(rev_total / n, 4),
        "wins_daily_avg": round(wins_total / n, 0),
        "ecpm_avg":       round(ecpm_avg, 4),
        "win_rate_avg":   round((wins_total / bids_total * 100) if bids_total > 0 else 0.0, 4),
        "days_sampled":   n,
        "baseline_start": start,
        "baseline_end":   end,
    }


# ---------------------------------------------------------------------------
# Start.IO-specific watch helpers (kind="startio_activation")
# ---------------------------------------------------------------------------
#
# These extend the standard publisher-level watch with Start.IO-specific
# baselines and a custom revert path that restores minBidFloor=None on the
# demand entries our optimizer touched (the standard remove_floor_change()
# uses a different mechanism and would NOT undo our edits).
# ---------------------------------------------------------------------------

def _get_startio_baseline(publisher_id: int) -> dict:
    """
    Capture an all-time Start.IO baseline for one publisher using the patched
    POST /v1/report endpoint (WINS proxied via IMPRESSIONS — see
    core/ll_report.py docstring). All-time is what the API returns regardless
    of date filters; we record it as a snapshot for trend comparison.
    """
    try:
        rows = llr.report(
            ["PUBLISHER_ID", "DEMAND_ID", "DEMAND_NAME"],
            ["BIDS", "WINS", "IMPRESSIONS", "GROSS_REVENUE"],
            "2026-01-01", _yesterday_str(),
        )
    except Exception as e:
        return {"error": str(e), "captured_at": _now_iso()}

    pid = int(publisher_id)
    startio_rows = [
        r for r in rows
        if "start" in str(r.get("DEMAND_NAME", "")).lower()
        and int(r.get("PUBLISHER_ID") or 0) == pid
    ]

    bids = sum(float(r.get("BIDS") or 0) for r in startio_rows)
    wins = sum(float(r.get("WINS") or 0) for r in startio_rows)
    imps = sum(float(r.get("IMPRESSIONS") or 0) for r in startio_rows)
    rev  = sum(float(r.get("GROSS_REVENUE") or 0) for r in startio_rows)
    return {
        "captured_at":  _now_iso(),
        "n_demands":    len(startio_rows),
        "bids":         bids,
        "wins":         wins,
        "imps":         imps,
        "rev_total":    round(rev, 4),
        "eff_ecpm":     round((rev / imps) * 1000, 4) if imps else 0.0,
        "proxy_wr_pct": round((imps / bids) * 100, 4) if bids else 0.0,
        "note":         "all-time snapshot (POST /v1/report ignores date filters)",
    }


def register_startio_watch(
    publisher_id: int,
    publisher_name: str,
    changes: list[dict],
) -> str:
    """
    Register a Start.IO-flavored watch.

    Adds two things on top of the standard watch:
      • startio_baseline — Start.IO-only revenue/eCPM snapshot
      • kind="startio_activation" — routes reverts through the
        minBidFloor-restoring path instead of the typeField mechanism

    Args:
        publisher_id:   LL publisher ID (e.g. 290115375)
        publisher_name: Display name (e.g. "Start.IO - Video Magnite")
        changes:        list of change dicts from the optimizer; each must
                        include {demand_id, demand_name, old_floor, new_floor, action}
    """
    baseline_pub     = _get_baseline(publisher_name)
    startio_baseline = _get_startio_baseline(publisher_id)

    watch_id   = str(uuid.uuid4())[:8]
    applied_at = _now_iso()
    expires_at = (datetime.now(timezone.utc) + timedelta(days=WATCH_DAYS)).isoformat()

    watch = {
        "id":               watch_id,
        "kind":             "startio_activation",
        "publisher_id":     publisher_id,
        "publisher_name":   publisher_name,
        "changes":          changes,
        "baseline":         baseline_pub,
        "startio_baseline": startio_baseline,
        "applied_at":       applied_at,
        "expires_at":       expires_at,
        "status":           "watching",
        "checks":           [],
    }

    state = _load_state()
    state["watches"].append(watch)
    _save_state(state)

    print(
        f"[watchdog] Registered Start.IO watch {watch_id} for {publisher_name!r} "
        f"(pub_id={publisher_id})  "
        f"pub_baseline_rev/day=${baseline_pub['rev_daily_avg']:.2f}  "
        f"startio_baseline_rev=${startio_baseline.get('rev_total', 0):.2f}  "
        f"({len(changes)} activations)  expires {expires_at[:10]}"
    )
    return watch_id


def _revert_startio_watch(watch: dict) -> list[dict]:
    """
    Restore minBidFloor=None on every demand entry our optimizer touched.

    The standard remove_floor_change() targets typeFields[type=3].setOnRule,
    which is a different field from minBidFloor. To undo what
    startio_floor_optimizer_apr17.py did, we must clear minBidFloor itself.
    """
    pub_id = watch["publisher_id"]
    pub_name = watch["publisher_name"]
    results: list[dict] = []

    try:
        pub = llm.get_publisher(pub_id)
    except Exception as e:
        print(f"[watchdog] revert fetch error pub_id={pub_id}: {e}")
        return [{"error": str(e)}]

    target_ids = {int(c["demand_id"]) for c in watch.get("changes", []) if c.get("demand_id") is not None}
    modified = False

    for pref in pub.get("biddingpreferences", []):
        for v in pref.get("value", []):
            try:
                did = int(v.get("id") or 0)
            except (TypeError, ValueError):
                continue
            if did in target_ids:
                old_floor = v.get("minBidFloor")
                v["minBidFloor"] = None
                modified = True
                results.append({
                    "demand_id":      did,
                    "publisher_id":   pub_id,
                    "publisher_name": pub_name,
                    "old_floor":      old_floor,
                    "new_floor":      None,
                    "applied":        True,
                    "action":         "floor_remove_startio",
                    "timestamp":      _now_iso(),
                })

    if modified:
        try:
            llm._put(f"/v1/publishers/{pub_id}", pub)
            print(f"[watchdog] Reverted {len(results)} Start.IO floor(s) on {pub_name}")
        except Exception as e:
            print(f"[watchdog] PUT failed during Start.IO revert pub_id={pub_id}: {e}")
            for r in results:
                r["applied"] = False
                r["error"] = str(e)

    return results


def _check_startio_watch(watch: dict) -> dict:
    """
    Combined check: standard publisher-level deltas + Start.IO-specific
    snapshot delta (rev_total / eff_ecpm vs the captured baseline).

    Returns the standard check dict augmented with startio_* fields.
    """
    check = _check_watch(watch)

    # Add Start.IO-specific snapshot (all-time, so this is more like a slow
    # trend than a daily delta — useful for catching multi-day shifts)
    pub_id = watch.get("publisher_id")
    if pub_id:
        current = _get_startio_baseline(pub_id)
        base = watch.get("startio_baseline", {})
        base_rev = float(base.get("rev_total") or 0)
        cur_rev  = float(current.get("rev_total") or 0)
        base_ecpm = float(base.get("eff_ecpm") or 0)
        cur_ecpm  = float(current.get("eff_ecpm") or 0)

        check["startio_rev_total"]      = cur_rev
        check["startio_rev_delta_abs"]  = round(cur_rev - base_rev, 2)
        check["startio_ecpm"]           = cur_ecpm
        check["startio_ecpm_delta_pct"] = round(((cur_ecpm - base_ecpm) / base_ecpm * 100) if base_ecpm > 0 else 0.0, 2)
        check["startio_n_demands"]      = current.get("n_demands", 0)

    return check


# ---------------------------------------------------------------------------
# lurlEnabled fleet watch helpers (kind="lurl_enable")
# ---------------------------------------------------------------------------
#
# After flipping lurlEnabled False→True on a publisher, we want to monitor:
#   • publisher revenue (must not drop > REV_DROP_THRESHOLD)
#   • publisher eCPM (want to see it rise; DSPs learning win prices)
#   • implied margin (we track via GROSS_REVENUE vs a baseline margin proxy —
#     since lurlEnabled doesn't change our LL margin field, "margin maintained"
#     effectively means rev didn't collapse and eCPM held up)
# If rev drops beyond threshold AND eCPM didn't improve, we PUT lurlEnabled=False.
# ---------------------------------------------------------------------------

def register_lurl_watch(
    publisher_id: int,
    publisher_name: str,
    prior_lurl: bool,
    new_lurl: bool = True,
) -> str:
    """Register a watch after flipping lurlEnabled on a publisher."""
    baseline = _get_baseline(publisher_name)
    watch_id   = str(uuid.uuid4())[:8]
    applied_at = _now_iso()
    expires_at = (datetime.now(timezone.utc) + timedelta(days=WATCH_DAYS)).isoformat()

    change = {
        "action":        "lurl_enable",
        "publisher_id":  publisher_id,
        "publisher_name": publisher_name,
        "prior_lurl":    prior_lurl,
        "new_lurl":      new_lurl,
        "applied":       True,
        "timestamp":     applied_at,
    }

    watch = {
        "id":             watch_id,
        "kind":           "lurl_enable",
        "publisher_id":   publisher_id,
        "publisher_name": publisher_name,
        "changes":        [change],
        "baseline":       baseline,
        "applied_at":     applied_at,
        "expires_at":     expires_at,
        "status":         "watching",
        "checks":         [],
    }

    state = _load_state()
    state["watches"].append(watch)
    _save_state(state)

    print(
        f"[watchdog] Registered LURL watch {watch_id} for {publisher_name!r} "
        f"(pub_id={publisher_id})  "
        f"baseline rev/day=${baseline['rev_daily_avg']:.2f}  "
        f"eCPM=${baseline['ecpm_avg']:.3f}  expires {expires_at[:10]}"
    )
    return watch_id


def _revert_lurl_watch(watch: dict) -> list[dict]:
    """Restore lurlEnabled to its prior value on the publisher."""
    pub_id   = watch["publisher_id"]
    pub_name = watch["publisher_name"]
    change   = watch["changes"][0]
    prior    = bool(change.get("prior_lurl", False))

    try:
        pub = llm.get_publisher(pub_id)
    except Exception as e:
        print(f"[watchdog] LURL revert fetch error pub_id={pub_id}: {e}")
        return [{"error": str(e), "applied": False}]

    pub["lurlEnabled"] = prior
    try:
        llm._put(f"/v1/publishers/{pub_id}", pub)
        print(f"[watchdog] Reverted lurlEnabled → {prior} on {pub_name} (pub_id={pub_id})")
        return [{
            "action":         "lurl_revert",
            "publisher_id":   pub_id,
            "publisher_name": pub_name,
            "restored_lurl":  prior,
            "applied":        True,
            "timestamp":      _now_iso(),
        }]
    except Exception as e:
        print(f"[watchdog] LURL revert PUT failed pub_id={pub_id}: {e}")
        return [{"error": str(e), "applied": False, "publisher_id": pub_id}]


# ---------------------------------------------------------------------------
# Register a new watch
# ---------------------------------------------------------------------------

def register_watch(publisher_name: str, changes: list[dict]) -> str:
    """
    Register a new watchdog entry after floor changes have been applied.

    Args:
        publisher_name: e.g. "AppStock" or "PubNative - Display In-App EU"
        changes: list of action dicts returned by apply_floor_change()

    Returns:
        watch_id (str)
    """
    # The LL stats API reports PubNative as a single "PubNative" publisher
    # regardless of sub-entry name.  Use the root name for baseline lookups.
    baseline_key = "pubnative" if "pubnative" in publisher_name.lower() else publisher_name
    baseline     = _get_baseline(baseline_key)
    watch_id   = str(uuid.uuid4())[:8]
    applied_at = _now_iso()
    expires_at = (datetime.now(timezone.utc) + timedelta(days=WATCH_DAYS)).isoformat()

    watch = {
        "id":             watch_id,
        "publisher_name": publisher_name,
        "changes":        changes,
        "baseline":       baseline,
        "applied_at":     applied_at,
        "expires_at":     expires_at,
        "status":         "watching",
        "checks":         [],
    }

    state = _load_state()
    state["watches"].append(watch)
    _save_state(state)

    print(
        f"[watchdog] Registered watch {watch_id} for {publisher_name!r}  "
        f"baseline: rev/day=${baseline['rev_daily_avg']:.2f}  "
        f"wins/day={baseline['wins_daily_avg']:.0f}  "
        f"eCPM=${baseline['ecpm_avg']:.3f}  "
        f"expires {expires_at[:10]}"
    )
    return watch_id


# ---------------------------------------------------------------------------
# Check a single watch
# ---------------------------------------------------------------------------

def _check_watch(watch: dict) -> dict:
    """
    Fetch yesterday's stats for this watch's publisher and compare to baseline.

    Returns a check dict with deltas and recommended action ("hold" or "revert").
    """
    yesterday = _yesterday_str()
    pub_name  = watch["publisher_name"]
    baseline  = watch["baseline"]

    rows = ll_fetch(
        "DATE,PUBLISHER",
        "GROSS_REVENUE,WINS,BIDS,GROSS_ECPM",
        yesterday, yesterday,
    )

    # Use root name for matching (PubNative sub-entries all report as "pubnative")
    pub_lower = "pubnative" if "pubnative" in pub_name.lower() else pub_name.lower()
    day_rows  = [
        r for r in rows
        if pub_lower in (r.get("PUBLISHER_NAME") or r.get("PUBLISHER") or "").lower()
    ]

    if not day_rows:
        return {
            "checked_at":    _now_iso(),
            "date_checked":  yesterday,
            "rev":           0.0,
            "wins":          0,
            "ecpm":          0.0,
            "rev_delta_pct": -100.0,
            "win_delta_pct": -100.0,
            "ecpm_delta_pct": 0.0,
            "action":        "no_data",
            "note":          "No stats returned for this publisher/date",
        }

    rev   = sum(float(r.get("GROSS_REVENUE", 0) or 0) for r in day_rows)
    wins  = sum(float(r.get("WINS", 0) or 0) for r in day_rows)
    bids  = sum(float(r.get("BIDS", 0) or 0) for r in day_rows)
    ecpm_vals = [float(r.get("GROSS_ECPM", 0) or 0) for r in day_rows if float(r.get("GROSS_ECPM", 0) or 0) > 0]
    ecpm  = sum(ecpm_vals) / len(ecpm_vals) if ecpm_vals else 0.0

    base_rev  = baseline.get("rev_daily_avg", 0.0)
    base_ecpm = baseline.get("ecpm_avg", 0.0)

    rev_delta_pct  = ((rev  - base_rev)  / base_rev  * 100) if base_rev  > 0 else 0.0
    ecpm_delta_pct = ((ecpm - base_ecpm) / base_ecpm * 100) if base_ecpm > 0 else 0.0

    # Revert if revenue is down enough AND eCPM hasn't improved enough to justify it
    should_revert = (
        rev_delta_pct < -REV_DROP_THRESHOLD
        and ecpm_delta_pct < ECPM_LIFT_THRESHOLD
    )

    action = "revert" if should_revert else "hold"

    note = ""
    if should_revert:
        note = (
            f"Revenue down {rev_delta_pct:.1f}% vs baseline "
            f"and eCPM only {ecpm_delta_pct:+.1f}% — reverting floors."
        )
    elif rev_delta_pct < -REV_DROP_THRESHOLD and ecpm_delta_pct >= ECPM_LIFT_THRESHOLD:
        note = (
            f"Revenue down {rev_delta_pct:.1f}% but eCPM up {ecpm_delta_pct:+.1f}% "
            f"— floor is filtering low-quality bids. Holding."
        )
    elif rev_delta_pct >= 0:
        note = f"Revenue up {rev_delta_pct:+.1f}% vs baseline. Floor is working."
    else:
        note = f"Revenue {rev_delta_pct:+.1f}% vs baseline — within acceptable range."

    return {
        "checked_at":     _now_iso(),
        "date_checked":   yesterday,
        "rev":            round(rev, 4),
        "wins":           int(wins),
        "ecpm":           round(ecpm, 4),
        "rev_delta_pct":  round(rev_delta_pct, 2),
        "ecpm_delta_pct": round(ecpm_delta_pct, 2),
        "action":         action,
        "note":           note,
    }


# ---------------------------------------------------------------------------
# Revert a watch
# ---------------------------------------------------------------------------

def _revert_watch(watch: dict) -> list[dict]:
    """
    Remove all floor changes associated with this watch.
    Returns list of revert results.
    """
    results = []
    pub_name = watch["publisher_name"]
    reason   = f"Auto-revert by watchdog: {watch['checks'][-1]['note']}"

    for change in watch["changes"]:
        demand_name = change.get("demand_name")
        if not demand_name:
            continue
        try:
            r = remove_floor_change(
                publisher_name=pub_name,
                demand_name=demand_name,
                reason=reason,
                dry_run=False,
            )
            results.append(r)
        except Exception as exc:
            print(f"[watchdog] ERROR reverting {pub_name}/{demand_name}: {exc}")
            results.append({"demand_name": demand_name, "error": str(exc)})

    return results


# ---------------------------------------------------------------------------
# Slack notifications
# ---------------------------------------------------------------------------

def _slack_check_blocks(watch: dict, check: dict) -> list[dict]:
    pub      = watch["publisher_name"]
    baseline = watch["baseline"]
    action   = check["action"]
    day      = check["date_checked"]
    kind     = watch.get("kind", "standard")

    if action == "revert":
        icon  = ":rotating_light:"
        title = "Pilot Watchdog — Auto-Revert Triggered"
    elif action == "no_data":
        icon  = ":warning:"
        title = "Pilot Watchdog — No Data"
    else:
        icon  = ":white_check_mark:"
        title = "Pilot Watchdog — Floor Stable"

    if kind == "startio_activation":
        title += "  (Start.IO Activation)"

    lines = [
        f"{icon} *{title}*",
        f"Publisher: {pub}",
        f"Date checked: {day}",
        f"",
        f"*Yesterday vs 7-day baseline:*",
        f"Revenue:  ${check['rev']:.2f}  vs  ${baseline['rev_daily_avg']:.2f}/day  ({check['rev_delta_pct']:+.1f}%)",
        f"eCPM:     ${check['ecpm']:.3f}  vs  ${baseline['ecpm_avg']:.3f}  ({check['ecpm_delta_pct']:+.1f}%)",
        f"Wins:     {check['wins']:,}",
    ]

    if kind == "startio_activation" and "startio_rev_total" in check:
        sb = watch.get("startio_baseline", {})
        lines.extend([
            f"",
            f"*Start.IO-only (snapshot vs activation baseline):*",
            f"Start.IO rev_total:  ${check['startio_rev_total']:.2f}  "
            f"(vs ${float(sb.get('rev_total') or 0):.2f} at activation, "
            f"Δ ${check['startio_rev_delta_abs']:+.2f})",
            f"Start.IO eff_eCPM:   ${check['startio_ecpm']:.3f}  "
            f"({check['startio_ecpm_delta_pct']:+.1f}%)",
            f"Active Start.IO demands: {check['startio_n_demands']}  "
            f"|  Activations applied: {len(watch.get('changes', []))}",
        ])

    lines.extend([
        f"",
        f"_{check['note']}_",
        f"",
        f"Watch expires: {watch['expires_at'][:10]}  |  {_now_et_str()}",
    ])

    return [{"type": "section", "text": {"type": "mrkdwn", "text": "\n".join(lines)}}]


def _slack_expired_blocks(watch: dict) -> list[dict]:
    pub      = watch["publisher_name"]
    baseline = watch["baseline"]
    checks   = watch["checks"]

    # Summarise trajectory over the watch period
    if checks:
        first = checks[0]
        last  = checks[-1]
        trend = (
            f"Revenue trend: {first['rev_delta_pct']:+.1f}% → {last['rev_delta_pct']:+.1f}%  |  "
            f"eCPM trend: {first['ecpm_delta_pct']:+.1f}% → {last['ecpm_delta_pct']:+.1f}%"
        )
    else:
        trend = "No checks recorded."

    lines = [
        f":checkered_flag: *Pilot Watchdog — 7-Day Watch Complete*",
        f"Publisher: {pub}",
        f"Baseline: ${baseline['rev_daily_avg']:.2f}/day rev  |  eCPM ${baseline['ecpm_avg']:.3f}",
        f"",
        trend,
        f"",
        f"Floors remain in place. Manual review recommended.",
        f"{_now_et_str()}",
    ]

    return [{"type": "section", "text": {"type": "mrkdwn", "text": "\n".join(lines)}}]


# ---------------------------------------------------------------------------
# Main run function (called by scheduler daily at 09:30 ET)
# ---------------------------------------------------------------------------

def run():
    """Check all active watches. Called daily by the scheduler."""
    state = _load_state()
    now   = datetime.now(timezone.utc)

    active = [w for w in state["watches"] if w["status"] == "watching"]

    if not active:
        print("[watchdog] No active watches.")
        return

    print(f"[watchdog] Checking {len(active)} active watch(es)…")

    for watch in active:
        pub      = watch["publisher_name"]
        watch_id = watch["id"]

        # Check if expired
        expires = datetime.fromisoformat(watch["expires_at"])
        if now >= expires:
            watch["status"] = "expired"
            print(f"[watchdog] Watch {watch_id} ({pub}) expired — posting summary.")
            blocks = _slack_expired_blocks(watch)
            slack.send_blocks(blocks, text=f"Pilot watchdog complete: {pub}")
            continue

        # Run the check (Start.IO watches get the augmented check)
        kind = watch.get("kind", "standard")
        print(f"[watchdog] Checking {watch_id} — {pub} (kind={kind})…")
        if kind == "startio_activation":
            check = _check_startio_watch(watch)
        else:
            check = _check_watch(watch)
        watch["checks"].append(check)

        print(
            f"[watchdog] {pub}  rev={check['rev']:.2f} ({check['rev_delta_pct']:+.1f}%)  "
            f"eCPM={check['ecpm']:.3f} ({check['ecpm_delta_pct']:+.1f}%)  "
            f"action={check['action']}"
        )

        if check["action"] == "revert":
            # Route to the right revert mechanism based on watch kind
            if kind == "startio_activation":
                revert_results = _revert_startio_watch(watch)
            elif kind == "lurl_enable":
                revert_results = _revert_lurl_watch(watch)
            else:
                revert_results = _revert_watch(watch)
            watch["status"]         = "reverted"
            watch["reverted_at"]    = _now_iso()
            watch["revert_results"] = revert_results
            # Log the revert
            for r in revert_results:
                if r.get("applied"):
                    log_action(r)

        # Post Slack update
        blocks     = _slack_check_blocks(watch, check)
        slack_text = (
            f"Pilot watchdog {'REVERT' if check['action'] == 'revert' else 'check'}: {pub}"
        )
        slack.send_blocks(blocks, text=slack_text)

    _save_state(state)
    print("[watchdog] Done.")


# ---------------------------------------------------------------------------
# __main__ — status dump
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1 and sys.argv[1] == "status":
        state = _load_state()
        watches = state.get("watches", [])
        if not watches:
            print("No watches registered.")
        for w in watches:
            print(
                f"[{w['id']}] {w['publisher_name']:<40} "
                f"status={w['status']:<10} "
                f"applied={w['applied_at'][:10]}  "
                f"expires={w['expires_at'][:10]}  "
                f"checks={len(w['checks'])}"
            )
    else:
        run()
