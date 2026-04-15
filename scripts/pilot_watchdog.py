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

    if action == "revert":
        icon  = ":rotating_light:"
        title = "Pilot Watchdog — Auto-Revert Triggered"
    elif action == "no_data":
        icon  = ":warning:"
        title = "Pilot Watchdog — No Data"
    else:
        icon  = ":white_check_mark:"
        title = "Pilot Watchdog — Floor Stable"

    lines = [
        f"{icon} *{title}*",
        f"Publisher: {pub}",
        f"Date checked: {day}",
        f"",
        f"*Yesterday vs 7-day baseline:*",
        f"Revenue:  ${check['rev']:.2f}  vs  ${baseline['rev_daily_avg']:.2f}/day  ({check['rev_delta_pct']:+.1f}%)",
        f"eCPM:     ${check['ecpm']:.3f}  vs  ${baseline['ecpm_avg']:.3f}  ({check['ecpm_delta_pct']:+.1f}%)",
        f"Wins:     {check['wins']:,}",
        f"",
        f"_{check['note']}_",
        f"",
        f"Watch expires: {watch['expires_at'][:10]}  |  {_now_et_str()}",
    ]

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

        # Run the check
        print(f"[watchdog] Checking {watch_id} — {pub}…")
        check = _check_watch(watch)
        watch["checks"].append(check)

        print(
            f"[watchdog] {pub}  rev={check['rev']:.2f} ({check['rev_delta_pct']:+.1f}%)  "
            f"eCPM={check['ecpm']:.3f} ({check['ecpm_delta_pct']:+.1f}%)  "
            f"action={check['action']}"
        )

        if check["action"] == "revert":
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
