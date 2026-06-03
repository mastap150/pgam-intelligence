"""
agents/dsp_buyer/burn_rate_watchdog.py
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Burn-rate auto-revert for `budget_pacing: front_loaded` apply rows.

Why this exists
---------------
The DSP buyer agent's existing 6h auto-rollback cron checks for
**delivery degradation** (CTR/VTR drops) on a 24h-vs-24h window — that's
the right signal for most levers, but for a `front_loaded` flip the
risk profile is different: the lever can succeed (delivery goes UP)
while still over-burning budget faster than the remaining flight can
absorb. Performance looks fine, then the campaign starves the last 2-3
days because the daily run-rate ate everything.

This watchdog closes that gap. It runs alongside margin_watchdog
(every 5 min), specifically targeting campaigns where budget_pacing
was flipped to `front_loaded` in the last 96h:

  1. For each active front_loaded apply row not yet rolled back:
  2. Pull today's media spend at SS via /report (UTC day).
  3. Compute remaining_budget = ss_tag_budget - lifetime_media_cost.
  4. safe_daily = remaining_budget / days_remaining.
  5. If today_spend > safe_daily × BURN_RATIO_THRESHOLD AND we're
     past BURN_MIN_HOURS_AFTER_APPLY hours since flip → revert.

Revert path
-----------
Direct SS PATCH (same flat shape margin_watchdog uses for pause):

  PUT /demand_tags/{tag_id}/budgets/{budget_id}
  { "budget_pacing": "smooth" }

Then:
  - Insert compensating ledger row (lever=budget_pacing, action=flip,
    before_state={pacing: front_loaded}, after_state={pacing: smooth},
    run_id=burn_rate_revert:{ledger_id}:{ts})
  - UPDATE original buyer_agent_actions row:
      rolled_back_at = NOW()
      rolled_back_reason = "burn rate {ratio:.1f}x safe-daily"
      rolled_back_by_id = compensating_row.id
  - Post Slack P3 alert.

Guards
------
- BURN_MIN_HOURS_AFTER_APPLY: don't react in the first hour — signal
  is too noisy.
- BURN_MIN_TODAY_SPEND: if today's spend is tiny in absolute terms
  ($10), don't trigger even if ratio looks bad.
- Days-remaining floor: if flight ends today, no point reverting.
- Idempotent: once rolled_back_at is set, the candidate query skips it.
"""

from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Any, Optional

import requests
from dotenv import load_dotenv
from psycopg.types.json import Json

from core.dsp_neon import connect as dsp_connect
from core.slack import send_text

load_dotenv(override=True)

# ── Tunables ───────────────────────────────────────────────────────────────
# When today's burn exceeds this multiple of the safe-daily, revert.
BURN_RATIO_THRESHOLD = float(os.environ.get("BURN_RATIO_THRESHOLD", "2.0"))
# Don't react in the first hour after a flip — signal too noisy.
BURN_MIN_HOURS_AFTER_APPLY = int(os.environ.get("BURN_MIN_HOURS_AFTER_APPLY", "1"))
# Only consider rows from the last N hours.
BURN_LOOKBACK_HOURS = int(os.environ.get("BURN_LOOKBACK_HOURS", "96"))
# Absolute floor on today's spend before we trigger (avoids tiny-number noise).
BURN_MIN_TODAY_SPEND_USD = float(os.environ.get("BURN_MIN_TODAY_SPEND_USD", "50.0"))


def burn_rate_watchdog() -> dict[str, Any]:
    """Entry point invoked by scheduler.py every 5 min."""
    started_at = datetime.now(timezone.utc)
    out: dict[str, Any] = {
        "started_at": started_at.isoformat(),
        "evaluated": 0,
        "reverted": 0,
        "errors": 0,
        "details": [],
    }

    conn = dsp_connect()
    try:
        candidates = _load_candidates(conn)
        out["evaluated"] = len(candidates)

        for row in candidates:
            try:
                outcome = _evaluate_one(conn, row)
                out["details"].append(outcome)
                if outcome.get("action") == "reverted":
                    out["reverted"] += 1
            except Exception as e:
                out["errors"] += 1
                out["details"].append({
                    "ledger_id": row["id"],
                    "campaign_id": row["campaign_id"],
                    "action": "error",
                    "detail": str(e)[:200],
                })
    finally:
        conn.close()

    print(f"[burn_rate_watchdog] evaluated={out['evaluated']} reverted={out['reverted']} errors={out['errors']}")
    return out


# ── Candidate query ────────────────────────────────────────────────────────


def _load_candidates(conn) -> list[dict[str, Any]]:
    """Active front_loaded flips not yet rolled back, within lookback window."""
    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT a.id, a.campaign_id, a.advertiser_id,
                   a.before_state, a.after_state, a.created_at,
                   c.start_date, c.end_date, c.ss_tag_mirror
              FROM buyer_agent_actions a
              JOIN ss_campaigns c ON c.id = a.campaign_id
             WHERE a.lever = 'budget_pacing'
               AND a.applied = TRUE
               AND a.rolled_back_at IS NULL
               AND a.after_state->>'budget_pacing' = 'front_loaded'
               AND a.created_at > NOW() - INTERVAL '{BURN_LOOKBACK_HOURS} hours'
               AND a.created_at < NOW() - INTERVAL '{BURN_MIN_HOURS_AFTER_APPLY} hours'
               AND c.status = 'active'
             ORDER BY a.created_at DESC
            """
        )
        cols = [d.name for d in cur.description]
        return [dict(zip(cols, r)) for r in cur.fetchall()]


# ── Per-candidate evaluation ───────────────────────────────────────────────


def _evaluate_one(conn, row: dict[str, Any]) -> dict[str, Any]:
    """Decide whether to revert this one apply row. Returns outcome dict."""
    ledger_id = row["id"]
    campaign_id = row["campaign_id"]
    mirror = row["ss_tag_mirror"] or {}
    start_date = row["start_date"]
    end_date = row["end_date"]

    # Days remaining in flight (>= 1 to avoid div-by-zero).
    today = datetime.now(timezone.utc).date()
    days_remaining = max(1, (end_date - today).days) if end_date else 0
    if days_remaining <= 0:
        return {"ledger_id": ledger_id, "campaign_id": campaign_id, "action": "kept",
                "detail": "flight ended"}

    tag_id = mirror.get("ss_tag_id")
    budget_id = mirror.get("tag_budget_id") or (row["after_state"] or {}).get("budget_id")
    total_budget = float(mirror.get("tag_budget_gross_cost") or 0)
    lifetime_imps = float(mirror.get("lifetime_impressions") or 0)
    if not tag_id or not budget_id or total_budget <= 0:
        return {"ledger_id": ledger_id, "campaign_id": campaign_id, "action": "skipped",
                "detail": "missing ss_tag_mirror data (tag_id/budget_id/total_budget)"}

    # Pull lifetime media cost directly from SS /report (start_date → today)
    # rather than estimating from mirror's imps × rate. The mirror's
    # lifetime_impressions field gets clobbered to 0 when SS /report hiccups
    # (see 2026-06-03 incident), which would inflate remaining_budget and
    # silence this watchdog. Always re-query SS for the authoritative number.
    if not start_date:
        return {"ledger_id": ledger_id, "campaign_id": campaign_id, "action": "skipped",
                "detail": "campaign missing start_date"}
    lifetime_spend = _fetch_lifetime_spend(int(tag_id), start_date.isoformat())
    if lifetime_spend is None:
        return {"ledger_id": ledger_id, "campaign_id": campaign_id, "action": "skipped",
                "detail": "SS /report lifetime-spend fetch failed"}
    remaining_budget = max(0.0, total_budget - lifetime_spend)
    safe_daily = remaining_budget / days_remaining

    # Today's actual SS spend (UTC day).
    today_spend = _fetch_today_spend(int(tag_id))
    if today_spend is None:
        return {"ledger_id": ledger_id, "campaign_id": campaign_id, "action": "skipped",
                "detail": "SS /report today-spend fetch failed"}

    ratio = (today_spend / safe_daily) if safe_daily > 0 else 0.0
    outcome_base = {
        "ledger_id": ledger_id,
        "campaign_id": campaign_id,
        "today_spend": round(today_spend, 2),
        "safe_daily": round(safe_daily, 2),
        "remaining_budget": round(remaining_budget, 2),
        "days_remaining": days_remaining,
        "ratio": round(ratio, 2),
    }

    if today_spend < BURN_MIN_TODAY_SPEND_USD:
        return {**outcome_base, "action": "kept",
                "detail": f"today's spend ${today_spend:.2f} < ${BURN_MIN_TODAY_SPEND_USD:.2f} floor"}

    if ratio < BURN_RATIO_THRESHOLD:
        return {**outcome_base, "action": "kept",
                "detail": f"ratio {ratio:.2f}x < {BURN_RATIO_THRESHOLD:.1f}x threshold"}

    # TRIGGER — revert to smooth.
    revert_result = _revert_to_smooth(int(tag_id), int(budget_id))
    if not revert_result["ok"]:
        return {**outcome_base, "action": "error",
                "detail": f"SS PUT failed: {revert_result['detail']}"}

    # Compensating ledger row + mark original as rolled_back.
    compensating_id = _write_compensating_ledger(conn, row, ratio, today_spend, safe_daily)
    return {**outcome_base, "action": "reverted",
            "detail": f"burn {ratio:.1f}x safe-daily — reverted to smooth, compensating ledger #{compensating_id}"}


# ── SS interaction ─────────────────────────────────────────────────────────


def _ss_auth() -> Optional[dict[str, str]]:
    base = os.environ.get("SPRINGSERVE_BASE_URL", "https://console.springserve.com/api/v0").rstrip("/")
    email = os.environ.get("SPRINGSERVE_EMAIL")
    password = os.environ.get("SPRINGSERVE_PASSWORD")
    if not (email and password):
        return None
    res = requests.post(f"{base}/auth", json={"email": email, "password": password}, timeout=15)
    if not res.ok:
        return None
    token = res.json().get("token")
    if not token:
        return None
    return {"Authorization": token, "Content-Type": "application/json"}


def _fetch_today_spend(tag_id: int) -> Optional[float]:
    """Pull today's UTC-day billable_cost for the given demand tag.
    Returns None on any failure (caller treats as 'skip', not 'no spend')."""
    headers = _ss_auth()
    if not headers:
        return None
    base = os.environ.get("SPRINGSERVE_BASE_URL", "https://console.springserve.com/api/v0").rstrip("/")
    today = datetime.now(timezone.utc).date().isoformat()
    body = {
        "start_date": today,
        "end_date": today,
        "interval": "cumulative",
        "dimensions": ["demand_tag_id"],
    }
    try:
        res = requests.post(f"{base}/report", headers=headers, json=body, timeout=20)
    except Exception:
        return None
    if not res.ok:
        return None
    try:
        raw = res.json()
    except Exception:
        return None
    rows = raw if isinstance(raw, list) else (raw.get("data") if isinstance(raw, dict) else None)
    if not isinstance(rows, list):
        return None
    matched = [r for r in rows if int(r.get("demand_tag_id", -1)) == tag_id]
    if not matched:
        return 0.0  # today actually has zero spend so far
    return float(matched[0].get("billable_cost", 0) or 0)


def _fetch_lifetime_spend(tag_id: int, start_ymd: str) -> Optional[float]:
    """Pull cumulative billable_cost from start_date → today for the demand tag.
    Used to compute remaining_budget without trusting the mirror's possibly-
    zeroed lifetime_impressions field."""
    headers = _ss_auth()
    if not headers:
        return None
    base = os.environ.get("SPRINGSERVE_BASE_URL", "https://console.springserve.com/api/v0").rstrip("/")
    today = datetime.now(timezone.utc).date().isoformat()
    body = {
        "start_date": start_ymd,
        "end_date": today,
        "interval": "cumulative",
        "dimensions": ["demand_tag_id"],
    }
    try:
        res = requests.post(f"{base}/report", headers=headers, json=body, timeout=20)
    except Exception:
        return None
    if not res.ok:
        return None
    try:
        raw = res.json()
    except Exception:
        return None
    rows = raw if isinstance(raw, list) else (raw.get("data") if isinstance(raw, dict) else None)
    if not isinstance(rows, list):
        return None
    matched = [r for r in rows if int(r.get("demand_tag_id", -1)) == tag_id]
    if not matched:
        # No rows matched. Could be (a) tag really had zero lifetime spend
        # (treat as None — let caller skip rather than over-estimate
        # remaining budget) or (b) SS report hiccup (same — skip).
        return None
    return float(matched[0].get("billable_cost", 0) or 0)


def _revert_to_smooth(tag_id: int, budget_id: int) -> dict[str, Any]:
    """PUT budget_pacing=smooth on the budget object. Returns {ok, detail}."""
    headers = _ss_auth()
    if not headers:
        return {"ok": False, "detail": "SS auth not configured"}
    base = os.environ.get("SPRINGSERVE_BASE_URL", "https://console.springserve.com/api/v0").rstrip("/")
    # First read the demand tag to get the full budgets array — SS expects
    # us to PUT the full budgets array even when changing one field.
    tag_res = requests.get(f"{base}/demand_tags/{tag_id}", headers=headers, timeout=15)
    if not tag_res.ok:
        return {"ok": False, "detail": f"GET tag HTTP {tag_res.status_code}"}
    tag = tag_res.json()
    budgets = tag.get("budgets") or []
    if not budgets:
        return {"ok": False, "detail": "tag has no budgets array"}
    updated = []
    found = False
    for b in budgets:
        if int(b.get("id", -1)) == budget_id:
            updated.append({**b, "budget_pacing": "smooth"})
            found = True
        else:
            updated.append(b)
    if not found:
        return {"ok": False, "detail": f"budget_id {budget_id} not in tag's budgets"}
    put_res = requests.put(
        f"{base}/demand_tags/{tag_id}",
        headers=headers,
        json={"budgets": updated},
        timeout=20,
    )
    if not put_res.ok:
        return {"ok": False, "detail": f"PUT HTTP {put_res.status_code}: {put_res.text[:200]}"}
    # Verify
    v_res = requests.get(f"{base}/demand_tags/{tag_id}", headers=headers, timeout=15)
    if v_res.ok:
        v_budgets = v_res.json().get("budgets") or []
        for b in v_budgets:
            if int(b.get("id", -1)) == budget_id:
                if b.get("budget_pacing") == "smooth":
                    return {"ok": True, "detail": "verified smooth"}
                return {"ok": False, "detail": f"verify shows {b.get('budget_pacing')!r}"}
    return {"ok": True, "detail": "PUT 200 (verify GET failed but assume success)"}


# ── Ledger + Slack ─────────────────────────────────────────────────────────


def _write_compensating_ledger(
    conn,
    original: dict[str, Any],
    ratio: float,
    today_spend: float,
    safe_daily: float,
) -> int:
    """Insert compensating ledger row, mark original rolled_back, post Slack."""
    rolled_back_reason = f"burn rate {ratio:.2f}x safe-daily (today ${today_spend:.2f} vs safe ${safe_daily:.2f})"
    run_id = f"burn_rate_revert:{original['id']}:{datetime.now(timezone.utc).isoformat()}"
    before_state = original["after_state"] or {}  # was front_loaded
    after_state = original["before_state"] or {}  # back to smooth
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO buyer_agent_actions
              (campaign_id, advertiser_id, lever, action, reason,
               before_state, after_state, dry_run, applied, push_status,
               verify_ok, run_id)
            VALUES
              (%s, %s, 'budget_pacing', 'flip', %s,
               %s::JSONB, %s::JSONB, FALSE, TRUE, 'pushed',
               TRUE, %s)
            RETURNING id
            """,
            (
                original["campaign_id"], original["advertiser_id"],
                f"auto-revert: {rolled_back_reason}",
                Json(before_state), Json(after_state),
                run_id,
            ),
        )
        compensating_id = cur.fetchone()[0]
        cur.execute(
            """
            UPDATE buyer_agent_actions
               SET rolled_back_at = NOW(),
                   rolled_back_reason = %s,
                   rolled_back_by_id = %s,
                   push_status = 'rolled_back'
             WHERE id = %s
            """,
            (rolled_back_reason, compensating_id, original["id"]),
        )
    conn.commit()
    _post_slack(
        f":rotating_light: *Burn-rate auto-revert* — {original['campaign_id']}\n"
        f"• Today's spend ${today_spend:.2f} = {ratio:.1f}× safe-daily ${safe_daily:.2f}\n"
        f"• Reverted budget_pacing front_loaded → smooth\n"
        f"• Original apply: ledger #{original['id']} · compensating: #{compensating_id}"
    )
    return compensating_id


def _post_slack(text: str) -> None:
    try:
        send_text(text)
    except Exception as e:
        print(f"[burn_rate_watchdog] slack post failed: {e}")


if __name__ == "__main__":
    import json
    print(json.dumps(burn_rate_watchdog(), indent=2, default=str))
