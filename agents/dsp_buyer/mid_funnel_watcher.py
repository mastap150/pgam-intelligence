"""
agents/dsp_buyer/mid_funnel_watcher.py
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Intra-day watcher for the Mid Funnel campaign (and any other campaign
flagged for close watch). Runs every 30 min via scheduler.py.

Why this exists separately from the daily 9am ET digest:
  - Daily digest reads from the Neon mirror, which has known SS /report
    intermittency (Mid Funnel showing 847K when reality is 1.95M, etc.).
  - This watcher pulls LIVE from SS each tick — no mirror dependency.
  - 30-min cadence catches material shifts within the day rather than
    24h after the fact.
  - Targets specific campaigns the operator wants close visibility on
    (whitelist below). Different from the burn_rate_watchdog which is
    triggered by lever-applied state, not by campaign identity.

Alerts fired (each with 4h dedup in campaign_watcher_alerts):
  1. vtr_below_floor    — today's VTR < VTR_FLOOR_PCT for >1K imps
  2. vtr_auto_brake     — paired action with vtr_below_floor when imps
                          ≥ VTR_BRAKE_MIN_IMPS: steps SS freq cap down by
                          VTR_BRAKE_STEP_PCT (default 25%), floored at
                          VTR_BRAKE_MIN_CAP. Logs to buyer_agent_actions.
                          Disable with WATCHER_VTR_AUTO_BRAKE=0.
  3. delivery_cliff      — today's imps < CLIFF_PCT * yesterday's imps
                          (after ≥6h into UTC day to avoid noise)
  4. daily_rate_breach   — yesterday's delivered imps < DAILY_RATE_BREACH_PCT
                          of (remaining_goal / days_left). Catches the
                          front-loaded-but-collapsing failure mode where
                          cumulative pacing still looks healthy.
  5. pacing_shift_high   — cumulative pacing > 150% (over-burn signal)
  6. pacing_shift_low    — cumulative pacing < 60% (under-pace signal)
  7. budget_burnout      — at current daily spend rate, remaining
                          budget exhausts in <BURNOUT_DAYS days

Impression goal is OPERATOR-CANONICAL: ss_campaigns.budget_total (gross
USD IO commitment) / ss_campaigns.gross_rate_cpm_usd × 1000. Mid Funnel
= $38,500 / $7 × 1000 = 5,500,000 imps. This matches what the daily
digest now shows after the 2026-06-22 fix. The prior watcher math used
SS lifetime budget / hardcoded $1.95 → 5.87M, which never matched the
digest and caused phantom under-pace silence even as delivery collapsed.

Each alert posts a Slack message AND records in campaign_watcher_alerts
so a re-fire within 4h is suppressed.

Watchlist (operator-curated):
  - clearline-2378315 (Amazon Mid Funnel) — explicit operator request 2026-06-11
"""

from __future__ import annotations

import os
import json
from datetime import datetime, date, timedelta, timezone
from typing import Any, Optional

import requests
from dotenv import load_dotenv
from psycopg.types.json import Json

from core.dsp_neon import connect as dsp_connect
from core.slack import send_text

load_dotenv(override=True)

# ── Watchlist + thresholds ─────────────────────────────────────────────────

WATCHLIST = os.environ.get(
    "WATCHER_CAMPAIGN_IDS",
    "clearline-2378315",  # Mid Funnel
).split(",")

VTR_FLOOR_PCT = float(os.environ.get("WATCHER_VTR_FLOOR_PCT", "70"))
CLIFF_PCT = float(os.environ.get("WATCHER_CLIFF_PCT", "50"))
CLIFF_MIN_HOURS_INTO_DAY = float(os.environ.get("WATCHER_CLIFF_MIN_HOURS", "6"))
PACING_OVER_PCT = float(os.environ.get("WATCHER_PACING_OVER_PCT", "150"))
PACING_UNDER_PCT = float(os.environ.get("WATCHER_PACING_UNDER_PCT", "60"))
BURNOUT_DAYS = float(os.environ.get("WATCHER_BURNOUT_DAYS", "7"))
DEDUP_HOURS = int(os.environ.get("WATCHER_DEDUP_HOURS", "4"))

# Daily-rate breach: trips when yesterday's full-day delivery was under
# DAILY_RATE_BREACH_PCT × the required-to-finish daily rate. Catches the
# Mid Funnel failure mode (cumulative pacing looks fine after a front
# load, but per-day delivery has collapsed) which the cumulative-pacing
# under/over thresholds miss entirely.
DAILY_RATE_BREACH_PCT = float(os.environ.get("WATCHER_DAILY_RATE_BREACH_PCT", "70"))
DAILY_RATE_MIN_HOURS_INTO_DAY = float(os.environ.get("WATCHER_DAILY_RATE_MIN_HOURS", "12"))
# Don't trip the breach until the campaign has substantial cumulative
# history — a fresh campaign on day 1 with cum=0 shouldn't fire. Default
# 100K imps marks "this campaign has actually been running and a
# zero-delivery day is anomalous, not initial conditions".
DAILY_RATE_MIN_CUM = int(os.environ.get("WATCHER_DAILY_RATE_MIN_CUM", "100000"))

# VTR auto-brake: when today's VTR drops below the floor on enough imps,
# automatically step the SS demand-tag freq cap DOWN by a fixed ratio
# (default 25%) to suppress re-exposure to low-VTR HHs. Floored at
# VTR_BRAKE_MIN_CAP so we never freeze delivery entirely. Set
# WATCHER_VTR_AUTO_BRAKE=0 to disable and keep alert-only behaviour.
VTR_AUTO_BRAKE = os.environ.get("WATCHER_VTR_AUTO_BRAKE", "1") == "1"
VTR_BRAKE_STEP_PCT = float(os.environ.get("WATCHER_VTR_BRAKE_STEP_PCT", "25"))
VTR_BRAKE_MIN_CAP = int(os.environ.get("WATCHER_VTR_BRAKE_MIN_CAP", "5"))
VTR_BRAKE_MIN_IMPS = int(os.environ.get("WATCHER_VTR_BRAKE_MIN_IMPS", "5000"))


def mid_funnel_watcher() -> dict[str, Any]:
    """Entry point invoked every 30 min by scheduler.py."""
    started_at = datetime.now(timezone.utc)
    out: dict[str, Any] = {
        "started_at": started_at.isoformat(),
        "campaigns_checked": 0,
        "alerts_fired": 0,
        "alerts_deduped": 0,
        "errors": 0,
        "results": [],
    }

    headers = _ss_auth()
    if not headers:
        out["errors"] += 1
        out["results"].append({"error": "SS credentials not configured"})
        return out

    conn = dsp_connect()
    try:
        for campaign_id in WATCHLIST:
            campaign_id = campaign_id.strip()
            if not campaign_id:
                continue
            try:
                result = _check_one(conn, headers, campaign_id)
                out["campaigns_checked"] += 1
                out["alerts_fired"] += result.get("fired", 0)
                out["alerts_deduped"] += result.get("deduped", 0)
                out["results"].append(result)
            except Exception as e:
                out["errors"] += 1
                out["results"].append({"campaign_id": campaign_id, "error": str(e)[:200]})
    finally:
        conn.close()

    print(f"[mid_funnel_watcher] checked={out['campaigns_checked']} "
          f"fired={out['alerts_fired']} deduped={out['alerts_deduped']}")
    return out


# ── Per-campaign check ─────────────────────────────────────────────────────


def _check_one(conn, headers, campaign_id: str) -> dict[str, Any]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT id, name, springserve_demand_tag_ids[1] AS tag_id,
                   start_date, end_date, gross_rate_cpm_usd,
                   budget_total
              FROM ss_campaigns WHERE id = %s AND status = 'active'
            """, (campaign_id,)
        )
        row = cur.fetchone()
    if not row:
        return {"campaign_id": campaign_id, "skipped": "not active"}
    cid, name, tag_id, start_date, end_date, gross_cpm, budget_total = row
    if not tag_id:
        return {"campaign_id": campaign_id, "skipped": "no tag_id"}

    # Pull SS live state
    base = os.environ["SPRINGSERVE_BASE_URL"].rstrip("/")
    tag_res = requests.get(f"{base}/demand_tags/{int(tag_id)}", headers=headers, timeout=15)
    if not tag_res.ok:
        return {"campaign_id": campaign_id, "error": f"GET tag {tag_res.status_code}"}
    tag = tag_res.json()
    lifetime_budget = None
    for b in (tag.get("budgets") or []):
        if b.get("budget_period") == "lifetime" and b.get("budget_metric") == "gross_cost":
            lifetime_budget = float(b.get("budget_value") or 0)
            break

    # Pull cumulative + today + yesterday
    now = datetime.now(timezone.utc)
    hours_in = now.hour + now.minute / 60.0
    today_iso = now.date().isoformat()
    yesterday_iso = (now.date() - timedelta(days=1)).isoformat()
    start_iso = start_date.isoformat() if hasattr(start_date, "isoformat") else str(start_date)

    cumulative = _fetch_report(headers, int(tag_id), start_iso, today_iso)
    today = _fetch_report(headers, int(tag_id), today_iso, today_iso)
    yesterday = _fetch_report(headers, int(tag_id), yesterday_iso, yesterday_iso)

    cum_imps = cumulative.get("impressions", 0)
    cum_spent = cumulative.get("billable_cost", 0.0)
    today_imps = today.get("impressions", 0)
    today_spent = today.get("billable_cost", 0.0)
    today_vtr = (today.get("fourth_quartile", 0) / today_imps * 100) if today_imps else None
    yest_imps = yesterday.get("impressions", 0)
    yest_spent = yesterday.get("billable_cost", 0.0)

    # Pacing math
    if isinstance(end_date, date):
        end_dt = datetime(end_date.year, end_date.month, end_date.day, 23, 59, 59, tzinfo=timezone.utc)
    else:
        end_dt = datetime.fromisoformat(str(end_date).replace("Z", "+00:00"))
    days_left = max(0.1, (end_dt - now).total_seconds() / 86400)
    if isinstance(start_date, date):
        start_dt = datetime(start_date.year, start_date.month, start_date.day, tzinfo=timezone.utc)
    else:
        start_dt = datetime.fromisoformat(str(start_date).replace("Z", "+00:00"))
    days_elapsed = (now - start_dt).total_seconds() / 86400
    days_total = max(0.1, days_elapsed + days_left)

    # Impression goal — OPERATOR-CANONICAL ONLY.
    # The IO commitment is stored in ss_campaigns.budget_total (gross USD)
    # and ss_campaigns.gross_rate_cpm_usd (gross CPM). For Mid Funnel that's
    # $38,500 / $7 × 1000 = 5,500,000 imps — the number the digest now
    # shows correctly after PR #303/#304/#306/#309/#312.
    #
    # The PRIOR pacing math here divided the SS-side lifetime budget
    # ($11,453) by a hardcoded $1.95 — that's the SS-rate path the digest
    # was already corrected away from. It produced 5.87M (or 6.47M at the
    # real $1.77 SS rate), neither of which matches the operator IO, so
    # pacing_pct never matched the digest and the under-pace threshold
    # never fired even when delivery collapsed to <10% of required rate.
    #
    # Fall back to SS lifetime / SS rate ONLY if the operator hasn't
    # populated budget_total yet (e.g. early in a campaign's lifecycle) so
    # the watcher is still useful on fresh campaigns. The fallback uses
    # the LIVE SS tag rate (from `tag["rate"]`), never a hardcoded value.
    impression_goal = 0
    goal_source = "none"
    if budget_total and gross_cpm and float(gross_cpm) > 0:
        impression_goal = int(float(budget_total) * 1000 / float(gross_cpm))
        goal_source = "operator_canonical"
    elif lifetime_budget:
        ss_rate = float(tag.get("rate") or 0)
        if ss_rate > 0:
            impression_goal = int(lifetime_budget / ss_rate * 1000)
            goal_source = "ss_lifetime_fallback"

    pacing_expected = impression_goal * (days_elapsed / days_total) if days_total > 0 else 0
    pacing_pct = (cum_imps / pacing_expected * 100) if pacing_expected > 0 else None

    # Budget burnout (use yesterday spend as the rate proxy)
    remaining_budget = (lifetime_budget or 0) - cum_spent
    burnout_days = (remaining_budget / yest_spent) if yest_spent > 0 else float("inf")

    # ── Alert checks ───────────────────────────────────────────────────────
    alerts = []

    if today_vtr is not None and today_imps >= 1000 and today_vtr < VTR_FLOOR_PCT:
        alerts.append({
            "type": "vtr_below_floor",
            "severity": "warning",
            "fingerprint": f"vtr:{today_vtr:.1f}:floor:{VTR_FLOOR_PCT}",
            "message": (
                f"⚠️ *{name}* — VTR today is *{today_vtr:.1f}%*, "
                f"below {VTR_FLOOR_PCT:.0f}% floor on {today_imps:,} imps."
            ),
            "snapshot": {"today_imps": today_imps, "today_vtr_pct": today_vtr,
                         "floor_pct": VTR_FLOOR_PCT},
        })

        # Auto-brake: step the freq cap DOWN to suppress repeat exposures
        # to low-VTR HHs. Only fires when we have enough volume to trust
        # the VTR signal (≥VTR_BRAKE_MIN_IMPS today) and the current cap
        # is above the floor. Dedup via the same vtr_below_floor 4h window
        # — won't re-fire the brake on the same alert.
        if (VTR_AUTO_BRAKE
                and today_imps >= VTR_BRAKE_MIN_IMPS
                and not _was_recently_fired(conn, campaign_id, "vtr_auto_brake", DEDUP_HOURS)):
            brake = _auto_tighten_freq_cap(headers, int(tag_id), tag, campaign_id, name,
                                            today_vtr=today_vtr, today_imps=today_imps,
                                            conn=conn)
            if brake is not None:
                alerts.append(brake)

    # Cliff: compare today's HOURLY rate vs yesterday's hourly rate (rather
    # than today's partial total vs yesterday's full-day total — that always
    # fires before mid-day). Requires ≥6h into UTC day for stability.
    if hours_in >= CLIFF_MIN_HOURS_INTO_DAY and yest_imps > 5000:
        today_hourly = today_imps / max(hours_in, 0.1)
        yest_hourly = yest_imps / 24.0
        if today_hourly < yest_hourly * (1 - CLIFF_PCT / 100):
            drop_pct = (today_hourly / yest_hourly - 1) * 100 if yest_hourly else 0
            alerts.append({
                "type": "delivery_cliff",
                "severity": "critical",
                "fingerprint": f"cliff:{int(today_hourly)}:vs:{int(yest_hourly)}",
                "message": (
                    f"🚨 *{name}* — hourly delivery cliff: today running "
                    f"{int(today_hourly):,}/hr vs yesterday {int(yest_hourly):,}/hr "
                    f"({drop_pct:+.0f}%)."
                ),
                "snapshot": {"today_hourly": round(today_hourly, 0),
                             "yesterday_hourly": round(yest_hourly, 0),
                             "today_imps_partial": today_imps,
                             "hours_in": round(hours_in, 1)},
            })

    # Required daily rate to hit the canonical goal — useful in both
    # over-pace and under-pace messages so the operator sees the actual
    # number to steer toward.
    imps_remaining = max(0, impression_goal - cum_imps)
    daily_need = int(imps_remaining / days_left) if days_left > 0 else 0

    if pacing_pct is not None and pacing_pct > PACING_OVER_PCT:
        alerts.append({
            "type": "pacing_shift_high",
            "severity": "warning",
            "fingerprint": f"over:{int(pacing_pct/10)*10}",  # bucketed to 10s
            "message": (
                f"📈 *{name}* — over-pacing *{pacing_pct:.0f}%* "
                f"({cum_imps:,} / goal {impression_goal:,} vs expected by today {int(pacing_expected):,}). "
                f"Source: {goal_source}."
            ),
            "snapshot": {"pacing_pct": pacing_pct, "cum_imps": cum_imps,
                         "expected": int(pacing_expected),
                         "impression_goal": impression_goal,
                         "goal_source": goal_source},
        })

    # Daily-rate breach — the alarm that catches the actual failure mode
    # for front-loaded campaigns: cumulative pacing looks fine (>60%) but
    # recent daily delivery is in catastrophic free-fall. Compares
    # yesterday's full-day delivery (stable, not partial) to the daily
    # rate needed-from-here to hit the goal.
    #
    # Guard requires `cum_imps > DAILY_RATE_MIN_CUM` so a fresh campaign
    # on day 1 doesn't trip immediately. The previous guard required
    # `yest_imps > 0` which silently skipped the alert when yesterday
    # delivered ZERO — the absolute worst case (campaign auto-paused,
    # broken creative, deal pool collapsed). Mid Funnel on 2026-06-27
    # had yest_imps=0 from the prior day's auto-pause and the watcher
    # said "no alerts" while delivery was completely broken. Fix: drop
    # the > 0 floor on yest, anchor instead on substantial cum history.
    if (daily_need > 0
            and cum_imps > DAILY_RATE_MIN_CUM
            and hours_in >= DAILY_RATE_MIN_HOURS_INTO_DAY  # wait until yesterday is settled
            and yest_imps < daily_need * (DAILY_RATE_BREACH_PCT / 100)):
        rate_pct = (yest_imps / daily_need) * 100
        # Emoji + severity escalates when yesterday was literally zero —
        # that's the most-critical breach and the message should say so.
        is_zero = yest_imps == 0
        emoji = "🛑" if is_zero else "🚨"
        prefix = "ZERO-DELIVERY DAY" if is_zero else "daily-rate breach"
        alerts.append({
            "type": "daily_rate_breach",
            "severity": "critical",
            "fingerprint": f"rate:{'zero' if is_zero else int(rate_pct/10)*10}",
            "message": (
                f"{emoji} *{name}* — {prefix}: yesterday delivered "
                f"*{yest_imps:,}* but needed *{daily_need:,}/day* to hit "
                f"{impression_goal:,} by end-of-flight ({rate_pct:.0f}% of required). "
                f"Cumulative pacing {pacing_pct:.0f}% hides this — front-load is masking "
                f"the daily collapse. Source: {goal_source}."
            ),
            "snapshot": {"yesterday_imps": yest_imps, "daily_need": daily_need,
                         "rate_pct": round(rate_pct, 1),
                         "cum_imps": cum_imps,
                         "impression_goal": impression_goal,
                         "is_zero_delivery_day": is_zero,
                         "cumulative_pacing_pct": round(pacing_pct, 1) if pacing_pct else None,
                         "goal_source": goal_source},
        })

    if pacing_pct is not None and pacing_pct < PACING_UNDER_PCT:
        alerts.append({
            "type": "pacing_shift_low",
            "severity": "warning",
            "fingerprint": f"under:{int(pacing_pct/10)*10}",
            "message": (
                f"📉 *{name}* — under-pacing *{pacing_pct:.0f}%* "
                f"({cum_imps:,} / goal {impression_goal:,} vs expected by today {int(pacing_expected):,}). "
                f"Need *{daily_need:,}/day* for next {days_left:.1f}d. Source: {goal_source}."
            ),
            "snapshot": {"pacing_pct": pacing_pct, "cum_imps": cum_imps,
                         "expected": int(pacing_expected),
                         "impression_goal": impression_goal,
                         "daily_need": daily_need,
                         "days_left": round(days_left, 1),
                         "goal_source": goal_source},
        })

    if burnout_days < BURNOUT_DAYS and remaining_budget > 0:
        alerts.append({
            "type": "budget_burnout",
            "severity": "warning",
            "fingerprint": f"burnout:{int(burnout_days)}",
            "message": (
                f"💸 *{name}* — at yesterday's spend rate (${yest_spent:.2f}/day), "
                f"remaining ${remaining_budget:.2f} burns out in *{burnout_days:.1f} days* "
                f"(flight ends in {days_left:.1f}d)."
            ),
            "snapshot": {"remaining_budget": round(remaining_budget, 2),
                         "yesterday_spend": round(yest_spent, 2),
                         "burnout_days": round(burnout_days, 1),
                         "flight_days_left": round(days_left, 1)},
        })

    # ── Dedup + fire ───────────────────────────────────────────────────────
    fired = 0
    deduped = 0
    for alert in alerts:
        if _was_recently_fired(conn, campaign_id, alert["type"], DEDUP_HOURS):
            deduped += 1
            continue
        _record_alert(conn, campaign_id, alert)
        _post_slack(alert["message"])
        fired += 1

    return {
        "campaign_id": campaign_id,
        "name": name,
        "live_state": {
            "cum_imps": cum_imps,
            "today_imps": today_imps,
            "today_vtr_pct": round(today_vtr, 1) if today_vtr else None,
            "pacing_pct": round(pacing_pct, 1) if pacing_pct else None,
            "impression_goal": impression_goal,
            "goal_source": goal_source,
            "daily_need_for_remaining": daily_need,
            "days_left": round(days_left, 1),
            "remaining_budget": round(remaining_budget, 2) if lifetime_budget else None,
            "burnout_days": round(burnout_days, 1) if burnout_days != float("inf") else None,
        },
        "alerts_evaluated": len(alerts),
        "fired": fired,
        "deduped": deduped,
    }


# ── Dedup + record ─────────────────────────────────────────────────────────


def _was_recently_fired(conn, campaign_id: str, alert_type: str, hours: int) -> bool:
    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT 1 FROM campaign_watcher_alerts
             WHERE campaign_id = %s AND alert_type = %s
               AND fired_at > NOW() - INTERVAL '{hours} hours'
             LIMIT 1
            """, (campaign_id, alert_type),
        )
        return cur.fetchone() is not None


def _record_alert(conn, campaign_id: str, alert: dict[str, Any]) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO campaign_watcher_alerts
              (campaign_id, watcher, alert_type, severity,
               fingerprint, message, metric_snapshot, slack_posted)
            VALUES
              (%s, 'mid_funnel', %s, %s,
               %s, %s, %s::JSONB, TRUE)
            """,
            (campaign_id, alert["type"], alert["severity"],
             alert.get("fingerprint"), alert.get("message"),
             json.dumps(alert.get("snapshot", {}))),
        )
    conn.commit()


# ── SS helpers ─────────────────────────────────────────────────────────────


def _ss_auth() -> Optional[dict[str, str]]:
    base = os.environ.get("SPRINGSERVE_BASE_URL", "").rstrip("/")
    email = os.environ.get("SPRINGSERVE_EMAIL")
    password = os.environ.get("SPRINGSERVE_PASSWORD")
    if not (base and email and password):
        return None
    res = requests.post(f"{base}/auth", json={"email": email, "password": password}, timeout=15)
    if not res.ok:
        return None
    token = res.json().get("token")
    return {"Authorization": token, "Content-Type": "application/json"} if token else None


def _fetch_report(headers, tag_id: int, start_iso: str, end_iso: str) -> dict[str, float]:
    base = os.environ["SPRINGSERVE_BASE_URL"].rstrip("/")
    body = {"start_date": start_iso, "end_date": end_iso,
            "interval": "cumulative", "dimensions": ["demand_tag_id"]}
    try:
        res = requests.post(f"{base}/report", headers=headers, json=body, timeout=20)
    except Exception:
        return {}
    if not res.ok:
        return {}
    raw = res.json() if res.ok else []
    rows = raw if isinstance(raw, list) else raw.get("data", [])
    row = next((r for r in rows if int(r.get("demand_tag_id", -1)) == tag_id), None)
    if not row:
        return {}
    return {
        "impressions": int(row.get("impressions", 0)),
        "billable_cost": float(row.get("billable_cost", 0)),
        "fourth_quartile": float(row.get("fourth_quartile", 0)),
    }


def _post_slack(message: str) -> None:
    try:
        send_text(message)
    except Exception as e:
        print(f"[mid_funnel_watcher] slack post failed: {e}")


# ── VTR auto-brake ─────────────────────────────────────────────────────────


def _auto_tighten_freq_cap(headers, tag_id: int, tag: dict[str, Any],
                           campaign_id: str, name: str,
                           today_vtr: float, today_imps: int,
                           conn) -> Optional[dict[str, Any]]:
    """Step the SS demand tag's freq cap down by VTR_BRAKE_STEP_PCT.

    Returns an alert dict to surface the action in Slack, or None if the
    cap is already at the floor or the SS PATCH failed.
    """
    caps = tag.get("frequency_caps") or []
    # Find the daily cap entry. Watcher only operates on per-day caps —
    # hourly/lifetime caps would need different math.
    day_cap = next(
        (c for c in caps if c.get("frequency_cap_period") == "day"
                          and c.get("frequency_cap_period_amount") == 1
                          and c.get("frequency_cap_metric") == "impressions"),
        None,
    )
    if not day_cap:
        return None

    current = int(day_cap.get("frequency_cap_value") or 0)
    if current <= VTR_BRAKE_MIN_CAP:
        return {
            "type": "vtr_auto_brake",
            "severity": "warning",
            "fingerprint": f"brake:floored:{current}",
            "message": (
                f"🧰 *{name}* — VTR {today_vtr:.1f}% below {VTR_FLOOR_PCT:.0f}% floor "
                f"but freq cap already at min {current}/day. No auto-brake possible — "
                f"manual review needed (consider creative or publisher action)."
            ),
            "snapshot": {"current_cap": current, "today_vtr_pct": today_vtr,
                         "min_cap": VTR_BRAKE_MIN_CAP},
        }

    target = max(VTR_BRAKE_MIN_CAP, int(round(current * (1 - VTR_BRAKE_STEP_PCT / 100))))
    if target >= current:
        target = current - 1  # always step at least 1 down so the brake is meaningful

    # Build the full freq cap payload — SS PUT validates every required field
    # on the array element (per 2026-06-24 incident: missing
    # frequency_cap_metric / frequency_cap_period_amount → 400 with
    # cryptic "can't be blank" errors).
    new_cap_entry = {
        "frequency_cap_period": "day",
        "frequency_cap_period_amount": 1,
        "frequency_cap_metric": "impressions",
        "frequency_cap_value": target,
        "frequency_cap_type": day_cap.get("frequency_cap_type") or "springserve",
        "allow_empty_household_id": bool(day_cap.get("allow_empty_household_id", False)),
        "allow_empty_household_ids": bool(day_cap.get("allow_empty_household_ids", False)),
    }
    base = os.environ["SPRINGSERVE_BASE_URL"].rstrip("/")
    try:
        res = requests.put(
            f"{base}/demand_tags/{tag_id}",
            headers=headers,
            json={"frequency_caps": [new_cap_entry]},
            timeout=20,
        )
    except Exception as e:
        return {
            "type": "vtr_auto_brake",
            "severity": "critical",
            "fingerprint": f"brake:error:{current}->{target}",
            "message": (
                f"🚨 *{name}* — VTR auto-brake FAILED (network): "
                f"intended cap {current}/day → {target}/day. Error: {e}"
            ),
            "snapshot": {"intended_target": target, "current_cap": current,
                         "error": str(e)[:200]},
        }
    if not res.ok:
        return {
            "type": "vtr_auto_brake",
            "severity": "critical",
            "fingerprint": f"brake:error:{current}->{target}",
            "message": (
                f"🚨 *{name}* — VTR auto-brake FAILED (HTTP {res.status_code}): "
                f"intended cap {current}/day → {target}/day. "
                f"Body: {res.text[:200]}"
            ),
            "snapshot": {"intended_target": target, "current_cap": current,
                         "http_status": res.status_code},
        }

    # Log to the canonical buyer_agent_actions ledger so the auto-rollback
    # cron + retro generator both see it.
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO buyer_agent_actions (
                  campaign_id, advertiser_id, run_id, lever, action,
                  applied, dry_run, push_status, pushed_at, reason,
                  before_state, after_state
                )
                SELECT %s, advertiser_id, %s, 'freq_cap', 'tighten',
                       TRUE, FALSE, 'pushed', NOW(), %s,
                       %s::JSONB, %s::JSONB
                  FROM ss_campaigns WHERE id = %s
                """,
                (
                    campaign_id,
                    f"vtr_auto_brake:{datetime.now(timezone.utc).isoformat()}",
                    (f"VTR auto-brake: today's VTR {today_vtr:.1f}% on {today_imps:,} imps "
                     f"fell below {VTR_FLOOR_PCT:.0f}% floor. Stepped freq cap "
                     f"{current}/day → {target}/day ({VTR_BRAKE_STEP_PCT:.0f}% reduction) to "
                     f"suppress repeat exposure on low-VTR HHs."),
                    json.dumps({"frequency_caps": [{"cap_per_hh": current, "cap_window": "day"}]}),
                    json.dumps({"frequency_caps": [{"cap_per_hh": target, "cap_window": "day"}]}),
                    campaign_id,
                ),
            )
        conn.commit()
    except Exception as e:
        print(f"[mid_funnel_watcher] ledger write failed: {e}")

    return {
        "type": "vtr_auto_brake",
        "severity": "warning",
        "fingerprint": f"brake:{current}->{target}",
        "message": (
            f"🧰 *{name}* — VTR auto-brake fired: today's VTR {today_vtr:.1f}% on "
            f"{today_imps:,} imps below {VTR_FLOOR_PCT:.0f}% floor. "
            f"Freq cap stepped *{current}/day → {target}/day*. "
            f"Will continue stepping down on next VTR breach (floor {VTR_BRAKE_MIN_CAP}/day)."
        ),
        "snapshot": {"from_cap": current, "to_cap": target,
                     "today_vtr_pct": today_vtr, "today_imps": today_imps,
                     "floor_pct": VTR_FLOOR_PCT},
    }


if __name__ == "__main__":
    print(json.dumps(mid_funnel_watcher(), indent=2, default=str))
