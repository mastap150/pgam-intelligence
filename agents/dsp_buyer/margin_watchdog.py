"""
agents/dsp_buyer/margin_watchdog.py
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Python port of the DSP margin watchdog. Replaces the broken Vercel
deployment of /api/v1/cpa/margin-watchdog, which on 2026-06-01 was
returning `total_cost=0` for all campaigns and silently failing to
write rows to `ss_campaign_margin_events` despite the same code path
working locally with `tsx`.

Logic mirrors `src/lib/cpa.ts` in pgam-dsp-dashboard:

  For each active campaign (status='active'):
    revenue   = qualified_calls × cpa_rate          (CPA)
                impressions × gross_rate_cpm_usd/1000  (CPM)
    fees      = platform_fee + bandwidth + tech + data + agency
    total_cost = media_cost + fees
    margin    = (revenue - total_cost) / revenue × 100

  Decide action:
    - total_cost < min_spend_to_pause → "ok" (not yet evaluable)
    - margin_pct < margin_floor_pct   → "would_pause_dry_run" or "paused"
    - margin_pct ≤ floor + 2pp        → "approaching_floor"
    - else                            → "ok"

  Side effects (only when action is consequential):
    - INSERT row into ss_campaign_margin_events (always)
    - UPDATE ss_campaigns.last_margin_* snapshot (always)
    - If enforce && paused: HTTP POST to SS to pause the demand tag
    - Slack post on paused / would_pause / approaching_floor (rate-limited)

Enforce mode: DSP_WATCHDOG_ENFORCE=1 env var. Off by default — same
semantic as the dashboard's CPA_WATCHDOG_ENFORCE.
"""

from __future__ import annotations

import os
import time
from datetime import datetime, timezone
from typing import Any

import psycopg
import requests
from dotenv import load_dotenv
from psycopg.types.json import Json

from core.dsp_neon import connect as dsp_connect
from core.slack import send_text

load_dotenv(override=True)

# ── Fee config ────────────────────────────────────────────────────────────
# Mirrors src/lib/margin.ts DEFAULT_INTERNAL_FEES + DEFAULT_INTERNAL_FEES_CPA.
# When dashboard ships its fee_config_advertiser table we'll read from there.
MAGNITE_PLATFORM_FEE_PCT = float(os.environ.get("MAGNITE_PLATFORM_FEE_PCT", "0.10"))
PUBMATIC_PLATFORM_FEE_PCT = float(os.environ.get("PUBMATIC_PLATFORM_FEE_PCT", "0.07"))

# CPM defaults (rate card has markup baked in)
CPM_BANDWIDTH_PCT = 0.0025
CPM_TECH_PCT = 0.085
CPM_DATA_CPM = 0.0
CPM_AGENCY_PCT = 0.0

# CPA defaults (PGAM bears media cost, only real infra costs)
CPA_BANDWIDTH_PCT = 0.0025
CPA_TECH_PCT = 0.005
CPA_DATA_CPM = 0.0
CPA_AGENCY_PCT = 0.0

# Warn within this many pp of floor.
APPROACHING_FLOOR_PP = 2.0

# Default min duration for a "qualified call" when payout_config doesn't set it.
DEFAULT_QUALIFIED_CALL_DURATION_SECONDS = 30


def _enforce_mode() -> bool:
    """Match dashboard's CPA_WATCHDOG_ENFORCE semantic but namespaced for the Python tick."""
    return os.environ.get("DSP_WATCHDOG_ENFORCE", os.environ.get("CPA_WATCHDOG_ENFORCE", "0")) == "1"


# ── Per-campaign metrics ─────────────────────────────────────────────────


def _platform_fee_pct(supply_platform: str) -> float:
    if supply_platform == "magnite":
        return MAGNITE_PLATFORM_FEE_PCT
    if supply_platform == "pubmatic":
        return PUBMATIC_PLATFORM_FEE_PCT
    return 0.0


def _compute_margin(camp: dict[str, Any]) -> dict[str, Any]:
    """Return total_cost, net_revenue, margin_pct + breakdown."""
    is_cpm = camp["commercial_model"] == "cpm"
    media_cost = float(camp["media_cost"] or 0)
    impressions = int(camp["impressions"] or 0)

    # Revenue
    if is_cpm:
        gross_rate = float(camp["gross_rate_cpm_usd"] or 0)
        net_revenue = (impressions * gross_rate) / 1000.0
    else:
        net_revenue = float(camp["_qualified_calls"] or 0) * float(camp["cpa_rate"] or 0)

    # Fees
    platform_fee = media_cost * _platform_fee_pct(camp["supply_platform"])
    if is_cpm:
        bandwidth_pct, tech_pct, data_cpm, agency_pct = (
            CPM_BANDWIDTH_PCT, CPM_TECH_PCT, CPM_DATA_CPM, CPM_AGENCY_PCT,
        )
    else:
        bandwidth_pct, tech_pct, data_cpm, agency_pct = (
            CPA_BANDWIDTH_PCT, CPA_TECH_PCT, CPA_DATA_CPM, CPA_AGENCY_PCT,
        )
    bandwidth_fee = media_cost * bandwidth_pct
    tech_fee = media_cost * tech_pct
    data_fee = (data_cpm * impressions) / 1000.0
    agency_fee = media_cost * agency_pct
    internal_fees = bandwidth_fee + tech_fee + data_fee + agency_fee

    total_cost = media_cost + platform_fee + internal_fees
    net_margin = net_revenue - total_cost
    margin_pct = (net_margin / net_revenue * 100.0) if net_revenue > 0 else 0.0

    return {
        "gross_media_spend": media_cost,
        "platform_fee_amount": platform_fee,
        "internal_fee_amount": internal_fees,
        "total_cost": total_cost,
        "net_revenue": net_revenue,
        "net_margin": net_margin,
        "margin_pct": margin_pct,
    }


def _decide(metrics: dict[str, Any], camp: dict[str, Any], enforce: bool) -> dict[str, str]:
    """Pure decision function — mirrors decide() in cpa.ts."""
    if camp.get("margin_watchdog_paused_manually"):
        return {"action": "skipped_manual", "detail": "margin_watchdog_paused_manually=TRUE"}
    floor = camp.get("margin_floor_pct")
    min_spend = camp.get("min_spend_to_pause")
    if floor is None or min_spend is None:
        return {"action": "error", "detail": f"missing margin_floor_pct or min_spend_to_pause on {camp['id']}"}
    floor = float(floor)
    min_spend = float(min_spend)
    total_cost = metrics["total_cost"]
    if total_cost < min_spend:
        return {
            "action": "ok",
            "detail": f"total_cost {total_cost:.2f} < min_spend_to_pause {min_spend:.2f} — not yet evaluable",
        }
    if metrics["margin_pct"] < floor:
        verb = "paused" if enforce else "would_pause_dry_run"
        return {
            "action": verb,
            "detail": f"margin_pct {metrics['margin_pct']:.2f}% < floor {floor:.2f}% after total_cost {total_cost:.2f}",
        }
    if metrics["margin_pct"] - floor < APPROACHING_FLOOR_PP:
        return {
            "action": "approaching_floor",
            "detail": f"margin_pct {metrics['margin_pct']:.2f}% approaching floor {floor:.2f}%",
        }
    return {"action": "ok", "detail": f"margin_pct {metrics['margin_pct']:.2f}% above floor {floor:.2f}%"}


# ── DB ─────────────────────────────────────────────────────────────────


def _load_active_campaigns(conn: psycopg.Connection) -> list[dict[str, Any]]:
    """Read every active campaign with the columns the watchdog needs."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT
              id, advertiser_id, supply_platform,
              springserve_campaign_id, springserve_demand_tag_id,
              commercial_model, status,
              cpa_rate, gross_rate_cpm_usd,
              margin_floor_pct, min_spend_to_pause, hourly_spend_cap,
              media_cost, impressions, budget_spent,
              margin_watchdog_paused_manually,
              ss_tag_mirror
            FROM ss_campaigns
            WHERE status = 'active'
        """)
        cols = [d.name for d in cur.description]
        rows = cur.fetchall()
    return [dict(zip(cols, r)) for r in rows]


def _count_qualified_calls(conn: psycopg.Connection, campaign_id: str) -> int:
    """Lifetime count of qualified calls — used for CPA revenue."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT COUNT(*)::int FROM ctv_calls
             WHERE campaign_id = %s
               AND qualified = TRUE
               AND COALESCE(call_duration, 0) >= %s
            """,
            (campaign_id, DEFAULT_QUALIFIED_CALL_DURATION_SECONDS),
        )
        row = cur.fetchone()
    return int(row[0]) if row else 0


def _record_margin_event(
    conn: psycopg.Connection,
    camp: dict[str, Any],
    metrics: dict[str, Any],
    decision: dict[str, str],
    enforce: bool,
) -> int:
    """Insert an audit row + update last_margin_* snapshot. Returns event id."""
    is_cpm = camp["commercial_model"] == "cpm"
    cpa_rate_for_row = None if is_cpm else float(camp.get("cpa_rate") or 0)
    qualified_calls = camp.get("_qualified_calls") or 0
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO ss_campaign_margin_events (
                campaign_id, advertiser_id,
                commercial_model,
                gross_media_spend, platform_fee_amount, internal_fee_amount, total_cost,
                qualified_calls, cpa_rate,
                impressions, gross_rate_cpm_usd,
                net_revenue, net_margin, margin_pct,
                margin_floor_pct, min_spend_to_pause,
                action, action_detail, enforce_mode
            ) VALUES (
                %s, %s, %s,
                %s, %s, %s, %s,
                %s, %s,
                %s, %s,
                %s, %s, %s,
                %s, %s,
                %s, %s, %s
            ) RETURNING id
            """,
            (
                camp["id"], camp["advertiser_id"], camp["commercial_model"],
                metrics["gross_media_spend"], metrics["platform_fee_amount"],
                metrics["internal_fee_amount"], metrics["total_cost"],
                qualified_calls, cpa_rate_for_row,
                int(camp["impressions"] or 0), camp.get("gross_rate_cpm_usd"),
                metrics["net_revenue"], metrics["net_margin"], metrics["margin_pct"],
                camp.get("margin_floor_pct"), camp.get("min_spend_to_pause"),
                decision["action"], decision["detail"], enforce,
            ),
        )
        event_id = cur.fetchone()[0]
        cur.execute(
            """
            UPDATE ss_campaigns
               SET last_margin_evaluated_at = NOW(),
                   last_margin_pct = %s,
                   last_margin_action = %s
             WHERE id = %s
            """,
            (metrics["margin_pct"], decision["action"], camp["id"]),
        )
    conn.commit()
    return event_id


# ── SS pause (enforce-mode only) ─────────────────────────────────────────


def _pause_at_ss(camp: dict[str, Any], reason: str) -> dict[str, Any]:
    """Pause the demand tag at SpringServe — flat PUT shape per the project's
    confirmed-working pattern (see pgam_ss_freq_cap_quirk)."""
    base = (os.environ.get("SPRINGSERVE_BASE_URL", "https://console.springserve.com/api/v0")).rstrip("/")
    email = os.environ.get("SPRINGSERVE_EMAIL")
    password = os.environ.get("SPRINGSERVE_PASSWORD")
    if not (email and password):
        return {"ok": False, "detail": "SS credentials not configured"}

    # Resolve demand tag id from ss_tag_mirror or springserve_demand_tag_id.
    mirror = camp.get("ss_tag_mirror") or {}
    tag_id = mirror.get("ss_tag_id") if isinstance(mirror, dict) else None
    if not tag_id:
        tag_id = camp.get("springserve_demand_tag_id")
    if not tag_id:
        return {"ok": False, "detail": "no demand_tag_id"}
    try:
        tag_id_int = int(tag_id)
    except Exception:
        return {"ok": False, "detail": f"invalid tag_id: {tag_id}"}

    auth = requests.post(
        f"{base}/auth",
        json={"email": email, "password": password},
        timeout=15,
    )
    if not auth.ok:
        return {"ok": False, "detail": f"SS auth HTTP {auth.status_code}"}
    token = auth.json().get("token")
    headers = {"Authorization": token, "Content-Type": "application/json"}
    res = requests.put(
        f"{base}/demand_tags/{tag_id_int}",
        headers=headers,
        json={"is_active": False},
        timeout=15,
    )
    if not res.ok:
        return {"ok": False, "detail": f"SS PUT HTTP {res.status_code}: {res.text[:200]}"}
    return {"ok": True, "detail": f"paused tag {tag_id_int}: {reason}"}


# ── Slack ────────────────────────────────────────────────────────────────


def _post_slack(text: str) -> None:
    """Fire-and-forget — Slack failure doesn't crash the watchdog."""
    try:
        send_text(text)
    except Exception as e:
        print(f"[dsp_margin_watchdog] slack post failed: {e}")


# ── Main ─────────────────────────────────────────────────────────────────


def margin_watchdog() -> dict[str, Any]:
    """Run one watchdog tick. Returns a summary dict (also printed)."""
    enforce = _enforce_mode()
    started = time.time()
    summary = {
        "ok": True,
        "enforce": enforce,
        "evaluated": 0,
        "actions": {"ok": 0, "approaching_floor": 0, "would_pause_dry_run": 0, "paused": 0, "error": 0, "skipped_manual": 0},
    }

    with dsp_connect() as conn:
        campaigns = _load_active_campaigns(conn)
        summary["evaluated"] = len(campaigns)
        if not campaigns:
            print(f"[dsp_margin_watchdog] no active campaigns ({round(time.time()-started,2)}s)")
            return summary

        for camp in campaigns:
            try:
                if camp["commercial_model"] == "cpa":
                    camp["_qualified_calls"] = _count_qualified_calls(conn, camp["id"])
                else:
                    camp["_qualified_calls"] = 0
                metrics = _compute_margin(camp)
                decision = _decide(metrics, camp, enforce)
                action = decision["action"]
                summary["actions"][action] = summary["actions"].get(action, 0) + 1

                pause_result = None
                if enforce and action == "paused":
                    pause_result = _pause_at_ss(camp, decision["detail"])

                _record_margin_event(conn, camp, metrics, decision, enforce)

                if action in ("paused", "would_pause_dry_run"):
                    msg = (
                        f":rotating_light: {camp['commercial_model'].upper()} margin floor breach — "
                        f"*{camp['id']}* (adv *{camp['advertiser_id']}*): {decision['detail']}. "
                        + (f"Auto-paused at SS ({'ok' if pause_result and pause_result.get('ok') else 'fail'})."
                           if enforce else "DRY RUN — no vendor action.")
                    )
                    _post_slack(msg)
                elif action == "approaching_floor":
                    _post_slack(
                        f":warning: {camp['commercial_model'].upper()} margin approaching floor — "
                        f"*{camp['id']}*: {decision['detail']}"
                    )
            except Exception as e:
                summary["actions"]["error"] = summary["actions"].get("error", 0) + 1
                print(f"[dsp_margin_watchdog] {camp.get('id')} error: {type(e).__name__}: {e}")

    elapsed = round(time.time() - started, 2)
    print(
        f"[dsp_margin_watchdog] enforce={enforce} evaluated={summary['evaluated']} "
        f"actions={summary['actions']} ({elapsed}s)"
    )
    return summary


if __name__ == "__main__":
    import pprint
    pprint.pprint(margin_watchdog())
