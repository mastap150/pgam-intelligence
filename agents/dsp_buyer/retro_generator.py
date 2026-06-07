"""
agents/dsp_buyer/retro_generator.py
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Post-flight retrospective generator — Stage 1 of the buyer agent learning
system.

For every campaign whose `end_date` has passed in the last N days and which
doesn't yet have a `campaign_retros` row, this agent:

  1. Pulls final lifetime delivery + cost from SS /report.
  2. Walks the `buyer_agent_actions` ledger for every applied lever.
  3. For each apply, compares the intended-metric in the 24h-before window
     vs the 24h-after window and tags the lever:
        - WORKED  : metric moved in the intended direction by ≥10%
        - NEUTRAL : within ±5%
        - HARMED  : moved against intent by ≥10%
  4. Detects known config traps (UTC end_date that truncated ET prime-time,
     daily cap with smooth pacing while goal was tight, bid_shading enabled
     but cleared CPM never moved, etc.).
  5. Composes a narrative summary.
  6. Inserts one row into `campaign_retros`.
  7. Posts a Slack digest.

Runs daily at 09:30 ET via pgam-intelligence scheduler. Idempotent — the
UNIQUE constraint on `campaign_id` prevents duplicate retros, and the
`slack_posted_at` field prevents duplicate Slack posts.

The lever_outcomes + config_traps JSONB fields are the data substrate for
Stage 2 (knowledge base) — when ≥5 retros exist per (advertiser, demand_type)
tuple, status-report.ts thresholds can be dynamically tuned from observed
success rates rather than hardcoded.
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

import psycopg
import requests
from dotenv import load_dotenv
from psycopg.types.json import Json

from core.dsp_neon import connect as dsp_connect
from core.slack import send_text

load_dotenv(override=True)

# ── Tunables ───────────────────────────────────────────────────────────────
# How many days back to look for newly-ended campaigns missing a retro.
RETRO_LOOKBACK_DAYS = int(os.environ.get("RETRO_LOOKBACK_DAYS", "7"))
# Improvement threshold for tagging a lever as WORKED.
WORKED_DELTA_PCT = 0.10
# Tolerance band for tagging a lever as NEUTRAL.
NEUTRAL_BAND_PCT = 0.05


def retro_generator() -> dict[str, Any]:
    """Entry point invoked by scheduler.py daily at 09:30 ET."""
    started_at = datetime.now(timezone.utc)
    out: dict[str, Any] = {
        "started_at": started_at.isoformat(),
        "candidates_found": 0,
        "retros_generated": 0,
        "errors": 0,
        "campaign_ids": [],
    }

    conn = dsp_connect()
    try:
        candidates = _load_ended_campaigns_needing_retro(conn)
        out["candidates_found"] = len(candidates)
        for camp in candidates:
            try:
                retro_id = _generate_retro(conn, camp)
                out["retros_generated"] += 1
                out["campaign_ids"].append({"id": camp["id"], "retro_id": retro_id})
            except Exception as e:
                out["errors"] += 1
                print(f"[retro_generator] error on {camp['id']}: {e}")
    finally:
        conn.close()

    print(f"[retro_generator] candidates={out['candidates_found']} "
          f"generated={out['retros_generated']} errors={out['errors']}")
    return out


# ── Candidate selection ────────────────────────────────────────────────────


def _load_ended_campaigns_needing_retro(conn) -> list[dict[str, Any]]:
    """Campaigns with end_date in [now-LOOKBACK, now-6h] and no existing retro.

    The 6h buffer ensures SS /report has flushed final numbers (SS aggregates
    daily totals with up to several hours of lag in our experience)."""
    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT c.id, c.name, c.advertiser_id,
                   c.start_date, c.end_date,
                   c.springserve_demand_tag_ids[1] AS tag_id,
                   c.budget_total, c.gross_rate_cpm_usd, c.commercial_model,
                   c.ss_tag_mirror
              FROM ss_campaigns c
         LEFT JOIN campaign_retros r ON r.campaign_id = c.id
             WHERE c.end_date IS NOT NULL
               AND c.end_date < NOW() - INTERVAL '6 hours'
               AND c.end_date > NOW() - INTERVAL '{RETRO_LOOKBACK_DAYS} days'
               AND r.id IS NULL
             ORDER BY c.end_date DESC
            """
        )
        cols = [d.name for d in cur.description]
        return [dict(zip(cols, r)) for r in cur.fetchall()]


# ── Retro generation ───────────────────────────────────────────────────────


def _generate_retro(conn, camp: dict[str, Any]) -> int:
    """Build + insert the retro row + post Slack. Returns the retro id."""
    tag_id = camp["tag_id"]
    headers = _ss_auth()
    if not headers:
        raise RuntimeError("SS credentials not configured")

    # 1. Final lifetime delivery.
    final = _fetch_lifetime(headers, int(tag_id), camp["start_date"], camp["end_date"])
    delivered = final.get("impressions", 0)
    spent = final.get("billable_cost", 0)
    q4 = final.get("fourth_quartile", 0)
    clicks = final.get("clicks", 0)
    avg_vtr_pct = (q4 / delivered * 100) if delivered else 0
    avg_ctr_pct = (clicks / delivered * 100) if delivered else 0
    avg_cleared_cpm = (spent / delivered * 1000) if delivered else 0

    # 2. Goal computation (pulls live SS budgets, falls back to Neon).
    goal_imps = _compute_goal(camp, headers)
    delivery_pct = (delivered / goal_imps * 100) if goal_imps else None

    # 3. Revenue + margin.
    gross_rate = float(camp.get("gross_rate_cpm_usd") or 7.0)
    gross_revenue = delivered * gross_rate / 1000
    realized_margin_pct = (
        (gross_revenue - spent) / gross_revenue * 100 if gross_revenue > 0 else None
    )

    # 4. Lever outcomes — walk every applied ledger row.
    lever_outcomes = _analyze_lever_outcomes(conn, camp, headers)

    # 5. Config traps.
    config_traps = _detect_config_traps(camp, lever_outcomes, avg_cleared_cpm)

    # 6. Summary text.
    summary = _compose_summary(camp, delivered, goal_imps, delivery_pct,
                                avg_vtr_pct, realized_margin_pct,
                                lever_outcomes, config_traps)

    # 7. Insert.
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO campaign_retros
              (campaign_id, advertiser_id, campaign_name,
               flight_start, flight_end,
               delivered_imps, goal_imps, delivery_pct,
               lifetime_media_cost, gross_revenue, realized_margin_pct,
               avg_vtr_pct, avg_ctr_pct, avg_cleared_cpm,
               lever_outcomes, config_traps, summary_text)
            VALUES
              (%s, %s, %s,
               %s, %s,
               %s, %s, %s,
               %s, %s, %s,
               %s, %s, %s,
               %s::JSONB, %s::JSONB, %s)
            RETURNING id
            """,
            (camp["id"], camp["advertiser_id"], camp["name"],
             camp["start_date"], camp["end_date"],
             delivered, goal_imps, round(delivery_pct, 3) if delivery_pct else None,
             round(spent, 4), round(gross_revenue, 4),
             round(realized_margin_pct, 3) if realized_margin_pct else None,
             round(avg_vtr_pct, 3), round(avg_ctr_pct, 5), round(avg_cleared_cpm, 3),
             Json(lever_outcomes), Json(config_traps), summary),
        )
        retro_id = cur.fetchone()[0]

    conn.commit()

    # 8. Slack.
    _post_slack_retro(camp, delivered, goal_imps, delivery_pct,
                       realized_margin_pct, avg_vtr_pct,
                       lever_outcomes, config_traps, retro_id)

    with conn.cursor() as cur:
        cur.execute("UPDATE campaign_retros SET slack_posted_at=NOW() WHERE id=%s", (retro_id,))
    conn.commit()

    return retro_id


# ── Lever outcome analysis ─────────────────────────────────────────────────


# Per-lever: which metric is the lever trying to move, and in which direction.
INTENT_BY_LEVER = {
    "freq_cap":            ("daily_imps", "up"),     # loosening → more delivery
    "publisher_blacklist": ("avg_vtr",    "up"),     # remove drag → higher VTR
    "daily_budget":        ("daily_spend","up"),     # nudge up → spend more
    "budget_pacing":       ("daily_imps", "up"),     # smooth→front_loaded burns faster
    "creative_pause":      ("avg_vtr",    "up"),     # pause loser → tag avg VTR rises
    "creative_weight_up":  ("avg_vtr",    "up"),     # weight to winner → VTR rises
    "geo_narrow":          ("avg_vtr",    "up"),     # drop bad DMAs → VTR rises
    "state_block":         ("avg_vtr",    "up"),     # drop bad states → VTR rises
    "budget_reallocate":   ("daily_imps", "up"),     # recipient side: more budget
}


def _analyze_lever_outcomes(conn, camp: dict[str, Any], headers) -> list[dict[str, Any]]:
    """For each applied ledger row in this campaign, compute pre/post deltas
    on the intended metric and tag worked / neutral / harmed."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT id, lever, action, reason, created_at,
                   before_state, after_state, applied, rolled_back_at
              FROM buyer_agent_actions
             WHERE campaign_id = %s
               AND applied = TRUE
               AND lever = ANY(%s)
             ORDER BY created_at ASC
            """,
            (camp["id"], list(INTENT_BY_LEVER.keys())),
        )
        rows = [dict(zip([c.name for c in cur.description], r)) for r in cur.fetchall()]

    outcomes: list[dict[str, Any]] = []
    tag_id = int(camp["tag_id"]) if camp.get("tag_id") else None

    for r in rows:
        lever = r["lever"]
        intent = INTENT_BY_LEVER.get(lever)
        if not intent or not tag_id:
            outcomes.append({
                "ledger_id": r["id"], "lever": lever, "action": r["action"],
                "applied_at": r["created_at"].isoformat(),
                "verdict": "skipped", "evidence": "no intent rule or tag_id missing",
            })
            continue
        metric, direction = intent
        applied_at = r["created_at"]

        # 24h window before vs after the apply
        pre_start = (applied_at - timedelta(days=1)).date().isoformat()
        pre_end = applied_at.date().isoformat()
        post_start = applied_at.date().isoformat()
        post_end = (applied_at + timedelta(days=1)).date().isoformat()

        pre = _fetch_window_metrics(headers, tag_id, pre_start, pre_end)
        post = _fetch_window_metrics(headers, tag_id, post_start, post_end)

        pre_val = _extract_metric(pre, metric)
        post_val = _extract_metric(post, metric)
        verdict, delta_pct, evidence = _judge_outcome(pre_val, post_val, direction, metric)

        outcomes.append({
            "ledger_id": r["id"],
            "lever": lever,
            "action": r["action"],
            "applied_at": applied_at.isoformat(),
            "intended_metric": metric,
            "intended_direction": direction,
            "pre_value": round(pre_val, 4) if pre_val is not None else None,
            "post_value": round(post_val, 4) if post_val is not None else None,
            "delta_pct": round(delta_pct * 100, 2) if delta_pct is not None else None,
            "verdict": verdict,
            "evidence": evidence,
            "rolled_back": r.get("rolled_back_at") is not None,
        })

    return outcomes


def _extract_metric(window: dict, metric: str) -> Optional[float]:
    if not window:
        return None
    imps = window.get("impressions", 0)
    cost = window.get("billable_cost", 0)
    q4 = window.get("fourth_quartile", 0)
    if metric == "daily_imps":
        return float(imps)
    if metric == "daily_spend":
        return float(cost)
    if metric == "avg_vtr":
        return float(q4 / imps) if imps else None
    if metric == "avg_cpm":
        return float(cost / imps * 1000) if imps else None
    return None


def _judge_outcome(
    pre: Optional[float],
    post: Optional[float],
    direction: str,
    metric: str,
) -> tuple[str, Optional[float], str]:
    """Return (verdict, delta_pct, evidence_string)."""
    if pre is None or post is None or pre == 0:
        return ("inconclusive", None,
                f"insufficient data (pre={pre}, post={post})")
    delta = (post - pre) / abs(pre)
    # Adjust sign based on intended direction.
    signed = delta if direction == "up" else -delta
    if signed >= WORKED_DELTA_PCT:
        return ("worked", delta,
                f"{metric} {pre:.4g} → {post:.4g} ({signed*100:+.1f}% in intended direction)")
    if abs(signed) < NEUTRAL_BAND_PCT:
        return ("neutral", delta,
                f"{metric} {pre:.4g} → {post:.4g} (within ±{NEUTRAL_BAND_PCT*100:.0f}% band)")
    if signed <= -WORKED_DELTA_PCT:
        return ("harmed", delta,
                f"{metric} {pre:.4g} → {post:.4g} ({signed*100:+.1f}% AGAINST intent)")
    return ("neutral", delta,
            f"{metric} {pre:.4g} → {post:.4g} ({signed*100:+.1f}% — modest)")


# ── Config trap detection ──────────────────────────────────────────────────


def _detect_config_traps(
    camp: dict[str, Any],
    lever_outcomes: list[dict[str, Any]],
    avg_cleared_cpm: float,
) -> list[dict[str, Any]]:
    """Pattern-match known config issues from this campaign's history."""
    traps: list[dict[str, Any]] = []

    # Trap 1: UTC end_date truncated ET prime-time.
    end_date = camp.get("end_date")
    if end_date:
        # We stored end_date as DATE in Neon; SS API uses UTC timestamps.
        # The trap is: if the original SS end_date is 23:59:59Z (= 19:59 ET),
        # we lost the 8pm-midnight ET window. Heuristic: if no extension
        # ledger row exists, flag.
        end_extension_seen = any(
            "extend" in (o.get("evidence","") or "").lower() or
            "end_date" in (o.get("evidence","") or "").lower()
            for o in lever_outcomes
        )
        if not end_extension_seen:
            traps.append({
                "trap_id": "utc_end_date_truncates_et",
                "severity": "warning",
                "description": "Default SS end_date is UTC midnight = 7:59pm ET, losing the 4-hour prime-time block",
                "fix_recommendation": "Set end_date as YYYY-MM-DDT03:59:59Z (4h offset) to capture full ET day",
            })

    # Trap 2: smooth-paced daily cap on tight-deadline campaign.
    mirror = camp.get("ss_tag_mirror") or {}
    saw_smooth_daily = any(
        o.get("lever") == "budget_pacing" and "smooth" in (o.get("evidence","") or "").lower()
        for o in lever_outcomes
    )
    if saw_smooth_daily:
        # Did delivery actually get throttled? Look for budget_pacing rows
        # that ended NEUTRAL or HARMED.
        throttled = any(
            o.get("lever") == "budget_pacing" and o.get("verdict") in ("neutral","harmed")
            for o in lever_outcomes
        )
        if throttled:
            traps.append({
                "trap_id": "smooth_pacing_throttled_delivery",
                "severity": "critical",
                "description": "Daily/lifetime cap with smooth pacing throttled delivery even when cap wasn't bound",
                "fix_recommendation": "Use null/ASAP pacing on caps when timeline is tight; lifetime imp cap alone is sufficient overage protection",
            })

    # Trap 3: bid_shading enabled but cleared CPM didn't move.
    # We can check this from buyer_agent_actions where lever='daily_budget'
    # was the workaround label used for bid_shading enables.
    # Simplest heuristic: if avg_cleared_cpm is within $0.05 of bid_floor,
    # shading didn't actuate.
    bid_floor = float(camp.get("ss_tag_mirror", {}).get("ss_tag_rate") or 0) if isinstance(camp.get("ss_tag_mirror"), dict) else 0
    if 0 < bid_floor < avg_cleared_cpm and (avg_cleared_cpm - bid_floor) > 0.10:
        # Realized CPM is well ABOVE bid_floor — shading didn't matter.
        # Whether shading was actually enabled requires reading ledger; for
        # now this is informational only.
        traps.append({
            "trap_id": "cleared_cpm_above_floor",
            "severity": "info",
            "description": f"Cleared CPM ${avg_cleared_cpm:.2f} vs bid_floor ${bid_floor:.2f} — bid_shading (if enabled) didn't reduce cost-per-imp on this SS account",
            "fix_recommendation": "Skip bid_shading on future campaigns until confirmed to actuate on a test campaign",
        })

    # Trap 4: under-delivered (>5% short of goal).
    pass  # Captured in delivery_pct field directly; summary will mention.

    return traps


# ── Summary text ───────────────────────────────────────────────────────────


def _compose_summary(
    camp: dict[str, Any],
    delivered: int,
    goal_imps: Optional[int],
    delivery_pct: Optional[float],
    avg_vtr: float,
    margin_pct: Optional[float],
    lever_outcomes: list[dict[str, Any]],
    config_traps: list[dict[str, Any]],
) -> str:
    lines = []
    lines.append(f"Campaign: {camp.get('name')} ({camp['id']})")
    lines.append(f"Flight: {camp['start_date']} → {camp['end_date']}")
    if goal_imps:
        lines.append(f"Delivered: {delivered:,} / {goal_imps:,} = {delivery_pct:.1f}%")
    else:
        lines.append(f"Delivered: {delivered:,} (no goal set)")
    lines.append(f"VTR avg: {avg_vtr:.1f}%   Margin: {margin_pct:.1f}%" if margin_pct else f"VTR avg: {avg_vtr:.1f}%")
    lines.append("")

    if lever_outcomes:
        worked = [o for o in lever_outcomes if o["verdict"] == "worked"]
        harmed = [o for o in lever_outcomes if o["verdict"] == "harmed"]
        neutral = [o for o in lever_outcomes if o["verdict"] == "neutral"]
        inconclusive = [o for o in lever_outcomes if o["verdict"] not in ("worked","neutral","harmed")]
        lines.append(
            f"Lever outcomes ({len(worked)} worked, {len(neutral)} neutral, "
            f"{len(harmed)} harmed, {len(inconclusive)} inconclusive):"
        )
        for o in lever_outcomes:
            v = o["verdict"].upper()
            lines.append(f"  - {o['lever']} ({o.get('action','')}): {v} — {o.get('evidence','')}")
        lines.append("")

    if config_traps:
        lines.append("Config traps for future campaigns:")
        for t in config_traps:
            lines.append(f"  [{t['severity'].upper()}] {t['description']}")
            lines.append(f"     → fix: {t['fix_recommendation']}")

    return "\n".join(lines)


# ── Goal computation ───────────────────────────────────────────────────────


def _compute_goal(camp: dict[str, Any], headers: Optional[dict] = None) -> Optional[int]:
    """Goal preference order:
      1. SS-side lifetime budget if metric=impressions (mechanical cap = goal).
      2. SS-side lifetime budget if metric=gross_cost, divided by SS rate.
      3. Fallback: campaign.budget_total / gross_rate_cpm_usd * 1000.
    Pulled LIVE from SS, not from the mirror, because the mirror only stores
    flat fields and may be stale."""
    tag_id = camp.get("tag_id")
    if tag_id and headers:
        base = os.environ.get("SPRINGSERVE_BASE_URL", "").rstrip("/")
        try:
            res = requests.get(f"{base}/demand_tags/{int(tag_id)}", headers=headers, timeout=15)
            if res.ok:
                tag = res.json()
                rate = float(tag.get("rate") or 0)
                for b in (tag.get("budgets") or []):
                    if b.get("budget_period") == "lifetime":
                        if b.get("budget_metric") == "impressions":
                            return int(b.get("budget_value") or 0)
                        if b.get("budget_metric") == "gross_cost" and rate > 0:
                            return int(round(float(b.get("budget_value") or 0) / rate * 1000))
        except Exception:
            pass
    # Fallback: Neon-side budget_total / gross_cpm * 1000
    budget = camp.get("budget_total")
    gross = camp.get("gross_rate_cpm_usd")
    if budget and gross and float(gross) > 0:
        return int(round(float(budget) * 1000 / float(gross)))
    return None


# ── SS API helpers ─────────────────────────────────────────────────────────


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
    return {"Authorization": token, "Content-Type": "application/json"} if token else None


def _fetch_lifetime(headers, tag_id: int, start_date, end_date) -> dict[str, float]:
    base = os.environ.get("SPRINGSERVE_BASE_URL").rstrip("/")
    body = {
        "start_date": start_date.isoformat() if hasattr(start_date, "isoformat") else str(start_date),
        "end_date": end_date.isoformat() if hasattr(end_date, "isoformat") else str(end_date),
        "interval": "cumulative",
        "dimensions": ["demand_tag_id"],
    }
    try:
        res = requests.post(f"{base}/report", headers=headers, json=body, timeout=30)
    except Exception:
        return {}
    if not res.ok:
        return {}
    rows = res.json() if isinstance(res.json(), list) else res.json().get("data", [])
    row = next((r for r in rows if int(r.get("demand_tag_id", -1)) == tag_id), None)
    if not row:
        return {}
    return {
        "impressions": int(row.get("impressions", 0)),
        "billable_cost": float(row.get("billable_cost", 0)),
        "fourth_quartile": float(row.get("fourth_quartile", 0)),
        "clicks": int(row.get("clicks", 0)),
    }


def _fetch_window_metrics(headers, tag_id: int, start: str, end: str) -> dict[str, float]:
    return _fetch_lifetime(headers, tag_id, start, end)


# ── Slack post ─────────────────────────────────────────────────────────────


def _post_slack_retro(
    camp: dict[str, Any],
    delivered: int,
    goal_imps: Optional[int],
    delivery_pct: Optional[float],
    margin_pct: Optional[float],
    avg_vtr: float,
    lever_outcomes: list[dict[str, Any]],
    config_traps: list[dict[str, Any]],
    retro_id: int,
) -> None:
    name = camp.get("name", camp["id"])
    pct_str = f"{delivery_pct:.1f}%" if delivery_pct else "—"
    margin_str = f"{margin_pct:.1f}%" if margin_pct else "—"

    icon = "🎯" if (delivery_pct and delivery_pct >= 99) else (
           "✅" if (delivery_pct and delivery_pct >= 95) else "⚠️")

    worked = sum(1 for o in lever_outcomes if o["verdict"] == "worked")
    harmed = sum(1 for o in lever_outcomes if o["verdict"] == "harmed")
    neutral = sum(1 for o in lever_outcomes if o["verdict"] == "neutral")

    msg_lines = [
        f"{icon} *Campaign retro: {name}* — flight ended",
        f"• Delivered: *{delivered:,}* / {goal_imps:,} = *{pct_str}*" if goal_imps else f"• Delivered: *{delivered:,}*",
        f"• VTR avg: {avg_vtr:.1f}%  •  Margin: {margin_str}",
        f"• Lever outcomes: ✅ {worked} worked · ➖ {neutral} neutral · ❌ {harmed} harmed",
    ]
    if config_traps:
        msg_lines.append(f"• Config traps detected: {len(config_traps)}")
        for t in config_traps[:3]:
            msg_lines.append(f"   - [{t['severity']}] {t['description']}")
    msg_lines.append(f"• Full retro: campaign_retros.id={retro_id}")

    try:
        send_text("\n".join(msg_lines))
    except Exception as e:
        print(f"[retro_generator] slack post failed: {e}")


if __name__ == "__main__":
    import json
    print(json.dumps(retro_generator(), indent=2, default=str))
