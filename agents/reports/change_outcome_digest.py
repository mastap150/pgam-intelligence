"""
agents/reports/change_outcome_digest.py

Daily Slack digest at 09:00 ET that closes the feedback loop on every
autonomous change made by the system.

Two sections per post
=====================

Section A — "Made yesterday" (changes 0-24h old)
  For each: actor, what changed, why, expected lift if known.
  These are the *birth notifications* — what the agents did.

Section B — "Outcomes from 48-72h ago" (the lift indication)
  For each ledger entry that was made 48-72h ago:
    - Pre-change revenue rate vs post-change revenue rate (per demand)
    - Verdict: WINNER / NEUTRAL / LOSER
    - If LOSER: confirm whether intervention_journal already reverted
    - If WINNER: estimate weekly $ lift
    - If NEUTRAL: note "no signal"

Section C — "Running tally" (week-to-date)
  Total auto-changes this week.
  Net estimated lift from winners minus losers.

Why daily, not real-time
------------------------
Real-time per-write Slack messages would be noisy (50+ writes/day).
A single morning digest is digestible: one read, full context. The
intervention_journal still posts immediately when it auto-reverts a
loser (P1-equivalent), so urgent reverts are visible in real time.

Why 48-72h window for outcomes
------------------------------
LL's bid-shading ML and DSP responses settle within 48h. Earlier
than that and we'd attribute noise to our changes; later than that
and we'd miss recent changes in the digest.
"""
from __future__ import annotations

import gzip
import json
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

from core import floor_ledger, slack

DATA_DIR = Path(__file__).parent.parent.parent / "data"
HOURLY_PATH = DATA_DIR / "hourly_pub_demand.json.gz"

# Window definitions
RECENT_HOURS = 24             # "made yesterday" window
OUTCOME_MIN_HOURS = 48        # changes must be at least this old to evaluate
OUTCOME_MAX_HOURS = 72        # ...and at most this old (otherwise stale)
PRE_WINDOW_HOURS = 48         # baseline = 48h before change
POST_WINDOW_HOURS_MIN = 24    # need at least this much post-data
WEEK_HOURS = 24 * 7

# Verdict thresholds
WINNER_RATIO = 1.10          # post >= 110% of pre = winner
LOSER_RATIO = 0.85           # post < 85% of pre = loser

# Recognized actor categories — labels for the digest
ACTOR_LABELS = {
    "trend_hunter": "🤖 Trend hunter",
    "config_health_scanner": "🔧 Config health",
    "auto_wire_gaps": "➕ Auto-wire",
    "auto_unpause": "🔄 Auto-unpause",
    "auto_revert_harmful": "⏪ Harmful-revert",
    "auto_adjust_wirings": "✂️ Adjust wirings",
    "intervention_journal": "📓 Intervention journal",
    "contract_floor_sentry": "🛡 Contract sentry",
    "9dots_contract": "🛡 9 Dots contract",
    "junk_filter": "🧹 Junk filter",
    "enable_lurl": "📊 Enable LURL",
    "enable_schain": "🔗 Enable supply chain",
    "qps_raise": "⚡ QPS raise",
    "manual_wire": "✋ Manual wire",
    "demand_gap_wiring": "➕ Demand-gap wire",
    "floor_optimizer_revert": "⏪ Floor optimizer revert",
    "ninedots_underpriced_raise": "📈 9 Dots underpriced raise",
    "revenue_lift_batch": "💰 Revenue lift batch",
}


def _category_for_actor(actor: str) -> str:
    actor_lc = (actor or "").lower()
    for key, label in ACTOR_LABELS.items():
        if key.lower() in actor_lc:
            return label
    return "🔘 Other"


def _is_evaluation_only(actor: str) -> bool:
    """Skip 'tag-only' ledger entries that don't represent real writes."""
    actor_lc = (actor or "").lower()
    return "intervention_journal_eval" in actor_lc


def _per_demand_rate(rows: list[dict], demand_id: int,
                     start: datetime, end: datetime) -> tuple[float, int]:
    """Sum revenue + count distinct hours in [start, end] for a demand."""
    total = 0.0
    seen = set()
    for r in rows:
        if int(r.get("DEMAND_ID", 0) or 0) != demand_id:
            continue
        d = str(r.get("DATE", ""))
        h = int(r.get("HOUR", 0) or 0)
        if not d:
            continue
        try:
            dt = datetime(int(d[:4]), int(d[5:7]), int(d[8:10]), h, tzinfo=timezone.utc)
        except (ValueError, TypeError):
            continue
        if start <= dt <= end:
            total += float(r.get("GROSS_REVENUE", 0) or 0)
            seen.add((d, h))
    return total, len(seen)


def _evaluate_outcome(entry: dict, hourly: list[dict]) -> dict:
    """Compute pre/post rates and verdict for a ledger entry."""
    ts_dt = datetime.fromisoformat(entry["ts_utc"].replace("Z", "+00:00"))
    now = datetime.now(timezone.utc)

    pre_rev, pre_h = _per_demand_rate(hourly, entry["demand_id"],
                                       ts_dt - timedelta(hours=PRE_WINDOW_HOURS), ts_dt)
    post_rev, post_h = _per_demand_rate(hourly, entry["demand_id"],
                                          ts_dt, now)

    if pre_h < 6 or post_h < POST_WINDOW_HOURS_MIN:
        return {"verdict": "insufficient_data", "pre_h": pre_h, "post_h": post_h}

    pre_rate = pre_rev / pre_h
    post_rate = post_rev / post_h

    if pre_rate < 1.0:
        return {"verdict": "tiny_baseline", "pre_rate": pre_rate}

    ratio = post_rate / pre_rate
    if ratio >= WINNER_RATIO:
        verdict = "winner"
    elif ratio < LOSER_RATIO:
        verdict = "loser"
    else:
        verdict = "neutral"

    return {
        "verdict": verdict,
        "pre_rate_per_h": round(pre_rate, 2),
        "post_rate_per_h": round(post_rate, 2),
        "ratio": round(ratio, 3),
        "weekly_lift_estimate": round((post_rate - pre_rate) * 24 * 7, 2),
    }


def _was_reverted(entry_id: str, all_ledger: list[dict]) -> bool:
    for r in all_ledger:
        if r.get("reverted_from") == entry_id:
            return True
    return False


def build_digest() -> dict:
    if not HOURLY_PATH.exists():
        return {"skipped": True, "reason": "no hourly data"}
    with gzip.open(HOURLY_PATH, "rt") as f:
        hourly = json.load(f)
    all_ledger = floor_ledger.read_all()

    now = datetime.now(timezone.utc)
    recent_cutoff = (now - timedelta(hours=RECENT_HOURS)).isoformat()
    outcome_min = (now - timedelta(hours=OUTCOME_MAX_HOURS)).isoformat()
    outcome_max = (now - timedelta(hours=OUTCOME_MIN_HOURS)).isoformat()
    week_cutoff = (now - timedelta(hours=WEEK_HOURS)).isoformat()

    # Filter to actionable ledger entries (skip eval-tag entries)
    actionable = [r for r in all_ledger
                  if r.get("applied") and not r.get("dry_run")
                  and not _is_evaluation_only(r.get("actor", ""))]

    # Section A: changes made in last 24h
    recent = [r for r in actionable if r.get("ts_utc", "") >= recent_cutoff]

    # Section B: outcomes for changes 48-72h ago
    eval_window = [r for r in actionable
                   if outcome_min <= r.get("ts_utc", "") <= outcome_max]
    outcomes = []
    for entry in eval_window:
        actor = entry.get("actor", "")
        # Skip pure-tag actors (don't change live state)
        if "_eval" in actor or "intervention_journal_eval" in actor:
            continue
        # Wire/unpause have None floors — outcome on those is "did the demand earn money?"
        out = _evaluate_outcome(entry, hourly)
        out["entry"] = entry
        out["was_reverted"] = _was_reverted(entry.get("id", ""), all_ledger)
        outcomes.append(out)

    # Section C: weekly tally
    week_actions = [r for r in actionable if r.get("ts_utc", "") >= week_cutoff]
    weekly_winners_lift = sum(
        o.get("weekly_lift_estimate", 0)
        for o in outcomes if o.get("verdict") == "winner"
    )
    weekly_losers_loss = sum(
        o.get("weekly_lift_estimate", 0)  # negative for losers
        for o in outcomes if o.get("verdict") == "loser" and not o.get("was_reverted")
    )
    net_estimate = weekly_winners_lift + weekly_losers_loss  # losers contribute negative

    # Build Slack digest
    lines = [
        f":scroll: *Autonomous change accountability — {now.strftime('%Y-%m-%d')}*",
        "",
    ]

    # Section A
    if recent:
        by_actor = defaultdict(list)
        for r in recent:
            by_actor[_category_for_actor(r.get("actor", ""))].append(r)
        lines.append(f"*A. Changes made in last 24h ({len(recent)} total)*")
        for label, items in sorted(by_actor.items(), key=lambda kv: -len(kv[1])):
            lines.append(f"  {label} — {len(items)}")
            for r in items[:3]:
                old, new = r.get("old_floor"), r.get("new_floor")
                action = (f"floor {old} → {new}" if old != new and (old or new)
                          else "wiring/status change")
                lines.append(f"    • demand `{r.get('demand_id')}` {action}  "
                              f"_{(r.get('demand_name','') or '')[:30]}_")
            if len(items) > 3:
                lines.append(f"    _...and {len(items)-3} more_")
    else:
        lines.append("*A. Changes made in last 24h:* none")

    lines.append("")

    # Section B
    if outcomes:
        winners = [o for o in outcomes if o["verdict"] == "winner"]
        losers = [o for o in outcomes if o["verdict"] == "loser"]
        neutrals = [o for o in outcomes if o["verdict"] == "neutral"]
        insufficient = [o for o in outcomes if o["verdict"] in ("insufficient_data", "tiny_baseline")]

        lines.append(f"*B. Outcomes from changes 48-72h ago ({len(outcomes)} evaluable)*")
        if winners:
            lines.append(f"  ✅ {len(winners)} WINNERS  (~+${sum(w.get('weekly_lift_estimate',0) for w in winners):,.0f}/wk est)")
            for o in winners[:3]:
                e = o["entry"]
                lines.append(f"    • demand `{e['demand_id']}` ratio {o['ratio']:.2f}  "
                              f"_{(e.get('demand_name','') or '')[:30]}_  "
                              f"by `{e.get('actor','?')[:25]}`")
        if losers:
            reverted = sum(1 for l in losers if l.get("was_reverted"))
            lines.append(f"  🔻 {len(losers)} LOSERS  ({reverted} already reverted)")
            for o in losers[:3]:
                e = o["entry"]
                tag = " ✓reverted" if o.get("was_reverted") else " ⚠️ not reverted"
                lines.append(f"    • demand `{e['demand_id']}` ratio {o['ratio']:.2f}{tag}  "
                              f"_{(e.get('demand_name','') or '')[:30]}_")
        if neutrals:
            lines.append(f"  ➖ {len(neutrals)} neutral (within ±10%)")
        if insufficient:
            lines.append(f"  ❓ {len(insufficient)} insufficient data")
    else:
        lines.append("*B. Outcomes from 48-72h ago:* no evaluable changes in window")

    lines.append("")

    # Section C
    lines.append(f"*C. Weekly tally (last 7 days)*")
    lines.append(f"  Total auto-changes: {len(week_actions)}")
    if outcomes:
        sign = "+" if net_estimate >= 0 else ""
        lines.append(f"  Net estimated impact from evaluable: {sign}${net_estimate:,.0f}/wk")
    lines.append("")
    lines.append("_intervention_journal auto-reverts losers within 48h. "
                  "Reply with 'investigate <demand_id>' to dig into any item._")

    msg = "\n".join(lines)
    try:
        slack.send_text(msg)
    except Exception as e:
        print(f"[change_outcome_digest] Slack failed: {e}")

    return {
        "ran_at": now.isoformat(),
        "changes_24h": len(recent),
        "outcomes_evaluable": len(outcomes),
        "weekly_actions": len(week_actions),
        "net_estimate_wk": round(net_estimate, 2) if outcomes else None,
    }


def run() -> dict:
    """Scheduler entry."""
    return build_digest()


if __name__ == "__main__":
    print(json.dumps(run(), indent=2, default=str))
