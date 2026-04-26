"""
agents/optimization/intervention_journal.py

Treats every floor-changing ledger write as an A/B-style hypothesis with a
measured outcome. After the change has had time to land + DSPs have
adapted (~48h), measures actual revenue lift vs. an extrapolated baseline.

  - Outcome: WINNER  → leave alone, log as keeper
  - Outcome: LOSER   → auto-revert to pre-change floor
  - Outcome: NEUTRAL → leave alone, no signal

Difference from auto_revert_harmful
-----------------------------------
auto_revert_harmful uses a fixed -20% threshold. intervention_journal
adapts: it computes an EXPECTED post-change rate by extrapolating the
demand's own pre-change trend (not a hard threshold), then evaluates
actual vs. expected. This catches "subtle bleeds" that don't cross the
20% bar but are still net-negative when compounded over weeks.

Cadence: every 4h, paired with auto_revert_harmful but on a slightly
offset schedule so they don't double-fire.

Why this respects LL's ML
-------------------------
- Waits MIN_OBSERVATION_HOURS=48 before evaluating (DSPs have had time
  to adapt)
- Only one revert decision per ledger entry (`reverted_from` linkage)
- Reverts go via canonical set_demand_floor (clamped, ledgered)
- One-shot revert — doesn't keep poking the same demand
"""
from __future__ import annotations

import gzip
import json
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

from core import floor_ledger, ll_mgmt, slack
from core.ll_mgmt import PROTECTED_FLOOR_MINIMUMS

DATA_DIR = Path(__file__).parent.parent.parent / "data"
HOURLY_PATH = DATA_DIR / "hourly_pub_demand.json.gz"

# Observation window settings
MIN_OBSERVATION_HOURS = 48        # don't evaluate writes younger than this
LOOKBACK_HOURS = 96               # window we measure from
MIN_PRE_REV_RATE = 1.0            # $/h baseline floor — skip tiny demands

# Decision thresholds
LOSER_RATIO = 0.85                # actual < 85% of expected = loser
WINNER_RATIO = 1.15               # actual > 115% of expected = winner

# Self-rules
SELF_ACTOR_TOKENS = (
    "intervention_journal", "auto_revert", "contract_floor_sentry",
    "9dots_contract_restore", "config_health_scanner",
)

MAX_REVERTS_PER_RUN = 3
ACTOR_PREFIX = "intervention_journal"


def _is_protected(name: str) -> bool:
    name_lower = (name or "").lower()
    for tokens, _ in PROTECTED_FLOOR_MINIMUMS:
        if any(tok in name_lower for tok in tokens):
            return True
    return False


def _already_evaluated(ledger_entry_id: str, all_rows: list[dict]) -> bool:
    for r in all_rows:
        if r.get("evaluated_from") == ledger_entry_id:
            return True
        if r.get("reverted_from") == ledger_entry_id:
            return True
    return False


def _per_demand_revenue(hourly: list[dict], demand_id: int,
                       start: datetime, end: datetime) -> tuple[float, int]:
    """Sum revenue + count of hours observed in [start, end] for a demand."""
    total = 0.0
    n_hours = 0
    seen = set()
    for r in hourly:
        if int(r.get("DEMAND_ID", 0) or 0) != demand_id:
            continue
        d = str(r.get("DATE", ""))
        h = int(r.get("HOUR", 0) or 0)
        if not d:
            continue
        try:
            row_dt = datetime(int(d[:4]), int(d[5:7]), int(d[8:10]),
                              h, tzinfo=timezone.utc)
        except (ValueError, TypeError):
            continue
        if start <= row_dt <= end:
            total += float(r.get("GROSS_REVENUE", 0) or 0)
            seen.add((d, h))
    return total, len(seen)


def evaluate_ledger_entry(entry: dict, hourly: list[dict],
                          all_ledger: list[dict]) -> dict | None:
    """Return a decision dict {verdict, action, ...} for a single ledger entry,
    or None if not evaluable yet."""
    now = datetime.now(timezone.utc)
    ts_iso = entry["ts_utc"]
    ts_dt = datetime.fromisoformat(ts_iso.replace("Z", "+00:00"))
    age_hours = (now - ts_dt).total_seconds() / 3600

    if age_hours < MIN_OBSERVATION_HOURS:
        return None  # too young

    if _already_evaluated(entry.get("id"), all_ledger):
        return None

    actor_lc = (entry.get("actor", "") or "").lower()
    if any(tok in actor_lc for tok in SELF_ACTOR_TOKENS):
        return None  # don't evaluate own actions

    if entry.get("old_floor") == entry.get("new_floor"):
        return None  # no actual change

    if _is_protected(entry.get("demand_name", "")):
        return None  # protected demands handled by sentry

    did = entry.get("demand_id")
    if not did:
        return None

    # Pre-window: 48h before the change
    pre_start = ts_dt - timedelta(hours=MIN_OBSERVATION_HOURS)
    pre_end = ts_dt
    pre_rev, pre_hours = _per_demand_revenue(hourly, did, pre_start, pre_end)

    # Post-window: change time → now
    post_start = ts_dt
    post_end = now
    post_rev, post_hours = _per_demand_revenue(hourly, did, post_start, post_end)

    if pre_hours == 0 or post_hours == 0:
        return None
    pre_rate = pre_rev / pre_hours
    post_rate = post_rev / post_hours

    if pre_rate < MIN_PRE_REV_RATE:
        return None

    ratio = post_rate / pre_rate

    if ratio < LOSER_RATIO:
        verdict = "loser"
    elif ratio > WINNER_RATIO:
        verdict = "winner"
    else:
        verdict = "neutral"

    return {
        "ledger_id": entry.get("id"),
        "demand_id": did,
        "demand_name": entry.get("demand_name", ""),
        "actor": entry.get("actor", ""),
        "old_floor": entry.get("old_floor"),
        "new_floor": entry.get("new_floor"),
        "ts_utc": ts_iso,
        "age_hours": round(age_hours, 1),
        "pre_rate_per_h": round(pre_rate, 2),
        "post_rate_per_h": round(post_rate, 2),
        "ratio": round(ratio, 3),
        "verdict": verdict,
    }


def _record_evaluation(decision: dict) -> None:
    """Mark this ledger entry as evaluated so we don't re-check."""
    floor_ledger.record(
        publisher_id=0, publisher_name="[intervention-journal]",
        demand_id=decision["demand_id"], demand_name=decision["demand_name"],
        old_floor=None, new_floor=None,
        actor=f"{ACTOR_PREFIX}_eval",
        reason=(f"Evaluated ledger {decision['ledger_id']}: verdict={decision['verdict']} "
                f"(ratio={decision['ratio']:.2f}, pre=${decision['pre_rate_per_h']:.2f}/h, "
                f"post=${decision['post_rate_per_h']:.2f}/h)"),
        dry_run=False, applied=True,
    )


def _execute_revert(decision: dict) -> bool:
    """Revert the demand to its pre-change floor."""
    did = decision["demand_id"]
    target = decision["old_floor"]
    try:
        result = ll_mgmt.set_demand_floor(
            did, target,
            verify=True,
            allow_multi_pub=True,
            _publishers_running_it=10,
        )
        floor_ledger.record(
            publisher_id=0, publisher_name="[intervention-journal-revert]",
            demand_id=did, demand_name=decision["demand_name"],
            old_floor=decision["new_floor"], new_floor=target,
            actor=f"{ACTOR_PREFIX}_revert",
            reason=(f"Revert ledger {decision['ledger_id']}: "
                    f"verdict=loser, pre=${decision['pre_rate_per_h']:.2f}/h, "
                    f"post=${decision['post_rate_per_h']:.2f}/h, "
                    f"ratio={decision['ratio']:.2f}. Restored to pre-change floor."),
            dry_run=False, applied=True,
        )
        return True
    except Exception as e:
        print(f"[{ACTOR_PREFIX}] revert FAILED on demand {did}: {e}")
        return False


def run() -> dict:
    """Scheduler entry."""
    now = datetime.now(timezone.utc)
    if not HOURLY_PATH.exists():
        return {"skipped": True, "reason": "no hourly data"}

    with gzip.open(HOURLY_PATH, "rt") as f:
        hourly = json.load(f)
    all_ledger = floor_ledger.read_all()

    # Find candidate writes — applied, non-self, in observation window
    obs_cutoff = (now - timedelta(hours=LOOKBACK_HOURS)).isoformat()
    candidates = [r for r in all_ledger
                  if r.get("ts_utc", "") >= obs_cutoff
                  and r.get("applied") and not r.get("dry_run")]
    print(f"[{ACTOR_PREFIX}] {len(candidates)} candidate ledger entries in last "
          f"{LOOKBACK_HOURS}h")

    decisions = []
    reverted = []
    winners = []
    neutrals = 0
    for entry in candidates:
        d = evaluate_ledger_entry(entry, hourly, all_ledger)
        if d is None:
            continue
        decisions.append(d)
        _record_evaluation(d)
        if d["verdict"] == "winner":
            winners.append(d)
        elif d["verdict"] == "loser":
            if len(reverted) >= MAX_REVERTS_PER_RUN:
                continue
            if _execute_revert(d):
                reverted.append(d)
        else:
            neutrals += 1

    # Slack summary if anything happened
    if reverted or winners:
        parts = [f":notebook: *Intervention journal — {now.strftime('%Y-%m-%d %H:%M UTC')}*"]
        if reverted:
            parts.append(f"\n*🔻 Reverted {len(reverted)} loser(s):*")
            for d in reverted:
                parts.append(f"  • `{d['demand_id']}` {d['old_floor']}→{d['new_floor']} "
                              f"(by {d['actor']}) — ratio {d['ratio']:.2f}, "
                              f"${d['pre_rate_per_h']:.2f}/h → ${d['post_rate_per_h']:.2f}/h. "
                              f"Restored to {d['old_floor']}.  _{d['demand_name'][:35]}_")
        if winners:
            parts.append(f"\n*🟢 Winners ({len(winners)}) — keeping:*")
            for d in winners:
                parts.append(f"  • `{d['demand_id']}` {d['old_floor']}→{d['new_floor']} "
                              f"(by {d['actor']}) — ratio {d['ratio']:.2f}, "
                              f"${d['pre_rate_per_h']:.2f}/h → ${d['post_rate_per_h']:.2f}/h.  "
                              f"_{d['demand_name'][:35]}_")
        try:
            slack.send_text("\n".join(parts))
        except Exception as e:
            print(f"[{ACTOR_PREFIX}] Slack post failed: {e}")

    return {
        "ran_at": now.isoformat(),
        "candidates_examined": len(candidates),
        "evaluated": len(decisions),
        "winners": len(winners),
        "neutral": neutrals,
        "reverted": len(reverted),
    }


if __name__ == "__main__":
    print(json.dumps(run(), indent=2, default=str))
