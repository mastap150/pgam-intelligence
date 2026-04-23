"""
Proposer — posts optimizer proposals to Slack for review, and applies the
ones the operator green-lights via CLI.

Flow (week 1 — supervised):
  1. Optimizer writes proposals.json with ranked actions + CIs.
  2. This module reads that, dedupes against recently-posted proposals,
     and posts a compact Slack message: top 10 with "apply cmd".
  3. Operator reviews, then runs:
        python -m intelligence.proposer --apply prop_xyz prop_abc
     to actually write the floors via ll_mgmt and record to floor_ledger.

Flow (week 2+ — autonomous, flip AUTO_APPLY_ENABLED):
  Same pipeline, but proposals with confidence=="high" AND net_lift >
  AUTO_APPLY_MIN_LIFT are applied automatically; everything else still
  goes to Slack for human review.

Every applied proposal:
  - re-checks holdout (paranoid)
  - pushes the change via llm._put
  - writes a floor_ledger entry tagged actor="optimizer.<proposal_id>"
  - schedules a post-write verify the next time the verifier runs

Proposals that aren't applied within PROPOSAL_TTL_HOURS are pruned.
"""
from __future__ import annotations

import argparse
import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from core import floor_ledger, ll_mgmt, slack
from intelligence import holdout, quarantine

DATA_DIR = Path(__file__).parent.parent / "data"
PROPOSALS_PATH = DATA_DIR / "proposals.json"
POSTED_PATH = DATA_DIR / "proposals_posted.json"   # slack dedupe memory

AUTO_APPLY_ENABLED = os.environ.get("PGAM_OPTIMIZER_AUTO_APPLY", "0") == "1"
AUTO_APPLY_MIN_LIFT = 25.0      # weekly $
AUTO_APPLY_MIN_CONFIDENCE = "high"
SLACK_TOP_N = 10
PROPOSAL_TTL_HOURS = 48


def _load_proposals() -> dict:
    if not PROPOSALS_PATH.exists():
        return {"proposals": []}
    return json.loads(PROPOSALS_PATH.read_text())


def _load_posted() -> dict:
    if not POSTED_PATH.exists():
        return {}
    return json.loads(POSTED_PATH.read_text())


def _save_posted(d: dict) -> None:
    POSTED_PATH.write_text(json.dumps(d, indent=2))


def _fmt_floor(f) -> str:
    if f is None:
        return "null"
    return f"${float(f):.2f}"


# ────────────────────────────────────────────────────────────────────────────
# Posting
# ────────────────────────────────────────────────────────────────────────────

def post_to_slack(top_n: int = SLACK_TOP_N) -> dict:
    data = _load_proposals()
    props = data.get("proposals", [])
    if not props:
        slack.send_text("🤖 *Floor optimizer*: no proposals today — model says current floors are near-optimal.")
        return {"posted": 0}

    posted = _load_posted()
    fresh = [p for p in props if p["id"] not in posted]
    if not fresh:
        return {"posted": 0, "note": "all proposals already posted"}

    shown = fresh[:top_n]
    total_lift = sum(p["expected_weekly_net_lift"] for p in shown)

    mode = "AUTO" if AUTO_APPLY_ENABLED else "SUPERVISED"
    mode_note = (
        "⚠️ auto-apply ON — high-confidence proposals will write without review"
        if AUTO_APPLY_ENABLED else
        "🔒 SUPERVISED — nothing applies without human approval"
    )
    header = (f"🤖 *Floor optimizer* — {len(props)} proposals "
              f"(posting top {len(shown)}, total E[net] = +${total_lift:,.0f}/wk)\n"
              f"{mode_note}")

    lines = []
    for p in shown:
        cur = _fmt_floor(p["current_floor"])
        new = _fmt_floor(p["proposed_floor"])
        ci = f"[{p['ci_low_net']:+.0f} … {p['ci_high_net']:+.0f}]"
        lines.append(
            f"`{p['id']}`  *{p['publisher_name'][:25]}* / _{p['demand_name'][:30]}_\n"
            f"   {cur} → {new}   +${p['expected_weekly_net_lift']:.0f}/wk {ci}  "
            f"[{p['confidence']}]  _{p['reason']}_"
        )

    approve_cmd = "python -m intelligence.proposer --apply " + " ".join(p["id"] for p in shown)
    body = "\n\n".join([header, "\n".join(lines),
                        f"```\n# approve all shown:\n{approve_cmd}\n```"])
    slack.send_blocks(
        [{"type": "section", "text": {"type": "mrkdwn", "text": body}}],
        text=f"Floor optimizer: {len(shown)} proposals",
    )

    for p in shown:
        posted[p["id"]] = {"posted_utc": datetime.now(timezone.utc).isoformat(),
                           "snapshot": p}
    _save_posted(posted)
    return {"posted": len(shown), "ids": [p["id"] for p in shown], "total_lift": total_lift}


# ────────────────────────────────────────────────────────────────────────────
# Apply
# ────────────────────────────────────────────────────────────────────────────

def apply_one(proposal: dict, *, dry_run: bool = False) -> dict:
    """Apply a single proposal via ll_mgmt.set_demand_floor — the only write
    path that actually persists (see its docstring for the Apr-18 revert-bug
    postmortem)."""
    pub_id = int(proposal["publisher_id"])
    did = int(proposal["demand_id"])

    # paranoid re-check (single-pub proposals only — portfolio proposals carry
    # publisher_id=0 and have already done their own cross-pub holdout check
    # inside portfolio_optimizer.generate).
    if pub_id != 0:
        if holdout.is_tuple_held_out(pub_id, did):
            return {"id": proposal["id"], "applied": False, "reason": "holdout_or_inactive_reblocked"}
        if quarantine.is_in_quarantine(pub_id, did):
            return {"id": proposal["id"], "applied": False, "reason": "quarantine_reblocked"}

    # Multi-pub guard — refuse unless the proposal explicitly acknowledges
    # that it's been aggregated across all pubs running this demand.
    n_pubs = proposal.get("demand_runs_on_n_pubs", 1)
    allow_multi = bool(proposal.get("multi_pub_acknowledged", False))
    if n_pubs > 1 and not allow_multi:
        return {"id": proposal["id"], "applied": False,
                "reason": f"multi_pub_unacknowledged:{n_pubs}_pubs"}

    new_floor = float(proposal["proposed_floor"])
    try:
        r = ll_mgmt.set_demand_floor(
            demand_id=did,
            new_floor=new_floor,
            verify=True,
            dry_run=dry_run,
            allow_multi_pub=allow_multi,
            _publishers_running_it=n_pubs,
        )
    except Exception as e:
        return {"id": proposal["id"], "applied": False, "reason": f"write_failed:{e}"}

    if dry_run or r.get("dry_run"):
        return {"id": proposal["id"], "applied": False, "reason": "dry_run",
                "new_floor": new_floor}
    if r.get("no_change"):
        return {"id": proposal["id"], "applied": False, "reason": "no_change",
                "floor": r.get("floor")}

    old_floor = r.get("old_floor")
    floor_ledger.record(
        publisher_id=pub_id, demand_id=did,
        old_floor=old_floor, new_floor=new_floor,
        actor=f"optimizer.{proposal['id']}",
        reason=proposal.get("reason", ""),
        publisher_name=proposal.get("publisher_name", ""),
        demand_name=proposal.get("demand_name", ""),
        dry_run=False, applied=True,
        source_log="data/proposals.json",
    )
    print(f"✓ applied+verified demand_id={did}  "
          f"{_fmt_floor(old_floor)} → ${new_floor:.2f}  (live={r.get('live_floor_after')})")
    return {"id": proposal["id"], "applied": True,
            "old_floor": old_floor, "new_floor": new_floor,
            "verified": r.get("verified", False)}


def apply_ids(ids: list[str], *, dry_run: bool = False) -> list[dict]:
    data = _load_proposals()
    by_id = {p["id"]: p for p in data.get("proposals", [])}
    results = []
    for pid in ids:
        p = by_id.get(pid)
        if not p:
            results.append({"id": pid, "applied": False, "reason": "not_in_current_proposals"})
            continue
        results.append(apply_one(p, dry_run=dry_run))
    return results


def auto_apply() -> list[dict]:
    """Apply proposals that clear the autonomy bar. Safe no-op when
    AUTO_APPLY_ENABLED is false — in that case, nothing ever qualifies."""
    if not AUTO_APPLY_ENABLED:
        return []
    data = _load_proposals()
    qualifying = [
        p for p in data.get("proposals", [])
        if p["confidence"] == AUTO_APPLY_MIN_CONFIDENCE
        and p["expected_weekly_net_lift"] >= AUTO_APPLY_MIN_LIFT
        and p["ci_low_net"] > 0
    ]
    return [apply_one(p, dry_run=False) for p in qualifying]


# ────────────────────────────────────────────────────────────────────────────
# Prune stale
# ────────────────────────────────────────────────────────────────────────────

def prune_stale() -> int:
    posted = _load_posted()
    cutoff = datetime.now(timezone.utc) - timedelta(hours=PROPOSAL_TTL_HOURS)
    kept = {pid: rec for pid, rec in posted.items()
            if datetime.fromisoformat(rec["posted_utc"]) > cutoff}
    pruned = len(posted) - len(kept)
    if pruned:
        _save_posted(kept)
    return pruned


def run() -> dict:
    """Scheduler entry: post to Slack, auto-apply the high-confidence tail,
    prune stale memory."""
    posted = post_to_slack()
    auto = auto_apply()
    pruned = prune_stale()
    return {"posted": posted, "auto_applied": auto, "pruned": pruned}


def main():
    ap = argparse.ArgumentParser()
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--post", action="store_true", help="post proposals to Slack")
    g.add_argument("--apply", nargs="+", metavar="PROPOSAL_ID")
    g.add_argument("--auto", action="store_true",
                   help="apply any proposals clearing the autonomy bar")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    if args.post:
        print(json.dumps(post_to_slack(), indent=2))
    elif args.apply:
        results = apply_ids(args.apply, dry_run=args.dry_run)
        print(json.dumps(results, indent=2, default=str))
    elif args.auto:
        results = auto_apply()
        print(json.dumps(results, indent=2, default=str))


if __name__ == "__main__":
    main()
