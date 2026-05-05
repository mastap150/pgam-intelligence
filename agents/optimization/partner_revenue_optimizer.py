"""
agents/optimization/partner_revenue_optimizer.py

Per-partner floor-lift optimizer.

Targets a small explicit whitelist of partner-pubs (AppStock, Start.IO Video
Magnite, Start.IO Display Magnite, PubNative In-App Magnite) and lifts the
floor on partner-UNIQUE low-yield demands so the freed impressions can flow
to the higher-eCPM unique demands already wired on the same publisher.

Why only unique demands
-----------------------
RON / shared demands are wired to ≥3 publishers; a floor change on one of
them propagates to every other publisher using that demand. We do NOT touch
those — only demands wired to exactly one publisher (the target partner).
This means every change has zero cross-partner blast radius by construction.

Why this isn't a re-incarnation of floor_optimizer
--------------------------------------------------
``scripts/floor_optimizer.py`` was unregistered 2026-04-25 (PR #16) because
its kill switch was being bypassed and writes were landing every 2h
without oversight. This agent is different in three concrete ways:

  1. Partner whitelist — only the four pubs listed in PARTNER_PUBS are
     touched. floor_optimizer scanned everything.
  2. Unique-only — never touches a demand wired to >1 publisher. Even if
     someone widens the whitelist, RON demands stay safe.
  3. Strict caps — MAX_CHANGES_PER_RUN=3, MAX_CHANGES_PER_PARTNER_DAILY=1.
     At most 4 floor changes per day across the whole agent.

Plus: kill switch defaults OFF (``PARTNER_OPTIMIZER_ENABLED=1`` required to
write), every change goes through ``set_demand_floor()`` so it inherits the
contract-floor clamp + ledger + verify, and the existing
``auto_revert_harmful`` (every 4h) will revert any change that causes >20%
demand-level revenue drop in 48h.

Cadence
-------
Every 4 hours, aligned with ``auto_revert_harmful`` and ``revenue_guardian``.
That gives the safety nets one full cycle to evaluate each write before the
next one lands.

Test #1 (the "PubNative Sovrn dilution" hypothesis from the partner deep-dive):
The 4 Sovrn-PubNative banner demands earn $0.09–$0.26 eCPM consuming ~429K
imps/wk, while Magnite-PubNative-In-App demands sit at $1.50+ eCPM on a
fraction of the volume. Lifting the Sovrn floors should re-route some of
that imp pool to higher-yield demand. If revenue drops >20%,
auto_revert_harmful undoes it; if it lifts revenue, the change sticks.

Operation
---------
1. Load 7d revenue + impressions per (pub, demand) from the hourly store.
2. Build demand_id → set(pub_ids) wiring map (across the whole fleet — needed
   to identify which demands are unique vs shared).
3. For each PARTNER_PUB:
     - Compute partner pub-wide imps total
     - Find candidate demands wired ONLY to that pub with:
         eCPM_7d < LOW_YIELD_ECPM_CEILING AND
         imps_7d > LOW_YIELD_MIN_IMP_SHARE * pub_total_imps AND
         rev_7d  > LOW_YIELD_MIN_REV
     - For each candidate, propose a floor at NEW_FLOOR_ON_LIFT
     - Skip if current floor is already >= proposed (no-op)
     - Skip if a change for this partner already happened today (per-partner cap)
     - Skip if global MAX_CHANGES_PER_RUN reached
4. Apply via set_demand_floor; ledger with actor partner_revenue_optimizer_<date>.
5. Slack post per change + summary digest.
"""
from __future__ import annotations

import gzip
import json
import os
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone

from core import floor_ledger, ll_mgmt, slack

ACTOR_PREFIX = "partner_revenue_optimizer"

# Hard kill switch — must be set to "1" for writes to land. Defaults OFF so
# the agent can be deployed and observed in dry-run before going live.
ENABLED_ENV_VAR = "PARTNER_OPTIMIZER_ENABLED"

# Partner publisher whitelist. Adding a new partner = explicit code change here.
# (We never want this list growing unsupervised — see floor_optimizer note above.)
PARTNER_PUBS: dict[int, str] = {
    290115377: "AppStock",
    290115375: "Start.IO Video Magnite",
    290115374: "Start.IO Display Magnite",
    290115373: "PubNative In-App Magnite",
}

# Low-yield thresholds for the "lift floor on partner-unique low-yield demand"
# rule. Tuned conservatively from the 2026-04-26 partner deep-dive.
#
# Tuning history:
# - 2026-04-26 initial: ECPM_CEILING=$0.50, IMP_SHARE=5%, MIN_REV=$5
# - 2026-05-05: IMP_SHARE 5% → 1% after pub-wide volume on PubNative collapsed
#   7×, leaving the remaining Sovrn-PubNative siblings (e.g. demand 777) at
#   ~2% imp share — high enough to drag yield, low enough that the 5%
#   threshold excluded them. Dropping to 1% keeps the agent useful on
#   smaller-volume tail demands.
LOW_YIELD_ECPM_CEILING = 0.50      # only consider demands earning < $0.50 eCPM
LOW_YIELD_MIN_IMP_SHARE = 0.01     # ...consuming > 1% of partner pub's imps
LOW_YIELD_MIN_REV = 5.0            # ...with > $5/7d (skip dust)
NEW_FLOOR_ON_LIFT = 0.50           # lift floor to $0.50

# Blast-radius caps.
MAX_CHANGES_PER_RUN = 3
MAX_CHANGES_PER_PARTNER_DAILY = 1
LOOKBACK_DAYS_FOR_DAILY_CAP = 1

_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
HOURLY_PATH = os.path.join(_REPO_ROOT, "data", "hourly_pub_demand.json.gz")


def _load_pub_demand_7d() -> tuple[dict, dict]:
    """Return (pub_demand_funnel, demand_pubs) maps from the 7d hourly store.

    pub_demand_funnel[(pid, did)] = {imps, rev, name}
    demand_pubs[did] = set(pids that earned $ or imps in last 7d)
    """
    if not os.path.exists(HOURLY_PATH):
        return {}, {}
    with gzip.open(HOURLY_PATH, "rt") as f:
        rows = json.load(f)
    cutoff = (date.today() - timedelta(days=7)).isoformat()
    funnel: dict = defaultdict(lambda: {"imps": 0, "rev": 0.0, "name": ""})
    demand_pubs: dict = defaultdict(set)
    for r in rows:
        if str(r.get("DATE", "")) < cutoff:
            continue
        pid = r.get("PUBLISHER_ID")
        did = r.get("DEMAND_ID")
        if pid is None or did is None:
            continue
        rev = float(r.get("GROSS_REVENUE", 0) or 0)
        imps = int(r.get("IMPRESSIONS", 0) or 0)
        if rev <= 0 and imps <= 0:
            continue
        pid, did = int(pid), int(did)
        f = funnel[(pid, did)]
        f["imps"] += imps
        f["rev"] += rev
        f["name"] = r.get("DEMAND_NAME", "") or f["name"]
        demand_pubs[did].add(pid)
    return dict(funnel), dict(demand_pubs)


def _changes_today_per_partner() -> dict[int, int]:
    """Count partner_revenue_optimizer ledger entries per partner pub today."""
    cutoff = datetime.now(timezone.utc) - timedelta(days=LOOKBACK_DAYS_FOR_DAILY_CAP)
    cutoff_iso = cutoff.isoformat().replace("+00:00", "Z")
    counts: dict[int, int] = defaultdict(int)
    for row in floor_ledger.read_all():
        if not (row.get("actor", "") or "").startswith(ACTOR_PREFIX):
            continue
        if row.get("dry_run") or not row.get("applied"):
            continue
        if row.get("ts_utc", "") < cutoff_iso:
            continue
        pid = row.get("publisher_id")
        if pid is not None:
            counts[int(pid)] += 1
    return dict(counts)


def _build_candidates(funnel: dict, demand_pubs: dict) -> list[dict]:
    """Return all (pub × demand) candidates that match the low-yield rule.

    A candidate is partner-unique (demand wired to exactly 1 pub == this
    partner pub), low-yield, and consuming material imp share. Sorted by
    estimated $-upside desc.
    """
    # Per-pub total imps for share calculations
    pub_total_imps: dict[int, int] = defaultdict(int)
    for (pid, _did), f in funnel.items():
        if pid in PARTNER_PUBS:
            pub_total_imps[pid] += f["imps"]

    candidates: list[dict] = []
    for (pid, did), f in funnel.items():
        if pid not in PARTNER_PUBS:
            continue
        wired_pub_count = len(demand_pubs.get(did, set()))
        if wired_pub_count != 1:
            continue  # SHARED / RON — never touch
        imps = f["imps"]; rev = f["rev"]; name = f["name"]
        if imps == 0:
            continue
        ecpm = rev / imps * 1000
        if ecpm >= LOW_YIELD_ECPM_CEILING:
            continue
        share = imps / pub_total_imps[pid] if pub_total_imps[pid] else 0
        if share < LOW_YIELD_MIN_IMP_SHARE:
            continue
        if rev < LOW_YIELD_MIN_REV:
            continue
        # Estimated upside: if these imps re-route to median partner-unique
        # eCPM (~$1.50 in our case) and we recover even 50%, that's the gain.
        # Conservative back-of-envelope; real result depends on auction.
        estimated_upside = max(0.0, (1.50 * 0.5 - ecpm) * imps / 1000)
        candidates.append({
            "publisher_id": pid,
            "publisher_name": PARTNER_PUBS[pid],
            "demand_id": did,
            "demand_name": name,
            "imps_7d": imps,
            "rev_7d": round(rev, 2),
            "ecpm_7d": round(ecpm, 4),
            "imp_share": round(share, 4),
            "proposed_floor": NEW_FLOOR_ON_LIFT,
            "estimated_upside_7d": round(estimated_upside, 2),
        })
    candidates.sort(key=lambda c: c["estimated_upside_7d"], reverse=True)
    return candidates


def _slack_change_msg(c: dict, old_floor: float | None, dry_run: bool) -> str:
    prefix = ":test_tube:" if dry_run else ":arrow_up:"
    tag = "DRY-RUN " if dry_run else ""
    return (
        f"{prefix} *{tag}Partner floor lift — {c['publisher_name']}*\n"
        f"• demand `{c['demand_id']}` ({(c['demand_name'] or '')[:55]})\n"
        f"• floor `${old_floor if old_floor is not None else 0:.2f}` → "
        f"`${c['proposed_floor']:.2f}` (eCPM ${c['ecpm_7d']:.2f}, "
        f"{c['imps_7d']:,} imps/7d, {c['imp_share']*100:.0f}% of pub)\n"
        f"• estimated 7d upside: ${c['estimated_upside_7d']:.0f} "
        f"(rolls back automatically if revenue drops >20%)"
    )


def optimize() -> dict:
    enabled = os.environ.get(ENABLED_ENV_VAR, "").strip() == "1"
    ll_dry_run = os.environ.get("LL_DRY_RUN", "false").strip().lower() == "true"
    effective_dry = ll_dry_run or not enabled

    actor = f"{ACTOR_PREFIX}_{datetime.now(timezone.utc).strftime('%Y%m%d')}"
    print(f"[{ACTOR_PREFIX}] enabled={enabled} ll_dry_run={ll_dry_run} → effective_dry={effective_dry}")

    funnel, demand_pubs = _load_pub_demand_7d()
    if not funnel:
        print(f"[{ACTOR_PREFIX}] hourly data missing/empty — bail")
        return {"applied": [], "skipped": [], "candidates": 0}

    candidates = _build_candidates(funnel, demand_pubs)
    print(f"[{ACTOR_PREFIX}] {len(candidates)} candidate floor lifts (across {len(PARTNER_PUBS)} partners)")

    daily_counts = _changes_today_per_partner()
    print(f"[{ACTOR_PREFIX}] changes today by partner: {daily_counts}")

    applied: list[dict] = []
    skipped: list[dict] = []
    partners_touched_this_run: set[int] = set()

    for c in candidates:
        if len(applied) >= MAX_CHANGES_PER_RUN:
            skipped.append({**c, "skip_reason": "global_run_cap"}); continue
        pid = c["publisher_id"]
        if pid in partners_touched_this_run:
            skipped.append({**c, "skip_reason": "already_touched_this_run"}); continue
        if daily_counts.get(pid, 0) >= MAX_CHANGES_PER_PARTNER_DAILY:
            skipped.append({**c, "skip_reason": "partner_daily_cap"}); continue

        # Pull current floor; skip no-ops
        try:
            demand = next((d for d in ll_mgmt.get_demands(include_archived=True)
                           if d.get("id") == c["demand_id"]), None)
            old_floor = demand.get("minBidFloor") if demand else None
            old_floor_val = float(old_floor) if old_floor is not None else 0.0
        except Exception as e:
            print(f"[{ACTOR_PREFIX}] could not fetch demand {c['demand_id']}: {e}")
            skipped.append({**c, "skip_reason": "demand_fetch_failed"}); continue

        if old_floor_val >= c["proposed_floor"]:
            skipped.append({**c, "skip_reason": "current_floor_already_at_or_above"}); continue

        try:
            if effective_dry:
                print(f"[{ACTOR_PREFIX}] DRY-RUN would lift demand={c['demand_id']} "
                      f"floor=${old_floor_val:.2f}→${c['proposed_floor']:.2f}")
                # Ledger the dry-run too — useful for diff vs apply mode
                floor_ledger.record(
                    publisher_id=pid, publisher_name=c["publisher_name"],
                    demand_id=c["demand_id"], demand_name=c["demand_name"],
                    old_floor=old_floor_val, new_floor=c["proposed_floor"],
                    actor=actor,
                    reason=(f"Partner low-yield floor lift: eCPM ${c['ecpm_7d']:.2f}, "
                            f"{c['imp_share']*100:.0f}% of pub imps, est upside ${c['estimated_upside_7d']:.0f}/7d"),
                    dry_run=True, applied=False,
                )
            else:
                ll_mgmt.set_demand_floor(
                    c["demand_id"], c["proposed_floor"],
                    verify=True, allow_multi_pub=False,  # belt+suspenders RON guard
                )
                floor_ledger.record(
                    publisher_id=pid, publisher_name=c["publisher_name"],
                    demand_id=c["demand_id"], demand_name=c["demand_name"],
                    old_floor=old_floor_val, new_floor=c["proposed_floor"],
                    actor=actor,
                    reason=(f"Partner low-yield floor lift: eCPM ${c['ecpm_7d']:.2f}, "
                            f"{c['imp_share']*100:.0f}% of pub imps, est upside ${c['estimated_upside_7d']:.0f}/7d"),
                    dry_run=False, applied=True,
                )

            applied.append({**c, "old_floor": old_floor_val, "applied_dry": effective_dry})
            partners_touched_this_run.add(pid)
            try:
                slack.send_text(_slack_change_msg(c, old_floor_val, dry_run=effective_dry))
            except Exception as e:
                print(f"[{ACTOR_PREFIX}] Slack post failed: {e}")
        except Exception as e:
            print(f"[{ACTOR_PREFIX}] FAILED to lift demand={c['demand_id']}: {e}")
            skipped.append({**c, "skip_reason": f"write_failed: {str(e)[:120]}"})

    return {
        "ran_at": datetime.now(timezone.utc).isoformat(),
        "actor": actor,
        "enabled": enabled, "ll_dry_run": ll_dry_run, "effective_dry": effective_dry,
        "candidates": len(candidates),
        "applied": applied,
        "skipped": skipped,
    }


def run() -> dict:
    return optimize()


if __name__ == "__main__":
    print(json.dumps(run(), indent=2, default=str))
