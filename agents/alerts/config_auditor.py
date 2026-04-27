"""
agents/alerts/config_auditor.py

Daily LL configuration auditor — FLAG-ONLY companion.

Walks the live state of LiveRamp (LL) and flags rules, wirings, and floors
that look off and probably need attention. This is the "are we set up
correctly?" check that complements the per-domain agents
(contract_floor_sentry, floor_gap, dead_demand, etc.) — they each watch a
specific failure mode; this one is the broad sweep.

Calibration
-----------
Most demand-level fields ($0 floor, orphan-but-paused, etc.) are LEGITIMATE
defaults in LL — per-publisher floors do the real work, and demands sit
wired-but-paused all the time during testing. Flagging raw config state
generates 700+ findings on a real fleet (we tried).

So every "is this misconfigured?" check is gated on REVENUE EARNED, using
the same data file as ``config_health_scanner`` (data/hourly_pub_demand.json.gz):
we only flag demands that have earned >= MIN_DEMAND_REV_7D in the last 7
days. A zero floor on a dormant test demand is fine; a zero floor on a
demand that earned $5K last week is leaving margin on the table.

Relationship to config_health_scanner
-------------------------------------
Disjoint sibling. ``agents/optimization/config_health_scanner.py`` runs at
06:30 ET and AUTO-FIXES known-good config defaults (supplyChainEnabled,
lurlEnabled, qpsLimit util). This auditor runs at 06:45 ET and FLAGS issues
that need human judgment (floor anomalies, orphan/zombie wirings,
contract-min breaches). No field overlap — do not add checks for
supplyChainEnabled / lurlEnabled / qpsLimit here, and do not add
auto-remediation for floors / wirings here.

TB note: TB has its own sentry stack (``tb_contract_floor_sentry``,
``revenue_guardian``) as of 2026-04-26. We do not duplicate TB checks here.

Per memory (2026-04-18): PGAM is LL-only — TB is dormant. The TB section
therefore inverts the usual logic: ANY signs of TB activity (reachable creds,
active inventories, non-zero floors) are flagged as anomalies. If TB auth
fails outright, that's the expected steady state and we report "dormant".

What it checks (LL only)
------------------------
  P1  contract floor below minimum   (defense-in-depth on contract_floor_sentry)
  P2  $0 / null floor AND demand earned >= $50 in last 7d   (real waste)
  P2  outlier high floor (>$15) AND demand earned in last 7d (likely typo)
  P3  active revenue-earning demand with no publisher wirings (orphan)
  P3  paused but recently-revenue-earning demand still wired (zombie)

Output
------
- JSON report → data/config_audit_report.json
- Slack digest (deduped daily) summarising counts; only posts if findings
- stdout summary line

Manual run:
    python -m agents.alerts.config_auditor
"""
from __future__ import annotations

import gzip
import json
import os
import re
import traceback
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone

from core import ll_mgmt, slack
from core.ll_mgmt import PROTECTED_FLOOR_MINIMUMS

ACTOR = "config_auditor"
_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
REPORT_PATH = os.path.join(_REPO_ROOT, "data", "config_audit_report.json")
HOURLY_PATH = os.path.join(_REPO_ROOT, "data", "hourly_pub_demand.json.gz")

# Revenue gate — share threshold with config_health_scanner so the two
# agents have a consistent definition of "demand worth touching".
MIN_DEMAND_REV_7D = 50.0

# Outlier threshold — anything above this on a non-CTV demand is almost
# certainly a typo (we've never legitimately set a floor this high).
HIGH_FLOOR_THRESHOLD = 15.00

# Wrapper-side / prebid-server demands set their floors in the wrapper or
# prebid config, NOT in the LL minBidFloor field. The LL field on these is
# essentially decorative — flagging "$0 floor" on them generates noise.
# This regex matches integration patterns we know are wrapper-side.
WRAPPER_SIDE_NAME_PATTERN = re.compile(
    r"prebid server|bidmachine|blueseax|magnite\s*-\s*smaato|"
    r"magnite\s*-\s*illumin|unruly|verve\s*-\s*ron|onetag|sovrn|"
    r"pubmatic\s*-\s*ron|pubmatic\s+ron",
    re.IGNORECASE,
)


def _is_wrapper_side_demand(name: str) -> bool:
    """True if the demand's floor lives in wrapper/prebid config, not LL."""
    return bool(WRAPPER_SIDE_NAME_PATTERN.search(name or ""))


# ── LL checks ───────────────────────────────────────────────────────────────


def _contract_minimum_for(name: str) -> float | None:
    name_lower = (name or "").lower()
    for tokens, min_floor in PROTECTED_FLOOR_MINIMUMS:
        if any(tok in name_lower for tok in tokens):
            return min_floor
    return None


def _load_demand_rev_7d() -> dict[int, float]:
    """Load demand_id → revenue($) for the last 7 days from the hourly store.

    Same data file config_health_scanner uses, so the two agents share a
    consistent definition of "demand worth touching". Returns an empty dict
    if the file is missing — every revenue-gated check then becomes a no-op,
    which is the right failure mode (don't flag based on stale state).
    """
    if not os.path.exists(HOURLY_PATH):
        print(f"[{ACTOR}] WARNING: {HOURLY_PATH} missing — revenue gating disabled, all checks no-op")
        return {}
    try:
        with gzip.open(HOURLY_PATH, "rt") as f:
            rows = json.load(f)
    except Exception as e:
        print(f"[{ACTOR}] WARNING: could not read {HOURLY_PATH}: {e}")
        return {}
    cutoff = (date.today() - timedelta(days=7)).isoformat()
    rev: dict[int, float] = defaultdict(float)
    for r in rows:
        if str(r.get("DATE", "")) < cutoff:
            continue
        did = r.get("DEMAND_ID")
        if did is None:
            continue
        try:
            rev[int(did)] += float(r.get("GROSS_REVENUE", 0) or 0)
        except (TypeError, ValueError):
            continue
    return dict(rev)


def _audit_ll() -> dict:
    findings: list[dict] = []
    demands = ll_mgmt.get_demands(include_archived=False)
    publishers = ll_mgmt.get_publishers(include_archived=False)
    demand_rev = _load_demand_rev_7d()

    # Build demand-id → set(publisher_ids) wiring map by walking each publisher's
    # biddingpreferences. One pass, O(P × wirings_per_pub).
    wiring_map: dict[int, set[int]] = {}
    for p in publishers:
        pid = p.get("id")
        for pref in p.get("biddingpreferences", []):
            for item in pref.get("value", []):
                did = item.get("id")
                if did is None:
                    continue
                wiring_map.setdefault(int(did), set()).add(int(pid))

    for d in demands:
        did = d.get("id")
        name = d.get("name") or ""
        floor = d.get("minBidFloor")
        status = d.get("status")  # 1 = active, 2 = paused

        try:
            floor_val = float(floor) if floor is not None else None
        except (TypeError, ValueError):
            floor_val = None

        contract_min = _contract_minimum_for(name)
        wired_pubs = wiring_map.get(int(did), set()) if did is not None else set()
        rev_7d = demand_rev.get(int(did), 0.0) if did is not None else 0.0
        is_revenue_earning = rev_7d >= MIN_DEMAND_REV_7D

        # P1 — contract floor below minimum (always check, regardless of revenue)
        if contract_min is not None and (floor_val is None or floor_val < contract_min):
            findings.append({
                "severity": "P1",
                "kind": "contract_floor_below_min",
                "demand_id": did, "demand_name": name,
                "live_floor": floor_val, "expected_min": contract_min,
                "rev_7d": round(rev_7d, 2),
                "fix": "contract_floor_sentry should restore on next hourly run; "
                       "if it persists for >2h, investigate write-path.",
            })
            continue  # don't double-count the same demand on floor sanity

        # P2 — $0 / null floor on a REVENUE-EARNING active DIRECT demand.
        # Wrapper-side / prebid-server demands set floors in wrapper config;
        # the LL field is decorative on those. Skip them to avoid noise.
        if (
            status == 1
            and is_revenue_earning
            and (floor_val is None or floor_val == 0)
            and not _is_wrapper_side_demand(name)
        ):
            findings.append({
                "severity": "P2",
                "kind": "zero_floor_direct_demand",
                "demand_id": did, "demand_name": name,
                "live_floor": floor_val, "rev_7d": round(rev_7d, 2),
                "fix": "Direct demand earning $%.0f/wk with no LL floor — verify "
                       "win rate first; if >30%%, leave it. If <30%%, consider a small floor." % rev_7d,
            })

        # P2 — outlier high floor on a revenue-earning demand
        # (a $35 floor on a dead test demand is fine; on a live one it blocks fill)
        if floor_val is not None and floor_val > HIGH_FLOOR_THRESHOLD and is_revenue_earning:
            findings.append({
                "severity": "P2",
                "kind": "outlier_high_floor",
                "demand_id": did, "demand_name": name,
                "live_floor": floor_val, "threshold": HIGH_FLOOR_THRESHOLD,
                "rev_7d": round(rev_7d, 2),
                "fix": "Verify intent — likely typo (e.g. $35 instead of $3.50).",
            })

        # P3 — revenue-earning active demand with no publisher wirings
        # (a paid demand with zero wirings means revenue happens via some path
        # we don't see — worth investigating)
        if status == 1 and is_revenue_earning and not wired_pubs:
            findings.append({
                "severity": "P3",
                "kind": "orphan_revenue_demand",
                "demand_id": did, "demand_name": name,
                "rev_7d": round(rev_7d, 2),
                "fix": "Earning revenue but no publisher wirings visible — verify wiring source.",
            })

        # P3 — paused demand that was earning recently still wired
        # (a long-paused demand wired to pubs is just stale config; one paused
        # *this week* that earned recently is a likely accidental pause)
        if status == 2 and is_revenue_earning and wired_pubs:
            findings.append({
                "severity": "P3",
                "kind": "zombie_wiring_recent_revenue",
                "demand_id": did, "demand_name": name,
                "wired_publisher_count": len(wired_pubs),
                "rev_7d": round(rev_7d, 2),
                "fix": "Demand earned $%.0f in last 7d but is now paused — accidental pause? "
                       "auto_unpause should catch it; if it persists, investigate." % rev_7d,
            })

    return {
        "demands_scanned": len(demands),
        "publishers_scanned": len(publishers),
        "revenue_earning_demands": sum(1 for v in demand_rev.values() if v >= MIN_DEMAND_REV_7D),
        "findings": findings,
    }


# ── Slack digest ────────────────────────────────────────────────────────────


def _slack_dedup_key() -> str:
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    return f"config_auditor:digest:{today}"


def _format_digest(ll: dict) -> str | None:
    """Build a single Slack message summarising findings. None = nothing to say."""
    all_findings = ll["findings"]
    if not all_findings:
        return None

    by_sev: dict[str, list[dict]] = {"P1": [], "P2": [], "P3": []}
    for f in all_findings:
        by_sev.setdefault(f["severity"], []).append(f)

    header_emoji = ":rotating_light:" if by_sev["P1"] else (
        ":warning:" if by_sev["P2"] else ":information_source:"
    )
    lines = [
        f"{header_emoji} *LL config audit* — "
        f"{len(by_sev['P1'])} P1 / {len(by_sev['P2'])} P2 / {len(by_sev['P3'])} P3 "
        f"(scanned {ll['demands_scanned']} demands, "
        f"{ll.get('revenue_earning_demands', 0)} revenue-earning)"
    ]

    # Show up to 6 of each severity to keep the message readable.
    for sev in ("P1", "P2", "P3"):
        items = by_sev[sev]
        if not items:
            continue
        # Sort each tier by 7d revenue desc — biggest dollar impact first.
        items_sorted = sorted(items, key=lambda f: f.get("rev_7d", 0), reverse=True)
        lines.append(f"\n*{sev}* ({len(items)}):")
        for f in items_sorted[:6]:
            ident = f.get("demand_id") or f.get("kind")
            name = (f.get("demand_name") or "")[:40]
            rev = f.get("rev_7d")
            rev_str = f" ${rev:.0f}/7d" if rev else ""
            lines.append(f"• `{f['kind']}` — `{ident}` {name}{rev_str}".rstrip())
        if len(items) > 6:
            lines.append(f"  …and {len(items) - 6} more")

    lines.append(f"\nFull report: `{REPORT_PATH}`")
    return "\n".join(lines)


# ── Entry points ────────────────────────────────────────────────────────────


def audit() -> dict:
    print(f"[{ACTOR}] starting LL config audit (revenue-gated)")

    try:
        ll_result = _audit_ll()
    except Exception as e:
        print(f"[{ACTOR}] LL audit failed: {e}")
        traceback.print_exc()
        ll_result = {"demands_scanned": 0, "publishers_scanned": 0,
                     "findings": [], "error": str(e)[:200]}

    report = {
        "ran_at": datetime.now(timezone.utc).isoformat(),
        "actor": ACTOR,
        "ll": ll_result,
    }

    # Persist the JSON report.
    try:
        os.makedirs(os.path.dirname(REPORT_PATH), exist_ok=True)
        with open(REPORT_PATH, "w") as f:
            json.dump(report, f, indent=2, default=str)
    except Exception as e:
        print(f"[{ACTOR}] failed to write report: {e}")

    # Slack digest, deduped daily.
    digest = _format_digest(ll_result)
    if digest:
        try:
            if not slack.already_sent_today(_slack_dedup_key()):
                slack.send_text(digest)
                slack.mark_sent(_slack_dedup_key())
        except Exception as e:
            print(f"[{ACTOR}] Slack post failed: {e}")

    total = len(ll_result["findings"])
    print(
        f"[{ACTOR}] done — {total} findings; "
        f"revenue-earning demands: {ll_result.get('revenue_earning_demands', 0)}; "
        f"report at {REPORT_PATH}"
    )
    return report


def run() -> dict:
    return audit()


if __name__ == "__main__":
    result = run()
    print(json.dumps(result, indent=2, default=str))
