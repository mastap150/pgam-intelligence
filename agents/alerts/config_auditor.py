"""
agents/alerts/config_auditor.py

Daily LL + TB configuration auditor.

Walks the live state of LiveRamp (LL) and TechBid (TB) and flags rules,
wirings, and floors that look off and probably need attention. This is the
"are we set up correctly?" check that complements the per-domain agents
(contract_floor_sentry, floor_gap, dead_demand, etc.) — they each watch a
specific failure mode; this one is the broad sweep.

Per memory (2026-04-18): PGAM is LL-only — TB is dormant. The TB section
therefore inverts the usual logic: ANY signs of TB activity (reachable creds,
active inventories, non-zero floors) are flagged as anomalies. If TB auth
fails outright, that's the expected steady state and we report "dormant".

What it checks
--------------
LL — active stack:
  P1  contract floor below minimum   (defense-in-depth on contract_floor_sentry)
  P2  $0 / null floor on active demand
  P2  outlier high floor (>$15) — likely typo, blocks fill
  P3  active demand with no publisher wirings (orphan)
  P3  paused demand (status=2) still wired to active publishers (zombie wiring)

TB — should be dormant:
  P1  TB API reachable AND any active inventory/placement/non-zero floor
  (P3 if reachable but everything is zeroed out → still worth a look)

Output
------
- JSON report → data/config_audit_report.json
- Slack digest (deduped daily) summarising counts; only posts if findings
- stdout summary line

Manual run:
    python -m agents.alerts.config_auditor
"""
from __future__ import annotations

import json
import os
import traceback
from datetime import datetime, timezone

from core import ll_mgmt, slack
from core.ll_mgmt import PROTECTED_FLOOR_MINIMUMS

ACTOR = "config_auditor"
REPORT_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
    "data", "config_audit_report.json",
)

# Outlier threshold — anything above this on a non-CTV demand is almost
# certainly a typo (we've never legitimately set a floor this high).
HIGH_FLOOR_THRESHOLD = 15.00


# ── LL checks ───────────────────────────────────────────────────────────────


def _contract_minimum_for(name: str) -> float | None:
    name_lower = (name or "").lower()
    for tokens, min_floor in PROTECTED_FLOOR_MINIMUMS:
        if any(tok in name_lower for tok in tokens):
            return min_floor
    return None


def _audit_ll() -> dict:
    findings: list[dict] = []
    demands = ll_mgmt.get_demands(include_archived=False)
    publishers = ll_mgmt.get_publishers(include_archived=False)

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

        # P1 — contract floor below minimum
        if contract_min is not None and (floor_val is None or floor_val < contract_min):
            findings.append({
                "severity": "P1",
                "kind": "contract_floor_below_min",
                "demand_id": did, "demand_name": name,
                "live_floor": floor_val, "expected_min": contract_min,
                "fix": "contract_floor_sentry should restore on next hourly run; "
                       "if it persists for >2h, investigate write-path.",
            })
            continue  # don't double-count the same demand on floor sanity

        # P2 — $0 / null floor on active demand (skip paused demands)
        if status == 1 and (floor_val is None or floor_val == 0):
            findings.append({
                "severity": "P2",
                "kind": "zero_floor_active_demand",
                "demand_id": did, "demand_name": name,
                "live_floor": floor_val,
                "fix": "Set a real floor — $0 lets any bid win regardless of margin.",
            })

        # P2 — outlier high floor
        if floor_val is not None and floor_val > HIGH_FLOOR_THRESHOLD:
            findings.append({
                "severity": "P2",
                "kind": "outlier_high_floor",
                "demand_id": did, "demand_name": name,
                "live_floor": floor_val, "threshold": HIGH_FLOOR_THRESHOLD,
                "fix": "Verify intent — likely typo (e.g. $35 instead of $3.50).",
            })

        # P3 — active demand with no publisher wirings (orphan)
        if status == 1 and not wired_pubs:
            findings.append({
                "severity": "P3",
                "kind": "orphan_active_demand",
                "demand_id": did, "demand_name": name,
                "fix": "Either wire to ≥1 publisher or pause the demand.",
            })

        # P3 — paused demand still wired (zombie wiring)
        if status == 2 and wired_pubs:
            findings.append({
                "severity": "P3",
                "kind": "zombie_wiring_paused_demand",
                "demand_id": did, "demand_name": name,
                "wired_publisher_count": len(wired_pubs),
                "fix": "Unwire from publishers or re-activate the demand.",
            })

    return {
        "demands_scanned": len(demands),
        "publishers_scanned": len(publishers),
        "findings": findings,
    }


# ── TB shadow check ─────────────────────────────────────────────────────────


def _audit_tb() -> dict:
    """TB should be dormant. Flag any active state."""
    findings: list[dict] = []
    try:
        from core import tb_mgmt
        # If creds are bad / TB endpoint is dead, this raises — which is the
        # expected dormant state per memory ("PGAM is LL-only — TB inactive").
        inventories = tb_mgmt.list_inventories()
    except Exception as e:
        return {
            "reachable": False,
            "status": "dormant_as_expected",
            "error": str(e)[:200],
            "findings": [],
        }

    active_inv = [i for i in inventories if i.get("status")]
    placements_all: list[dict] = []
    try:
        # list_placements with no inventory filter returns the whole account
        placements_all = tb_mgmt.list_placements()
    except Exception as e:
        placements_all = []
        findings.append({
            "severity": "P3",
            "kind": "tb_placements_unreadable",
            "detail": str(e)[:200],
        })

    active_placements = [p for p in placements_all if p.get("status")]
    nonzero_floor_placements = [
        p for p in placements_all
        if (p.get("price") or 0) and float(p["price"]) > 0
    ]

    if active_inv or active_placements or nonzero_floor_placements:
        findings.append({
            "severity": "P1",
            "kind": "tb_unexpectedly_live",
            "active_inventories": len(active_inv),
            "active_placements": len(active_placements),
            "placements_with_nonzero_floor": len(nonzero_floor_placements),
            "fix": "TB should be dormant. Either disable the active TB state "
                   "or update memory if PGAM is intentionally re-enabling TB.",
        })
    elif inventories or placements_all:
        # API reachable but everything is zeroed out — still note it.
        findings.append({
            "severity": "P3",
            "kind": "tb_reachable_but_idle",
            "inventory_count": len(inventories),
            "placement_count": len(placements_all),
            "fix": "TB API is reachable but no active state. Consider revoking "
                   "TB credentials if truly retired.",
        })

    return {
        "reachable": True,
        "inventory_count": len(inventories),
        "placement_count": len(placements_all),
        "findings": findings,
    }


# ── Slack digest ────────────────────────────────────────────────────────────


def _slack_dedup_key() -> str:
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    return f"config_auditor:digest:{today}"


def _format_digest(ll: dict, tb: dict) -> str | None:
    """Build a single Slack message summarising findings. None = nothing to say."""
    all_findings = ll["findings"] + tb["findings"]
    if not all_findings:
        return None

    by_sev: dict[str, list[dict]] = {"P1": [], "P2": [], "P3": []}
    for f in all_findings:
        by_sev.setdefault(f["severity"], []).append(f)

    header_emoji = ":rotating_light:" if by_sev["P1"] else (
        ":warning:" if by_sev["P2"] else ":information_source:"
    )
    lines = [
        f"{header_emoji} *Config audit* — "
        f"{len(by_sev['P1'])} P1 / {len(by_sev['P2'])} P2 / {len(by_sev['P3'])} P3 "
        f"(LL demands scanned: {ll['demands_scanned']}, "
        f"TB: {'reachable' if tb.get('reachable') else 'dormant'})"
    ]

    # Show up to 6 of each severity to keep the message readable.
    for sev in ("P1", "P2", "P3"):
        items = by_sev[sev]
        if not items:
            continue
        lines.append(f"\n*{sev}* ({len(items)}):")
        for f in items[:6]:
            ident = f.get("demand_id") or f.get("kind")
            name = (f.get("demand_name") or "")[:50]
            lines.append(f"• `{f['kind']}` — `{ident}` {name}".rstrip())
        if len(items) > 6:
            lines.append(f"  …and {len(items) - 6} more")

    lines.append(f"\nFull report: `{REPORT_PATH}`")
    return "\n".join(lines)


# ── Entry points ────────────────────────────────────────────────────────────


def audit() -> dict:
    print(f"[{ACTOR}] starting LL + TB config audit")

    try:
        ll_result = _audit_ll()
    except Exception as e:
        print(f"[{ACTOR}] LL audit failed: {e}")
        traceback.print_exc()
        ll_result = {"demands_scanned": 0, "publishers_scanned": 0,
                     "findings": [], "error": str(e)[:200]}

    try:
        tb_result = _audit_tb()
    except Exception as e:
        print(f"[{ACTOR}] TB audit failed: {e}")
        tb_result = {"reachable": False, "status": "audit_error",
                     "error": str(e)[:200], "findings": []}

    report = {
        "ran_at": datetime.now(timezone.utc).isoformat(),
        "actor": ACTOR,
        "ll": ll_result,
        "tb": tb_result,
    }

    # Persist the JSON report.
    try:
        os.makedirs(os.path.dirname(REPORT_PATH), exist_ok=True)
        with open(REPORT_PATH, "w") as f:
            json.dump(report, f, indent=2, default=str)
    except Exception as e:
        print(f"[{ACTOR}] failed to write report: {e}")

    # Slack digest, deduped daily.
    digest = _format_digest(ll_result, tb_result)
    if digest:
        try:
            if not slack.already_sent_today(_slack_dedup_key()):
                slack.send_text(digest)
                slack.mark_sent(_slack_dedup_key())
        except Exception as e:
            print(f"[{ACTOR}] Slack post failed: {e}")

    total = len(ll_result["findings"]) + len(tb_result["findings"])
    print(
        f"[{ACTOR}] done — {total} findings "
        f"(LL: {len(ll_result['findings'])}, TB: {len(tb_result['findings'])}); "
        f"report at {REPORT_PATH}"
    )
    return report


def run() -> dict:
    return audit()


if __name__ == "__main__":
    result = run()
    print(json.dumps(result, indent=2, default=str))
