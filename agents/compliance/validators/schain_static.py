"""
agents/compliance/validators/schain_static.py

Phase 4 validator: static supply-chain configuration on LL demands +
publishers.

This is **defense-in-depth audit, not auto-fix**.
agents/optimization/config_health_scanner.py already auto-flips:
    - supplyChainEnabled  False → True   on revenue-earning demands
    - dontAddSupplyChainNode False → True on revenue-earning publishers
…subject to MIN_REV_7D=$50 and MAX_AUTOFIX_PER_CATEGORY=5/run caps.

This validator re-checks the state AFTER the auto-fixer has run and
surfaces anything still wrong: backlog (auto-fixer caps), regressions
(someone toggled back via UI), or below-threshold entities that the
auto-fixer skipped but are still observed earning.

Findings produced:

  schain.demand_supplychain_disabled   CRITICAL
    A revenue-earning LL demand has supplyChainEnabled = False. DSPs
    throttle or block bids on inventory without a verified schain. Real
    revenue loss — usually a single-digit-to-low-double-digit % bid drop.

  schain.pub_node_injection_enabled    HIGH
    A revenue-earning LL publisher has dontAddSupplyChainNode = False.
    LL will append pgamrtb.com as a 3rd node, breaching Magnite's max-
    2-node policy and getting filtered. Note: only flags explicit False;
    None / missing means "use default" and we don't second-guess that.

Sentinel publisher_keys:
    _ll_demand:<id>   for demand-level findings
    _ll_pub:<id>      for LL-publisher-level findings
Both are excluded from publisher compliance scoring (see scoring.py).
"""
from __future__ import annotations

from agents.compliance.validators.adstxt_universal import Finding


# Minimum trailing-7d revenue for an entity to be eligible for a finding.
# Mirrors config_health_scanner.MIN_*_REV_7D = $50 so the two systems
# agree on what counts as "live".
DEFAULT_MIN_REV_7D = 50.0


def _demand_sentinel(demand_id: int | str) -> str:
    return f"_ll_demand:{demand_id}"


def _pub_sentinel(pub_id: int | str) -> str:
    return f"_ll_pub:{pub_id}"


def audit_demands(
    demands: list[dict],
    demand_rev_7d: dict[int, float],
    *,
    min_rev_7d: float = DEFAULT_MIN_REV_7D,
) -> list[Finding]:
    """Flag revenue-earning demands with supplyChainEnabled explicitly False.

    Treats missing supplyChainEnabled as True per LL convention (matches
    config_health_scanner.check_demand_supplychain — `d.get(..., True)`).
    """
    findings: list[Finding] = []
    for d in demands:
        did = d.get("id")
        if did is None:
            continue
        # status: 1=active, 2=paused — both are eligible. Archived/deleted skipped upstream.
        if d.get("status") not in (1, 2):
            continue
        rev_7d = float(demand_rev_7d.get(int(did), 0) or 0)
        if rev_7d < min_rev_7d:
            continue
        if d.get("supplyChainEnabled", True) is False:
            findings.append(Finding.make(
                publisher_key=_demand_sentinel(did),
                check_id="schain.demand_supplychain_disabled",
                severity="critical",
                detail={
                    "demand_id":     did,
                    "demand_name":   d.get("name") or "",
                    "revenue_7d":    round(rev_7d, 2),
                    "auto_fix_owner": "config_health_scanner.check_demand_supplychain",
                },
            ))
    return findings


def audit_publishers(
    publishers: list[dict],
    pub_rev_7d: dict[int, float],
    *,
    min_rev_7d: float = DEFAULT_MIN_REV_7D,
) -> list[Finding]:
    """Flag revenue-earning publishers with dontAddSupplyChainNode explicitly False.

    Treats missing field as "use default" — does NOT flag. This matches
    config_health_scanner.check_pub_dont_add_supplychain_node which only
    flips explicit False.
    """
    findings: list[Finding] = []
    for p in publishers:
        pid = p.get("id")
        if pid is None:
            continue
        if p.get("status") not in (1, 2):
            continue
        # Skip test / copy publishers (mirrors auto-fixer's filter).
        name = (p.get("name") or "").upper()
        if "TEST" in name or name.startswith("COPY -"):
            continue
        rev_7d = float(pub_rev_7d.get(int(pid), 0) or 0)
        if rev_7d < min_rev_7d:
            continue
        if p.get("dontAddSupplyChainNode") is False:
            findings.append(Finding.make(
                publisher_key=_pub_sentinel(pid),
                check_id="schain.pub_node_injection_enabled",
                severity="high",
                detail={
                    "publisher_id":   pid,
                    "publisher_name": p.get("name") or "",
                    "revenue_7d":     round(rev_7d, 2),
                    "auto_fix_owner": "config_health_scanner.check_pub_dont_add_supplychain_node",
                    "consequence":    "LL appends pgamrtb.com as 3rd schain node — Magnite filters.",
                },
            ))
    return findings


def all_sentinel_keys(demands: list[dict], publishers: list[dict]) -> list[str]:
    """Sentinel keys for every active demand + publisher considered this run.

    Returned to the runner so resolve_cleared() can auto-resolve schain
    findings that were open last run but pass this run.
    """
    out: list[str] = []
    out.extend(_demand_sentinel(d["id"]) for d in demands
               if d.get("id") is not None and d.get("status") in (1, 2))
    out.extend(_pub_sentinel(p["id"]) for p in publishers
               if p.get("id") is not None and p.get("status") in (1, 2))
    return out
