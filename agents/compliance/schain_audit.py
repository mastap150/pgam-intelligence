"""
agents/compliance/schain_audit.py

Phase 4 orchestrator: pull demands + publishers + revenue, run the
validators, return findings.

Side-effect free apart from the LL management API reads. The runner
owns Finding persistence and resolve_cleared() bookkeeping.

Degrades cleanly on three failure modes:
  - LL_UI_EMAIL / LL_UI_PASSWORD missing → skip with log
  - Revenue snapshot file missing       → skip with log
  - Any individual LL call exception    → propagated to runner, which
                                          logs and continues without
                                          schain findings this run.
"""
from __future__ import annotations

import gzip
import json
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path

from core import ll_mgmt

from agents.compliance.validators.adstxt_universal import Finding
from agents.compliance.validators.schain_static import (
    DEFAULT_MIN_REV_7D,
    all_sentinel_keys,
    audit_demands,
    audit_publishers,
)


# Reuses the same hourly snapshot config_health_scanner reads from.
# Single source of truth for "what was earning recently".
_HOURLY_PATH = Path(__file__).parent.parent.parent / "data" / "hourly_pub_demand.json.gz"
_LOOKBACK_DAYS = 7


@dataclass(frozen=True)
class SchainAuditResult:
    skipped_reason: str | None
    demands_audited: int
    publishers_audited: int
    findings: list[Finding]
    sentinel_keys: list[str]


def _load_revenue_maps(lookback_days: int = _LOOKBACK_DAYS) -> tuple[dict, dict, bool]:
    """Mirror of config_health_scanner._load_revenue_maps. Returns (demand_rev, pub_rev, ok)."""
    if not _HOURLY_PATH.exists():
        return {}, {}, False
    try:
        with gzip.open(_HOURLY_PATH, "rt") as f:
            rows = json.load(f)
    except Exception:
        return {}, {}, False

    cutoff = (date.today() - timedelta(days=lookback_days)).isoformat()
    drev: dict = defaultdict(float)
    prev: dict = defaultdict(float)
    for r in rows:
        if not isinstance(r, dict):
            continue
        if str(r.get("DATE", "")) < cutoff:
            continue
        rev = float(r.get("GROSS_REVENUE", 0) or 0)
        drev[int(r.get("DEMAND_ID", 0) or 0)] += rev
        prev[int(r.get("PUBLISHER_ID", 0) or 0)] += rev
    return dict(drev), dict(prev), True


def run_schain_audit(min_rev_7d: float = DEFAULT_MIN_REV_7D) -> SchainAuditResult:
    """End-to-end schain audit. Returns findings + sentinel keys for the runner."""
    if not ll_mgmt.ll_mgmt_configured():
        return SchainAuditResult(
            skipped_reason="LL_UI credentials not configured",
            demands_audited=0, publishers_audited=0,
            findings=[], sentinel_keys=[],
        )

    demand_rev, pub_rev, rev_ok = _load_revenue_maps()
    if not rev_ok:
        return SchainAuditResult(
            skipped_reason=f"revenue snapshot missing at {_HOURLY_PATH}",
            demands_audited=0, publishers_audited=0,
            findings=[], sentinel_keys=[],
        )

    demands = ll_mgmt.get_demands()
    publishers = ll_mgmt.get_publishers()

    findings: list[Finding] = []
    findings.extend(audit_demands(demands, demand_rev, min_rev_7d=min_rev_7d))
    findings.extend(audit_publishers(publishers, pub_rev, min_rev_7d=min_rev_7d))

    return SchainAuditResult(
        skipped_reason=None,
        demands_audited=len(demands),
        publishers_audited=len(publishers),
        findings=findings,
        sentinel_keys=all_sentinel_keys(demands, publishers),
    )
