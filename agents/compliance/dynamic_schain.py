"""
agents/compliance/dynamic_schain.py

Phase 4 dynamic schain audit — reads from
pgam_direct.compliance_schain_emissions_24h (populated by the hourly
ClickHouse → Postgres rollup running in pgam-direct/web's
/api/cron/schain-rollup).

This is the companion to validators/schain_static.py:
  - static  : audits LL-side config (supplyChainEnabled,
               dontAddSupplyChainNode) via the LL mgmt API
  - dynamic : audits the actual emitted schain from real bid traffic
               via the bidder-edge → Kafka → ClickHouse pipeline

Findings (severity rubric):

  schain.dynamic_incomplete_high_rate   CRITICAL
    >5 % of last 24h emissions for a publisher carry
    schain.complete=0 — DSPs treating it as untrusted supply.

  schain.dynamic_hop_violation_high_rate  CRITICAL
    >5 % of emissions carry hops>2 — Magnite filtering.

  schain.dynamic_incomplete_observed    HIGH
    Any non-zero complete=0 rate but below the high-rate threshold.
    Bleed signal worth eyeballing before it scales.

  schain.dynamic_hop_violation_observed HIGH
    Any non-zero hops>2 emissions but below the high-rate threshold.

Uses sentinel publisher_key '_dynamic_schain_pub:<id>' so findings
roll up separately from the static-config findings (which use
'_ll_pub:<id>'). Both are excluded from publisher compliance scoring.

Degrades cleanly when:
  - the emissions table doesn't exist yet (pre-migration)
  - it exists but has no rows (cron hasn't run yet, or no bid traffic)
"""
from __future__ import annotations

from dataclasses import dataclass

from core.neon import connect

from agents.compliance.validators.adstxt_universal import Finding


# Thresholds. Tunable via env in a follow-up; baselines tuned from a
# back-of-envelope read of the existing config_health_scanner's posture
# (auto-fixer flips False→True on revenue-earning entities; this
# validator catches the bleed before the auto-fixer reaches it).
HIGH_RATE_THRESHOLD = 0.05            # 5 % bad emissions → critical
ANY_OBSERVED_MIN_EMISSIONS = 100      # need at least N emissions to call it "observed"


@dataclass(frozen=True)
class DynamicSchainStats:
    skipped_reason: str | None
    publishers_seen: int
    findings_count: int


_LOAD_24H_SQL = """
SELECT publisher_id, supply_partner,
       emissions, complete, incomplete,
       hops_2, hops_gt_2, hops_max_seen,
       incomplete_rate, hop_violation_rate
FROM pgam_direct.compliance_schain_emissions_24h
WHERE emissions > 0
ORDER BY emissions DESC;
"""


def _publisher_sentinel(publisher_id: str | int) -> str:
    return f"_dynamic_schain_pub:{publisher_id}"


def _make_finding(
    publisher_id: int,
    *,
    check_id: str,
    severity: str,
    row: dict,
    fingerprint_extra: str = "",
) -> Finding:
    return Finding.make(
        publisher_key=_publisher_sentinel(publisher_id),
        check_id=check_id,
        severity=severity,
        detail={
            "publisher_id":           publisher_id,
            "supply_partner":         row.get("supply_partner"),
            "emissions_24h":          int(row.get("emissions") or 0),
            "incomplete_emissions":   int(row.get("incomplete") or 0),
            "hop_violations":         int(row.get("hops_gt_2") or 0),
            "hops_max_seen":          int(row.get("hops_max_seen") or 0),
            "incomplete_rate":        round(float(row.get("incomplete_rate") or 0), 4),
            "hop_violation_rate":     round(float(row.get("hop_violation_rate") or 0), 4),
            "auto_fix_owner":         "config_health_scanner",
        },
        fingerprint_extra=fingerprint_extra,
    )


def run_dynamic_schain_audit() -> tuple[DynamicSchainStats, list[Finding], list[str]]:
    """Pull 24h emissions roll-up and emit findings."""
    try:
        with connect() as conn:
            with conn.cursor() as cur:
                cur.execute(_LOAD_24H_SQL)
                cols = [c.name for c in cur.description]
                rows = [dict(zip(cols, r)) for r in cur.fetchall()]
    except Exception as exc:
        # Most likely: table doesn't exist yet (pre-migration), or web-
        # side cron hasn't started filling it.
        return (
            DynamicSchainStats(skipped_reason=f"query failed: {exc}",
                               publishers_seen=0, findings_count=0),
            [], [],
        )

    if not rows:
        return (
            DynamicSchainStats(skipped_reason="no emissions in trailing 24h",
                               publishers_seen=0, findings_count=0),
            [], [],
        )

    findings: list[Finding] = []
    sentinel_keys: set[str] = set()

    for row in rows:
        pid = int(row["publisher_id"])
        sentinel_keys.add(_publisher_sentinel(pid))
        emissions = int(row.get("emissions") or 0)
        incomplete = int(row.get("incomplete") or 0)
        hop_violations = int(row.get("hops_gt_2") or 0)
        inc_rate = float(row.get("incomplete_rate") or 0)
        hop_rate = float(row.get("hop_violation_rate") or 0)

        # 1. schain.complete = 0 rate
        if emissions >= ANY_OBSERVED_MIN_EMISSIONS and incomplete > 0:
            if inc_rate >= HIGH_RATE_THRESHOLD:
                findings.append(_make_finding(
                    pid,
                    check_id="schain.dynamic_incomplete_high_rate",
                    severity="critical",
                    row=row,
                    fingerprint_extra="incomplete",
                ))
            else:
                findings.append(_make_finding(
                    pid,
                    check_id="schain.dynamic_incomplete_observed",
                    severity="high",
                    row=row,
                    fingerprint_extra="incomplete",
                ))

        # 2. hop violations (>2 nodes)
        if emissions >= ANY_OBSERVED_MIN_EMISSIONS and hop_violations > 0:
            if hop_rate >= HIGH_RATE_THRESHOLD:
                findings.append(_make_finding(
                    pid,
                    check_id="schain.dynamic_hop_violation_high_rate",
                    severity="critical",
                    row=row,
                    fingerprint_extra="hop_violation",
                ))
            else:
                findings.append(_make_finding(
                    pid,
                    check_id="schain.dynamic_hop_violation_observed",
                    severity="high",
                    row=row,
                    fingerprint_extra="hop_violation",
                ))

    stats = DynamicSchainStats(
        skipped_reason=None,
        publishers_seen=len(sentinel_keys),
        findings_count=len(findings),
    )
    return stats, findings, sorted(sentinel_keys)
