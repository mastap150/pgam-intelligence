"""
agents/compliance/demand_detector.py

Detects newly-observed LL demand_partner names and demand_names that
don't classify to any SSP in ssp_registry. Both are operational
signals that someone needs to look at — either a new integration
worth knowing about, or a registry gap that's silently breaking the
per-SSP reseller-line check (Phase 2 / Phase 5 wouldn't audit
ads.txt for an SSP it doesn't recognise).

Findings produced:

  compliance.new_demand_observed             INFO (or HIGH if material rev)
    A demand_name never seen in any prior run. Material = >$50/7d.
    Auto-resolves on next run once it's been observed (no longer
    "new" by definition).

  compliance.demand_unmapped_to_ssp          MEDIUM (or HIGH if material rev)
    classify_demand_name() returned None. Either ssp_registry needs
    a pattern added (LL renamed the partner), or this is a genuinely
    new SSP we haven't onboarded. Auto-resolves when the demand
    classifies successfully on a future run (i.e. registry updated).

Sentinel publisher_key: `_demand:<demand_name>` — keeps findings
per-demand-name so the same auto-resolve logic applies.
"""
from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass

from core.api import fetch as ll_fetch, n_days_ago, sf, today
from core.neon import connect

from agents.compliance.ssp_registry import classify_demand_name
from agents.compliance.validators.adstxt_universal import Finding


_LOOKBACK_DAYS = 7

# Severity thresholds — info below, high above.
MATERIAL_REV_7D = 50.0


_LOAD_HISTORICAL_SQL = """
SELECT demand_name, first_seen_at, ssp_key
FROM pgam_direct.compliance_observed_demands;
"""

_UPSERT_SQL = """
INSERT INTO pgam_direct.compliance_observed_demands
    (demand_name, first_seen_at, last_seen_at, revenue_7d_latest,
     ssp_key, seen_count)
VALUES
    (%(demand_name)s, now(), now(), %(revenue)s, %(ssp_key)s, 1)
ON CONFLICT (demand_name) DO UPDATE SET
    last_seen_at      = now(),
    revenue_7d_latest = EXCLUDED.revenue_7d_latest,
    ssp_key           = EXCLUDED.ssp_key,
    seen_count        = pgam_direct.compliance_observed_demands.seen_count + 1;
"""


@dataclass(frozen=True)
class DemandDetectorStats:
    skipped_reason: str | None
    total_demands_seen: int
    new_demands: int
    unmapped_demands: int
    findings_count: int


def _sentinel(demand_name: str) -> str:
    return f"_demand:{demand_name}"


def run_demand_detector(
    lookback_days: int = _LOOKBACK_DAYS,
) -> tuple[DemandDetectorStats, list[Finding], list[str]]:
    """Pull current demand_names from LL, diff against history, emit findings."""
    end = today()
    start = n_days_ago(max(lookback_days - 1, 0))
    try:
        rows = ll_fetch("DEMAND_PARTNER",
                         ["GROSS_REVENUE", "IMPRESSIONS"], start, end)
    except Exception as exc:
        return (
            DemandDetectorStats(
                skipped_reason=f"LL fetch failed: {exc}",
                total_demands_seen=0, new_demands=0,
                unmapped_demands=0, findings_count=0,
            ),
            [], [],
        )

    # Aggregate per demand_name.
    current: dict[str, float] = defaultdict(float)
    for r in rows:
        name = (
            r.get("DEMAND_PARTNER_NAME") or r.get("DEMAND_PARTNER")
            or r.get("demand_partner") or r.get("demand_partner_name")
        )
        if not name:
            continue
        rev = sf(r.get("GROSS_REVENUE"))
        if rev <= 0:
            continue
        current[str(name)] += rev

    if not current:
        return (
            DemandDetectorStats(
                skipped_reason="no demand_partner rows in window",
                total_demands_seen=0, new_demands=0,
                unmapped_demands=0, findings_count=0,
            ),
            [], [],
        )

    # Load historical.
    try:
        with connect() as conn:
            with conn.cursor() as cur:
                cur.execute(_LOAD_HISTORICAL_SQL)
                historical = {
                    r[0]: {"first_seen_at": r[1], "ssp_key_prev": r[2]}
                    for r in cur.fetchall()
                }
    except Exception as exc:
        return (
            DemandDetectorStats(
                skipped_reason=f"history load failed: {exc}",
                total_demands_seen=len(current), new_demands=0,
                unmapped_demands=0, findings_count=0,
            ),
            [], [],
        )

    findings: list[Finding] = []
    sentinel_keys: list[str] = []
    new_count = 0
    unmapped_count = 0
    upsert_rows: list[dict] = []

    for name, rev in current.items():
        sentinel_keys.append(_sentinel(name))
        exp = classify_demand_name(name)
        ssp_key = exp.ssp_key if exp else None

        is_new = name not in historical

        if is_new:
            new_count += 1
            severity = "high" if rev >= MATERIAL_REV_7D else "info"
            findings.append(Finding.make(
                publisher_key=_sentinel(name),
                check_id="compliance.new_demand_observed",
                severity=severity,
                detail={
                    "demand_name":      name,
                    "revenue_7d":       round(rev, 2),
                    "classified_ssp":   ssp_key,
                    "note": ("Never observed in prior runs. Verify this is "
                             "a known/expected integration. Auto-resolves "
                             "once the demand_name persists into the next "
                             "run."),
                },
                fingerprint_extra="new",
            ))

        if ssp_key is None:
            unmapped_count += 1
            severity = "high" if rev >= MATERIAL_REV_7D else "medium"
            findings.append(Finding.make(
                publisher_key=_sentinel(name),
                check_id="compliance.demand_unmapped_to_ssp",
                severity=severity,
                detail={
                    "demand_name": name,
                    "revenue_7d":  round(rev, 2),
                    "fix": ("Either add a name pattern to "
                            "agents/compliance/ssp_registry.PHASE_2_SSP_"
                            "EXPECTATIONS for the existing SSP this maps "
                            "to, OR onboard a brand-new SSP entry with "
                            "its required ads.txt RESELLER line."),
                },
                fingerprint_extra="unmapped",
            ))

        upsert_rows.append({
            "demand_name": name,
            "revenue":     round(rev, 4),
            "ssp_key":     ssp_key,
        })

    # Persist current state (history + revenue snapshot).
    try:
        with connect() as conn:
            with conn.cursor() as cur:
                cur.executemany(_UPSERT_SQL, upsert_rows)
            conn.commit()
    except Exception as exc:
        print(f"[demand_detector] history upsert failed (non-fatal): {exc}")

    stats = DemandDetectorStats(
        skipped_reason=None,
        total_demands_seen=len(current),
        new_demands=new_count,
        unmapped_demands=unmapped_count,
        findings_count=len(findings),
    )
    return stats, findings, sorted(set(sentinel_keys))
