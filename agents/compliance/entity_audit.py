"""
agents/compliance/entity_audit.py

Phase 5 orchestrator — per-app + per-domain compliance audit.

Flow:
  1. Build entity universe (top N by 7d revenue) from
     ll_daily_publisher_{domain,bundle}_demand. Persist snapshot to
     compliance_supply_entities.
  2. For each entity with a resolvable audit_host: crawl ads.txt
     (domain) or app-ads.txt (bundle).
  3. Run the tiered universal DIRECT-line validator using the
     publisher's seller_id from PGAM sellers.json.
  4. For each entity × active SSP, run the conditional reseller-line
     validator (Phase 2 logic, applied per-entity instead of per-partner).
  5. Bundles without a resolved dev_domain raise a
     compliance.bundle_dev_domain_unresolved info finding so they're
     visible but not noisy.

Findings use publisher_key = "dom:<domain>" or "app:<bundle>" so they
don't collide with the Phase 1 partner-level findings.
"""
from __future__ import annotations

import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field

from core import ll_mgmt

from agents.compliance.crawlers.adstxt import (
    AdsTxtFetch, fetch_adstxt, fetch_adstxt_merged,
)
from agents.compliance.crawlers.sellersjson import fetch_pgam_sellers_json
from agents.compliance.entity_universe import (
    DEFAULT_TOP_N,
    Entity,
    UniverseStats,
    build_entity_universe,
    persist_entity_universe,
)
from agents.compliance.observed_monetization import ObservedRow
from agents.compliance.ssp_registry import get_expectation
from agents.compliance.validators.adstxt_resellers import (
    validate_resellers_for_publisher,
)
from agents.compliance.validators.adstxt_universal import Finding
from agents.compliance.validators.seller_id_tier import (
    build_pgam_seat_registry,
    validate_universal_direct_tiered,
)


DEFAULT_RATE_HZ = 2.0
DEFAULT_WORKERS = 6

# Materiality tiers — entity revenue drives severity calibration.
# Without this, every missing-RESELLER line on a $0.30/7d app fires
# CRITICAL and floods the digest. Tiers are deliberately wide because
# the operational difference between "$2/7d" and "$8/7d" is noise; the
# real signal is "is this material revenue or pocket change?".
TIER_MATERIAL_USD  = 500.0   # critical: real money flowing through unauthorized path
TIER_HIGH_USD      = 50.0    # high:     non-trivial revenue
TIER_MEDIUM_USD    = 1.0     # medium:   above-noise floor
# Below TIER_MEDIUM_USD: info-only (audit log; doesn't surface in digest body).

# Per-entity reseller-line check floor. Default 0.0 = audit every
# entity in the universe regardless of revenue, so the report fully
# covers low-revenue inventory and we have a clean compliance posture
# across the board (not just the top earners). Override the env var if
# the resulting noise floor becomes a problem.
PHASE5_RESELLER_MIN_REV_7D = float(
    os.environ.get("PGAM_COMPLIANCE_PHASE5_RESELLER_MIN_REV", "0.0")
)


@dataclass(frozen=True)
class EntityAuditResult:
    universe_stats: UniverseStats
    findings: list[Finding]
    sentinel_keys: list[str]
    fetches: list[AdsTxtFetch]
    skipped_reason: str | None = None
    supply_partners: list[dict] | None = None        # [{id, name}]
    # The full entity universe + parsed ads.txt fetches keyed by entity_key,
    # exposed so the runner can build the per-(entity × SSP) audit matrix
    # without re-crawling. Empty dict if Phase 5 was skipped.
    entities: list[Entity] = field(default_factory=list)
    fetches_by_entity: dict[str, AdsTxtFetch | None] = field(default_factory=dict)
    pgam_seat_registry: dict[str, dict] = field(default_factory=dict)


def _revenue_tier(rev_7d: float) -> str:
    """Map entity revenue → materiality tier name. Drives severity downgrade."""
    if rev_7d >= TIER_MATERIAL_USD:
        return "material"
    if rev_7d >= TIER_HIGH_USD:
        return "high"
    if rev_7d >= TIER_MEDIUM_USD:
        return "medium"
    return "trace"


def _calibrate_severity(base: str, rev_7d: float) -> str:
    """Downgrade a finding's severity for low-revenue entities.

    Rule: a CRITICAL finding on a $0.30/7d app isn't critical — it's
    hygiene. We keep the same check_id (so the dashboard still tracks
    it) but flag it at a severity that matches its impact.

    Trace-tier findings used to collapse to 'info', which the digest
    doesn't render — that silently hid every low-revenue anomaly from
    the report. We now downgrade trace-tier findings to 'medium'
    instead so they surface in the digest's Medium section. Their
    impact is small but visibility is the point: the user wants the
    audit to cover every entity in the universe, not just the top
    earners, so we can demonstrate full compliance.

    Mapping:
        base=critical → material:critical, high:high,   medium:medium, trace:medium
        base=high     → material:high,     high:high,   medium:medium, trace:medium
        base=medium   → material:medium,   high:medium, medium:medium, trace:medium
        base=info     → info (untouched)
    """
    tier = _revenue_tier(rev_7d)
    if base == "info":
        return "info"
    if tier == "trace":
        return "medium"
    if tier == "medium":
        return "medium" if base in ("critical", "high", "medium") else "info"
    if tier == "high":
        return "high" if base == "critical" else base
    return base  # material → keep base


def _resolve_supply_partners() -> list[dict]:
    """Pull active LL supply partners (publishers) for the universe filter.

    The 12 entries shown at https://ui.pgamrtb.com/suppliers map 1:1 to
    ll_mgmt.get_publishers(). status=1 == Active in LL; status=2 ==
    Paused / Stopped (e.g. "Test Supply Partner") which we skip.
    """
    pubs = ll_mgmt.get_publishers(include_archived=False)
    return [
        {"id": str(p.get("id")), "name": p.get("name") or ""}
        for p in pubs
        if p.get("status") == 1 and p.get("id") is not None
    ]


def _crawl(entity: Entity) -> AdsTxtFetch | None:
    """Fetch ads.txt + app-ads.txt for one entity; return merged AdsTxtFetch.

    Uses fetch_adstxt_merged so an entity whose ads.txt is empty but
    app-ads.txt has 8000 lines (typical LL-classified-as-domain app
    publisher like aigames.ae) still gets validated against the lines
    declared in the right file. Cascade includes HTTP-fallback +
    browser-UA + parent-domain retries.
    """
    if not entity.audit_host:
        return None
    return fetch_adstxt_merged(
        entity.entity_key,
        entity.audit_host,
        use_cache=True,
    )


def _enrich_and_calibrate(
    findings: list[Finding],
    entity: Entity,
) -> list[Finding]:
    """Downgrade severities by entity revenue tier + embed revenue/tier in detail.

    Returns NEW Finding objects (frozen dataclass) so callers can pass them
    through unchanged. Adds `revenue_7d` and `materiality_tier` keys so the
    digest can revenue-rank without rejoining to the universe table.
    """
    out: list[Finding] = []
    tier = _revenue_tier(entity.revenue_7d)
    for f in findings:
        new_sev = _calibrate_severity(f.severity, entity.revenue_7d)
        new_detail = dict(f.detail)
        new_detail.setdefault("revenue_7d", round(entity.revenue_7d, 2))
        new_detail.setdefault("materiality_tier", tier)
        if entity.ll_publisher_name:
            new_detail.setdefault("ll_publisher_name", entity.ll_publisher_name)
        out.append(Finding(
            publisher_key=f.publisher_key,
            category=f.category,
            check_id=f.check_id,
            severity=new_sev,
            fingerprint=f.fingerprint,
            detail=new_detail,
        ))
    return out


def _validate_entity(
    entity: Entity,
    fetch: AdsTxtFetch | None,
    pgam_seat_registry: dict[str, dict],
) -> list[Finding]:
    findings: list[Finding] = []

    # Unresolvable bundle — info-level finding so the dashboard surfaces
    # the gap but doesn't drown the digest.
    if fetch is None:
        findings.append(Finding.make(
            publisher_key=entity.entity_key,
            check_id="compliance.bundle_dev_domain_unresolved",
            severity="info",
            detail={
                "bundle":     entity.entity_value,
                "publisher":  entity.ll_publisher_name,
                "revenue_7d": round(entity.revenue_7d, 2),
                "materiality_tier": _revenue_tier(entity.revenue_7d),
                "consequence": ("app-ads.txt host unknown — extend "
                                "agents/enrichment/app_name_enrichment to "
                                "capture iTunes sellerUrl into "
                                "app_metadata.dev_domain."),
            },
        ))
        return findings

    # Tiered universal DIRECT-line check.
    findings.extend(validate_universal_direct_tiered(
        publisher_key=entity.entity_key,
        expected_seller_id=entity.expected_seller_id,
        fetch=fetch,
        pgam_seat_registry=pgam_seat_registry,
    ))

    # Conditional reseller-line check — per-entity, not per-partner.
    # Skip below the materiality floor: a missing reseller line on a
    # $0.30/7d app earns one finding per active SSP (~10 SSPs = 10
    # findings) and contributes nothing actionable. We still log the
    # universal-DIRECT check above because that's about the path itself,
    # not this window's revenue.
    if (fetch.http_status == 200
            and entity.active_ssps
            and entity.revenue_7d >= PHASE5_RESELLER_MIN_REV_7D):
        obs_rows: list[ObservedRow] = []
        for ssp_key in entity.active_ssps:
            exp = get_expectation(ssp_key)
            if exp is None:
                continue
            obs_rows.append(ObservedRow(
                publisher_key=entity.entity_key,
                ssp_key=ssp_key,
                ssp_domain=exp.ads_txt_domain,
                revenue_usd=0.0,         # individual breakdown lives elsewhere
                impressions=0,
                demand_count=0,
                demand_names=(),
            ))
        findings.extend(validate_resellers_for_publisher(
            entity.entity_key, fetch, obs_rows,
        ))

    return _enrich_and_calibrate(findings, entity)


def run_entity_audit(
    top_n: int | None = None,
    rate_hz: float = DEFAULT_RATE_HZ,
    workers: int = DEFAULT_WORKERS,
) -> EntityAuditResult:
    # Top-N cap defaults to None (audit every entity under every active
    # supply partner). Set PGAM_COMPLIANCE_PHASE5_TOP_N for a safety cap
    # during initial rollout.
    env_top_n = os.environ.get("PGAM_COMPLIANCE_PHASE5_TOP_N")
    if top_n is None and env_top_n and env_top_n.isdigit():
        top_n = int(env_top_n)

    # Resolve the active LL supply partners. Without LL_UI creds we
    # cannot scope the audit correctly, so we degrade to a logged skip
    # rather than silently auditing the wrong universe.
    if not ll_mgmt.ll_mgmt_configured():
        return EntityAuditResult(
            universe_stats=UniverseStats(0, 0, 0, 0, 0),
            findings=[], sentinel_keys=[], fetches=[],
            skipped_reason="LL_UI credentials not configured",
            supply_partners=[],
        )

    try:
        partners = _resolve_supply_partners()
    except Exception as exc:
        return EntityAuditResult(
            universe_stats=UniverseStats(0, 0, 0, 0, 0),
            findings=[], sentinel_keys=[], fetches=[],
            skipped_reason=f"ll_mgmt.get_publishers failed: {exc}",
            supply_partners=[],
        )

    partner_ids = {p["id"] for p in partners}
    if not partner_ids:
        return EntityAuditResult(
            universe_stats=UniverseStats(0, 0, 0, 0, 0),
            findings=[], sentinel_keys=[], fetches=[],
            skipped_reason="No active LL supply partners found",
            supply_partners=[],
        )

    entities, stats = build_entity_universe(
        top_n=top_n, partner_filter=partner_ids,
    )
    persist_entity_universe(entities)

    # Pull PGAM sellers.json once for the tier registry.
    sellers_payload = fetch_pgam_sellers_json()
    registry = build_pgam_seat_registry(sellers_payload)

    # Parallel crawl with global rate cap. Same shape as the Phase 1
    # crawler in runner.py — keeps memory low on the 512MB Render box.
    fetches: dict[str, AdsTxtFetch] = {}
    import time
    min_interval = 1.0 / max(rate_hz, 0.1)
    next_slot = [time.monotonic()]

    def _gated_crawl(entity: Entity) -> tuple[Entity, AdsTxtFetch | None]:
        # Skip the gate for unresolvable bundles — no HTTP to slow down.
        if not entity.audit_host:
            return entity, None
        wait_until = next_slot[0]
        now = time.monotonic()
        if now < wait_until:
            time.sleep(wait_until - now)
        next_slot[0] = max(time.monotonic(), wait_until) + min_interval
        return entity, _crawl(entity)

    # Wall-clock timeout for the WHOLE Phase 5 crawl. Without it, a
    # single publisher whose ads.txt server hangs (DNS that resolves
    # but never SYN-ACKs, TCP keepalive holes, etc.) can block the
    # ThreadPool's join() forever — observed locally on 2026-06-06
    # where the runner stayed wedged for 40+ minutes after Phase 4
    # completed. Per-fetch HTTP timeout already exists in _crawl()
    # but doesn't protect against a stuck future in the pool.
    #
    # Budget: 5 min default; override via env. When the deadline hits
    # we stop accepting new completions and skip the remaining entities
    # (they show as audit_host-unreachable in the matrix, same as a
    # 404 — bad signal but doesn't kill the run).
    PHASE5_CRAWL_TIMEOUT_SEC = float(
        os.environ.get("PGAM_COMPLIANCE_PHASE5_TIMEOUT_SEC", "300")
    )
    import concurrent.futures as _cf
    deadline = time.monotonic() + PHASE5_CRAWL_TIMEOUT_SEC
    completed_n = 0
    timed_out = False
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(_gated_crawl, e): e for e in entities}
        try:
            for fut in as_completed(futures, timeout=PHASE5_CRAWL_TIMEOUT_SEC):
                if time.monotonic() > deadline:
                    timed_out = True
                    break
                try:
                    e, f = fut.result(timeout=15)
                except (Exception, _cf.TimeoutError):
                    continue
                completed_n += 1
                if f is not None:
                    fetches[e.entity_key] = f
        except _cf.TimeoutError:
            timed_out = True
        finally:
            if timed_out:
                # Cancel pending futures so pool.shutdown returns
                # promptly instead of waiting for stuck network calls.
                for pending in futures:
                    if not pending.done():
                        pending.cancel()
                pool.shutdown(wait=False, cancel_futures=True)
                skipped = len(entities) - completed_n
                print(f"[entity_audit] Phase 5 crawl timed out after "
                      f"{PHASE5_CRAWL_TIMEOUT_SEC:.0f}s — completed "
                      f"{completed_n}/{len(entities)} entities, "
                      f"skipping {skipped}")

    # Validate.
    findings: list[Finding] = []
    for entity in entities:
        fetch = fetches.get(entity.entity_key)
        findings.extend(_validate_entity(entity, fetch, registry))

    sentinel_keys = [e.entity_key for e in entities]
    # fetches_by_entity needs an entry for EVERY entity (None for the
    # ones we couldn't crawl, e.g. unresolvable bundles) so the matrix
    # builder can emit `pgam_direct_present=False` rows for them.
    fetches_by_entity: dict[str, AdsTxtFetch | None] = {
        e.entity_key: fetches.get(e.entity_key) for e in entities
    }
    return EntityAuditResult(
        universe_stats=stats,
        findings=findings,
        sentinel_keys=sentinel_keys,
        fetches=list(fetches.values()),
        skipped_reason=None,
        supply_partners=partners,
        entities=entities,
        fetches_by_entity=fetches_by_entity,
        pgam_seat_registry=registry,
    )
