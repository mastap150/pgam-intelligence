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
from dataclasses import dataclass

from core import ll_mgmt

from agents.compliance.crawlers.adstxt import AdsTxtFetch, fetch_adstxt
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


@dataclass(frozen=True)
class EntityAuditResult:
    universe_stats: UniverseStats
    findings: list[Finding]
    sentinel_keys: list[str]
    fetches: list[AdsTxtFetch]
    skipped_reason: str | None = None
    supply_partners: list[dict] | None = None        # [{id, name}]


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
    """Fetch the ads.txt or app-ads.txt for one entity. None if unresolvable."""
    if not entity.audit_host:
        return None
    return fetch_adstxt(
        entity.entity_key,
        entity.audit_host,
        variant=entity.audit_variant,
    )


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
                "revenue_7d": entity.revenue_7d,
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
    if fetch.http_status == 200 and entity.active_ssps:
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

    return findings


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

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(_gated_crawl, e): e for e in entities}
        for fut in as_completed(futures):
            try:
                e, f = fut.result()
            except Exception:
                continue
            if f is not None:
                fetches[e.entity_key] = f

    # Validate.
    findings: list[Finding] = []
    for entity in entities:
        fetch = fetches.get(entity.entity_key)
        findings.extend(_validate_entity(entity, fetch, registry))

    sentinel_keys = [e.entity_key for e in entities]
    return EntityAuditResult(
        universe_stats=stats,
        findings=findings,
        sentinel_keys=sentinel_keys,
        fetches=list(fetches.values()),
        skipped_reason=None,
        supply_partners=partners,
    )
