"""
agents/compliance/inventory_roundtrip_audit.py

Phase 6 orchestrator: pulls every (entity × publisher × demand) row
with trailing-7d revenue from the LL stats API, reconciles each entity
against PGAM's sellers.json + the ll_bridge, and emits findings for
revenue earned on undeclared inventory.

Unlike Phase 5 (which uses the active-LL-supply-partner filter to
gate ads.txt crawling), Phase 6 audits EVERY revenue-driving entity
because the question is about coverage, not crawl. The check is
cheap (in-memory lookups against the sellers.json registry + bridge
set) so we can sweep the full ~24K-entity universe without rate
concerns.
"""
from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass

from core.api import fetch as ll_fetch, n_days_ago, sf, today
from core.neon import connect

from agents.compliance.crawlers.sellersjson import fetch_pgam_sellers_json
from agents.compliance.validators.adstxt_universal import Finding
from agents.compliance.validators.sellersjson_inventory_roundtrip import (
    DEFAULT_MIN_REV_7D,
    EntityRevenueRow,
    RoundtripStats,
    audit_inventory_roundtrip,
)


_LOOKBACK_DAYS = 7

_LL_DOMAIN_BREAKDOWN = "DOMAIN,PUBLISHER,DEMAND_PARTNER"
_LL_BUNDLE_BREAKDOWN = "BUNDLE,PUBLISHER,DEMAND_PARTNER"


_BRIDGE_SQL = """
SELECT ll_publisher_id, seller_type, seller_id
FROM pgam_direct.compliance_ll_partner_bridge;
"""

_APP_METADATA_SQL = """
SELECT bundle_id, dev_domain
FROM pgam_direct.app_metadata
WHERE dev_domain IS NOT NULL;
"""


@dataclass(frozen=True)
class RoundtripAuditResult:
    skipped_reason: str | None
    stats: RoundtripStats | None
    findings: list[Finding]
    sentinel_keys: list[str]


def _aggregate_per_entity(
    breakdown: str, value_keys: tuple[str, ...], lookback_days: int,
) -> list[dict]:
    """Pull LL stats for one breakdown and aggregate per (publisher, entity)."""
    end = today()
    start = n_days_ago(max(lookback_days - 1, 0))
    try:
        rows = ll_fetch(breakdown, ["GROSS_REVENUE"], start, end)
    except Exception as exc:
        print(f"[inventory_roundtrip] LL fetch failed for {breakdown}: {exc}")
        return []

    agg: dict[tuple[str, str], dict] = defaultdict(lambda: {
        "revenue": 0.0, "publisher_name": ""
    })
    for r in rows:
        value = ""
        for k in value_keys:
            v = r.get(k)
            if v not in (None, ""):
                value = str(v).strip()
                break
        if not value:
            continue
        pid = ""
        for k in ("PUBLISHER_ID", "PUBLISHER", "publisher_id", "publisher"):
            v = r.get(k)
            if v not in (None, ""):
                pid = str(v).strip()
                break
        rev = sf(r.get("GROSS_REVENUE"))
        if rev <= 0:
            continue
        key = (pid, value)
        bucket = agg[key]
        bucket["revenue"] += rev
        if not bucket["publisher_name"]:
            bucket["publisher_name"] = (
                r.get("PUBLISHER_NAME") or r.get("publisher_name") or ""
            )
    out = [
        {"publisher_id": k[0], "entity_value": k[1],
         "publisher_name": v["publisher_name"], "revenue": v["revenue"]}
        for k, v in agg.items()
    ]
    # 50K+ raw LL stats rows are now redundant with the smaller `out`
    # aggregation. Drop the reference + force a GC pass — on the 512MB
    # Render box this often saves 30–50MB and prevents OOM during the
    # bundle-breakdown second pass.
    del rows
    del agg
    import gc as _gc
    _gc.collect()
    return out


def _build_lookups() -> tuple[dict[str, str], set[str], set[str], dict[str, str]]:
    """Build the four maps the roundtrip validator needs.

    Returns (pgam_publisher_domains, bridged_intermediary, bridged_publisher,
             dev_domain_map).
    """
    pgam_publisher_domains: dict[str, str] = {}  # domain → seller_id
    payload = fetch_pgam_sellers_json()
    for s in (payload.get("sellers") or []):
        if not isinstance(s, dict):
            continue
        sid = str(s.get("seller_id") or "").strip()
        dom = (s.get("domain") or "").strip().lower()
        if dom.startswith("www."):
            dom = dom[4:]
        stype = (s.get("seller_type") or "").upper().strip()
        if sid and dom and stype in ("PUBLISHER", "BOTH"):
            pgam_publisher_domains[dom] = sid

    bridged_intermediary: set[str] = set()
    bridged_publisher: set[str] = set()
    dev_domain_map: dict[str, str] = {}

    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute(_BRIDGE_SQL)
            for ll_pid, stype, _sid in cur.fetchall():
                if not ll_pid:
                    continue
                stype_u = (stype or "").upper()
                if stype_u in ("INTERMEDIARY", "BOTH"):
                    bridged_intermediary.add(str(ll_pid))
                if stype_u in ("PUBLISHER", "BOTH"):
                    bridged_publisher.add(str(ll_pid))
            cur.execute(_APP_METADATA_SQL)
            for bundle_id, dev_domain in cur.fetchall():
                if bundle_id and dev_domain:
                    dev_domain_map[bundle_id] = dev_domain

    return pgam_publisher_domains, bridged_intermediary, bridged_publisher, dev_domain_map


def run_inventory_roundtrip_audit(
    lookback_days: int = _LOOKBACK_DAYS,
    min_rev_7d: float = DEFAULT_MIN_REV_7D,
) -> RoundtripAuditResult:
    """End-to-end Phase 6: pull entities, build lookups, audit, return findings."""
    print("[inventory_roundtrip] start", flush=True)

    try:
        dom_rows = _aggregate_per_entity(
            _LL_DOMAIN_BREAKDOWN, ("DOMAIN", "domain"), lookback_days,
        )
        bun_rows = _aggregate_per_entity(
            _LL_BUNDLE_BREAKDOWN, ("BUNDLE", "bundle"), lookback_days,
        )
    except Exception as exc:
        return RoundtripAuditResult(
            skipped_reason=f"LL stats fetch failed: {exc}",
            stats=None, findings=[], sentinel_keys=[],
        )

    if not dom_rows and not bun_rows:
        return RoundtripAuditResult(
            skipped_reason="no revenue-bearing entities in trailing window",
            stats=None, findings=[], sentinel_keys=[],
        )

    try:
        pgam_domains, bridged_int, bridged_pub, dev_domain_map = _build_lookups()
    except Exception as exc:
        return RoundtripAuditResult(
            skipped_reason=f"sellers.json / bridge load failed: {exc}",
            stats=None, findings=[], sentinel_keys=[],
        )

    print(f"[inventory_roundtrip] universe: {len(dom_rows)} domain rows, "
          f"{len(bun_rows)} bundle rows  · "
          f"sellers.json declared domains: {len(pgam_domains)}  · "
          f"intermediary-bridged partners: {len(bridged_int)}  · "
          f"publisher-bridged partners: {len(bridged_pub)}",
          flush=True)

    entities: list[EntityRevenueRow] = []
    for r in dom_rows:
        entities.append(EntityRevenueRow(
            entity_key=f"dom:{r['entity_value'].lower()}",
            kind="domain",
            entity_value=r["entity_value"],
            ll_publisher_id=r["publisher_id"],
            ll_publisher_name=r["publisher_name"] or None,
            dev_domain=None,
            revenue_7d=r["revenue"],
        ))
    for r in bun_rows:
        bundle = r["entity_value"]
        entities.append(EntityRevenueRow(
            entity_key=f"app:{bundle}",
            kind="app",
            entity_value=bundle,
            ll_publisher_id=r["publisher_id"],
            ll_publisher_name=r["publisher_name"] or None,
            dev_domain=dev_domain_map.get(bundle),
            revenue_7d=r["revenue"],
        ))

    findings, stats = audit_inventory_roundtrip(
        entities,
        pgam_publisher_domains=pgam_domains,
        bridged_intermediary_partners=bridged_int,
        bridged_publisher_partners=bridged_pub,
        min_rev_7d=min_rev_7d,
    )

    # Sentinel keys = the resolvable set for auto-resolve. Per-entity
    # findings key on `dom:`/`app:`; per-partner unbridged findings key
    # on `_ll_publisher:`. We need BOTH classes in the resolvable set
    # so auto-resolve doesn't get confused.
    sentinel_keys = (
        [e.entity_key for e in entities if e.revenue_7d >= min_rev_7d]
        + [f.publisher_key for f in findings
           if f.publisher_key.startswith("_ll_publisher:")]
    )
    # Dedup while preserving order.
    seen = set()
    sentinel_keys = [k for k in sentinel_keys if not (k in seen or seen.add(k))]
    print(
        f"[inventory_roundtrip] result: "
        f"{stats.declared_direct} direct + "
        f"{stats.declared_intermediary} intermediary + "
        f"{stats.unbridged_partner} unbridged + "
        f"{stats.undeclared} undeclared  · "
        f"${stats.revenue_at_risk_7d:,.0f}/7d at risk  · "
        f"{len(findings)} findings",
        flush=True,
    )
    return RoundtripAuditResult(
        skipped_reason=None,
        stats=stats,
        findings=findings,
        sentinel_keys=sentinel_keys,
    )
