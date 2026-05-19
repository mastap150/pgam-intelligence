"""
agents/compliance/entity_universe.py

Phase 5 universe builder: top-N (publisher × domain) and (publisher ×
bundle) entities by trailing 7d gross revenue.

Data source: pgam_direct.ll_daily_publisher_{domain,bundle}_demand
(populated hourly by agents/etl/ll_4dim_etl.py). No external API calls.

For each entity row we also compute:
  - active_ssps[]  : the SSP keys (Rubicon/PubMatic/etc) observed
                     monetizing THIS specific app or domain in the
                     window — drives the per-entity conditional
                     reseller-line check
  - expected_seller_id : the publisher's PGAM seller_id from sellers.json
                         via compliance_publishers.ll_publisher_id
  - audit_host    : the hostname we'll fetch ads.txt from
                     (domain entity → the domain itself;
                      app entity   → app_metadata.dev_domain)

Truncate-and-rebuild semantics: an entity that stops earning for >7d
drops out of the universe entirely, so we never raise findings on dead
inventory.
"""
from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field

from core.neon import connect

from agents.compliance.ssp_registry import classify_demand_name


# DEFAULT_TOP_N = None  → audit every entity under each supply partner.
# Override with PGAM_COMPLIANCE_PHASE5_TOP_N if you need a safety cap
# during initial rollout. The Phase 5.1 universe is naturally bounded
# by the ~12 active LL supply partners' inventory, typically a few
# hundred to a few thousand entities total.
DEFAULT_TOP_N: int | None = None
DEFAULT_LOOKBACK_DAYS = 7


_PULL_DOMAIN_SQL = """
SELECT
    r.publisher_id,
    MAX(r.publisher_name)            AS publisher_name,
    r.domain                          AS entity_value,
    r.demand_name,
    SUM(r.gross_revenue)::numeric    AS revenue,
    SUM(r.impressions)::bigint       AS impressions
FROM pgam_direct.ll_daily_publisher_domain_demand r
WHERE r.report_date >= (current_date - %(lookback)s::int)
  AND r.gross_revenue > 0
  AND COALESCE(r.domain, '') <> ''
GROUP BY r.publisher_id, r.domain, r.demand_name;
"""

_PULL_BUNDLE_SQL = """
SELECT
    r.publisher_id,
    MAX(r.publisher_name)            AS publisher_name,
    r.bundle                          AS entity_value,
    r.demand_name,
    SUM(r.gross_revenue)::numeric    AS revenue,
    SUM(r.impressions)::bigint       AS impressions
FROM pgam_direct.ll_daily_publisher_bundle_demand r
WHERE r.report_date >= (current_date - %(lookback)s::int)
  AND r.gross_revenue > 0
  AND COALESCE(r.bundle, '') <> ''
GROUP BY r.publisher_id, r.bundle, r.demand_name;
"""

_LL_PUB_TO_SELLER_SQL = """
SELECT ll_publisher_id, seller_id, domain
FROM pgam_direct.compliance_publishers
WHERE is_active = TRUE AND ll_publisher_id IS NOT NULL;
"""

_APP_METADATA_SQL = """
SELECT bundle_id, dev_domain
FROM pgam_direct.app_metadata
WHERE dev_domain IS NOT NULL;
"""


_TRUNCATE_SQL = "TRUNCATE TABLE pgam_direct.compliance_supply_entities;"

_INSERT_SQL = """
INSERT INTO pgam_direct.compliance_supply_entities
    (entity_key, kind, entity_value, ll_publisher_id, ll_publisher_name,
     audit_host, audit_variant, revenue_7d, impressions_7d, active_ssps,
     unclassified_demand_count, expected_seller_id,
     first_seen_at, last_seen_at)
VALUES
    (%(entity_key)s, %(kind)s, %(entity_value)s, %(ll_publisher_id)s,
     %(ll_publisher_name)s, %(audit_host)s, %(audit_variant)s,
     %(revenue_7d)s, %(impressions_7d)s, %(active_ssps)s,
     %(unclassified_demand_count)s, %(expected_seller_id)s,
     now(), now());
"""


@dataclass
class Entity:
    entity_key: str                   # 'dom:<domain>' | 'app:<bundle>'
    kind: str                          # 'domain' | 'app'
    entity_value: str
    ll_publisher_id: str
    ll_publisher_name: str | None
    audit_host: str | None             # None for unresolvable bundles
    audit_variant: str                 # 'ads.txt' | 'app-ads.txt'
    revenue_7d: float = 0.0
    impressions_7d: int = 0
    active_ssps: list[str] = field(default_factory=list)
    unclassified_demand_count: int = 0
    expected_seller_id: str | None = None


@dataclass(frozen=True)
class UniverseStats:
    total_entities_seen: int
    domains_in_universe: int
    apps_in_universe: int
    apps_unresolved: int            # bundles without dev_domain — skipped
    top_n_selected: int


def _normalize_domain(s: str) -> str:
    d = (s or "").strip().lower()
    if d.startswith("http://"):
        d = d[7:]
    elif d.startswith("https://"):
        d = d[8:]
    d = d.split("/", 1)[0]
    if d.startswith("www."):
        d = d[4:]
    return d


def build_entity_universe(
    top_n: int | None = DEFAULT_TOP_N,
    lookback_days: int = DEFAULT_LOOKBACK_DAYS,
    partner_filter: set[str] | None = None,
) -> tuple[list[Entity], UniverseStats]:
    """Pull, classify, rank, return entities under active supply partners.

    Args:
        top_n:          Cap on entity count (None = audit everything).
        lookback_days:  Revenue window (default 7).
        partner_filter: If set, only include entities whose LL publisher_id
                         is in this set — i.e., audit only the inventory of
                         the LL "Suppliers" view, not unrelated publishers.
                         If None, no partner filter is applied.
    """
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute(_PULL_DOMAIN_SQL, {"lookback": lookback_days})
            dom_rows = [
                {"publisher_id":   str(r[0]),
                 "publisher_name": r[1],
                 "entity_value":   r[2],
                 "demand_name":    r[3],
                 "revenue":        float(r[4] or 0),
                 "impressions":    int(r[5] or 0)}
                for r in cur.fetchall()
            ]
            cur.execute(_PULL_BUNDLE_SQL, {"lookback": lookback_days})
            bun_rows = [
                {"publisher_id":   str(r[0]),
                 "publisher_name": r[1],
                 "entity_value":   r[2],
                 "demand_name":    r[3],
                 "revenue":        float(r[4] or 0),
                 "impressions":    int(r[5] or 0)}
                for r in cur.fetchall()
            ]
            cur.execute(_LL_PUB_TO_SELLER_SQL)
            seller_map = {
                str(r[0]): {"seller_id": r[1], "domain": r[2]}
                for r in cur.fetchall()
            }
            cur.execute(_APP_METADATA_SQL)
            dev_domain_map = {r[0]: r[1] for r in cur.fetchall()}

    # Filter to the LL supply partners' inventory if a filter was supplied.
    if partner_filter is not None:
        partner_set = {str(p) for p in partner_filter}
        dom_rows = [r for r in dom_rows if r["publisher_id"] in partner_set]
        bun_rows = [r for r in bun_rows if r["publisher_id"] in partner_set]

    entities: dict[str, Entity] = {}

    def _ingest(rows: list[dict], kind: str) -> None:
        for r in rows:
            value = (
                _normalize_domain(r["entity_value"]) if kind == "domain"
                else (r["entity_value"] or "").strip()
            )
            if not value:
                continue
            key = f"{'dom' if kind == 'domain' else 'app'}:{value}"
            ent = entities.get(key)
            if ent is None:
                if kind == "domain":
                    audit_host, variant = value, "ads.txt"
                else:
                    audit_host = dev_domain_map.get(value)
                    variant = "app-ads.txt"
                seller_info = seller_map.get(r["publisher_id"]) or {}
                ent = Entity(
                    entity_key=key,
                    kind=kind,
                    entity_value=value,
                    ll_publisher_id=r["publisher_id"],
                    ll_publisher_name=r["publisher_name"],
                    audit_host=audit_host,
                    audit_variant=variant,
                    expected_seller_id=seller_info.get("seller_id"),
                )
                entities[key] = ent
            ent.revenue_7d += r["revenue"]
            ent.impressions_7d += r["impressions"]
            exp = classify_demand_name(r["demand_name"] or "")
            if exp is not None:
                if exp.ssp_key not in ent.active_ssps:
                    ent.active_ssps.append(exp.ssp_key)
            else:
                ent.unclassified_demand_count += 1

    _ingest(dom_rows, "domain")
    _ingest(bun_rows, "app")

    # Rank by revenue (so the digest's "top critical" highlights big-$ entities).
    # Apply top_n cap only if explicitly set. Bundles without dev_domain stay
    # in the universe at this stage so the audit can raise an info finding
    # rather than silently drop them.
    ranked = sorted(entities.values(), key=lambda e: e.revenue_7d, reverse=True)
    selected = ranked if top_n is None else ranked[:top_n]

    apps_unresolved = sum(1 for e in selected if e.kind == "app" and not e.audit_host)
    stats = UniverseStats(
        total_entities_seen=len(entities),
        domains_in_universe=sum(1 for e in selected if e.kind == "domain"),
        apps_in_universe=sum(1 for e in selected if e.kind == "app"),
        apps_unresolved=apps_unresolved,
        top_n_selected=len(selected),
    )
    return selected, stats


def persist_entity_universe(entities: list[Entity]) -> int:
    """Truncate + insert per-run universe snapshot."""
    if not entities:
        with connect() as conn:
            with conn.cursor() as cur:
                cur.execute(_TRUNCATE_SQL)
            conn.commit()
        return 0
    rows = [
        {
            "entity_key":               e.entity_key,
            "kind":                     e.kind,
            "entity_value":             e.entity_value,
            "ll_publisher_id":          e.ll_publisher_id,
            "ll_publisher_name":        e.ll_publisher_name,
            "audit_host":               e.audit_host,
            "audit_variant":            e.audit_variant,
            "revenue_7d":               round(e.revenue_7d, 4),
            "impressions_7d":           e.impressions_7d,
            "active_ssps":              sorted(e.active_ssps),
            "unclassified_demand_count": e.unclassified_demand_count,
            "expected_seller_id":       e.expected_seller_id,
        }
        for e in entities
    ]
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute(_TRUNCATE_SQL)
            cur.executemany(_INSERT_SQL, rows)
        conn.commit()
    return len(rows)
