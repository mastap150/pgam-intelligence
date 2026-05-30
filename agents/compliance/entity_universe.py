"""
agents/compliance/entity_universe.py

Phase 5 universe builder: top-N (publisher × domain) and (publisher ×
bundle) entities by trailing 7d gross revenue.

Data source: the LL stats API (stats.ortb.net) — live, pulled directly
per run. Previously read from pgam_direct.ll_daily_publisher_
{domain,bundle}_demand, but that ETL went stale (last report_date
2026-05-12, agent's first prod run flagged it 2026-05-28) which
silently zeroed out Phase 5. Hitting the LL stats API directly removes
the ETL dependency entirely; the operational price is one extra LL
API call per run (~5-15s vs <1s on the Neon read).

For each entity row we also compute:
  - active_ssps[]  : the SSP keys (Rubicon/PubMatic/etc) observed
                     monetizing THIS specific app or domain in the
                     window — drives the per-entity conditional
                     reseller-line check
  - expected_seller_id : the publisher's PGAM seller_id from sellers.json
                         via compliance_publishers.ll_publisher_id
                         (Neon — populated by Phase 2 ll_bridge, fresh)
  - audit_host    : the hostname we'll fetch ads.txt from
                     (domain entity → the domain itself;
                      app entity   → app_metadata.dev_domain via Neon,
                      populated by app_name_enrichment, fresh)

Truncate-and-rebuild semantics: an entity that stops earning for >7d
drops out of the universe entirely, so we never raise findings on dead
inventory.
"""
from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field

from core.api import fetch as ll_fetch, n_days_ago, sf, today
from core.neon import connect

from agents.compliance.ssp_registry import classify_demand_name


# DEFAULT_TOP_N = None  → audit every entity under each supply partner.
# Override with PGAM_COMPLIANCE_PHASE5_TOP_N if you need a safety cap
# during initial rollout. The Phase 5.1 universe is naturally bounded
# by the ~12 active LL supply partners' inventory, typically a few
# hundred to a few thousand entities total.
DEFAULT_TOP_N: int | None = None
DEFAULT_LOOKBACK_DAYS = 7


# LL stats API breakdowns. Always include PUBLISHER in the breakdown
# so we can attribute per-entity revenue back to its supply partner
# (Phase 5 filters to active LL Suppliers — needs publisher_id per row).
_LL_DOMAIN_BREAKDOWN = "DOMAIN,PUBLISHER,DEMAND_PARTNER"
_LL_BUNDLE_BREAKDOWN = "BUNDLE,PUBLISHER,DEMAND_PARTNER"

# Field-name aliases — LL has been observed returning either casing /
# underscore variant depending on the dimension combination, so we
# probe in order of likelihood.
_DOMAIN_KEYS    = ("DOMAIN", "domain")
_BUNDLE_KEYS    = ("BUNDLE", "bundle", "BUNDLE_NAME")
_PUB_ID_KEYS    = ("PUBLISHER_ID", "PUBLISHER", "publisher_id", "publisher")
_PUB_NAME_KEYS  = ("PUBLISHER_NAME", "publisher_name")
_DEMAND_KEYS    = ("DEMAND_PARTNER_NAME", "DEMAND_PARTNER", "demand_partner",
                   "demand_partner_name", "DEMAND_NAME", "demand_name")


def _first(row: dict, keys: tuple[str, ...]) -> str:
    for k in keys:
        v = row.get(k)
        if v not in (None, ""):
            return str(v)
    return ""


def _pull_ll_breakdown(breakdown: str, value_keys: tuple[str, ...],
                        lookback_days: int) -> list[dict]:
    """Pull (entity × publisher × demand) from LL stats API. Returns dict
    rows shaped like the previous Neon SQL output so the rest of the
    builder is unchanged."""
    end = today()
    start = n_days_ago(max(lookback_days - 1, 0))
    try:
        rows = ll_fetch(breakdown, ["GROSS_REVENUE", "IMPRESSIONS"], start, end)
    except Exception as exc:
        print(f"[entity_universe] LL fetch failed for {breakdown}: {exc}")
        return []
    out: list[dict] = []
    for r in rows:
        value = _first(r, value_keys)
        if not value:
            continue
        rev = sf(r.get("GROSS_REVENUE"))
        if rev <= 0:
            continue
        out.append({
            "publisher_id":   _first(r, _PUB_ID_KEYS),
            "publisher_name": _first(r, _PUB_NAME_KEYS),
            "entity_value":   value.strip(),
            "demand_name":    _first(r, _DEMAND_KEYS),
            "revenue":        rev,
            "impressions":    int(sf(r.get("IMPRESSIONS"))),
        })
    # Drop the raw LL stats payload — it's typically 30–50MB on a
    # 7-day window and we don't need it after aggregation. Cuts peak
    # memory during the second-pass bundle breakdown.
    del rows
    import gc as _gc
    _gc.collect()
    return out


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
    # Per-entity revenue pulled LIVE from LL stats API — replaces the
    # previously-used Neon ll_4dim_etl tables which were prone to ETL
    # staleness silently zeroing out Phase 5. See module docstring.
    dom_rows = _pull_ll_breakdown(_LL_DOMAIN_BREAKDOWN, _DOMAIN_KEYS, lookback_days)
    bun_rows = _pull_ll_breakdown(_LL_BUNDLE_BREAKDOWN, _BUNDLE_KEYS, lookback_days)

    # Neon-side lookup maps (these tables ARE fresh — populated by the
    # Phase 2 ll_bridge and the app_name_enrichment ETL).
    with connect() as conn:
        with conn.cursor() as cur:
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
