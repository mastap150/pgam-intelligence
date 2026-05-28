"""
agents/compliance/validators/sellersjson_inventory_roundtrip.py

Phase 6 — the "where we earn → must be in our sellers.json" check.

Inverts the direction of the downstream-sellers.json audit (Phase 3,
which verifies our seat appears in each SSP's sellers.json) and asks
the symmetric upstream question:

    "For every domain + bundle we ACTUALLY earn revenue on, is it
     covered by an entry in PGAM's own sellers.json — either as a
     direct PUBLISHER (this property's domain is declared) or via an
     INTERMEDIARY supply partner (Start.IO / Smaato / etc.) that
     IS declared in our sellers.json?"

Inspired by the round-trip check in vivek12367/adscan-dashboard's
validation/sellers_json.py — that repo audits "observed in the wild
vs declared" from the publisher-ads.txt side; this version audits the
same gap from the per-entity-revenue side, using LL's per-publisher
stats as the source of truth for "actually monetizing."

Findings produced:

  compliance.earning_on_undeclared_inventory     CRITICAL
    Entity earns >0 USD trailing 7d AND is neither directly declared
    in PGAM's sellers.json (no PUBLISHER/BOTH entry with matching
    domain) NOR flowing under a declared INTERMEDIARY supply partner.
    This is leaked revenue from a compliance perspective — DSPs
    auditing supply paths can't reconcile this monetization.

  compliance.earning_via_unbridged_partner       HIGH
    Entity earns through an LL publisher_id we haven't been able to
    bridge to any sellers.json entry. Could be a brand-new supply
    partner that needs onboarding into sellers.json, OR a name-match
    failure in our ll_bridge that needs tuning. Worth investigating
    before this becomes "undeclared inventory" once bridging fixes.

Sentinel keys reuse the existing entity convention (`dom:` / `app:`)
so these findings dovetail with the rest of the Phase 5 entity audit.
A single entity can land both a Phase 5 ads.txt finding AND a Phase 6
sellers.json declaration finding — they're orthogonal checks on the
same property.
"""
from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass

from agents.compliance.validators.adstxt_universal import Finding


# Severity thresholds (USD trailing-7d) for prioritisation. Findings
# always fire when the gap exists; severity stays critical. The
# threshold here is for SUPPRESSING tiny-tail entities from the
# universe entirely, so we don't emit 10K low-value findings.
DEFAULT_MIN_REV_7D = 1.0


@dataclass(frozen=True)
class EntityRevenueRow:
    """Shape the audit takes from upstream — matches the dicts the
    LL stats puller returns, but typed for clarity."""
    entity_key: str       # 'dom:foo.com' | 'app:com.bar.baz'
    kind: str             # 'domain' | 'app'
    entity_value: str
    ll_publisher_id: str
    ll_publisher_name: str | None
    dev_domain: str | None  # resolved app-ads.txt host for bundles, None for unresolved
    revenue_7d: float


@dataclass(frozen=True)
class RoundtripStats:
    entities_seen: int
    declared_direct: int       # entity's own domain in sellers.json
    declared_intermediary: int  # under an INTERMEDIARY/BOTH in sellers.json
    unbridged_partner: int     # ll_publisher_id not in our bridge
    undeclared: int            # earning but nowhere in sellers.json
    revenue_at_risk_7d: float


def _normalize(s: str | None) -> str:
    return (s or "").strip().lower()


def audit_inventory_roundtrip(
    entities: list[EntityRevenueRow],
    *,
    pgam_publisher_domains: dict[str, str],         # domain → seller_id (PUBLISHER or BOTH)
    bridged_intermediary_partners: set[str],         # ll_publisher_ids bridged to INTERMEDIARY/BOTH sellers
    bridged_publisher_partners: set[str],            # ll_publisher_ids bridged to PUBLISHER/BOTH sellers
    min_rev_7d: float = DEFAULT_MIN_REV_7D,
) -> tuple[list[Finding], RoundtripStats]:
    """Run the round-trip check across a list of revenue-bearing entities.

    Pure function — caller orchestrates DB reads + the LL pull and
    feeds results in. Easy to unit-test with synthetic inputs.

    Decision tree for each entity:
        1. entity's domain (or dev_domain for apps) in PGAM sellers.json
           as a PUBLISHER/BOTH entry  → DECLARED_DIRECT (no finding)
        2. else, entity's ll_publisher_id is bridged to an INTERMEDIARY
           or BOTH sellers.json entry                  → DECLARED_INTERMEDIARY (no finding)
        3. else, ll_publisher_id is bridged but only to PUBLISHER (not
           an intermediary path) → DECLARED_DIRECT_PARTNER (no finding —
           the partner is declared as a direct relationship, individual
           inventory inherits that)
        4. else, ll_publisher_id is NOT bridged at all  → unbridged_partner (HIGH)
        5. else                                          → undeclared (CRITICAL)
    """
    findings: list[Finding] = []
    declared_direct = 0
    declared_intermediary = 0
    unbridged = 0
    undeclared = 0
    revenue_at_risk = 0.0
    # Aggregator for the per-partner "unbridged" finding emission.
    _unbridged_partners: dict[str, dict] = defaultdict(lambda: {
        "count": 0, "revenue_7d": 0.0, "partner_name": None, "top_entities": [],
    })

    for e in entities:
        if e.revenue_7d < min_rev_7d:
            continue

        # Direction A: domain (or app dev_domain) declared directly in sellers.json
        lookup_host = _normalize(e.entity_value if e.kind == "domain" else e.dev_domain)
        if lookup_host and lookup_host in pgam_publisher_domains:
            declared_direct += 1
            continue

        # Direction B: flowing through a declared INTERMEDIARY supply partner
        if e.ll_publisher_id in bridged_intermediary_partners:
            declared_intermediary += 1
            continue

        # Direction C: bridged to a PUBLISHER-type seller (direct partner
        # whose individual inventory inherits the declaration)
        if e.ll_publisher_id in bridged_publisher_partners:
            declared_direct += 1
            continue

        # Direction D: ll_publisher_id wasn't bridged at all — name-match
        # failure or genuinely new partner. AGGREGATE these by partner
        # (one finding per partner, not per entity) so we don't flood
        # compliance_findings with thousands of low-value rows when the
        # ll_bridge has a coverage gap. The per-partner finding carries
        # the top-revenue entities in its detail for drill-down.
        if e.ll_publisher_id and e.ll_publisher_id not in (
            bridged_intermediary_partners | bridged_publisher_partners
        ):
            unbridged += 1
            revenue_at_risk += e.revenue_7d
            _unbridged_partners[e.ll_publisher_id]["count"] += 1
            _unbridged_partners[e.ll_publisher_id]["revenue_7d"] += e.revenue_7d
            _unbridged_partners[e.ll_publisher_id]["partner_name"] = (
                e.ll_publisher_name
            )
            _unbridged_partners[e.ll_publisher_id]["top_entities"].append({
                "entity_value": e.entity_value,
                "kind": e.kind,
                "revenue_7d": round(e.revenue_7d, 2),
            })
            continue

        # Direction E: undeclared — earning revenue, no sellers.json coverage
        undeclared += 1
        revenue_at_risk += e.revenue_7d
        findings.append(Finding.make(
            publisher_key=e.entity_key,
            check_id="compliance.earning_on_undeclared_inventory",
            severity="critical",
            detail={
                "entity_value":     e.entity_value,
                "kind":             e.kind,
                "lookup_host":      lookup_host,
                "ll_publisher_id":  e.ll_publisher_id,
                "ll_publisher_name": e.ll_publisher_name,
                "revenue_7d":       round(e.revenue_7d, 2),
                "fix": (
                    "Add an entry to PGAM sellers.json declaring this "
                    "inventory — either the property directly (PUBLISHER) "
                    "or its upstream supply partner (INTERMEDIARY)."
                ),
            },
            fingerprint_extra=e.entity_key,
        ))

    # Emit one HIGH finding per unbridged partner — not per entity —
    # rolling up the entity count + total revenue. detail.top_entities
    # carries the 10 highest-revenue entities under that partner for
    # drill-down.
    for pid, agg in _unbridged_partners.items():
        top = sorted(agg["top_entities"], key=lambda x: -x["revenue_7d"])[:10]
        findings.append(Finding.make(
            publisher_key=f"_ll_publisher:{pid}",
            check_id="compliance.earning_via_unbridged_partner",
            severity="high",
            detail={
                "ll_publisher_id":   pid,
                "ll_publisher_name": agg["partner_name"],
                "entities_count":    agg["count"],
                "revenue_7d":        round(agg["revenue_7d"], 2),
                "top_entities":      top,
                "consequence": (
                    "LL publisher_id isn't bridged to any sellers.json entry. "
                    "Either tune ll_bridge to match this LL partner's name to "
                    "an existing sellers.json INTERMEDIARY/PUBLISHER row, OR "
                    "add the partner to sellers.json if genuinely new."
                ),
            },
            fingerprint_extra=pid,
        ))

    stats = RoundtripStats(
        entities_seen=sum(1 for e in entities if e.revenue_7d >= min_rev_7d),
        declared_direct=declared_direct,
        declared_intermediary=declared_intermediary,
        unbridged_partner=unbridged,
        undeclared=undeclared,
        revenue_at_risk_7d=round(revenue_at_risk, 2),
    )
    return findings, stats
