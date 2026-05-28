"""
agents/compliance/universe.py

Build the per-day publisher universe from PGAM's sellers.json + UPSERT
to pgam_direct.compliance_publishers.

A publisher is included when:
    - seller_type IN (PUBLISHER, BOTH)
    - domain field is non-empty and parses to a hostname

Active flag handling
--------------------
Every row we see this run is touched (last_seen_at = now()) and
is_active=True. Rows that existed but weren't re-seen this run get
is_active flipped to False (soft-delete; we keep their history).
"""
from __future__ import annotations

from dataclasses import dataclass

from core.neon import connect

from agents.compliance.crawlers.sellersjson import (
    SellerEntry,
    fetch_pgam_sellers_json,
    fetch_publisher_entries,
    parse_sellers,
)


@dataclass(frozen=True)
class Publisher:
    publisher_key: str
    kind: str
    domain: str
    seller_id: str
    seller_type: str
    seller_name: str | None

    @staticmethod
    def from_entry(entry: SellerEntry) -> "Publisher | None":
        dom = entry.normalized_domain
        if not dom:
            return None
        return Publisher(
            publisher_key=dom,
            kind="domain",
            domain=dom,
            seller_id=entry.seller_id,
            seller_type=entry.seller_type,
            seller_name=entry.name,
        )


_UPSERT_SQL = """
INSERT INTO pgam_direct.compliance_publishers
    (publisher_key, kind, domain, seller_id, seller_type, seller_name,
     source, first_seen_at, last_seen_at, is_active)
VALUES
    (%(publisher_key)s, %(kind)s, %(domain)s, %(seller_id)s, %(seller_type)s,
     %(seller_name)s, 'pgam_sellers_json', now(), now(), TRUE)
ON CONFLICT (publisher_key) DO UPDATE SET
    kind         = EXCLUDED.kind,
    domain       = EXCLUDED.domain,
    seller_id    = EXCLUDED.seller_id,
    seller_type  = EXCLUDED.seller_type,
    seller_name  = EXCLUDED.seller_name,
    last_seen_at = now(),
    is_active    = TRUE;
"""

_DEACTIVATE_SQL = """
UPDATE pgam_direct.compliance_publishers
SET is_active = FALSE
WHERE is_active = TRUE
  AND publisher_key <> ALL(%(seen)s);
"""


def build_universe(url: str | None = None) -> list[Publisher]:
    """The PUBLISHER + BOTH set — entities whose ads.txt Phase 1 actually crawls."""
    entries = fetch_publisher_entries(url=url)
    return _dedup_by_key(entries)


def build_full_registry(url: str | None = None) -> list[Publisher]:
    """ALL sellers.json entries (PUBLISHER + BOTH + INTERMEDIARY).

    Phase 1's ads.txt crawl operates on build_universe() (publisher-like
    only). compliance_publishers, however, doubles as the ll_bridge
    matching pool — and the bridge needs INTERMEDIARY entries (Start.io,
    Smaato, BidMachine, …) too, otherwise it can't map LL supply
    partners to sellers.json at all, which silently fails Phase 6's
    round-trip check (the failure mode that flooded the first prod
    run with 6,224 "earning via unbridged partner" entities).

    Use this builder for compliance_publishers UPSERT; use
    build_universe() for the Phase 1 crawl loop.
    """
    payload = fetch_pgam_sellers_json(url=url)
    entries = parse_sellers(payload)
    # Drop entries with no usable domain (the bridge needs domain stems).
    entries = [e for e in entries if e.normalized_domain]
    return _dedup_by_key(entries)


def _dedup_by_key(entries: list[SellerEntry]) -> list[Publisher]:
    by_key: dict[str, Publisher] = {}
    for e in entries:
        pub = Publisher.from_entry(e)
        if pub is None:
            continue
        # If two entries share a domain (rare), prefer PUBLISHER over
        # BOTH over INTERMEDIARY for stability; otherwise the first wins.
        existing = by_key.get(pub.publisher_key)
        if existing is None:
            by_key[pub.publisher_key] = pub
            continue
        precedence = {"PUBLISHER": 0, "BOTH": 1, "INTERMEDIARY": 2}
        if precedence.get(pub.seller_type, 9) < precedence.get(existing.seller_type, 9):
            by_key[pub.publisher_key] = pub
    return sorted(by_key.values(), key=lambda p: p.publisher_key)


def sync_universe(publishers: list[Publisher]) -> tuple[int, int]:
    """UPSERT publishers, deactivate keys no longer seen. Returns (upserted, deactivated)."""
    if not publishers:
        return 0, 0
    rows = [
        {
            "publisher_key": p.publisher_key,
            "kind":          p.kind,
            "domain":        p.domain,
            "seller_id":     p.seller_id,
            "seller_type":   p.seller_type,
            "seller_name":   p.seller_name,
        }
        for p in publishers
    ]
    seen_keys = [p.publisher_key for p in publishers]
    with connect() as conn:
        with conn.cursor() as cur:
            cur.executemany(_UPSERT_SQL, rows)
            cur.execute(_DEACTIVATE_SQL, {"seen": seen_keys})
            deactivated = cur.rowcount or 0
        conn.commit()
    return len(rows), deactivated
