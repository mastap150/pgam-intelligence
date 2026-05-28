"""
agents/compliance/ll_bridge.py

Match LL `publisher_id` ↔ sellers.json domain so the conditional reseller
validator can join observed monetization (keyed by LL publisher_id in
ll_daily_partner_revenue) against compliance_publishers (keyed by domain).

Data source: pgam_direct.ll_daily_partner_revenue — already populated by
agents/etl/partner_revenue_etl.py. No external API calls.

Matching cascade (first hit wins, score recorded):
  1. exact_name        — LL publisher_name == sellers.json seller.name (case-insensitive)
  2. domain_substring  — LL publisher_name contains the sellers.json domain
                          stem (e.g. "pch.com" or "pch" inside "PCH Network")
  3. token_overlap     — ≥60 % token Jaccard between names

Anything below 0.6 confidence is treated as unmatched and logged. A row
can only be bridged to one LL publisher_id at a time — if a new run
produces a higher-confidence match, the row is updated.
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime, timezone

from core.neon import connect

_LOOKBACK_DAYS = 30  # consider LL publishers active in trailing window only
_MIN_SCORE = 0.60


# ── Source queries ───────────────────────────────────────────────────────────

_ACTIVE_LL_PUBS_SQL = """
SELECT publisher_id, publisher_name, SUM(gross_revenue) AS revenue
FROM pgam_direct.ll_daily_partner_revenue
WHERE report_date >= (current_date - %(lookback)s::int)
GROUP BY publisher_id, publisher_name
HAVING SUM(gross_revenue) > 0
ORDER BY revenue DESC;
"""

_COMPLIANCE_PUBLISHERS_SQL = """
SELECT publisher_key, domain, seller_name, seller_type, seller_id
FROM pgam_direct.compliance_publishers
WHERE is_active = TRUE;
"""

_UPSERT_PARTNER_BRIDGE_SQL = """
INSERT INTO pgam_direct.compliance_ll_partner_bridge
    (ll_publisher_id, publisher_key, seller_type, seller_id,
     ll_publisher_name, bridge_method, bridge_score, bridged_at)
VALUES (%(ll_publisher_id)s, %(publisher_key)s, %(seller_type)s,
        %(seller_id)s, %(ll_publisher_name)s, %(bridge_method)s,
        %(bridge_score)s, now())
ON CONFLICT (ll_publisher_id) DO UPDATE SET
    publisher_key     = EXCLUDED.publisher_key,
    seller_type       = EXCLUDED.seller_type,
    seller_id         = EXCLUDED.seller_id,
    ll_publisher_name = EXCLUDED.ll_publisher_name,
    bridge_method     = EXCLUDED.bridge_method,
    bridge_score      = EXCLUDED.bridge_score,
    bridged_at        = now()
"""

_UPDATE_BRIDGE_SQL = """
UPDATE pgam_direct.compliance_publishers
SET ll_publisher_id   = %(ll_publisher_id)s,
    ll_publisher_name = %(ll_publisher_name)s,
    ll_match_method   = %(ll_match_method)s,
    ll_match_score    = %(ll_match_score)s,
    ll_matched_at     = now()
WHERE publisher_key = %(publisher_key)s
  AND (
        ll_publisher_id IS NULL
        OR ll_publisher_id = %(ll_publisher_id)s
        OR COALESCE(ll_match_score, 0) < %(ll_match_score)s
      );
"""


# ── Matching primitives ──────────────────────────────────────────────────────


_TOKEN_SPLIT = re.compile(r"[^a-z0-9]+")
_STOPWORDS = {
    "the", "and", "a", "of", "inc", "llc", "ltd", "co", "corp",
    "media", "network", "com", "io", "tv", "publisher", "publishing",
}


def _normalize(s: str) -> str:
    return (s or "").strip().lower()


def _tokens(s: str) -> set[str]:
    parts = _TOKEN_SPLIT.split(_normalize(s))
    return {p for p in parts if p and p not in _STOPWORDS and len(p) > 1}


def _domain_stem(domain: str) -> str:
    """First label of a domain: 'pch.com' -> 'pch', 'co.uk-style' edge cases ignored."""
    d = _normalize(domain)
    return d.split(".", 1)[0]


@dataclass(frozen=True)
class MatchCandidate:
    publisher_key: str
    method: str       # 'exact_name' | 'domain_substring' | 'token_overlap'
    score: float      # 0..1


def _score(ll_name: str, compliance_pub: dict) -> MatchCandidate | None:
    """Best-effort match score between one LL publisher and one compliance row."""
    ll_norm = _normalize(ll_name)
    seller_name = _normalize(compliance_pub.get("seller_name") or "")
    domain = _normalize(compliance_pub.get("domain") or "")
    stem = _domain_stem(domain)

    if not ll_norm:
        return None

    # 1. exact_name
    if seller_name and ll_norm == seller_name:
        return MatchCandidate(compliance_pub["publisher_key"], "exact_name", 1.0)

    # 2. name_substring — the sellers.json seller_name appears verbatim
    # inside the LL publisher name. Catches the dominant LL naming
    # convention: "<Partner> - <Channel/Region/Modifier>" /
    # "<Partner> <Channel> <without|with Node>" etc.
    #   "BidMachine - In App Display & Video"     → "Bidmachine" ✓
    #   "zMaticoo In App US Reseller"             → "zMaticoo"   ✓
    #   "Start.IO Display without Node"           → "Start.io"   ✓
    #   "Smaato - In App"                         → "Smaato"     ✓
    #   "Illumin Display & Video"                 → "Illumin"    ✓
    # Length floor of 4 chars to avoid spurious matches on tiny brand
    # names ("Ezo" inside "Ezoic" etc would be too aggressive at 3).
    if seller_name and len(seller_name) >= 4 and seller_name in ll_norm:
        return MatchCandidate(compliance_pub["publisher_key"], "name_substring", 0.95)

    # 3. domain_substring (domain or stem appears in LL name)
    if domain and domain in ll_norm:
        return MatchCandidate(compliance_pub["publisher_key"], "domain_substring", 0.95)
    if stem and len(stem) >= 3 and stem in ll_norm:
        # Penalize short stems mildly to avoid spurious "ab" inside many names.
        return MatchCandidate(compliance_pub["publisher_key"], "domain_substring", 0.85)

    # 3. token_overlap
    if seller_name:
        a, b = _tokens(ll_name), _tokens(seller_name)
        if a and b:
            inter = len(a & b)
            union = len(a | b)
            j = inter / union if union else 0.0
            if j >= 0.5:
                return MatchCandidate(compliance_pub["publisher_key"], "token_overlap", j)

    return None


# ── Public entry point ───────────────────────────────────────────────────────


@dataclass(frozen=True)
class BridgeStats:
    ll_publishers_seen: int
    matched: int
    unmatched: int
    method_counts: dict


def run_bridge(lookback_days: int = _LOOKBACK_DAYS) -> BridgeStats:
    """Pull active LL publishers, match against compliance_publishers, UPDATE bridges."""
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute(_ACTIVE_LL_PUBS_SQL, {"lookback": lookback_days})
            ll_rows = [
                {"publisher_id": r[0], "publisher_name": r[1], "revenue": float(r[2] or 0)}
                for r in cur.fetchall()
            ]
            cur.execute(_COMPLIANCE_PUBLISHERS_SQL)
            compliance_rows = [
                {"publisher_key": r[0], "domain": r[1], "seller_name": r[2],
                 "seller_type":   r[3], "seller_id": r[4]}
                for r in cur.fetchall()
            ]

    # Greedy assignment: for each LL pub, find its best-scoring compliance
    # row. EVERY LL pub gets its own bridge row in
    # compliance_ll_partner_bridge (many-to-one — 4 Start.IO variants all
    # bridge to start.io). The legacy single-column UPDATE on
    # compliance_publishers also runs so the highest-scoring match per
    # row stays visible to anything still reading that column.
    method_counts: dict[str, int] = {}
    matched = 0
    legacy_updates: list[dict] = []
    bridge_inserts: list[dict] = []

    for ll in ll_rows:
        best: MatchCandidate | None = None
        best_row: dict | None = None
        for pub in compliance_rows:
            cand = _score(ll["publisher_name"], pub)
            if cand and (best is None or cand.score > best.score):
                best = cand
                best_row = pub
        if best is None or best.score < _MIN_SCORE or best_row is None:
            continue
        matched += 1
        method_counts[best.method] = method_counts.get(best.method, 0) + 1
        legacy_updates.append({
            "publisher_key":     best.publisher_key,
            "ll_publisher_id":   str(ll["publisher_id"]),
            "ll_publisher_name": ll["publisher_name"],
            "ll_match_method":   best.method,
            "ll_match_score":    round(best.score, 3),
        })
        bridge_inserts.append({
            "ll_publisher_id":   str(ll["publisher_id"]),
            "publisher_key":     best.publisher_key,
            "seller_type":       best_row.get("seller_type"),
            "seller_id":         best_row.get("seller_id"),
            "ll_publisher_name": ll["publisher_name"],
            "bridge_method":     best.method,
            "bridge_score":      round(best.score, 3),
        })

    if legacy_updates or bridge_inserts:
        with connect() as conn:
            with conn.cursor() as cur:
                if legacy_updates:
                    cur.executemany(_UPDATE_BRIDGE_SQL, legacy_updates)
                if bridge_inserts:
                    cur.executemany(_UPSERT_PARTNER_BRIDGE_SQL, bridge_inserts)
            conn.commit()

    return BridgeStats(
        ll_publishers_seen=len(ll_rows),
        matched=matched,
        unmatched=len(ll_rows) - matched,
        method_counts=method_counts,
    )


# ── Exported helpers for tests ───────────────────────────────────────────────


def score_match(ll_name: str, seller_name: str, domain: str) -> MatchCandidate | None:
    """Test-only convenience wrapper around _score."""
    return _score(ll_name, {
        "publisher_key": domain,
        "domain": domain,
        "seller_name": seller_name,
    })
