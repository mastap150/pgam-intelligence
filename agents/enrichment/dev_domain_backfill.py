"""
agents/enrichment/dev_domain_backfill.py

Pre-Phase-5 dev_domain resolver for the compliance audit.

The Phase 5 entity auditor needs `app_metadata.dev_domain` to know
where to fetch each app's app-ads.txt. Without it the bundle is
flagged 'unresolved' and skips line-level validation entirely — and
today that gap covered 23 of 50 top-revenue entities (nearly half the
audit universe).

The existing `app_name_enrichment.py` daily ETL only uses iTunes
Search, which silently misses every Android-only app. This module
fills the gap using the full `play_store_resolver.resolve_bundle()`
cascade (heuristic → iTunes → Play Store scrape → fallback) and is
sourced from LIVE LL stats so today's top bundles are covered THIS
run, not next run.

Run sequence inside the daily compliance pipeline:

   partner_activity
   ↓
   resolve_top_unresolved_bundles()   ← here
   ↓
   Phase 5 entity_audit               ← uses app_metadata.dev_domain

Cost: ~30s for 30 bundles (one HTTP probe + iTunes + maybe Play Store
each). Memory-cheap because we only hold the bundle list + per-
bundle resolver result; no big LL stats payloads.
"""
from __future__ import annotations

from dataclasses import dataclass

from core.api import fetch as ll_fetch, n_days_ago, sf, today
from core.neon import connect

from agents.enrichment.play_store_resolver import resolve_bundle


DEFAULT_TOP_N = 30
DEFAULT_LOOKBACK_DAYS = 7


@dataclass(frozen=True)
class BackfillStats:
    candidates_seen:    int    # bundles missing dev_domain
    attempted:          int    # how many we ran the cascade for
    resolved:           int    # cascade found a dev_domain
    unresolved:         int    # all 4 methods failed


_BUNDLES_WITH_DEV_DOMAIN_SQL = """
SELECT bundle_id
FROM pgam_direct.app_metadata
WHERE dev_domain IS NOT NULL;
"""


def _load_known_dev_domains() -> set[str]:
    """Set of bundle_ids that already have a resolved dev_domain."""
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute(_BUNDLES_WITH_DEV_DOMAIN_SQL)
            return {r[0] for r in cur.fetchall()}


def _pull_top_bundles_live(top_n: int, lookback_days: int) -> list[str]:
    """Top-N bundles by trailing-7d revenue from LL stats.

    BUNDLE-only breakdown — much smaller payload than the
    BUNDLE,PUBLISHER,DEMAND_PARTNER breakdown Phase 5 uses for the
    full audit. Cheaper memory footprint.
    """
    end = today()
    start = n_days_ago(max(lookback_days - 1, 0))
    try:
        rows = ll_fetch("BUNDLE", ["GROSS_REVENUE"], start, end)
    except Exception as exc:
        print(f"[dev_domain_backfill] LL fetch failed: {exc}")
        return []
    # Aggregate per-bundle (LL sometimes splits a bundle across rows).
    agg: dict[str, float] = {}
    for r in rows:
        bundle = (r.get("BUNDLE") or r.get("bundle") or
                  r.get("BUNDLE_NAME") or "").strip()
        if not bundle:
            continue
        rev = sf(r.get("GROSS_REVENUE"))
        if rev <= 0:
            continue
        agg[bundle] = agg.get(bundle, 0.0) + rev
    # Sort by revenue desc, return bundle IDs.
    ranked = sorted(agg.items(), key=lambda kv: -kv[1])
    del rows
    return [b for b, _ in ranked[:top_n]]


_UPSERT_DEV_DOMAIN_SQL = """
INSERT INTO pgam_direct.app_metadata
    (bundle_id, source, last_fetched, fetch_attempts, updated_at,
     dev_domain, dev_url_resolved_at)
VALUES
    (%(bundle_id)s, %(source)s, now(), 1, now(),
     %(dev_domain)s, now())
ON CONFLICT (bundle_id) DO UPDATE SET
    dev_domain          = COALESCE(EXCLUDED.dev_domain,
                                   pgam_direct.app_metadata.dev_domain),
    dev_url_resolved_at = now(),
    source              = EXCLUDED.source,
    last_fetched        = now(),
    fetch_attempts      = pgam_direct.app_metadata.fetch_attempts + 1,
    updated_at          = now();
"""


def _upsert_dev_domain(bundle: str, dev_domain: str, method: str) -> None:
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute(_UPSERT_DEV_DOMAIN_SQL, {
                "bundle_id":  bundle,
                "source":     f"resolver:{method}",
                "dev_domain": dev_domain,
            })
        conn.commit()


def resolve_top_unresolved_bundles(
    top_n: int = DEFAULT_TOP_N,
    lookback_days: int = DEFAULT_LOOKBACK_DAYS,
) -> BackfillStats:
    """End-to-end: pull top bundles → filter to unresolved → resolve.

    Returns a stats record so the runner can log it. Conservative:
    on any partial failure (LL fetch fails, Neon write fails) we
    return what we have rather than raising — the audit can still
    run, just with the existing dev_domain coverage.
    """
    bundles = _pull_top_bundles_live(top_n=top_n, lookback_days=lookback_days)
    if not bundles:
        return BackfillStats(candidates_seen=0, attempted=0,
                             resolved=0, unresolved=0)

    known = _load_known_dev_domains()
    candidates = [b for b in bundles if b not in known]
    if not candidates:
        return BackfillStats(candidates_seen=0, attempted=0,
                             resolved=0, unresolved=0)

    resolved = 0
    unresolved = 0
    attempted = 0
    for bundle in candidates:
        attempted += 1
        try:
            result = resolve_bundle(bundle)
        except Exception as exc:
            print(f"[dev_domain_backfill] resolve failed for {bundle}: {exc}")
            unresolved += 1
            continue
        if result.dev_domain:
            try:
                _upsert_dev_domain(bundle, result.dev_domain, result.method)
                resolved += 1
            except Exception as exc:
                print(f"[dev_domain_backfill] upsert failed for {bundle}: {exc}")
                unresolved += 1
        else:
            unresolved += 1

    return BackfillStats(
        candidates_seen=len(candidates),
        attempted=attempted,
        resolved=resolved,
        unresolved=unresolved,
    )
