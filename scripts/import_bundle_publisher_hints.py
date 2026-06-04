"""
scripts/import_bundle_publisher_hints.py

Bulk-import bundle → publisher_domain mappings from an LL supply CSV
into pgam_direct.app_metadata.dev_domain.

The LL export has four columns: DOMAIN, BUNDLE, BID_REQUESTS,
GROSS_REVENUE. The DOMAIN column is a *mixed* signal — for some rows
it's the actual app publisher's website (com.fugo.wow → fugo.com.tr,
legitimate); for many it's an SSP / ad-mediation network (e.g.
applovin.com attached to 9300+ unrelated bundles).

This ingester filters out the noise and UPSERTs the legitimate
publisher-domain mappings, so entity_universe._APP_METADATA_SQL picks
them up on the next daily cron without further changes.

Filters applied (any one disqualifies a row):

  1. domain ∈ compliance_publishers.domain  — that's our LL supply
     partner list (smaato.com, algorix.co, admanmedia.com, …), all
     definitionally ad-side hosts not publishers.
  2. domain ∈ ssp_registry.ads_txt_domain — demand SSPs (rubicon,
     pubmatic, …), same story.
  3. domain has ≥ MAX_BUNDLES_PER_DOMAIN bundles attached in this CSV.
     A real publisher rarely owns more than ~80–100 apps; anything
     well past that is almost certainly an ad network.
  4. domain matches a junk pattern (`about:*`, `*.blogspot.com`,
     `domainname.com` literal, bare `www.` host with no TLD, …).
  5. domain is on the explicit MANUAL_AD_NETWORK_DENYLIST below for
     networks not yet in the registries (applovin, vungle, unity, …).

Domain values are normalized: lowercased, `www.` prefix dropped,
trailing slashes/paths removed. That dedups e.g. `voodoo.io` and
`www.voodoo.io` into one resolver entry.

Usage:
  python -m scripts.import_bundle_publisher_hints --csv <path> [--dry-run]

Dry-run prints the filter stats and a sample of accepted rows without
touching Neon. Live mode UPSERTs with source='ll_supply_csv'.
"""
from __future__ import annotations

import argparse
import csv
import sys
from collections import Counter, defaultdict
from datetime import date
from pathlib import Path

from core.neon import connect


# Tuneable. The 100-499 band on the user-supplied CSV is a mix of real
# game studios (Voodoo, Kwalee, Crazy Labs, Easybrain, …) and a handful
# of networks (Pangle, Mars Media, Supersonic). Anything ≥ 100 needs
# manual review; ≥ 500 is unambiguously a network. We default to the
# loose threshold (500) and rely on the denylist for the borderline
# band — set this lower if you want to be aggressive.
MAX_BUNDLES_PER_DOMAIN = 500


# Networks / SDKs / mediation hosts that aren't in compliance_publishers
# or ssp_registry but still appear attached to many bundles in LL exports.
# Add to this list when a new export surfaces a new vendor.
MANUAL_AD_NETWORK_DENYLIST: set[str] = {
    "applovin.com",
    "liftoff.io",
    "vungle.com",
    "bigo.sg",
    "unity.com",
    "yandex.com",
    "ironsrc.com",
    "toponad.com",
    "lacunads.com",
    "nexxen.com",
    "appodeal.com",
    "loopme.com",
    "pangleglobal.com",
    "mars.media",
    "supersonic.com",
    "anzu.io",
    "pubrevplus.com",
    "ignitemediatech.com",
    "ysocorp.com",        # Yso Corp — Russian SDK aggregator
    "say.games",          # Say Games — publisher BUT bundles span subsidiaries; treat as network
    "dauup.com",
    "aigames.ae",
    "domainname.com",     # placeholder / parking
    "app-stock.com",      # supply aggregator
    "pgammedia.com",      # our own — already covered as pgam_direct
    "pgamssp.com",        # our SSP host, never a publisher
}


def _is_junk_domain(d: str) -> bool:
    if not d:
        return True
    if d.startswith("about:"):
        return True
    if "blogspot.com" in d:        # parking on blogspot
        return True
    if d.endswith(".web.app/") or d.endswith(".web.app"):
        return True               # firebase staging URLs
    if "." not in d:               # no TLD at all
        return True
    if d in {"www", "localhost"}:
        return True
    return False


def _normalize_domain(raw: str) -> str:
    d = (raw or "").strip().lower()
    if not d:
        return ""
    if d.startswith("http://"):
        d = d[7:]
    elif d.startswith("https://"):
        d = d[8:]
    # Drop query string before path — some CSV rows look like
    # `hungrystudio.com?utm_source=googleplay…` and we want the bare
    # host. Same idea for fragments. Path stripping comes after so
    # that order doesn't matter.
    for sep in ("?", "#", "/"):
        if sep in d:
            d = d.split(sep, 1)[0]
    if d.startswith("www."):
        d = d[4:]
    return d


def _looks_like_bundle_id(d: str) -> bool:
    """The CSV occasionally has the bundle_id repeated in the DOMAIN
    column (LL artifact). e.g. `com.block.juggle,com.block.juggle,0,0`.
    Drop those — Android bundle IDs have dots but they're not hosts."""
    if not d:
        return False
    # Numeric-only with no dots is iOS App Store ID — definitely not a host.
    if d.isdigit():
        return True
    # Heuristic for Android bundles: starts with com./io./net./org./app./
    # AND has no slash and at least 3 segments. Real domains rarely fit
    # all three. (`com.example.com` is technically valid but vanishingly
    # rare in this data; the picker would still pick the higher-volume
    # real publisher entry if it exists.)
    prefixes = ("com.", "io.", "net.", "org.", "app.", "co.", "me.")
    if d.startswith(prefixes) and d.count(".") >= 2:
        return True
    return False


def _load_registry_denylist() -> set[str]:
    """Pull supply-partner and SSP domains out of Neon as the seed
    denylist. These are *definitionally* not publisher domains."""
    out: set[str] = set()
    with connect() as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT domain FROM pgam_direct.compliance_publishers "
            "WHERE domain IS NOT NULL"
        )
        out.update(_normalize_domain(r[0]) for r in cur.fetchall())
    # SSP registry is Python code — import lazily to avoid a hard dep
    # if someone runs this offline.
    try:
        from agents.compliance.ssp_registry import PHASE_2_SSP_EXPECTATIONS
        for exp in PHASE_2_SSP_EXPECTATIONS:
            d = getattr(exp, "ads_txt_domain", None)
            if d:
                out.add(_normalize_domain(d))
    except Exception as exc:
        print(f"[hints] ssp_registry import failed (continuing): {exc}",
              file=sys.stderr)
    return {d for d in out if d}


# Sources we treat as authoritative — those WON'T be overwritten by the
# CSV import. Anything else (NULL, 'unknown', 'resolver:heuristic') is
# low-confidence and gets replaced when the CSV has a real mapping with
# bid-volume signal behind it. The original "preserve everything" rule
# was too conservative — a stale 'unknown' guess like
# `com.block.juggle → block.com` (naive bundle-prefix heuristic) was
# preventing the correct `hungrystudio.com` mapping from landing.
_AUTHORITATIVE_SOURCES = ("itunes", "resolver:itunes", "ll_supply_csv")


_UPSERT_SQL = """
INSERT INTO pgam_direct.app_metadata
    (bundle_id, dev_domain, source, last_fetched, fetch_attempts,
     updated_at, dev_url_resolved_at)
VALUES
    (%(bundle_id)s, %(dev_domain)s, %(source)s, now(), 1,
     now(), now())
ON CONFLICT (bundle_id) DO UPDATE SET
    -- Overwrite dev_domain if the existing row's source is NOT in the
    -- authoritative list. App Store / Play Store lookups stay; NULL
    -- and low-confidence guesses get replaced.
    dev_domain          = CASE
        WHEN pgam_direct.app_metadata.source = ANY(%(authoritative)s)
        THEN pgam_direct.app_metadata.dev_domain
        ELSE EXCLUDED.dev_domain
    END,
    source              = CASE
        WHEN pgam_direct.app_metadata.source = ANY(%(authoritative)s)
        THEN pgam_direct.app_metadata.source
        ELSE EXCLUDED.source
    END,
    dev_url_resolved_at = CASE
        WHEN pgam_direct.app_metadata.source = ANY(%(authoritative)s)
        THEN pgam_direct.app_metadata.dev_url_resolved_at
        ELSE EXCLUDED.dev_url_resolved_at
    END,
    updated_at          = now();
"""


def run(csv_path: Path, dry_run: bool) -> dict:
    # ── Build denylist & per-domain bundle counts in one pass ────────
    registry_deny = _load_registry_denylist()
    full_deny = registry_deny | {_normalize_domain(d) for d in MANUAL_AD_NETWORK_DENYLIST}

    # Pass 1: count bundles per (normalized) domain so we can apply
    # MAX_BUNDLES_PER_DOMAIN below.
    counts: Counter = Counter()
    with csv_path.open() as f:
        reader = csv.DictReader(f)
        for row in reader:
            dom = _normalize_domain(row.get("DOMAIN", ""))
            bundle = (row.get("BUNDLE") or "").strip()
            if dom and bundle:
                counts[dom] += 1

    # Pass 2: emit acceptable (bundle, domain) pairs.
    # When multiple non-denylisted domains appear for the same bundle,
    # pick the one with the highest BID_REQUESTS. This matters because
    # the CSV often has ~12 rows per bundle (one per attribution path):
    # ad-network entries get filtered by the denylist, but several real
    # publisher candidates can survive (e.g. `www.hungrystudio.com` AND
    # `hungrystudio.com?utm_source=…` for the same bundle). The row
    # with the most bid volume is overwhelmingly the canonical
    # publisher — for com.block.juggle, that's www.hungrystudio.com at
    # 256M bids vs the ad-network noise rows at <1M each.
    candidates: dict[str, dict[str, int]] = {}  # bundle → {domain: bid_requests}
    reasons = Counter()
    with csv_path.open() as f:
        reader = csv.DictReader(f)
        for row in reader:
            dom_raw = (row.get("DOMAIN") or "").strip()
            bundle = (row.get("BUNDLE") or "").strip()
            if not bundle:
                reasons["no_bundle"] += 1
                continue
            if not dom_raw:
                reasons["no_domain"] += 1
                continue
            dom = _normalize_domain(dom_raw)
            if _is_junk_domain(dom):
                reasons["junk_pattern"] += 1
                continue
            if _looks_like_bundle_id(dom):
                reasons["bundle_id_in_domain_column"] += 1
                continue
            if dom in full_deny:
                reasons["denylisted_registry_or_manual"] += 1
                continue
            if counts[dom] >= MAX_BUNDLES_PER_DOMAIN:
                reasons["over_bundle_threshold"] += 1
                continue
            try:
                bid = int(row.get("BID_REQUESTS") or 0)
            except ValueError:
                bid = 0
            candidates.setdefault(bundle, {})
            # If the same (bundle, normalized-domain) appears twice
            # (e.g. `www.X` and `X` both normalize to `X`), sum volumes
            # so the picker doesn't fragment the signal.
            candidates[bundle][dom] = candidates[bundle].get(dom, 0) + bid

    # For each bundle, pick the domain with the highest summed volume.
    accepted: dict[str, str] = {}
    sample: list[tuple[str, str, int]] = []
    for bundle, doms in candidates.items():
        if len(doms) > 1:
            reasons["bundle_domain_conflict_resolved_by_volume"] += 1
        winner_dom, winner_vol = max(doms.items(), key=lambda kv: kv[1])
        accepted[bundle] = winner_dom
        if len(sample) < 10:
            sample.append((bundle, winner_dom, winner_vol))

    print(f"[hints] CSV rows scanned: {sum(reasons.values()) + len(accepted):,}")
    print(f"[hints] Accepted (bundle → publisher domain) pairs: {len(accepted):,}")
    print(f"[hints] Unique publisher domains covered: "
          f"{len(set(accepted.values())):,}")
    print(f"[hints] Denylist counts:")
    for k, v in sorted(reasons.items(), key=lambda x: -x[1]):
        print(f"   {k:35s} {v:,}")
    print(f"[hints] Registry denylist size: {len(registry_deny):,} "
          f"+ manual {len(MANUAL_AD_NETWORK_DENYLIST):,}")
    print(f"[hints] Sample accepted pairs (with winning-domain bid volume):")
    for b, d, vol in sample:
        print(f"   {b:50s} → {d}  ({vol:,} bids)")

    if dry_run:
        print("[hints] dry-run: no writes performed.")
        return {"accepted": len(accepted), "dry_run": True}

    # Live UPSERT — batched.
    written = 0
    skipped = 0
    with connect() as conn, conn.cursor() as cur:
        for bundle, dom in accepted.items():
            try:
                cur.execute(_UPSERT_SQL, {
                    "bundle_id":     bundle,
                    "dev_domain":    dom,
                    "source":        "ll_supply_csv",
                    "authoritative": list(_AUTHORITATIVE_SOURCES),
                })
                written += 1
            except Exception as exc:
                skipped += 1
                if skipped <= 5:
                    print(f"[hints] upsert failed for {bundle}: {exc}",
                          file=sys.stderr)
            if written % 1000 == 0 and written > 0:
                conn.commit()
                print(f"[hints]   committed {written:,}…")
        conn.commit()

    print(f"[hints] WROTE: {written:,} rows (skipped {skipped})")
    return {"accepted": len(accepted), "written": written, "skipped": skipped}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", required=True, help="LL supply CSV path")
    ap.add_argument("--dry-run", action="store_true",
                    help="Print stats but don't write to Neon")
    args = ap.parse_args()

    csv_path = Path(args.csv).expanduser()
    if not csv_path.exists():
        print(f"CSV not found: {csv_path}", file=sys.stderr)
        return 1

    run(csv_path, args.dry_run)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
