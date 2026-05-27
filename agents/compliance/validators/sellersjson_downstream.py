"""
agents/compliance/validators/sellersjson_downstream.py

Phase 3 validator: PGAM's seat in each downstream SSP's sellers.json.

For each SSP in the registry:
  - Fetch their sellers.json
  - Locate the entry with our declared account_id
  - Verify the entry's name/domain identifies PGAM
  - Optionally verify seller_type (most SSPs use INTERMEDIARY for resellers)

Findings carry sentinel publisher_key = f"_ssp:{ssp_key}" so the upsert
machinery handles them uniformly with publisher-scoped findings, but the
publisher scoring query excludes them (they're SSP-level health, not
publisher-level).

Severity rubric:
  sellersjson.downstream_unreachable    HIGH      fetch failed / non-200
  sellersjson.downstream_seat_missing   CRITICAL  account_id absent
  sellersjson.downstream_seat_wrong_id  CRITICAL  present, name/domain not PGAM
  sellersjson.downstream_seat_wrong_type HIGH     present + ours, wrong seller_type
"""
from __future__ import annotations

from agents.compliance.crawlers.downstream_sellersjson import DownstreamFetch
from agents.compliance.ssp_registry import SspExpectation, is_pgam_seller_entry
from agents.compliance.validators.adstxt_universal import Finding


def _sentinel_key(ssp_key: str) -> str:
    return f"_ssp:{ssp_key}"


def _detail(exp: SspExpectation, fetch: DownstreamFetch, **extra) -> dict:
    base = {
        "ssp":             exp.ssp_key,
        "url":             fetch.url,
        "expected_account_id": exp.account_id,
    }
    base.update(extra)
    return base


def validate_downstream_sellersjson(
    exp: SspExpectation,
    fetch: DownstreamFetch,
) -> list[Finding]:
    pub_key = _sentinel_key(exp.ssp_key)

    if not fetch.ok:
        return [Finding.make(
            publisher_key=pub_key,
            check_id="sellersjson.downstream_unreachable",
            severity="high",
            detail=_detail(exp, fetch, http_status=fetch.http_status,
                           error=fetch.error),
        )]

    # Find any seller entry with our account_id.
    matches = [
        s for s in fetch.sellers
        if str(s.get("seller_id") or "") == str(exp.account_id)
    ]

    if not matches:
        return [Finding.make(
            publisher_key=pub_key,
            check_id="sellersjson.downstream_seat_missing",
            severity="critical",
            detail=_detail(exp, fetch, seller_count=fetch.seller_count),
        )]

    # If any match looks like a PGAM entry, accept. Otherwise raise wrong_id.
    pgam_matches = [s for s in matches if is_pgam_seller_entry(s)]
    if not pgam_matches:
        return [Finding.make(
            publisher_key=pub_key,
            check_id="sellersjson.downstream_seat_wrong_id",
            severity="critical",
            detail=_detail(exp, fetch,
                           observed_names=[s.get("name") for s in matches],
                           observed_domains=[s.get("domain") for s in matches],
                           is_confidential=[
                               bool(s.get("is_confidential")) for s in matches
                           ]),
        )]

    # Seller_type check (skip when expectation is None).
    findings: list[Finding] = []
    if exp.expected_downstream_seller_type:
        wanted = exp.expected_downstream_seller_type.upper()
        observed_types = {
            (s.get("seller_type") or "").upper() for s in pgam_matches
        }
        # PGAM-as-BOTH or PGAM-as-wanted both count as "OK". Some SSPs
        # write BOTH when the seat handles direct + reseller flow.
        if not (wanted in observed_types or "BOTH" in observed_types):
            findings.append(Finding.make(
                publisher_key=pub_key,
                check_id="sellersjson.downstream_seat_wrong_type",
                severity="high",
                detail=_detail(exp, fetch,
                               expected=wanted,
                               observed=sorted(observed_types)),
            ))

    return findings
