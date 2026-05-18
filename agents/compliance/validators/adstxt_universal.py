"""
agents/compliance/validators/adstxt_universal.py

Phase 1 validator: the universal PGAM DIRECT line.

Every publisher we monetize for must declare PGAM as a DIRECT seller in
their ads.txt (or app-ads.txt) with the partner-specific seller_id pulled
from PGAM's sellers.json. Anything else means demand can't legitimately
buy through us for that partner.

Checks produced:
    adstxt.universal_direct_missing       CRITICAL   line absent entirely
    adstxt.universal_direct_wrong_seller  CRITICAL   line present, wrong account_id
    adstxt.universal_direct_wrong_type    CRITICAL   line present, RESELLER not DIRECT
    adstxt.file_unreachable               HIGH       ads.txt 4xx/5xx/network err
    adstxt.file_empty                     HIGH       200 but no parsable lines

Each check carries a stable fingerprint so the upsert keeps a single open
row per (publisher × check × distinct-detail). For most checks the
fingerprint is just the check_id, because a publisher either is or isn't
missing the line — there's no sub-variant to bucket on.
"""
from __future__ import annotations

import hashlib
from dataclasses import dataclass

from agents.compliance.crawlers.adstxt import AdsTxtFetch

# The universal PGAM seller domain. Every partner's ads.txt must contain
# `pgammedia.com, <their seller_id>, DIRECT`.
PGAM_SELLER_DOMAIN = "pgammedia.com"


@dataclass(frozen=True)
class Finding:
    publisher_key: str
    category: str
    check_id: str
    severity: str
    fingerprint: str
    detail: dict

    @staticmethod
    def make(publisher_key: str, check_id: str, severity: str,
             detail: dict, fingerprint_extra: str = "") -> "Finding":
        base = check_id if not fingerprint_extra else f"{check_id}:{fingerprint_extra}"
        fp = hashlib.sha1(base.encode("utf-8")).hexdigest()[:16]
        return Finding(
            publisher_key=publisher_key,
            category=check_id.split(".", 1)[0],
            check_id=check_id,
            severity=severity,
            fingerprint=fp,
            detail=detail,
        )


def validate_universal_direct(
    publisher_key: str,
    expected_seller_id: str,
    fetch: AdsTxtFetch,
) -> list[Finding]:
    """Run the Phase 1 check set against one ads.txt fetch."""
    findings: list[Finding] = []

    # Unreachable / non-200.
    if fetch.error is not None or fetch.http_status != 200:
        findings.append(
            Finding.make(
                publisher_key=publisher_key,
                check_id="adstxt.file_unreachable",
                severity="high",
                detail={
                    "variant": fetch.variant,
                    "url": fetch.url,
                    "http_status": fetch.http_status,
                    "error": fetch.error,
                },
            )
        )
        return findings

    # Reachable but empty.
    if not fetch.lines:
        findings.append(
            Finding.make(
                publisher_key=publisher_key,
                check_id="adstxt.file_empty",
                severity="high",
                detail={
                    "variant": fetch.variant,
                    "url": fetch.url,
                    "body_sha256": fetch.body_sha256,
                },
            )
        )
        return findings

    # Look for any line matching pgammedia.com.
    pgam_lines = [ln for ln in fetch.lines if ln.domain == PGAM_SELLER_DOMAIN]

    if not pgam_lines:
        findings.append(
            Finding.make(
                publisher_key=publisher_key,
                check_id="adstxt.universal_direct_missing",
                severity="critical",
                detail={
                    "variant": fetch.variant,
                    "url": fetch.url,
                    "expected_line": (
                        f"{PGAM_SELLER_DOMAIN}, {expected_seller_id}, DIRECT"
                    ),
                },
            )
        )
        return findings

    # We have one+ pgammedia.com lines. Check seller_id + relationship.
    matching_id = [ln for ln in pgam_lines if ln.account_id == expected_seller_id]

    if not matching_id:
        # Wrong seller_id: PGAM is listed but with a different account.
        observed = sorted({ln.account_id for ln in pgam_lines})
        findings.append(
            Finding.make(
                publisher_key=publisher_key,
                check_id="adstxt.universal_direct_wrong_seller",
                severity="critical",
                detail={
                    "variant": fetch.variant,
                    "url": fetch.url,
                    "expected_seller_id": expected_seller_id,
                    "observed_seller_ids": observed,
                },
            )
        )
        return findings

    # Right seller_id — verify at least one says DIRECT.
    has_direct = any(ln.relationship == "DIRECT" for ln in matching_id)
    if not has_direct:
        observed_rels = sorted({ln.relationship for ln in matching_id})
        findings.append(
            Finding.make(
                publisher_key=publisher_key,
                check_id="adstxt.universal_direct_wrong_type",
                severity="critical",
                detail={
                    "variant": fetch.variant,
                    "url": fetch.url,
                    "seller_id": expected_seller_id,
                    "observed_relationships": observed_rels,
                    "expected": "DIRECT",
                },
            )
        )

    return findings
