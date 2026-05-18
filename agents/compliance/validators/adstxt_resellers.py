"""
agents/compliance/validators/adstxt_resellers.py

Phase 2 validator: conditional RESELLER-line presence.

For each publisher × SSP pair where the SSP is actively monetizing the
publisher's inventory through PGAM (observed_monetization row exists),
check that the publisher's ads.txt contains the canonical
`<ssp_domain>, <account_id>, RESELLER[, <cert>]` line from
agents/compliance/ssp_registry.

Checks produced:
    adstxt.reseller_missing            CRITICAL   line absent — unauthorized monetization path
    adstxt.reseller_wrong_seller       CRITICAL   line present, but different account_id
    adstxt.reseller_wrong_type         HIGH       account_id matches but relationship != RESELLER
    adstxt.reseller_cert_mismatch      MEDIUM     account & type match, cert authority differs

Fingerprinting includes ssp_key so a publisher missing both rubicon and
pubmatic lines surfaces as two independent findings (and resolves
independently as each is fixed).
"""
from __future__ import annotations

from agents.compliance.crawlers.adstxt import AdsTxtFetch
from agents.compliance.observed_monetization import ObservedRow
from agents.compliance.ssp_registry import SspExpectation, get_expectation
from agents.compliance.validators.adstxt_universal import Finding


def _detail(fetch: AdsTxtFetch, exp: SspExpectation, **extra) -> dict:
    base = {
        "variant":        fetch.variant,
        "url":            fetch.url,
        "ssp":            exp.ssp_key,
        "expected_line":  _format_expected_line(exp),
    }
    base.update(extra)
    return base


def _format_expected_line(exp: SspExpectation) -> str:
    parts = [exp.ads_txt_domain, exp.account_id, exp.relationship]
    if exp.cert_authority:
        parts.append(exp.cert_authority)
    return ", ".join(parts)


def validate_resellers_for_publisher(
    publisher_key: str,
    fetch: AdsTxtFetch,
    observed: list[ObservedRow],
) -> list[Finding]:
    """Run reseller checks for one publisher.

    Skips entirely if the fetch wasn't a 200 — the universal validator
    will already have raised a file_unreachable/empty finding, and we
    can't usefully reason about reseller lines without parsed content.
    """
    if fetch.http_status != 200 or not fetch.lines:
        return []

    findings: list[Finding] = []

    for obs in observed:
        exp = get_expectation(obs.ssp_key)
        if exp is None:
            continue  # registry drift; would only happen mid-deploy

        ssp_lines = [
            ln for ln in fetch.lines
            if ln.domain == exp.ads_txt_domain.lower()
        ]

        if not ssp_lines:
            findings.append(Finding.make(
                publisher_key=publisher_key,
                check_id="adstxt.reseller_missing",
                severity="critical",
                detail=_detail(fetch, exp,
                               observed_revenue_usd=obs.revenue_usd,
                               observed_demand_names=list(obs.demand_names)),
                fingerprint_extra=exp.ssp_key,
            ))
            continue

        matching_id = [ln for ln in ssp_lines if ln.account_id == exp.account_id]

        if not matching_id:
            findings.append(Finding.make(
                publisher_key=publisher_key,
                check_id="adstxt.reseller_wrong_seller",
                severity="critical",
                detail=_detail(fetch, exp,
                               expected_account_id=exp.account_id,
                               observed_account_ids=sorted({
                                   ln.account_id for ln in ssp_lines
                               })),
                fingerprint_extra=exp.ssp_key,
            ))
            continue

        wanted_rel = exp.relationship.upper()
        rel_match = [ln for ln in matching_id if ln.relationship == wanted_rel]

        if not rel_match:
            findings.append(Finding.make(
                publisher_key=publisher_key,
                check_id="adstxt.reseller_wrong_type",
                severity="high",
                detail=_detail(fetch, exp,
                               observed_relationships=sorted({
                                   ln.relationship for ln in matching_id
                               }),
                               expected=wanted_rel),
                fingerprint_extra=exp.ssp_key,
            ))
            continue

        # Optional: cert authority. Only flag if the expectation specifies one
        # AND none of the matching lines carries it. Cert is column 4 and
        # may be omitted by the publisher even when we declare one — many
        # legitimate ads.txt files do this, so this is medium not high.
        if exp.cert_authority:
            observed_certs = {(ln.cert_authority or "").lower() for ln in rel_match}
            if exp.cert_authority.lower() not in observed_certs:
                findings.append(Finding.make(
                    publisher_key=publisher_key,
                    check_id="adstxt.reseller_cert_mismatch",
                    severity="medium",
                    detail=_detail(fetch, exp,
                                   expected_cert=exp.cert_authority,
                                   observed_certs=sorted(observed_certs)),
                    fingerprint_extra=exp.ssp_key,
                ))

    return findings
