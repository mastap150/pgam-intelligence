"""
agents/compliance/supply_partner_audit.py

Daily audit of each LL supply partner's sellers.json.

Sibling to the demand-side Phase 3 audit (`validators/sellersjson_downstream.py`)
but pointed at the OTHER side of the supply chain: for each LL supply
partner (Smaato, BidMachine, Algorix, Start.IO, …) that brings us
inventory, verify their `<domain>/sellers.json` declares our PGAM
seat correctly. Without this, downstream DSPs auditing the supply
chain from the demand side will see PGAM as an undeclared reseller
of the supply partner — bids get blocked even if our own ads.txt is
clean.

Findings produced (sentinel publisher_key `_supply:<domain>`):

  sellersjson.supply_partner_unreachable    HIGH
    Fetch failed / non-200 / not JSON.

  sellersjson.supply_partner_seat_missing   CRITICAL
    Our PGAM seller_id (the one we have for them in our own sellers.json)
    isn't present in their sellers.json. They don't recognize us as a
    reseller — bids will be filtered.

  sellersjson.supply_partner_seat_wrong_id  CRITICAL
    Seat ID present but the entry's name/domain doesn't identify PGAM
    (someone else has that seat ID with them).

  sellersjson.supply_partner_seat_wrong_type HIGH
    Seat present and is ours, but seller_type doesn't match
    INTERMEDIARY (most supply partners declare downstream resellers
    as INTERMEDIARY; some use PUBLISHER for direct-only seats).

Sentinel key + auto-resolve: `_supply:<domain>` is added to the
resolvable set so cleared findings flip to resolved on the next run.
"""
from __future__ import annotations

import hashlib
from dataclasses import dataclass, field

import requests

from agents.compliance.ssp_registry import is_pgam_seller_entry
from agents.compliance.validators.adstxt_universal import Finding


HTTP_TIMEOUT_SEC = 60
USER_AGENT = "pgam-intelligence/compliance (+https://pgammedia.com)"


@dataclass(frozen=True)
class SupplyPartner:
    """A single LL supply partner we want to audit."""
    publisher_key: str            # compliance_publishers.publisher_key (== domain)
    domain:        str            # e.g. 'smaato.com'
    pgam_seat:     str            # the seat ID we have for them
    pgam_seat_type: str           # what we declare them as in OUR sellers.json
    ll_publisher_names: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class SupplyPartnerFetch:
    publisher_key: str
    url:           str
    http_status:   int | None
    body_sha256:   str | None
    seller_count:  int | None
    sellers:       list[dict]
    error:         str | None

    @property
    def ok(self) -> bool:
        return self.http_status == 200 and bool(self.sellers)


_SUPPLY_PARTNER_SQL = """
-- All LL supply partners we know about: INTERMEDIARY/BOTH entries in
-- our sellers.json that are bridged to one or more active LL
-- publisher IDs (i.e. they actually bring us inventory).
SELECT
    cp.publisher_key,
    cp.domain,
    cp.seller_id      AS pgam_seat,
    cp.seller_type    AS pgam_seat_type,
    ARRAY_AGG(DISTINCT b.ll_publisher_name)
        FILTER (WHERE b.ll_publisher_name IS NOT NULL) AS ll_names
FROM pgam_direct.compliance_publishers cp
JOIN pgam_direct.compliance_ll_partner_bridge b
  ON b.publisher_key = cp.publisher_key
WHERE cp.is_active = TRUE
  AND cp.seller_type IN ('INTERMEDIARY', 'BOTH')
  AND cp.domain IS NOT NULL
GROUP BY cp.publisher_key, cp.domain, cp.seller_id, cp.seller_type
ORDER BY cp.publisher_key;
"""


def load_supply_partners() -> list[SupplyPartner]:
    """Pull the supply-partner audit roster from Neon."""
    from core.neon import connect
    out: list[SupplyPartner] = []
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute(_SUPPLY_PARTNER_SQL)
            for pub_key, domain, seat, seat_type, ll_names in cur.fetchall():
                out.append(SupplyPartner(
                    publisher_key=pub_key,
                    domain=domain,
                    pgam_seat=seat,
                    pgam_seat_type=(seat_type or "").upper(),
                    ll_publisher_names=list(ll_names or []),
                ))
    return out


def _sellers_json_url(domain: str) -> str:
    return f"https://{domain}/sellers.json"


def fetch_supply_partner(p: SupplyPartner) -> SupplyPartnerFetch:
    """Fetch one supply partner's sellers.json. Returns a parsed dict-shaped
    result; never raises — network errors land in `error`."""
    url = _sellers_json_url(p.domain)
    try:
        resp = requests.get(
            url,
            headers={"User-Agent": USER_AGENT, "Accept": "application/json,*/*"},
            timeout=HTTP_TIMEOUT_SEC,
        )
    except requests.RequestException as exc:
        return SupplyPartnerFetch(
            publisher_key=p.publisher_key, url=url, http_status=None,
            body_sha256=None, seller_count=None, sellers=[], error=str(exc),
        )

    if resp.status_code != 200:
        return SupplyPartnerFetch(
            publisher_key=p.publisher_key, url=url, http_status=resp.status_code,
            body_sha256=None, seller_count=None, sellers=[],
            error=f"HTTP {resp.status_code}",
        )

    body = resp.content
    sha = hashlib.sha256(body).hexdigest()[:16]
    try:
        payload = resp.json()
    except ValueError as exc:
        return SupplyPartnerFetch(
            publisher_key=p.publisher_key, url=url, http_status=200,
            body_sha256=sha, seller_count=None, sellers=[],
            error=f"json decode: {exc}",
        )

    sellers = payload.get("sellers") if isinstance(payload, dict) else None
    if not isinstance(sellers, list):
        return SupplyPartnerFetch(
            publisher_key=p.publisher_key, url=url, http_status=200,
            body_sha256=sha, seller_count=0, sellers=[],
            error="payload missing 'sellers' array",
        )

    return SupplyPartnerFetch(
        publisher_key=p.publisher_key, url=url, http_status=200,
        body_sha256=sha, seller_count=len(sellers), sellers=sellers,
        error=None,
    )


def _sentinel_key(domain: str) -> str:
    return f"_supply:{domain}"


def validate_supply_partner(
    p: SupplyPartner,
    fetch: SupplyPartnerFetch,
) -> list[Finding]:
    """Check that the supply partner's sellers.json declares PGAM.

    Identity is name/domain-based, NOT seat-ID-based: the partner
    assigns us their OWN account_id in their sellers.json (a value
    they pick, unrelated to the seat ID we store for them in our own
    sellers.json). We can only match by name/domain (`is_pgam_seller_entry`
    looks for 'pgam' / 'pgammedia' / 'pgamssp' tokens).
    """
    pub_key = _sentinel_key(p.domain)

    if not fetch.ok:
        return [Finding.make(
            publisher_key=pub_key,
            check_id="sellersjson.supply_partner_unreachable",
            severity="high",
            detail={
                "supply_partner":   p.domain,
                "url":              fetch.url,
                "http_status":      fetch.http_status,
                "error":            fetch.error,
                "ll_publishers":    p.ll_publisher_names[:6],
            },
        )]

    pgam_matches = [s for s in fetch.sellers if is_pgam_seller_entry(s)]

    if not pgam_matches:
        return [Finding.make(
            publisher_key=pub_key,
            check_id="sellersjson.supply_partner_pgam_not_listed",
            severity="critical",
            detail={
                "supply_partner":   p.domain,
                "url":              fetch.url,
                "seller_count":     fetch.seller_count,
                "ll_publishers":    p.ll_publisher_names[:6],
                "consequence": (
                    f"{p.domain}'s sellers.json doesn't list PGAM as a seller "
                    "(no entry with name/domain identifying us). DSPs auditing "
                    "the supply chain from the demand side will treat "
                    f"PGAM-routed bids from {p.domain} as unauthorized."
                ),
            },
        )]

    # PGAM is listed — verify it's marked as a reseller-shaped entry
    # (INTERMEDIARY most commonly; PUBLISHER if we hold a direct seat
    # with them; BOTH if both flows). Anything else is suspicious.
    observed_types = {
        (s.get("seller_type") or "").upper() for s in pgam_matches
    }
    observed_ids = [str(s.get("seller_id") or "") for s in pgam_matches]
    acceptable = {"PUBLISHER", "INTERMEDIARY", "BOTH"}
    findings: list[Finding] = []
    if not (observed_types & acceptable):
        findings.append(Finding.make(
            publisher_key=pub_key,
            check_id="sellersjson.supply_partner_pgam_wrong_type",
            severity="high",
            detail={
                "supply_partner":   p.domain,
                "url":              fetch.url,
                "expected_one_of":  sorted(acceptable),
                "observed":         sorted(observed_types),
                "observed_seat_ids": observed_ids[:3],
            },
        ))
    return findings


@dataclass(frozen=True)
class SupplyPartnerAuditResult:
    partners_audited:   int
    findings:           list[Finding]
    sentinel_keys:      list[str]
    fetch_metadata:     list[SupplyPartnerFetch]


def run_supply_partner_audit() -> SupplyPartnerAuditResult:
    """End-to-end audit. Used by the runner; returns findings +
    sentinel keys so the resolve_cleared pipeline can pick them up."""
    partners = load_supply_partners()
    all_findings: list[Finding] = []
    sentinel_keys: list[str] = []
    fetches: list[SupplyPartnerFetch] = []
    for p in partners:
        sentinel_keys.append(_sentinel_key(p.domain))
        try:
            f = fetch_supply_partner(p)
        except Exception as exc:
            # Defensive — should never raise.
            print(f"[supply_partner_audit] unexpected error for "
                  f"{p.domain}: {exc}")
            continue
        fetches.append(f)
        all_findings.extend(validate_supply_partner(p, f))
    return SupplyPartnerAuditResult(
        partners_audited=len(partners),
        findings=all_findings,
        sentinel_keys=sentinel_keys,
        fetch_metadata=fetches,
    )
