"""
agents/compliance/ssp_registry.py

Static catalogue of downstream SSPs whose RESELLER lines must appear on
a publisher's ads.txt whenever that SSP is actively monetizing the
publisher through PGAM.

Two things live here:

  1. The canonical (domain, account_id, relationship, cert) tuple that
     must appear verbatim in the partner's ads.txt.
  2. The list of substrings used to classify an LL demand_name to an
     SSP. LL demand names are operator-edited free text — Rubicon may
     appear as "Rubicon", "Magnite", "Magnite (Rubicon)", "RUBICON".
     Case-insensitive substring matching catches all of these.

To add a new required line: append to PHASE_2_SSP_EXPECTATIONS. To
expand demand-name coverage for an existing SSP: edit its
demand_name_patterns tuple.

Source of truth for the Phase 2 list = the universal ads.txt contract
Priyesh provided in the compliance-agent kickoff (May 2026).
"""
from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class SspExpectation:
    ssp_key: str                               # stable identifier
    ads_txt_domain: str                        # what appears in column 1 of ads.txt
    account_id: str                            # column 2
    relationship: str                          # column 3 — always RESELLER for this phase
    cert_authority: str | None                 # column 4 (optional per spec)
    demand_name_patterns: tuple[str, ...] = field(default_factory=tuple)

    def matches_demand_name(self, name: str) -> bool:
        if not name:
            return False
        lower = name.lower()
        return any(p in lower for p in self.demand_name_patterns)


PHASE_2_SSP_EXPECTATIONS: tuple[SspExpectation, ...] = (
    SspExpectation(
        ssp_key="rubicon",
        ads_txt_domain="rubiconproject.com",
        account_id="24852",
        relationship="RESELLER",
        cert_authority="0bfd66d529a55807",
        demand_name_patterns=("rubicon", "magnite"),
    ),
    SspExpectation(
        ssp_key="pubmatic",
        ads_txt_domain="pubmatic.com",
        account_id="165708",
        relationship="RESELLER",
        cert_authority="5d62403b186f2ace",
        demand_name_patterns=("pubmatic",),
    ),
    SspExpectation(
        ssp_key="unruly",
        ads_txt_domain="video.unrulymedia.com",
        account_id="5921144960123684292",
        relationship="RESELLER",
        cert_authority=None,
        demand_name_patterns=("unruly", "tremor"),
    ),
    SspExpectation(
        ssp_key="zeta",
        ads_txt_domain="zetaglobal.net",
        account_id="748",
        relationship="RESELLER",
        cert_authority=None,
        demand_name_patterns=("zeta",),
    ),
    SspExpectation(
        ssp_key="loopme",
        ads_txt_domain="loopme.com",
        account_id="19940",
        relationship="RESELLER",
        cert_authority="6c8d5f95897a5a3b",
        demand_name_patterns=("loopme",),
    ),
    SspExpectation(
        ssp_key="sovrn",
        ads_txt_domain="lijit.com",
        account_id="402418",
        relationship="RESELLER",
        cert_authority="fafdf38b16bf6b2b",
        demand_name_patterns=("sovrn", "lijit"),
    ),
    SspExpectation(
        ssp_key="triplelift",
        ads_txt_domain="triplelift.com",
        account_id="14680",
        relationship="RESELLER",
        cert_authority="6c33edb13117fd86",
        demand_name_patterns=("triplelift", "triple lift"),
    ),
    SspExpectation(
        ssp_key="sharethrough",
        ads_txt_domain="sharethrough.com",
        account_id="VQlYJeXR",
        relationship="RESELLER",
        cert_authority="d53b998a7bd4ecd2",
        demand_name_patterns=("sharethrough",),
    ),
    SspExpectation(
        ssp_key="appnexus",
        ads_txt_domain="appnexus.com",
        account_id="8106",
        relationship="RESELLER",
        cert_authority=None,
        demand_name_patterns=("appnexus", "xandr", "microsoft advertising"),
    ),
    SspExpectation(
        ssp_key="smaato",
        ads_txt_domain="smaato.com",
        account_id="1100058906",
        relationship="RESELLER",
        cert_authority="07bcf65f187117b4",
        demand_name_patterns=("smaato",),
    ),
)


_BY_KEY: dict[str, SspExpectation] = {e.ssp_key: e for e in PHASE_2_SSP_EXPECTATIONS}


def get_expectation(ssp_key: str) -> SspExpectation | None:
    return _BY_KEY.get(ssp_key)


def classify_demand_name(demand_name: str) -> SspExpectation | None:
    """Return the SSP whose demand-name pattern matches, or None.

    Match order is registry order — first hit wins. Patterns are
    deliberately specific enough that overlap is rare; if it ever
    happens, reorder PHASE_2_SSP_EXPECTATIONS.
    """
    for exp in PHASE_2_SSP_EXPECTATIONS:
        if exp.matches_demand_name(demand_name):
            return exp
    return None
