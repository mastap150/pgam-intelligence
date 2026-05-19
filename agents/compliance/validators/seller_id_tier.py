"""
agents/compliance/validators/seller_id_tier.py

Tiered PGAM DIRECT-line check (Phase 5).

Replaces Phase 1's binary "wrong_seller" critical-or-pass with a tiered
verdict that distinguishes between three different ways a partner can
have the wrong seller_id on their ads.txt:

  1. Pass             — exact match on expected seller_id, type DIRECT
  2. wrong_type       — seller_id matches, type isn't DIRECT (e.g. RESELLER)
                         → CRITICAL  (downstream demand will be wrong)
  3. wrong_seat_known — seller_id is in PGAM sellers.json BUT not this
                         partner's seat — typically an Aditude / prebid-
                         wrapper INTERMEDIARY seat being used as DIRECT
                         → HIGH      (PGAM still gets monetized, but the
                                       partner isn't declaring their own
                                       seat; supply-path transparency hit)
  4. wrong_seat_unknown — seller_id NOT in PGAM sellers.json at all
                          → CRITICAL  (unauthorized seat OR seller_id has
                                        been rotated out — investigate)
  5. missing           — no pgamssp.com line in ads.txt at all
                          → CRITICAL

Inputs:
  - expected_seller_id: the partner's own seller_id from PGAM sellers.json
  - fetch:              the parsed AdsTxtFetch
  - pgam_seat_registry: map seller_id → {seller_type, name, domain} for the
                        ENTIRE PGAM sellers.json (not just publishers).
                        Lets us tell INTERMEDIARY (Aditude) from PUBLISHER
                        (other partner's seat) when classifying observed
                        seats.
"""
from __future__ import annotations

from agents.compliance.crawlers.adstxt import AdsTxtFetch
from agents.compliance.validators.adstxt_universal import (
    Finding,
    PGAM_SELLER_DOMAIN,
)


def validate_universal_direct_tiered(
    publisher_key: str,
    expected_seller_id: str | None,
    fetch: AdsTxtFetch,
    pgam_seat_registry: dict[str, dict],
) -> list[Finding]:
    """Tiered check (replaces validate_universal_direct's binary pass/fail)."""
    findings: list[Finding] = []

    # Pre-checks: reuse the same unreachable / empty findings from Phase 1.
    if fetch.error is not None or fetch.http_status != 200:
        findings.append(Finding.make(
            publisher_key=publisher_key,
            check_id="adstxt.file_unreachable",
            severity="high",
            detail={"variant": fetch.variant, "url": fetch.url,
                    "http_status": fetch.http_status, "error": fetch.error},
        ))
        return findings

    if not fetch.lines:
        findings.append(Finding.make(
            publisher_key=publisher_key,
            check_id="adstxt.file_empty",
            severity="high",
            detail={"variant": fetch.variant, "url": fetch.url,
                    "body_sha256": fetch.body_sha256},
        ))
        return findings

    pgam_lines = [ln for ln in fetch.lines if ln.domain == PGAM_SELLER_DOMAIN]
    if not pgam_lines:
        findings.append(Finding.make(
            publisher_key=publisher_key,
            check_id="adstxt.universal_direct_missing",
            severity="critical",
            detail={"variant": fetch.variant, "url": fetch.url,
                    "expected_line": (
                        f"{PGAM_SELLER_DOMAIN}, {expected_seller_id or '<unknown>'}, DIRECT"
                    )},
        ))
        return findings

    # We have at least one pgamssp.com line. Check exact match path first.
    if expected_seller_id:
        own_lines = [ln for ln in pgam_lines if ln.account_id == expected_seller_id]
        if own_lines:
            # Exact seller_id match — verify at least one says DIRECT.
            if any(ln.relationship == "DIRECT" for ln in own_lines):
                return []  # pass
            findings.append(Finding.make(
                publisher_key=publisher_key,
                check_id="adstxt.universal_direct_wrong_type",
                severity="critical",
                detail={"variant": fetch.variant, "url": fetch.url,
                        "seller_id": expected_seller_id,
                        "observed_relationships": sorted({
                            ln.relationship for ln in own_lines
                        }),
                        "expected": "DIRECT"},
            ))
            return findings

    # Expected seat not found. Classify each observed seat by tier.
    observed_seats: list[dict] = []
    has_known_seat = False
    has_unknown_seat = False
    for ln in pgam_lines:
        registry_entry = pgam_seat_registry.get(ln.account_id)
        if registry_entry is None:
            observed_seats.append({
                "seller_id":   ln.account_id,
                "relationship": ln.relationship,
                "tier":        "unknown",
            })
            has_unknown_seat = True
        else:
            observed_seats.append({
                "seller_id":   ln.account_id,
                "relationship": ln.relationship,
                "tier":        "known",
                "seller_type":  registry_entry.get("seller_type"),
                "owner_name":   registry_entry.get("name"),
                "owner_domain": registry_entry.get("domain"),
            })
            has_known_seat = True

    # Tier verdict — unknown seat is always more severe than known.
    if has_unknown_seat:
        findings.append(Finding.make(
            publisher_key=publisher_key,
            check_id="adstxt.universal_direct_unknown_seat",
            severity="critical",
            detail={"variant": fetch.variant, "url": fetch.url,
                    "expected_seller_id": expected_seller_id,
                    "observed_seats": observed_seats},
            fingerprint_extra="unknown",
        ))
    elif has_known_seat:
        # All observed seats are in PGAM's registry but none is this
        # partner's own. Surface separately so the dashboard can
        # distinguish "Aditude intermediary used" from "another partner's
        # seat used" by inspecting observed_seats[].seller_type.
        findings.append(Finding.make(
            publisher_key=publisher_key,
            check_id="adstxt.universal_direct_wrong_seat",
            severity="high",
            detail={"variant": fetch.variant, "url": fetch.url,
                    "expected_seller_id": expected_seller_id,
                    "observed_seats": observed_seats},
            fingerprint_extra="known",
        ))

    return findings


def build_pgam_seat_registry(sellers_payload: dict) -> dict[str, dict]:
    """Map seller_id → {seller_type, name, domain} for tier classification."""
    out: dict[str, dict] = {}
    for s in sellers_payload.get("sellers") or []:
        if not isinstance(s, dict):
            continue
        sid = str(s.get("seller_id") or "").strip()
        if not sid:
            continue
        out[sid] = {
            "seller_type": (s.get("seller_type") or "").upper().strip(),
            "name":        s.get("name"),
            "domain":      s.get("domain"),
        }
    return out
