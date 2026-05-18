"""
tests/test_compliance.py

Standalone, no-network/no-DB tests for the supply-compliance Phase 1 stack.

Covers:
  - sellers.json parsing (publisher filter, type precedence)
  - ads.txt parser (lines, variables, comments, blanks, malformed)
  - universal DIRECT validator (all five Phase 1 checks fire correctly)
  - universe deduplication

Run:
    python tests/test_compliance.py

Exits non-zero on any failure. No pytest dependency — matches the
test_msn_insights pattern.
"""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from agents.compliance.crawlers.adstxt import AdsTxtFetch, parse_adstxt  # noqa: E402
from agents.compliance.crawlers.sellersjson import parse_sellers  # noqa: E402
from agents.compliance.ll_bridge import score_match  # noqa: E402
from agents.compliance.observed_monetization import classify_rows_for_tests  # noqa: E402
from agents.compliance.ssp_registry import (  # noqa: E402
    PHASE_2_SSP_EXPECTATIONS,
    classify_demand_name,
    get_expectation,
)
from agents.compliance.universe import Publisher, build_universe  # noqa: E402
from agents.compliance.validators.adstxt_resellers import (  # noqa: E402
    validate_resellers_for_publisher,
)
from agents.compliance.validators.adstxt_universal import (  # noqa: E402
    PGAM_SELLER_DOMAIN,
    validate_universal_direct,
)
from agents.compliance.observed_monetization import ObservedRow  # noqa: E402


# ── Helpers ──────────────────────────────────────────────────────────────────


def _check(label: str, ok: bool, detail: str = "") -> None:
    mark = "✓" if ok else "✗"
    suffix = f" — {detail}" if detail else ""
    print(f"  {mark} {label}{suffix}")
    if not ok:
        raise AssertionError(f"FAILED: {label}{suffix}")


def _make_fetch(
    body: str | None,
    *,
    status: int = 200,
    error: str | None = None,
    variant: str = "ads.txt",
) -> AdsTxtFetch:
    lines = []
    variables: dict = {}
    if body is not None and status == 200:
        lines, variables = parse_adstxt(body)
    return AdsTxtFetch(
        publisher_key="example.com",
        variant=variant,
        url=f"https://example.com/{variant}",
        http_status=status,
        body=body if status == 200 else None,
        body_sha256="abc123" if body else None,
        error=error,
        lines=lines,
        variables=variables,
    )


# ── ads.txt parser ───────────────────────────────────────────────────────────


def test_adstxt_parser() -> None:
    print("\n[ads.txt parser]")

    body = """\
# Header comment
pgammedia.com, abc123, DIRECT, cert-1
rubiconproject.com, 24852, RESELLER, 0bfd66d529a55807

# variable directives
subdomain=videos.example.com
ownerdomain=example.com

pubmatic.com, 165708, RESELLER, 5d62403b186f2ace   # inline comment
malformed line with only two fields, foo
"""
    lines, variables = parse_adstxt(body)

    _check("3 well-formed lines parsed", len(lines) == 3, f"got {len(lines)}")
    _check("first line domain lowercased",
           lines[0].domain == "pgammedia.com")
    _check("first line account_id preserved",
           lines[0].account_id == "abc123")
    _check("first line relationship uppercased",
           lines[0].relationship == "DIRECT")
    _check("first line cert captured",
           lines[0].cert_authority == "cert-1")
    _check("inline comments stripped (pubmatic line is clean)",
           lines[2].account_id == "165708")
    _check("subdomain variable captured",
           variables.get("subdomain") == ["videos.example.com"])
    _check("ownerdomain variable captured",
           variables.get("ownerdomain") == ["example.com"])
    _check("malformed line dropped",
           all("malformed" not in l.domain for l in lines))


# ── sellers.json parser ──────────────────────────────────────────────────────


def test_sellersjson_parser() -> None:
    print("\n[sellers.json parser]")

    payload = {
        "contact_email": "info@pgammedia.com",
        "version": "1.0",
        "sellers": [
            {"seller_id": "111", "seller_type": "PUBLISHER",
             "name": "Site A", "domain": "site-a.com"},
            {"seller_id": "222", "seller_type": "INTERMEDIARY",
             "name": "Downstream SSP", "domain": "ssp.example.com"},
            {"seller_id": "333", "seller_type": "BOTH",
             "name": "PGAM Media", "domain": "https://www.pgammedia.com/"},
            {"seller_id": "", "seller_type": "PUBLISHER",
             "name": "junk", "domain": "junk.com"},
            {"seller_id": "555", "seller_type": "PUBLISHER",
             "name": "no domain", "domain": ""},
        ],
    }
    entries = parse_sellers(payload)
    _check("empty seller_id rows dropped", len(entries) == 4)

    pub_like = [e for e in entries if e.is_publisher_like]
    _check("only PUBLISHER + BOTH classified publisher-like",
           len(pub_like) == 3)  # 111, 333, 555 (555 has empty domain but is PUB)

    by_id = {e.seller_id: e for e in entries}
    _check("https/www stripped from BOTH domain",
           by_id["333"].normalized_domain == "pgammedia.com")
    _check("empty domain returns None on normalize",
           by_id["555"].normalized_domain is None)


# ── universe deduplication ───────────────────────────────────────────────────


def test_universe_dedup(monkeypatched_url: str = "") -> None:
    print("\n[universe dedup]")

    # Monkey-patch fetch_publisher_entries via the module to avoid network.
    from agents.compliance.crawlers import sellersjson as _sj

    payload = {
        "sellers": [
            {"seller_id": "a1", "seller_type": "BOTH",
             "name": "Site A v1", "domain": "site-a.com"},
            {"seller_id": "a2", "seller_type": "PUBLISHER",
             "name": "Site A v2", "domain": "site-a.com"},  # same domain
            {"seller_id": "b1", "seller_type": "PUBLISHER",
             "name": "Site B", "domain": "site-b.com"},
        ]
    }

    original = _sj.fetch_pgam_sellers_json
    _sj.fetch_pgam_sellers_json = lambda url=None: payload  # type: ignore
    try:
        pubs = build_universe()
    finally:
        _sj.fetch_pgam_sellers_json = original  # type: ignore

    _check("dedup produced 2 publishers", len(pubs) == 2)
    by_key = {p.publisher_key: p for p in pubs}
    # PUBLISHER beats BOTH per precedence.
    _check("PUBLISHER wins over BOTH on same domain",
           by_key["site-a.com"].seller_id == "a2")


# ── universal DIRECT validator ───────────────────────────────────────────────


def test_validator_passing() -> None:
    print("\n[validator — pass]")
    body = f"{PGAM_SELLER_DOMAIN}, seller-42, DIRECT\n"
    findings = validate_universal_direct("example.com", "seller-42", _make_fetch(body))
    _check("no findings when line is correct", len(findings) == 0,
           f"got {[f.check_id for f in findings]}")


def test_validator_missing() -> None:
    print("\n[validator — missing line]")
    body = "rubiconproject.com, 24852, RESELLER\n"
    findings = validate_universal_direct("example.com", "seller-42", _make_fetch(body))
    _check("one finding raised", len(findings) == 1)
    _check("check_id = universal_direct_missing",
           findings[0].check_id == "adstxt.universal_direct_missing")
    _check("severity = critical", findings[0].severity == "critical")


def test_validator_wrong_seller() -> None:
    print("\n[validator — wrong seller_id]")
    body = f"{PGAM_SELLER_DOMAIN}, wrong-id, DIRECT\n"
    findings = validate_universal_direct("example.com", "seller-42", _make_fetch(body))
    _check("one finding raised", len(findings) == 1)
    _check("check_id = wrong_seller",
           findings[0].check_id == "adstxt.universal_direct_wrong_seller")
    _check("observed_seller_ids surfaced",
           findings[0].detail["observed_seller_ids"] == ["wrong-id"])


def test_validator_wrong_type() -> None:
    print("\n[validator — wrong relationship]")
    body = f"{PGAM_SELLER_DOMAIN}, seller-42, RESELLER\n"
    findings = validate_universal_direct("example.com", "seller-42", _make_fetch(body))
    _check("one finding raised", len(findings) == 1)
    _check("check_id = wrong_type",
           findings[0].check_id == "adstxt.universal_direct_wrong_type")


def test_validator_unreachable() -> None:
    print("\n[validator — unreachable]")
    fetch = _make_fetch(None, status=None, error="connection refused")
    findings = validate_universal_direct("example.com", "seller-42", fetch)
    _check("one finding raised on network error", len(findings) == 1)
    _check("check_id = file_unreachable",
           findings[0].check_id == "adstxt.file_unreachable")
    _check("severity = high", findings[0].severity == "high")


def test_validator_404() -> None:
    print("\n[validator — 404]")
    fetch = _make_fetch(None, status=404)
    findings = validate_universal_direct("example.com", "seller-42", fetch)
    _check("404 → unreachable finding", len(findings) == 1)
    _check("check_id = file_unreachable",
           findings[0].check_id == "adstxt.file_unreachable")


def test_validator_empty() -> None:
    print("\n[validator — empty 200]")
    fetch = _make_fetch("# just comments\n   \n")
    findings = validate_universal_direct("example.com", "seller-42", fetch)
    _check("empty body → file_empty finding", len(findings) == 1)
    _check("check_id = file_empty",
           findings[0].check_id == "adstxt.file_empty")


def test_validator_multiple_pgam_lines_one_correct() -> None:
    print("\n[validator — multiple pgam lines, one is DIRECT with right id]")
    body = (
        f"{PGAM_SELLER_DOMAIN}, seller-99, RESELLER\n"
        f"{PGAM_SELLER_DOMAIN}, seller-42, DIRECT\n"
    )
    findings = validate_universal_direct("example.com", "seller-42", _make_fetch(body))
    _check("passes when ANY matching line is DIRECT", len(findings) == 0,
           f"got {[f.check_id for f in findings]}")


# ── Phase 2: SSP registry ────────────────────────────────────────────────────


def test_ssp_registry_completeness() -> None:
    print("\n[ssp registry — completeness]")
    keys = {e.ssp_key for e in PHASE_2_SSP_EXPECTATIONS}
    expected = {
        "rubicon", "pubmatic", "unruly", "zeta", "loopme",
        "sovrn", "triplelift", "sharethrough", "appnexus", "smaato",
    }
    _check("all 10 SSPs registered", keys == expected,
           f"missing: {expected - keys}, extra: {keys - expected}")
    for e in PHASE_2_SSP_EXPECTATIONS:
        _check(f"{e.ssp_key} has account_id", bool(e.account_id))
        _check(f"{e.ssp_key} is RESELLER", e.relationship == "RESELLER")


def test_ssp_registry_demand_classification() -> None:
    print("\n[ssp registry — demand-name classification]")
    cases = [
        ("Rubicon Project",         "rubicon"),
        ("Magnite CTV",             "rubicon"),
        ("PubMatic OpenWrap",       "pubmatic"),
        ("Unruly Group",            "unruly"),
        ("Tremor Video",            "unruly"),
        ("Zeta DSP",                "zeta"),
        ("LoopMe Direct",           "loopme"),
        ("Sovrn Holdings",          "sovrn"),
        ("Lijit",                   "sovrn"),
        ("TripleLift",              "triplelift"),
        ("Triple Lift Native",      "triplelift"),
        ("Sharethrough",            "sharethrough"),
        ("AppNexus",                "appnexus"),
        ("Xandr Invest",            "appnexus"),
        ("Microsoft Advertising",   "appnexus"),
        ("Smaato Exchange",         "smaato"),
        ("Some Random Demand",      None),
        ("",                        None),
    ]
    for name, expected_key in cases:
        result = classify_demand_name(name)
        actual = result.ssp_key if result else None
        _check(f"'{name}' → {expected_key}", actual == expected_key,
               f"got {actual}")


# ── Phase 2: reseller validator ──────────────────────────────────────────────


def test_reseller_validator_pass() -> None:
    print("\n[reseller validator — pass]")
    rubicon = get_expectation("rubicon")
    body = (
        f"{PGAM_SELLER_DOMAIN}, abc, DIRECT\n"
        f"rubiconproject.com, 24852, RESELLER, 0bfd66d529a55807\n"
    )
    obs = [ObservedRow("example.com", "rubicon", "rubiconproject.com",
                       100.0, 1000, 1, ("Rubicon Project",))]
    findings = validate_resellers_for_publisher("example.com", _make_fetch(body), obs)
    _check("no findings when line present + correct", len(findings) == 0,
           f"got {[f.check_id for f in findings]}")


def test_reseller_validator_missing() -> None:
    print("\n[reseller validator — missing]")
    body = f"{PGAM_SELLER_DOMAIN}, abc, DIRECT\n"
    obs = [ObservedRow("example.com", "rubicon", "rubiconproject.com",
                       100.0, 1000, 1, ("Rubicon Project",))]
    findings = validate_resellers_for_publisher("example.com", _make_fetch(body), obs)
    _check("one critical finding", len(findings) == 1)
    _check("check_id = reseller_missing",
           findings[0].check_id == "adstxt.reseller_missing")
    _check("severity = critical", findings[0].severity == "critical")
    _check("ssp in fingerprint scope",
           findings[0].detail["ssp"] == "rubicon")


def test_reseller_validator_wrong_seller() -> None:
    print("\n[reseller validator — wrong seller_id]")
    body = (
        f"{PGAM_SELLER_DOMAIN}, abc, DIRECT\n"
        "rubiconproject.com, 99999, RESELLER\n"        # someone else's seat
    )
    obs = [ObservedRow("example.com", "rubicon", "rubiconproject.com",
                       100.0, 1000, 1, ("Rubicon Project",))]
    findings = validate_resellers_for_publisher("example.com", _make_fetch(body), obs)
    _check("one finding", len(findings) == 1)
    _check("check_id = reseller_wrong_seller",
           findings[0].check_id == "adstxt.reseller_wrong_seller")


def test_reseller_validator_wrong_type() -> None:
    print("\n[reseller validator — wrong type]")
    body = (
        f"{PGAM_SELLER_DOMAIN}, abc, DIRECT\n"
        "rubiconproject.com, 24852, DIRECT, 0bfd66d529a55807\n"
    )
    obs = [ObservedRow("example.com", "rubicon", "rubiconproject.com",
                       100.0, 1000, 1, ("Rubicon Project",))]
    findings = validate_resellers_for_publisher("example.com", _make_fetch(body), obs)
    _check("one finding", len(findings) == 1)
    _check("check_id = reseller_wrong_type",
           findings[0].check_id == "adstxt.reseller_wrong_type")
    _check("severity = high (downgrade from critical)",
           findings[0].severity == "high")


def test_reseller_validator_cert_mismatch() -> None:
    print("\n[reseller validator — cert mismatch]")
    body = (
        f"{PGAM_SELLER_DOMAIN}, abc, DIRECT\n"
        "rubiconproject.com, 24852, RESELLER, DIFFERENT_CERT\n"
    )
    obs = [ObservedRow("example.com", "rubicon", "rubiconproject.com",
                       100.0, 1000, 1, ("Rubicon Project",))]
    findings = validate_resellers_for_publisher("example.com", _make_fetch(body), obs)
    _check("one medium finding", len(findings) == 1)
    _check("check_id = reseller_cert_mismatch",
           findings[0].check_id == "adstxt.reseller_cert_mismatch")
    _check("severity = medium", findings[0].severity == "medium")


def test_reseller_validator_skipped_when_not_observed() -> None:
    print("\n[reseller validator — skipped when SSP not monetizing]")
    body = f"{PGAM_SELLER_DOMAIN}, abc, DIRECT\n"
    # Publisher has no observed monetization through any SSP → no findings.
    findings = validate_resellers_for_publisher("example.com", _make_fetch(body), [])
    _check("no findings when observed list empty", len(findings) == 0)


def test_reseller_validator_multiple_ssps_independent_fingerprints() -> None:
    print("\n[reseller validator — multiple missing ssps fingerprint independently]")
    body = f"{PGAM_SELLER_DOMAIN}, abc, DIRECT\n"
    obs = [
        ObservedRow("example.com", "rubicon", "rubiconproject.com",
                    100.0, 1000, 1, ("Rubicon Project",)),
        ObservedRow("example.com", "pubmatic", "pubmatic.com",
                    50.0, 500, 1, ("PubMatic",)),
    ]
    findings = validate_resellers_for_publisher("example.com", _make_fetch(body), obs)
    _check("two findings raised", len(findings) == 2)
    fps = {f.fingerprint for f in findings}
    _check("fingerprints distinct per SSP", len(fps) == 2)


# ── Phase 2: observed monetization classification ────────────────────────────


def test_observed_monetization_classification() -> None:
    print("\n[observed monetization — classification + grouping]")
    raw = [
        {"publisher_key": "pub-a.com", "demand_id": "1",
         "demand_name": "Rubicon Project", "revenue_usd": 50.0, "impressions": 1000},
        {"publisher_key": "pub-a.com", "demand_id": "2",
         "demand_name": "Magnite CTV",    "revenue_usd": 25.0, "impressions": 500},
        {"publisher_key": "pub-a.com", "demand_id": "3",
         "demand_name": "PubMatic",       "revenue_usd": 10.0, "impressions": 200},
        {"publisher_key": "pub-b.com", "demand_id": "4",
         "demand_name": "Random DSP",     "revenue_usd": 5.0,  "impressions": 100},
    ]
    observed, unclassified = classify_rows_for_tests(raw)
    by_key = {(o.publisher_key, o.ssp_key): o for o in observed}
    _check("pub-a rubicon aggregated across two demand_names",
           by_key[("pub-a.com", "rubicon")].revenue_usd == 75.0)
    _check("pub-a rubicon demand_count = 2",
           by_key[("pub-a.com", "rubicon")].demand_count == 2)
    _check("pub-a pubmatic separate row",
           by_key[("pub-a.com", "pubmatic")].revenue_usd == 10.0)
    _check("unclassified demand isolated", len(unclassified) == 1)
    _check("unclassified row preserved",
           unclassified[0]["demand_name"] == "Random DSP")


# ── Phase 2: LL bridge fuzzy matching ────────────────────────────────────────


def test_bridge_exact_name() -> None:
    print("\n[ll bridge — exact name match]")
    m = score_match("Publishers Clearing House",
                    seller_name="Publishers Clearing House",
                    domain="pch.com")
    _check("matched", m is not None)
    _check("method = exact_name", m.method == "exact_name")
    _check("score = 1.0", m.score == 1.0)


def test_bridge_domain_substring() -> None:
    print("\n[ll bridge — domain substring]")
    m = score_match("BlackEnterprise Editorial",
                    seller_name="Black Enterprise",
                    domain="blackenterprise.com")
    _check("matched via domain substring", m is not None)
    _check("method = domain_substring", m.method == "domain_substring")


def test_bridge_token_overlap() -> None:
    print("\n[ll bridge — token overlap]")
    m = score_match("Future Publishing UK",
                    seller_name="Future Publishing",
                    domain="futureplc.com")
    _check("matched via token overlap", m is not None,
           f"got {m}")


def test_bridge_no_match() -> None:
    print("\n[ll bridge — no match]")
    m = score_match("Some Unrelated Site",
                    seller_name="Publishers Clearing House",
                    domain="pch.com")
    _check("no match returned", m is None)


def test_bridge_short_stem_not_promoted() -> None:
    print("\n[ll bridge — short stems don't trigger spurious matches]")
    # 'ab' inside 'about' shouldn't be a domain_substring hit
    m = score_match("Aboutness Magazine",
                    seller_name="Different Site Inc",
                    domain="ab.io")
    _check("very-short stem suppressed", m is None or m.method != "domain_substring")


# ── Main ─────────────────────────────────────────────────────────────────────


def main() -> int:
    print("=== compliance Phase 1 unit tests ===")
    tests = [
        test_adstxt_parser,
        test_sellersjson_parser,
        test_universe_dedup,
        test_validator_passing,
        test_validator_missing,
        test_validator_wrong_seller,
        test_validator_wrong_type,
        test_validator_unreachable,
        test_validator_404,
        test_validator_empty,
        test_validator_multiple_pgam_lines_one_correct,
        # Phase 2
        test_ssp_registry_completeness,
        test_ssp_registry_demand_classification,
        test_reseller_validator_pass,
        test_reseller_validator_missing,
        test_reseller_validator_wrong_seller,
        test_reseller_validator_wrong_type,
        test_reseller_validator_cert_mismatch,
        test_reseller_validator_skipped_when_not_observed,
        test_reseller_validator_multiple_ssps_independent_fingerprints,
        test_observed_monetization_classification,
        test_bridge_exact_name,
        test_bridge_domain_substring,
        test_bridge_token_overlap,
        test_bridge_no_match,
        test_bridge_short_stem_not_promoted,
    ]
    failures = 0
    for t in tests:
        try:
            t()
        except AssertionError as exc:
            failures += 1
            print(f"  !! {exc}")
        except Exception as exc:
            failures += 1
            print(f"  !! {t.__name__} crashed: {exc}")
    print()
    if failures:
        print(f"FAIL — {failures} failure(s)")
        return 1
    print("OK — all tests passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
