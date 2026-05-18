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
from agents.compliance.universe import Publisher, build_universe  # noqa: E402
from agents.compliance.validators.adstxt_universal import (  # noqa: E402
    PGAM_SELLER_DOMAIN,
    validate_universal_direct,
)


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
