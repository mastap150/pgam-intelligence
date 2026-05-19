"""
agents/compliance/crawlers/adstxt.py

ads.txt / app-ads.txt fetcher + parser.

Spec: IAB Tech Lab ads.txt v1.1.
Line format:   domain, account_id, relationship[, cert_authority_id]
Comments:      `#` to end of line
Variable directives (`subdomain=`, `ownerdomain=`, `managerdomain=`):
               captured but not consumed by Phase 1 validators.
"""
from __future__ import annotations

import hashlib
from dataclasses import dataclass

import requests

HTTP_TIMEOUT_SEC = 15
USER_AGENT = "pgam-intelligence/compliance (+https://pgammedia.com)"


@dataclass(frozen=True)
class AdsTxtLine:
    domain: str          # lowercased
    account_id: str      # case preserved (case can matter on some SSPs)
    relationship: str    # uppercased: DIRECT | RESELLER
    cert_authority: str | None


@dataclass(frozen=True)
class AdsTxtFetch:
    publisher_key: str
    variant: str         # 'ads.txt' | 'app-ads.txt'
    url: str
    http_status: int | None
    body: str | None
    body_sha256: str | None
    error: str | None
    lines: list[AdsTxtLine]
    variables: dict[str, list[str]]

    @property
    def ok(self) -> bool:
        return self.http_status == 200 and self.body is not None


def _fetch_one(
    url: str,
    extra_headers: dict[str, str] | None = None,
) -> tuple[int | None, str | None, dict[str, str], str | None]:
    """HTTP GET with optional conditional-GET headers. Returns
    (status, body, response_headers, error)."""
    headers = {"User-Agent": USER_AGENT, "Accept": "text/plain,*/*"}
    if extra_headers:
        headers.update(extra_headers)
    try:
        resp = requests.get(
            url,
            headers=headers,
            timeout=HTTP_TIMEOUT_SEC,
            allow_redirects=True,
        )
        body = resp.text if resp.status_code == 200 else None
        return resp.status_code, body, dict(resp.headers), None
    except requests.RequestException as exc:
        return None, None, {}, str(exc)


def parse_adstxt(body: str) -> tuple[list[AdsTxtLine], dict[str, list[str]]]:
    """Parse an ads.txt body into (lines, variables).

    `variables` captures directives like `subdomain=` / `ownerdomain=` /
    `managerdomain=` — multi-valued because the spec allows multiple
    subdomain= lines. Phase 1 validators don't consume these, but the
    schema is here so adding subdomain-aware checks later is a small diff.
    """
    lines: list[AdsTxtLine] = []
    variables: dict[str, list[str]] = {}

    for raw in body.splitlines():
        stripped = raw.split("#", 1)[0].strip()
        if not stripped:
            continue

        # Variable directive  (KEY=VALUE)
        if "=" in stripped and "," not in stripped:
            key, _, value = stripped.partition("=")
            key = key.strip().lower()
            value = value.strip()
            if key and value:
                variables.setdefault(key, []).append(value)
            continue

        parts = [p.strip() for p in stripped.split(",")]
        if len(parts) < 3:
            continue
        domain, account_id, relationship = parts[0], parts[1], parts[2]
        cert = parts[3] if len(parts) >= 4 and parts[3] else None
        lines.append(
            AdsTxtLine(
                domain=domain.lower(),
                account_id=account_id,
                relationship=relationship.upper(),
                cert_authority=cert,
            )
        )

    return lines, variables


def _line_from_dict(d: dict) -> AdsTxtLine:
    return AdsTxtLine(
        domain=d.get("domain", ""),
        account_id=d.get("account_id", ""),
        relationship=d.get("relationship", ""),
        cert_authority=d.get("cert_authority"),
    )


def _line_to_dict(ln: AdsTxtLine) -> dict:
    return {
        "domain": ln.domain,
        "account_id": ln.account_id,
        "relationship": ln.relationship,
        "cert_authority": ln.cert_authority,
    }


def fetch_adstxt(
    publisher_key: str,
    domain: str,
    variant: str = "ads.txt",
    *,
    use_cache: bool = False,
) -> AdsTxtFetch:
    """Fetch and parse a single ads.txt or app-ads.txt file.

    use_cache=True enables conditional-GET caching against
    pgam_direct.compliance_adstxt_cache. When the server returns 304
    Not Modified, we reuse the cached parse without re-downloading.
    Set False (default) for unit tests or environments without Neon.
    """
    assert variant in ("ads.txt", "app-ads.txt")
    url = f"https://{domain}/{variant}"

    cache_entry = None
    extra_headers: dict[str, str] = {}
    if use_cache:
        # Lazy import — keeps the unit-test path Neon-free.
        from agents.compliance.crawlers.adstxt_cache import (
            bump_cache_hit,
            conditional_headers,
            load_cache_entry,
            store_fresh_entry,
        )
        cache_entry = load_cache_entry(publisher_key, variant)
        extra_headers = conditional_headers(cache_entry)

    status, body, resp_headers, err = _fetch_one(url, extra_headers=extra_headers)

    # 304 Not Modified — reuse cached parse if we have one.
    if status == 304 and cache_entry is not None:
        from agents.compliance.crawlers.adstxt_cache import bump_cache_hit
        bump_cache_hit(publisher_key, variant)
        return AdsTxtFetch(
            publisher_key=publisher_key,
            variant=variant,
            url=url,
            http_status=200,                 # treat as fresh — validators don't care
            body=None,                       # body not stored in cache; lines suffice
            body_sha256=cache_entry.body_sha256,
            error=None,
            lines=[_line_from_dict(d) for d in cache_entry.parsed_lines],
            variables=cache_entry.parsed_variables or {},
        )

    sha = None
    lines: list[AdsTxtLine] = []
    variables: dict[str, list[str]] = {}

    if body is not None and status == 200:
        sha = hashlib.sha256(body.encode("utf-8", errors="replace")).hexdigest()[:16]
        lines, variables = parse_adstxt(body)
        if use_cache:
            from agents.compliance.crawlers.adstxt_cache import store_fresh_entry
            store_fresh_entry(
                publisher_key, variant,
                etag=resp_headers.get("ETag"),
                last_modified=resp_headers.get("Last-Modified"),
                body_sha256=sha,
                parsed_lines=[_line_to_dict(ln) for ln in lines],
                parsed_variables=variables,
            )

    return AdsTxtFetch(
        publisher_key=publisher_key,
        variant=variant,
        url=url,
        http_status=status,
        body=body if status == 200 else None,
        body_sha256=sha,
        error=err,
        lines=lines,
        variables=variables,
    )
