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


def _fetch_one(url: str) -> tuple[int | None, str | None, str | None]:
    try:
        resp = requests.get(
            url,
            headers={"User-Agent": USER_AGENT, "Accept": "text/plain,*/*"},
            timeout=HTTP_TIMEOUT_SEC,
            allow_redirects=True,
        )
        return resp.status_code, resp.text, None
    except requests.RequestException as exc:
        return None, None, str(exc)


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


def fetch_adstxt(publisher_key: str, domain: str, variant: str = "ads.txt") -> AdsTxtFetch:
    """Fetch and parse a single ads.txt or app-ads.txt file."""
    assert variant in ("ads.txt", "app-ads.txt")
    url = f"https://{domain}/{variant}"
    status, body, err = _fetch_one(url)

    sha = None
    lines: list[AdsTxtLine] = []
    variables: dict[str, list[str]] = {}

    if body is not None and status == 200:
        sha = hashlib.sha256(body.encode("utf-8", errors="replace")).hexdigest()[:16]
        lines, variables = parse_adstxt(body)

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
