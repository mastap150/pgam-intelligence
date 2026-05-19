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


_BROWSER_UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.4 Safari/605.1.15"
)


def _parent_domain(host: str) -> str | None:
    """For 'pub-privacy.xmeye.net' return 'xmeye.net'. Returns None when
    the host is already at the eTLD+1 level (best-effort — we treat the
    last two labels as the registrable domain, which is wrong for .co.uk
    style suffixes but Right Enough for the publisher inventory we see
    in practice; revisit if we start seeing London IPTV publishers)."""
    h = (host or "").strip().lower()
    if h.startswith("www."):
        return None
    parts = h.split(".")
    if len(parts) <= 2:
        return None
    return ".".join(parts[-2:])


def fetch_adstxt_merged(
    publisher_key: str, host: str, *, use_cache: bool = False,
) -> AdsTxtFetch:
    """Fetch BOTH /ads.txt and /app-ads.txt and return their merged
    parse, with progressively-degraded fallbacks.

    Why: LL's DOMAIN breakdown often classifies app publishers as
    domains. Their ads.txt is empty/tiny but their app-ads.txt is huge.
    A publisher is "compliant" if EITHER file declares the required
    lines, so we union the parsed lines from whichever files came back
    200.

    Fallback cascade if neither file returns 200 over HTTPS with our
    default User-Agent:
      1. HTTP (http://) — some publishers haven't migrated to TLS
      2. Browser-like User-Agent — bypasses Cloudflare anti-bot
      3. Parent domain — for subdomain inventory (pub-privacy.xmeye.net
         → xmeye.net) the actual app-ads.txt usually lives on the apex
    """
    primary = fetch_adstxt(publisher_key, host, variant="ads.txt",
                            use_cache=use_cache)
    appads  = fetch_adstxt(publisher_key, host, variant="app-ads.txt",
                            use_cache=use_cache)

    if primary.http_status != 200 and appads.http_status != 200:
        # First fallback: browser UA over HTTPS (Cloudflare often
        # 403s our crawler UA but lets Safari through unchallenged).
        for variant in ("ads.txt", "app-ads.txt"):
            url = f"https://{host}/{variant}"
            try:
                resp = requests.get(
                    url,
                    headers={"User-Agent": _BROWSER_UA,
                             "Accept": "text/plain,*/*"},
                    timeout=HTTP_TIMEOUT_SEC,
                    allow_redirects=True,
                )
            except requests.RequestException:
                continue
            if resp.status_code == 200 and resp.text:
                lines, variables = parse_adstxt(resp.text)
                sha = hashlib.sha256(
                    resp.text.encode("utf-8", errors="replace")
                ).hexdigest()[:16]
                fetch = AdsTxtFetch(
                    publisher_key=publisher_key, variant=variant,
                    url=url, http_status=200, body=resp.text,
                    body_sha256=sha, error=None,
                    lines=lines, variables=variables,
                )
                if variant == "ads.txt":
                    primary = fetch
                else:
                    appads = fetch

    if primary.http_status != 200 and appads.http_status != 200:
        # Second fallback: HTTP (no TLS).
        for variant in ("ads.txt", "app-ads.txt"):
            url = f"http://{host}/{variant}"
            try:
                resp = requests.get(
                    url,
                    headers={"User-Agent": USER_AGENT,
                             "Accept": "text/plain,*/*"},
                    timeout=HTTP_TIMEOUT_SEC,
                    allow_redirects=True,
                )
            except requests.RequestException:
                continue
            if resp.status_code == 200 and resp.text:
                lines, variables = parse_adstxt(resp.text)
                sha = hashlib.sha256(
                    resp.text.encode("utf-8", errors="replace")
                ).hexdigest()[:16]
                fetch = AdsTxtFetch(
                    publisher_key=publisher_key, variant=variant,
                    url=url, http_status=200, body=resp.text,
                    body_sha256=sha, error=None,
                    lines=lines, variables=variables,
                )
                if variant == "ads.txt":
                    primary = fetch
                else:
                    appads = fetch

    if primary.http_status != 200 and appads.http_status != 200:
        # Third fallback: parent domain (subdomain inventory).
        parent = _parent_domain(host)
        if parent and parent != host:
            return fetch_adstxt_merged(publisher_key, parent,
                                        use_cache=use_cache)

    # Merge whatever succeeded.
    merged_lines: list[AdsTxtLine] = []
    files_seen: list[str] = []
    if primary.http_status == 200:
        merged_lines.extend(primary.lines)
        files_seen.append("ads.txt")
    if appads.http_status == 200:
        merged_lines.extend(appads.lines)
        files_seen.append("app-ads.txt")

    if files_seen:
        # Prefer app-ads.txt's variant tag when present (matches the
        # IAB-correct file for app inventory). Status 200 either way.
        primary_for_meta = appads if appads.http_status == 200 else primary
        return AdsTxtFetch(
            publisher_key=publisher_key,
            variant=" + ".join(files_seen),
            url=primary_for_meta.url,
            http_status=200,
            body=None,
            body_sha256=primary_for_meta.body_sha256,
            error=None,
            lines=merged_lines,
            variables=primary_for_meta.variables,
        )

    # Both files failed everywhere — surface the worse of the two errors.
    return primary if (primary.http_status or 0) <= (appads.http_status or 0) else appads


def fetch_adstxt_with_fallback(
    publisher_key: str, domain: str, *, use_cache: bool = False,
) -> AdsTxtFetch:
    """Fetch app-ads.txt; if it 404s, fall back to /ads.txt.

    Some publishers serve a single ads.txt that covers both their site
    and their app inventory, even though IAB spec is `app-ads.txt` for
    apps. We don't want to false-flag those as "missing app-ads.txt"
    when the data is actually present on the sibling endpoint.

    Returns the app-ads.txt fetch if it's 200; else the ads.txt fetch
    (tagging variant='ads.txt' so downstream validators don't double-flag).
    """
    primary = fetch_adstxt(publisher_key, domain, variant="app-ads.txt",
                            use_cache=use_cache)
    if primary.http_status == 200 and primary.lines:
        return primary
    secondary = fetch_adstxt(publisher_key, domain, variant="ads.txt",
                              use_cache=use_cache)
    # Prefer secondary if it succeeded, else return the more informative
    # primary error.
    if secondary.http_status == 200:
        return secondary
    return primary


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
