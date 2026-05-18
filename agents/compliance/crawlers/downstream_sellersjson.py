"""
agents/compliance/crawlers/downstream_sellersjson.py

Fetcher for each downstream SSP's sellers.json. Confirms PGAM's seat
exists with the right name/type — catches the "SSP dropped you" failure
mode that ads.txt alone can't detect.

Most SSPs publish at https://<root_domain>/sellers.json (the registry
default); per-SSP overrides live on SspExpectation.sellers_json_url.

Large files: PubMatic / AppNexus sellers.json regularly run 100K+
entries (~5–15 MB). We stream-fetch with a 60 s timeout and rely on
requests' default chunked read; total memory ~one parsed JSON object.
"""
from __future__ import annotations

import hashlib
from dataclasses import dataclass

import requests

from agents.compliance.ssp_registry import SspExpectation

HTTP_TIMEOUT_SEC = 60
USER_AGENT = "pgam-intelligence/compliance (+https://pgammedia.com)"


@dataclass(frozen=True)
class DownstreamFetch:
    ssp_key: str
    url: str
    http_status: int | None
    body_sha256: str | None
    seller_count: int | None
    sellers: list[dict]
    error: str | None

    @property
    def ok(self) -> bool:
        return self.http_status == 200 and bool(self.sellers)


def fetch_downstream_sellers_json(exp: SspExpectation) -> DownstreamFetch:
    url = exp.effective_sellers_json_url
    try:
        resp = requests.get(
            url,
            headers={"User-Agent": USER_AGENT, "Accept": "application/json,*/*"},
            timeout=HTTP_TIMEOUT_SEC,
        )
    except requests.RequestException as exc:
        return DownstreamFetch(
            ssp_key=exp.ssp_key, url=url, http_status=None,
            body_sha256=None, seller_count=None, sellers=[],
            error=str(exc),
        )

    status = resp.status_code
    if status != 200:
        return DownstreamFetch(
            ssp_key=exp.ssp_key, url=url, http_status=status,
            body_sha256=None, seller_count=None, sellers=[],
            error=f"HTTP {status}",
        )

    body = resp.content
    sha = hashlib.sha256(body).hexdigest()[:16]
    try:
        payload = resp.json()
    except ValueError as exc:
        return DownstreamFetch(
            ssp_key=exp.ssp_key, url=url, http_status=status,
            body_sha256=sha, seller_count=None, sellers=[],
            error=f"json decode: {exc}",
        )

    sellers = payload.get("sellers") if isinstance(payload, dict) else None
    if not isinstance(sellers, list):
        return DownstreamFetch(
            ssp_key=exp.ssp_key, url=url, http_status=status,
            body_sha256=sha, seller_count=0, sellers=[],
            error="payload missing 'sellers' array",
        )

    return DownstreamFetch(
        ssp_key=exp.ssp_key, url=url, http_status=status,
        body_sha256=sha, seller_count=len(sellers), sellers=sellers,
        error=None,
    )
