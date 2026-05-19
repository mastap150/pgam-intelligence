"""
agents/compliance/crawlers/sellersjson.py

Fetcher + parser for PGAM's own sellers.json (sellers.pgamssp.com).

This file is the authoritative list of every (publisher, seller_id) tuple
PGAM authorizes to monetize through us. Every entry where
seller_type IN (PUBLISHER, BOTH) is a publisher whose ads.txt must contain
`pgamssp.com, <seller_id>, DIRECT`.

URL is configurable via PGAM_SELLERS_JSON_URL because the account-scoped
path component (the long hex after the host) can rotate.
"""
from __future__ import annotations

import os
from dataclasses import dataclass

import requests

DEFAULT_SELLERS_JSON_URL = (
    "https://sellers.pgamssp.com/62ebe78298926f0faf3a822a/sellers.json"
)
HTTP_TIMEOUT_SEC = 30


@dataclass(frozen=True)
class SellerEntry:
    seller_id: str
    seller_type: str   # PUBLISHER | INTERMEDIARY | BOTH
    name: str | None
    domain: str | None

    @property
    def normalized_domain(self) -> str | None:
        if not self.domain:
            return None
        d = self.domain.strip().lower()
        if d.startswith("http://"):
            d = d[7:]
        elif d.startswith("https://"):
            d = d[8:]
        d = d.split("/", 1)[0]
        if d.startswith("www."):
            d = d[4:]
        return d or None

    @property
    def is_publisher_like(self) -> bool:
        """Entries we should monitor ads.txt for."""
        return self.seller_type in ("PUBLISHER", "BOTH")


def fetch_pgam_sellers_json(url: str | None = None) -> dict:
    """Fetch PGAM's hosted sellers.json. Returns the parsed dict.

    Raises requests.RequestException on network failure or non-200.
    """
    target = url or os.environ.get("PGAM_SELLERS_JSON_URL") or DEFAULT_SELLERS_JSON_URL
    headers = {"User-Agent": "pgam-intelligence/compliance"}
    resp = requests.get(target, headers=headers, timeout=HTTP_TIMEOUT_SEC)
    resp.raise_for_status()
    return resp.json()


def parse_sellers(payload: dict) -> list[SellerEntry]:
    """Pull seller rows out of a sellers.json payload."""
    raw_sellers = payload.get("sellers") or []
    out: list[SellerEntry] = []
    for s in raw_sellers:
        if not isinstance(s, dict):
            continue
        sid = str(s.get("seller_id") or "").strip()
        if not sid:
            continue
        out.append(
            SellerEntry(
                seller_id=sid,
                seller_type=str(s.get("seller_type") or "").upper().strip(),
                name=(s.get("name") or None),
                domain=(s.get("domain") or None),
            )
        )
    return out


def fetch_publisher_entries(url: str | None = None) -> list[SellerEntry]:
    """Convenience: fetch + filter to publisher-like entries with a usable domain."""
    payload = fetch_pgam_sellers_json(url=url)
    entries = parse_sellers(payload)
    return [e for e in entries if e.is_publisher_like and e.normalized_domain]
