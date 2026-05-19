"""
agents/enrichment/play_store_resolver.py

Android bundle → developer website resolver.

iTunes Search API resolves iOS bundles plus most reverse-DNS bundles
that are actually iOS publishers. For Android-only apps it returns
nothing — Apple obviously doesn't index them — which used to leave
~30 % of LL's bundle revenue stranded as "unresolved" in compliance
audits.

Resolution cascade:
  1. Heuristic — for bundles matching `com.<word>.<rest>`, try
     `<word>.com` first. Cheap and works for the indie/template
     publishers that dominate Android casual-games inventory
     (com.fiogonia.dominoes → fiogonia.com, com.startio.X → startio.com).
     Cheap because we just probe whether app-ads.txt 404s.
  2. iTunes Search API — for numeric iOS IDs and reverse-DNS bundles,
     hit https://itunes.apple.com/lookup and pull `sellerUrl` from the
     result. Free, no auth, ~20 req/min unofficial limit (we don't
     hammer; one call per unresolved bundle per run).
  3. Play Store HTML — scrape
     https://play.google.com/store/apps/details?id=<bundle>&hl=en for the
     "Visit website" outgoing link. Brittle (Google rewrites the markup
     every ~6 months) but reliable when current.
  4. Give up → caller raises an info-level compliance finding so the
     gap is visible without polluting critical alerts.

All HTTP work is conservative (10s timeout, single GET, no retries
inside the resolver — the caller wraps in retry if it cares).
"""
from __future__ import annotations

import re
from dataclasses import dataclass

import requests

USER_AGENT = (
    "pgam-intelligence/compliance "
    "(+https://pgammedia.com; bundle-resolver)"
)
HTTP_TIMEOUT_SEC = 10
PROBE_TIMEOUT_SEC = 6  # heuristic probe; bail fast


# Pattern: com.X.rest → "X" is candidate. Skip generic prefixes that
# don't carry brand info.
_REVERSE_DNS = re.compile(r"^[a-z]+\.([a-z][a-z0-9\-]+)\.")
_GENERIC_OWNERS = {
    "google", "android", "example", "test", "demo", "app",
    "games", "studio", "studios", "mobile", "free", "io",
}


@dataclass(frozen=True)
class ResolveResult:
    bundle: str
    dev_domain: str | None
    method: str   # 'heuristic' | 'play_store' | 'unresolved'
    raw_url: str | None = None


def _normalize_domain(s: str) -> str | None:
    s = (s or "").strip().lower()
    if not s:
        return None
    if s.startswith("http://"):
        s = s[7:]
    elif s.startswith("https://"):
        s = s[8:]
    s = s.split("/", 1)[0]
    if s.startswith("www."):
        s = s[4:]
    s = s.split(":", 1)[0]
    return s or None


def _heuristic_candidate(bundle: str) -> str | None:
    """com.fiogonia.dominoes → 'fiogonia.com'. Returns None for non-match."""
    m = _REVERSE_DNS.match(bundle.lower())
    if not m:
        return None
    word = m.group(1)
    if word in _GENERIC_OWNERS or len(word) < 3:
        return None
    return f"{word}.com"


def _probe_app_ads(domain: str) -> bool:
    """HEAD /app-ads.txt — returns True only on 200."""
    try:
        resp = requests.head(
            f"https://{domain}/app-ads.txt",
            headers={"User-Agent": USER_AGENT},
            timeout=PROBE_TIMEOUT_SEC,
            allow_redirects=True,
        )
        return resp.status_code == 200
    except requests.RequestException:
        return False


# Play Store outgoing "Visit website" link patterns we've seen. Kept
# generous because Google rewrites the markup periodically.
_PLAYSTORE_PATTERNS = [
    # href="https://www.google.com/url?q=https://example.com/...&sa=..."
    re.compile(r'href="https?://www\.google\.com/url\?q=(https?://[^&"]+)'),
    # href="https://example.com/..."   ...inside a context labelled "website"
    re.compile(r'<a[^>]+href="(https?://[^"]+)"[^>]*>\s*Visit website'),
]


_NUMERIC_RE = re.compile(r"^\d{6,12}$")

ITUNES_LOOKUP_URL = "https://itunes.apple.com/lookup"


def _itunes_seller_url(bundle: str) -> str | None:
    """One-shot iTunes Search API lookup. Returns sellerUrl or None."""
    if _NUMERIC_RE.match(bundle):
        params = {"id": bundle}
    else:
        params = {"bundleId": bundle}
    try:
        resp = requests.get(
            ITUNES_LOOKUP_URL,
            params=params,
            headers={"User-Agent": USER_AGENT},
            timeout=HTTP_TIMEOUT_SEC,
        )
        if resp.status_code != 200:
            return None
        body = resp.json()
    except (requests.RequestException, ValueError):
        return None
    results = body.get("results") or []
    if not results:
        return None
    return (results[0].get("sellerUrl") or "").strip() or None


def _scrape_play_store(bundle: str) -> str | None:
    """Best-effort: fetch the Play Store details page, find outgoing
    'Visit website' link, return its hostname. Returns None on any
    failure (network / markup change / no website listed)."""
    url = f"https://play.google.com/store/apps/details?id={bundle}&hl=en"
    try:
        resp = requests.get(
            url,
            headers={"User-Agent": USER_AGENT, "Accept-Language": "en-US,en"},
            timeout=HTTP_TIMEOUT_SEC,
        )
    except requests.RequestException:
        return None
    if resp.status_code != 200:
        return None
    body = resp.text
    for pat in _PLAYSTORE_PATTERNS:
        m = pat.search(body)
        if not m:
            continue
        candidate_url = m.group(1)
        host = _normalize_domain(candidate_url)
        if not host or host == "play.google.com":
            continue
        return host
    return None


def resolve_bundle(bundle: str) -> ResolveResult:
    """End-to-end resolution. Heuristic → iTunes → Play Store → unresolved."""
    bundle = (bundle or "").strip()
    if not bundle:
        return ResolveResult(bundle="", dev_domain=None, method="unresolved")

    # 1. Heuristic — verified (the candidate hosts app-ads.txt)
    candidate = _heuristic_candidate(bundle)
    if candidate and _probe_app_ads(candidate):
        return ResolveResult(
            bundle=bundle, dev_domain=candidate, method="heuristic",
            raw_url=f"https://{candidate}",
        )

    # 2. iTunes Search API — covers both numeric iOS IDs and any
    # reverse-DNS bundle that's also indexed in the App Store.
    seller_url = _itunes_seller_url(bundle)
    if seller_url:
        host = _normalize_domain(seller_url)
        if host:
            return ResolveResult(
                bundle=bundle, dev_domain=host, method="itunes",
                raw_url=seller_url,
            )

    # 3. Play Store HTML scrape.
    play_host = _scrape_play_store(bundle)
    if play_host:
        return ResolveResult(
            bundle=bundle, dev_domain=play_host, method="play_store",
            raw_url=f"https://play.google.com/store/apps/details?id={bundle}",
        )

    # 4. Final fallback — the heuristic candidate even if app-ads.txt 404'd.
    # Sometimes a publisher hosts ads.txt but not app-ads.txt; the
    # crawler's app-ads → ads.txt fallback will pick that up.
    if candidate:
        return ResolveResult(
            bundle=bundle, dev_domain=candidate, method="heuristic_unverified",
            raw_url=f"https://{candidate}",
        )

    return ResolveResult(bundle=bundle, dev_domain=None, method="unresolved")
