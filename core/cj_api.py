"""
core/cj_api.py
~~~~~~~~~~~~~~

Client for **CJ Affiliate** (Commission Junction) — the second affiliate leg,
alongside `core/impact_api.py`.

    Base:   per-API hosts under *.api.cj.com (see ENDPOINTS)
    Auth:   Authorization header carrying a Personal Access Token or the
            legacy Developer Key
    Format: XML by default; JSON when the endpoint honours Accept. Both are
            parsed here — see `_parse`.

PGAM is a **publisher** on CJ: destination.com, CID 7112482, website/PID
101849129 (docs/expedia-affiliate-decision.md). Hotels.com and Vrbo are live;
Marriott, Hilton, IHG and Hyatt sit unapproved with `linkId: null`, and the
gated credit-card catalogue is the highest-value category on the site.

What this can and cannot do
---------------------------
CJ's published REST surface for publishers is **Link Search**, **Advertiser
Lookup**, the **Automated Offer Feed** (credit-card creative from financial
advertisers), and the GraphQL Commission Detail API. Advertiser Lookup returns
advertisers *both joined and not joined*, with program details.

There is **no endpoint that joins a program**. Applying to an advertiser is a
UI act, and this module deliberately does not simulate one: driving
members.cj.com with a script risks the account that is currently earning, and
the categories worth having (cards) are manually reviewed, where a bulk
application pattern is what gets a publisher declined. So this module finds
and ranks programs; a human clicks apply.

Read-only. There is no write layer and no write gate, because CJ exposes
nothing for a publisher to write.

CREDENTIAL HAZARD — why `_redact` exists
----------------------------------------
CJ's documented 401 for a bad key is:

    "Not Authenticated: xxxxxx"   where xxxxxx is the key you sent

The API echoes the credential back inside the error body. Anything that logs
or stores that body verbatim writes a live token into Render's log stream,
which is exactly how a secret escapes a system that never committed one.
Every error path here runs the body through `_redact` first, and
`CjError.body` is redacted before the exception is even constructed.

Distinguishing the 401s (from CJ's own error table)
---------------------------------------------------
    no message              -> the resource URL is wrong, not the key
    "You must specify..."   -> no key sent at all
    "Not Authenticated: .." -> key sent, key rejected

Three very different fixes behind one status code, so `_auth_error_kind`
names which one it is rather than reporting a flat 401.

Quick start
-----------
    from core import cj_api as cj

    cj.test_connection()
    joined = cj.joined_advertisers()
    candidates = cj.advertiser_lookup(joined=False, keywords="hotel")
"""

from __future__ import annotations

import os
import re
import threading
import time
import xml.etree.ElementTree as ET
from typing import Any

import requests
from dotenv import load_dotenv

load_dotenv(override=True)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

# A PAT is the current credential; the legacy Developer Key still authenticates
# the same header, so both names are read and either is enough.
CJ_TOKEN = (os.getenv("CJ_PERSONAL_ACCESS_TOKEN", "")
            or os.getenv("CJ_DEVELOPER_KEY", ""))

# requestor-cid on Advertiser Lookup. destination.com's company id.
CJ_COMPANY_ID = os.getenv("CJ_COMPANY_ID", "")
# website-id on Link Search. destination.com's PID.
CJ_WEBSITE_ID = os.getenv("CJ_WEBSITE_ID", "")

# Per-API hosts, env-overridable because CJ has moved these before and a
# wrong host answers 401 with no message rather than 404 (see the error table
# above) — which reads as a bad credential and sends you debugging the token.
ENDPOINTS: dict[str, str] = {
    "advertiser_lookup": os.getenv(
        "CJ_ADVERTISER_LOOKUP_URL",
        "https://advertiser-lookup.api.cj.com/v3/advertiser-lookup"),
    "link_search": os.getenv(
        "CJ_LINK_SEARCH_URL",
        "https://link-search.api.cj.com/v2/link-search"),
}

TIMEOUT = int(os.getenv("CJ_TIMEOUT", "60"))
MAX_RETRIES = int(os.getenv("CJ_MAX_RETRIES", "3"))
_MIN_INTERVAL = float(os.getenv("CJ_MIN_INTERVAL", "0.35"))
PAGE_SIZE = int(os.getenv("CJ_PAGE_SIZE", "100"))
MAX_PAGES = int(os.getenv("CJ_MAX_PAGES", "200"))

_LOCK = threading.Lock()
_last_call_at = 0.0
_LOG_PREFIX = "[cj_api]"
_SESSION: requests.Session | None = None


class CjError(RuntimeError):
    """Any non-2xx / unparseable response from CJ. `body` is always redacted."""

    def __init__(self, message: str, status: int | None = None, body: str = ""):
        super().__init__(_redact(message))
        self.status = status
        self.body = _redact(body)


class CjAuthError(CjError):
    """401 — see `_auth_error_kind` for which of the three causes it is."""


# ---------------------------------------------------------------------------
# Redaction
# ---------------------------------------------------------------------------

def _redact(text: str) -> str:
    """
    Remove the credential from any string before it is logged, stored, or
    raised.

    Not decoration. CJ echoes the key back in its "Not Authenticated: xxxxxx"
    body, so without this the token lands in Render's log stream the first
    time a key expires — and a rotated-out key sitting in logs is still a
    disclosed key.

    Also catches the token appearing in a URL query string, which is not how
    this module authenticates but is how someone debugging by hand will do it.
    """
    if not text:
        return text
    out = str(text)
    if CJ_TOKEN:
        out = out.replace(CJ_TOKEN, "***REDACTED***")
    # Belt to that brace: catch the documented echo shape even if the token in
    # the environment has since changed, or the call used set_credentials.
    out = re.sub(r"(Not Authenticated:\s*)\S+", r"\1***REDACTED***", out)
    out = re.sub(r"((?:pat|key|token|authorization)=)[^&\s]+", r"\1***REDACTED***",
                 out, flags=re.IGNORECASE)
    return out


def configured() -> bool:
    """True when a token and at least one account id are present."""
    return bool(CJ_TOKEN and (CJ_COMPANY_ID or CJ_WEBSITE_ID))


def missing_env() -> list[str]:
    missing = []
    if not CJ_TOKEN:
        missing.append("CJ_PERSONAL_ACCESS_TOKEN")
    if not CJ_COMPANY_ID:
        missing.append("CJ_COMPANY_ID")
    if not CJ_WEBSITE_ID:
        missing.append("CJ_WEBSITE_ID")
    return missing


def set_credentials(token: str, company_id: str = "", website_id: str = "") -> None:
    """Supply credentials for this process only. Nothing is written to disk."""
    global CJ_TOKEN, CJ_COMPANY_ID, CJ_WEBSITE_ID, _SESSION
    CJ_TOKEN = token.strip()
    if company_id:
        CJ_COMPANY_ID = company_id.strip()
    if website_id:
        CJ_WEBSITE_ID = website_id.strip()
    _SESSION = None


# ---------------------------------------------------------------------------
# Parameter encoding
# ---------------------------------------------------------------------------

def encode_cj(value: str) -> str:
    """
    Encode one parameter value the way CJ requires.

    From CJ's own overview: a space must encode to "+" and a literal "+" must
    encode to "%2B". Several standard URI helpers get exactly this pair wrong
    — they percent-encode the space and pass "+" through, which CJ then reads
    as a space. A keyword search for "5+ star" silently becomes "5  star".

    Applied before requests' own encoding sees the value, so the result is
    passed through as-is.
    """
    return str(value).replace("+", "%2B").replace(" ", "+")


# ---------------------------------------------------------------------------
# Transport
# ---------------------------------------------------------------------------

def _session() -> requests.Session:
    global _SESSION
    if _SESSION is None:
        sess = requests.Session()
        sess.headers.update({
            "Authorization": CJ_TOKEN,
            # Ask for JSON; CJ answers XML on the endpoints that ignore it,
            # and _parse handles both rather than assuming either.
            "Accept": "application/json, application/xml;q=0.9",
            "User-Agent": "pgam-intelligence/cj_api",
        })
        _SESSION = sess
    return _SESSION


def _auth_error_kind(body: str) -> str:
    """
    Turn CJ's three different 401s into the fix each one needs.

    All three arrive as a bare 401, and reporting that alone sends whoever is
    debugging to the credential — which is the right guess only one time in
    three.
    """
    text = (body or "").strip()
    if not text:
        return ("the resource URL is wrong (CJ answers 401, not 404, for an "
                "unknown path) — check the endpoint in ENDPOINTS, not the token")
    lowered = text.lower()
    if "must specify" in lowered:
        return "no credential was sent — the Authorization header was empty"
    if "not authenticated" in lowered:
        return "the credential was sent and rejected — token invalid or expired"
    return "401 with an unrecognised body"


def _throttle() -> None:
    global _last_call_at
    gap = time.time() - _last_call_at
    if gap < _MIN_INTERVAL:
        time.sleep(_MIN_INTERVAL - gap)
    _last_call_at = time.time()


def _request(path_or_url: str, params: dict | None = None) -> requests.Response:
    """One GET with retries. Returns the raw response for `_parse` to decode."""
    last_exc: Exception | None = None

    # Values are pre-encoded per CJ's rule, so requests must not re-encode
    # them. Building the query string here keeps that guarantee in one place.
    query = ""
    if params:
        query = "&".join(f"{k}={encode_cj(v)}" for k, v in params.items()
                         if v not in (None, ""))
    url = f"{path_or_url}?{query}" if query else path_or_url

    for attempt in range(1, MAX_RETRIES + 1):
        with _LOCK:
            _throttle()
            try:
                resp = _session().get(url, timeout=TIMEOUT)
            except requests.RequestException as exc:
                last_exc = exc
                resp = None

        if resp is None:
            if attempt == MAX_RETRIES:
                raise CjError(f"GET failed after {MAX_RETRIES} attempts: "
                              f"{last_exc}") from last_exc
            time.sleep(2 ** attempt)
            continue

        if resp.status_code == 401:
            raise CjAuthError(
                f"CJ returned 401 — {_auth_error_kind(resp.text)}",
                status=401, body=resp.text[:1000])

        if resp.status_code == 429 or resp.status_code >= 500:
            if attempt == MAX_RETRIES:
                raise CjError(f"CJ returned {resp.status_code} after "
                              f"{MAX_RETRIES} attempts",
                              status=resp.status_code, body=resp.text[:1000])
            wait = resp.headers.get("Retry-After")
            try:
                delay = float(wait) if wait else 2 ** attempt
            except ValueError:
                delay = 2 ** attempt
            print(f"{_LOG_PREFIX} {resp.status_code} — retrying in "
                  f"{delay:.0f}s (attempt {attempt}/{MAX_RETRIES})")
            time.sleep(min(delay, 60))
            continue

        if not resp.ok:
            raise CjError(f"CJ returned {resp.status_code}",
                          status=resp.status_code, body=resp.text[:1000])
        return resp

    raise CjError("exhausted retries")


# ---------------------------------------------------------------------------
# Parsing — CJ answers XML on endpoints that ignore Accept
# ---------------------------------------------------------------------------

def _xml_to_records(text: str) -> list[dict]:
    """
    Flatten a CJ XML body to a list of record dicts.

    CJ's bodies are one wrapper element holding a repeated record element,
    each record a set of leaf values plus some nested branches. The record
    element name differs per API and has changed before, so this finds the
    repeated container structurally rather than hardcoding a tag.

    Selection is by DEPTH, shallowest first — not by child count. A single
    advertiser carrying three <action> children would otherwise make <actions>
    look like the record list, and the whole response would parse as three
    commission rows with no advertiser in sight. That is the failure mode this
    heuristic exists to avoid, and it is silent: you get plausible rows of the
    wrong thing.

    Uses the stdlib parser: no new dependency, and CJ bodies are small.
    """
    try:
        root = ET.fromstring(text)
    except ET.ParseError as exc:
        raise CjError(f"response was neither JSON nor parseable XML: {exc}",
                      body=text[:1000]) from exc

    def _tag(el) -> str:
        return el.tag.split("}")[-1]          # strip any namespace

    # Locating the record container, in two steps.
    #
    # Primary signal: CJ stamps the container with paging attributes
    # (total-matched / records-returned / page-number). That is an explicit
    # marker from the vendor and beats any structural guess.
    #
    # Fallback, when those are absent: descend through single-child wrappers
    # until a node either holds several same-tagged children (those are the
    # records) or holds mixed children (that node IS the single record).
    # Descending matters — <cj-api> holding one <advertisers> is a wrapper,
    # not a one-record response — and stopping at mixed tags matters just as
    # much, or a lone advertiser carrying three <action> children parses as
    # three commissions and no advertiser.
    PAGING_ATTRS = {"total-matched", "records-returned", "page-number"}

    records_parent = None
    for el in root.iter():
        if PAGING_ATTRS & {k.split("}")[-1] for k in el.attrib} and list(el):
            records_parent = el
            break

    if records_parent is None:
        node = root
        while True:
            children = list(node)
            if len(children) >= 2 and len({_tag(c) for c in children}) == 1:
                records_parent = node
                break
            if len(children) == 1 and list(children[0]):
                node = children[0]
                continue
            break
        if records_parent is None:
            # Mixed or leaf children: this node is itself the one record.
            return _flatten_records([node])

    return _flatten_records(list(records_parent))


def _flatten_records(nodes: list) -> list[dict]:
    """Turn record elements into flat dicts of leaf values."""
    def _tag(el) -> str:
        return el.tag.split("}")[-1]

    records: list[dict] = []
    for node in nodes:
        row: dict[str, Any] = {}

        def _walk(el, prefix: str) -> None:
            for child in el:
                tag = _tag(child)
                if list(child):
                    _walk(child, f"{prefix}{tag}-")
                    continue
                value = (child.text or "").strip()
                if value == "":
                    continue
                # Stored under both the bare tag and a compound path, because
                # CJ nests values that matter — <primary-category><parent> is
                # the advertiser's category, and a bare "parent" key is both
                # ambiguous and easy to collide.
                row.setdefault(tag, value)
                if prefix:
                    row[f"{prefix}{tag}"] = value

        _walk(node, "")
        # Attributes carry real values on some CJ elements (link ids among them).
        for key, value in node.attrib.items():
            row.setdefault(key.split("}")[-1], value)
        if row:
            records.append(row)
    return records


def _parse(resp: requests.Response) -> list[dict]:
    """Decode a CJ response to records, whichever format it arrived in."""
    if not resp.content:
        return []
    ctype = (resp.headers.get("Content-Type") or "").lower()
    if "json" in ctype:
        body = resp.json()
        if isinstance(body, list):
            return [r for r in body if isinstance(r, dict)]
        if isinstance(body, dict):
            lists = [v for v in body.values() if isinstance(v, list)]
            if len(lists) == 1:
                return [r for r in lists[0] if isinstance(r, dict)]
            # Some JSON bodies nest one more level: {"advertisers": {"advertiser": [...]}}
            for value in body.values():
                if isinstance(value, dict):
                    inner = [v for v in value.values() if isinstance(v, list)]
                    if len(inner) == 1:
                        return [r for r in inner[0] if isinstance(r, dict)]
            return [body]
        return []
    return _xml_to_records(resp.text)


def response_format(resp: requests.Response) -> str:
    """"json" or "xml" — reported by the probe so the format is a known fact."""
    return "json" if "json" in (resp.headers.get("Content-Type") or "").lower() else "xml"


# ---------------------------------------------------------------------------
# Field-name tolerance
#
# Same posture as core/impact_api.py: one logical field, several spellings.
# CJ's XML uses hyphenated tags (advertiser-name); a JSON body would more
# likely use camelCase. Both are listed rather than one being assumed.
# ---------------------------------------------------------------------------

ADVERTISER_FIELDS: dict[str, tuple[str, ...]] = {
    "advertiser_id":   ("advertiser-id", "advertiserId", "cid", "advertiser_id"),
    "name":            ("advertiser-name", "advertiserName", "name"),
    "relationship":    ("relationship-status", "relationshipStatus",
                        "account-status", "status"),
    # "primary-category-parent" is the compound key _xml_to_records builds for
    # <primary-category><parent>, which is where CJ actually puts the category.
    "category":        ("primary-category-parent", "primary-category",
                        "primaryCategory", "category"),
    "subcategory":     ("primary-category-child", "primarySubCategory"),
    "network_rank":    ("network-rank", "networkRank", "seven-day-epc-rank"),
    "seven_day_epc":   ("seven-day-epc", "sevenDayEpc", "7-day-epc"),
    "three_month_epc": ("three-month-epc", "threeMonthEpc", "3-month-epc"),
    "program_url":     ("program-url", "programUrl", "program-terms-url"),
    "mobile_certified": ("mobile-tracking-certified", "mobileTrackingCertified"),
    "actions":         ("actions", "commission", "commissions"),
}


def advertiser_field(row: dict, logical: str, default: Any = None) -> Any:
    """Read one logical advertiser field, trying each known spelling."""
    try:
        candidates = ADVERTISER_FIELDS[logical]
    except KeyError:
        raise KeyError(
            f"{logical!r} is not a known CJ advertiser field; known: "
            f"{', '.join(sorted(ADVERTISER_FIELDS))}") from None
    for key in candidates:
        if key in row and row[key] not in (None, ""):
            return row[key]
    return default


# Relationship states CJ reports. "joined" is the only one that earns; the
# others are what a status watcher exists to notice a change in.
RELATIONSHIP_STATES = ("joined", "notjoined", "pending", "declined",
                       "extended", "temp_removed", "permanent_removed")


# ---------------------------------------------------------------------------
# Reads
# ---------------------------------------------------------------------------

def advertiser_lookup(joined: bool | None = True,
                      keywords: str | None = None,
                      advertiser_ids: str | None = None,
                      records_per_page: int = PAGE_SIZE,
                      page_number: int = 1) -> list[dict]:
    """
    Advertisers in the CJ network, joined or not.

    `joined=True` -> only current relationships; `joined=False` -> the
    catalogue of programs not yet applied to, which is the discovery surface
    for a shortlist; `joined=None` -> whatever CJ defaults to.

    `advertiser_ids` takes CJ's own literal values ("joined", "notjoined", or
    a comma list of ids) and overrides `joined` when given.
    """
    if not configured():
        raise CjAuthError(
            "CJ credentials not configured: set "
            + ", ".join(missing_env())
            + ". Token comes from members.cj.com -> Account -> Manage API Keys.")

    if advertiser_ids is None:
        advertiser_ids = ("joined" if joined else
                          "notjoined" if joined is False else None)

    params = {
        "requestor-cid": CJ_COMPANY_ID,
        "advertiser-ids": advertiser_ids,
        "keywords": keywords,
        "records-per-page": records_per_page,
        "page-number": page_number,
    }
    return _parse(_request(ENDPOINTS["advertiser_lookup"], params))


def joined_advertisers() -> list[dict]:
    """Every advertiser PGAM currently has a relationship with."""
    return advertiser_lookup(joined=True)


def link_search(keywords: str | None = None,
                advertiser_ids: str | None = "joined",
                category: str | None = None,
                link_type: str | None = None,
                promotion_type: str | None = None,
                records_per_page: int = PAGE_SIZE,
                page_number: int = 1) -> list[dict]:
    """
    Links in the CJ network matching the given criteria.

    This is where a wired advertiser's real link id comes from — the value
    that turns a `linkId: null` placeholder in destination.com's
    `cj-advertisers.ts` into a live route.
    """
    if not configured():
        raise CjAuthError(
            "CJ credentials not configured: set " + ", ".join(missing_env()))

    params = {
        "website-id": CJ_WEBSITE_ID,
        "advertiser-ids": advertiser_ids,
        "keywords": keywords,
        "category": category,
        "link-type": link_type,
        "promotion-type": promotion_type,
        "records-per-page": records_per_page,
        "page-number": page_number,
    }
    return _parse(_request(ENDPOINTS["link_search"], params))


def offer_feed(records_per_page: int = PAGE_SIZE,
               page_number: int = 1) -> list[dict]:
    """
    The Automated Offer Feed — credit-card content, links and images from CJ's
    financial advertisers, served through Link Search.

    Worth its own function because this is the highest-value category on
    destination.com per 08_monetization_strategy.md ($100-200 per approved
    card), and because access to it is gated: a publisher not approved for the
    financial vertical gets an empty result rather than an error. An empty
    return here means "not approved yet", not "no offers exist".
    """
    return link_search(promotion_type="Offer Feed",
                       records_per_page=records_per_page,
                       page_number=page_number)


def test_connection() -> dict:
    """
    Cheapest authenticated read, with a verdict rather than an exception.

    Returns which of the three 401 causes applies when it fails, because they
    need three different fixes.
    """
    if not configured():
        return {"ok": False, "error": f"missing {', '.join(missing_env())}"}
    try:
        rows = joined_advertisers()
    except CjAuthError as exc:
        return {"ok": False, "auth": True, "error": str(exc),
                "status": exc.status}
    except CjError as exc:
        return {"ok": False, "error": str(exc), "status": exc.status}
    return {"ok": True, "joined_advertisers": len(rows),
            "sample": [advertiser_field(r, "name") for r in rows[:5]]}
