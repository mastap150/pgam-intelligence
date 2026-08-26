"""
core/impact_api.py
~~~~~~~~~~~~~~~~~~

Client for the **impact.com** partner-marketing REST API — PGAM's affiliate
leg, alongside the programmatic legs in `core/ll_*`, `core/tb_*`, `core/tbx_*`.

    Base URL:  https://api.impact.com
    Auth:      HTTP Basic — Account SID as username, Auth Token as password
    Encoding:  XML by default; JSON only when `Accept: application/json` is
               sent, which this module always does

Why this module and not the impact.com MCP server
-------------------------------------------------
impact.com ships a remote MCP server (integrations.impact.com/ai-solutions).
That is a *session* tool: it lets a person ask questions interactively. It
does not put a row in Neon, so no dashboard and no scheduled agent can read
it. This module is the warehouse leg. The two are complements, not
alternatives — see `docs/impact-affiliate-etl.md`.

Account type
------------
The same API serves both sides of the marketplace under different path roots:

    /Mediapartners/{AccountSid}/...   publisher side — PGAM's own sites
    /Advertisers/{AccountSid}/...     brand side — running a program

PGAM monetises its own properties (healthnation.com, destination.com,
boxingnews, …), so the default is `Mediapartners`. Override with
IMPACT_ACCOUNT_TYPE if a PGAM *advertiser* account is ever wired up; the SID
and token differ per account, so an advertiser leg is a second credential
pair, not a flag flip on this one.

VERIFICATION STATUS — read before trusting a field name
-------------------------------------------------------
This client was written from the published API shape WITHOUT a live account:
`api.impact.com` is unreachable from the network this repo's cloud sessions
run on, and no IMPACT_* credential existed anywhere when it was written
(2026-08-26). Transport (Basic auth, JSON accept header, `@page`/`@numpages`
envelope, `Page`/`PageSize` paging) is standard across the API and is the part
most likely correct. Individual *field* names on an Action are the part most
likely wrong for a given account, because impact.com exposes account-specific
custom fields alongside the standard set.

So: every consumer of this module reads fields through `action_field()` /
`ACTION_FIELDS`, which try several spellings, and the ETL stores the whole raw
record as JSONB so a mis-mapped column can be fixed with SQL instead of a
re-pull. Run `python3 scripts/impact_probe.py --actions` on a machine that has
the credentials before believing any number this produces.

Quick start
-----------
    from core import impact_api as imp

    imp.test_connection()

    actions = imp.actions(date_start="2026-08-01", date_end="2026-08-26")
    campaigns = imp.campaigns()

    # what reports this account can actually run (ids are account-specific)
    for r in imp.report_catalog():
        print(r.get("Id"), "|", r.get("Name"))
"""

from __future__ import annotations

import os
import threading
import time
from typing import Any, Iterator

import requests
from dotenv import load_dotenv

load_dotenv(override=True)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

IMPACT_BASE = os.getenv("IMPACT_API_BASE", "https://api.impact.com").rstrip("/")
IMPACT_SID = os.getenv("IMPACT_ACCOUNT_SID", "")
IMPACT_TOKEN = os.getenv("IMPACT_AUTH_TOKEN", "")

# "Mediapartners" (publisher side) or "Advertisers" (brand side).
IMPACT_ACCOUNT_TYPE = os.getenv("IMPACT_ACCOUNT_TYPE", "Mediapartners").strip("/")

TIMEOUT = int(os.getenv("IMPACT_TIMEOUT", "60"))
MAX_RETRIES = int(os.getenv("IMPACT_MAX_RETRIES", "3"))

# impact.com publishes a per-account request ceiling rather than a per-second
# one, and does not document the burst behaviour. Serialise calls and keep a
# floor on the gap, the same conservative posture core/tbx_api.py takes
# against an undocumented limiter. Tunable once a real 429 is observed.
_MIN_INTERVAL = float(os.getenv("IMPACT_MIN_INTERVAL", "0.35"))
_LOCK = threading.Lock()
_last_call_at = 0.0

# Hard stop on pagination. A paging bug that never advances would otherwise
# walk forever against a metered API; 400 pages at the default 500-row page
# size is 200k actions, far beyond any window this repo asks for.
MAX_PAGES = int(os.getenv("IMPACT_MAX_PAGES", "400"))
PAGE_SIZE = int(os.getenv("IMPACT_PAGE_SIZE", "500"))

_LOG_PREFIX = "[impact_api]"

_SESSION: requests.Session | None = None


class ImpactError(RuntimeError):
    """Any non-2xx / malformed response from impact.com."""

    def __init__(self, message: str, status: int | None = None, body: str = ""):
        super().__init__(message)
        self.status = status
        self.body = body


class ImpactAuthError(ImpactError):
    """Credentials missing or rejected (401 / 403)."""


# ---------------------------------------------------------------------------
# Field-name tolerance
#
# One logical field, several spellings across API versions and account
# configurations. Ordered most- to least-likely; first present wins. Nothing
# downstream should read `row["Payout"]` directly — an account that calls it
# something else would read as zero revenue rather than as an error, which is
# the failure mode this whole table exists to prevent.
# ---------------------------------------------------------------------------

ACTION_FIELDS: dict[str, tuple[str, ...]] = {
    "action_id":          ("Id", "ActionId", "OrderId", "Oid"),
    "campaign_id":        ("CampaignId", "ProgramId", "AdvertiserId"),
    "campaign_name":      ("CampaignName", "ProgramName", "AdvertiserName"),
    "tracker_id":         ("ActionTrackerId", "EventTypeId"),
    "tracker_name":       ("ActionTrackerName", "EventTypeName", "ActionTracker"),
    # Event date is when the conversion happened — the date revenue belongs to.
    # Creation/locking/modification are lifecycle stamps, NOT the revenue date.
    "event_date":         ("EventDate", "ActionDate", "EventTime", "Date"),
    "creation_date":      ("CreationDate", "CreatedDate"),
    "locking_date":       ("LockingDate", "LockDate"),
    "modification_date":  ("ModificationDate", "ModifiedDate", "LastModified"),
    "referring_date":     ("ReferringDate", "ClickDate"),
    "state":              ("State", "Status", "ActionStatus"),
    "payout":             ("Payout", "PayoutAmount", "Commission"),
    "sale_amount":        ("SaleAmount", "Amount", "IntendedAmount", "OrderAmount"),
    "currency":           ("Currency", "SaleCurrency", "AmountCurrency"),
    "payout_currency":    ("PayoutCurrency", "CurrencyPayout"),
    # SubId1 is how a multi-property publisher splits revenue by site. PGAM
    # runs several, so this is the column that answers "which of our sites
    # earned this" — and it is only populated if the tracking links set it.
    "sub_id1":            ("SubId1", "Subid1", "SharedId"),
    "sub_id2":            ("SubId2", "Subid2"),
    "sub_id3":            ("SubId3", "Subid3"),
    "promo_code":         ("PromoCode", "Coupon", "CouponCode"),
    "customer_country":   ("CustomerCountry", "Country", "CustomerRegion"),
    "referring_domain":   ("ReferringDomain", "ReferralUrl", "ReferringSite"),
}

# Action lifecycle. PENDING and APPROVED can still reverse; LOCKED is the only
# state impact.com will not walk back, so it is the only one safe to invoice
# against. See docs/impact-affiliate-etl.md §"Reversals".
ACTION_STATES = ("PENDING", "APPROVED", "REVERSED", "LOCKED")


def action_field(row: dict, logical: str, default: Any = None) -> Any:
    """
    Read one logical field out of an action row, trying each known spelling.

    Raises KeyError for an unknown logical name — a typo here would otherwise
    silently return the default and read as missing data.
    """
    try:
        candidates = ACTION_FIELDS[logical]
    except KeyError:
        raise KeyError(
            f"{logical!r} is not a known impact.com action field; "
            f"known: {', '.join(sorted(ACTION_FIELDS))}"
        ) from None
    for key in candidates:
        if key in row and row[key] not in (None, ""):
            return row[key]
    return default


# ---------------------------------------------------------------------------
# Transport
# ---------------------------------------------------------------------------

def configured() -> bool:
    """True when IMPACT_ACCOUNT_SID + IMPACT_AUTH_TOKEN are both present."""
    return bool(IMPACT_SID and IMPACT_TOKEN)


def missing_env() -> list[str]:
    """Which credential variables are absent. Used for actionable log lines."""
    return [
        name for name, value in (
            ("IMPACT_ACCOUNT_SID", IMPACT_SID),
            ("IMPACT_AUTH_TOKEN", IMPACT_TOKEN),
        ) if not value
    ]


def set_credentials(sid: str, token: str) -> None:
    """
    Supply credentials for this process only.

    For interactive use where the operator would rather not persist the token
    anywhere. Nothing is written to disk — unlike the JWT platforms, Basic auth
    has no token to cache, so there is no artefact to clean up afterwards.
    """
    global IMPACT_SID, IMPACT_TOKEN, _SESSION
    IMPACT_SID, IMPACT_TOKEN = sid.strip(), token.strip()
    _SESSION = None


def _session() -> requests.Session:
    global _SESSION
    if _SESSION is None:
        sess = requests.Session()
        sess.auth = (IMPACT_SID, IMPACT_TOKEN)
        sess.headers.update({
            # Without this impact.com answers XML and every parse below fails
            # on a body that looks like a successful response.
            "Accept": "application/json",
            "User-Agent": "pgam-intelligence/impact_api",
        })
        _SESSION = sess
    return _SESSION


def _account_path(suffix: str) -> str:
    """Build `/Mediapartners/{SID}/<suffix>` (or the Advertisers root)."""
    if not configured():
        raise ImpactAuthError(
            "impact.com credentials not configured: set "
            + " + ".join(missing_env() or ["IMPACT_ACCOUNT_SID", "IMPACT_AUTH_TOKEN"])
            + ". Find them in the impact.com UI under Settings → API Access."
        )
    return f"/{IMPACT_ACCOUNT_TYPE}/{IMPACT_SID}/{suffix.lstrip('/')}"


def _throttle() -> None:
    global _last_call_at
    gap = time.time() - _last_call_at
    if gap < _MIN_INTERVAL:
        time.sleep(_MIN_INTERVAL - gap)
    _last_call_at = time.time()


def _request(method: str, path: str, params: dict | None = None) -> Any:
    """
    One HTTP call with retries. Returns the decoded JSON body.

    Retries 429 and 5xx with backoff, honouring `Retry-After` when the server
    sends one. Does NOT retry 4xx other than 429 — a bad parameter answered
    the same way three times is three times the cost for the same error.
    """
    url = path if path.startswith("http") else f"{IMPACT_BASE}{path}"
    last_exc: Exception | None = None

    for attempt in range(1, MAX_RETRIES + 1):
        with _LOCK:
            _throttle()
            try:
                resp = _session().request(
                    method, url, params=params, timeout=TIMEOUT,
                )
            except requests.RequestException as exc:
                last_exc = exc
                resp = None

        if resp is None:
            if attempt == MAX_RETRIES:
                raise ImpactError(f"{method} {url} failed: {last_exc}") from last_exc
            time.sleep(2 ** attempt)
            continue

        if resp.status_code in (401, 403):
            raise ImpactAuthError(
                f"impact.com rejected the credentials ({resp.status_code}). "
                f"Check IMPACT_ACCOUNT_SID / IMPACT_AUTH_TOKEN, and that the "
                f"token belongs to a {IMPACT_ACCOUNT_TYPE} account.",
                status=resp.status_code, body=resp.text[:1000],
            )

        if resp.status_code == 429 or resp.status_code >= 500:
            if attempt == MAX_RETRIES:
                raise ImpactError(
                    f"{method} {url} gave {resp.status_code} after "
                    f"{MAX_RETRIES} attempts",
                    status=resp.status_code, body=resp.text[:1000],
                )
            wait = resp.headers.get("Retry-After")
            try:
                delay = float(wait) if wait else 2 ** attempt
            except ValueError:
                delay = 2 ** attempt
            print(f"{_LOG_PREFIX} {resp.status_code} on {path} — retrying in "
                  f"{delay:.0f}s (attempt {attempt}/{MAX_RETRIES})")
            time.sleep(min(delay, 60))
            continue

        if not resp.ok:
            raise ImpactError(
                f"{method} {url} gave {resp.status_code}",
                status=resp.status_code, body=resp.text[:1000],
            )

        if not resp.content:
            return {}
        try:
            return resp.json()
        except ValueError as exc:
            # Almost always XML: the Accept header was stripped by a proxy, or
            # the endpoint ignores it. Say which, rather than "invalid JSON".
            head = resp.text[:200].lstrip()
            hint = (" — body looks like XML, so the Accept header did not take"
                    if head.startswith("<") else "")
            raise ImpactError(
                f"{method} {url} returned non-JSON{hint}",
                status=resp.status_code, body=resp.text[:1000],
            ) from exc

    raise ImpactError(f"{method} {url} exhausted retries")


def get(path: str, params: dict | None = None) -> Any:
    """GET an absolute API path (already account-scoped)."""
    return _request("GET", path, params=params)


# ---------------------------------------------------------------------------
# Pagination
#
# impact.com wraps list responses in an envelope of `@`-prefixed metadata:
#
#     {"Actions": [...], "@page": "1", "@numpages": "4",
#      "@pagesize": "500", "@total": "1832", "@nextpageuri": "/Media..."}
#
# Two quirks that both produce silently short results if ignored:
#   1. A one-element result may arrive as an object, not a list of one.
#   2. `@nextpageuri` is authoritative when present; incrementing `Page`
#      independently can skip or repeat a page if the server re-sorts.
# ---------------------------------------------------------------------------

def _envelope_rows(body: Any, collection: str) -> list[dict]:
    if not isinstance(body, dict):
        return []
    rows = body.get(collection)
    if rows is None:
        # Some endpoints name the collection differently than the path. Fall
        # back to the single list-valued key rather than returning nothing.
        lists = [v for k, v in body.items()
                 if not k.startswith("@") and isinstance(v, list)]
        rows = lists[0] if len(lists) == 1 else []
    if isinstance(rows, dict):
        return [rows]
    if isinstance(rows, list):
        return [r for r in rows if isinstance(r, dict)]
    return []


def paginate(path: str, collection: str,
             params: dict | None = None,
             page_size: int = PAGE_SIZE,
             max_pages: int = MAX_PAGES) -> Iterator[dict]:
    """Yield every row across pages. `collection` is the envelope key."""
    query = dict(params or {})
    query.setdefault("PageSize", page_size)
    query.setdefault("Page", 1)

    next_path: str | None = path
    next_params: dict | None = query
    pages = 0

    while next_path and pages < max_pages:
        body = get(next_path, params=next_params)
        pages += 1
        rows = _envelope_rows(body, collection)
        for row in rows:
            yield row

        nxt = body.get("@nextpageuri") if isinstance(body, dict) else None
        if nxt:
            # Already carries its own query string; passing params again would
            # duplicate (and on some servers override) the paging cursor.
            next_path, next_params = str(nxt), None
            continue

        # No cursor: fall back to page counting, and stop when the reported
        # page count is reached. A missing/unparseable @numpages with a full
        # page of rows keeps walking; an empty page always stops.
        page = _int(body.get("@page"), pages)
        numpages = _int(body.get("@numpages"), 0)
        if numpages and page >= numpages:
            break
        if not rows:
            break
        query = dict(query)
        query["Page"] = page + 1
        next_path, next_params = path, query

    if pages >= max_pages:
        # Loud: a truncated pull is missing revenue, and this is the one place
        # that can tell the difference between "done" and "gave up".
        print(f"{_LOG_PREFIX} WARNING: stopped at IMPACT_MAX_PAGES={max_pages} "
              f"on {path}. Results are TRUNCATED — narrow the date window or "
              f"raise IMPACT_MAX_PAGES.")


def _int(value: Any, default: int = 0) -> int:
    try:
        return int(str(value).strip())
    except (TypeError, ValueError):
        return default


# ---------------------------------------------------------------------------
# Reads
# ---------------------------------------------------------------------------

def actions(date_start: str, date_end: str,
            state: str | None = None,
            campaign_id: str | int | None = None,
            extra_params: dict | None = None) -> list[dict]:
    """
    Every action (conversion) whose event date falls in [date_start, date_end].

    Dates are YYYY-MM-DD and inclusive at the API's day granularity. The
    endpoint also accepts full ISO-8601 timestamps; day bounds are what the
    ETL wants, so day bounds are what this sends.

    Returns raw rows exactly as the API gave them — no field renaming here, so
    that the ETL can store the original alongside its own mapping. Read
    individual fields with `action_field()`.
    """
    params: dict[str, Any] = {
        "ActionDateStart": f"{date_start}T00:00:00Z",
        "ActionDateEnd": f"{date_end}T23:59:59Z",
    }
    if state:
        params["State"] = state
    if campaign_id is not None:
        params["CampaignId"] = campaign_id
    if extra_params:
        params.update(extra_params)
    return list(paginate(_account_path("Actions"), "Actions", params))


def actions_modified_since(modified_start: str, modified_end: str | None = None,
                           extra_params: dict | None = None) -> list[dict]:
    """
    Actions whose *lifecycle* changed in a window, regardless of event date.

    This is the reversal catcher. An action that converted in March can be
    reversed in June; a window over event dates will never see that change,
    so a revenue table maintained only by event-date windows drifts upward
    forever. Keying on modification date closes it.

    `ModificationDateStart` is not part of the documented core parameter set
    for every account, so a 4xx here is a real possibility rather than a bug
    in the caller — the ETL treats a failure as "fall back to a wide event
    -date sweep" and says so, instead of dying.
    """
    params: dict[str, Any] = {
        "ModificationDateStart": f"{modified_start}T00:00:00Z",
    }
    if modified_end:
        params["ModificationDateEnd"] = f"{modified_end}T23:59:59Z"
    if extra_params:
        params.update(extra_params)
    return list(paginate(_account_path("Actions"), "Actions", params))


def campaigns() -> list[dict]:
    """Programs this account is joined to (publisher side) or runs (brand)."""
    return list(paginate(_account_path("Campaigns"), "Campaigns"))


def report_catalog() -> list[dict]:
    """
    Reports this account can run.

    Report ids are ACCOUNT-SPECIFIC — there is no portable constant to
    hardcode, which is why nothing in this repo hardcodes one. Run the probe,
    read the id you want out of this list, and pin it in the environment.
    """
    return list(paginate(_account_path("Reports"), "Reports"))


def run_report(report_id: str, params: dict | None = None) -> dict:
    """
    Run one report and return the decoded body.

    Report parameter names vary per report (`START_DATE`, `SUBAID`,
    `timeframe`, …); the catalog entry for a report documents its own. Passed
    through untouched for that reason.
    """
    return get(_account_path(f"Reports/{report_id}"), params=params or {})


def report_rows(report_id: str, params: dict | None = None) -> list[dict]:
    """`run_report` reduced to its record list, whatever the envelope calls it."""
    return _envelope_rows(run_report(report_id, params), "Records")


def test_connection() -> dict:
    """
    Cheapest possible authenticated read, with a human-readable verdict.

    Returns a dict rather than raising so a probe or a health check can report
    a failure without a traceback.
    """
    if not configured():
        return {"ok": False, "error": f"missing {', '.join(missing_env())}"}
    try:
        rows = campaigns()
    except ImpactError as exc:
        return {"ok": False, "error": str(exc),
                "status": getattr(exc, "status", None)}
    return {"ok": True, "account_type": IMPACT_ACCOUNT_TYPE,
            "campaigns": len(rows),
            "sample": [c.get("CampaignName") or c.get("Name")
                       for c in rows[:5]]}
