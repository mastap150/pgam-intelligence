"""
core/tbx_api.py
~~~~~~~~~~~~~~~

Client for the **new Teqblaze platform** that PGAM was onboarded to
(OpenAPI 3.0 spec `Pgam v1.11.15`, vendored at
`docs/api/teqblaze-openapi.json`).

    Base URL:  https://api.pgammedia.com
    Auth:      POST /login → JWT, sent as `Authorization: Bearer <jwt>`

This is a DIFFERENT platform from the legacy `core/tb_api.py` /
`core/tb_mgmt.py` pair
-------------------------------------------------------------------
    module              host                       auth
    ──────────────────  ─────────────────────────  ─────────────────────
    core/tb_api.py      ssp.pgammedia.com/api      token in URL path
    core/tb_mgmt.py     ssp.pgammedia.com/api      token in URL path
    core/tbx_api.py     api.pgammedia.com          Bearer JWT      ← this
    core/tbx_mgmt.py    api.pgammedia.com          Bearer JWT

Entity vocabulary also differs. Legacy TB speaks *inventory* +
*placement*; the new platform speaks *supply source* (which owns
placements) + *demand source*, both grouped under *companies*. Do not
assume IDs are portable between the two — they are not.

What lives here vs tbx_mgmt
---------------------------
`tbx_api` is the transport + **read** layer: auth, retries, pagination,
dictionaries, and every analytics surface (Report, Bids Overview, HUMAN,
Schain Utilisation, Sellers Validation, Ads.txt Verification, Scanner
Statistics, Discrepancy, Traffic Logger).

`tbx_mgmt` is the entity + **write** layer (supply/demand sources,
placement floors, filter lists, deals) with dry-run, clamps, verify and
ledger discipline.

Quick start
-----------
    from core import tbx_api as tbx

    tbx.test_connection()

    rows, total = tbx.report(
        date_from="2026-08-12", date_to="2026-08-18",
        attributes=["date", "supply_source"],
        metrics=["imps_sum", "ssp_price_sum", "dsp_price_sum", "profit", "margin"],
    )

    ivt = tbx.human_report("risk-metrics",
                           date_from="2026-08-12", date_to="2026-08-18",
                           attributes=["inventory_key"],
                           metrics=["requests_sum", "mfa_rate", "sivt_rate", "givt_rate"])
"""

from __future__ import annotations

import hashlib
import json
import os
import sys
import threading
import time
from typing import Any, Iterator

import requests
from dotenv import load_dotenv

load_dotenv(override=True)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

TBX_BASE     = os.getenv("TBX_API_BASE", "https://api.pgammedia.com").rstrip("/")
TBX_EMAIL    = os.getenv("TBX_EMAIL", "")
TBX_PASSWORD = os.getenv("TBX_PASSWORD", "")

_TOKEN_CACHE_DEFAULT = "/tmp/pgam_tbx_token.json"
TOKEN_CACHE  = os.getenv("TBX_TOKEN_CACHE", _TOKEN_CACHE_DEFAULT)

# Default report timezone. PGAM books revenue in ET, matching the
# scheduler's TZ and the LL/legacy-TB reporting convention.
DEFAULT_TZ   = os.getenv("TBX_TIMEZONE", "US/Eastern")

TIMEOUT      = int(os.getenv("TBX_TIMEOUT", "60"))
MAX_RETRIES  = int(os.getenv("TBX_MAX_RETRIES", "3"))

# The legacy TB API only tolerated one concurrent query per user. The new
# platform's limits are undocumented, so we stay conservative: one call at
# a time, with a floor on the gap between calls. Both are env-tunable once
# real limits are known.
_MIN_INTERVAL = float(os.getenv("TBX_MIN_INTERVAL", "0.25"))
_LOCK         = threading.Lock()
_last_call_at = 0.0

_LOG_PREFIX = "[tbx_api]"


class TbxError(RuntimeError):
    """Any non-2xx / malformed response from the new Teqblaze platform."""

    def __init__(self, message: str, status: int | None = None, body: str = ""):
        super().__init__(message)
        self.status = status
        self.body = body


class TbxAuthError(TbxError):
    """Credentials missing, rejected, or token could not be refreshed."""


# ---------------------------------------------------------------------------
# Vocabulary — extracted from the OpenAPI spec so callers fail fast locally
# instead of round-tripping a 422.
# ---------------------------------------------------------------------------

REPORT_ATTRIBUTES: tuple[str, ...] = (
    "date", "supply_company", "supply_source", "supply_source_type",
    "supply_integration_type", "placement", "demand_company", "demand_source",
    "demand_source_type", "country", "supply_deal", "demand_deal", "seat",
    "traffic_type", "ad_format", "size", "inventory_key", "publisher", "os",
    "region", "inventory_ssp_id", "inventory_dsp_id", "crid",
    "ab_test_feature", "consent_regulation_compliance",
)

REPORT_METRICS: tuple[str, ...] = (
    # Funnel counts
    "ssp_requests_sum", "ssp_cookie_requests_sum", "platform_cookies_sum",
    "requests_sum", "cookie_requests_sum", "ssp_idfa_requests_sum",
    "deal_ssp_request_sum", "deal_dsp_request_sum", "responses_sum",
    "wins_sum", "ssp_wins_sum", "imps_sum", "clicks_sum",
    # Video events
    "video_events_first_quartile_sum", "video_events_midpoint_sum",
    "video_events_third_quartile_sum", "video_events_complete_sum",
    # Rates
    "ssp_idfa_requests_percent", "vcr", "demand_win_rate", "supply_win_rate",
    "demand_fill_rate", "supply_fill_rate", "ctr", "demand_bid_rate",
    "render_rate", "timeout_rate", "dsp_sync_rate", "ssp_sync_rate",
    "ssp_conversion_rate", "supply_bid_rate",
    # Money
    "ssp_price_sum", "supply_ecpm", "dsp_price_sum", "demand_ecpm",
    "profit", "margin", "supply_srpm", "demand_srpm",
    "avg_supply_bid_floor", "avg_supply_bid_price",
    "avg_demand_bid_floor", "avg_demand_bid_price",
)

# Metric → the LL/legacy-TB name it corresponds to, so cross-platform
# rollups (admin dashboard, Neon `pgam_direct`) stay apples-to-apples.
#
#   LL GROSS_REVENUE == total DSP spend      == dsp_price_sum
#   LL PUB_PAYOUT    == what the pub is owed == ssp_price_sum
#   LL PROFIT        == platform margin      == profit
METRIC_ALIASES: dict[str, str] = {
    "GROSS_REVENUE":   "dsp_price_sum",
    "PUB_PAYOUT":      "ssp_price_sum",
    "PROFIT":          "profit",
    "IMPRESSIONS":     "imps_sum",
    "WINS":            "wins_sum",
    "BIDS":            "requests_sum",
    "AVG_FLOOR_PRICE": "avg_supply_bid_floor",
    "AVG_BID_PRICE":   "avg_supply_bid_price",
    "GROSS_ECPM":      "demand_ecpm",
}

DATE_GRANULARITIES = ("hour", "day", "month")

# `POST /dictionaries/{type}` — the platform's own lookup tables. Full list
# in the spec; these are the ones the agents actually need.
DICTIONARY_TYPES = (
    "countries", "regions", "companies", "supply-companies", "demand-companies",
    "supply-sources", "demand-sources", "placements", "seats", "scanners",
    "operation-systems", "traffic-type", "ad-format", "banner-sizes",
    "iab-categories", "failure-reasons", "ivt-reasons", "timezones",
    "filter-list-record-types", "supply-deal", "demand-deal", "presets",
    "qps-limit-optimizations", "target-srcpm-list", "billing-types",
    "seller-domain-node-rank", "verification-list", "logger-events",
    "traffic-logger-objects", "adapters", "adapters-settings",
)


# ---------------------------------------------------------------------------
# Token management
# ---------------------------------------------------------------------------

def configured() -> bool:
    """True when TBX_EMAIL + TBX_PASSWORD are present in the environment."""
    return bool(TBX_EMAIL and TBX_PASSWORD)


def set_credentials(email: str, password: str) -> None:
    """
    Supply credentials for this process only.

    For interactive use on a machine that can reach the platform, when the
    operator would rather not persist the password anywhere — not in `.env`,
    not in a shell history, not in a CI secret store. Nothing is written to
    disk except the short-lived JWT in TOKEN_CACHE (mode 0600), which is what
    every other entry point caches anyway.

    Scheduled jobs should keep using the environment; this exists so a one-off
    read does not require provisioning a secret first.
    """
    global TBX_EMAIL, TBX_PASSWORD
    email = (email or "").strip()
    password = password or ""
    if not email or not password:
        raise TbxAuthError("set_credentials() needs both an email and a password")
    TBX_EMAIL, TBX_PASSWORD = email, password
    # A cached token from a different account must not be reused: the cache is
    # keyed on (base, email), and _load_cached_token compares against these
    # globals, so switching identity mid-process invalidates it naturally.


def prompt_for_credentials(email: str | None = None) -> None:
    """
    Read credentials from a terminal and hand them to `set_credentials`.

    The password is read with `getpass`, so it is not echoed and does not enter
    shell history. Refuses to run without a TTY rather than silently reading a
    piped password from stdin — a password arriving on a pipe usually means it
    came from a file or a command line, which defeats the point.
    """
    import getpass

    if not sys.stdin.isatty():
        raise TbxAuthError(
            "prompt_for_credentials() needs an interactive terminal. "
            "Set TBX_EMAIL / TBX_PASSWORD in the environment for non-interactive runs."
        )
    email = (email or os.getenv("TBX_EMAIL") or "").strip()
    if not email:
        email = input("TBX email: ").strip()
    password = getpass.getpass(f"TBX password for {email} (not echoed): ")
    set_credentials(email, password)


def _token_cache_path() -> str:
    """
    Token cache file for the account currently configured.

    Correctness never depended on the filename — the blob records
    `(base, email)` and `_load_cached_token` refuses a mismatch. What a
    per-account filename buys is *coexistence*: a read-only reporting user
    and a write-capable user on the same host would otherwise overwrite
    each other's token on every call and re-login constantly. If a second
    `/login` invalidates the first token (unconfirmed — §8.1.5 of
    docs/teqblaze-new-platform.md), that churn is a mutual logout rather
    than merely wasted calls.

    An explicit `TBX_TOKEN_CACHE` is honoured exactly as given.
    """
    if TOKEN_CACHE != _TOKEN_CACHE_DEFAULT:
        return TOKEN_CACHE
    ident = hashlib.sha256(f"{TBX_BASE}|{TBX_EMAIL}".encode()).hexdigest()[:8]
    root, ext = os.path.splitext(_TOKEN_CACHE_DEFAULT)
    return f"{root}_{ident}{ext}"


def _load_cached_token() -> str:
    """Cached JWT if it exists and has >5min of life left, else ''."""
    try:
        with open(_token_cache_path()) as f:
            blob = json.load(f)
    except (OSError, ValueError):
        return ""
    if blob.get("base") != TBX_BASE or blob.get("email") != TBX_EMAIL:
        # Cache belongs to a different host/account — ignore it.
        return ""
    if float(blob.get("expires_at", 0)) - time.time() < 300:
        return ""
    return str(blob.get("token") or "")


def _save_token(token: str, expires_in: int | float | None) -> None:
    # `expires_in` is documented in seconds. Fall back to a conservative
    # 55 minutes when the platform omits it.
    ttl = float(expires_in) if expires_in else 3300.0
    blob = {
        "token": token,
        "expires_at": time.time() + ttl,
        "base": TBX_BASE,
        "email": TBX_EMAIL,
    }
    path = _token_cache_path()
    try:
        with open(path, "w") as f:
            json.dump(blob, f)
        os.chmod(path, 0o600)
    except OSError as exc:
        print(f"{_LOG_PREFIX} WARN: could not cache token — {exc}", file=sys.stderr)


def _login() -> str:
    """POST /login and return a fresh JWT."""
    if not configured():
        raise TbxAuthError(
            "TBX_EMAIL / TBX_PASSWORD are not set. They live in the Render "
            "dashboard (Environment → Add Environment Variable) for the "
            "pgam-intelligence-scheduler worker, or a local .env for dev. "
            "See docs/teqblaze-new-platform.md."
        )
    url = f"{TBX_BASE}/login"
    try:
        resp = requests.post(
            url,
            json={"email": TBX_EMAIL, "password": TBX_PASSWORD},
            headers={"Accept": "application/json"},
            timeout=TIMEOUT,
        )
    except requests.RequestException as exc:
        raise TbxAuthError(f"POST /login unreachable: {exc}") from exc

    if resp.status_code in (401, 403, 422):
        raise TbxAuthError(
            f"POST /login rejected the credentials (HTTP {resp.status_code}). "
            f"This is real signal, not noise — the account may be locked, the "
            f"password rotated, or the IP not allowlisted. Body: {resp.text[:300]}",
            status=resp.status_code,
            body=resp.text[:1000],
        )
    if not resp.ok:
        raise TbxAuthError(
            f"POST /login failed: HTTP {resp.status_code} — {resp.text[:300]}",
            status=resp.status_code,
            body=resp.text[:1000],
        )

    try:
        data = resp.json()
    except ValueError as exc:
        raise TbxAuthError(f"POST /login returned non-JSON: {resp.text[:300]}") from exc

    token = data.get("access_token") or data.get("token")
    if not token:
        raise TbxAuthError(f"POST /login returned no access_token: {str(data)[:300]}")

    _save_token(token, data.get("expires_in"))
    print(f"{_LOG_PREFIX} authenticated as {TBX_EMAIL} (expires_in={data.get('expires_in')})")
    return str(token)


def get_token(force: bool = False) -> str:
    """Return a usable JWT, logging in only when the cache can't serve it."""
    if not force:
        cached = _load_cached_token()
        if cached:
            return cached
    return _login()


def force_refresh_token() -> str:
    """Discard the cached JWT and log in again."""
    try:
        os.remove(_token_cache_path())
    except OSError:
        pass
    return get_token(force=True)


def logout() -> bool:
    """POST /logout and drop the local cache. Best-effort."""
    try:
        _request("POST", "/logout", retry_auth=False)
    except TbxError as exc:
        print(f"{_LOG_PREFIX} logout: {exc}")
    try:
        os.remove(_token_cache_path())
    except OSError:
        pass
    return True


# ---------------------------------------------------------------------------
# Transport
# ---------------------------------------------------------------------------

def _throttle() -> None:
    global _last_call_at
    gap = time.time() - _last_call_at
    if gap < _MIN_INTERVAL:
        time.sleep(_MIN_INTERVAL - gap)
    _last_call_at = time.time()


def _request(
    method: str,
    path: str,
    payload: dict | None = None,
    params: dict | None = None,
    retry_auth: bool = True,
    expect_json: bool = True,
    files: list | None = None,
) -> Any:
    """
    One authenticated call against the new platform.

    Handles: rate throttle, JWT attach, 401 re-login (once), and retry with
    exponential backoff on 429 / 5xx / network error.

    `files` sends a `multipart/form-data` body instead of JSON. Several write
    endpoints require multipart rather than JSON — see `post_form`.
    """
    url = f"{TBX_BASE}/{path.lstrip('/')}"
    attempt = 0
    refreshed = False

    while True:
        attempt += 1
        token = get_token()
        headers = {
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
        }
        try:
            with _LOCK:
                _throttle()
                resp = requests.request(
                    method.upper(), url,
                    # requests sets the multipart boundary itself; never pass
                    # both a JSON body and files.
                    json=payload if (payload is not None and files is None) else None,
                    files=files or None,
                    params=params or None,
                    headers=headers,
                    timeout=TIMEOUT,
                )
        except requests.RequestException as exc:
            if attempt <= MAX_RETRIES:
                backoff = 2 ** attempt
                print(f"{_LOG_PREFIX} {method} {path} network error ({exc}); "
                      f"retry {attempt}/{MAX_RETRIES} in {backoff}s")
                time.sleep(backoff)
                continue
            raise TbxError(f"{method} {path} unreachable after {MAX_RETRIES} retries: {exc}") from exc

        if resp.status_code == 401 and retry_auth and not refreshed:
            # A 401 here means the JWT died mid-run. Surface it loudly —
            # per the TB playbook, silent auth failure means silent
            # write failure — then refresh once and retry.
            print(f"{_LOG_PREFIX} 401 on {method} {path} — refreshing JWT and retrying once",
                  file=sys.stderr)
            force_refresh_token()
            refreshed = True
            continue

        if resp.status_code == 429 or resp.status_code >= 500:
            if attempt <= MAX_RETRIES:
                backoff = 2 ** attempt
                print(f"{_LOG_PREFIX} {method} {path} HTTP {resp.status_code}; "
                      f"retry {attempt}/{MAX_RETRIES} in {backoff}s")
                time.sleep(backoff)
                continue

        if not resp.ok:
            raise TbxError(
                f"{method} {path} failed: HTTP {resp.status_code} — {resp.text[:400]}",
                status=resp.status_code,
                body=resp.text[:2000],
            )

        if not expect_json:
            return resp.content
        if not resp.content:
            return {}
        try:
            return resp.json()
        except ValueError as exc:
            raise TbxError(
                f"{method} {path} returned non-JSON: {resp.text[:300]}",
                status=resp.status_code,
            ) from exc


def get(path: str, params: dict | None = None) -> Any:
    """Authenticated GET. Exposed for endpoints without a typed helper."""
    return _request("GET", path, params=params)


def post(path: str, payload: dict | None = None) -> Any:
    """Authenticated POST. Exposed for endpoints without a typed helper."""
    return _request("POST", path, payload=payload or {})


def build_form_parts(
    fields: dict | None = None,
    files: dict | None = None,
) -> list[tuple]:
    """
    Build the parts list for a `multipart/form-data` body.

    Two wrinkles this handles, both of which will silently 422 otherwise:

    * **Array fields need PHP-style `name[]` keys.** The platform is a Laravel
      app, and endpoints like `/filter-lists/{id}/add-value` declare `value` as
      an array of strings. Sent as a bare repeated `value`, only the last one
      survives.
    * **Scalar parts need a `(None, value)` tuple** so `requests` emits them as
      form fields inside the multipart body rather than switching to
      urlencoded.

    `files` maps a field name to `(filename, bytes_or_str, content_type)`.
    """
    parts: list[tuple] = []
    for key, value in (fields or {}).items():
        if isinstance(value, (list, tuple, set)):
            for item in value:
                parts.append((f"{key}[]", (None, str(item))))
        elif isinstance(value, bool):
            # Laravel reads "1"/"0" for booleans in form bodies; "True" is truthy
            # as a non-empty string, so a False would invert.
            parts.append((key, (None, "1" if value else "0")))
        else:
            parts.append((key, (None, str(value))))
    for key, spec in (files or {}).items():
        parts.append((key, spec))
    return parts


def post_form(path: str, fields: dict | None = None,
              files: dict | None = None) -> Any:
    """
    Authenticated `multipart/form-data` POST.

    Required by the endpoints the spec declares as multipart rather than JSON:
    `/filter-lists/{id}/add-value`, `/remove-value`, `/import-values`, the
    equivalents on `/deals` and `/adapters`, and `/platform-settings/upload-temp`.
    """
    return _request("POST", path, files=build_form_parts(fields, files))


def download(path: str, params: dict | None = None) -> bytes:
    """Authenticated GET returning raw bytes (exports, sellers.json, images)."""
    return _request("GET", path, params=params, expect_json=False)


# ---------------------------------------------------------------------------
# Pagination
# ---------------------------------------------------------------------------

def paginate(
    path: str,
    payload: dict | None = None,
    per_page: int = 250,
    max_pages: int = 200,
) -> Iterator[dict]:
    """
    Yield every row from a paginated POST list endpoint.

    Every list/report endpoint on this platform answers with
    `{"data": [...], "meta": {"current_page", "last_page", "total", ...}}`,
    so one walker covers them all.

    `max_pages` is a runaway guard, not a coverage decision — if it trips we
    log the shortfall rather than silently truncating.
    """
    payload = dict(payload or {})
    page = 1
    while page <= max_pages:
        payload["per_page"] = per_page
        payload["page"] = page
        body = _request("POST", path, payload=payload)
        rows = body.get("data") if isinstance(body, dict) else body
        for row in (rows or []):
            yield row
        meta = (body or {}).get("meta") or {} if isinstance(body, dict) else {}
        last_page = int(meta.get("last_page") or page)
        if page >= last_page:
            return
        page += 1
    print(f"{_LOG_PREFIX} WARN: {path} stopped at max_pages={max_pages} — "
          f"results are TRUNCATED, raise max_pages or tighten the filter",
          file=sys.stderr)


def fetch_all(path: str, payload: dict | None = None, per_page: int = 250,
              max_pages: int = 200) -> list[dict]:
    """`paginate` materialised into a list."""
    return list(paginate(path, payload, per_page=per_page, max_pages=max_pages))


# ---------------------------------------------------------------------------
# Dictionaries (lookup tables)
# ---------------------------------------------------------------------------

_DICT_CACHE: dict[str, list[dict]] = {}


def dictionary(dict_type: str, refresh: bool = False, per_page: int = 500) -> list[dict]:
    """
    Fetch one of the platform's lookup tables, e.g. `countries`,
    `supply-sources`, `failure-reasons`.

    Cached per-process because agents read the same tables repeatedly
    inside a single run.
    """
    if not refresh and dict_type in _DICT_CACHE:
        return _DICT_CACHE[dict_type]
    rows = fetch_all(f"/dictionaries/{dict_type}", {}, per_page=per_page)
    _DICT_CACHE[dict_type] = rows
    return rows


def dictionary_map(dict_type: str, key: str = "id", value: str = "name",
                   refresh: bool = False) -> dict:
    """`dictionary()` collapsed into a {key: value} lookup."""
    return {
        row.get(key): row.get(value)
        for row in dictionary(dict_type, refresh=refresh)
        if row.get(key) is not None
    }


def country_ids(codes_or_names: list[str]) -> list[int]:
    """
    Resolve country names / ISO codes to the platform's numeric country IDs,
    which is what every geo filter and `geo_settings` write expects.

    Unmatched inputs are reported rather than dropped silently.
    """
    rows = dictionary("countries")
    index: dict[str, int] = {}
    for row in rows:
        cid = row.get("id")
        if cid is None:
            continue
        for field in ("name", "code", "alpha2", "alpha3", "iso", "iso2", "iso3"):
            val = row.get(field)
            if isinstance(val, str) and val:
                index[val.strip().lower()] = int(cid)

    resolved, missing = [], []
    for token in codes_or_names:
        cid = index.get(str(token).strip().lower())
        if cid is None:
            missing.append(token)
        else:
            resolved.append(cid)
    if missing:
        print(f"{_LOG_PREFIX} WARN: unresolved countries {missing} — "
              f"check the `countries` dictionary for the exact spelling")
    return resolved


# ---------------------------------------------------------------------------
# Report  (POST /report/{hash})
# ---------------------------------------------------------------------------

def _request_hash(payload: dict) -> str:
    """
    Deterministic hash for a report request.

    `/report/{hash}` keys a server-side result set so that paging through one
    query stays consistent. Deriving it from the canonical payload means the
    same query reuses the same hash across pages and across runs.
    """
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.md5(canonical.encode()).hexdigest()


def build_report_payload(
    date_from: str,
    date_to: str,
    attributes: list[str] | None = None,
    metrics: list[str] | None = None,
    date_granularity: str | None = None,
    timezone: str | None = None,
    filters: dict | None = None,
    sort: list[dict] | None = None,
) -> dict:
    """
    Assemble a `/report` body, validating names against the spec's vocabulary.

    `attributes` / `metrics` accept either the platform's own names or the
    LL-style aliases in `METRIC_ALIASES` (GROSS_REVENUE, PUB_PAYOUT, …), so
    code ported from the LL agents reads the same.

    `filters` is merged into the request's `filter` object verbatim — that
    covers the dimension filters (`supply_source: [22, 35]`,
    `traffic_type: ["CTV"]`, `inventory_key: ["example.com"]`, …) and the
    metric filters, which take `{"operator": ">", "value": "100"}` and behave
    like SQL HAVING.
    """
    attributes = [METRIC_ALIASES.get(a, a) for a in (attributes or [])]
    metrics = [METRIC_ALIASES.get(m, m) for m in (metrics or [])]

    bad_attrs = [a for a in attributes if a not in REPORT_ATTRIBUTES]
    if bad_attrs:
        raise ValueError(
            f"unknown report attribute(s) {bad_attrs}; valid: {list(REPORT_ATTRIBUTES)}"
        )
    bad_metrics = [m for m in metrics if m not in REPORT_METRICS]
    if bad_metrics:
        raise ValueError(
            f"unknown report metric(s) {bad_metrics}; valid: {list(REPORT_METRICS)}"
        )
    if date_granularity and date_granularity not in DATE_GRANULARITIES:
        raise ValueError(
            f"date_granularity must be one of {DATE_GRANULARITIES}, got {date_granularity!r}"
        )

    flt: dict[str, Any] = {
        "date_from": date_from,
        "date_to": date_to,
        "timezone": timezone or DEFAULT_TZ,
    }
    # The platform buckets by `filter.date` (hour/day/month) rather than by a
    # separate attribute; `date` still has to appear in `attributes` for the
    # bucket to come back as a column.
    if date_granularity:
        flt["date"] = date_granularity
    elif "date" in attributes:
        flt["date"] = "day"
    if filters:
        flt.update(filters)

    payload: dict[str, Any] = {"filter": flt}
    if attributes:
        payload["attributes"] = attributes
    if metrics:
        payload["metrics"] = metrics
    if sort:
        payload["sort"] = sort
    return payload


def report(
    date_from: str,
    date_to: str,
    attributes: list[str] | None = None,
    metrics: list[str] | None = None,
    date_granularity: str | None = None,
    timezone: str | None = None,
    filters: dict | None = None,
    sort: list[dict] | None = None,
    per_page: int = 500,
    max_pages: int = 200,
) -> tuple[list[dict], dict]:
    """
    Run a report and return `(rows, totals)`.

    `totals` is the platform's own `total` block — use it rather than summing
    `rows`, because rate metrics (margin, win rate, VCR) are ratios and do
    not add up.

        rows, totals = tbx.report(
            "2026-08-01", "2026-08-18",
            attributes=["date", "supply_source", "country"],
            metrics=["imps_sum", "ssp_price_sum", "dsp_price_sum", "margin"],
            filters={"traffic_type": ["CTV"], "imps_sum": {"operator": ">", "value": "1000"}},
            sort=[{"field": "dsp_price_sum", "direction": "desc"}],
        )
    """
    payload = build_report_payload(
        date_from, date_to, attributes, metrics,
        date_granularity, timezone, filters, sort,
    )
    hsh = _request_hash(payload)

    rows: list[dict] = []
    totals: dict = {}
    page = 1
    while page <= max_pages:
        body = _request("POST", f"/report/{hsh}",
                        payload={**payload, "per_page": per_page, "page": page})
        rows.extend(body.get("data") or [])
        totals = body.get("total") or totals
        meta = body.get("meta") or {}
        last_page = int(meta.get("last_page") or page)
        if page >= last_page:
            break
        page += 1
    else:
        print(f"{_LOG_PREFIX} WARN: report stopped at max_pages={max_pages} — TRUNCATED",
              file=sys.stderr)

    return rows, totals


def report_chart(
    date_from: str,
    date_to: str,
    metrics: list[str] | None = None,
    date_granularity: str = "day",
    timezone: str | None = None,
    filters: dict | None = None,
) -> dict:
    """Time-series form of `report()` — `POST /report/chart/{hash}`."""
    payload = build_report_payload(
        date_from, date_to, ["date"], metrics,
        date_granularity, timezone, filters,
    )
    return _request("POST", f"/report/chart/{_request_hash(payload)}", payload=payload)


def report_columns() -> dict:
    """`GET /report/columns-list` — the platform's own column catalogue.

    Worth calling after a platform upgrade: it is the authoritative list, and
    will show new attributes/metrics before this module's constants do.
    """
    return _request("GET", "/report/columns-list")


def export_report(
    date_from: str,
    date_to: str,
    attributes: list[str] | None = None,
    metrics: list[str] | None = None,
    **kwargs,
) -> Any:
    """`POST /report/export/{hash}` — server-side export of the same query."""
    payload = build_report_payload(date_from, date_to, attributes, metrics, **kwargs)
    return _request("POST", f"/report/export/{_request_hash(payload)}", payload=payload)


def keep_hash_alive(request_hash: str) -> dict:
    """
    `POST /active-hash/update/{hash}` — extend a cached result set's TTL.

    Only needed for a long paging walk over a very large report, where the
    server-side result set could expire mid-walk.
    """
    return _request("POST", f"/active-hash/update/{request_hash}")


# ---------------------------------------------------------------------------
# Bids Overview  — why bid requests get dropped
# ---------------------------------------------------------------------------

def bids_overview(
    kind: str = "incoming",
    date_from: str | None = None,
    date_to: str | None = None,
    filters: dict | None = None,
    sort: list[dict] | None = None,
    per_page: int = 250,
    max_pages: int = 50,
) -> list[dict]:
    """
    `POST /bids-overview/{incoming|outgoing|responses}`.

    The fill-funnel diagnostic: per supply source / placement / demand source
    it returns total_count, valid_count, dropped_count, drop_rate and a
    reason breakdown. `bids_overview_details` expands one slice into named
    drop reasons.
    """
    if kind not in ("incoming", "outgoing", "responses"):
        raise ValueError("kind must be one of: incoming, outgoing, responses")
    flt = dict(filters or {})
    if date_from:
        flt["date_from"] = date_from
    if date_to:
        flt["date_to"] = date_to
    payload: dict[str, Any] = {"filter": flt}
    if sort:
        payload["sort"] = sort
    return fetch_all(f"/bids-overview/{kind}", payload, per_page=per_page, max_pages=max_pages)


def bids_overview_details(kind: str = "incoming", filters: dict | None = None) -> dict:
    """`POST /bids-overview/details/{kind}` — drop counts by named reason."""
    return _request("POST", f"/bids-overview/details/{kind}",
                    payload={"filter": dict(filters or {})})


def failure_reasons() -> list[dict]:
    """The `failure-reasons` dictionary — maps reason_id → human label."""
    return dictionary("failure-reasons")


# ---------------------------------------------------------------------------
# HUMAN report  — invalid-traffic risk (MFA / SIVT / GIVT)
# ---------------------------------------------------------------------------

def human_report(
    kind: str = "risk-metrics",
    date_from: str | None = None,
    date_to: str | None = None,
    attributes: list[str] | None = None,
    metrics: list[str] | None = None,
    timezone: str | None = None,
    inventory_key: list[str] | None = None,
    per_page: int = 250,
    max_pages: int = 50,
) -> list[dict]:
    """
    `POST /human-report/{risk-metrics|traffic-report}`.

    risk-metrics  → requests_sum, mfa_sum/mfa_rate, sivt_sum/sivt_rate,
                    givt_sum/givt_rate, by date and/or inventory_key.
    traffic-report → impressions_sum, charge_amount_sum (what HUMAN bills).

    This is the first programmatic IVT feed PGAM has: the existing
    compliance agents infer quality from ads.txt and schain posture only.
    """
    if kind not in ("risk-metrics", "traffic-report"):
        raise ValueError("kind must be 'risk-metrics' or 'traffic-report'")
    flt: dict[str, Any] = {"timezone": timezone or DEFAULT_TZ}
    if date_from:
        flt["date_from"] = date_from
    if date_to:
        flt["date_to"] = date_to
    if inventory_key:
        flt["inventory_key"] = inventory_key
    payload: dict[str, Any] = {"filter": flt}
    if attributes:
        payload["attributes"] = attributes
    if metrics:
        payload["metrics"] = metrics
    return fetch_all(f"/human-report/{kind}", payload, per_page=per_page, max_pages=max_pages)


def human_report_settings() -> dict:
    """`GET /human-report/settings` — is the HUMAN integration live at all."""
    return _request("GET", "/human-report/settings")


# ---------------------------------------------------------------------------
# Supply-chain integrity surfaces
# ---------------------------------------------------------------------------

def schain_utilisation(
    date_from: str,
    date_to: str,
    attributes: list[str] | None = None,
    metrics: list[str] | None = None,
    timezone: str | None = None,
    filters: dict | None = None,
    per_page: int = 250,
    max_pages: int = 100,
) -> list[dict]:
    """
    `POST /schain-utilisation`.

    Incoming vs outgoing schain posture per supply/demand/inventory_key:
    node counts, sellers.json-verified node counts, ads.txt-verified node
    counts, and whether the chain is complete — as *observed in live
    traffic*, which the static config audits in `agents/compliance` cannot
    see.
    """
    flt: dict[str, Any] = {
        "date_from": date_from, "date_to": date_to,
        "timezone": timezone or DEFAULT_TZ,
    }
    flt.update(filters or {})
    payload: dict[str, Any] = {"filter": flt}
    if attributes:
        payload["attributes"] = attributes
    if metrics:
        payload["metrics"] = metrics
    return fetch_all("/schain-utilisation", payload, per_page=per_page, max_pages=max_pages)


def sellers_validation(
    date_from: str,
    date_to: str,
    attributes: list[str] | None = None,
    metrics: list[str] | None = None,
    timezone: str | None = None,
    filters: dict | None = None,
    per_page: int = 250,
    max_pages: int = 100,
) -> list[dict]:
    """
    `POST /sellers-validation`.

    Per seller_domain + inventory_key: sellers.json verification state,
    ads.txt verification state, node position and node rank — the platform's
    own crawl. Covers the same ground as `agents/compliance/crawlers` without
    PGAM having to fetch a single ads.txt itself.
    """
    flt: dict[str, Any] = {
        "date_from": date_from, "date_to": date_to,
        "timezone": timezone or DEFAULT_TZ,
    }
    flt.update(filters or {})
    payload: dict[str, Any] = {"filter": flt}
    if attributes:
        payload["attributes"] = attributes
    if metrics:
        payload["metrics"] = metrics
    return fetch_all("/sellers-validation", payload, per_page=per_page, max_pages=max_pages)


def ads_txt_verification(
    filters: dict | None = None,
    sort: list[dict] | None = None,
    per_page: int = 250,
    max_pages: int = 100,
) -> list[dict]:
    """
    `POST /ads-txt-verification` — crawl results per publisher domain:
    crawled_domain, ads_txt_url, status. `/ads-txt-verification/history`
    gives the same per-domain trail over time.
    """
    payload: dict[str, Any] = {"filter": dict(filters or {})}
    if sort:
        payload["sort"] = sort
    return fetch_all("/ads-txt-verification", payload, per_page=per_page, max_pages=max_pages)


def ads_txt_verification_history(filters: dict | None = None,
                                 per_page: int = 250) -> list[dict]:
    """`POST /ads-txt-verification/history`."""
    return fetch_all("/ads-txt-verification/history",
                     {"filter": dict(filters or {})}, per_page=per_page)


def scanner_settings(source_type: str | None = None) -> Any:
    """
    `GET /scanner-settings` (or `/scanner-settings/{source_type}`) — the
    third-party scanners configured on the account, each with `scanner_id`,
    `name`, `key`, `type` (prebid/postbid) and `status`.

    Note the vendors here are the platform's own scanner integrations — the
    spec names Pixalate, Protected Media, FraudSensor, MediaGuard and GeoEdge.
    **HUMAN is not one of them**; it has its own module, `human_report` and
    `human_report_settings`. Do not read this expecting to find HUMAN.

    Which scanners are *enabled per source* is a different question again: that
    lives on each supply/demand source's own `scanner_settings[]` array, keyed
    on the `setting_id` values this returns.
    """
    path = f"/scanner-settings/{source_type}" if source_type else "/scanner-settings"
    return _request("GET", path)


def scanner_statistics(
    kind: str = "prebid",
    date_from: str | None = None,
    date_to: str | None = None,
    attributes: list[str] | None = None,
    metrics: list[str] | None = None,
    timezone: str | None = None,
    filters: dict | None = None,
    per_page: int = 250,
) -> list[dict]:
    """
    `POST /scanner-statistics/{prebid|postbid}`.

    prebid  → requests_sum, blocked_sum, blocked_rate (what the scanner
              refused before bidding)
    postbid → scan_attempts, scans

    Answers "is the fraud scanning we pay for actually blocking anything,
    and on which supply source".
    """
    flt: dict[str, Any] = {"timezone": timezone or DEFAULT_TZ}
    if date_from:
        flt["date_from"] = date_from
    if date_to:
        flt["date_to"] = date_to
    flt.update(filters or {})
    payload: dict[str, Any] = {"filter": flt}
    if attributes:
        payload["attributes"] = attributes
    if metrics:
        payload["metrics"] = metrics
    return fetch_all(f"/scanner-statistics/{kind}", payload, per_page=per_page)


# ---------------------------------------------------------------------------
# Discrepancy report  — platform numbers vs partner-reported numbers
# ---------------------------------------------------------------------------

def discrepancy_report(
    date_from: str,
    date_to: str,
    filters: dict | None = None,
    sort: list[dict] | None = None,
    per_page: int = 250,
    max_pages: int = 50,
) -> list[dict]:
    """
    `POST /discrepancy-report`.

    For every source with an API-sync URL configured, returns impressions /
    spend as the platform counted them beside impressions_api / spend_api as
    the partner reports them, plus the discrepancy %. This is automated
    revenue recon — the thing `agents/recon` currently reconstructs by hand.

    Filter on the discrepancy itself to get straight to the outliers:

        discrepancy_report("2026-08-01", "2026-08-18",
                           filters={"spend_discrepancy": {"operator": ">", "value": 10}})
    """
    flt: dict[str, Any] = {"date_from": date_from, "date_to": date_to}
    flt.update(filters or {})
    payload: dict[str, Any] = {"filter": flt}
    if sort:
        payload["sort"] = sort
    return fetch_all("/discrepancy-report", payload, per_page=per_page, max_pages=max_pages)


def discrepancy_statistic(kind: str) -> dict:
    """`GET /report/statistic/{type}` — discrepancy summary counters."""
    return _request("GET", f"/report/statistic/{kind}")


def validate_api_sync_url(url: str, url_type: str = "csv",
                          map_headers: dict | None = None) -> dict:
    """
    `POST /discrepancy-report/validate-api-url` — dry-check a partner's
    reporting URL before committing it with `set_api_sync_url`.
    """
    payload: dict[str, Any] = {"url": url, "type": url_type}
    if map_headers:
        payload["map_headers"] = map_headers
    return _request("POST", "/discrepancy-report/validate-api-url", payload=payload)


# ---------------------------------------------------------------------------
# Traffic logger  — raw request/response samples
# ---------------------------------------------------------------------------

def traffic_logger(filters: dict | None = None, per_page: int = 100,
                   max_pages: int = 10) -> list[dict]:
    """
    `POST /traffic-logger` — sampled raw bid traffic (supply_request,
    supply_response, demand_request, demand_response bodies plus the event
    that fired).

    Debug-grade and verbose: default caps are deliberately small. Use it to
    answer "what exactly did we send this DSP", not for aggregation.
    """
    return fetch_all("/traffic-logger", {"filter": dict(filters or {})},
                     per_page=per_page, max_pages=max_pages)


def traffic_logger_columns() -> dict:
    """`GET /traffic-logger/columns-list`."""
    return _request("GET", "/traffic-logger/columns-list")


# ---------------------------------------------------------------------------
# Help centre — the platform documents itself through its own API
# ---------------------------------------------------------------------------
#
# The UI at https://ssp-new.pgammedia.com/help-center/... is a front end over
# these three endpoints. That matters practically: the documentation is
# machine-readable, so a session with credentials can pull the whole thing
# instead of asking someone to copy-paste a page out of a browser. Worth
# remembering whenever a "can you read this doc page" question comes up.

def help_center(space: str = "management-api") -> Any:
    """
    `GET /help-center/{space}` — the article tree for one space.

    `space` is the slug from the UI path, e.g. `management-api` for
    https://ssp-new.pgammedia.com/help-center/management-api.
    """
    return _request("GET", f"/help-center/{space}")


def help_center_article(space: str, article_id: str | int) -> Any:
    """`GET /help-center/{space}/{id}` — one article's full body."""
    return _request("GET", f"/help-center/{space}/{article_id}")


def help_center_search(space: str, query: str) -> Any:
    """`GET /help-center/{space}/search?query=…`."""
    return _request("GET", f"/help-center/{space}/search", params={"query": query})


def dump_help_center(space: str = "management-api") -> dict:
    """
    Walk a space and return `{article_id: article}` for everything in it.

    Used by `scripts/tbx_pull.py` so the platform's own documentation lands in
    the artifact alongside the data. The tree shape is undocumented in the
    spec — `data` may be a list or a nested dict — so this handles both and
    reports what it could not parse rather than silently returning less.
    """
    tree = help_center(space)
    ids: list[str] = []

    def walk(node: Any) -> None:
        if isinstance(node, dict):
            if node.get("id") is not None and ("title" in node or "name" in node):
                ids.append(str(node["id"]))
            for value in node.values():
                walk(value)
        elif isinstance(node, list):
            for item in node:
                walk(item)

    walk(tree)
    out: dict[str, Any] = {"_tree": tree, "_article_ids": ids}
    for aid in ids:
        try:
            out[aid] = help_center_article(space, aid)
        except TbxError as exc:
            out[aid] = {"error": str(exc)[:200]}
    if not ids:
        print(f"{_LOG_PREFIX} help centre '{space}': found no article ids in the "
              f"tree — inspect `_tree` in the output by hand", file=sys.stderr)
    return out


# ---------------------------------------------------------------------------
# Dashboard
# ---------------------------------------------------------------------------

def dashboard_chart(payload: dict) -> dict:
    """`POST /dashboard/chart` — the platform's own dashboard series."""
    return _request("POST", "/dashboard/chart", payload=payload)


def dashboard_widgets() -> Any:
    """`GET /dashboard-widget` — saved widget definitions."""
    return _request("GET", "/dashboard-widget")


# ---------------------------------------------------------------------------
# Connectivity check
# ---------------------------------------------------------------------------

def test_connection(verbose: bool = True) -> bool:
    """
    Verify credentials and basic read access. Safe: read-only.

    Returns True when login succeeded and `/permissions` answered.
    """
    if not configured():
        if verbose:
            print(f"{_LOG_PREFIX} not configured — TBX_EMAIL / TBX_PASSWORD absent")
        return False
    try:
        get_token(force=True)
        perms = _request("GET", "/permissions")
        if verbose:
            count = len(perms.get("data", perms)) if isinstance(perms, dict) else len(perms or [])
            print(f"{_LOG_PREFIX} ✓ connected to {TBX_BASE} — {count} permission entries")
        return True
    except TbxError as exc:
        if verbose:
            print(f"{_LOG_PREFIX} ✗ {exc}", file=sys.stderr)
        return False


def permissions() -> Any:
    """`GET /permissions` — what this account is allowed to do.

    Worth reading before wiring a write agent: the account may hold read
    access to a module and no write access.
    """
    return _request("GET", "/permissions")


if __name__ == "__main__":
    test_connection()
