"""
core/msn_partner_hub.py — Authenticated client for the MSN Partner Hub API.

The Partner Hub (https://www.msn.com/en-us/partnerhub/...) is a SPA that
authenticates via MSAL.js PKCE auth-code flow against
login.microsoftonline.com and then calls api.msn.com endpoints with a
short-lived Bearer JWE in the `authorization` header. The JWE is opaque
(encrypted by MSN) so we can't decode or refresh it ourselves — but we
don't need to. We let MSAL handle the OAuth dance inside a real browser
via Playwright and call the API from inside that browser's page context
where the SDK auto-attaches the bearer.

Design choices
--------------
- One-time interactive login: first run pops a Chromium window so the user
  can enter credentials + handle MFA if prompted. Subsequent runs use the
  persisted user-data-dir and start headless.
- We hit `api.msn.com` via `page.evaluate(fetch(...))` so we never have to
  see or manage the bearer ourselves.
- The Partner Hub page is reloaded every `_REFRESH_AFTER_MIN` minutes to
  ensure the in-page bearer hasn't expired (default lifetime ~1h).
- All endpoint params are derived; only `partnerId`, `startDate`, `endDate`,
  and pagination differ between calls.

Environment
-----------
- MSN_EMAIL, MSN_PASSWORD — populated in .env
- MSN_SESSION_DIR (optional) — overrides ~/.pgam/msn-session
- MSN_HEADLESS (optional, default "1") — set to "0" to force a visible
  browser (useful for the first interactive login or debugging)
"""

from __future__ import annotations

import json
import os
import time
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterator, Optional
from urllib.parse import urlencode

try:
    # Lazy import: Playwright is a heavy dep (~300MB Chromium) we don't
    # want to require for engineers who only run other agents.
    from playwright.sync_api import (
        Browser,
        BrowserContext,
        Page,
        Playwright,
        TimeoutError as PlaywrightTimeout,
        sync_playwright,
    )
    _PLAYWRIGHT_AVAILABLE = True
except ImportError:
    _PLAYWRIGHT_AVAILABLE = False


# ---------------------------------------------------------------------------
# Constants — discovered from DevTools traces against Partner Hub on
# 2026-05-16 for BoxingNews (partner AA1lKiff). The apikey is a public
# client identifier shared across all Partner Hub users.
# ---------------------------------------------------------------------------
API_HOST = "https://api.msn.com"
API_BASE_PATH = "/msn/v0/pages/ugc/insights/content"
PARTNER_HUB_URL = "https://www.msn.com/en-us/partnerhub/analytics/realtime/headline"

APIKEY = "tfFF5vu2Sk8ndqqn6je2Vo4qOFve5LeicxEpNSnoZK"
DEFAULT_PARTNER_ID = "AA1lKiff"           # BoxingNews
DEFAULT_PARTNER_TYPE = "2"

# Flight tags from the trace; copy verbatim so we look like the SPA.
# The realtime endpoints and the report/earning endpoints emit different
# flight sets from the same SPA — we pass the appropriate one per call.
_UGC_FLIGHTS = (
    "prg-ugc-benchmark,prg-ugc-revagvnext,prg-ugc-timespent,"
    "prg-ugc-aiusage,prg-ugc-shortinsight,prg-ugc-pcm"
)
_UGC_REPORT_FLIGHTS = (
    "prg-ugc-benchmark,prg-ugc-shortinsight,prg-ugc-pcm,"
    "prg-ugc-fixshorts,prg-ugc-sastokenux"
)

# Sentinel values MSN's UI uses for "all" / "no filter".
_ALL = "-2"
_NO_TITLE = "-1"

# Page reload cadence. MSN bearers seem to last ~1h; we refresh well
# before that to avoid mid-batch 401s.
_REFRESH_AFTER_MIN = 40

# Pagination — realtime endpoint returns up to 20 records per page and
# we paginate via $skip until recordCount is exhausted. Hard cap so a
# runaway recordCount can't loop forever.
PAGE_SIZE = 20
MAX_PAGES = 50


def _default_session_dir() -> Path:
    override = os.environ.get("MSN_SESSION_DIR")
    if override:
        return Path(override).expanduser()
    return Path.home() / ".pgam" / "msn-session"


def _iso_z(dt: datetime) -> str:
    """Format a UTC datetime as MSN expects: 2026-05-16T20:35Z (no seconds)."""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    dt = dt.astimezone(timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%MZ")


def _now_utc() -> datetime:
    return datetime.now(tz=timezone.utc)


class PartnerHubError(RuntimeError):
    """Raised when the Partner Hub flow can't complete (auth, network, etc)."""


class PartnerHubClient:
    """Long-lived, reusable Partner Hub session.

    Typical use:

        with PartnerHubClient().session() as client:
            page1 = client.fetch_realtime(skip=0)
            page2 = client.fetch_realtime(skip=20)
            daily = client.fetch_aggregate()

    The context manager guarantees the browser is closed cleanly. For
    long-running schedulers, you can also instantiate and call `start()`
    / `close()` directly; the client will auto-refresh the in-page bearer
    by reloading the Partner Hub URL when needed.
    """

    def __init__(
        self,
        partner_id: str = DEFAULT_PARTNER_ID,
        email: Optional[str] = None,
        password: Optional[str] = None,
        session_dir: Optional[Path] = None,
        headless: Optional[bool] = None,
    ) -> None:
        if not _PLAYWRIGHT_AVAILABLE:
            raise PartnerHubError(
                "playwright is not installed. Run: "
                "`pip install playwright && playwright install chromium`"
            )
        self.partner_id = partner_id
        self.email = email or os.environ.get("MSN_EMAIL", "").strip()
        self.password = password or os.environ.get("MSN_PASSWORD", "").strip()
        self.session_dir = (session_dir or _default_session_dir()).resolve()
        self.session_dir.mkdir(parents=True, exist_ok=True)

        # Default to headless except for first run (no session yet) so the
        # user can complete login + MFA interactively. Override with
        # MSN_HEADLESS=0 to force visible.
        env_headless = os.environ.get("MSN_HEADLESS")
        if headless is not None:
            self.headless = headless
        elif env_headless is not None:
            self.headless = env_headless not in ("0", "false", "no")
        else:
            self.headless = self._has_existing_session()

        self._pw: Optional[Playwright] = None
        self._ctx: Optional[BrowserContext] = None
        self._page: Optional[Page] = None
        self._last_refresh_at: Optional[datetime] = None

        # 2026-05-18: api.msn.com auths on the MSAL Bearer JWE token
        # that the SPA injects via an axios interceptor. Our own
        # page.evaluate(fetch(...)) bypasses axios so the Bearer
        # never gets attached → 401.
        # Fix: listen for the SPA's first authenticated request on
        # api.msn.com, snapshot its Authorization header, then
        # replay it on our own paginated calls via the Authorization
        # header directly.
        self._captured_bearer: Optional[str] = None
        self._bearer_captured_at: Optional[datetime] = None

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def _has_existing_session(self) -> bool:
        """A persisted Chromium user-data-dir has a Default/Cookies SQLite
        file once a login has succeeded. Cheap heuristic to decide whether
        first-run UX is needed."""
        cookies_db = self.session_dir / "Default" / "Cookies"
        return cookies_db.exists()

    def start(self) -> "PartnerHubClient":
        if self._ctx is not None:
            return self
        self._pw = sync_playwright().start()
        self._ctx = self._pw.chromium.launch_persistent_context(
            user_data_dir=str(self.session_dir),
            headless=self.headless,
            viewport={"width": 1400, "height": 900},
            # Real-UA reduces "browser looks weird" detection from the
            # Partner Hub stack and makes the Network trace match what
            # the user sees in their own DevTools.
            user_agent=(
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/148.0.0.0 Safari/537.36"
            ),
        )
        self._page = self._ctx.new_page()
        # Attach the Bearer-capture listener BEFORE navigation so we
        # don't miss the SPA's first authenticated request.
        self._page.on("request", self._on_request)
        self._open_partner_hub()
        return self

    def _on_request(self, request: Any) -> None:
        """Network-listener callback. Captures the Authorization Bearer
        token specifically from Partner Hub UGC-insights requests —
        which is the SCOPE of token we need to replay against the
        /realtime endpoint.

        2026-05-19: previously we accepted any Bearer for any api.msn.com
        request. The SPA fires many api.msn.com calls (homepage feed,
        weather, telemetry) that carry public/anon bearers — first-to-
        capture wins, and on a cold GH Actions runner the public bearers
        often land first. Scoping to API_BASE_PATH (the UGC insights
        surface) ensures we only ever capture the bearer the Partner
        Hub SPA uses for its own authenticated insights calls."""
        try:
            url = request.url
            # Match the exact path prefix we're going to call ourselves.
            # Other api.msn.com endpoints carry different/anon bearers
            # that 401 against /realtime.
            if API_BASE_PATH not in url:
                return
            headers = request.headers
            auth = headers.get("authorization") or headers.get("Authorization")
            if auth and auth.lower().startswith("bearer "):
                if self._captured_bearer != auth:
                    is_first = self._captured_bearer is None
                    self._captured_bearer = auth
                    self._bearer_captured_at = _now_utc()
                    if is_first:
                        # Helpful breadcrumb (only on first capture per
                        # session — subsequent refreshes are silent).
                        tail = auth[-12:] if len(auth) > 12 else "<short>"
                        print(f"[msn_partner_hub] captured Partner Hub bearer (...{tail}) from {url[:80]}")
        except Exception:
            # A listener that throws kills future events; swallow.
            pass

    def close(self) -> None:
        try:
            if self._ctx is not None:
                self._ctx.close()
        finally:
            self._ctx = None
            self._page = None
            if self._pw is not None:
                self._pw.stop()
                self._pw = None

    @contextmanager
    def session(self) -> Iterator["PartnerHubClient"]:
        self.start()
        try:
            yield self
        finally:
            self.close()

    # ------------------------------------------------------------------
    # Login + page management
    # ------------------------------------------------------------------

    def _open_partner_hub(self) -> None:
        assert self._page is not None
        self._page.goto(PARTNER_HUB_URL, wait_until="domcontentloaded")
        self._ensure_logged_in()
        # Wait for the SPA to mount and fire its first analytics call.
        # The dashboard makes the `realtime` request on its own; once
        # we see it land, the in-page bearer is ready for our own
        # fetches to piggy-back on.
        self._wait_for_app_ready()
        self._last_refresh_at = _now_utc()

    def _ensure_logged_in(self) -> None:
        """Detect whether MSAL has redirected us to login.microsoftonline.com
        and, if so, drive the username/password form. Persistent context
        means this typically only runs on the very first call.

        2026-05-18 update: MSN's Partner Hub serves a public unauthenticated
        shell at /en-us/partnerhub/* with a "Sign in" CTA rather than
        immediately redirecting unauthed users to login.microsoftonline.com.
        The previous heuristic ("URL contains 'partnerhub' → we're logged
        in") therefore false-positived on a fresh session, never triggered
        the login flow, and 401'd on the first API call.
        New heuristic: explicitly look for the "Sign in" CTA on the page;
        if present, click it to kick off the MSAL redirect dance. THEN
        watch for the login URL the same way as before.
        """
        assert self._page is not None

        # Step 1: trigger the login redirect if we're sitting on the
        # anonymous Partner Hub shell. Try a few common Sign-in selectors;
        # any one match is enough.
        sign_in_selectors = (
            'a:has-text("Sign in")',
            'button:has-text("Sign in")',
            '[aria-label="Sign in"]',
            '[data-testid="signin-button"]',
        )
        for sel in sign_in_selectors:
            try:
                btn = self._page.locator(sel).first
                if btn.is_visible(timeout=1500):
                    btn.click()
                    break
            except PlaywrightTimeout:
                continue
            except Exception:
                # Selector mis-fires shouldn't kill the flow.
                continue

        # Step 2: poll for the URL to become the Microsoft login surface
        # (which fires either via the Sign-in click above or via the
        # SPA's MSAL handshake on mount).
        for _ in range(60):  # up to ~30s polling
            url = self._page.url
            if "login.microsoftonline.com" in url or "login.live.com" in url:
                break
            self._page.wait_for_timeout(500)
        else:
            # Never reached a login redirect within 30s. Could mean:
            #   (a) we're genuinely already authenticated (session cookie
            #       picked up the OAuth dance silently), OR
            #   (b) the Sign-in element wasn't found and the SPA never
            #       redirected (likely a UI change on MSN's side).
            # Caller's _call() will surface a 401 in case (b), so we
            # return here and let the API speak for itself.
            return

        if not self.email or not self.password:
            # No creds in env — punt to the human running this. Visible
            # browser will be open; they can finish login interactively
            # and we'll resume after Partner Hub loads.
            print(
                "[msn_partner_hub] No MSN_EMAIL/MSN_PASSWORD in env. "
                "Complete login interactively in the Chromium window. "
                "Waiting up to 5 minutes..."
            )
            self._page.wait_for_url("**/partnerhub/**", timeout=5 * 60_000)
            return

        try:
            # Email page
            self._page.locator('input[type="email"]').first.fill(self.email)
            self._page.locator('input[type="submit"], button[type="submit"]').first.click()
            # Password page
            self._page.locator('input[type="password"]').first.wait_for(timeout=15_000)
            self._page.locator('input[type="password"]').first.fill(self.password)
            self._page.locator('input[type="submit"], button[type="submit"]').first.click()
            # "Stay signed in?" — answer Yes to persist refresh tokens.
            try:
                self._page.locator('input[type="submit"][value="Yes"], button:has-text("Yes")').first.click(
                    timeout=10_000
                )
            except PlaywrightTimeout:
                pass  # MFA path or page skipped — fine, we just wait below
            # MFA may interrupt here; we wait for the final redirect.
            self._page.wait_for_url("**/partnerhub/**", timeout=5 * 60_000)
        except PlaywrightTimeout as exc:
            raise PartnerHubError(
                f"Timed out waiting for login redirect: {exc}. "
                "If MFA is required, set MSN_HEADLESS=0 and complete it manually once; "
                "the session will then persist."
            ) from exc

    def _wait_for_app_ready(self) -> None:
        """The SPA fires its own `realtime` XHR on mount carrying the
        Authorization Bearer header — that's what we listen for to
        confirm auth state.

        2026-05-19: bumped from 25s → 90s after observing the SPA
        consistently fails to fire its first authenticated call within
        25s on cold GH Actions Linux runners. The MSAL bootstrap on a
        constrained runner is slow; locally on a warm Mac it's ~3-5s,
        but on a cold runner it can be 30-60s before the SPA decides
        it's authenticated and pings the API.

        Also added explicit URL polling: if the page navigated away
        from Partner Hub (e.g. to a login prompt) we surface that with
        a clearer error than the generic 401-from-API-call."""
        assert self._page is not None
        try:
            self._page.wait_for_load_state("networkidle", timeout=20_000)
        except PlaywrightTimeout:
            # Some Partner Hub pages keep WebSockets open, so networkidle
            # never fires. That's fine — we'll just poll for the bearer.
            pass
        # Poll for up to 90s waiting for the SPA's first authenticated
        # api.msn.com request (captured by self._on_request).
        max_iterations = 180  # 180 × 500ms = 90s
        for i in range(max_iterations):
            if self._captured_bearer is not None:
                if i > 10:
                    # Helpful telemetry: how long did it actually take?
                    print(f"[msn_partner_hub] bearer captured after {i*0.5:.1f}s")
                return
            # Every 20s, dump the current URL so we can debug what state
            # the page got stuck in.
            if i > 0 and i % 40 == 0:
                try:
                    print(f"[msn_partner_hub] still waiting for bearer at {i*0.5:.0f}s, page url: {self._page.url}")
                except Exception:
                    pass
            self._page.wait_for_timeout(500)
        # No bearer captured. Most likely the page is unauthenticated.
        # Surface the final URL so failed runs are easier to debug.
        try:
            final_url = self._page.url
        except Exception:
            final_url = "<unknown>"
        print(
            f"[msn_partner_hub] WARNING: no Bearer captured from SPA traffic "
            f"within 90s. Final URL: {final_url}. The SPA may be "
            f"unauthenticated, or the API surface changed."
        )

    def _maybe_refresh(self) -> None:
        """Reload Partner Hub when the in-page bearer is getting stale."""
        if self._last_refresh_at is None:
            return
        age = _now_utc() - self._last_refresh_at
        if age >= timedelta(minutes=_REFRESH_AFTER_MIN):
            assert self._page is not None
            self._page.reload(wait_until="domcontentloaded")
            self._wait_for_app_ready()
            self._last_refresh_at = _now_utc()

    # ------------------------------------------------------------------
    # API calls — executed inside the page's JS context so MSAL auto-
    # attaches the Bearer JWE.
    # ------------------------------------------------------------------

    def _build_common_params(
        self,
        *,
        start: datetime,
        end: datetime,
    ) -> dict[str, str]:
        return {
            "apikey": APIKEY,
            "brandId": _ALL,
            "clickSource": _ALL,
            "contentType": _ALL,
            "date": _ALL,
            "device": _ALL,
            "endDate": _iso_z(end),
            "fdhead": _UGC_FLIGHTS,
            "lang": _ALL,
            "mkt": _ALL,
            "ocid": "msph",
            "partnerId": self.partner_id,
            "partnerType": DEFAULT_PARTNER_TYPE,
            "scn": "MSNRPSAuth",
            "skipaadal": "true",
            "startDate": _iso_z(start),
            "timeout": "30000",
            "title": _NO_TITLE,
            "ugc-flights": _UGC_FLIGHTS,
            "vertical": _ALL,
            "wrapodata": "false",
        }

    def _call(self, path: str, params: dict[str, str]) -> dict[str, Any]:
        """Run `fetch(url, …)` inside Partner Hub's page context, with
        the SPA's captured Bearer token attached, and return the parsed
        JSON. Raises PartnerHubError on non-2xx.

        Until 2026-05-18 we relied on the page-context fetch to pick up
        the SPA's axios interceptor — but the interceptor only catches
        axios calls, not raw fetch(). So we explicitly attach the
        captured Authorization header here."""
        self._maybe_refresh()
        assert self._page is not None
        url = f"{API_HOST}{path}?{urlencode(params, safe=',-')}"
        bearer = self._captured_bearer or ""
        js = """
        async ({ url, bearer }) => {
            const headers = {
                'accept': '*/*',
                'content-type': 'application/json',
            };
            if (bearer) {
                headers['authorization'] = bearer;
            }
            const r = await fetch(url, {
                method: 'GET',
                credentials: 'include',
                headers,
            });
            const text = await r.text();
            return { status: r.status, body: text };
        }
        """
        result = self._page.evaluate(js, {"url": url, "bearer": bearer})
        status = result.get("status")
        body = result.get("body") or ""
        if status == 401:
            # The persistent context exists but the in-page MSAL bearer
            # is missing or expired. Most common cause: first run created
            # the session dir but the user closed the window before
            # completing login. Surface this as an actionable error.
            raise PartnerHubError(
                "MSN API returned 401 Unauthorized. The persistent Playwright "
                "session at "
                f"{self.session_dir} exists but isn't authenticated. Re-run "
                "with MSN_HEADLESS=0 in the environment to open a visible "
                "browser and complete login (and MFA if prompted) once; the "
                "session persists thereafter."
            )
        if status != 200:
            raise PartnerHubError(
                f"MSN API {path} returned HTTP {status}: {body[:300]}"
            )
        try:
            return json.loads(body)
        except json.JSONDecodeError as exc:
            raise PartnerHubError(
                f"MSN API {path} returned non-JSON: {body[:300]}"
            ) from exc

    # ------------------------------------------------------------------
    # Public methods
    # ------------------------------------------------------------------

    def fetch_realtime(
        self,
        *,
        skip: int = 0,
        top: int = PAGE_SIZE,
        end: Optional[datetime] = None,
        window_hours: int = 24,
    ) -> dict[str, Any]:
        """Hit /realtime and return parsed JSON (recordList + recordCount).

        The endpoint is a 24h rolling window keyed off the (start, end)
        timestamps. We mirror Partner Hub's behavior of asking for
        "ending now, starting 24h ago"."""
        end_dt = end or _now_utc()
        start_dt = end_dt - timedelta(hours=window_hours)
        params = self._build_common_params(start=start_dt, end=end_dt)
        params.update({
            "$orderBy": "view",
            "$skip": str(skip),
            "$top": str(top),
        })
        return self._call(f"{API_BASE_PATH}/realtime", params)

    def fetch_realtime_all(
        self,
        *,
        end: Optional[datetime] = None,
        window_hours: int = 24,
        max_pages: int = MAX_PAGES,
    ) -> tuple[list[dict[str, Any]], int]:
        """Paginate /realtime until recordCount is exhausted.

        Returns (all_records, record_count). Each record has at least
        `title`, `titleStatus`, `docID`, `readCount`."""
        records: list[dict[str, Any]] = []
        record_count = 0
        seen_doc_ids: set[str] = set()
        for page in range(max_pages):
            skip = page * PAGE_SIZE
            payload = self.fetch_realtime(skip=skip, end=end, window_hours=window_hours)
            chunk = payload.get("recordList") or []
            record_count = int(payload.get("recordCount") or 0)
            if not chunk:
                break
            # Defensive dedup — repeated calls can race the rolling
            # window. We keep the first occurrence (highest rank).
            for rec in chunk:
                doc_id = rec.get("docID")
                if not doc_id or doc_id in seen_doc_ids:
                    continue
                seen_doc_ids.add(doc_id)
                records.append(rec)
            if skip + len(chunk) >= record_count:
                break
        return records, record_count

    def fetch_realtime_buckets(
        self,
        *,
        end: Optional[datetime] = None,
        window_hours: int = 24,
        top: int = 96,
    ) -> dict[str, Any]:
        """Hit /realtime with the bucketed-traffic parameter set.

        Discovered 2026-05-16: the same `/realtime` path is polymorphic.
        Omitting `$orderBy` and passing `date=-1` flips the response from
        per-article rows to **per-15-minute total-PV buckets** across the
        rolling 24h window. `$top=96` covers 24h × 4 buckets/hr. This is
        the timeline chart on Partner Hub's Overview tab.

        Returns the raw payload; each `recordList` entry is
        `{ "date": "2026-05-16T23:30Z", "readCount": 33 }`.
        """
        end_dt = end or _now_utc()
        start_dt = end_dt - timedelta(hours=window_hours)
        params = self._build_common_params(start=start_dt, end=end_dt)
        # Strip the article-grouping filters; pass date=-1 to flip the
        # endpoint into time-bucketing mode.
        params.pop("title", None)
        params.pop("device", None)
        params.pop("clickSource", None)
        params.pop("vertical", None)
        params["date"] = "-1"
        params["$skip"] = "0"
        params["$top"] = str(top)
        return self._call(f"{API_BASE_PATH}/realtime", params)

    def fetch_aggregate(
        self,
        *,
        end: Optional[datetime] = None,
        window_days: int = 30,
    ) -> dict[str, Any]:
        """Legacy shim. Historically tried candidate paths under
        insights/content/* which all 404'd — the endpoint sniffer proved
        MSN keeps monetization under insights/earning/* instead. Callers
        should migrate to fetch_earning_adsrev / fetch_partner_docstats /
        fetch_partner_rejected_docstats. This method returns an empty
        recordList so existing callers no-op instead of throwing."""
        return {"recordList": [], "recordCount": 0}

    # ------------------------------------------------------------------
    # Confirmed via endpoint sniffer 2026-07-13 — see
    # agents/etl/msn_endpoint_sniffer.py output at
    # ~/.pgam/msn-endpoint-sniff-<ts>.jsonl
    # ------------------------------------------------------------------

    def fetch_earning_adsrev(
        self,
        *,
        end: Optional[datetime] = None,
        window_months: int = 12,
    ) -> dict[str, Any]:
        """Monthly ads-revenue rows. Each row has:
            { "date": "2026-05",              # YYYY-MM bucket
              "interaction": 1500987.0,       # monthly interaction count
              "netRevenue": 807.77,           # net USD paid to us
              "adsAmount": 517.81,            # gross ads spend attributed
              "processedDate": "2026-06-16" } # when we got paid

        `interaction` is the closest thing MSN exposes to a monthly
        impression count — pair with our own read counts from realtime
        snapshots to model CTR at a monthly level. Default window is
        12 months, capped at MSN's own retention.
        """
        end_dt = end or _now_utc()
        # adsrev endDate/startDate are YYYY-MM strings, not ISO datetimes.
        end_ym = end_dt.strftime("%Y-%m")
        start_ym = (end_dt - timedelta(days=32 * window_months)).strftime("%Y-%m")
        params = {
            "apikey": APIKEY,
            "brandId": _ALL,
            "adsType": _ALL,
            "date": "-1",
            "endDate": end_ym,
            "startDate": start_ym,
            "fdhead": _UGC_FLIGHTS,
            "ocid": "msph",
            "partnerId": self.partner_id,
            "partnerType": DEFAULT_PARTNER_TYPE,
            "scn": "MSNRPSAuth",
            "skipaadal": "true",
            "timeout": "30000",
            "ugc-flights": _UGC_FLIGHTS,
            "wrapodata": "false",
            "$skip": "0",
            "$top": str(window_months + 1),
        }
        return self._call("/msn/v0/pages/ugc/insights/earning/adsrev", params)

    def fetch_partner_docstats(
        self,
        *,
        end: Optional[datetime] = None,
    ) -> dict[str, Any]:
        """Publish-rate rollup for the current partner:
            { "recordList": [{ "providerId": "BB1jwrk8",
                               "contentPublishRate": 98.19,
                               "contentSubmitted": 1543,
                               "contentPublished": 1515,
                               "contentRejected": 28 }], ... }

        This is the rollup MSN's Content Report card shows on the home
        page — a rolling ~4-week publish-rate KPI.

        Empirically the endpoint returns an empty recordList for anything
        other than a **29-day** window ending yesterday-UTC. Tested
        7/14/29/30/60 and only 29 returns data. Not documented anywhere;
        MSN's SPA always requests exactly 29 days from a fixed offset.
        No window_days knob exposed — call it what MSN's UI calls it and
        move on.
        """
        end_dt = end or (_now_utc() - timedelta(days=1))
        start_dt = end_dt - timedelta(days=29)
        params = {
            "apikey": APIKEY,
            "endDate": end_dt.strftime("%Y-%m-%d"),
            "startDate": start_dt.strftime("%Y-%m-%d"),
            "fdhead": _UGC_REPORT_FLIGHTS,
            "ocid": "msph",
            "partnerId": self.partner_id,
            "partnerType": DEFAULT_PARTNER_TYPE,
            "scn": "MSNRPSAuth",
            "skipaadal": "true",
            "timeout": "30000",
            "ugc-flights": _UGC_REPORT_FLIGHTS,
            "wrapodata": "false",
        }
        return self._call("/msn/v0/pages/ugc/contents/report/partnerdocstats", params)

    def fetch_partner_rejected_docstats(self) -> dict[str, Any]:
        """Rejection failures for the current partner:
            { "failures": [ ... per-doc entries ... ],
              "docCount": 29,
              "logEndTime": "2026-07-13T18:00Z" }

        This is the API replacement for the manual "Content Rejection
        Report" CSV export that had to be downloaded from Partner Hub
        home. Kills that workflow — we can now pull rejections at any
        cadence.
        """
        params = {
            "apikey": APIKEY,
            "fdhead": _UGC_REPORT_FLIGHTS,
            "ocid": "msph",
            "partnerId": self.partner_id,
            "partnerType": DEFAULT_PARTNER_TYPE,
            "scn": "MSNRPSAuth",
            "skipaadal": "true",
            "timeout": "30000",
            "ugc-flights": _UGC_REPORT_FLIGHTS,
            "wrapodata": "false",
        }
        return self._call(
            "/msn/v0/pages/ugc/contents/report/partnerrejecteddocstats", params
        )


__all__ = [
    "PartnerHubClient",
    "PartnerHubError",
    "API_HOST",
    "API_BASE_PATH",
    "DEFAULT_PARTNER_ID",
]
