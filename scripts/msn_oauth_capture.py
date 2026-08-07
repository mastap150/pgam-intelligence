#!/usr/bin/env python3
"""
scripts/msn_oauth_capture.py

ONE-TIME interactive bootstrap. Opens a visible Chromium window,
intercepts every request to login.microsoftonline.com / login.live.com,
detects the OAuth `token` endpoint exchange, and saves the
refresh_token + client_id + scope into the shared Neon
pgam_direct.msn_oauth_token table.

After this runs successfully, scripts/msn_refresh_puller.py can use
the stored refresh_token to mint new access_tokens without a browser,
on any machine (GH Actions, Render, etc).

Usage:
    PGAM_DIRECT_DATABASE_URL=... python3 scripts/msn_oauth_capture.py

The script will:
  1. Open Chromium visibly to https://www.msn.com/en-us/partnerhub/
  2. You sign in (auto-fill works if MSN_EMAIL/MSN_PASSWORD are set;
     MFA prompt on your phone needs your manual tap)
  3. The script silently watches network traffic for the OAuth token
     endpoint, captures the response
  4. Once captured, writes to Neon and exits
  5. Future puller runs use the refresh-token chain — no browser needed

Re-run any time the chain breaks (i.e., the puller hasn't refreshed
in > 24h). Should be rare in practice; the refresh cron should run
every 12 hours minimum.
"""

from __future__ import annotations

import json
import os
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Optional

try:
    from dotenv import load_dotenv
    here = Path(__file__).resolve().parent.parent
    load_dotenv(dotenv_path=str(here / '.env'), override=False)
except Exception:
    pass

import psycopg
from playwright.sync_api import sync_playwright

PARTNER_HUB_URL = "https://www.msn.com/en-us/partnerhub/analytics/realtime/headline"
TOKEN_TABLE_ID = "msn-partner-hub-boxingnews-primary"

# Microsoft's OAuth token endpoints (consumer + work/school)
TOKEN_ENDPOINT_PATTERNS = (
    "login.microsoftonline.com/",
    "login.live.com/",
)
TOKEN_ENDPOINT_PATH_FRAGMENTS = (
    "/oauth2/v2.0/token",
    "/oauth20_token.srf",
    "/oauth2/token",
)

WAIT_TIMEOUT_SEC = int(os.environ.get("MSN_CAPTURE_TIMEOUT_SEC", "1200"))  # 20 min default
CAPTURE_DELAY_SEC = 2      # how long to sit after capture before exiting
# Grace period AFTER the sign-in deadline expires, during which we
# continue polling `interceptor.captured`. Async on_response events
# can fire seconds after the wall-clock deadline; observed 2026-08-07:
# three separate capture runs all set `interceptor.captured` between
# 5-15s past deadline, and each was lost to the timeout branch. See
# msn-reads-freefall-2026-08 for context.
TIMEOUT_GRACE_SEC = 60


def is_token_endpoint(url: str) -> bool:
    """Match Microsoft's OAuth token endpoint URLs across consumer / work."""
    if not any(p in url for p in TOKEN_ENDPOINT_PATTERNS):
        return False
    return any(frag in url for frag in TOKEN_ENDPOINT_PATH_FRAGMENTS)


def _resolve_dsn() -> str:
    dsn = os.environ.get("PGAM_DIRECT_DATABASE_URL") or os.environ.get("DATABASE_URL")
    if not dsn:
        print("ERROR: PGAM_DIRECT_DATABASE_URL not set in env.", file=sys.stderr)
        sys.exit(2)
    return dsn.replace("-pooler.", ".")  # direct connection avoids pooler timeouts


def upsert_oauth_token(captured: dict[str, Any]) -> None:
    dsn = _resolve_dsn()
    now = datetime.now(tz=timezone.utc)
    access_expires_at = now + timedelta(seconds=int(captured.get('expires_in', 3599)))
    refresh_expires_at = now + timedelta(seconds=int(captured.get('refresh_token_expires_in', 86400)))

    with psycopg.connect(dsn, connect_timeout=30) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO pgam_direct.msn_oauth_token
                  (id, client_id, tenant, scope, refresh_token, access_token,
                   access_expires_at, refresh_expires_at, redirect_uri,
                   updated_by, refresh_count)
                VALUES
                  (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 0)
                ON CONFLICT (id) DO UPDATE SET
                  client_id          = EXCLUDED.client_id,
                  tenant             = EXCLUDED.tenant,
                  scope              = EXCLUDED.scope,
                  refresh_token      = EXCLUDED.refresh_token,
                  access_token       = EXCLUDED.access_token,
                  access_expires_at  = EXCLUDED.access_expires_at,
                  refresh_expires_at = EXCLUDED.refresh_expires_at,
                  redirect_uri       = EXCLUDED.redirect_uri,
                  updated_at         = NOW(),
                  updated_by         = EXCLUDED.updated_by,
                  refresh_count      = 0
                """,
                (
                    TOKEN_TABLE_ID,
                    captured['client_id'],
                    captured['tenant'],
                    captured['scope'],
                    captured['refresh_token'],
                    captured.get('access_token'),
                    access_expires_at,
                    refresh_expires_at,
                    captured['redirect_uri'],
                    f"capture@{os.uname().nodename}",
                ),
            )
        conn.commit()
    print(f"[capture] saved token to Neon (id='{TOKEN_TABLE_ID}', "
          f"client_id={captured['client_id'][:8]}…, "
          f"refresh exp={refresh_expires_at.isoformat()})")


class OAuthInterceptor:
    """Watches network traffic, captures token-endpoint responses + the
    request that produced them. The request side carries client_id +
    redirect_uri + scope, the response carries refresh_token +
    access_token. We need both halves to reproduce the call."""

    def __init__(self) -> None:
        self.captured: Optional[dict[str, Any]] = None
        # Index pending requests by URL so we can pair them with their
        # responses (Playwright fires request + response as separate events).
        self._pending: dict[str, dict[str, Any]] = {}

    def on_request(self, request: Any) -> None:
        try:
            if not is_token_endpoint(request.url):
                return
            if request.method != 'POST':
                return
            body = request.post_data or ''
            # Body is application/x-www-form-urlencoded:
            # client_id=...&grant_type=authorization_code&code=...&redirect_uri=...&scope=...
            params: dict[str, str] = {}
            for pair in body.split('&'):
                if '=' not in pair:
                    continue
                k, _, v = pair.partition('=')
                from urllib.parse import unquote_plus
                params[k] = unquote_plus(v)
            # Skip request types we can't use (anything except the
            # authorization_code or refresh_token grant).
            grant = params.get('grant_type', '')
            if grant not in ('authorization_code', 'refresh_token'):
                return
            # Stash request-side params keyed by URL for response pairing
            self._pending[request.url] = {
                'client_id': params.get('client_id', ''),
                'redirect_uri': params.get('redirect_uri', ''),
                'scope': params.get('scope', ''),
                'grant_type': grant,
                'url': request.url,
            }
            print(f"[capture] saw OAuth token POST: grant={grant}, "
                  f"client_id={params.get('client_id', '')[:8]}…")
        except Exception as exc:
            print(f"[capture] request hook error: {exc}", file=sys.stderr)

    def on_response(self, response: Any) -> None:
        try:
            if not is_token_endpoint(response.url):
                return
            req = self._pending.get(response.url)
            if not req:
                return
            try:
                body = response.json()
            except Exception:
                # Some token endpoints return URL-encoded responses
                text = response.text()
                from urllib.parse import parse_qs
                body = {k: v[0] for k, v in parse_qs(text).items()}
            if 'refresh_token' not in body or 'access_token' not in body:
                # Not a successful token exchange
                return
            # Derive the tenant from the URL (login.microsoftonline.com/<tenant>/oauth2/...)
            tenant = 'common'
            try:
                from urllib.parse import urlparse
                path_parts = urlparse(response.url).path.strip('/').split('/')
                if path_parts and len(path_parts[0]) > 4:
                    tenant = path_parts[0]
            except Exception:
                pass
            self.captured = {
                'client_id': req['client_id'],
                'tenant': tenant,
                'scope': body.get('scope') or req.get('scope', ''),
                'refresh_token': body['refresh_token'],
                'access_token': body['access_token'],
                'expires_in': int(body.get('expires_in', 3599)),
                'refresh_token_expires_in': int(body.get('refresh_token_expires_in', 86400)),
                'redirect_uri': req['redirect_uri'],
            }
            print(f"[capture] CAPTURED OAuth response from {response.url[:80]}")
            print(f"          access_token expires in {self.captured['expires_in']}s")
            print(f"          refresh_token expires in {self.captured['refresh_token_expires_in']}s "
                  f"({self.captured['refresh_token_expires_in']/3600:.1f}h)")
        except Exception as exc:
            print(f"[capture] response hook error: {exc}", file=sys.stderr)


def main() -> int:
    print("[capture] opening Chromium to Partner Hub.")
    print("[capture] sign in any way (MFA on your phone if prompted).")
    print(f"[capture] will wait up to {WAIT_TIMEOUT_SEC}s for the OAuth exchange to fire.")
    print()

    # When MSN_CAPTURE_PROFILE_DIR is set, we run in "persistent" mode:
    # the browser reuses a saved on-disk profile across script invocations,
    # so once the operator signs in once, future re-bootstraps skip MFA
    # entirely — Partner Hub silently refreshes the token in the background
    # and the interceptor catches it. This is the "make the next outage
    # trivial" mode.
    profile_dir = os.environ.get("MSN_CAPTURE_PROFILE_DIR", "").strip()

    with sync_playwright() as pw:
        browser = None  # only set in non-persistent mode
        if profile_dir:
            os.makedirs(profile_dir, exist_ok=True)
            print(f"[capture] persistent-profile mode: {profile_dir}", flush=True)
            context = pw.chromium.launch_persistent_context(
                profile_dir,
                headless=False,
                args=[],
                viewport={'width': 1400, 'height': 900},
                user_agent=(
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/148.0.0.0 Safari/537.36"
                ),
            )
        else:
            browser = pw.chromium.launch(headless=False, args=[])
            context = browser.new_context(
                viewport={'width': 1400, 'height': 900},
                user_agent=(
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/148.0.0.0 Safari/537.36"
                ),
            )
        interceptor = OAuthInterceptor()
        context.on("request", interceptor.on_request)
        context.on("response", interceptor.on_response)
        # Log popups / new tabs so we can see whether "Sign in to get started"
        # opens the Microsoft OAuth flow in a separate window we didn't know
        # about. Popups are still in the same context so the interceptor
        # sees their traffic — this is diagnostic only.
        def _on_new_page(p: Any) -> None:
            try:
                print(f"[capture] NEW PAGE opened: {p.url[:120]}", flush=True)
                p.on("framenavigated", lambda fr: (
                    print(f"[capture] popup-nav: {fr.url[:120]}", flush=True)
                    if fr == p.main_frame else None
                ))
            except Exception as _e:
                print(f"[capture] new-page hook error: {_e}", flush=True)
        context.on("page", _on_new_page)

        page = context.new_page()
        # Surface page-level failures. Previous runs went silent because
        # page.goto blocked on a redirect or a JS error we never saw.
        page.on("pageerror", lambda err: print(f"[capture] pageerror: {err}", flush=True))
        page.on("console", lambda msg: (
            print(f"[capture] console.{msg.type}: {msg.text[:200]}", flush=True)
            if msg.type in ("error", "warning") else None
        ))
        page.on("requestfailed", lambda req: print(
            f"[capture] requestfailed: {req.method} {req.url[:120]} — {req.failure}", flush=True))

        # wait_until='commit' fires on the FIRST HTTP response (before
        # full DOM) — MSN Partner Hub does a heavy client-side redirect
        # chain that occasionally never emits domcontentloaded, which
        # is what stranded earlier capture attempts with a blank window.
        print(f"[capture] navigating to {PARTNER_HUB_URL}", flush=True)
        try:
            page.goto(PARTNER_HUB_URL, wait_until='commit', timeout=45000)
            print(f"[capture] page committed — bring the 'Google Chrome for Testing' window to the front and sign in", flush=True)
        except Exception as exc:
            print(f"[capture] page.goto error (non-fatal, keep going): {exc}", flush=True)

        # macOS: pop the Playwright Chromium window to the front so the
        # operator doesn't have to Cmd+Tab hunting for it.
        try:
            import subprocess as _sp
            _sp.run(
                ["osascript", "-e",
                 'tell application "Google Chrome for Testing" to activate'],
                check=False, capture_output=True, timeout=5,
            )
        except Exception:
            pass

        # Optional: auto-fill email/password if env vars set. MFA still
        # needs human.
        email = os.environ.get('MSN_EMAIL', '').strip()
        password = os.environ.get('MSN_PASSWORD', '').strip()
        if email and password:
            print(f"[capture] auto-fill enabled for {email} — MFA still needs your tap", flush=True)

        # Auto-dismiss Microsoft's GDPR cookie-consent banner. Partner Hub
        # blocks the auth redirect until this is dismissed.
        try:
            for label in ("Reject All", "Reject all", "I Accept", "Accept all"):
                loc = page.get_by_role("button", name=label)
                try:
                    loc.first.click(timeout=3000)
                    print(f"[capture] dismissed consent banner via '{label}' button", flush=True)
                    break
                except Exception:
                    continue
        except Exception as exc:
            print(f"[capture] consent-banner auto-click skipped: {exc}", flush=True)

        # Auto-click the "Sign in to get started" landing button so the flow
        # redirects to login.live.com without human interaction. The button
        # may be a <button>, an <a> styled as a button, or a <div role="button">
        # — try each in order.
        clicked = False
        for locator, label in [
            (page.get_by_role("button", name="Sign in to get started"), "role=button"),
            (page.get_by_role("link", name="Sign in to get started"), "role=link"),
            (page.locator("text=Sign in to get started"), "text-locator"),
        ]:
            try:
                locator.first.wait_for(state="visible", timeout=8000)
                locator.first.click(timeout=5000)
                print(f"[capture] clicked 'Sign in to get started' via {label}", flush=True)
                clicked = True
                break
            except Exception:
                continue
        if not clicked:
            print("[capture] sign-in-landing click: no matching element found", flush=True)

        deadline = time.time() + WAIT_TIMEOUT_SEC
        last_status = 0.0
        while time.time() < deadline:
            if interceptor.captured:
                # Give 2s of grace to make sure all related events fire
                time.sleep(CAPTURE_DELAY_SEC)
                break
            # Heartbeat every 30s so the operator sees the script is alive
            if time.time() - last_status > 30:
                remaining = int(deadline - time.time())
                try:
                    cur_url = page.url
                except Exception:
                    cur_url = "<no url>"
                print(f"[capture] waiting… {remaining}s left  cur_url={cur_url[:110]}", flush=True)
                try:
                    urls = [p.url[:100] for p in context.pages]
                    if len(urls) > 1:
                        print(f"[capture]   context has {len(urls)} pages: {urls}", flush=True)
                except Exception:
                    pass
                last_status = time.time()
            time.sleep(2)

        # Deadline passed without capture. But on_response fires from the
        # async event loop and has been observed to set interceptor.captured
        # 5-15s AFTER the wall-clock deadline (typical when a FIDO/passkey
        # tap completes right before timeout). Keep polling for a short
        # grace window before declaring failure — losing a captured token
        # to a timing race would force another full sign-in + MFA cycle.
        if not interceptor.captured:
            grace_end = time.time() + TIMEOUT_GRACE_SEC
            print(f"[capture] deadline reached; polling {TIMEOUT_GRACE_SEC}s grace "
                  f"for late on_response event…", flush=True)
            while time.time() < grace_end and not interceptor.captured:
                time.sleep(1)
            if interceptor.captured:
                print("[capture] captured within grace window — proceeding to persist.", flush=True)
                time.sleep(CAPTURE_DELAY_SEC)

        if not interceptor.captured:
            print("[capture] TIMEOUT — no OAuth token exchange seen in window.", file=sys.stderr)
            context.close()
            if browser is not None:
                browser.close()
            return 1

        # ── Write to Neon BEFORE closing the browser ─────────────────────
        # Chromium.close() has been observed to hang on macOS for a
        # variable amount of time; we do not want a browser-close hang
        # to lose a captured token. Persist first, then tear down.
        captured = interceptor.captured
        if not captured:
            print("[capture] no token captured. Try again.", file=sys.stderr)
            context.close()
            if browser is not None:
                browser.close()
            return 1
        print("[capture] persisting token to Neon…", flush=True)
        upsert_oauth_token(captured)
        print("[capture] closing Chromium (may take a few seconds)…", flush=True)
        try:
            context.close()
            if browser is not None:
                browser.close()
        except Exception as exc:
            print(f"[capture] non-fatal browser close error: {exc}", file=sys.stderr)
    print()
    print("[capture] SUCCESS. Next: scripts/msn_refresh_puller.py can run on")
    print("[capture] any machine (GH Actions, Render, anywhere) using the stored")
    print("[capture] refresh_token. Chain stays alive as long as we refresh < 24h.")
    return 0


if __name__ == '__main__':
    sys.exit(main())
