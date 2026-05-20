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

WAIT_TIMEOUT_SEC = 7 * 60  # 7 min — plenty of time for MFA tap
CAPTURE_DELAY_SEC = 2      # how long to sit after capture before exiting


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

    with sync_playwright() as pw:
        # Fresh context — we do NOT want to use the existing
        # ~/.pgam/msn-session because it's expired and would skip
        # the OAuth exchange we need to intercept.
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

        page = context.new_page()
        page.goto(PARTNER_HUB_URL, wait_until='domcontentloaded')

        # Optional: auto-fill email/password if env vars set. MFA still
        # needs human.
        email = os.environ.get('MSN_EMAIL', '').strip()
        password = os.environ.get('MSN_PASSWORD', '').strip()
        if email and password:
            print(f"[capture] auto-fill enabled for {email} — MFA still needs your tap")

        deadline = time.time() + WAIT_TIMEOUT_SEC
        while time.time() < deadline:
            if interceptor.captured:
                # Give 2s of grace to make sure all related events fire
                time.sleep(CAPTURE_DELAY_SEC)
                break
            time.sleep(2)
        else:
            print("[capture] TIMEOUT — no OAuth token exchange seen in window.", file=sys.stderr)
            context.close()
            browser.close()
            return 1

        context.close()
        browser.close()

    captured = interceptor.captured
    if not captured:
        print("[capture] no token captured. Try again.", file=sys.stderr)
        return 1

    upsert_oauth_token(captured)
    print()
    print("[capture] SUCCESS. Next: scripts/msn_refresh_puller.py can run on")
    print("[capture] any machine (GH Actions, Render, anywhere) using the stored")
    print("[capture] refresh_token. Chain stays alive as long as we refresh < 24h.")
    return 0


if __name__ == '__main__':
    sys.exit(main())
