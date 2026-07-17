#!/usr/bin/env python3
"""
scripts/msn_oauth_bootstrap_silent.py

Companion to msn_oauth_capture.py that tries a NON-INTERACTIVE
bootstrap first. Reuses the existing Chromium user-data-dir at
~/.pgam/msn-session/ — which retains MSAL's cached refresh_token
in localStorage across browser restarts. On load of Partner Hub,
MSAL will silently POST to /oauth2/v2.0/token to renew the access
token; we intercept that response and save the fresh refresh_token
to Neon. No sign-in prompt, no MFA.

Falls through with a clear "needs interactive" message if:
- No persisted session exists yet, OR
- MSAL doesn't fire a silent token exchange within the wait window
  (indicates cached token is still valid, or session is truly stale)

In the second case, run scripts/msn_oauth_capture.py (interactive)
and complete a real sign-in + MFA once.

Env
---
- PGAM_DIRECT_DATABASE_URL — where to write the token row
- MSN_SESSION_DIR (optional) — overrides ~/.pgam/msn-session
- MSN_HEADLESS (optional, default "1")
- WAIT_TIMEOUT_SEC (optional, default 60)

Exit codes
----------
0 — captured and saved
2 — no silent capture; run the interactive script instead
1 — hard error (DSN missing, DB write failed, etc.)
"""

from __future__ import annotations

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

# Reuse the interceptor + endpoint patterns from the interactive script.
from msn_oauth_capture import (  # type: ignore
    OAuthInterceptor,
    PARTNER_HUB_URL,
    TOKEN_TABLE_ID,
)


def _resolve_dsn() -> str:
    dsn = os.environ.get("PGAM_DIRECT_DATABASE_URL") or os.environ.get("DATABASE_URL")
    if not dsn:
        print("ERROR: PGAM_DIRECT_DATABASE_URL not set in env.", file=sys.stderr)
        sys.exit(1)
    return dsn


def _default_session_dir() -> Path:
    override = os.environ.get("MSN_SESSION_DIR")
    if override:
        return Path(override).expanduser()
    return Path.home() / ".pgam" / "msn-session"


def _save_token(captured: dict[str, Any]) -> None:
    now = datetime.now(timezone.utc)
    access_exp = now + timedelta(seconds=captured.get("expires_in", 3599))
    refresh_exp = now + timedelta(seconds=captured.get("refresh_token_expires_in", 86400))
    with psycopg.connect(_resolve_dsn(), connect_timeout=30) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO pgam_direct.msn_oauth_token
                  (id, client_id, tenant, scope, refresh_token, access_token,
                   access_expires_at, refresh_expires_at, redirect_uri,
                   updated_at, updated_by, refresh_count)
                VALUES
                  (%(id)s, %(client_id)s, %(tenant)s, %(scope)s, %(refresh_token)s,
                   %(access_token)s, %(access_exp)s, %(refresh_exp)s, %(redirect_uri)s,
                   now(), 'msn_oauth_bootstrap_silent', 0)
                ON CONFLICT (id) DO UPDATE SET
                  client_id          = EXCLUDED.client_id,
                  tenant             = EXCLUDED.tenant,
                  scope              = EXCLUDED.scope,
                  refresh_token      = EXCLUDED.refresh_token,
                  access_token       = EXCLUDED.access_token,
                  access_expires_at  = EXCLUDED.access_expires_at,
                  refresh_expires_at = EXCLUDED.refresh_expires_at,
                  redirect_uri       = EXCLUDED.redirect_uri,
                  updated_at         = now(),
                  updated_by         = 'msn_oauth_bootstrap_silent',
                  refresh_count      = pgam_direct.msn_oauth_token.refresh_count + 1
                """,
                {
                    "id":            TOKEN_TABLE_ID,
                    "client_id":     captured["client_id"],
                    "tenant":        captured["tenant"],
                    "scope":         captured["scope"],
                    "refresh_token": captured["refresh_token"],
                    "access_token":  captured["access_token"],
                    "access_exp":    access_exp,
                    "refresh_exp":   refresh_exp,
                    "redirect_uri":  captured["redirect_uri"],
                },
            )
        conn.commit()
    print(f"[silent-bootstrap] SAVED refresh_token to pgam_direct.msn_oauth_token id={TOKEN_TABLE_ID}")
    print(f"[silent-bootstrap]   refresh_token_expires_at = {refresh_exp.isoformat()}")


def main() -> int:
    session_dir = _default_session_dir()
    if not (session_dir / "Default" / "Cookies").exists():
        print(f"[silent-bootstrap] no persisted session at {session_dir}/Default/Cookies", file=sys.stderr)
        print(f"[silent-bootstrap] this bootstrap variant needs an already-logged-in Chromium profile.", file=sys.stderr)
        print(f"[silent-bootstrap] run scripts/msn_oauth_capture.py once (interactive) first.", file=sys.stderr)
        return 2

    wait_sec = int(os.environ.get("WAIT_TIMEOUT_SEC", "60"))
    headless_env = os.environ.get("MSN_HEADLESS", "1")
    headless = headless_env not in ("0", "false", "no")

    print(f"[silent-bootstrap] using session_dir={session_dir}, headless={headless}, wait={wait_sec}s")
    print(f"[silent-bootstrap] watching for MSAL silent token exchange...")

    interceptor = OAuthInterceptor()
    with sync_playwright() as pw:
        ctx = pw.chromium.launch_persistent_context(
            user_data_dir=str(session_dir),
            headless=headless,
            viewport={"width": 1400, "height": 900},
            user_agent=(
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/148.0.0.0 Safari/537.36"
            ),
        )
        ctx.on("request", interceptor.on_request)
        ctx.on("response", interceptor.on_response)
        page = ctx.new_page()
        try:
            page.goto(PARTNER_HUB_URL, wait_until="domcontentloaded")
        except Exception as exc:
            print(f"[silent-bootstrap] page.goto failed: {exc}", file=sys.stderr)
            ctx.close()
            return 1

        deadline = time.time() + wait_sec
        while time.time() < deadline:
            if interceptor.captured:
                # Grace period so any related requests can finish
                time.sleep(2)
                break
            time.sleep(2)
        ctx.close()

    if not interceptor.captured:
        print(f"[silent-bootstrap] no silent OAuth exchange fired in {wait_sec}s.", file=sys.stderr)
        print(f"[silent-bootstrap] possible reasons:", file=sys.stderr)
        print(f"  - MSAL's cached access_token is still valid, so no refresh fired.", file=sys.stderr)
        print(f"  - Session cookies are expired; MSAL wants a real sign-in.", file=sys.stderr)
        print(f"[silent-bootstrap] run scripts/msn_oauth_capture.py (interactive) instead.", file=sys.stderr)
        return 2

    try:
        _save_token(interceptor.captured)
    except Exception as exc:
        print(f"[silent-bootstrap] DB write failed: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
