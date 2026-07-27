#!/usr/bin/env python3
"""
scripts/msn_oauth_route_capture.py

Third-generation MSN OAuth bootstrap. Uses Playwright's `context.route()`
to intercept the token-endpoint POST at the network layer — this catches
the response body regardless of MSAL's execution context (main frame,
hidden iframe, service worker, cross-origin popup). Previous versions
listened on `context.on("response", …)` which observably missed the
POST in MSAL v5.6.2's redirect flow on 2026-07-27.

Uses the persistent profile at `~/.pgam/msn-chromium-profile` — which
already has fresh MSN cookies from tonight's sign-in — so no MFA
required; MSAL silent-refreshes on the first api.msn.com call.

Env
---
- PGAM_DIRECT_DATABASE_URL — target for the token row
- MSN_CAPTURE_PROFILE_DIR (default ~/.pgam/msn-chromium-profile)
- MSN_CAPTURE_TIMEOUT_SEC (default 300)  — cap on the wait loop
- MSN_CAPTURE_HEADLESS  (default "0" — visible)
- MSN_CAPTURE_SCREENSHOT_DIR (optional) — dump last.png every 15s

Exit codes:
  0 — captured and saved
  1 — no capture inside the timeout
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
from playwright.sync_api import sync_playwright, Route, Request

PARTNER_HUB_URL = "https://www.msn.com/en-us/partnerhub/analytics/realtime/headline"
TOKEN_TABLE_ID = "msn-partner-hub-boxingnews-primary"

TOKEN_URL_PATTERNS = (
    "**/oauth2/v2.0/token**",
    "**/oauth20_token.srf**",
    "**/oauth2/token**",
)

DEFAULT_PROFILE = str(Path.home() / ".pgam" / "msn-chromium-profile")
WAIT_TIMEOUT_SEC = int(os.environ.get("MSN_CAPTURE_TIMEOUT_SEC", "300"))
HEADLESS = os.environ.get("MSN_CAPTURE_HEADLESS", "0") == "1"


def _dsn() -> str:
    v = os.environ.get("PGAM_DIRECT_DATABASE_URL") or os.environ.get("DATABASE_URL")
    if not v:
        raise SystemExit("PGAM_DIRECT_DATABASE_URL not set")
    return v.replace("-pooler.", ".")


def _persist(payload: dict) -> None:
    now = datetime.now(timezone.utc)
    access_exp = now + timedelta(seconds=int(payload.get("expires_in", 3599)))
    refresh_exp = now + timedelta(seconds=int(payload.get("refresh_token_expires_in", 86400)))
    with psycopg.connect(_dsn(), connect_timeout=30) as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO pgam_direct.msn_oauth_token
              (id, client_id, tenant, scope, refresh_token, access_token,
               access_expires_at, refresh_expires_at, redirect_uri,
               updated_by, refresh_count)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,0)
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
                payload["client_id"],
                payload.get("tenant", "consumers"),
                payload.get("scope", ""),
                payload["refresh_token"],
                payload.get("access_token"),
                access_exp,
                refresh_exp,
                payload.get("redirect_uri", ""),
                f"route-capture@{os.uname().nodename}",
            ),
        )
        conn.commit()
    print(f"[route] SAVED to Neon (id={TOKEN_TABLE_ID}, "
          f"client_id={payload['client_id'][:8]}…, "
          f"refresh_expires={refresh_exp.isoformat()})", flush=True)


class Captured:
    payload: Optional[dict] = None


def _make_handler(cap: Captured):
    """Build the route handler that inspects the token endpoint response
    body and stashes a captured payload for the main loop to persist."""

    def handler(route: Route, request: Request) -> None:
        try:
            # Fetch the real response, then hand it back to the browser
            # unchanged. `route.fetch()` performs the HTTP call from the
            # Playwright driver, giving us .body() on the return.
            resp = route.fetch()
            body_bytes = resp.body()
            # Always fulfill so the SPA never hangs — we're a passive tap.
            route.fulfill(response=resp, body=body_bytes)

            if request.method != "POST":
                return

            try:
                body = json.loads(body_bytes.decode("utf-8"))
            except Exception:
                # Some endpoints return application/x-www-form-urlencoded;
                # fall back to a naive parse.
                try:
                    from urllib.parse import parse_qs
                    text = body_bytes.decode("utf-8", errors="ignore")
                    parsed = parse_qs(text)
                    body = {k: v[0] for k, v in parsed.items()}
                except Exception:
                    return

            if "refresh_token" not in body or "access_token" not in body:
                return

            # Pull request-side fields (client_id / redirect_uri / scope)
            # off the POST body — MSAL sends form-urlencoded.
            post_body = request.post_data or ""
            req_params: dict[str, str] = {}
            for pair in post_body.split("&"):
                if "=" in pair:
                    k, _, v = pair.partition("=")
                    from urllib.parse import unquote_plus
                    req_params[k] = unquote_plus(v)

            # Derive tenant from URL (login.microsoftonline.com/<tenant>/…)
            tenant = "consumers"
            try:
                from urllib.parse import urlparse
                parts = urlparse(request.url).path.strip("/").split("/")
                if parts and len(parts[0]) > 4:
                    tenant = parts[0]
            except Exception:
                pass

            cap.payload = {
                "client_id":     req_params.get("client_id", ""),
                "tenant":        tenant,
                "scope":         body.get("scope") or req_params.get("scope", ""),
                "refresh_token": body["refresh_token"],
                "access_token":  body["access_token"],
                "expires_in":    int(body.get("expires_in", 3599)),
                "refresh_token_expires_in": int(body.get("refresh_token_expires_in", 86400)),
                "redirect_uri":  req_params.get("redirect_uri", ""),
            }
            grant = req_params.get("grant_type", "?")
            print(f"[route] CAPTURED grant={grant} client_id={cap.payload['client_id'][:8]}… "
                  f"scope={cap.payload['scope'][:50]}…", flush=True)
        except Exception as exc:
            print(f"[route] handler error: {exc}", flush=True)
            # Best-effort: continue the request so we don't hang the browser.
            try:
                route.continue_()
            except Exception:
                pass

    return handler


def _log_page_events(page) -> None:
    page.on("pageerror", lambda err: print(f"[route] pageerror: {err}", flush=True))
    page.on("requestfailed", lambda req: print(
        f"[route] requestfailed: {req.method} {req.url[:110]} — {req.failure}", flush=True))


def main() -> int:
    profile_dir = os.environ.get("MSN_CAPTURE_PROFILE_DIR", "").strip() or DEFAULT_PROFILE
    os.makedirs(profile_dir, exist_ok=True)
    shot_dir = os.environ.get("MSN_CAPTURE_SCREENSHOT_DIR", "").strip()
    if shot_dir:
        os.makedirs(shot_dir, exist_ok=True)

    print(f"[route] profile={profile_dir} headless={HEADLESS} wait={WAIT_TIMEOUT_SEC}s", flush=True)

    cap = Captured()
    with sync_playwright() as pw:
        ctx = pw.chromium.launch_persistent_context(
            profile_dir,
            headless=HEADLESS,
            args=[],
            viewport={"width": 1400, "height": 900},
        )
        # Route both the microsoftonline and live endpoints — context.route
        # covers ALL pages in this context, including popups + iframes.
        for pattern in TOKEN_URL_PATTERNS:
            ctx.route(pattern, _make_handler(cap))

        page = ctx.new_page() if not ctx.pages else ctx.pages[0]
        _log_page_events(page)
        print(f"[route] navigating to {PARTNER_HUB_URL}", flush=True)
        try:
            page.goto(PARTNER_HUB_URL, wait_until="commit", timeout=45000)
        except Exception as exc:
            print(f"[route] page.goto non-fatal error: {exc}", flush=True)

        # Force MSAL to mint a fresh token pair via the refresh_token grant.
        # The persistent profile has a valid access_token cached, which
        # means MSAL won't POST to the token endpoint until it expires
        # (~1h). We can't wait. Delete accesstoken entries from MSAL's
        # localStorage while keeping the refreshtoken entry — the next
        # api.msn.com call will trigger `grant_type=refresh_token` which
        # returns a fresh refresh_token in the response body (our tap).
        try:
            page.wait_for_load_state("domcontentloaded", timeout=30000)
        except Exception:
            pass
        time.sleep(2)
        try:
            deleted = page.evaluate(
                """
                () => {
                  const rm = [];
                  for (const k of Object.keys(localStorage)) {
                    // Delete both v2 and v1 MSAL access-token entries. Keep
                    // refreshtoken entries so MSAL can mint a fresh one.
                    if (/accesstoken/i.test(k)) { rm.push(k); localStorage.removeItem(k); }
                  }
                  return rm.length;
                }
                """
            )
            print(f"[route] cleared {deleted} MSAL accesstoken entries; reloading", flush=True)
            page.reload(wait_until="commit", timeout=45000)
        except Exception as exc:
            print(f"[route] force-refresh step failed (non-fatal): {exc}", flush=True)

        # Bring the window forward on macOS so the operator sees state
        try:
            import subprocess
            subprocess.run(
                ["osascript", "-e", 'tell application "Chromium" to activate'],
                check=False, capture_output=True, timeout=3,
            )
        except Exception:
            pass

        deadline = time.time() + WAIT_TIMEOUT_SEC
        last_heartbeat = 0.0
        while time.time() < deadline:
            if cap.payload:
                break
            time.sleep(2)
            if time.time() - last_heartbeat > 15:
                remaining = int(deadline - time.time())
                try:
                    cur_url = page.url
                except Exception:
                    cur_url = "<none>"
                print(f"[route] waiting… {remaining}s left  url={cur_url[:110]}", flush=True)
                last_heartbeat = time.time()
                if shot_dir:
                    try:
                        page.screenshot(path=os.path.join(shot_dir, "last.png"), full_page=False)
                    except Exception as _e:
                        pass

        if not cap.payload:
            print("[route] TIMEOUT — no OAuth POST intercepted", file=sys.stderr)
            try:
                ctx.close()
            except Exception:
                pass
            return 1

        # Persist BEFORE close so a hang can't lose the token.
        print("[route] persisting to Neon…", flush=True)
        _persist(cap.payload)
        try:
            ctx.close()
        except Exception as exc:
            print(f"[route] non-fatal close error: {exc}", flush=True)

    print("[route] done. chain restored.", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
