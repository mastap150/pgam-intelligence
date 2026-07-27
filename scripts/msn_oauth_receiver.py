#!/usr/bin/env python3
"""
scripts/msn_oauth_receiver.py

One-shot local HTTP receiver for a refreshed MSAL token pair. Bound to
127.0.0.1:8877 only — never exposed to the network. Used as a workaround
for the interactive-capture flow when the Playwright Chromium instance
can't or won't fire the OAuth exchange:

  1. Start this script.
  2. In the operator's real Chrome (already signed into Partner Hub),
     paste a JS snippet that reads the refresh_token from MSAL v2
     localStorage, calls Microsoft's /oauth2/v2.0/token endpoint with
     grant_type=refresh_token, and POSTs the resulting fresh token pair
     to http://127.0.0.1:8877/tokens.
  3. This receiver upserts the fresh refresh_token into Neon's
     pgam_direct.msn_oauth_token and exits.

Nothing crosses the machine boundary. The Microsoft OAuth call runs in
the operator's browser context; the DB write runs from this script.

Auth model: none — the listener binds to loopback only. Any local
process could POST to it during its brief lifetime, but that's the same
trust boundary as running any local dev server.

Exit codes:
  0 — token persisted
  1 — bind failure / DB write failure
  timeout (300s default) — no POST received, exits with 2
"""

from __future__ import annotations

import json
import os
import sys
import time
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

try:
    from dotenv import load_dotenv
    load_dotenv(dotenv_path=str(Path(__file__).resolve().parent.parent / ".env"), override=False)
except Exception:
    pass

import psycopg

TOKEN_TABLE_ID = "msn-partner-hub-boxingnews-primary"
BIND_HOST = "127.0.0.1"
BIND_PORT = int(os.environ.get("MSN_OAUTH_RECEIVER_PORT", "8877"))
IDLE_TIMEOUT_SEC = int(os.environ.get("MSN_OAUTH_RECEIVER_TIMEOUT", "300"))


def _dsn() -> str:
    v = os.environ.get("PGAM_DIRECT_DATABASE_URL") or os.environ.get("DATABASE_URL")
    if not v:
        print("[receiver] ERROR: PGAM_DIRECT_DATABASE_URL missing", file=sys.stderr)
        sys.exit(1)
    return v.replace("-pooler.", ".")


def _persist(payload: dict) -> None:
    now = datetime.now(timezone.utc)
    access_expires_at = now + timedelta(seconds=int(payload.get("expires_in", 3599)))
    refresh_expires_at = now + timedelta(seconds=int(payload.get("refresh_token_expires_in", 86400)))
    with psycopg.connect(_dsn(), connect_timeout=30) as conn, conn.cursor() as cur:
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
                payload["client_id"],
                payload.get("tenant", "consumers"),
                payload.get("scope", ""),
                payload["refresh_token"],
                payload.get("access_token"),
                access_expires_at,
                refresh_expires_at,
                payload.get("redirect_uri", ""),
                f"receiver@{os.uname().nodename}",
            ),
        )
        conn.commit()
    print(f"[receiver] SAVED to Neon (id={TOKEN_TABLE_ID}, "
          f"client_id={payload['client_id'][:8]}…, "
          f"refresh_expires={refresh_expires_at.isoformat()})", flush=True)


class Handler(BaseHTTPRequestHandler):
    server_should_stop = False

    def log_message(self, fmt, *args):
        print(f"[receiver] {self.address_string()} — {fmt % args}", flush=True)

    def _cors(self):
        # Restrict to msn.com only — no wildcard, no external origins
        origin = self.headers.get("Origin", "")
        allowed = origin if origin in ("https://www.msn.com", "https://msn.com") else "https://www.msn.com"
        self.send_header("Access-Control-Allow-Origin", allowed)
        self.send_header("Access-Control-Allow-Methods", "POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        # Chrome Private Network Access: a public HTTPS page (msn.com) fetching
        # a private-IP resource (127.0.0.1) is blocked with "Failed to fetch"
        # unless the server opts in via this header on the preflight OPTIONS.
        self.send_header("Access-Control-Allow-Private-Network", "true")

    def do_OPTIONS(self):
        self.send_response(204)
        self._cors()
        self.end_headers()

    def do_POST(self):
        if self.path != "/tokens":
            self.send_response(404); self._cors(); self.end_headers(); return
        length = int(self.headers.get("Content-Length", "0"))
        raw = self.rfile.read(length) if length else b"{}"
        try:
            payload = json.loads(raw.decode("utf-8"))
        except Exception as exc:
            self.send_response(400); self._cors(); self.end_headers()
            self.wfile.write(f"bad json: {exc}".encode())
            return
        required = ("client_id", "refresh_token")
        missing = [k for k in required if not payload.get(k)]
        if missing:
            self.send_response(400); self._cors(); self.end_headers()
            self.wfile.write(f"missing fields: {missing}".encode())
            return
        try:
            _persist(payload)
        except Exception as exc:
            self.send_response(500); self._cors(); self.end_headers()
            self.wfile.write(f"db write failed: {exc}".encode())
            return
        self.send_response(200); self._cors(); self.end_headers()
        self.wfile.write(b'{"ok":true}')
        Handler.server_should_stop = True


def main() -> int:
    try:
        srv = HTTPServer((BIND_HOST, BIND_PORT), Handler)
    except OSError as exc:
        print(f"[receiver] bind {BIND_HOST}:{BIND_PORT} failed: {exc}", file=sys.stderr)
        return 1
    srv.timeout = 5
    print(f"[receiver] listening on http://{BIND_HOST}:{BIND_PORT}/tokens "
          f"(idle timeout {IDLE_TIMEOUT_SEC}s)", flush=True)
    deadline = time.time() + IDLE_TIMEOUT_SEC
    while not Handler.server_should_stop:
        srv.handle_request()
        if time.time() > deadline:
            print("[receiver] idle timeout, no token received", flush=True)
            srv.server_close()
            return 2
    srv.server_close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
