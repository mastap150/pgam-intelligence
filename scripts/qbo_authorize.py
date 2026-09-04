#!/usr/bin/env python3
"""
scripts/qbo_authorize.py

One-time QuickBooks OAuth: get a refresh token without the OAuth Playground.

Intuit's Playground works but is fiddly — you pick scopes, copy a code, paste
it into a second form, and it silently hands you sandbox tokens if the wrong
environment is selected. This does the whole dance locally instead: it starts
a throwaway web server on the redirect URI, prints a link, and captures the
code Intuit sends back.

Setup (once, in the Intuit app you created)
-------------------------------------------
On developer.intuit.com → your app → Keys & credentials → Production, add this
exact Redirect URI:

    http://localhost:8000/callback

Then put the production client id and secret in your local .env:

    QBO_CLIENT_ID=...
    QBO_CLIENT_SECRET=...

Usage
-----
    python3 scripts/qbo_authorize.py

Open the printed link, approve for PGAM Media LLC, and the script prints the
two values you still need — QBO_REFRESH_TOKEN and QBO_REALM_ID. Add them to
.env (or Render) and you are done.

The refresh token rotates on every use afterwards; core.qbo_api persists the
rotation to Neon, so this script only ever needs to run once per app.
"""

from __future__ import annotations

import base64
import http.server
import json
import os
import secrets
import socketserver
import sys
import threading
import urllib.error
import urllib.parse
import urllib.request
import webbrowser
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

try:
    from dotenv import load_dotenv
    load_dotenv(override=True)
except ImportError:
    pass

AUTH_URL = "https://appcenter.intuit.com/connect/oauth2"
TOKEN_URL = "https://oauth.platform.intuit.com/oauth2/v1/tokens/bearer"
SCOPE = "com.intuit.quickbooks.accounting"

PORT = 8000
REDIRECT_URI = f"http://localhost:{PORT}/callback"

# Filled in by the callback handler, read by the main thread.
RESULT: dict = {}
DONE = threading.Event()


class _Handler(http.server.BaseHTTPRequestHandler):
    def do_GET(self) -> None:  # noqa: N802 - stdlib naming
        parsed = urllib.parse.urlparse(self.path)
        if parsed.path != "/callback":
            self.send_response(404)
            self.end_headers()
            return

        params = urllib.parse.parse_qs(parsed.query)
        RESULT.update({k: v[0] for k, v in params.items()})

        ok = "code" in RESULT and "realmId" in RESULT
        body = (
            "<h2>Connected.</h2><p>Return to your terminal — the refresh token "
            "is printed there. You can close this tab.</p>"
            if ok else
            f"<h2>Authorization failed.</h2><pre>{RESULT}</pre>"
        )
        self.send_response(200 if ok else 400)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.end_headers()
        self.wfile.write(body.encode())
        DONE.set()

    def log_message(self, *args) -> None:
        """Silence the default per-request logging to stderr."""


def _exchange(code: str, client_id: str, client_secret: str) -> dict:
    basic = base64.b64encode(f"{client_id}:{client_secret}".encode()).decode()
    body = urllib.parse.urlencode({
        "grant_type":   "authorization_code",
        "code":         code,
        "redirect_uri": REDIRECT_URI,
    }).encode()

    req = urllib.request.Request(
        TOKEN_URL,
        data=body,
        headers={
            "Authorization": f"Basic {basic}",
            "Content-Type":  "application/x-www-form-urlencoded",
            "Accept":        "application/json",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return json.loads(resp.read().decode())
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode()[:500]
        raise SystemExit(
            f"\nToken exchange failed ({exc.code}): {detail}\n\n"
            "Most common cause: the Redirect URI on the Intuit app does not "
            f"exactly match {REDIRECT_URI}"
        ) from exc


def main() -> int:
    client_id = os.environ.get("QBO_CLIENT_ID", "")
    client_secret = os.environ.get("QBO_CLIENT_SECRET", "")
    if not client_id or not client_secret:
        print("Set QBO_CLIENT_ID and QBO_CLIENT_SECRET (in .env or the shell) first.")
        return 1

    state = secrets.token_urlsafe(16)
    authorize_url = f"{AUTH_URL}?" + urllib.parse.urlencode({
        "client_id":     client_id,
        "response_type": "code",
        "scope":         SCOPE,
        "redirect_uri":  REDIRECT_URI,
        "state":         state,
    })

    socketserver.TCPServer.allow_reuse_address = True
    server = socketserver.TCPServer(("127.0.0.1", PORT), _Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()

    print("\nOpen this and approve for PGAM Media LLC:\n")
    print(f"  {authorize_url}\n")
    try:
        webbrowser.open(authorize_url)
    except Exception:
        pass
    print("Waiting for the callback… (Ctrl-C to abort)")

    try:
        if not DONE.wait(timeout=300):
            print("\nTimed out after 5 minutes.")
            return 1
    except KeyboardInterrupt:
        return 1
    finally:
        server.shutdown()

    if RESULT.get("state") != state:
        print("\nState mismatch — discarding this response rather than trusting it.")
        return 1
    if "code" not in RESULT:
        print(f"\nNo authorization code came back: {RESULT}")
        return 1

    tokens = _exchange(RESULT["code"], client_id, client_secret)
    realm = RESULT.get("realmId", "")

    print("\n" + "=" * 62)
    print("Add these to .env (local) or Render → Environment:\n")
    print(f"QBO_REFRESH_TOKEN={tokens['refresh_token']}")
    print(f"QBO_REALM_ID={realm}")
    print("=" * 62)
    if realm and realm != "193514590350384":
        print(
            f"\n⚠  Realm {realm} is not PGAM Media LLC (193514590350384).\n"
            "   You may have approved the wrong company, or a sandbox one."
        )
    print(
        "\nThen:  python3 scripts/qbo_record_payments.py        # dry run"
        "\n       python3 scripts/qbo_record_payments.py --apply"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
