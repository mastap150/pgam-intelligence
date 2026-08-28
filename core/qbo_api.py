"""
core/qbo_api.py

Minimal QuickBooks Online Accounting API client.

Exists because the Claude QBO MCP connector is read-mostly — it exposes
reports and invoice edits but no way to record a payment against an invoice.
The Accounting API does, so anything that needs to write cash (or read the
bank side) goes through here.

Credentials (Render env)
------------------------
  QBO_CLIENT_ID       Intuit app client id
  QBO_CLIENT_SECRET   Intuit app client secret
  QBO_REFRESH_TOKEN   seed refresh token — first run only, see below
  QBO_REALM_ID        company id; PGAM Media LLC is 193514590350384

Intuit rotates the refresh token on every use and kills the previous one
after 24h. Render's disk is ephemeral, so the rotated token is persisted to
Neon (`qbo_oauth_token`) rather than to disk — otherwise the first redeploy
more than a day after a run would leave the env seed stale and auth would
start failing. QBO_REFRESH_TOKEN is only ever a seed.
"""

from __future__ import annotations

import base64
import json
import os
import urllib.error
import urllib.parse
import urllib.request
from typing import Any

API_BASE = "https://quickbooks.api.intuit.com"
TOKEN_URL = "https://oauth.platform.intuit.com/oauth2/v1/tokens/bearer"
MINOR_VERSION = "75"

TIMEOUT = 60


class QBOError(RuntimeError):
    """An API call failed. Carries the response body, which is where Intuit
    puts the actual reason — the HTTP status alone is rarely enough."""


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

def config() -> dict:
    return {
        "client_id":     os.environ.get("QBO_CLIENT_ID", ""),
        "client_secret": os.environ.get("QBO_CLIENT_SECRET", ""),
        "refresh_seed":  os.environ.get("QBO_REFRESH_TOKEN", ""),
        "realm_id":      os.environ.get("QBO_REALM_ID", ""),
    }


def missing_config(cfg: dict | None = None) -> list[str]:
    """Names of required settings that are absent. Callers skip when non-empty
    so an unconfigured deploy logs a line instead of raising."""
    cfg = cfg or config()
    return [
        key for key in ("client_id", "client_secret", "realm_id")
        if not cfg.get(key)
    ]


# ---------------------------------------------------------------------------
# Token store
# ---------------------------------------------------------------------------

def ensure_token_table() -> None:
    from core.neon import connect

    with connect() as conn, conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS qbo_oauth_token (
                realm_id      TEXT PRIMARY KEY,
                refresh_token TEXT        NOT NULL,
                updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
            )
            """
        )
        conn.commit()


def _stored_refresh_token(realm_id: str) -> str | None:
    from core.neon import connect

    with connect() as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT refresh_token FROM qbo_oauth_token WHERE realm_id = %s",
            (realm_id,),
        )
        row = cur.fetchone()
    return row[0] if row else None


def _store_refresh_token(realm_id: str, token: str) -> None:
    from core.neon import connect

    with connect() as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO qbo_oauth_token (realm_id, refresh_token, updated_at)
            VALUES (%s, %s, now())
            ON CONFLICT (realm_id)
            DO UPDATE SET refresh_token = EXCLUDED.refresh_token,
                          updated_at    = now()
            """,
            (realm_id, token),
        )
        conn.commit()


def access_token(cfg: dict | None = None) -> str:
    """Exchange the refresh token for an access token, persisting the rotation."""
    cfg = cfg or config()
    refresh = _stored_refresh_token(cfg["realm_id"]) or cfg["refresh_seed"]
    if not refresh:
        raise QBOError(
            "No QBO refresh token available — set QBO_REFRESH_TOKEN for the first run."
        )

    basic = base64.b64encode(
        f"{cfg['client_id']}:{cfg['client_secret']}".encode()
    ).decode()
    body = urllib.parse.urlencode(
        {"grant_type": "refresh_token", "refresh_token": refresh}
    ).encode()

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
        with urllib.request.urlopen(req, timeout=TIMEOUT) as resp:
            payload = json.loads(resp.read().decode())
    except urllib.error.HTTPError as exc:
        raise QBOError(
            f"token refresh failed ({exc.code}): {exc.read().decode()[:500]}"
        ) from exc

    rotated = payload.get("refresh_token")
    if rotated and rotated != refresh:
        _store_refresh_token(cfg["realm_id"], rotated)
        print("[qbo_api] refresh token rotated and persisted")

    return payload["access_token"]


# ---------------------------------------------------------------------------
# Requests
# ---------------------------------------------------------------------------

def _request(cfg: dict, token: str, method: str, path: str,
             params: dict | None = None, payload: dict | None = None) -> dict:
    qs = urllib.parse.urlencode({**(params or {}), "minorversion": MINOR_VERSION})
    url = f"{API_BASE}/v3/company/{cfg['realm_id']}/{path}?{qs}"

    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
    data = None
    if payload is not None:
        data = json.dumps(payload).encode()
        headers["Content-Type"] = "application/json"

    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=TIMEOUT) as resp:
            return json.loads(resp.read().decode())
    except urllib.error.HTTPError as exc:
        raise QBOError(
            f"{method} {path} failed ({exc.code}): {exc.read().decode()[:800]}"
        ) from exc


def get(cfg: dict, token: str, path: str, params: dict | None = None) -> dict:
    return _request(cfg, token, "GET", path, params=params)


def post(cfg: dict, token: str, path: str, payload: dict) -> dict:
    return _request(cfg, token, "POST", path, payload=payload)


def report(cfg: dict, token: str, name: str, params: dict | None = None) -> dict:
    return get(cfg, token, f"reports/{name}", params=params)


def query(cfg: dict, token: str, statement: str) -> list[dict]:
    """Run a QBO SQL-ish query and return the rows, whatever entity they are."""
    res = get(cfg, token, "query", {"query": statement})
    body = res.get("QueryResponse", {})
    for key, value in body.items():
        if isinstance(value, list):
            return value
    return []


# ---------------------------------------------------------------------------
# Lookups
# ---------------------------------------------------------------------------

def _escape(value: str) -> str:
    return value.replace("'", "''")


def find_account(cfg: dict, token: str, *, name: str | None = None,
                 account_type: str | None = None) -> dict | None:
    """Look up a GL account by exact name, else by account type.

    Account ids differ per realm, so nothing here may hardcode one.
    """
    if name:
        rows = query(
            cfg, token,
            f"select Id, Name, AccountType, AccountSubType from Account "
            f"where Name = '{_escape(name)}'",
        )
        if rows:
            return rows[0]
    if account_type:
        rows = query(
            cfg, token,
            f"select Id, Name, AccountType, AccountSubType from Account "
            f"where AccountSubType = '{_escape(account_type)}'",
        )
        if rows:
            return rows[0]
    return None


def find_invoice_by_doc_number(cfg: dict, token: str, doc_number: str) -> dict | None:
    rows = query(
        cfg, token,
        f"select * from Invoice where DocNumber = '{_escape(doc_number)}'",
    )
    return rows[0] if rows else None
