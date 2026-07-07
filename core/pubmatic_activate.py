"""
core/pubmatic_activate.py

PubMatic Activate (curator) API client.

Base URL:  https://apps.pubmatic.com/api/activate
Seat:      PGAM_Activate_US   (organizationid = 17496)

Authentication
--------------
PubMatic uses OAuth 2.0 with client_secret_basic. The credential set for
a Developer Integration is FOUR values — access + refresh tokens are only
half of it:

    client_id       + client_secret          ← app registration (rarely rotates)
    access_token    + refresh_token          ← 60-day, refreshable via the pair above

The well-known config that describes the flow:
    https://api.pubmatic.com/.well-known/oauth-authorization-server
    token_endpoint = https://apps.pubmatic.com/v1/developer-integrations/developer/token

This client supports two auth modes; it auto-picks based on which env vars
are populated:

  1) OAuth (production) — PUBMATIC_ACTIVATE_CLIENT_ID +
                          PUBMATIC_ACTIVATE_CLIENT_SECRET +
                          PUBMATIC_ACTIVATE_TOKEN +
                          PUBMATIC_ACTIVATE_REFRESH_TOKEN
     Access tokens are cached in /tmp/pgam_pubmatic_activate_token.json
     and auto-refreshed on 401.

  2) Session token (dev/probe) — PUBMATIC_ACTIVATE_PUBTOKEN
     'pubtoken' header captured from any Activate UI XHR
     (DevTools → Network → any request → Request Headers).
     Session-scoped; expires on browser logout. One-off scripts only.

Required regardless of mode
---------------------------
  PUBMATIC_ACTIVATE_ORG_ID=17496

Advertisers on the PGAM_Activate_US seat (snapshot 2026-07-07)
--------------------------------------------------------------
  27017  Bamboo HR
  26641  IHG
  25871  MF
  26428  Amazon
  25784  JP Morgan
"""

from __future__ import annotations

import base64
import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

from dotenv import load_dotenv

load_dotenv(override=True)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
ACTIVATE_BASE = os.environ.get(
    "PUBMATIC_ACTIVATE_BASE_URL",
    "https://apps.pubmatic.com/api/activate",
)
TOKEN_ENDPOINT = os.environ.get(
    "PUBMATIC_ACTIVATE_TOKEN_ENDPOINT",
    "https://apps.pubmatic.com/v1/developer-integrations/developer/token",
)
ORG_ID        = os.environ.get("PUBMATIC_ACTIVATE_ORG_ID", "").strip()
CLIENT_ID     = os.environ.get("PUBMATIC_ACTIVATE_CLIENT_ID", "").strip()
CLIENT_SECRET = os.environ.get("PUBMATIC_ACTIVATE_CLIENT_SECRET", "").strip()
BEARER_TOKEN  = os.environ.get("PUBMATIC_ACTIVATE_TOKEN", "").strip()
REFRESH_TOK   = os.environ.get("PUBMATIC_ACTIVATE_REFRESH_TOKEN", "").strip()
PUB_TOKEN     = os.environ.get("PUBMATIC_ACTIVATE_PUBTOKEN", "").strip()

TOKEN_CACHE = "/tmp/pgam_pubmatic_activate_token.json"

# ---------------------------------------------------------------------------
# Auth
# ---------------------------------------------------------------------------

class ActivateAuthError(RuntimeError):
    """Raised when the API rejects our credentials or no credentials are set."""


def _load_cached_token() -> tuple[str, str]:
    """Return (access_token, refresh_token) from cache if unexpired."""
    if not os.path.exists(TOKEN_CACHE):
        return ("", "")
    try:
        with open(TOKEN_CACHE) as f:
            data = json.load(f)
        expires_at = float(data.get("expires_at", 0))
        if expires_at > time.time() + 300:   # 5-min safety margin
            return (data.get("access_token", ""), data.get("refresh_token", ""))
    except (json.JSONDecodeError, OSError, ValueError):
        pass
    return ("", "")


def _save_token(access_token: str, refresh_token: str, expires_in: int):
    try:
        with open(TOKEN_CACHE, "w") as f:
            json.dump({
                "access_token":  access_token,
                "refresh_token": refresh_token,
                "expires_at":    time.time() + int(expires_in),
            }, f)
    except OSError:
        pass


def refresh_access_token() -> tuple[str, str]:
    """
    Exchange the refresh token for a fresh access token via OAuth.
    Returns (access_token, refresh_token).  Raises ActivateAuthError.
    """
    if not (CLIENT_ID and CLIENT_SECRET and REFRESH_TOK):
        raise ActivateAuthError(
            "OAuth refresh needs PUBMATIC_ACTIVATE_CLIENT_ID + _CLIENT_SECRET "
            "+ _REFRESH_TOKEN. See core/pubmatic_activate.py docstring for the "
            "PubMatic Developer Integration credential set."
        )
    body = urllib.parse.urlencode({
        "grant_type":    "refresh_token",
        "refresh_token": REFRESH_TOK,
        "client_id":     CLIENT_ID,          # some servers require it in the body too
    }).encode()
    creds = base64.b64encode(f"{CLIENT_ID}:{CLIENT_SECRET}".encode()).decode()
    req = urllib.request.Request(TOKEN_ENDPOINT, data=body, method="POST")
    req.add_header("Authorization", f"Basic {creds}")
    req.add_header("Content-Type", "application/x-www-form-urlencoded")
    req.add_header("Accept", "application/json")
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read().decode())
    except urllib.error.HTTPError as e:
        raise ActivateAuthError(
            f"Refresh failed: HTTP {e.code} — {e.read().decode(errors='replace')[:400]}"
        ) from e
    access  = data.get("access_token", "")
    refresh = data.get("refresh_token", REFRESH_TOK)   # PubMatic may return same
    expires = int(data.get("expires_in", 3600))
    if not access:
        raise ActivateAuthError(f"Refresh response missing access_token: {data}")
    _save_token(access, refresh, expires)
    return (access, refresh)


def _current_bearer() -> str:
    """Return a valid access token — cache first, then refresh, then env."""
    cached_access, _ = _load_cached_token()
    if cached_access:
        return cached_access
    if CLIENT_ID and CLIENT_SECRET and REFRESH_TOK:
        access, _ = refresh_access_token()
        return access
    return BEARER_TOKEN   # last-resort: whatever's pasted in env


def _auth_headers() -> dict:
    """
    Pick an auth mode based on which env vars are populated.
    Session pubtoken wins if both are set — it's the mode that currently works
    for hitting /api/activate/* while our OAuth scope is being sorted with support.
    """
    if PUB_TOKEN:
        return {"pubtoken": PUB_TOKEN}
    token = _current_bearer()
    if token:
        return {"Authorization": f"Bearer {token}"}
    raise ActivateAuthError(
        "No PubMatic Activate credentials configured. Set one of:\n"
        "  Mode A (OAuth): PUBMATIC_ACTIVATE_CLIENT_ID + _CLIENT_SECRET + _TOKEN + _REFRESH_TOKEN\n"
        "  Mode B (session): PUBMATIC_ACTIVATE_PUBTOKEN (from UI DevTools)"
    )


def activate_configured() -> bool:
    """Return True if we have enough credentials to attempt a call."""
    if not ORG_ID:
        return False
    if PUB_TOKEN:
        return True
    return bool(BEARER_TOKEN or (CLIENT_ID and CLIENT_SECRET and REFRESH_TOK))


# ---------------------------------------------------------------------------
# Core request
# ---------------------------------------------------------------------------

def _build_request(path: str, method: str, params: dict | None, body: dict | None):
    url = f"{ACTIVATE_BASE}{path}"
    if params:
        url += "?" + urllib.parse.urlencode(params)
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, method=method)
    req.add_header("Accept", "application/json")
    req.add_header("organizationid", ORG_ID)
    req.add_header("usepubmaticerrorformat", "true")
    if data is not None:
        req.add_header("Content-Type", "application/json")
    for k, v in _auth_headers().items():
        req.add_header(k, v)
    return req


def _request(
    path: str,
    method: str = "GET",
    params: dict | None = None,
    body: dict | None = None,
    timeout: int = 30,
) -> dict | list:
    """
    Low-level Activate API request. Returns parsed JSON.

    Auto-refreshes the bearer once on 401 if OAuth creds are set. Session
    pubtoken auth doesn't retry — that mode implies the operator holds a
    fresh token from the UI.

    Raises:
        ActivateAuthError on 401/403 after retry, or missing config.
        urllib.error.HTTPError on other non-2xx.
    """
    if not ORG_ID:
        raise ActivateAuthError(
            "PUBMATIC_ACTIVATE_ORG_ID not set. Add PUBMATIC_ACTIVATE_ORG_ID=17496 to .env."
        )

    def _do():
        req = _build_request(path, method, params, body)
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.read().decode()

    try:
        raw = _do()
    except urllib.error.HTTPError as e:
        can_retry = (
            e.code == 401
            and not PUB_TOKEN
            and CLIENT_ID and CLIENT_SECRET and REFRESH_TOK
        )
        if can_retry:
            # Drop the cache and force a refresh, then retry once.
            try:
                os.remove(TOKEN_CACHE)
            except OSError:
                pass
            refresh_access_token()
            try:
                raw = _do()
            except urllib.error.HTTPError as e2:
                body_txt = e2.read().decode(errors="replace")[:400]
                raise ActivateAuthError(
                    f"HTTP {e2.code} from {path} after refresh — {body_txt}"
                ) from e2
        elif e.code in (401, 403):
            body_txt = e.read().decode(errors="replace")[:400]
            raise ActivateAuthError(
                f"HTTP {e.code} from {path} — credentials rejected. Body: {body_txt}"
            ) from e
        else:
            raise

    if not raw:
        return {}
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return {"raw": raw}


# ---------------------------------------------------------------------------
# High-level wrappers
#
# Endpoint paths below reflect what the Activate SPA calls (confirmed by
# DevTools 2026-07-07). Where a path is inferred but unconfirmed, it's
# noted; those may need adjustment once we have a working credential.
# ---------------------------------------------------------------------------

def get_advertiser_fees(advertiser_id: int, fee_type: str = "AD_SERVING") -> dict:
    """
    Confirmed endpoint. Returns custom fee config for an advertiser.
    Example seen in prod: GET /api/activate/fees/custom/AD_SERVING/advertiser/27017
    """
    return _request(f"/fees/custom/{fee_type}/advertiser/{advertiser_id}")


def list_advertisers() -> list:
    """
    Inferred. Adjust the path once the SPA is observed hitting the list route.
    """
    result = _request("/advertisers")
    if isinstance(result, dict) and "data" in result:
        return result["data"]
    return result if isinstance(result, list) else []


def get_advertiser(advertiser_id: int) -> dict:
    """Inferred."""
    return _request(f"/advertiser/{advertiser_id}")


def list_campaigns(advertiser_id: int) -> list:
    """Inferred. SPA URL suggests /advertiser/{id}/campaign."""
    result = _request(f"/advertiser/{advertiser_id}/campaigns")
    if isinstance(result, dict) and "data" in result:
        return result["data"]
    return result if isinstance(result, list) else []


def list_deals(advertiser_id: int) -> list:
    """Inferred."""
    result = _request(f"/advertiser/{advertiser_id}/deals")
    if isinstance(result, dict) and "data" in result:
        return result["data"]
    return result if isinstance(result, list) else []


def get_organization() -> dict:
    """Inferred — organization/seat metadata."""
    return _request(f"/organization/{ORG_ID}")


# ---------------------------------------------------------------------------
# CLI
#
# Usage:
#   python -m core.pubmatic_activate advertisers
#   python -m core.pubmatic_activate advertiser 27017
#   python -m core.pubmatic_activate fees 27017
#   python -m core.pubmatic_activate campaigns 27017
#   python -m core.pubmatic_activate deals 27017
#   python -m core.pubmatic_activate org
#   python -m core.pubmatic_activate config
# ---------------------------------------------------------------------------

_HELP = """\
PubMatic Activate CLI

Commands:
  config                          Print resolved config (safe — masks tokens)
  refresh                         Force an OAuth refresh (requires client_id/secret)
  advertisers                     List advertisers under the seat
  advertiser <id>                 Fetch one advertiser
  fees <adv_id> [fee_type]        Get custom fees (default fee_type=AD_SERVING)
  campaigns <adv_id>              List an advertiser's campaigns
  deals <adv_id>                  List an advertiser's deals
  org                             Fetch organization/seat metadata
"""


def _mask(s: str) -> str:
    if not s:
        return "(unset)"
    if len(s) <= 8:
        return "*" * len(s)
    return f"{s[:4]}…{s[-4:]}"


def _cli_config():
    print("PubMatic Activate — resolved config")
    print(f"  ACTIVATE_BASE                    : {ACTIVATE_BASE}")
    print(f"  TOKEN_ENDPOINT                   : {TOKEN_ENDPOINT}")
    print(f"  PUBMATIC_ACTIVATE_ORG_ID         : {ORG_ID or '(unset)'}")
    print(f"  PUBMATIC_ACTIVATE_CLIENT_ID      : {_mask(CLIENT_ID)}")
    print(f"  PUBMATIC_ACTIVATE_CLIENT_SECRET  : {_mask(CLIENT_SECRET)}")
    print(f"  PUBMATIC_ACTIVATE_TOKEN          : {_mask(BEARER_TOKEN)}")
    print(f"  PUBMATIC_ACTIVATE_REFRESH_TOKEN  : {_mask(REFRESH_TOK)}")
    print(f"  PUBMATIC_ACTIVATE_PUBTOKEN       : {_mask(PUB_TOKEN)}")
    cached_access, _ = _load_cached_token()
    print(f"  cached access (from {TOKEN_CACHE}) : {'present' if cached_access else '(none)'}")
    if PUB_TOKEN:
        print("  → auth mode: session pubtoken")
    elif CLIENT_ID and CLIENT_SECRET and REFRESH_TOK:
        print("  → auth mode: OAuth (refreshable)")
    elif BEARER_TOKEN:
        print("  → auth mode: OAuth bearer (no refresh — cannot recover from expiry)")
    else:
        print("  → auth mode: (none)")


def _dump(x):
    print(json.dumps(x, indent=2, default=str))


def main(argv: list[str]) -> int:
    if not argv or argv[0] in ("-h", "--help", "help"):
        print(_HELP)
        return 0
    cmd, *rest = argv
    try:
        if cmd == "config":
            _cli_config()
        elif cmd == "refresh":
            access, refresh = refresh_access_token()
            print(f"OK — new access token cached at {TOKEN_CACHE}")
            print(f"     access:  {_mask(access)}")
            print(f"     refresh: {_mask(refresh)}")
        elif cmd == "advertisers":
            _dump(list_advertisers())
        elif cmd == "advertiser":
            _dump(get_advertiser(int(rest[0])))
        elif cmd == "fees":
            adv = int(rest[0])
            fee_type = rest[1] if len(rest) > 1 else "AD_SERVING"
            _dump(get_advertiser_fees(adv, fee_type))
        elif cmd == "campaigns":
            _dump(list_campaigns(int(rest[0])))
        elif cmd == "deals":
            _dump(list_deals(int(rest[0])))
        elif cmd == "org":
            _dump(get_organization())
        else:
            print(f"Unknown command: {cmd}\n\n{_HELP}", file=sys.stderr)
            return 2
    except ActivateAuthError as e:
        print(f"AUTH ERROR: {e}", file=sys.stderr)
        return 3
    except (IndexError, ValueError) as e:
        print(f"USAGE ERROR: {e}\n\n{_HELP}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
