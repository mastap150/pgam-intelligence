"""
core/pubmatic_activate.py

PubMatic Activate (curator) API client.

Base URL:  https://apps.pubmatic.com/api/activate
Seat:      PGAM_Activate_US   (organizationid = 17496)

Authentication
--------------
The Activate app uses a session-cookie auth model, not the same OAuth
bearer that the public PubMatic API console mints. This client supports
both modes and auto-selects based on which env vars are set:

  1) OAuth Bearer  — PUBMATIC_ACTIVATE_TOKEN
                     60-day access token from the PubMatic API console.
                     Refresh via PUBMATIC_ACTIVATE_REFRESH_TOKEN.

                     STATUS 2026-07-07: the bearer minted for our seat
                     401s against /api/activate/*. Ticket open with our
                     PubMatic AM to confirm the correct token grant.

  2) Session token — PUBMATIC_ACTIVATE_PUBTOKEN
                     'pubtoken' header captured from any Activate UI XHR
                     (DevTools → Network → any request → Request Headers).
                     Session-scoped; expires when the browser logs out.
                     Fine for one-off scripts, not scheduled jobs.

Required env
------------
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

import json
import os
import sys
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
ORG_ID       = os.environ.get("PUBMATIC_ACTIVATE_ORG_ID", "").strip()
BEARER_TOKEN = os.environ.get("PUBMATIC_ACTIVATE_TOKEN", "").strip()
REFRESH_TOK  = os.environ.get("PUBMATIC_ACTIVATE_REFRESH_TOKEN", "").strip()
PUB_TOKEN    = os.environ.get("PUBMATIC_ACTIVATE_PUBTOKEN", "").strip()

# ---------------------------------------------------------------------------
# Auth
# ---------------------------------------------------------------------------

class ActivateAuthError(RuntimeError):
    """Raised when the API rejects our credentials or no credentials are set."""


def _auth_headers() -> dict:
    """
    Pick an auth mode based on which env vars are populated.
    Session pubtoken wins if both are set — it's the mode that currently works.
    """
    if PUB_TOKEN:
        return {"pubtoken": PUB_TOKEN}
    if BEARER_TOKEN:
        return {"Authorization": f"Bearer {BEARER_TOKEN}"}
    raise ActivateAuthError(
        "No PubMatic Activate credentials configured. Set one of:\n"
        "  PUBMATIC_ACTIVATE_PUBTOKEN   (session token from UI DevTools) — recommended today\n"
        "  PUBMATIC_ACTIVATE_TOKEN      (OAuth bearer from API console)   — awaiting PubMatic scope fix"
    )


def activate_configured() -> bool:
    """Return True if any credential + org_id are present."""
    return bool(ORG_ID and (BEARER_TOKEN or PUB_TOKEN))


# ---------------------------------------------------------------------------
# Core request
# ---------------------------------------------------------------------------

def _request(
    path: str,
    method: str = "GET",
    params: dict | None = None,
    body: dict | None = None,
    timeout: int = 30,
) -> dict | list:
    """
    Low-level Activate API request. Returns parsed JSON.

    Raises:
        ActivateAuthError on 401/403.
        urllib.error.HTTPError on other non-2xx.
    """
    if not ORG_ID:
        raise ActivateAuthError(
            "PUBMATIC_ACTIVATE_ORG_ID not set. Add PUBMATIC_ACTIVATE_ORG_ID=17496 to .env."
        )

    url = f"{ACTIVATE_BASE}{path}"
    if params:
        url += "?" + urllib.parse.urlencode(params)

    data = None
    if body is not None:
        data = json.dumps(body).encode()

    req = urllib.request.Request(url, data=data, method=method)
    req.add_header("Accept", "application/json")
    req.add_header("organizationid", ORG_ID)
    req.add_header("usepubmaticerrorformat", "true")
    if data is not None:
        req.add_header("Content-Type", "application/json")
    for k, v in _auth_headers().items():
        req.add_header(k, v)

    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read().decode()
    except urllib.error.HTTPError as e:
        if e.code in (401, 403):
            body_txt = e.read().decode(errors="replace")[:400]
            raise ActivateAuthError(
                f"HTTP {e.code} from {path} — credentials rejected. Body: {body_txt}"
            ) from e
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
    print(f"  ACTIVATE_BASE                : {ACTIVATE_BASE}")
    print(f"  PUBMATIC_ACTIVATE_ORG_ID     : {ORG_ID or '(unset)'}")
    print(f"  PUBMATIC_ACTIVATE_TOKEN      : {_mask(BEARER_TOKEN)}")
    print(f"  PUBMATIC_ACTIVATE_REFRESH... : {_mask(REFRESH_TOK)}")
    print(f"  PUBMATIC_ACTIVATE_PUBTOKEN   : {_mask(PUB_TOKEN)}")
    if PUB_TOKEN:
        print("  → auth mode: session pubtoken")
    elif BEARER_TOKEN:
        print("  → auth mode: OAuth bearer")
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
