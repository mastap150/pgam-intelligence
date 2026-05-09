"""
scripts/partnerize_api.py

Partnerize Partners API client for PGAM publisher properties
(boxingnews.com, destination.com).

Auth: HTTP Basic — Authorization header is base64(application_key:user_api_key).
Set PARTNERIZE_APPLICATION_KEY + PARTNERIZE_USER_API_KEY in .env.

Subcommands
-----------
  campaigns    List campaigns the publisher is approved on (status=a).
               Use --status=p (pending), r (rejected), d (declined) to filter.
  camrefs      List the publisher's camref tokens — one per campaign.
               These are the strings that go into prf.hn/click/camref:XXX/...
  discover     Browse every advertiser/campaign visible to the publisher,
               including ones not yet approved.
  create-link  Create a tracked deep-link for a campaign (DAZN, Expedia, etc).
               Returns the prf.hn URL ready to drop into env or markdown.

Examples
--------
  python scripts/partnerize_api.py campaigns --publisher-id=1101l99999
  python scripts/partnerize_api.py camrefs   --publisher-id=1101l99999
  python scripts/partnerize_api.py create-link \\
      --publisher-id=1101l99999 \\
      --campaign-id=1011l532 \\
      --destination='https://www.dazn.com/en-US/home' \\
      --description='boxingnews-dazn-default'
"""
import argparse
import base64
import json
import os
import sys
from typing import Any

import requests

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from dotenv import load_dotenv

load_dotenv(override=True)

BASE = "https://api.partnerize.com"


def _auth_header() -> dict[str, str]:
    app_key = os.environ.get("PARTNERIZE_APPLICATION_KEY")
    user_key = os.environ.get("PARTNERIZE_USER_API_KEY")
    if not app_key or not user_key:
        sys.exit(
            "Missing PARTNERIZE_APPLICATION_KEY or PARTNERIZE_USER_API_KEY. "
            "Get them from console.partnerize.com → Settings → Account."
        )
    token = base64.b64encode(f"{app_key}:{user_key}".encode()).decode()
    return {"Authorization": f"Basic {token}", "Accept": "application/json"}


def _get(path: str, **params) -> Any:
    r = requests.get(f"{BASE}{path}", headers=_auth_header(), params=params, timeout=60)
    if r.status_code >= 400:
        sys.exit(f"GET {path} → {r.status_code}\n{r.text[:800]}")
    return r.json()


def _post(path: str, body: dict) -> Any:
    headers = _auth_header() | {"Content-Type": "application/json"}
    r = requests.post(f"{BASE}{path}", headers=headers, json=body, timeout=60)
    if r.status_code >= 400:
        sys.exit(f"POST {path} → {r.status_code}\n{r.text[:800]}")
    return r.json()


def cmd_campaigns(args: argparse.Namespace) -> None:
    data = _get(f"/user/publisher/{args.publisher_id}/campaign/{args.status}")
    rows = data.get("campaigns", [])
    print(f"# {len(rows)} campaign(s) with status={args.status}\n")
    for entry in rows:
        c = entry.get("campaign", entry)
        cid = c.get("campaign_id")
        title = c.get("title") or c.get("campaign_title") or c.get("campaign_logo", "")
        adv = c.get("advertiser_id")
        deeplink = c.get("allow_deep_linking")
        currency = c.get("campaign_currency") or c.get("currency", "")
        print(f"  {cid:<14} adv={adv:<12} deep={deeplink:<2} cur={currency:<4} {title}")
    if args.json:
        print("\n--- raw ---")
        print(json.dumps(data, indent=2))


def cmd_camrefs(args: argparse.Namespace) -> None:
    data = _get(f"/reference/publisher/camref/{args.publisher_id}")
    print(json.dumps(data, indent=2))


def cmd_discover(args: argparse.Namespace) -> None:
    data = _get(f"/v2/publishers/{args.publisher_id}/discovery/advertisers")
    print(json.dumps(data, indent=2))


def cmd_create_link(args: argparse.Namespace) -> None:
    body: dict[str, Any] = {
        "campaign_id": args.campaign_id,
        "destination_url": args.destination,
        "active": True,
    }
    if args.description:
        body["description"] = args.description
    if args.pubref:
        body["params"] = [{"key": "pubref", "value": args.pubref}]
    data = _post(f"/v2/publishers/{args.publisher_id}/links", body)
    print(json.dumps(data, indent=2))


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = p.add_subparsers(dest="cmd", required=True)

    c = sub.add_parser("campaigns", help="List campaigns by approval status")
    c.add_argument("--publisher-id", required=True)
    c.add_argument("--status", default="a", help="a=approved, p=pending, r=rejected, d=declined")
    c.add_argument("--json", action="store_true", help="Also dump raw JSON")
    c.set_defaults(fn=cmd_campaigns)

    c = sub.add_parser("camrefs", help="List camref tokens per campaign")
    c.add_argument("--publisher-id", required=True)
    c.set_defaults(fn=cmd_camrefs)

    c = sub.add_parser("discover", help="Browse all advertisers visible to publisher")
    c.add_argument("--publisher-id", required=True)
    c.set_defaults(fn=cmd_discover)

    c = sub.add_parser("create-link", help="Create a tracked deep-link")
    c.add_argument("--publisher-id", required=True)
    c.add_argument("--campaign-id", required=True)
    c.add_argument("--destination", required=True, help="Target URL (e.g. https://www.dazn.com/...)")
    c.add_argument("--description", help="Internal label, e.g. boxingnews-dazn-inline")
    c.add_argument("--pubref", help="Attribution token appended as ?pubref=")
    c.set_defaults(fn=cmd_create_link)

    args = p.parse_args()
    args.fn(args)


if __name__ == "__main__":
    main()
