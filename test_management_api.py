"""
test_management_api.py

Probes the Teqblaze SSP platform (ssp.pgammedia.com) and the stats endpoint
(stats.ortb.net) to discover management/write API capabilities beyond the
stats API already in use.

GET requests ONLY — no mutations sent.

Usage:
    python test_management_api.py
"""

import os
import urllib.request
import urllib.error
import urllib.parse
import json
from dotenv import load_dotenv

load_dotenv(override=True)

TB_CLIENT_KEY = os.environ.get("TB_CLIENT_KEY", "")
TB_SECRET_KEY = os.environ.get("TB_SECRET_KEY", "")

if not TB_CLIENT_KEY or not TB_SECRET_KEY:
    print("ERROR: TB_CLIENT_KEY or TB_SECRET_KEY not found in .env — aborting.")
    exit(1)

KEYWORDS = {"floor", "update", "write", "modify", "management", "create", "delete", "edit"}

# ---------------------------------------------------------------------------
# Endpoint paths to probe
# ---------------------------------------------------------------------------
PATHS = [
    "/api/publisher/list",
    "/api/publisher/update",
    "/api/floor/list",
    "/api/floor/update",
    "/api/demand/list",
    "/api/demand/update",
    "/api/endpoint/list",
    "/api/settings",
    "/api/v1/publisher",
    "/api/v1/floor",
    "/api/v1/demand",
    "/admin/api/publisher",
    "/admin/api/floor",
]

DOMAINS = [
    "https://ssp.pgammedia.com",
    "https://stats.ortb.net",
]

# Auth variants to try for each path
AUTH_VARIANTS = [
    # No auth
    {},
    # Query-param auth (matches stats API pattern)
    {"clientKey": TB_CLIENT_KEY, "secretKey": TB_SECRET_KEY},
    # client_key / secret_key snake_case variant
    {"client_key": TB_CLIENT_KEY, "secret_key": TB_SECRET_KEY},
    # api_key single-param variant
    {"api_key": TB_CLIENT_KEY},
    # token variant
    {"token": TB_CLIENT_KEY},
]

AUTH_LABELS = [
    "no-auth",
    "clientKey+secretKey",
    "client_key+secret_key",
    "api_key",
    "token",
]

TIMEOUT = 10


def _body_preview(raw: bytes, limit: int = 300) -> str:
    try:
        text = raw.decode("utf-8", errors="replace")
    except Exception:
        text = repr(raw[:limit])
    return text[:limit].replace("\n", " ").replace("\r", "")


def _contains_keywords(text: str) -> list[str]:
    lower = text.lower()
    return [kw for kw in KEYWORDS if kw in lower]


def probe(url: str, auth_params: dict, auth_label: str) -> dict:
    if auth_params:
        full_url = url + "?" + urllib.parse.urlencode(auth_params)
    else:
        full_url = url

    result = {
        "url":        full_url,
        "base_url":   url,
        "auth_label": auth_label,
        "status":     None,
        "headers":    {},
        "body":       "",
        "keywords":   [],
        "error":      None,
    }

    try:
        req = urllib.request.Request(full_url, method="GET")
        req.add_header("User-Agent", "PGAM-Intelligence-Probe/1.0")
        req.add_header("Accept", "application/json, text/plain, */*")

        with urllib.request.urlopen(req, timeout=TIMEOUT) as resp:
            result["status"]  = resp.status
            result["headers"] = dict(resp.headers)
            raw               = resp.read(2000)
            result["body"]    = _body_preview(raw)
            result["keywords"] = _contains_keywords(result["body"])

    except urllib.error.HTTPError as e:
        result["status"]  = e.code
        result["headers"] = dict(e.headers) if e.headers else {}
        try:
            raw = e.read(2000)
            result["body"] = _body_preview(raw)
            result["keywords"] = _contains_keywords(result["body"])
        except Exception:
            result["body"] = ""

    except urllib.error.URLError as e:
        result["error"] = str(e.reason)

    except Exception as e:
        result["error"] = str(e)

    return result


def print_result(r: dict):
    print(f"\n  URL      : {r['base_url']}")
    print(f"  Auth     : {r['auth_label']}")
    if r["error"]:
        print(f"  ERROR    : {r['error']}")
        return
    print(f"  Status   : {r['status']}")
    # Print relevant headers
    for hk in ("content-type", "www-authenticate", "x-auth", "authorization",
               "x-api-key", "set-cookie", "server", "x-powered-by"):
        val = r["headers"].get(hk) or r["headers"].get(hk.title()) or r["headers"].get(hk.upper())
        if val:
            print(f"  {hk:<16}: {val[:120]}")
    if r["body"]:
        print(f"  Body     : {r['body']}")
    if r["keywords"]:
        print(f"  Keywords : {', '.join(r['keywords'])}")


def run():
    print("=" * 70)
    print("PGAM Intelligence — Management API Probe")
    print(f"Credentials loaded: clientKey={TB_CLIENT_KEY[:6]}... "
          f"secretKey={TB_SECRET_KEY[:6]}...")
    print(f"Domains: {', '.join(DOMAINS)}")
    print(f"Paths: {len(PATHS)}  ×  Auth variants: {len(AUTH_VARIANTS)}")
    print("=" * 70)

    # Track interesting results for summary
    hits_200: list[dict] = []
    hits_401: list[dict] = []
    hits_403: list[dict] = []
    hits_other: list[dict] = []

    for domain in DOMAINS:
        print(f"\n{'='*70}")
        print(f"DOMAIN: {domain}")
        print(f"{'='*70}")

        for path in PATHS:
            base_url = domain + path
            print(f"\n  {'─'*60}")
            print(f"  PATH: {path}")

            best: dict | None = None  # most informative result for this path

            for auth_params, auth_label in zip(AUTH_VARIANTS, AUTH_LABELS):
                r = probe(base_url, auth_params, auth_label)

                # Skip printing no-auth 404s that are identical to subsequent ones
                # — only print if we get something interesting
                interesting = (
                    r["error"] is None and r["status"] not in (404, 405)
                ) or r["error"] is not None

                if interesting:
                    print_result(r)

                    # Track for summary
                    if r["status"] == 200:
                        hits_200.append({**r, "path": path, "domain": domain})
                    elif r["status"] == 401:
                        hits_401.append({**r, "path": path, "domain": domain})
                    elif r["status"] == 403:
                        hits_403.append({**r, "path": path, "domain": domain})
                    elif r["status"] not in (None, 404, 405):
                        hits_other.append({**r, "path": path, "domain": domain})

                    # If we got a 200 with auth, no need to try other variants
                    if r["status"] == 200:
                        break
                else:
                    # Show a one-liner for uninteresting 404s
                    if auth_label == "no-auth":
                        print(f"  [{auth_label}] → {r['status'] or 'ERR:' + str(r['error'])[:40]}")

    # ---------------------------------------------------------------------------
    # Summary
    # ---------------------------------------------------------------------------
    print(f"\n\n{'='*70}")
    print("SUMMARY")
    print(f"{'='*70}")

    print(f"\n✅  200 OK  ({len(hits_200)} results — endpoint exists and responded):")
    if hits_200:
        for r in hits_200:
            kw = f"  [keywords: {', '.join(r['keywords'])}]" if r["keywords"] else ""
            print(f"  {r['domain']}{r['path']}  (auth: {r['auth_label']}){kw}")
    else:
        print("  None")

    print(f"\n🔐  401 Unauthorized  ({len(hits_401)} results — endpoint exists, needs different auth):")
    if hits_401:
        for r in hits_401:
            print(f"  {r['domain']}{r['path']}  (auth tried: {r['auth_label']})")
    else:
        print("  None")

    print(f"\n🚫  403 Forbidden  ({len(hits_403)} results — endpoint exists, access denied):")
    if hits_403:
        for r in hits_403:
            print(f"  {r['domain']}{r['path']}  (auth tried: {r['auth_label']})")
    else:
        print("  None")

    if hits_other:
        print(f"\n⚠️   Other interesting status codes ({len(hits_other)}):")
        for r in hits_other:
            print(f"  [{r['status']}] {r['domain']}{r['path']}  (auth: {r['auth_label']})")

    print(f"\n{'='*70}")
    print("INTERPRETATION")
    print(f"{'='*70}")
    print("  200 with data   → endpoint is live and accessible with current credentials")
    print("  401             → endpoint exists; try Bearer token or different credential format")
    print("  403             → endpoint exists; credentials recognised but insufficient permission")
    print("  404             → endpoint does not exist at this path")
    print("  Connection error→ domain not reachable or path blocked at network level")
    print()


if __name__ == "__main__":
    run()
