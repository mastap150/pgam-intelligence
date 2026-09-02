#!/usr/bin/env python3
"""One preflight for every platform credential this repo talks to.

Answers, in one command, the question that keeps costing sessions an hour:
"is this failing because the token is missing, because the network is blocked,
or because the token is wrong?" Those three look identical at the call site and
have completely different fixes.

    python3 scripts/platform_doctor.py            # check everything
    python3 scripts/platform_doctor.py --json     # machine-readable
    python3 scripts/platform_doctor.py --only vercel,render

Each platform is checked in three stages, and it stops at the first failure:

    token     is the env var set at all?
    network   can we open a connection to the host?
    auth      does the host accept the token?

Where to run it
---------------
Locally, or on a GitHub Actions runner. NOT usefully in a cloud session
(claude.ai/code): that sandbox's network policy denies CONNECT to every host
below except api.github.com, and its environment variable box is not a secrets
store. See CLAUDE.md, "Cloud sessions: what actually works".

The workflow at .github/workflows/platform-doctor.yml runs this on a runner
with the tokens wired from GitHub Actions secrets, which is the supported way
to exercise these credentials from a session.

Tokens are read from the environment and never printed, logged, or included in
any error message.
"""

from __future__ import annotations

import argparse
import json
import os
import socket
import sys
from typing import Dict, List
from urllib.parse import urlparse

import requests

TIMEOUT = 15

# name -> (env var, probe URL, what it unlocks). The probe is always the
# cheapest authenticated read the API offers — an identity or list endpoint,
# never anything that writes.
PLATFORMS: Dict[str, dict] = {
    "github": {
        "env": "GITHUB_TOKEN",
        "url": "https://api.github.com/user",
        "unlocks": "repo read/write (harness-provided in cloud sessions)",
    },
    "vercel": {
        "env": "VERCEL_TOKEN",
        "url": "https://api.vercel.com/v2/user",
        "unlocks": "scripts/vercel_env_sync.py",
    },
    "render": {
        "env": "RENDER_API_KEY",
        "url": "https://api.render.com/v1/owners?limit=1",
        "unlocks": "scripts/render_env_sync.py",
    },
    "neon": {
        "env": "NEON_API_KEY",
        "url": "https://api.neon.tech/api/v2/users/me",
        "unlocks": "scripts/neon_admin.py --list-projects",
    },
    "resend": {
        "env": "RESEND_API_KEY",
        "url": "https://api.resend.com/domains",
        "unlocks": "core/mailer.py with EMAIL_PROVIDER=resend",
    },
    "sendgrid": {
        "env": "SENDGRID_KEY",
        "url": "https://api.sendgrid.com/v3/scopes",
        "unlocks": "core/mailer.py (default provider), daily_email.py",
    },
}

# Reachability only — no credential of ours to present. admin.pgammedia.com is
# the Next.js app in pgam-direct/web; it is Clerk-gated, so a 401/403/redirect
# means reachable-and-working, not broken. Its DATA lives in Neon pgam_direct,
# which is why a DSN is usually the better route than scraping the dashboard.
REACHABILITY_ONLY: Dict[str, str] = {
    "admin.pgammedia.com": "https://admin.pgammedia.com",
}

OK, WARN, FAIL = "ok", "warn", "fail"


def _tcp_open(url: str) -> tuple[bool, str]:
    """Can we even open the socket? Distinguishes a policy block from a 401.

    Not authoritative behind an HTTP proxy: the connect can succeed against the
    proxy while the proxy then refuses CONNECT to the host. The ProxyError
    branch below is what actually catches that case, so treat a True here as
    "no DNS/route problem", not as "the host is reachable".
    """
    p = urlparse(url)
    host, port = p.hostname, p.port or (443 if p.scheme == "https" else 80)
    try:
        with socket.create_connection((host, port), timeout=TIMEOUT):
            return True, ""
    except socket.gaierror as exc:
        return False, f"DNS failure ({exc.strerror or exc})"
    except (TimeoutError, socket.timeout):
        return False, "timed out"
    except OSError as exc:
        return False, f"{exc.strerror or exc}"


def check_platform(name: str, spec: dict) -> dict:
    env_name = spec["env"]
    token = os.environ.get(env_name, "")
    row = {"name": name, "env": env_name, "unlocks": spec["unlocks"]}

    if not token:
        return {**row, "status": FAIL, "stage": "token",
                "detail": f"{env_name} is not set"}

    reachable, why = _tcp_open(spec["url"])
    if not reachable:
        # A blocked host is not a bad token — say so, so nobody rotates a
        # perfectly good credential chasing this.
        return {**row, "status": WARN, "stage": "network",
                "detail": f"{urlparse(spec['url']).hostname} unreachable: {why} "
                          f"(token is set; not validated)"}

    try:
        resp = requests.get(
            spec["url"],
            headers={"Authorization": f"Bearer {token}",
                     "Accept": "application/json"},
            timeout=TIMEOUT,
        )
    except requests.exceptions.ProxyError:
        return {**row, "status": WARN, "stage": "network",
                "detail": f"{urlparse(spec['url']).hostname} blocked by the "
                          f"sandbox network policy (proxy refused CONNECT); "
                          f"token is set but unvalidated"}
    except requests.RequestException as exc:
        return {**row, "status": WARN, "stage": "network",
                "detail": f"request failed: {type(exc).__name__}"}

    if resp.status_code in (200, 201, 204):
        return {**row, "status": OK, "stage": "auth", "detail": "authenticated"}
    if resp.status_code in (401, 403):
        return {**row, "status": FAIL, "stage": "auth",
                "detail": f"HTTP {resp.status_code} — token rejected "
                          f"(expired, revoked, or wrong scope)"}
    return {**row, "status": WARN, "stage": "auth",
            "detail": f"HTTP {resp.status_code} — unexpected, token may be fine"}


def check_reachability(name: str, url: str) -> dict:
    row = {"name": name, "env": "-", "unlocks": "dashboard (Clerk-gated)"}
    reachable, why = _tcp_open(url)
    if not reachable:
        return {**row, "status": WARN, "stage": "network",
                "detail": f"unreachable: {why}"}
    try:
        resp = requests.get(url, timeout=TIMEOUT, allow_redirects=False)
        return {**row, "status": OK, "stage": "network",
                "detail": f"reachable (HTTP {resp.status_code})"}
    except requests.exceptions.ProxyError:
        return {**row, "status": WARN, "stage": "network",
                "detail": "blocked by the sandbox network policy "
                          "(proxy refused CONNECT)"}
    except requests.RequestException as exc:
        return {**row, "status": WARN, "stage": "network",
                "detail": f"connected but request failed: {type(exc).__name__}"}


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Preflight every platform credential this repo uses.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    ap.add_argument("--only", help="comma-separated subset, e.g. vercel,render")
    ap.add_argument("--json", action="store_true", dest="as_json",
                    help="emit JSON instead of a table")
    args = ap.parse_args()

    wanted = None
    if args.only:
        wanted = {n.strip().lower() for n in args.only.split(",") if n.strip()}
        unknown = wanted - set(PLATFORMS) - set(REACHABILITY_ONLY)
        if unknown:
            print(f"error: unknown platform(s): {', '.join(sorted(unknown))}",
                  file=sys.stderr)
            return 2

    rows: List[dict] = []
    for name, spec in PLATFORMS.items():
        if wanted is None or name in wanted:
            rows.append(check_platform(name, spec))
    for name, url in REACHABILITY_ONLY.items():
        if wanted is None or name in wanted:
            rows.append(check_reachability(name, url))

    if args.as_json:
        print(json.dumps(rows, indent=2))
    else:
        mark = {OK: "ok  ", WARN: "WARN", FAIL: "FAIL"}
        w = max(len(r["name"]) for r in rows)
        print(f"\n{'':4} {'PLATFORM':<{w}}  {'STAGE':<8} DETAIL")
        for r in rows:
            print(f"{mark[r['status']]} {r['name']:<{w}}  "
                  f"{r['stage']:<8} {r['detail']}")

        fails = [r for r in rows if r["status"] == FAIL]
        warns = [r for r in rows if r["status"] == WARN]
        print()
        if fails:
            print(f"{len(fails)} failing: {', '.join(r['name'] for r in fails)}")
            for r in fails:
                if r["stage"] == "token":
                    print(f"  {r['name']}: set {r['env']} — unlocks {r['unlocks']}")
        if warns:
            print(f"{len(warns)} unverified: "
                  f"{', '.join(r['name'] for r in warns)}")
            print("  A blocked host is a sandbox network policy, not a bad "
                  "token — do not rotate\n  a credential over one. Run this "
                  "locally or via .github/workflows/platform-doctor.yml.")
        if not fails and not warns:
            print("all platforms reachable and authenticated")

    return 1 if any(r["status"] == FAIL for r in rows) else 0


if __name__ == "__main__":
    sys.exit(main())
