#!/usr/bin/env python3
"""Inspect Neon projects/branches, and audit the DSN env vars this repo needs.

Two jobs, one of which works with no network at all:

    --check-dsn     verify every DATABASE_URL this repo reads is set and
                    parseable. Pure local parsing, no connection, no API key.
                    This is the one that catches "the agent silently did
                    nothing because a DSN was missing".

    everything else Neon management API (console.neon.tech/api/v2), needs:
                    export NEON_API_KEY=...   # console.neon.tech -> API Keys

Usage
-----
    python3 scripts/neon_admin.py --check-dsn
    python3 scripts/neon_admin.py --list-projects
    python3 scripts/neon_admin.py --project <id> --list-branches

Values are never printed. DSNs are shown host-and-database only, with the
password redacted.
"""

from __future__ import annotations

import argparse
import os
import sys
from typing import Dict, List
from urllib.parse import urlparse

import requests

API = "https://console.neon.tech/api/v2"

# Every DSN env var read anywhere in this repo, and who needs it. Keep in step
# with .env.example — a name added there and not here is invisible to --check-dsn.
KNOWN_DSNS: Dict[str, str] = {
    "PGAM_DIRECT_DATABASE_URL": "primary — pgam_direct schema, most ETLs",
    "DATABASE_URL":             "fallback for PGAM_DIRECT_DATABASE_URL",
    "BOXINGNEWS_DATABASE_URL":  "boxingnews content DB",
    "FINANCE_DATABASE_URL":     "finance / P&L (access-restricted)",
    "HEALTHNATION_DATABASE_URL": "healthnation content DB, read-only",
    "DSP_DATABASE_URL":         "pgam-dsp-dashboard",
}


def die(msg: str) -> None:
    print(f"error: {msg}", file=sys.stderr)
    sys.exit(1)


def redact(dsn: str) -> str:
    """host/database only — never the password, never the full DSN."""
    try:
        p = urlparse(dsn)
    except Exception:
        return "<unparseable>"
    if not p.hostname:
        return "<no host>"
    db = (p.path or "").lstrip("/") or "<no db>"
    user = f"{p.username}@" if p.username else ""
    return f"{user}{p.hostname}/{db}"


def cmd_check_dsn() -> int:
    print("DSN environment audit\n")
    missing: List[str] = []
    bad: List[str] = []

    width = max(len(k) for k in KNOWN_DSNS)
    for name, purpose in KNOWN_DSNS.items():
        raw = os.environ.get(name, "")
        if not raw:
            print(f"  unset    {name:<{width}}  {purpose}")
            missing.append(name)
            continue
        p = urlparse(raw)
        if p.scheme not in ("postgres", "postgresql") or not p.hostname:
            print(f"  INVALID  {name:<{width}}  not a postgres:// URL")
            bad.append(name)
            continue
        print(f"  ok       {name:<{width}}  {redact(raw)}")

    print()
    if bad:
        print(f"{len(bad)} malformed: {', '.join(bad)}")
    if missing:
        print(f"{len(missing)} unset: {', '.join(missing)}")
        print("\nUnset is not automatically wrong — most jobs need only "
              "PGAM_DIRECT_DATABASE_URL\n(or DATABASE_URL as its fallback). "
              "It is wrong when a job that reads one\nis scheduled to run.")

    primary = os.environ.get("PGAM_DIRECT_DATABASE_URL") or os.environ.get("DATABASE_URL")
    if not primary:
        print("\nBLOCKING: neither PGAM_DIRECT_DATABASE_URL nor DATABASE_URL is set — "
              "\nevery ETL that writes pgam_direct will fail.")
        return 1
    return 1 if bad else 0


class Neon:
    def __init__(self, key: str):
        self.session = requests.Session()
        self.session.headers.update({
            "Authorization": f"Bearer {key}",
            "Accept": "application/json",
        })

    def _get(self, path: str, **kw) -> dict:
        resp = self.session.get(f"{API}{path}", timeout=30, **kw)
        if resp.status_code == 401:
            die("401 from Neon. NEON_API_KEY is missing, expired, or revoked.")
        if resp.status_code == 403:
            die("403 from Neon. The key lacks access to this project.")
        resp.raise_for_status()
        return resp.json()

    def list_projects(self) -> List[dict]:
        return self._get("/projects").get("projects", [])

    def list_branches(self, project_id: str) -> List[dict]:
        return self._get(f"/projects/{project_id}/branches").get("branches", [])

    def list_endpoints(self, project_id: str) -> List[dict]:
        return self._get(f"/projects/{project_id}/endpoints").get("endpoints", [])

    def list_roles(self, project_id: str, branch_id: str) -> List[dict]:
        return self._get(
            f"/projects/{project_id}/branches/{branch_id}/roles"
        ).get("roles", [])


def cmd_list_projects(nc: Neon) -> int:
    projects = nc.list_projects()
    print(f"{len(projects)} project(s):")
    for p in sorted(projects, key=lambda x: x.get("name", "")):
        print(f"  {p.get('name', '?'):<28} {p.get('id', '?'):<24} "
              f"{p.get('region_id', '?')}")
    return 0


def cmd_list_branches(nc: Neon, project_id: str) -> int:
    branches = nc.list_branches(project_id)
    print(f"{len(branches)} branch(es) in {project_id}:")
    for b in sorted(branches, key=lambda x: not x.get("default", False)):
        flag = "default" if b.get("default") else ""
        print(f"  {b.get('name', '?'):<28} {b.get('id', '?'):<24} {flag}")
    return 0


def cmd_inventory(nc: Neon) -> int:
    """Map every project to its endpoint hosts and role names.

    Read-only, and deliberately so. Its job is to answer "which project, branch
    and role is behind this DSN host?" before anyone resets a password — the
    hostname in a connection string does not name its project, and guessing
    wrong rotates a credential some other service is still using.

    Role names only. Neon returns passwords from a separate endpoint that this
    script never calls.
    """
    projects = nc.list_projects()
    print(f"{len(projects)} project(s)\n")

    for p in sorted(projects, key=lambda x: x.get("name", "")):
        pid = p.get("id", "?")
        print(f"{p.get('name', '?')}   [{pid}]   {p.get('region_id', '?')}")

        endpoints = nc.list_endpoints(pid)
        by_branch: Dict[str, List[str]] = {}
        for e in endpoints:
            by_branch.setdefault(e.get("branch_id", ""), []).append(
                f"{e.get('host', '?')} ({e.get('type', '?')})"
            )

        for b in nc.list_branches(pid):
            bid = b.get("id", "?")
            flag = " default" if b.get("default") else ""
            print(f"    branch {b.get('name', '?')}{flag}   [{bid}]")
            for host in by_branch.get(bid, []):
                print(f"      host  {host}")
            try:
                roles = [r.get("name", "?") for r in nc.list_roles(pid, bid)]
            except Exception as exc:
                roles = [f"<unreadable: {type(exc).__name__}>"]
            if roles:
                print(f"      roles {', '.join(roles)}")
        print()

    print("Match a DSN to a project by its host, then rotate that project's role.\n"
          "Resetting a role password invalidates every connection string using it,\n"
          "everywhere — Vercel, Render, Actions secrets and any local .env alike.")
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Inspect Neon projects and audit this repo's DSN env vars.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    ap.add_argument("--check-dsn", action="store_true",
                    help="audit DSN env vars locally (no API key, no network)")
    ap.add_argument("--list-projects", action="store_true",
                    help="list Neon projects")
    ap.add_argument("--inventory", action="store_true",
                    help="map every project to its endpoint hosts and role "
                         "names, to identify which one backs a given DSN "
                         "(read-only)")
    ap.add_argument("--project", help="Neon project id")
    ap.add_argument("--list-branches", action="store_true",
                    help="list branches in --project")
    args = ap.parse_args()

    if args.check_dsn:
        return cmd_check_dsn()

    if not (args.list_projects or args.list_branches or args.inventory):
        ap.print_help()
        return 0

    key = os.environ.get("NEON_API_KEY")
    if not key:
        die("NEON_API_KEY is not set.\n"
            "  Create one at https://console.neon.tech/app/settings/api-keys\n"
            "  then: export NEON_API_KEY=...\n"
            "  (--check-dsn needs no key and works offline)")

    nc = Neon(key)

    if args.inventory:
        return cmd_inventory(nc)

    if args.list_projects:
        return cmd_list_projects(nc)

    if not args.project:
        die("--project is required with --list-branches")
    return cmd_list_branches(nc, args.project)


if __name__ == "__main__":
    sys.exit(main())
