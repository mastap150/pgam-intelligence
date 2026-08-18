#!/usr/bin/env python3
"""Sync environment variables to a Vercel project via the REST API.

Exists so that a Claude Code session (local or cloud) can push env vars to
Vercel in one command instead of walking a human through the dashboard.

The Vercel MCP connector is read-only for env vars, so this uses the REST
API directly and needs a token:

    export VERCEL_TOKEN=...      # vercel.com/account/settings/tokens

Usage
-----
    # what's already set on a project (names + targets only, never values)
    python3 scripts/vercel_env_sync.py --project pgam-www --list

    # preview a sync without writing
    python3 scripts/vercel_env_sync.py --project pgam-www \
        --env-file ~/Desktop/pgam-www/.env.production --dry-run

    # push to production (upserts; existing keys are updated)
    python3 scripts/vercel_env_sync.py --project pgam-www \
        --env-file ~/Desktop/pgam-www/.env.production --target production

    # push only some keys, marked sensitive (write-only in the dashboard)
    python3 scripts/vercel_env_sync.py --project pgam-direct-web \
        --env-file .env --only DATABASE_URL,STRIPE_SECRET_KEY --sensitive

Values are never printed or logged — only key names.

Env vars still take effect only on the NEXT build, so redeploy afterwards.
"""

from __future__ import annotations

import argparse
import os
import sys
from typing import Dict, List

import requests

API = "https://api.vercel.com"

# PGAM's Vercel team ("ppatel-6748's projects"). Override with --team or
# VERCEL_TEAM_ID; pass --team "" to operate on a personal (teamless) scope.
DEFAULT_TEAM_ID = "team_8j7qA4FwBXkobcMfdhJj1umB"

VALID_TARGETS = ("production", "preview", "development")


def die(msg: str) -> None:
    print(f"error: {msg}", file=sys.stderr)
    sys.exit(1)


def parse_env_file(path: str) -> Dict[str, str]:
    """Parse a .env-style file into {key: value}.

    Skips comments, blank lines, and keys with empty values (an empty value
    in a .env.example is a placeholder, not something to push).
    """
    if not os.path.isfile(path):
        die(f"env file not found: {path}")

    out: Dict[str, str] = {}
    skipped_empty: List[str] = []

    with open(path, encoding="utf-8") as fh:
        for lineno, raw in enumerate(fh, 1):
            line = raw.strip()
            if not line or line.startswith("#"):
                continue
            if line.startswith("export "):
                line = line[len("export "):].lstrip()
            if "=" not in line:
                print(f"  warn: {path}:{lineno} has no '=', skipping")
                continue

            key, _, value = line.partition("=")
            key = key.strip()
            value = value.strip()

            # Strip one layer of matching quotes.
            if len(value) >= 2 and value[0] == value[-1] and value[0] in ("'", '"'):
                value = value[1:-1]

            if not key:
                continue
            if not value:
                skipped_empty.append(key)
                continue
            out[key] = value

    if skipped_empty:
        print(f"  skipped {len(skipped_empty)} key(s) with empty values: "
              f"{', '.join(sorted(skipped_empty))}")

    return out


class Vercel:
    def __init__(self, token: str, team_id: str | None):
        self.session = requests.Session()
        self.session.headers.update({
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        })
        self.params = {"teamId": team_id} if team_id else {}

    def _request(self, method: str, path: str, **kw) -> requests.Response:
        params = dict(self.params)
        params.update(kw.pop("params", {}))
        resp = self.session.request(
            method, f"{API}{path}", params=params, timeout=30, **kw
        )
        if resp.status_code == 403:
            die("403 from Vercel. The token is valid but lacks scope for this "
                "team/project, or the team id is wrong. Check --team.")
        if resp.status_code == 401:
            die("401 from Vercel. VERCEL_TOKEN is missing, expired, or revoked.")
        return resp

    def resolve_project(self, name_or_id: str) -> dict:
        resp = self._request("GET", f"/v9/projects/{name_or_id}")
        if resp.status_code == 404:
            die(f"project not found: {name_or_id!r}. Run with --list-projects "
                f"to see what this token can reach.")
        resp.raise_for_status()
        return resp.json()

    def list_projects(self) -> List[dict]:
        resp = self._request("GET", "/v9/projects", params={"limit": "100"})
        resp.raise_for_status()
        return resp.json().get("projects", [])

    def list_env(self, project_id: str) -> List[dict]:
        resp = self._request("GET", f"/v9/projects/{project_id}/env")
        resp.raise_for_status()
        return resp.json().get("envs", [])

    def upsert_env(self, project_id: str, payload: List[dict]) -> dict:
        resp = self._request(
            "POST",
            f"/v10/projects/{project_id}/env",
            params={"upsert": "true"},
            json=payload,
        )
        if resp.status_code >= 400:
            die(f"{resp.status_code} from Vercel: {resp.text[:500]}")
        return resp.json()


def cmd_list_projects(vc: Vercel) -> int:
    projects = vc.list_projects()
    print(f"{len(projects)} project(s):")
    for p in sorted(projects, key=lambda x: x["name"]):
        print(f"  {p['name']:<32} {p['id']}")
    return 0


def cmd_list(vc: Vercel, project: dict) -> int:
    envs = vc.list_env(project["id"])
    if not envs:
        print(f"{project['name']}: no environment variables set")
        return 0

    print(f"{project['name']}: {len(envs)} variable(s)")
    print(f"  {'KEY':<40} {'TYPE':<10} TARGETS")
    for e in sorted(envs, key=lambda x: x.get("key", "")):
        targets = e.get("target") or []
        if isinstance(targets, str):
            targets = [targets]
        # Custom environments come back as ids, not names.
        if e.get("customEnvironmentIds"):
            targets = list(targets) + [
                f"custom:{cid}" for cid in e["customEnvironmentIds"]
            ]
        print(f"  {e.get('key', '?'):<40} {e.get('type', '?'):<10} "
              f"{','.join(targets) or '-'}")
    return 0


def cmd_sync(vc: Vercel, project: dict, args: argparse.Namespace) -> int:
    values = parse_env_file(args.env_file)

    if args.only:
        wanted = {k.strip() for k in args.only.split(",") if k.strip()}
        missing = wanted - values.keys()
        if missing:
            die(f"--only named key(s) not present (or empty) in "
                f"{args.env_file}: {', '.join(sorted(missing))}")
        values = {k: v for k, v in values.items() if k in wanted}

    if args.skip:
        unwanted = {k.strip() for k in args.skip.split(",") if k.strip()}
        values = {k: v for k, v in values.items() if k not in unwanted}

    if not values:
        die("nothing to sync — no keys with non-empty values matched")

    targets = [t.strip() for t in args.target.split(",") if t.strip()]
    for t in targets:
        if t not in VALID_TARGETS:
            die(f"invalid target {t!r}; expected one or more of "
                f"{', '.join(VALID_TARGETS)}")

    existing = {e.get("key") for e in vc.list_env(project["id"])}

    payload = []
    for key, value in sorted(values.items()):
        # NEXT_PUBLIC_* is inlined into the client bundle, so encrypting it
        # buys nothing and 'sensitive' would be actively misleading.
        if args.sensitive and not key.startswith("NEXT_PUBLIC_"):
            var_type = "sensitive"
        elif key.startswith("NEXT_PUBLIC_"):
            var_type = "plain"
        else:
            var_type = "encrypted"

        entry = {
            "key": key,
            "value": value,
            "type": var_type,
            "target": targets,
        }
        if args.comment:
            entry["comment"] = args.comment
        payload.append(entry)

    verb = "would sync" if args.dry_run else "syncing"
    print(f"\n{verb} {len(payload)} variable(s) -> {project['name']} "
          f"[{', '.join(targets)}]")
    for entry in payload:
        action = "update" if entry["key"] in existing else "create"
        print(f"  {action:<6} {entry['key']:<40} ({entry['type']})")

    if args.dry_run:
        print("\ndry run — nothing was written")
        return 0

    result = vc.upsert_env(project["id"], payload)

    created = result.get("created") or []
    if isinstance(created, dict):
        created = [created]
    failed = result.get("failed") or []

    print(f"\nwrote {len(created)} variable(s)")
    if failed:
        print(f"{len(failed)} failed:")
        for f in failed:
            err = f.get("error", {})
            print(f"  {err.get('key', '?')}: {err.get('message', f)}")

    print("\nNOTE: env vars are applied at BUILD time. Redeploy for these to "
          "take effect:")
    print(f"  vercel --prod   # or Deployments -> latest -> ... -> Redeploy")

    return 1 if failed else 0


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Sync environment variables to a Vercel project.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    ap.add_argument("--project", help="Vercel project name or prj_ id")
    ap.add_argument("--env-file", help=".env-style file to read values from")
    ap.add_argument("--target", default="production",
                    help="comma-separated: production,preview,development "
                         "(default: production)")
    ap.add_argument("--only", help="comma-separated keys to sync (exclusive)")
    ap.add_argument("--skip", help="comma-separated keys to exclude")
    ap.add_argument("--sensitive", action="store_true",
                    help="store as write-only 'sensitive' vars (not "
                         "retrievable afterwards; NEXT_PUBLIC_* is exempt)")
    ap.add_argument("--comment", help="comment attached to each variable")
    ap.add_argument("--list", action="store_true",
                    help="list a project's variables (names only) and exit")
    ap.add_argument("--list-projects", action="store_true",
                    help="list reachable projects and exit")
    ap.add_argument("--dry-run", action="store_true",
                    help="show what would change, write nothing")
    ap.add_argument("--team", default=None,
                    help=f"team id (default: $VERCEL_TEAM_ID or "
                         f"{DEFAULT_TEAM_ID}); pass '' for personal scope")
    args = ap.parse_args()

    token = os.environ.get("VERCEL_TOKEN")
    if not token:
        die("VERCEL_TOKEN is not set.\n"
            "  Create one at https://vercel.com/account/settings/tokens\n"
            "  then: export VERCEL_TOKEN=...")

    team_id = args.team if args.team is not None else os.environ.get(
        "VERCEL_TEAM_ID", DEFAULT_TEAM_ID
    )
    vc = Vercel(token, team_id or None)

    if args.list_projects:
        return cmd_list_projects(vc)

    if not args.project:
        die("--project is required (or use --list-projects)")

    project = vc.resolve_project(args.project)

    if args.list:
        return cmd_list(vc, project)

    if not args.env_file:
        die("--env-file is required to sync (or use --list / --list-projects)")

    return cmd_sync(vc, project, args)


if __name__ == "__main__":
    sys.exit(main())
