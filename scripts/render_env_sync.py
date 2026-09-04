#!/usr/bin/env python3
"""Sync and audit environment variables on a Render service via the REST API.

The companion to scripts/vercel_env_sync.py, for the other half of the estate:
pgam-intelligence deploys as a Render worker, and render.yaml declares 14 keys
with `sync: false`, meaning every one is typed into the dashboard by hand. This
turns that into one command, and adds a --check that tells you when the
dashboard and render.yaml have drifted apart.

Needs a token:

    export RENDER_API_KEY=...   # dashboard -> Account Settings -> API Keys

Usage
-----
    # what services this key can reach
    python3 scripts/render_env_sync.py --list-services

    # what's set on the scheduler (names only, never values)
    python3 scripts/render_env_sync.py --service pgam-intelligence-scheduler --list

    # does the dashboard match what render.yaml declares?
    python3 scripts/render_env_sync.py --service pgam-intelligence-scheduler --check

    # preview a sync, then run it
    python3 scripts/render_env_sync.py --service pgam-intelligence-scheduler \
        --env-file .env --dry-run
    python3 scripts/render_env_sync.py --service pgam-intelligence-scheduler \
        --env-file .env --only TBX_EMAIL,TBX_PASSWORD

Values are never printed or logged — only key names.

Two deliberate differences from the Vercel script
-------------------------------------------------
1. Writes go one key at a time (PUT /env-vars/{key}), never through Render's
   bulk PUT /env-vars. The bulk endpoint REPLACES the whole set, so any key
   absent from the payload is deleted. Syncing a partial .env through it would
   silently wipe the rest of the service's config — the same whole-object
   replacement footgun documented for the TBX update endpoints in
   core/tbx_mgmt.py.

2. There is no --prune. Removing a variable is a destructive, one-way action;
   do it in the dashboard where you can see what you are deleting.

Restart timing matters here
---------------------------
Vercel bakes env vars in at build time, so a sync there is inert until you
redeploy. Render is the opposite: changing a variable restarts the service.
render.yaml records that scheduler restart loops silently dropped the 07:45 and
08:00 ET compliance cron windows on 2026-06-08. So this script warns when you
sync inside that window. Syncing is still fine — just know a restart is landing.
"""

from __future__ import annotations

import argparse
import datetime as _dt
import os
import re
import sys
from typing import Dict, List, Tuple

import requests

API = "https://api.render.com/v1"

# render.yaml's own comment: the scheduler runs in ET and the compliance jobs
# fire at 07:45 / 08:00. Restarting across that window is how the 2026-06-08
# incident dropped them.
RESTART_SENSITIVE_ET = (_dt.time(7, 30), _dt.time(8, 15))


def die(msg: str) -> None:
    print(f"error: {msg}", file=sys.stderr)
    sys.exit(1)


def parse_env_file(path: str) -> Dict[str, str]:
    """Parse a .env-style file into {key: value}.

    Skips comments, blank lines, and keys with empty values (an empty value in
    a .env.example is a placeholder, not something to push).
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


def parse_render_yaml(path: str) -> Tuple[Dict[str, str], List[str]]:
    """Extract the envVars block from render.yaml.

    Returns ({key: literal_value}, [keys declared sync:false]). Hand-rolled
    rather than via PyYAML, which is only a transitive dependency here and is
    not pinned in requirements.txt.
    """
    if not os.path.isfile(path):
        die(f"render.yaml not found: {path}")

    literals: Dict[str, str] = {}
    manual: List[str] = []
    current: str | None = None

    key_re = re.compile(r"^\s*-\s+key:\s*(\S+)\s*$")
    val_re = re.compile(r"^\s*value:\s*(.*?)\s*$")
    sync_re = re.compile(r"^\s*sync:\s*false\s*$")

    with open(path, encoding="utf-8") as fh:
        for raw in fh:
            line = raw.split("#", 1)[0] if not raw.lstrip().startswith("#") else ""
            if m := key_re.match(line):
                current = m.group(1)
                continue
            if current is None:
                continue
            if m := val_re.match(line):
                v = m.group(1)
                if len(v) >= 2 and v[0] == v[-1] and v[0] in ("'", '"'):
                    v = v[1:-1]
                literals[current] = v
                current = None
            elif sync_re.match(line):
                manual.append(current)
                current = None

    return literals, manual


class Render:
    def __init__(self, token: str):
        self.session = requests.Session()
        self.session.headers.update({
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
            "Content-Type": "application/json",
        })

    def _request(self, method: str, path: str, **kw) -> requests.Response:
        resp = self.session.request(method, f"{API}{path}", timeout=30, **kw)
        if resp.status_code == 401:
            die("401 from Render. RENDER_API_KEY is missing, expired, or revoked.")
        if resp.status_code == 403:
            die("403 from Render. The key is valid but lacks access to this "
                "service — check it belongs to the right workspace.")
        if resp.status_code == 429:
            die("429 from Render. Rate limited; retry in a minute.")
        return resp

    def _paginate(self, path: str, unwrap: str) -> List[dict]:
        """Walk Render's cursor pagination, which wraps each row as
        {"cursor": ..., "<unwrap>": {...}}."""
        out: List[dict] = []
        cursor: str | None = None
        while True:
            params = {"limit": "100"}
            if cursor:
                params["cursor"] = cursor
            resp = self._request("GET", path, params=params)
            resp.raise_for_status()
            rows = resp.json()
            if not rows:
                break
            for row in rows:
                item = row.get(unwrap) if isinstance(row, dict) else None
                if item is not None:
                    out.append(item)
            cursor = rows[-1].get("cursor") if isinstance(rows[-1], dict) else None
            if not cursor or len(rows) < 100:
                break
        return out

    def list_services(self) -> List[dict]:
        return self._paginate("/services", "service")

    def resolve_service(self, name_or_id: str) -> dict:
        if name_or_id.startswith(("srv-", "srv_", "crn-", "cron-")):
            resp = self._request("GET", f"/services/{name_or_id}")
            if resp.status_code == 404:
                die(f"service not found: {name_or_id!r}")
            resp.raise_for_status()
            return resp.json()

        matches = [s for s in self.list_services() if s.get("name") == name_or_id]
        if not matches:
            die(f"service not found: {name_or_id!r}. Run --list-services to see "
                f"what this key can reach.")
        if len(matches) > 1:
            die(f"{len(matches)} services named {name_or_id!r}; pass the srv- id "
                f"instead.")
        return matches[0]

    def list_env(self, service_id: str) -> Dict[str, str]:
        rows = self._paginate(f"/services/{service_id}/env-vars", "envVar")
        return {r["key"]: r.get("value", "") for r in rows if r.get("key")}

    def put_env(self, service_id: str, key: str, value: str) -> None:
        """Upsert ONE variable. Deliberately not the bulk PUT — see module docstring."""
        resp = self._request(
            "PUT", f"/services/{service_id}/env-vars/{key}", json={"value": value}
        )
        if resp.status_code >= 400:
            die(f"{resp.status_code} from Render setting {key}: {resp.text[:300]}")


def warn_if_restart_sensitive() -> None:
    try:
        import zoneinfo
        now = _dt.datetime.now(zoneinfo.ZoneInfo("America/New_York"))
    except Exception:
        return
    lo, hi = RESTART_SENSITIVE_ET
    if lo <= now.time() <= hi:
        print(f"\n  WARNING: it is {now:%H:%M} ET. Changing env vars restarts the "
              f"service,\n  and the 07:45 / 08:00 ET compliance windows are in "
              f"progress. render.yaml\n  records those being silently dropped by a "
              f"restart loop on 2026-06-08.\n  Consider waiting until after 08:15 ET.")


def cmd_list_services(rd: Render) -> int:
    services = rd.list_services()
    print(f"{len(services)} service(s):")
    for s in sorted(services, key=lambda x: x.get("name", "")):
        print(f"  {s.get('name', '?'):<38} {s.get('type', '?'):<12} {s.get('id', '?')}")
    return 0


def cmd_list(rd: Render, service: dict) -> int:
    env = rd.list_env(service["id"])
    if not env:
        print(f"{service['name']}: no environment variables set")
        return 0
    print(f"{service['name']}: {len(env)} variable(s)")
    for key in sorted(env):
        print(f"  {key}")
    return 0


def cmd_check(rd: Render, service: dict, yaml_path: str) -> int:
    """Diff render.yaml's declarations against what the dashboard actually has."""
    literals, manual = parse_render_yaml(yaml_path)
    actual = rd.list_env(service["id"])

    declared = set(literals) | set(manual)
    missing = sorted(k for k in manual if k not in actual)
    undeclared = sorted(k for k in actual if k not in declared)
    overridden = sorted(
        k for k, v in literals.items() if k in actual and actual[k] != v
    )

    print(f"\n{service['name']} vs {yaml_path}")
    print(f"  {len(declared)} declared, {len(actual)} set on the service")

    if missing:
        print(f"\n  MISSING — declared sync:false but not set ({len(missing)}):")
        for k in missing:
            print(f"    {k}")
        print("    These are why an agent fails at its first API call with no token.")

    if overridden:
        print(f"\n  OVERRIDDEN — render.yaml pins a literal, dashboard differs "
              f"({len(overridden)}):")
        for k in overridden:
            print(f"    {k}   (render.yaml: {literals[k]!r}, dashboard: <differs>)")
        print("    Expected for deliberate flips; check none is an accident.")

    if undeclared:
        print(f"\n  UNDECLARED — set on the service, absent from render.yaml "
              f"({len(undeclared)}):")
        for k in undeclared:
            print(f"    {k}")
        print("    Add to render.yaml so a rebuilt service keeps them.")

    if not (missing or overridden or undeclared):
        print("\n  in sync — every declared key is set, nothing undeclared")
        return 0

    return 1 if missing else 0


def cmd_sync(rd: Render, service: dict, args: argparse.Namespace) -> int:
    values = parse_env_file(args.env_file)

    if args.only:
        wanted = {k.strip() for k in args.only.split(",") if k.strip()}
        absent = wanted - values.keys()
        if absent:
            die(f"--only named key(s) not present (or empty) in "
                f"{args.env_file}: {', '.join(sorted(absent))}")
        values = {k: v for k, v in values.items() if k in wanted}

    if args.skip:
        unwanted = {k.strip() for k in args.skip.split(",") if k.strip()}
        values = {k: v for k, v in values.items() if k not in unwanted}

    if not values:
        die("nothing to sync — no keys with non-empty values matched")

    existing = rd.list_env(service["id"])

    verb = "would sync" if args.dry_run else "syncing"
    print(f"\n{verb} {len(values)} variable(s) -> {service['name']}")
    unchanged = 0
    todo: Dict[str, str] = {}
    for key, value in sorted(values.items()):
        if key in existing and existing[key] == value:
            unchanged += 1
            continue
        print(f"  {'update' if key in existing else 'create':<6} {key}")
        todo[key] = value

    if unchanged:
        print(f"  ({unchanged} already identical, skipped)")

    if not todo:
        print("\nnothing to do — the service already matches")
        return 0

    if args.dry_run:
        print("\ndry run — nothing was written")
        return 0

    warn_if_restart_sensitive()

    for key, value in sorted(todo.items()):
        rd.put_env(service["id"], key, value)

    print(f"\nwrote {len(todo)} variable(s)")
    print("\nNOTE: Render restarts the service on an env var change — unlike "
          "Vercel,\nno separate redeploy is needed, but the worker does go down "
          "briefly.")
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Sync and audit environment variables on a Render service.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    ap.add_argument("--service", help="Render service name or srv- id")
    ap.add_argument("--env-file", help=".env-style file to read values from")
    ap.add_argument("--only", help="comma-separated keys to sync (exclusive)")
    ap.add_argument("--skip", help="comma-separated keys to exclude")
    ap.add_argument("--list", action="store_true",
                    help="list a service's variables (names only) and exit")
    ap.add_argument("--list-services", action="store_true",
                    help="list reachable services and exit")
    ap.add_argument("--check", action="store_true",
                    help="diff the service against render.yaml and exit "
                         "(exit 1 if a sync:false key is unset)")
    ap.add_argument("--render-yaml", default="render.yaml",
                    help="path to render.yaml for --check (default: render.yaml)")
    ap.add_argument("--dry-run", action="store_true",
                    help="show what would change, write nothing")
    args = ap.parse_args()

    token = os.environ.get("RENDER_API_KEY")
    if not token:
        die("RENDER_API_KEY is not set.\n"
            "  Create one at https://dashboard.render.com/u/settings#api-keys\n"
            "  then: export RENDER_API_KEY=...")

    rd = Render(token)

    if args.list_services:
        return cmd_list_services(rd)

    if not args.service:
        die("--service is required (or use --list-services)")

    service = rd.resolve_service(args.service)

    if args.list:
        return cmd_list(rd, service)

    if args.check:
        return cmd_check(rd, service, args.render_yaml)

    if not args.env_file:
        die("--env-file is required to sync (or use --check / --list / "
            "--list-services)")

    return cmd_sync(rd, service, args)


if __name__ == "__main__":
    sys.exit(main())
