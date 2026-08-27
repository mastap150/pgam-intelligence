#!/usr/bin/env python3
"""
check_integrations.py — session capability preflight.

Answers "what can this session actually reach?" without spending a single API
call. Prints credential *presence* only — never a value.

Run this instead of guessing, and before ever asking a human to open a
dashboard. See .claude/INTEGRATIONS.md and the External Platform Access Policy
in CLAUDE.md.

    python3 scripts/check_integrations.py            # capability summary
    python3 scripts/check_integrations.py --platform hubspot
    python3 scripts/check_integrations.py --json
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent

# Platform -> (env var names, repo module, MCP ToolSearch query)
PLATFORMS: dict[str, dict] = {
    "github":     {"env": ["GH_TOKEN", "GITHUB_TOKEN"],
                   "module": None, "mcp": "github"},
    "vercel":     {"env": ["VERCEL_TOKEN", "VERCEL_TEAM_ID"],
                   "module": "scripts/vercel_env_sync.py", "mcp": "vercel"},
    "hubspot":    {"env": ["HUBSPOT_ACCESS_TOKEN", "HUBSPOT_PIPELINE_ID"],
                   "module": "agents/outbound/sdr_agent.py", "mcp": "hubspot"},
    "monday":     {"env": ["MONDAY_API_TOKEN"],
                   "module": "scripts/monday_cli.py", "mcp": "monday"},
    "apollo":     {"env": ["APOLLO_API_KEY"],
                   "module": "agents/outbound/sdr_agent.py", "mcp": "apollo"},
    "instantly":  {"env": ["INSTANTLY_API_KEY"],
                   "module": "agents/outbound/instantly_setup.py", "mcp": None},
    "neon":       {"env": ["PGAM_DIRECT_DATABASE_URL", "DSP_DATABASE_URL",
                           "DATABASE_URL", "FINANCE_DATABASE_URL",
                           "BOXINGNEWS_DATABASE_URL"],
                   "module": "core/neon.py", "mcp": None},
    "ll":         {"env": ["LL_API_BASE_URL", "LL_CLIENT_KEY", "LL_SECRET_KEY",
                           "LL_UI_EMAIL", "LL_UI_PASSWORD"],
                   "module": "core/api.py", "mcp": None},
    "tb":         {"env": ["TB_API_BASE_URL", "TB_CLIENT_KEY", "TB_SECRET_KEY",
                           "TB_ACCESS_TOKEN", "TB_EMAIL", "TB_PASSWORD"],
                   "module": "core/tb_api.py", "mcp": None},
    "pubmatic":   {"env": ["PUBMATIC_ACTIVATE_CLIENT_ID",
                           "PUBMATIC_ACTIVATE_CLIENT_SECRET",
                           "PUBMATIC_ACTIVATE_TOKEN",
                           "PUBMATIC_ACTIVATE_REFRESH_TOKEN",
                           "PUBMATIC_ACTIVATE_PUBTOKEN",
                           "PUBMATIC_ACTIVATE_ORG_ID"],
                   "module": "core/pubmatic_activate.py", "mcp": None},
    "msn":        {"env": ["MSN_EMAIL", "MSN_PASSWORD", "MSN_SESSION_DIR"],
                   "module": "core/msn_partner_hub.py", "mcp": None},
    "slack":      {"env": ["SLACK_WEBHOOK", "COMPLIANCE_SLACK_WEBHOOK",
                           "MSN_SLACK_WEBHOOK"],
                   "module": "core/slack.py", "mcp": None},
    "sendgrid":   {"env": ["SENDGRID_KEY", "EMAIL_FROM", "EMAIL_TO"],
                   "module": "core/config.py", "mcp": None},
    "google":     {"env": ["GOOGLE_CREDENTIALS_JSON", "GOOGLE_TOKEN_PICKLE_B64"],
                   "module": "reports/daily_report_sync.py", "mcp": None},
    "wordpress":  {"env": ["WP_SITE_URL", "WP_USERNAME", "WP_APP_PASS"],
                   "module": "healthnation-automation/wordpress_publisher.py",
                   "mcp": None},
    "springserve": {"env": ["SPRINGSERVE_BASE_URL", "SPRINGSERVE_EMAIL",
                            "SPRINGSERVE_PASSWORD"],
                    "module": None, "mcp": None},
    "pgam_internal": {"env": ["PGAM_DASHBOARD_BASE",
                              "PGAM_DASHBOARD_SERVICE_TOKEN",
                              "DSP_DASHBOARD_URL", "CRON_SECRET",
                              "DSP_CRON_SECRET"],
                      "module": None, "mcp": None},
    "anthropic":  {"env": ["ANTHROPIC_API_KEY"],
                   "module": "core/config.py", "mcp": None},
    # MCP-only platforms — no repo client, no env credential by design.
    "gmail":      {"env": [], "module": None, "mcp": "gmail"},
    "meta":       {"env": [], "module": None, "mcp": "meta"},
    "quickbooks": {"env": [], "module": None, "mcp": "quickbooks"},
    "vibe":       {"env": [], "module": None, "mcp": "vibe"},
    "higgsfield": {"env": [], "module": None, "mcp": "higgsfield"},
}

# Connectors verified live from a cloud session on 2026-08-21.
MCP_VERIFIED = {
    "github": "mastap150 (PGAMDSP)",
    "vercel": "team_8j7qA4FwBXkobcMfdhJj1umB, 17 projects",
    "hubspot": "portal 21341543",
    "monday": "user 36157998, Pro",
}
MCP_CONNECTED_UNPROBED = ["apollo", "gmail", "meta", "quickbooks", "vibe",
                          "higgsfield"]

CLIS = ["gh", "vercel", "git", "psql", "node", "python3", "npx",
        "aws", "gcloud", "supabase", "wrangler", "stripe"]


def session_class() -> dict:
    is_cloud = os.environ.get("CLAUDE_CODE_REMOTE", "").lower() == "true"
    return {
        "class": "cloud" if is_cloud else "local",
        "has_dotenv": (REPO / ".env").exists(),
        "egress": "github-only (org policy 403s other hosts)" if is_cloud
                  else "open (verify)",
        "proxy": os.environ.get("HTTPS_PROXY", ""),
    }


def probe(name: str, spec: dict) -> dict:
    present = [k for k in spec["env"] if os.environ.get(k, "").strip()]
    missing = [k for k in spec["env"] if k not in present]
    module = spec.get("module")
    return {
        "env_present": present,          # names only, never values
        "env_missing": missing,
        "module": module if module and (REPO / module).exists() else None,
        "mcp_query": spec.get("mcp"),
        "mcp_status": ("verified: " + MCP_VERIFIED[spec["mcp"]])
                      if spec.get("mcp") in MCP_VERIFIED
                      else ("connected (unprobed)"
                            if spec.get("mcp") in MCP_CONNECTED_UNPROBED
                            else None),
    }


def route(name: str, info: dict, sess: dict) -> str:
    """The route this session should actually take for this platform."""
    if info["mcp_status"]:
        return f"MCP — ToolSearch '{info['mcp_query']}' ({info['mcp_status']})"
    if info["env_present"] and sess["class"] == "local":
        via = info["module"] or "direct API"
        return f"direct — {via} (creds present)"
    if info["env_present"]:
        return ("creds present but egress is GitHub-only in a cloud session — "
                "no working route")
    if sess["class"] == "cloud":
        return ("NO ROUTE in a cloud session: no credential and no MCP "
                "connector. State this precisely; do not guess.")
    return f"no credential — needs {', '.join(info['env_missing'][:3])}"


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--platform", help="report a single platform")
    ap.add_argument("--json", action="store_true", dest="as_json")
    args = ap.parse_args()

    sess = session_class()
    names = ([args.platform.lower()] if args.platform else list(PLATFORMS))
    unknown = [n for n in names if n not in PLATFORMS]
    if unknown:
        print(f"unknown platform(s): {', '.join(unknown)}\n"
              f"known: {', '.join(sorted(PLATFORMS))}", file=sys.stderr)
        return 2

    results = {n: probe(n, PLATFORMS[n]) for n in names}
    clis = {c: bool(shutil.which(c)) for c in CLIS}

    if args.as_json:
        print(json.dumps({"session": sess, "clis": clis,
                          "platforms": {n: {**r, "route": route(n, r, sess)}
                                        for n, r in results.items()}}, indent=2))
        return 0

    print(f"session: {sess['class']}  |  .env: "
          f"{'present' if sess['has_dotenv'] else 'ABSENT'}  |  "
          f"egress: {sess['egress']}")
    print(f"clis:    {' '.join(c for c, ok in clis.items() if ok) or '(none)'}")
    missing_clis = [c for c, ok in clis.items() if not ok]
    if missing_clis:
        print(f"absent:  {' '.join(missing_clis)}")
    print()
    print("REMINDER: MCP tools are deferred — ToolSearch loads them. A missing")
    print("tool name is NOT evidence of missing access. See CLAUDE.md.")
    print()

    width = max(len(n) for n in names)
    for n in names:
        print(f"  {n.ljust(width)}  {route(n, results[n], sess)}")
        creds = results[n]["env_present"]
        if creds:
            print(f"  {' '.ljust(width)}    creds set: {', '.join(creds)}")
    print()
    print("Full registry: .claude/INTEGRATIONS.md")
    return 0


if __name__ == "__main__":
    sys.exit(main())
