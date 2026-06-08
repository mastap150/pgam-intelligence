"""
agents/outbound/instantly_setup.py
──────────────────────────────────
One-time CLI to wire PGAM's existing Instantly inboxes into the
Jordan-Reilly outbound persona.

WHY THIS EXISTS
───────────────
Priyesh's Instantly tenant already has 37 warmed inboxes. Manually
renaming each one + creating two new campaigns + attaching inboxes
+ pasting in 4-touch sequences is ~2 hours of click-work. This
script does it via the v2 API in ~30 seconds.

SUBCOMMANDS
───────────
    inspect
        Read-only. Lists every account/inbox in the tenant with:
            email, sender display name, warmup status, daily limit,
            today's sent count, currently-attached campaign IDs.
        Use this BEFORE setup to choose the subset.

    setup --limit N [--dry-run]
        Picks the healthiest N inboxes (warmup green, lowest send
        today). Renames them so the From line displays as
        "Jordan Reilly". Creates two PAUSED campaigns
        (brand_awareness, performance), loads the sequences from
        templates.py, and attaches the chosen inboxes — split
        evenly across both campaigns.

        --dry-run prints the plan without writing anything.

        Campaigns ship paused. You start them from Instantly UI
        once you've eyeballed the sequence rendering.

USAGE
─────
    INSTANTLY_API_KEY=... python -m agents.outbound.instantly_setup inspect
    INSTANTLY_API_KEY=... python -m agents.outbound.instantly_setup setup --limit 8 --dry-run
    INSTANTLY_API_KEY=... python -m agents.outbound.instantly_setup setup --limit 8

API VERSION
───────────
Targets Instantly API v2 (api.instantly.ai/api/v2). Auth via Bearer
token. If Instantly tweaks payload shapes between releases this
script will surface the error message + HTTP status; the inspection
command always works against any v2 release.

SAFETY POSTURE
──────────────
- Inspect is read-only.
- Setup --dry-run writes nothing.
- Setup live mode creates campaigns PAUSED. Inbox renames are the
  only "live" mutation, and they're idempotent (rerun = no-op if
  already named Jordan Reilly).
- The set of inboxes touched is logged so you can revert by reading
  the log and PATCHing back to the original names.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime
from typing import Any

from agents.outbound.templates import BRAND_AWARENESS_SEQUENCE, PERFORMANCE_SEQUENCE


INSTANTLY_BASE = "https://api.instantly.ai/api/v2"


# ─────────────────────────────────────────────────────────────────────
# HTTP
# ─────────────────────────────────────────────────────────────────────
def _api_key() -> str:
    key = os.environ.get("INSTANTLY_API_KEY", "").strip()
    if not key:
        print("ERROR: INSTANTLY_API_KEY env var is not set.", file=sys.stderr)
        sys.exit(2)
    return key


def _request(
    method: str,
    path: str,
    *,
    body: dict[str, Any] | None = None,
    query: dict[str, Any] | None = None,
    timeout: int = 30,
) -> tuple[int, Any]:
    url = INSTANTLY_BASE + path
    if query:
        from urllib.parse import urlencode
        url += "?" + urlencode({k: v for k, v in query.items() if v is not None})
    data = json.dumps(body).encode("utf-8") if body is not None else None
    req = urllib.request.Request(url, data=data, method=method)
    req.add_header("Authorization", f"Bearer {_api_key()}")
    req.add_header("Content-Type", "application/json")
    req.add_header("Accept", "application/json")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read().decode("utf-8")
            return resp.status, (json.loads(raw) if raw else None)
    except urllib.error.HTTPError as e:
        raw = e.read().decode("utf-8", errors="replace")
        try:
            return e.code, json.loads(raw) if raw else None
        except json.JSONDecodeError:
            return e.code, {"_raw": raw}
    except urllib.error.URLError as e:
        return 0, {"_error": str(e)}


# ─────────────────────────────────────────────────────────────────────
# Accounts (inboxes)
# ─────────────────────────────────────────────────────────────────────
def list_accounts() -> list[dict[str, Any]]:
    """Paginate through all Instantly email accounts."""
    out: list[dict[str, Any]] = []
    starting_after: str | None = None
    while True:
        status, payload = _request(
            "GET",
            "/accounts",
            query={"limit": 100, "starting_after": starting_after},
        )
        if status != 200 or not isinstance(payload, dict):
            print(f"[instantly_setup] list_accounts failed: {status} {payload}",
                  file=sys.stderr)
            break
        items = payload.get("items") or payload.get("data") or []
        out.extend(items)
        # v2 cursor pagination
        next_cursor = payload.get("next_starting_after") or payload.get("next_cursor")
        if not next_cursor or len(items) == 0:
            break
        starting_after = next_cursor
    return out


def _account_label(acc: dict[str, Any]) -> str:
    """Best-effort sender display name from an Instantly account record."""
    first = (acc.get("first_name") or "").strip()
    last = (acc.get("last_name") or "").strip()
    if first or last:
        return f"{first} {last}".strip()
    return acc.get("email") or "<no-name>"


def _account_warmup_status(acc: dict[str, Any]) -> str:
    """Try a handful of plausible field paths — Instantly's payload
    shape varies between API versions and account types."""
    for key in ("warmup_status", "warmup", "warmup_state"):
        val = acc.get(key)
        if isinstance(val, str):
            return val
        if isinstance(val, dict):
            inner = val.get("status") or val.get("state")
            if inner:
                return str(inner)
    return "?"


def _account_daily_limit(acc: dict[str, Any]) -> int:
    for key in ("daily_limit", "send_limit", "max_daily_sends"):
        v = acc.get(key)
        if isinstance(v, (int, float)):
            return int(v)
    return 0


def _account_sent_today(acc: dict[str, Any]) -> int:
    for key in ("sent_today", "today_sent", "emails_sent_today"):
        v = acc.get(key)
        if isinstance(v, (int, float)):
            return int(v)
    return 0


def rename_account(email: str, first_name: str, last_name: str) -> bool:
    status, payload = _request(
        "PATCH",
        f"/accounts/{email}",
        body={"first_name": first_name, "last_name": last_name},
    )
    if status not in (200, 204):
        print(f"[instantly_setup] rename {email} failed: {status} {payload}",
              file=sys.stderr)
        return False
    return True


# ─────────────────────────────────────────────────────────────────────
# Campaigns
# ─────────────────────────────────────────────────────────────────────
def list_campaigns() -> list[dict[str, Any]]:
    status, payload = _request("GET", "/campaigns", query={"limit": 100})
    if status != 200 or not isinstance(payload, dict):
        return []
    return payload.get("items") or payload.get("data") or []


def _build_sequence_steps(sequence: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Convert templates.py sequence → Instantly v2 sequence steps shape."""
    steps = []
    for s in sequence:
        steps.append(
            {
                "type": "email",
                "delay": int(s["day_offset"]),  # days from prior step / start
                "variants": [
                    {
                        "subject": s["subject_options"][0],
                        "body": s["body"],
                    }
                ],
            }
        )
    return steps


def _campaign_payload(
    name: str,
    inbox_emails: list[str],
    sequence: list[dict[str, Any]],
) -> dict[str, Any]:
    return {
        "name": name,
        "status": "paused",  # ALWAYS ship paused; user starts from UI
        "email_list": inbox_emails,
        "sequences": [{"steps": _build_sequence_steps(sequence)}],
        "campaign_schedule": {
            "schedules": [
                {
                    "name": "US business hours",
                    "timing": {"from": "09:00", "to": "17:00"},
                    "days": {
                        "1": True,  # Mon
                        "2": True,
                        "3": True,
                        "4": True,
                        "5": True,  # Fri
                        "6": False,
                        "0": False,
                    },
                    "timezone": "America/New_York",
                }
            ]
        },
        "daily_limit": 30,  # per-inbox cap; Instantly aggregates across inboxes
        "stop_on_reply": True,
        "stop_on_auto_reply": True,
        "track_opens": False,
        "track_clicks": False,
        "text_only": True,
    }


def create_campaign(
    name: str, inbox_emails: list[str], sequence: list[dict[str, Any]]
) -> str | None:
    body = _campaign_payload(name, inbox_emails, sequence)
    status, payload = _request("POST", "/campaigns", body=body)
    if status not in (200, 201) or not isinstance(payload, dict):
        print(f"[instantly_setup] create_campaign {name!r} failed: {status} {payload}",
              file=sys.stderr)
        return None
    return payload.get("id") or payload.get("campaign_id")


# ─────────────────────────────────────────────────────────────────────
# CLI: inspect
# ─────────────────────────────────────────────────────────────────────
def cmd_inspect() -> int:
    accounts = list_accounts()
    if not accounts:
        print("No accounts returned. Either the API key is wrong or the tenant is empty.")
        return 1

    # Sort: green warmup first, then by lowest sent_today
    def _sort_key(a: dict[str, Any]) -> tuple:
        warm = _account_warmup_status(a).lower()
        warm_rank = 0 if "active" in warm or "complete" in warm or "green" in warm else 1
        return (warm_rank, _account_sent_today(a), _account_label(a))

    accounts.sort(key=_sort_key)

    print(f"\n{len(accounts)} Instantly accounts in tenant:\n")
    print(f"{'#':<3}  {'email':<40}  {'sender':<22}  {'warmup':<12}  "
          f"{'limit':>6}  {'sent_today':>10}")
    print("-" * 110)
    for i, a in enumerate(accounts, 1):
        print(
            f"{i:<3}  "
            f"{(a.get('email') or '')[:40]:<40}  "
            f"{_account_label(a)[:22]:<22}  "
            f"{_account_warmup_status(a)[:12]:<12}  "
            f"{_account_daily_limit(a):>6}  "
            f"{_account_sent_today(a):>10}"
        )
    print()
    return 0


# ─────────────────────────────────────────────────────────────────────
# CLI: setup
# ─────────────────────────────────────────────────────────────────────
def cmd_setup(limit: int, dry_run: bool) -> int:
    accounts = list_accounts()
    if not accounts:
        print("No accounts. Aborting.", file=sys.stderr)
        return 1

    def _healthy_first(a: dict[str, Any]) -> tuple:
        warm = _account_warmup_status(a).lower()
        warm_rank = 0 if ("active" in warm or "complete" in warm or "green" in warm) else 1
        return (warm_rank, _account_sent_today(a))

    accounts.sort(key=_healthy_first)
    chosen = accounts[:limit]
    chosen_emails = [a["email"] for a in chosen if a.get("email")]

    print(f"\nSelected {len(chosen_emails)} inboxes for Jordan Reilly persona:\n")
    for a in chosen:
        print(
            f"  • {a.get('email')}  "
            f"(was: {_account_label(a)!r}, warmup: {_account_warmup_status(a)})"
        )

    # Split evenly across the two campaigns. Healthier inboxes go first
    # into Brand Awareness on the theory that brand teams reply slower
    # but with higher LTV.
    half = max(1, len(chosen_emails) // 2)
    brand_emails = chosen_emails[:half]
    perf_emails = chosen_emails[half:]
    if not perf_emails:
        # If we only have 1 inbox, share it across both campaigns.
        perf_emails = brand_emails

    print(f"\nCampaign plan:")
    print(f"  brand_awareness → {len(brand_emails)} inboxes: {brand_emails}")
    print(f"  performance     → {len(perf_emails)} inboxes: {perf_emails}")

    if dry_run:
        print("\n[dry-run] No writes. Re-run without --dry-run to execute.")
        return 0

    # 1) Rename chosen inboxes to Jordan Reilly
    print("\nRenaming inboxes…")
    rename_log = []
    for a in chosen:
        email = a.get("email")
        if not email:
            continue
        prev_label = _account_label(a)
        if prev_label.strip().lower() == "jordan reilly":
            print(f"  ✓ {email} already 'Jordan Reilly' — skip")
            rename_log.append({"email": email, "previous": prev_label, "skipped": True})
            continue
        ok = rename_account(email, "Jordan", "Reilly")
        rename_log.append({"email": email, "previous": prev_label, "renamed": ok})
        print(f"  {'✓' if ok else '✗'} {email}  (was: {prev_label!r})")
        time.sleep(0.1)

    # Persist rename log for one-click rollback if Priyesh wants
    log_path = f"logs/instantly_renames_{datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')}.json"
    try:
        os.makedirs("logs", exist_ok=True)
        with open(log_path, "w") as f:
            json.dump(rename_log, f, indent=2)
        print(f"\nRename log → {log_path}")
    except OSError as e:
        print(f"  (could not write rename log: {e})")

    # 2) Create the two campaigns (paused)
    print("\nCreating campaigns (paused)…")
    brand_id = create_campaign(
        "PGAM DSP — Brand Awareness Outbound (Jordan Reilly)",
        brand_emails,
        BRAND_AWARENESS_SEQUENCE,
    )
    perf_id = create_campaign(
        "PGAM DSP — Performance / Call (Jordan Reilly)",
        perf_emails,
        PERFORMANCE_SEQUENCE,
    )
    print(f"  brand_awareness campaign id: {brand_id}")
    print(f"  performance campaign id:    {perf_id}")

    print("\nDone. Next steps:")
    print("  1. Open Instantly UI. Eyeball both campaigns. Tweak copy if needed.")
    print("  2. Drop these into .env:")
    if brand_id:
        print(f"       INSTANTLY_CAMPAIGN_BRAND_AWARENESS_ID={brand_id}")
    if perf_id:
        print(f"       INSTANTLY_CAMPAIGN_PERFORMANCE_ID={perf_id}")
    print("  3. Start the campaigns from Instantly UI when ready.")
    print("  4. Flip SDR_DRY_RUN=false in .env to let sdr_agent push leads.")
    return 0


# ─────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────
def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="instantly_setup")
    sub = parser.add_subparsers(dest="cmd", required=True)

    sub.add_parser("inspect", help="List all Instantly accounts (read-only).")

    p_setup = sub.add_parser(
        "setup",
        help="Rename N healthiest inboxes to Jordan Reilly and create 2 paused campaigns.",
    )
    p_setup.add_argument("--limit", type=int, default=8,
                         help="Number of inboxes to dedicate to Jordan Reilly (default 8).")
    p_setup.add_argument("--dry-run", action="store_true",
                         help="Print the plan without writing.")

    args = parser.parse_args(argv)

    if args.cmd == "inspect":
        return cmd_inspect()
    if args.cmd == "setup":
        return cmd_setup(args.limit, args.dry_run)
    parser.print_help()
    return 1


if __name__ == "__main__":
    sys.exit(main())
