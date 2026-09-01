#!/usr/bin/env python3
"""
scripts/qbo_set_customer_emails.py

Set the billing email on QuickBooks customer records.

Why this is a script and not a connector call
---------------------------------------------
The Claude QBO MCP connector exposes search_customer and create_customer but
no update_customer, so there is no way to put an email on a customer that
already exists. create_customer is not a substitute — it would fork a second
record under a near-identical name and split that partner's A/R across both.
The Accounting API does a sparse update properly, so it goes here.

Why it matters
--------------
A customer with no email on record cannot be sent an invoice reminder from
QuickBooks, and shows up blank in any A/R chase list. Several of PGAM's
partners are in that state while their invoices carry an email inline — the
address exists on the document but not on the account.

Usage
-----
    python3 scripts/qbo_set_customer_emails.py            # dry run
    python3 scripts/qbo_set_customer_emails.py --apply
    python3 scripts/qbo_set_customer_emails.py --set "Acme=ap@acme.com" --apply

Requires QBO_CLIENT_ID / QBO_CLIENT_SECRET / QBO_REFRESH_TOKEN / QBO_REALM_ID.
Safe to re-run: a customer whose email already matches is skipped.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from core import qbo_api  # noqa: E402

# Customer DisplayName -> billing email. Names must match QuickBooks exactly.
CONTACTS = {
    "PubFusion": "billing@pubsfusion.com",
    "Oveeo":     "finance@oveeo.com",
    "Blasto":    "accounts@blasto.ai",
}


def find_customer(cfg: dict, token: str, name: str) -> dict | None:
    escaped = name.replace("'", "''")
    rows = qbo_api.query(
        cfg, token,
        f"select Id, DisplayName, PrimaryEmailAddr, SyncToken "
        f"from Customer where DisplayName = '{escaped}'",
    )
    return rows[0] if rows else None


def current_email(customer: dict) -> str:
    return ((customer.get("PrimaryEmailAddr") or {}).get("Address") or "").strip()


def set_email(cfg: dict, token: str, customer: dict, email: str) -> dict:
    """Sparse update of one field.

    SyncToken is QBO's optimistic-concurrency guard and must be the one read
    back a moment ago; sparse=true keeps every other field on the record
    untouched, which a full update would silently blank.
    """
    payload = {
        "Id":                customer["Id"],
        "SyncToken":         customer["SyncToken"],
        "sparse":            True,
        "PrimaryEmailAddr":  {"Address": email},
    }
    return qbo_api.post(cfg, token, "customer", payload)["Customer"]


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--apply", action="store_true",
                    help="write to QuickBooks (default is a dry run)")
    ap.add_argument("--set", action="append", default=[], metavar="NAME=EMAIL",
                    help="override or extend CONTACTS; repeatable")
    args = ap.parse_args()

    contacts = dict(CONTACTS)
    for pair in args.set:
        if "=" not in pair:
            print(f"--set expects NAME=EMAIL, got: {pair}")
            return 1
        name, email = pair.split("=", 1)
        contacts[name.strip()] = email.strip()

    cfg = qbo_api.config()
    missing = qbo_api.missing_config(cfg)
    if missing:
        print(f"Missing QBO config: {', '.join(missing)}")
        return 1

    qbo_api.ensure_token_table()
    token = qbo_api.access_token(cfg)

    planned, problems = [], []
    for name, email in contacts.items():
        customer = find_customer(cfg, token, name)
        if customer is None:
            problems.append(f"{name}: no customer with that exact DisplayName")
            continue

        existing = current_email(customer)
        if existing.lower() == email.lower():
            print(f"  · {name} already set to {email} — skipping")
            continue
        planned.append((name, customer, existing, email))

    if problems:
        print("\nRefusing to write — these need attention first:")
        for line in problems:
            print(f"  ✗ {line}")
        return 1

    if not planned:
        print("\nNothing to do — every contact already has the right email.")
        return 0

    print(f"\n{'APPLYING' if args.apply else 'PLAN'}:")
    for name, customer, existing, email in planned:
        was = existing or "(none)"
        print(f"  {name:<20} id {customer['Id']:<5} {was}  →  {email}")

    if not args.apply:
        print("\nDry run — nothing written. Re-run with --apply.")
        return 0

    print()
    for name, customer, _existing, email in planned:
        try:
            set_email(cfg, token, customer, email)
        except qbo_api.QBOError as exc:
            print(f"  ✗ {name}: {exc}")
            print("    Stopping here; everything before this was written.")
            return 1
        print(f"  ✓ {name} → {email}")

    print(f"\nDone. {len(planned)} customer record(s) updated.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
