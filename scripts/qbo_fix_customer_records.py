#!/usr/bin/env python3
"""
scripts/qbo_fix_customer_records.py

Fix QuickBooks customer records: set the billing email, and rename records
that carry a person's name instead of the company's.

Why this is a script and not a connector call
---------------------------------------------
The Claude QBO MCP connector exposes search_customer and create_customer but
no update_customer, so an existing record cannot be edited through it.
create_customer is not a substitute — it would fork a second record under a
near-identical name and split that partner's A/R across both. The Accounting
API does a sparse update properly, so it goes here.

Why the renames
---------------
Five customers are filed under the individual PGAM deals with rather than the
company that owes the money — Gurtej Kaler is illumin, Martin Hoch is Synatix,
Rachel Mendi is Kueez, Itay Rubinstein is 9Dots Media, Akvilė Pinčius is
Eskimi. $93,980.95 sits behind those five names.

That is not cosmetic. Every A/R report reads as though an individual is in
arrears, which is exactly how the August 2026 review concluded "Itay
Rubinstein has never paid" about 9Dots Media, who had in fact paid. The person
is preserved in GivenName/FamilyName; only the label the reports key off
changes.

Safety
------
A rename changes the customer's name on every historical invoice, statement
and report — it is reversible, but it is not local. Two things were checked
before writing this:

  * pgam-direct links QBO through `payee_directory.qbo_customer_id`, a numeric
    id with a unique index, mapped from `finance.qbo_partner_map` on recon
    partner keys — not on DisplayName. Renames do not break it.
  * None of the five target names already exist in the realm, so no rename
    can collide with a live record.

What could NOT be verified is PGAM Admin, the system that actually generates
the monthly invoices (its repo was not located). If it resolves customers by
name rather than by id, a rename would break the next billing run. Hence
--only: rename one small account first, watch the next generation, then do the
rest.

Usage
-----
    python3 scripts/qbo_fix_customer_records.py                    # dry run, all
    python3 scripts/qbo_fix_customer_records.py --apply
    python3 scripts/qbo_fix_customer_records.py --only "Itay Rubinstein" --apply
    python3 scripts/qbo_fix_customer_records.py --emails-only --apply
    python3 scripts/qbo_fix_customer_records.py --set "Acme=ap@acme.com" --apply

Requires QBO_CLIENT_ID / QBO_CLIENT_SECRET / QBO_REFRESH_TOKEN / QBO_REALM_ID.
Safe to re-run: anything already correct is skipped, including a record that
was renamed on an earlier run.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from core import qbo_api  # noqa: E402

# Keyed by the DisplayName currently in QuickBooks.
#   email      billing address to put on the record
#   rename_to  new DisplayName (and CompanyName)
#   given      first name, kept so the human contact is not lost
#   family     last name
CONTACTS: dict[str, dict[str, str]] = {
    # Missing billing emails.
    "PubFusion": {"email": "billing@pubsfusion.com"},
    "Oveeo":     {"email": "finance@oveeo.com"},
    "Blasto":    {"email": "accounts@blasto.ai"},

    # Filed under a person; the company is who owes the money.
    "Gurtej Kaler":    {"rename_to": "illumin",
                        "given": "Gurtej", "family": "Kaler"},
    "Martin Hoch":     {"rename_to": "Synatix",
                        "given": "Martin", "family": "Hoch"},
    "Rachel Mendi":    {"rename_to": "Kueez",
                        "given": "Rachel", "family": "Mendi"},
    "Itay Rubinstein": {"rename_to": "9Dots Media",
                        "given": "Itay",   "family": "Rubinstein"},
    "Akvilė Pinčius":  {"rename_to": "Eskimi",
                        "given": "Akvilė", "family": "Pinčius"},
}


def _find(cfg: dict, token: str, name: str) -> dict | None:
    escaped = name.replace("'", "''")
    rows = qbo_api.query(
        cfg, token,
        f"select Id, DisplayName, CompanyName, GivenName, FamilyName, "
        f"PrimaryEmailAddr, SyncToken from Customer "
        f"where DisplayName = '{escaped}'",
    )
    return rows[0] if rows else None


def _email_of(customer: dict) -> str:
    return ((customer.get("PrimaryEmailAddr") or {}).get("Address") or "").strip()


def resolve(cfg: dict, token: str, name: str, spec: dict) -> tuple[dict | None, str]:
    """Find the record, tolerating a rename applied by an earlier run.

    Returns (customer, note). A record already carrying the target name is
    returned so its email can still be checked — the run stays idempotent
    rather than aborting on "not found".
    """
    customer = _find(cfg, token, name)
    if customer is not None:
        return customer, ""

    target = spec.get("rename_to")
    if target:
        renamed = _find(cfg, token, target)
        if renamed is not None:
            return renamed, f"already renamed to {target}"

    return None, "no customer with that exact DisplayName"


def plan_for(customer: dict, spec: dict, emails_only: bool) -> dict:
    """What would change on this record. Empty payload means nothing to do."""
    payload: dict = {}
    described: list[str] = []

    email = spec.get("email")
    if email and _email_of(customer).lower() != email.lower():
        payload["PrimaryEmailAddr"] = {"Address": email}
        described.append(f"email {_email_of(customer) or '(none)'} → {email}")

    target = spec.get("rename_to")
    if target and not emails_only and customer.get("DisplayName") != target:
        payload["DisplayName"] = target
        payload["CompanyName"] = target
        described.append(f"name {customer['DisplayName']} → {target}")
        # Keep the human on the record; QBO shows these as the contact.
        if spec.get("given") and customer.get("GivenName") != spec["given"]:
            payload["GivenName"] = spec["given"]
        if spec.get("family") and customer.get("FamilyName") != spec["family"]:
            payload["FamilyName"] = spec["family"]

    return {"payload": payload, "described": described}


def apply(cfg: dict, token: str, customer: dict, payload: dict) -> dict:
    """Sparse update.

    SyncToken is QBO's optimistic-concurrency guard and must be the one read
    back a moment ago, so a concurrent edit is rejected rather than clobbered.
    sparse=true leaves every field not named here untouched — a full update
    would silently blank them.
    """
    body = {
        "Id":        customer["Id"],
        "SyncToken": customer["SyncToken"],
        "sparse":    True,
        **payload,
    }
    return qbo_api.post(cfg, token, "customer", body)["Customer"]


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--apply", action="store_true",
                    help="write to QuickBooks (default is a dry run)")
    ap.add_argument("--only", action="append", default=[], metavar="NAME",
                    help="restrict to these records; repeatable. Use this to "
                         "stage renames one at a time.")
    ap.add_argument("--emails-only", action="store_true",
                    help="set emails but skip every rename")
    ap.add_argument("--set", action="append", default=[], metavar="NAME=EMAIL",
                    help="override or extend the email map; repeatable")
    args = ap.parse_args()

    contacts = {name: dict(spec) for name, spec in CONTACTS.items()}
    for pair in args.set:
        if "=" not in pair:
            print(f"--set expects NAME=EMAIL, got: {pair}")
            return 1
        name, email = pair.split("=", 1)
        contacts.setdefault(name.strip(), {})["email"] = email.strip()

    if args.only:
        unknown = [n for n in args.only if n not in contacts]
        if unknown:
            print(f"--only names not in the map: {', '.join(unknown)}")
            return 1
        contacts = {n: contacts[n] for n in args.only}

    cfg = qbo_api.config()
    missing = qbo_api.missing_config(cfg)
    if missing:
        print(f"Missing QBO config: {', '.join(missing)}")
        return 1

    qbo_api.ensure_token_table()
    token = qbo_api.access_token(cfg)

    planned, problems = [], []
    for name, spec in contacts.items():
        customer, note = resolve(cfg, token, name, spec)
        if customer is None:
            problems.append(f"{name}: {note}")
            continue

        # A rename must not land on a name another record already holds.
        target = spec.get("rename_to")
        if target and not args.emails_only and customer["DisplayName"] != target:
            clash = _find(cfg, token, target)
            if clash is not None and clash["Id"] != customer["Id"]:
                problems.append(
                    f"{name}: '{target}' is already customer id {clash['Id']} — "
                    "renaming would collide; merge them in the QBO UI instead"
                )
                continue

        step = plan_for(customer, spec, args.emails_only)
        if not step["payload"]:
            suffix = f" ({note})" if note else ""
            print(f"  · {name} already correct{suffix} — skipping")
            continue
        planned.append((name, customer, step))

    if problems:
        print("\nRefusing to write — these need attention first:")
        for line in problems:
            print(f"  ✗ {line}")
        return 1

    if not planned:
        print("\nNothing to do — every record is already correct.")
        return 0

    print(f"\n{'APPLYING' if args.apply else 'PLAN'}:")
    for name, customer, step in planned:
        print(f"  {name}  (id {customer['Id']})")
        for line in step["described"]:
            print(f"      {line}")

    if not args.apply:
        print("\nDry run — nothing written. Re-run with --apply.")
        if any("name " in line for _, _, s in planned for line in s["described"]):
            print("A rename changes the name on every historical invoice and "
                  "report.\nConsider --only \"Itay Rubinstein\" first (smallest "
                  "balance) and\nwatch the next invoice generation before doing "
                  "the rest.")
        return 0

    print()
    for name, customer, step in planned:
        try:
            apply(cfg, token, customer, step["payload"])
        except qbo_api.QBOError as exc:
            print(f"  ✗ {name}: {exc}")
            print("    Stopping here; everything before this was written.")
            return 1
        print(f"  ✓ {name}: {'; '.join(step['described'])}")

    print(f"\nDone. {len(planned)} customer record(s) updated.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
