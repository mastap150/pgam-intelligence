#!/usr/bin/env python3
"""
scripts/qbo_record_payments.py

Record customer payments against QBO invoices where the money arrived net of
bank fees.

Why this is not just "edit the invoice down"
--------------------------------------------
When a customer pays $3,900.00 and $3,875.00 lands, they paid in full — the
bank took $25.00 in transit. Revenue is still $3,900.00 and the $25.00 is a
deductible bank charge. Trimming the invoice to $3,875.00 would understate
revenue, bury the expense, and (with no payment recorded) not even close the
invoice. So for each item this writes the pair QBO's own UI writes:

  1. Payment  — full invoice amount, into Undeposited Funds, linked to the
                invoice. This is what closes it.
  2. Deposit  — Undeposited Funds → bank, carrying the payment plus a negative
                line to Bank Charges for the fee, so the deposit nets to the
                figure that actually hit the statement and matches the bank
                feed line cleanly.

One deposit can cover several invoices — see the 1254 + 1269 entry.

Usage
-----
    python3 scripts/qbo_record_payments.py            # dry run, prints a plan
    python3 scripts/qbo_record_payments.py --apply    # writes to QuickBooks

Requires QBO_CLIENT_ID / QBO_CLIENT_SECRET / QBO_REFRESH_TOKEN / QBO_REALM_ID.
Safe to re-run: an invoice already at zero balance is skipped.
"""

from __future__ import annotations

import argparse
import sys
from datetime import date
from decimal import Decimal
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from core import qbo_api  # noqa: E402

# ---------------------------------------------------------------------------
# What to record
#
# From the 2026-08-28 bank reconciliation. `deposit` is what hit the statement;
# `fee` is invoice total minus deposit. Group several docs under one entry when
# a single wire covered them.
# ---------------------------------------------------------------------------

BATCH = [
    {"docs": ["1246"],         "deposit": "3875.00",  "fee": "25.00",
     "memo": "Vondos wire, $25.00 bank fee"},
    {"docs": ["1248"],         "deposit": "5938.26",  "fee": "33.00",
     "memo": "9Dots wire, $33.00 wire fee"},
    {"docs": ["1253"],         "deposit": "8132.13",  "fee": "33.00",
     "memo": "9Dots wire, $33.00 wire fee"},
    {"docs": ["1274"],         "deposit": "8560.57",  "fee": "18.92",
     "memo": "Vondos wire, $18.92 bank fee"},
    {"docs": ["1220"],         "deposit": "8008.19",  "fee": "41.71",
     "memo": "SmileWanted wire, $25.00 stated + $16.71 correspondent fee"},
    {"docs": ["1245"],         "deposit": "102.39",   "fee": "15.03",
     "memo": "SmileWanted wire, $15.03 correspondent fee"},
    {"docs": ["1240"],         "deposit": "2948.08",  "fee": "40.09",
     "memo": "SmileWanted wire, $25.00 stated + $15.09 correspondent fee"},
    {"docs": ["1254", "1269"], "deposit": "77531.15", "fee": "161.50",
     "memo": "9Dots wire covering 1254 + 1269, $38.50 stated + $123.00 correspondent fee"},
]

# Account names in this realm. find_account falls back to the subtype when a
# name does not match, so a rename does not break the run.
BANK_ACCOUNT_NAME = "Business Fundamentals Chk (6228)"
UNDEPOSITED_FUNDS_NAME = "Undeposited Funds"
BANK_CHARGES_NAME = "Bank Charges & Fees"


def _money(raw: str | Decimal) -> Decimal:
    return Decimal(str(raw)).quantize(Decimal("0.01"))


# ---------------------------------------------------------------------------
# Planning
# ---------------------------------------------------------------------------

def build_plan(cfg: dict, token: str) -> tuple[list[dict], list[str]]:
    """Resolve every doc number to a live invoice and check the arithmetic.

    Returns (plan, problems). Nothing is written if problems is non-empty.
    """
    plan, problems = [], []

    for entry in BATCH:
        invoices = []
        for doc in entry["docs"]:
            inv = qbo_api.find_invoice_by_doc_number(cfg, token, doc)
            if inv is None:
                problems.append(f"invoice {doc}: not found in QuickBooks")
                continue
            balance = _money(inv.get("Balance", 0))
            if balance == 0:
                print(f"  · {doc} already at zero balance — skipping")
                continue
            invoices.append(inv)

        if not invoices:
            continue

        billed = sum(_money(i.get("Balance", 0)) for i in invoices)
        deposit = _money(entry["deposit"])
        fee = _money(entry["fee"])

        # The stated fee must be exactly what is missing from the deposit,
        # otherwise the deposit will not net to the bank line and something in
        # the reconciliation is wrong.
        if billed - deposit != fee:
            problems.append(
                f"invoice(s) {'+'.join(entry['docs'])}: open balance {billed} "
                f"− deposit {deposit} = {billed - deposit}, but fee is stated "
                f"as {fee}"
            )
            continue

        customers = {i["CustomerRef"]["value"] for i in invoices}
        if len(customers) > 1:
            problems.append(
                f"invoice(s) {'+'.join(entry['docs'])}: span multiple customers, "
                "cannot be one payment"
            )
            continue

        plan.append({
            "docs":        entry["docs"],
            "memo":        entry["memo"],
            "invoices":    invoices,
            "customer_id": customers.pop(),
            "billed":      billed,
            "deposit":     deposit,
            "fee":         fee,
        })

    return plan, problems


# ---------------------------------------------------------------------------
# Writing
# ---------------------------------------------------------------------------

def record(cfg: dict, token: str, item: dict, accounts: dict) -> dict:
    payment_payload = {
        "CustomerRef":          {"value": item["customer_id"]},
        "TotalAmt":             float(item["billed"]),
        "TxnDate":              date.today().isoformat(),
        "DepositToAccountRef":  {"value": accounts["undeposited"]},
        "PrivateNote":          item["memo"],
        "Line": [
            {
                "Amount": float(_money(inv["Balance"])),
                "LinkedTxn": [{"TxnId": inv["Id"], "TxnType": "Invoice"}],
            }
            for inv in item["invoices"]
        ],
    }
    payment = qbo_api.post(cfg, token, "payment", payment_payload)["Payment"]

    deposit_payload = {
        "DepositToAccountRef": {"value": accounts["bank"]},
        "TxnDate":             date.today().isoformat(),
        "PrivateNote":         item["memo"],
        "Line": [
            {
                "Amount": float(item["billed"]),
                "LinkedTxn": [{"TxnId": payment["Id"], "TxnType": "Payment"}],
            },
            {
                "Amount":      float(-item["fee"]),
                "DetailType":  "DepositLineDetail",
                "Description": "Bank / wire fee deducted in transit",
                "DepositLineDetail": {
                    "AccountRef": {"value": accounts["bank_charges"]},
                },
            },
        ],
    }
    deposit = qbo_api.post(cfg, token, "deposit", deposit_payload)["Deposit"]

    return {"payment_id": payment["Id"], "deposit_id": deposit["Id"]}


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--apply", action="store_true",
                    help="write to QuickBooks (default is a dry run)")
    args = ap.parse_args()

    cfg = qbo_api.config()
    missing = qbo_api.missing_config(cfg)
    if missing:
        print(f"Missing QBO config: {', '.join(missing)}")
        return 1

    qbo_api.ensure_token_table()
    token = qbo_api.access_token(cfg)

    bank = qbo_api.find_account(cfg, token, name=BANK_ACCOUNT_NAME,
                                account_type="Checking")
    undeposited = qbo_api.find_account(cfg, token, name=UNDEPOSITED_FUNDS_NAME,
                                       account_type="UndepositedFunds")
    charges = qbo_api.find_account(cfg, token, name=BANK_CHARGES_NAME,
                                   account_type="BankCharges")

    for label, acct in (("bank", bank), ("undeposited funds", undeposited),
                        ("bank charges", charges)):
        if acct is None:
            print(f"Could not resolve the {label} account — aborting.")
            return 1
        print(f"  {label:18} → {acct['Name']} (id {acct['Id']})")

    accounts = {
        "bank":         bank["Id"],
        "undeposited":  undeposited["Id"],
        "bank_charges": charges["Id"],
    }

    print("\nResolving invoices…")
    plan, problems = build_plan(cfg, token)

    if problems:
        print("\nRefusing to write — these need attention first:")
        for line in problems:
            print(f"  ✗ {line}")
        return 1

    if not plan:
        print("\nNothing to do — every invoice is already settled.")
        return 0

    print(f"\n{'PLAN' if not args.apply else 'APPLYING'}:")
    total_billed = total_deposit = total_fee = Decimal("0")
    for item in plan:
        print(
            f"  {'+'.join(item['docs']):<12} "
            f"invoice {item['billed']:>10}  "
            f"deposit {item['deposit']:>10}  "
            f"fee {item['fee']:>7}"
        )
        total_billed += item["billed"]
        total_deposit += item["deposit"]
        total_fee += item["fee"]
    print(f"  {'TOTAL':<12} invoice {total_billed:>10}  "
          f"deposit {total_deposit:>10}  fee {total_fee:>7}")

    if not args.apply:
        print("\nDry run — nothing written. Re-run with --apply to record these.")
        return 0

    print()
    for item in plan:
        try:
            ids = record(cfg, token, item, accounts)
        except qbo_api.QBOError as exc:
            print(f"  ✗ {'+'.join(item['docs'])}: {exc}")
            print("    Stopping here; everything before this was written.")
            return 1
        print(f"  ✓ {'+'.join(item['docs'])} closed "
              f"(payment {ids['payment_id']}, deposit {ids['deposit_id']})")

    print(f"\nDone. {len(plan)} deposits recorded, {total_billed} of invoices "
          f"closed, {total_fee} booked to bank charges.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
