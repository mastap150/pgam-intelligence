"""
agents/recon/payment_matcher.py

Match bank deposits against open QuickBooks invoices, and propose the payments
that would close them.

Why this exists
---------------
PGAM's receivables problem is not that partners do not pay — it is that wires
arrive and are never matched in QuickBooks. Three customers written off in
August as "never paid" had in fact paid; the money was sitting in the bank
feed's For Review queue, unrecognised. $132,884.00 of invoices ended up closed
by hand.

The QBO Accounting API cannot read that For Review queue — it sees recorded
transactions only (Deposit, Purchase, JournalEntry), never pending bank-feed
lines. So the bank statement itself is the input: you export it, this matches
it against live open invoices, and hands a plan to scripts/qbo_record_payments.py.

What makes matching non-trivial
-------------------------------
  * Fees in transit. A $3,900.00 invoice arrives as $3,875.00 because a
    correspondent bank took $25.00. The customer paid in full; the shortfall
    is a bank charge, not a discount. Observed fees ran $15.03 to $161.50.
  * Grouped wires. One 9Dots deposit of $77,531.15 covered invoices 1254 and
    1269. Matching is subset-sum, not one-to-one.
  * Useless references. Experience Ten wired with "Invoice no.: 1228" and
    matched instantly. Smile Wanted wired "PGAM 2026 06" and sat unmatched for
    weeks. Reference matching is tried first because when it works it is
    exact; amount matching is the fallback.

This module never writes to QuickBooks. It reads invoices, proposes matches,
and emits a plan. Applying the plan is a separate, explicit command.

Usage
-----
    python3 -m agents.recon.payment_matcher statement.csv
    python3 -m agents.recon.payment_matcher statement.qbo --emit-plan plan.json
    python3 -m agents.recon.payment_matcher statement.csv --no-email

    # then, after reading the proposal:
    python3 scripts/qbo_record_payments.py --plan plan.json           # dry run
    python3 scripts/qbo_record_payments.py --plan plan.json --apply

Input formats
-------------
  .qbo / .ofx / .qfx   Web Connect / OFX. Preferred — carries FITID, a stable
                       per-transaction id, so re-importing an overlapping
                       statement cannot double-propose.
  .csv                 Columns are detected by header name. Handles both a
                       single signed amount column and separate debit/credit
                       columns.

Not scheduled
-------------
Deliberately operator-driven: it needs a statement file, so there is nothing
for a cron tick to do. The weekly nudge to run it comes from ar_aging_sentry.

State
-----
  Neon table `bank_txn_match`   one row per bank line ever seen, with its
                                proposal and status. Gives cross-run dedupe
                                and an audit trail of what was proposed when.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
import re
import sys
from bisect import bisect_left, bisect_right
from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from itertools import combinations
from pathlib import Path

from core import qbo_api

# ---------------------------------------------------------------------------
# Matching parameters
#
# A deposit matches a set of invoices when the shortfall (billed − deposit) is
# a plausible bank fee. Neither an absolute nor a percentage cap works alone:
# $15.03 on a $117.42 invoice is 12.8%, while $161.50 on $77,692.65 is 0.21%.
# So the allowance is the smaller of the two, which keeps the window tight at
# both ends of the range.
# ---------------------------------------------------------------------------

FEE_ABS_CAP = Decimal("250.00")     # covers the largest fee seen ($161.50)
FEE_PCT_CAP = Decimal("0.15")       # covers the worst ratio seen (12.8%)

# One QBO Payment belongs to one customer, so subsets never span customers.
# Beyond three invoices the false-positive rate climbs faster than the hit rate.
MAX_GROUP_SIZE = 3

# An invoice cannot be paid before it is raised, and a wire more than this far
# after the invoice date is more likely a coincidence than a payment.
MAX_AGE_DAYS = 400

# Deposits at or below this are noise (interest, rebates) and are not matched.
MIN_DEPOSIT = Decimal("1.00")


@dataclass
class BankTxn:
    """One credit line from the bank statement."""
    posted_on: date
    amount: Decimal
    description: str
    fitid: str | None = None

    def fingerprint(self) -> str:
        """Stable identity for cross-run dedupe.

        FITID when the bank gave us one — it is guaranteed unique by the OFX
        spec. Otherwise a hash of the fields a CSV does carry, which is enough
        to survive re-importing an overlapping date range.
        """
        if self.fitid:
            return f"fitid:{self.fitid}"
        raw = f"{self.posted_on.isoformat()}|{self.amount}|{self.description.strip().lower()}"
        return "sha:" + hashlib.sha256(raw.encode()).hexdigest()[:32]


@dataclass
class Invoice:
    qbo_id: str
    doc_number: str
    customer_id: str
    customer_name: str
    txn_date: date
    balance: Decimal


@dataclass
class Proposal:
    txn: BankTxn
    status: str                       # matched | ambiguous | unmatched
    invoices: list[Invoice] = field(default_factory=list)
    billed: Decimal = Decimal("0")
    fee: Decimal = Decimal("0")
    basis: str = ""                   # reference | amount
    alternatives: list[list[Invoice]] = field(default_factory=list)
    note: str = ""


# ---------------------------------------------------------------------------
# Statement parsing
# ---------------------------------------------------------------------------

def _money(raw: str) -> Decimal | None:
    """Parse a currency cell. Returns None when it is not a number at all."""
    if raw is None:
        return None
    text = str(raw).strip().replace(",", "").replace("$", "").replace(" ", "")
    if not text:
        return None
    negative = text.startswith("(") and text.endswith(")")
    if negative:
        text = text[1:-1]
    try:
        value = Decimal(text)
    except InvalidOperation:
        return None
    return (-value if negative else value).quantize(Decimal("0.01"))


def _parse_date(raw: str) -> date | None:
    text = (raw or "").strip()
    if not text:
        return None
    for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%d/%m/%Y", "%m/%d/%y", "%d-%b-%Y",
                "%b %d, %Y", "%Y/%m/%d", "%d %b %Y"):
        try:
            return datetime.strptime(text, fmt).date()
        except ValueError:
            continue
    return None


def _pick_column(headers: list[str], *candidates: str) -> str | None:
    """First header whose lowercased name contains one of the candidates.

    Bank CSV headers are wildly inconsistent ("Posting Date", "Date Posted",
    "Transaction Date"), so substring matching beats an exact-name table.
    """
    lowered = {h: (h or "").strip().lower() for h in headers}
    for candidate in candidates:
        for header, low in lowered.items():
            if candidate in low:
                return header
    return None


def _parse_csv(text: str) -> list[BankTxn]:
    # Some banks prepend a title/account block before the real header. Find the
    # first line that looks like a header row and start there.
    lines = text.splitlines()
    start = 0
    for index, line in enumerate(lines[:20]):
        low = line.lower()
        if ("date" in low) and ("amount" in low or "credit" in low or "deposit" in low):
            start = index
            break
    body = "\n".join(lines[start:])

    try:
        dialect = csv.Sniffer().sniff(body[:4096], delimiters=",;\t|")
    except csv.Error:
        dialect = csv.excel
    reader = csv.DictReader(io.StringIO(body), dialect=dialect)
    headers = [h for h in (reader.fieldnames or []) if h]
    if not headers:
        raise SystemExit("Could not read a header row from the CSV.")

    date_col = _pick_column(headers, "post date", "posting date", "date")
    desc_col = _pick_column(headers, "description", "memo", "narrative",
                            "detail", "payee", "name", "reference")
    amount_col = _pick_column(headers, "amount")
    credit_col = _pick_column(headers, "credit", "deposit", "money in", "paid in")

    if not date_col or not (amount_col or credit_col):
        raise SystemExit(
            f"Could not find date and amount columns. Headers seen: {headers}"
        )

    out: list[BankTxn] = []
    for row in reader:
        posted = _parse_date(row.get(date_col, ""))
        if posted is None:
            continue

        # Prefer a dedicated credit column; fall back to a signed amount column.
        amount = _money(row.get(credit_col, "")) if credit_col else None
        if amount is None and amount_col:
            amount = _money(row.get(amount_col, ""))
        if amount is None or amount <= 0:
            continue  # debits and blanks are not customer receipts

        description = (row.get(desc_col, "") or "").strip() if desc_col else ""
        out.append(BankTxn(posted_on=posted, amount=amount, description=description))
    return out


_STMTTRN_RE = re.compile(r"<STMTTRN>(.*?)</STMTTRN>", re.S | re.I)


def _ofx_field(block: str, tag: str) -> str:
    """Read one OFX field.

    OFX 1.x is SGML with unclosed tags, 2.x is real XML. Reading up to the next
    '<' or line break handles both without needing to know which we have.
    """
    match = re.search(rf"<{tag}>([^<\r\n]*)", block, re.I)
    return match.group(1).strip() if match else ""


def _parse_ofx(text: str) -> list[BankTxn]:
    out: list[BankTxn] = []
    for block in _STMTTRN_RE.findall(text):
        amount = _money(_ofx_field(block, "TRNAMT"))
        if amount is None or amount <= 0:
            continue

        # DTPOSTED looks like 20260812120000[-5:EST]; the date is the first 8.
        raw_date = _ofx_field(block, "DTPOSTED")[:8]
        try:
            posted = datetime.strptime(raw_date, "%Y%m%d").date()
        except ValueError:
            continue

        name = _ofx_field(block, "NAME")
        memo = _ofx_field(block, "MEMO")
        description = " ".join(part for part in (name, memo) if part)

        out.append(BankTxn(
            posted_on=posted,
            amount=amount,
            description=description,
            fitid=_ofx_field(block, "FITID") or None,
        ))
    return out


def parse_statement(path: Path) -> list[BankTxn]:
    # Banks often serve OFX as latin-1; decode leniently rather than crashing
    # on one stray byte in a payee name.
    text = path.read_text(encoding="utf-8", errors="replace")
    if path.suffix.lower() in (".qbo", ".ofx", ".qfx") or "<STMTTRN>" in text.upper():
        return _parse_ofx(text)
    return _parse_csv(text)


# ---------------------------------------------------------------------------
# QuickBooks side
# ---------------------------------------------------------------------------

def open_invoices(cfg: dict, token: str) -> list[Invoice]:
    """Every invoice with an outstanding balance, paginated."""
    out: list[Invoice] = []
    position, page = 1, 100

    while True:
        rows = qbo_api.query(
            cfg, token,
            f"select * from Invoice where Balance > '0' "
            f"startposition {position} maxresults {page}",
        )
        for row in rows:
            balance = _money(str(row.get("Balance", 0)))
            txn_date = _parse_date(row.get("TxnDate", ""))
            if balance is None or balance <= 0 or txn_date is None:
                continue
            customer = row.get("CustomerRef") or {}
            out.append(Invoice(
                qbo_id=str(row.get("Id")),
                doc_number=str(row.get("DocNumber") or ""),
                customer_id=str(customer.get("value") or ""),
                customer_name=str(customer.get("name") or "(unnamed)"),
                txn_date=txn_date,
                balance=balance,
            ))
        if len(rows) < page:
            break
        position += page

    return out


# ---------------------------------------------------------------------------
# Matching
# ---------------------------------------------------------------------------

def _allowed_fee(billed: Decimal) -> Decimal:
    return min(FEE_ABS_CAP, (billed * FEE_PCT_CAP).quantize(Decimal("0.01")))


def _plausible(billed: Decimal, deposit: Decimal) -> bool:
    """Could this deposit be this billed amount, net of a bank fee?"""
    shortfall = billed - deposit
    return Decimal("0") <= shortfall <= _allowed_fee(billed)


def _subsets_by_customer(invoices: list[Invoice]) -> dict[str, list[tuple[Decimal, tuple[Invoice, ...]]]]:
    """Pre-compute every candidate invoice group per customer, sorted by total.

    Built once and reused across all deposits: enumerating combinations inside
    the per-deposit loop is what makes a naive version quadratic in statement
    size. Sorted so each deposit becomes two bisections instead of a scan.
    """
    by_customer: dict[str, list[Invoice]] = {}
    for inv in invoices:
        by_customer.setdefault(inv.customer_id, []).append(inv)

    table: dict[str, list[tuple[Decimal, tuple[Invoice, ...]]]] = {}
    for customer_id, group in by_customer.items():
        sums: list[tuple[Decimal, tuple[Invoice, ...]]] = []
        for size in range(1, min(MAX_GROUP_SIZE, len(group)) + 1):
            for combo in combinations(group, size):
                sums.append((sum((i.balance for i in combo), Decimal("0")), combo))
        sums.sort(key=lambda pair: pair[0])
        table[customer_id] = sums
    return table


def _by_reference(txn: BankTxn, invoices: list[Invoice]) -> list[Invoice]:
    """Invoices whose doc number appears as a standalone token in the memo.

    Word boundaries matter: a bare substring search would match '1289' inside
    an account number or an amount.
    """
    text = txn.description
    if not text:
        return []
    hits = [
        inv for inv in invoices
        if inv.doc_number and re.search(rf"\b{re.escape(inv.doc_number)}\b", text)
    ]
    # Only trust a reference hit when it belongs to a single customer — a memo
    # quoting two customers' invoice numbers is not something to guess at.
    if hits and len({inv.customer_id for inv in hits}) == 1:
        return hits
    return []


def match(txns: list[BankTxn], invoices: list[Invoice]) -> list[Proposal]:
    subset_table = _subsets_by_customer(invoices)
    proposals: list[Proposal] = []

    for txn in txns:
        if txn.amount < MIN_DEPOSIT:
            continue

        # 1. Reference match. Exact when the partner quotes the invoice number.
        referenced = _by_reference(txn, invoices)
        if referenced:
            billed = sum((i.balance for i in referenced), Decimal("0"))
            if _plausible(billed, txn.amount):
                proposals.append(Proposal(
                    txn=txn, status="matched", invoices=list(referenced),
                    billed=billed, fee=billed - txn.amount, basis="reference",
                ))
                continue
            # The memo names invoices but the money does not add up. Say so
            # rather than silently falling through to a weaker amount match.
            proposals.append(Proposal(
                txn=txn, status="ambiguous", alternatives=[list(referenced)],
                note=(
                    f"memo cites {', '.join(i.doc_number for i in referenced)} "
                    f"totalling ${billed:,.2f}, but ${txn.amount:,.2f} arrived"
                ),
            ))
            continue

        # 2. Amount match. Every customer's candidate groups whose total sits
        #    just above the deposit by a plausible fee.
        candidates: list[tuple[Decimal, tuple[Invoice, ...]]] = []
        for sums in subset_table.values():
            keys = [total for total, _ in sums]
            lo = bisect_left(keys, txn.amount)
            hi = bisect_right(keys, txn.amount + FEE_ABS_CAP)
            for total, combo in sums[lo:hi]:
                if not _plausible(total, txn.amount):
                    continue
                if any(inv.txn_date > txn.posted_on for inv in combo):
                    continue  # cannot pay an invoice before it exists
                if any((txn.posted_on - inv.txn_date).days > MAX_AGE_DAYS for inv in combo):
                    continue
                candidates.append((total, combo))

        if not candidates:
            proposals.append(Proposal(
                txn=txn, status="unmatched",
                note="no open invoice or group of up to "
                     f"{MAX_GROUP_SIZE} matches this amount",
            ))
            continue

        # Prefer the tightest fit — the smallest implied fee. An exact match
        # (fee of zero) always wins over one needing a fee to explain it.
        candidates.sort(key=lambda pair: (pair[0] - txn.amount, len(pair[1])))
        best_total, best_combo = candidates[0]

        # Ambiguous only when a rival explains the deposit equally well. A
        # second candidate with a visibly larger implied fee is not a rival.
        rivals = [
            combo for total, combo in candidates[1:]
            if total - txn.amount == best_total - txn.amount
        ]
        if rivals:
            proposals.append(Proposal(
                txn=txn, status="ambiguous",
                alternatives=[list(best_combo)] + [list(r) for r in rivals[:4]],
                note=f"{len(rivals) + 1} invoice groups explain this deposit equally well",
            ))
            continue

        proposals.append(Proposal(
            txn=txn, status="matched", invoices=list(best_combo),
            billed=best_total, fee=best_total - txn.amount, basis="amount",
        ))

    return proposals


# ---------------------------------------------------------------------------
# State
# ---------------------------------------------------------------------------

def ensure_tables() -> None:
    from core.neon import connect

    with connect() as conn, conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS bank_txn_match (
                fingerprint  TEXT PRIMARY KEY,
                posted_on    DATE           NOT NULL,
                amount       NUMERIC(14,2)  NOT NULL,
                description  TEXT           NOT NULL DEFAULT '',
                status       TEXT           NOT NULL,
                basis        TEXT           NOT NULL DEFAULT '',
                doc_numbers  TEXT[]         NOT NULL DEFAULT '{}',
                customer     TEXT           NOT NULL DEFAULT '',
                billed       NUMERIC(14,2)  NOT NULL DEFAULT 0,
                fee          NUMERIC(14,2)  NOT NULL DEFAULT 0,
                note         TEXT           NOT NULL DEFAULT '',
                proposed_at  TIMESTAMPTZ    NOT NULL DEFAULT now(),
                applied_at   TIMESTAMPTZ
            )
            """
        )
        conn.commit()


def known_fingerprints() -> dict[str, str]:
    """Bank lines already seen, mapped to their status."""
    from core.neon import connect

    with connect() as conn, conn.cursor() as cur:
        cur.execute("SELECT fingerprint, status FROM bank_txn_match")
        return {row[0]: row[1] for row in cur.fetchall()}


def save(proposals: list[Proposal]) -> None:
    """Record this run's proposals.

    A line already marked applied is never overwritten — re-importing a
    statement that overlaps a previous one must not resurrect settled work.
    """
    from core.neon import connect

    with connect() as conn, conn.cursor() as cur:
        for p in proposals:
            cur.execute(
                """
                INSERT INTO bank_txn_match
                    (fingerprint, posted_on, amount, description, status, basis,
                     doc_numbers, customer, billed, fee, note)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (fingerprint) DO UPDATE SET
                    status      = EXCLUDED.status,
                    basis       = EXCLUDED.basis,
                    doc_numbers = EXCLUDED.doc_numbers,
                    customer    = EXCLUDED.customer,
                    billed      = EXCLUDED.billed,
                    fee         = EXCLUDED.fee,
                    note        = EXCLUDED.note,
                    proposed_at = now()
                WHERE bank_txn_match.applied_at IS NULL
                """,
                (
                    p.txn.fingerprint(), p.txn.posted_on, p.txn.amount,
                    p.txn.description[:500], p.status, p.basis,
                    [i.doc_number for i in p.invoices],
                    p.invoices[0].customer_name if p.invoices else "",
                    p.billed, p.fee, p.note[:500],
                ),
            )
        conn.commit()


# ---------------------------------------------------------------------------
# Output
# ---------------------------------------------------------------------------

def _usd(amount: Decimal) -> str:
    return f"${amount:,.2f}"


def emit_plan(proposals: list[Proposal], path: Path) -> int:
    """Write the confident matches in the shape qbo_record_payments.py reads."""
    entries = [
        {
            "docs":    [i.doc_number for i in p.invoices],
            "deposit": str(p.txn.amount),
            "fee":     str(p.fee),
            "memo": (
                f"{p.invoices[0].customer_name} wire {p.txn.posted_on.isoformat()}"
                + (f", {_usd(p.fee)} bank fee" if p.fee else "")
                + (f" — ref: {p.txn.description[:120]}" if p.txn.description else "")
            ),
        }
        for p in proposals if p.status == "matched"
    ]
    path.write_text(json.dumps(entries, indent=2) + "\n")
    return len(entries)


def _html(proposals: list[Proposal], run_date: str) -> str:
    matched = [p for p in proposals if p.status == "matched"]
    ambiguous = [p for p in proposals if p.status == "ambiguous"]
    unmatched = [p for p in proposals if p.status == "unmatched"]
    total = sum((p.billed for p in matched), Decimal("0"))
    fees = sum((p.fee for p in matched), Decimal("0"))

    parts = [
        "<div style='font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;"
        "max-width:760px;color:#222'>",
        f"<h2 style='margin-bottom:4px'>Payment matching — {run_date}</h2>",
        f"<p style='font-size:26px;margin:0 0 4px'><b>{_usd(total)}</b> "
        f"of invoices matched to deposits</p>",
        f"<p style='color:#777;margin:0 0 16px'>{len(matched)} matched · "
        f"{len(ambiguous)} need a decision · {len(unmatched)} unrecognised · "
        f"{_usd(fees)} in bank fees</p>",
        "<p style='color:#777;font-size:13px'>Nothing has been written to "
        "QuickBooks. Applying the plan is a separate command.</p>",
    ]

    if matched:
        parts.append("<h3>Matched — ready to apply</h3>")
        parts.append(
            "<table style='border-collapse:collapse;width:100%;font-size:14px'>"
            "<tr style='background:#f4f4f4'>"
            "<th style='text-align:left;padding:6px'>Date</th>"
            "<th style='text-align:left;padding:6px'>Customer</th>"
            "<th style='text-align:left;padding:6px'>Invoice(s)</th>"
            "<th style='text-align:right;padding:6px'>Billed</th>"
            "<th style='text-align:right;padding:6px'>Deposit</th>"
            "<th style='text-align:right;padding:6px'>Fee</th>"
            "<th style='text-align:left;padding:6px'>Via</th></tr>"
        )
        cell = "padding:6px;border-top:1px solid #eee"
        for p in matched:
            parts.append(
                f"<tr><td style='{cell}'>{p.txn.posted_on}</td>"
                f"<td style='{cell}'>{p.invoices[0].customer_name}</td>"
                f"<td style='{cell}'>{', '.join(i.doc_number for i in p.invoices)}</td>"
                f"<td style='{cell};text-align:right'>{_usd(p.billed)}</td>"
                f"<td style='{cell};text-align:right'>{_usd(p.txn.amount)}</td>"
                f"<td style='{cell};text-align:right'>{_usd(p.fee)}</td>"
                f"<td style='{cell};color:#777'>{p.basis}</td></tr>"
            )
        parts.append("</table>")

    if ambiguous:
        parts.append(
            "<h3>Need a decision</h3>"
            "<p style='color:#777;font-size:13px'>More than one reading fits. "
            "Resolve these in QuickBooks by hand.</p><ul>"
        )
        for p in ambiguous:
            options = " &nbsp;or&nbsp; ".join(
                ", ".join(i.doc_number for i in alt) for alt in p.alternatives
            )
            parts.append(
                f"<li><b>{p.txn.posted_on} — {_usd(p.txn.amount)}</b> "
                f"{p.txn.description[:100]}<br>"
                f"<span style='color:#777'>{p.note}: {options}</span></li>"
            )
        parts.append("</ul>")

    if unmatched:
        parts.append(
            "<h3>Unrecognised deposits</h3>"
            "<p style='color:#777;font-size:13px'>Not A/R, or the invoice is "
            "not yet raised, or more than "
            f"{MAX_GROUP_SIZE} invoices were paid in one wire.</p><ul>"
        )
        for p in unmatched:
            parts.append(
                f"<li>{p.txn.posted_on} — {_usd(p.txn.amount)} — "
                f"{p.txn.description[:120] or '(no description)'}</li>"
            )
        parts.append("</ul>")

    parts.append(
        "<p style='color:#777;font-size:13px;margin-top:20px'>Matching is far "
        "more reliable when partners quote the invoice number in the wire "
        "reference. Deposits matched by reference are exact; those matched by "
        "amount are inference.</p></div>"
    )
    return "".join(parts)


def send_email(html: str, run_date: str, matched_total: Decimal) -> bool:
    import urllib.request
    from core.config import RECIPIENTS, SENDER_EMAIL, SENDGRID_KEY

    if not SENDGRID_KEY or not RECIPIENTS:
        print("[payment_matcher] no SendGrid key or recipients — skipping email")
        return False

    payload = {
        "personalizations": [{"to": [{"email": r} for r in RECIPIENTS]}],
        "from": {"email": SENDER_EMAIL},
        "subject": f"Payment matching — {_usd(matched_total)} ready to apply ({run_date})",
        "content": [{"type": "text/html", "value": html}],
    }
    req = urllib.request.Request(
        "https://api.sendgrid.com/v3/mail/send",
        data=json.dumps(payload).encode(),
        headers={
            "Authorization": f"Bearer {SENDGRID_KEY}",
            "Content-Type":  "application/json",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            if resp.getcode() in (200, 202):
                print(f"[payment_matcher] emailed {len(RECIPIENTS)} recipient(s)")
                return True
            print(f"[payment_matcher] unexpected SendGrid status {resp.getcode()}")
    except Exception as exc:
        print(f"[payment_matcher] delivery failed: {exc}")
    return False


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("statement", type=Path,
                    help="bank export: .qbo/.ofx/.qfx (preferred) or .csv")
    ap.add_argument("--emit-plan", type=Path, metavar="FILE",
                    help="write matched proposals for qbo_record_payments.py")
    ap.add_argument("--no-email", action="store_true",
                    help="print the summary instead of emailing it")
    ap.add_argument("--no-state", action="store_true",
                    help="skip Neon entirely — no dedupe, nothing recorded")
    ap.add_argument("--all", action="store_true",
                    help="re-propose bank lines already seen in earlier runs")
    args = ap.parse_args(argv)

    if not args.statement.exists():
        print(f"No such file: {args.statement}")
        return 1

    cfg = qbo_api.config()
    missing = qbo_api.missing_config(cfg)
    if missing:
        print(f"Missing QBO config: {', '.join(missing)}")
        return 1

    txns = parse_statement(args.statement)
    if not txns:
        print("No credit transactions found in that statement.")
        return 1
    print(f"Parsed {len(txns)} deposit(s) from {args.statement.name}")

    if not args.no_state:
        ensure_tables()
        if not args.all:
            seen = known_fingerprints()
            before = len(txns)
            txns = [t for t in txns if t.fingerprint() not in seen]
            if before != len(txns):
                print(f"  skipping {before - len(txns)} already seen in earlier runs "
                      f"(--all to re-propose)")
    if not txns:
        print("Every deposit in this statement has been seen already. Nothing to do.")
        return 0

    token = qbo_api.access_token(cfg)
    invoices = open_invoices(cfg, token)
    print(f"{len(invoices)} open invoice(s) in QuickBooks")
    if not invoices:
        print("Nothing outstanding — no matching to do.")
        return 0

    proposals = match(txns, invoices)

    matched = [p for p in proposals if p.status == "matched"]
    ambiguous = [p for p in proposals if p.status == "ambiguous"]
    unmatched = [p for p in proposals if p.status == "unmatched"]
    total = sum((p.billed for p in matched), Decimal("0"))
    fees = sum((p.fee for p in matched), Decimal("0"))

    print(f"\n  matched      {len(matched):>3}   {_usd(total)} of invoices, "
          f"{_usd(fees)} of fees")
    print(f"  ambiguous    {len(ambiguous):>3}   need a human decision")
    print(f"  unmatched    {len(unmatched):>3}   not recognised as A/R")

    for p in matched:
        print(f"    {p.txn.posted_on}  {', '.join(i.doc_number for i in p.invoices):<14} "
              f"{p.invoices[0].customer_name[:26]:<26} "
              f"billed {p.billed:>11}  deposit {p.txn.amount:>11}  "
              f"fee {p.fee:>7}  [{p.basis}]")
    for p in ambiguous:
        print(f"    ? {p.txn.posted_on}  {_usd(p.txn.amount):>12}  {p.note}")

    if not args.no_state:
        save(proposals)

    if args.emit_plan:
        count = emit_plan(proposals, args.emit_plan)
        print(f"\nWrote {count} entr{'y' if count == 1 else 'ies'} to {args.emit_plan}")
        print(f"Review it, then:\n"
              f"  python3 scripts/qbo_record_payments.py --plan {args.emit_plan}\n"
              f"  python3 scripts/qbo_record_payments.py --plan {args.emit_plan} --apply")

    run_date = date.today().isoformat()
    if not args.no_email:
        send_email(_html(proposals, run_date), run_date, total)

    return 0


if __name__ == "__main__":
    sys.exit(main())
