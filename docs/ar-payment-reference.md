# Payment reference on invoices

## The line

Add to the customer-facing message on every invoice:

> **Payment reference — please quote `PGAM 1319` in the wire reference field.**
> Wires that arrive without an invoice number can take weeks to identify and
> may be chased as unpaid.

`1319` is the invoice number, substituted per invoice. `PGAM ` is a prefix, not
decoration: it survives bank reference fields that strip or truncate, and it
keeps a bare four-digit number from being mistaken for an account fragment.
Thirteen characters, so it fits even the shortest SWIFT reference field.

## Why

PGAM's receivables problem is not late payment, it is unmatched payment. In
August 2026 three customers were assessed as never having paid when all three
had paid; the wires were sitting unrecognised in the bank feed. $132,884.00 of
invoices ended up being closed by hand.

The difference between an easy match and a hard one is entirely the reference:

| Customer | Wire reference | Outcome |
|---|---|---|
| Experience Ten | `Invoice no.: 1228` | matched immediately |
| Smile Wanted | `PGAM 2026 06` | unmatched for weeks, customer wrongly flagged |

`agents/recon/payment_matcher.py` tries reference matching before anything
else, because a quoted invoice number is *exact*. Its fallback — subset-sum on
the amount, within a plausible bank-fee tolerance — is inference, and inference
is what produced the wrong calls above. Every invoice that carries this line
converts a guess into a string comparison.

## Where it goes

Three places, in order of how much they matter.

### 1. PGAM Admin — the real template (separate repo)

Invoices are created by PGAM Admin through the QBO API, not in the QuickBooks
UI. Their `private_memo` records it:

```
Auto-sent from PGAM Admin · billing_group='blitz' · draft #146 · auto-email
```

The QBO API does **not** populate `CustomerMemo` from the company's default
sales-form message — that default is applied by the QBO UI when a human
composes a form. So an invoice created via the API carries only what the
payload sets. This line has to be added to PGAM Admin's invoice payload
(`CustomerMemo`, with the doc number interpolated) or it will not appear on
any auto-generated invoice, regardless of what the QBO setting says.

Verify on the next generated invoice rather than assuming.

### 2. QuickBooks default sales-form message

Covers invoices raised by hand in the QBO UI. Settings → Account and settings →
Sales → Messages → "Default message shown on sales forms". Not reachable
through the MCP connector; set it in the UI.

Since the UI cannot interpolate the invoice number, use the generic form there:

> Payment reference — please quote your invoice number, prefixed `PGAM`, in the
> wire reference field.

### 3. Invoice reminder email

Currently silent on payment references. The template today reads:

```
Subject: Reminder: Invoice [Invoice No.] from PGAM Media Consulting LLC
Body:    Just a reminder that we have not received a payment for this invoice
         yet. Let us know if you have questions.

         Thanks for your business!
         PGAM Media LLC
```

`[Invoice No.]` is a supported substitution, so the body can carry the exact
reference. Worth adding, because a reminder is the moment a partner is most
likely to be about to pay — and because the reminder may be going to a partner
who has already paid unmatched. Suggested addition:

> When you pay, please quote `PGAM [Invoice No.]` in the wire reference so we
> can match it against your account. If you have already paid, reply with the
> payment date and reference and we will trace it.

Editable in the QBO UI only; `qbo_sales_get_settings` exposes this template
read-only.

## Also worth considering

Several August wires arrived short because correspondent banks deducted fees in
transit — $15.03 to $161.50 per wire, $368.25 unbooked at the time of writing.
If PGAM should receive the invoiced amount in full, the invoice needs to say so
explicitly, e.g. *"All bank charges, including intermediary and correspondent
fees, are for the sender's account."* That is a commercial change to payment
terms, not a formatting one — decide it separately.
