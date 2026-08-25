# destination.com has no inbound mailbox

Found 2026-08-25, after a freelance applicant (Points & Miles columnist,
"Thursday Dispatch") reached us through the on-site contact form to say that
`careers@destination.com` — the only route the careers page offers — bounced,
and that a second editorial address she found bounced too.

## What is actually broken

Every address `destination.com` advertises is on the **apex** domain, and the
apex has no inbound mail. Outbound site mail is a separate, working path:
`deals@mail.destination.com` (ESP subdomain) → lands at `info@pgammedia.com`.
Sending works; receiving was never set up.

Addresses advertised on the live site, all presumed dead:

| Address | Page | Consequence |
|---|---|---|
| `careers@destination.com` | `/careers` | **All 5 open roles.** `mailto:` is the only apply route — every applicant bounces |
| `corrections@destination.com` | `/about` | Corrections policy is unreachable; this is the second address the applicant tried |
| `privacy@destination.com` | `/privacy` | Privacy policy names it for data-subject requests — a GDPR/CCPA exposure, not just a nuisance |
| `legal@destination.com` | `/terms` | Notices under the Terms cannot be served |

`jane@example.com` (`/contact`) and `jane@acme.com` (`/advertise`) are form
input placeholders, not advertised addresses. Not a problem.

Confirmed by fetching the live pages; the careers page carries
`mailto:careers@destination.com` 14 times, once per role plus the page intro.

## Blast radius

Silent. A bounce goes to the sender, never to us, so there is no record of how
many applications were lost across the five roles. In the last 120 days exactly
one contact-form enquiry came from an applicant — the one who worked around the
bounce herself. Anyone less persistent is simply gone.

## Fix

### Forms, not published addresses — for two of the four

`/contact`, `/about` and `/advertise` already share one form component (name,
email, message; `/advertise` adds Company), posting to a handler that emails
`info@pgammedia.com` with a `Source:` tag naming the page. That is the path
that rescued the applicant this was found through. A `careers` variant is a
small, well-precedented change.

- **`careers@` → form.** Strictly better than `mailto:`: no bounce risk, and
  the submission arrives structured. Add a role selector (the five open roles)
  and link fields for portfolio/clips/LinkedIn. Skip file upload for now —
  freelance and editorial applicants link their work, and upload means storage,
  size limits and virus scanning for little gain.
- **`corrections@` → form.** Better structured as a form anyway: which page,
  what is wrong, source.
- **`privacy@` → keep a real address.** The privacy policy names it for
  data-subject requests. A form is generally accepted, but a request sent by
  email is still valid however it reaches us, and a silent bounce means a
  missed statutory deadline. This one must actually receive.
- **`legal@` → keep a real address.** Same reasoning; service of notice under
  the Terms should not depend on a form being up.

### Add the aliases regardless

Removing a `mailto:` from a page does not unpublish the address. It is in
search caches, in any job board that scraped the listing, and in the saved
mail of everyone who already copied it. Those messages keep arriving and keep
dying silently — which is the exact failure this document exists to record.

In Google Workspace, add `careers@`, `corrections@`, `privacy@`, `legal@` as
aliases or groups on `destination.com` delivering to `info@pgammedia.com`.
This needs apex MX records for `destination.com` pointing at Google — check
what is there now (`dig MX destination.com`) before assuming aliases alone are
enough. Do not touch the `mail.` subdomain records; outbound sending depends
on them.

A domain-wide catch-all to `info@pgammedia.com` would cover these four plus
anything anyone ever guesses, at the cost of more spam reaching the inbox.
Reasonable for a domain this size, but the four explicit aliases are the
safer default.

**Verify** by sending live mail to each of the four addresses and confirming
arrival — not by reading DNS and assuming.

## Why this is written down

The careers page is the kind of asset nobody re-tests after launch. The failure
is silent and self-concealing: the people who could tell us it is broken are
exactly the people whose mail did not arrive.
