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

**Do this first — it fixes all four at once, needs no deploy.** In Google
Workspace admin, add each of `careers@`, `corrections@`, `privacy@`, `legal@`
as an alias or group on `destination.com`, delivering to `info@pgammedia.com`.
That requires apex MX records for `destination.com` pointing at Google — check
what is there now (`dig MX destination.com`) before assuming aliases alone are
enough. Do not touch the `mail.` subdomain records; outbound sending depends
on them.

**Then, in the `destination-com` repo** (separate Vercel repo — not this one):
give `/careers` a real apply route that does not depend on mail delivery. The
contact form already works and lands in the right inbox; the `mailto:` links
should point at it, or at minimum sit alongside it as a fallback.

**Verify** by sending a live message to each of the four addresses and
confirming arrival — not by reading DNS and assuming.

## Why this is written down

The careers page is the kind of asset nobody re-tests after launch. The failure
is silent and self-concealing: the people who could tell us it is broken are
exactly the people whose mail did not arrive.
