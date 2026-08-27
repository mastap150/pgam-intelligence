# Attune Tag — internal notes

**Internal.** [`INSTALL.md`](./INSTALL.md) is the client-facing half and names no
vendor. This file is honest about what sits underneath.

## What this is

`attune-tag.js` is a first-party wrapper around the Vibe web pixel. The
advertiser installs one PGAM-hosted file and calls `attune('event','lead')`; the
wrapper boots `vbpx.js` underneath and forwards. The vendor snippet never
appears in a client's codebase, their tag manager, or our install docs.

| | Client sees | Actually happens |
|---|---|---|
| Script | `tag.pgammedia.com/attune.js` | loads `tracker.vibe.co/vbpx.js` |
| API | `attune('event','lead')` | `vbpx('event','lead')` |
| ID | "Attune measurement ID" | Vibe pixel ID, per advertiser |

## The one rule: never proxy it server-side

Attribution is IP-based. The ad plays on a household TV; the match happens when
a browser on that same household IP later reaches the tracker. **The beacon has
to originate from the visitor's browser.**

Route it through a PGAM origin — a reverse proxy, an edge worker, a
server-side tag — and the tracker records *our server's* IP. Every conversion
goes unattributed. Not degraded: gone. The vendor's own S2S documentation is
explicit that `ip` must be the end user's, not the server's.

Wrapping is safe. Proxying breaks the product.

## What this does not do

- **It is not a cloak.** `tracker.vibe.co` is in the served JS and in any
  DevTools Network tab. This removes the vendor from the install and from the
  client's source — not from inspection. Don't oversell it internally and don't
  imply otherwise to a client.
- **True first-party masking** would need a CNAME'd subdomain the vendor
  supports and serves certs for. That is a conversation with the Vibe account
  manager, not something to bodge. It is also worth checking their ToS position
  on wrapping before this goes near a second client.

## Hosting

Not yet deployed. It needs to be served from a PGAM-controlled origin:

- `tag.pgammedia.com/attune.js` (referenced throughout the docs)
- long-lived cache with a short `stale-while-revalidate`, `Cache-Control` no
  longer than ~1h so a fix propagates same-day
- CORS not required — it is a classic `<script src>`, not a fetch
- version the file (`attune.js` → pinned `attune-1.0.0.js`) once more than one
  advertiser is live, so a bad push can't take every client's measurement down

## Events

Three types only, mapped straight through: `pageview`, `lead`, `purchase`.
There is no separate "call" event — a call is a `lead`, tagged with
`{ source: 'call_click' }` so it is separable in reporting.

`price_usd` and `purchase_id` are the only `detail` keys read downstream.
Anything else passes through harmlessly and is useful for our own segmentation.

## Calls

Two layers, because neither covers the whole problem:

1. **`tel:` click → lead.** Automatic, no advertiser work, covers most mobile.
   Wired in the tag. Opt out with `data-attune-calls="off"`.
2. **Desk-phone callers.** Invisible to any browser tag. Needs dynamic number
   insertion (CallRail, CallTrackingMetrics) plus a server-side event to
   `https://t.vibe.co/s2s-conversion/events` with `a=lead`.

   **The blocker on (2):** S2S requires the *visitor's* IP, and a call-tracking
   webhook gives you a phone number. It only works if the DNI provider exposes
   the originating web session's IP. Confirm that against the provider's actual
   webhook payload before promising call tracking to a client — do not assume
   it is there.

   S2S events also do not populate retargeting audiences.

## Testing

```sh
./test/run.sh          # or: CHROME=/path/to/chrome ./test/run.sh
```

14 checks in headless Chromium against a **stubbed** vendor script. The runner
refuses to build if the test copy still points at the real tracker — aiming
these at production would fire junk events into a live pixel.

Covered and passing:

- vendor boots once, with the ID from `data-attune-id`
- events queued before the tag lands are replayed
- `lead` forwards; `purchase` carries `price_usd` and `purchase_id` through
- an unknown event name warns and is not forwarded
- a `tel:` click emits exactly one lead — including a click on a nested element
  inside the link, and not once per bubble phase
- a non-`tel:` link emits nothing
- no `data-attune-id` → warns, installs no global, never loads the vendor
- double inclusion initialises once
- no uncaught errors on the page

**Still needs a real staging check.** The stub proves our side of the contract;
it cannot prove the vendor's. Before a client installs this, put it on a staging
page with a real pixel ID and confirm in DevTools that `vbpx.js` fetches and
`s?aid=` fires with the right event name in the payload.
