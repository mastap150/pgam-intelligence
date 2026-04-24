# T10 — Prebid adapter + docs naming sanity

## Adapter in Prebid.js fork (mastap150/Prebid.js)
- `modules/pgamsspBidAdapter.js` — code: `pgamssp`, endpoint `https://us-east.pgammedia.com/pbjs`, sync `https://cs.pgammedia.com`
- `modules/pgamsspBidAdapter.md` — "PGAMSSP Bidder Adapter", `info@pgammedia.com`

**Only one adapter exists**: biddercode `pgamssp`. Endpoint hostname
`us-east.pgammedia.com` — not `rtb.pgammedia.com` (which is bidder-edge).
Need to confirm DNS + path exists or clarify two-endpoint model.

## Prebid Docs (mastap150/prebid.github.io)
Three competing doc files — all published on docs.prebid.org:
1. `dev-docs/bidders/pgam.md` — biddercode `pgam`
2. `dev-docs/bidders/pgammedia.md` — biddercode `pgammedia`, aliasCode `aniview`
3. `dev-docs/bidders/pgamssp.md` — biddercode `pgamssp`

Only #3 matches any actual adapter in Prebid.js. #1 and #2 advertise bidders
that do not exist in the fork. `pgammedia.md` aliases `aniview` which is even
more misleading.

## Backend service-map identifiers
- `web/src/lib/publishers.ts` / `schema.sql` / `rtb/presets.ts` use `pgamdirect`
  as the canonical identifier.
- `services/bidder-edge/internal/auth/auth.go` uses `pgamdirect` for HMAC
  key prefixes.
- Adapter advertises `pgamssp` in Prebid client code.
- Sellers.json entity is `PGAM Media`.
- Domain used in public traffic: `pgammedia.com`.

## Finding
**FND-008 REPRODUCED**: four distinct identifiers (`pgamdirect`, `pgamssp`,
`pgammedia`, `pgam`) collide in customer-visible surfaces. Publishers wiring
the adapter will read `pgam.md` (wrong code), try `biddercode: pgam` in
their adunit config, and fail silently — the module registers as `pgamssp`.

Recommended remediation:
1. Remove `dev-docs/bidders/pgam.md` and `dev-docs/bidders/pgammedia.md`
   from the docs fork before publishing.
2. Keep `pgamssp` as the single Prebid biddercode.
3. Decide whether internal service map stays on `pgamdirect` or moves to
   `pgamssp` — pick one and rename in all other places.
