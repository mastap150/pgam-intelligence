# Vadym — Slack-ready copy

Two asks on the Management API:

---

Hey Vadym — two API questions as we're building automation:

**1) `edit_placement_*` endpoints return HTTP 404**

All three placement-edit routes 404 with an HTML "Page not Found" page,
not a JSON error. Tested on dozens of placements across multiple
accounts (BoxingNews, OP.gg, rough_ros, whitepages, metatft, wowhead,
GeeksForGeeks, etc.) — same 404 every time.

- `POST /api/{token}/edit_placement_video`   → 404 HTML
- `POST /api/{token}/edit_placement_native`  → 404 HTML
- `POST /api/{token}/edit_placement_banner`  → 404 HTML  *(not in Postman collection — is there an equivalent for banner type?)*

The token is valid (same-request reads work), payload matches Postman
(`Content-Type: application/x-www-form-urlencoded`, `placement_id` +
fields). `edit_inventory` works fine with the same token, so write
access exists at inventory level. Can you check whether
`edit_placement_*` is gated for our credential, or if there's a
different endpoint for banner placements? This blocks automated
`is_optimal_price`, `price`, and `price_country` updates.

**2) SSP Company report attribute**

The `/ad-exchange/` report (the one grouped by SSP Company — Illumin,
Smaato, Dexerto, Start.io, OC Media, PubNative, etc.) — is that
exposed via `/api/{token}/report`? We tried every likely attribute
(`ssp`, `company_ssp`, `ssp_company`, `ssp_partner`, `partner`,
`supply`, `exchange`, `integration`, `source`) and all come back with
`"The selected attribute.0 is invalid."`.

Only `publisher`, `company_dsp`, `domain`, `placement`, `inventory`,
`country` etc. work — none give the SSP Company rollup.

Either the right `attribute[]` value, a separate endpoint, or a
confirmation that it's admin-only would unblock us. Thanks!

---

## Context (for us, not for Vadym)

Dry-run of optimal_price_sweep shows the scale of the opportunity:
- 238 placements account-wide with `is_optimal_price=False`
- Combined ~8M impressions / 14d
- Includes high-value units (GeeksForGeeks 300x250 at $2.20 eCPM,
  rough_ros at $1.28 eCPM, or_primary-over-header-1 at $1.34)

Every one of these would get TB's own yield ML the moment
`edit_placement_*` works.
