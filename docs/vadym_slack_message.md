# Vadym — Slack-ready copy

Two asks on the Management API:

---

Hey Vadym — two API questions as we're building automation:

**1) Placement create/edit routes aren't deployed on our tenant**

We isolated this cleanly using Laravel's own behavior. On our API
router:

- **Registered routes** return JSON errors (validation, auth, not-found by ID)
- **Unregistered routes** return the HTML "Page not Found" page

Same token, same request format:

```
POST /api/{token}/edit_inventory         → 404 JSON  "Inventory not found"       ✓ route exists
POST /api/{token}/create_inventory       → 400 JSON  "The title field required"  ✓ route exists
POST /api/{token}/token_lifetime         → 200 JSON  {...}                       ✓ route exists

POST /api/{token}/edit_placement_video   → 404 HTML page                         ✗ NOT REGISTERED
POST /api/{token}/edit_placement_native  → 404 HTML page                         ✗ NOT REGISTERED
POST /api/{token}/create_placement_video → 404 HTML page                         ✗ NOT REGISTERED
POST /api/{token}/create_placement_native→ 404 HTML page                         ✗ NOT REGISTERED
```

These four routes are in the public Postman collection you sent, but
they aren't registered on our tenant's API. We also tried 12
variations (`edit_placement`, `edit_placement/172`, `placement/edit`,
`update_placement`, `save_placement`, PUT, PATCH, JSON body, token in
body) — all HTML 404.

Need these routes enabled so we can update `is_optimal_price`,
`price`, and `price_country` programmatically. Can you flip the switch
or route this to whoever owns the tenant's API surface? Also, for
banner placements specifically — is the endpoint `edit_placement_video`
(since video is the default per your code) or is there a separate
`edit_placement_banner`?

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
