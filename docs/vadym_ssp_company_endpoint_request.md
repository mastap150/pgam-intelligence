# Message to Vadym — SSP Company report endpoint

**Subject:** API endpoint for /ad-exchange/ SSP Company report

---

Hey Vadym,

Thanks again for the help getting the Management API working — token
creation is solid now and we're pulling inventory, placements, and the
`/report` endpoint successfully.

Quick ask on one piece we haven't been able to find in the public API:

We're building automated optimization around the **/ad-exchange/ SSP
Company report** (the view with columns `SSP Company | SSP Requests |
Impressions | SSP Fill Rate | SSP Revenue | DSP Spend | Profit |
Margin | Bid Rate`). Example URL:

    https://ssp.pgammedia.com/ad-exchange/report-8d34378c63feecd56026196c1323c291

That view groups traffic by SSP Company (Illumin, Smaato, Dexerto,
Start.io, OC Media Solutions, PubNative, Media Lab RTB, Daily Motion,
Pijper Publishing, Zoomer Media, Mission Media, RevIQ, WeBlog RTB,
Native supply, …). We want to pull it programmatically so we can
auto-prune dead-weight partners and scale up the winners.

**What we've tried on `/api/{token}/report`:**
- `attribute[]=company_dsp` — returns DSP-side endpoints ✓
- `attribute[]=publisher`   — returns publisher accounts (RevIQ, Aditude, Adapex, etc.) ✓
- `attribute[]=ssp` / `company_ssp` / `ssp_company` / `ssp_partner` / `partner` / `supply` / `integration` / `exchange` / `source` — all reject with `"The selected attribute.0 is invalid."`

**What we need** — one of these would unblock us:
1. The correct `attribute[]` value for the SSP Company grouping on `/api/{token}/report`, **or**
2. A separate endpoint path (e.g. `/api/{token}/ad_exchange_report`) if that report has its own route, **or**
3. If the data is admin-only: confirmation so we know to route around it via DSP-endpoint name parsing.

**Context on why it matters:** we've built two optimizer agents
(`dsp_optimizer` for DSP endpoints, `ssp_company_optimizer` that
reverse-parses the DSP catalog names like `"Magnite - Smaato Display"`
to roll up to SSP Company level). The reverse-parse works but it's
fragile — a direct API would let us build proper automation around the
/ad-exchange/ view with revenue and fill-rate attribution direct from
source.

Thanks!

---

## Supporting context for the ask

**Our Postman collection** (`managemnt-api.postman_collection`) shows
only Inventory, Placement, `/report`, and Reference endpoints. None
surface the SSP Company aggregation. The TB admin UI clearly has it
— we just need the route.
