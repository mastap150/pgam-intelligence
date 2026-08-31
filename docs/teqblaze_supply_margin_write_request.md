# Message to Teqblaze — supply-source margin is read-only over the API

**Subject:** `SupplySourceRequest` omits margin_type / margin_min / margin_max

---

Hi,

One gap we've hit on the new platform (`api.pgammedia.com`) that we think
may be an oversight rather than a deliberate restriction.

**A supply source's margin cannot be set over the API.** The three fields
come back on a read and are refused on a write:

| schema | `margin_type` | `margin_min` | `margin_max` |
|---|---|---|---|
| `SupplySourceResource` (GET) | ✓ | ✓ | ✓ |
| `SupplySourceRequest` (POST `/update`) | ✗ | ✗ | ✗ |

`SupplySourceRequest` declares exactly eleven properties — `companies`,
`company_id`, `demand_sources`, `is_allowed_sources`, `name`, `region_id`,
`source`, `status`, `test_mode`, `type`, `uuid` — and none of them is
margin-shaped. Confirmed against the live account on supply source 264,
which returns `margin_type: "range", margin_min: 7, margin_max: 35` on a
GET and drops all three on the round trip.

**What makes us think it is an oversight:** the demand side does not have
this restriction. `DemandSourceRequest` accepts all three fields, and
`DemandSourceResource - DemandSourceRequest` is only
`{id, operation_systems, uuid}`. So the same three fields are writable on
one entity and not the other, on a platform where both are configured the
same way in the UI.

Placements look the same as supply: `PlacementListResource` and the
per-format placement resources all carry
`margin_status` / `margin_type` / `margin_min` / `margin_max`, but the only
placement endpoint is
`POST /supply-sources/{id}/placements/{placement_id}/status`, which toggles
status and nothing else.

**Three questions:**

1. Is the omission from `SupplySourceRequest` intentional? If supply margin
   is deliberately dashboard-only, we will build around that — we just want
   to stop looking for the endpoint.

2. If it is not intentional, can `margin_type` / `margin_min` /
   `margin_max` be added to `SupplySourceRequest`, the same way they exist
   on `DemandSourceRequest`?

3. Is there a separate endpoint for supply margin that is not in the
   OpenAPI spec we were given? We searched it for any path containing
   "margin" and found none.

**Why it matters to us.** Our two largest supply sources are realising well
under the book average and both sit inside bands that already permit more:

| id | name | realised | configured band |
|---|---|---|---|
| 65 | Illumin - Video Unruly OTTA | 12.5% | adaptive 5–95 |
| 194 | Illumin Display and Video EU | 7.6% | range 2–30 |

Neither needs a band change — just a margin move inside the existing band.
Doing that by hand in the dashboard is fine once; doing it as part of an
automated optimisation loop is not, and that loop is the reason we asked
for API access in the first place.

---

## One separate question, on how margin is computed

While checking the above we found something we cannot explain from the
settings alone, and it may be a definition difference rather than a bug.

We compute realised take rate as
`(dsp_price_sum - ssp_price_sum) / dsp_price_sum` from the `/report`
endpoint. Comparing that against each source's configured margin, **every
source that lands outside its configured band lands above it, never
below** — and the fixed-margin ones are strikingly consistent:

| id | name | configured | realised |
|---|---|---|---|
| 310 | Cox Media Group | fixed 10% | 14.0% |
| 1260 | decoist.com | fixed 10% | 15.8% |
| 1599 | bigmoneysmallmoney.com | fixed 10% | 15.2% |
| 1032 | TravelReveal | fixed 10% | 15.3% |
| 66 | Stirista - Illumin Banner copy1 | fixed 15% | 34.5% |
| 76 | Start.IO Video | fixed 7% | 9.4% |

Four sources configured at a flat 10% all realising 14–16% is too tidy to
be four independent misconfigurations. Our working assumption is that the
realised figure includes a spread that `margin_type` does not govern — a
platform fee, or smart-floor spread landing on top of the configured
margin. (`is_smart_floor` is `true` on every source we have checked.)

Could you confirm what sits between the configured margin and the realised
`dsp_price_sum - ssp_price_sum`? We would rather understand the
relationship than treat the gap as an error and "fix" settings that are
working as designed.

Thanks,
PGAM
