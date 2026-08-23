# Vadym — read-only API user for the new platform

**To:** vadym.kolisnichenko@teqblaze.com
**Cc:** kostiantyn.diachenko@teqblaze.com
**Subject:** API user for api.pgammedia.com + a few integration questions

Send-ready copy, same pattern as `docs/vadym_slack_message.md` and
`docs/vadym_ssp_company_endpoint_request.md`. Everything below is triaged from
`docs/teqblaze-new-platform.md` §8.1 — only the items Teqblaze can actually
answer, ordered by what is blocking us.

Context for whoever sends this: our client is written and tested against the
vendored spec but has never authenticated against the live host, so the first
three questions are the ones gating everything. §8.2 of that doc lists what the
API answers by itself — deliberately not asked here.

---

Hey Vadym,

We've built out our integration against the new platform's Management API
(the `Pgam v1.11.15` spec you sent) and we're ready to start pulling real
data. A few things we need from your side before we do.

**1) A dedicated read-only API user.**

Could you create an API user for us on `api.pgammedia.com`, scoped to
**reporting and entity reads only** — no write access? We'd rather not run
automation on a person's dashboard login: we want to be able to rotate the
credential without locking anyone out, and to have the audit trail show
"integration" rather than a colleague's name.

If the roles are configurable on your side, could you tell us **which
permission set corresponds to read-only**? We'll be calling
`GET /permissions` first thing, so we'll see what the account licenses — but
knowing the intended role name helps us tell "scoped too tightly" apart from
"module not provisioned".

We'll ask for a second, write-capable user separately when we're ready to
make changes through the API. Not yet.

**2) Do the `/update` endpoints replace the whole object, or patch it?**

`POST /supply-sources/{id}/update` and `POST /demand-sources/{id}/update`
both look like they take a complete entity. If that's a full replace, then
any field we leave out gets blanked — which on a live supply source means
silently dropping floors, allowed-demand lists or IAB blocks. So concretely:

- Can we `GET /supply-sources/{id}`, strip the read-only fields, and POST
  the rest back? And if so, **exactly which fields must be stripped**?
- A canonical round-trip example would settle it completely.

Related, and this one looks like it might be an oversight on your end:
comparing the read and write schemas, **`DemandSourceRequest` doesn't accept
`uuid`, but `SupplySourceRequest` does** — even though `uuid` comes back on
both resources. Is that intentional? We're stripping it on demand sources for
now, but if it's meant to be accepted we'd rather not be sending a different
shape than you expect.

Our write path stays locked until we have an answer here.

**3) JWT lifetime and concurrent sessions.**

The spec's example token expires in an hour and we don't see a refresh
endpoint, so we're assuming re-login. Can you confirm — and more importantly:
**does a second `/login` on the same user invalidate the first token?** If it
does, a scheduled job and an ad-hoc query running at the same time would
knock each other out, and we'd want separate users rather than a shared token
cache.

**4) Rate limits.**

Requests per second, concurrent queries per user, any per-endpoint caps on
reporting. Does a throttled request return `429`, and does it carry
`Retry-After`? The old platform allowed one concurrent query per user, so
we're currently serialising calls with a 0.25s gap as a guess — happy to
loosen that if there's room, or tighten it if we're being optimistic.

---

Three smaller ones, whenever convenient:

**5) Are your own optimisers currently on for any of our sources?**
Specifically `is_smart_floor`, `is_dynamic_margin`, and
`qps_limit.qps_optimization_by`. If the platform is already moving a floor or
a QPS envelope, we don't want to put our own automation on the same lever —
two controllers on one floor is a bad time. We just need to know which are
enabled and on what cadence so we can divide them up.

**6) Publisher and demand-source IDs — did those carry across?** You confirmed
placement IDs are unchanged and inventory IDs are new. Our daily reporting
tables key on `publisher_id` and `demand_id`, which is neither of those, so
we'd like to know before we join anything: are publisher and demand-source
IDs the same as on the old platform?

**7) Is there a sandbox or test account?** It'd be the cheapest way to answer
question 2 without either of us touching live money.

Thanks — and no rush on 5 through 7, the first four are what we're waiting on.

Priyesh
