# CJ Affiliate — API leg

Notes on PGAM's second affiliate network: what CJ's API can and cannot do,
what is built, and what has to happen before its output is trusted.

Written 2026-08-26. Sibling of `docs/impact-affiliate-etl.md`.

---

## 1. Why CJ matters more than impact.com right now

PGAM is an **active CJ publisher** via destination.com — CID `7112482`,
website/PID `101849129`, live since 2026-08-02
(`docs/expedia-affiliate-decision.md`). Hotels.com (advertiserId `1702763`)
and Vrbo are approved and routed through `/api/go/cj/{advertiser}`.

Two things are sitting unclaimed on that account:

- **Chain hotels awaiting approval** — Marriott, Hilton, IHG, Hyatt, all
  registered in destination.com's `cj-advertisers.ts` with `linkId: null`.
- **The gated credit-card catalogue**, which `08_monetization_strategy.md`
  puts at **$100–200 per approved card** and calls the highest-value
  affiliate category in the travel niche by far. CJ serves it through the
  **Automated Offer Feed**.

By contrast, impact.com appears in the strategy docs only as a *potential*
route (Garmin 4–8%, Chase Sapphire "via CJ/Impact"). The impact.com ETL is
worth having, but CJ is where the account already exists and the money
already is.

## 2. What CJ's API can and cannot do

From CJ's published REST overview, for the publisher side:

| API | Use |
|---|---|
| **Advertiser Lookup** | advertisers **joined and not joined**, with program details — the discovery surface |
| **Link Search** | links by keyword, category, relationship status, link type — where a real `linkId` comes from |
| **Automated Offer Feed** | credit-card content, links and images from financial advertisers, served via Link Search |
| Commission Detail (GraphQL) | earnings. The REST version is deprecated — removed 2019-06-01 |

**There is no endpoint that joins a program.** Applying is a UI act, and this
repo deliberately does not simulate one:

- Automating it means driving `members.cj.com` with a script, which risks the
  account that is *currently earning* on Hotels.com and Vrbo.
- Each application is a representation in PGAM's name about traffic and
  promotional methods, and those answers differ per program.
- The categories worth having are manually reviewed. A bulk-application
  pattern is precisely what gets a publisher declined from the financial
  vertical — the one category with the most upside here.

So the tooling finds and ranks programs, and a human clicks apply. That is
one click per program, against a shortlist that explains its own ordering.

## 3. Two API quirks that bite silently

**Parameter encoding.** CJ requires a space to encode as `+` and a literal `+`
to encode as `%2B`. Several standard URI helpers get exactly this pair wrong —
they percent-encode the space and pass `+` through, which CJ then reads as a
space. `encode_cj()` handles it, and `_request` builds the query string itself
so `requests` cannot re-encode over the top.

**Three different 401s.** CJ answers 401 for three unrelated causes:

| Body | Actual cause |
|---|---|
| *(empty)* | the resource **URL** is wrong — CJ does not 404 |
| `You must specify a developer key.` | no credential sent |
| `Not Authenticated: xxxxxx` | credential sent and rejected |

`_auth_error_kind()` names which one, because reporting a bare 401 sends you
to the credential — right only one time in three.

## 4. The credential hazard

> `Not Authenticated: xxxxxx` — **where `xxxxxx` is the key you sent.**

CJ echoes the credential back inside the error body. Anything that logs or
stores that body verbatim writes a live token into Render's log stream, which
is how a secret escapes a system that never committed one. And a key that has
since been rotated is still a disclosed key once it is in a log.

So `core/cj_api.py` runs every error body through `_redact()` before it is
logged, stored, or even used to construct the exception — `CjError.body` is
redacted at `__init__`. `_redact` also catches the documented echo shape
independently of what token is currently configured, and strips
`key=`/`token=`/`authorization=` from query strings, which is how someone
debugging by hand will paste it.

**Do not add a code path that prints a raw CJ response body.**

## 5. What is built

| File | Role |
|---|---|
| `core/cj_api.py` | Transport + reads. Bearer auth, retry/throttle, XML **and** JSON parsing, field-name tolerance, redaction. |
| `scripts/cj_probe.py` | Read-only probe: auth, relationships by state, ranked shortlist, offer-feed gate check. |
| `tests/test_cj.py` | 25 offline checks. |
| `.env.example`, `render.yaml` | Wiring. |

No new dependencies — XML is parsed with the stdlib, and CJ bodies are small.

### XML parsing is not incidental

CJ answers **XML** on endpoints that ignore `Accept`. `_parse` handles both
formats, and `_xml_to_records` finds the record container in two steps:
CJ's own paging attributes (`total-matched` / `records-returned`) first, then
a structural fallback that descends through single-child wrappers and stops at
mixed-tag children.

Both halves are load-bearing, and both failures are silent:

- Treating `<cj-api><advertisers>` as the record container yields **one** row
  with every advertiser mashed together.
- Picking the container by child count makes a lone advertiser carrying three
  `<action>` children parse as **three commissions and no advertiser**.

Each has its own test.

## 6. Verification status

`core/cj_api.py` was written from CJ's published REST overview, **not against
a live account** — the CJ hosts are unreachable from this repo's cloud
sessions and no CJ credential existed when it was written.

Confirmed from CJ's own documentation: the auth header, PAT-or-Developer-Key,
the three 401 bodies, the encoding rule, the publisher API list, and the
absence of a join endpoint.

Unconfirmed, and what the probe settles: the exact endpoint hosts and paths,
whether any endpoint honours `Accept: application/json`, the real field
spellings per account, which relationship states CJ actually returns, and
whether the offer feed is open to this publisher.

**Tested:** 25 offline checks — redaction (five of them), the 401
disambiguation, the encoding rule, XML record detection including both silent
failure modes above, namespaced bodies, and the unconfigured-read guard. Plus
an end-to-end probe run against a fake CJ speaking XML, which produced a
correct ranking and correctly read an empty offer feed as *not approved*.

**Not tested:** anything requiring the real API.

## 7. First run

```bash
export CJ_PERSONAL_ACCESS_TOKEN=...   # members.cj.com → Account → Manage API Keys
export CJ_COMPANY_ID=7112482
export CJ_WEBSITE_ID=101849129

python3 scripts/cj_probe.py                          # auth only
python3 scripts/cj_probe.py --relationships          # states — incl. the pending chains
python3 scripts/cj_probe.py --shortlist --top 25     # what to apply to
python3 scripts/cj_probe.py --offers                 # is the card vertical open?
```

`--offers` returning **empty means NOT APPROVED** for the financial vertical,
not that no offers exist. The two look identical from the API and imply
completely different next steps.

The probe exits non-zero if a critical field (`advertiser_id`, `name`,
`relationship`) fails to resolve, since those are what a shortlist row needs
to be actionable at all.

## 8. Follow-ups, in order

1. **Status watcher** — poll relationships, Slack on `pending → joined` or
   `declined`. This is what surfaces Marriott/Hilton/IHG/Hyatt the moment they
   flip, instead of someone remembering to check.
2. **Auto-wire on approval** — on `joined`, pull the advertiser's link ids via
   Link Search, write the `cj-advertisers.ts` entry, open a draft PR against
   `mastap150/destination-com`. A human merges.
3. **Post-wire verification** — assert `/api/go/cj/{advertiser}` still 302s
   and the tracking parameter survives the redirect, so a wired advertiser
   cannot be silently untracked.
4. **Commission ETL** — the GraphQL Commission Detail API into Neon, as a
   ledger not a rollup, for the same reason as impact.com: affiliate
   commissions reverse. See `docs/impact-affiliate-etl.md` §4.

Items 2 and 3 need `mastap150/destination-com` added to the session; it also
already writes an `affiliate_clicks` row via its own click bouncer, which is
first-party click data and better than any vendor report for EPC.

Shortlist scoring is deliberately crude for now — EPC dominates, network rank
breaks ties, and there is **no content-fit weighting**, because that needs
destination.com's own top-page data rather than a guess. The probe prints the
reason behind every rank so the ordering can be argued with.
