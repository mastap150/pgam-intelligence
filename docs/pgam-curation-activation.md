# PGAM Curation — the write path went live (2026-08-21)

**Companion to `docs/pgam-curation-platform-audit.md`.** That document is the
architecture; this one records what was actually built against it, so a later
session does not have to re-derive from a diff which of §17's MVP items are
done, which are open, and which were deliberately not built.

**Implemented in:** `pgam-dsp-dashboard`, branch
`claude/curation-package-activation-qtwak5` (PR #536).

> Filed as its own document rather than as a fourth addendum inside the audit
> only because the session that wrote it could not push to this repo over git
> and had to use the API, and the audit is 147 KB. Read it as Addendum D.

---

## 1. What changed

The engine described in audit §6 and the schema in §9 were both complete
before this. What did not exist was a single write: nothing in the product had
ever created a `curation_packages` row, so every surface read fixtures and
§0.1's "70% built" was describing a pipeline with no entry point.

That entry point now runs end to end:

```
POST /api/v1/curation/packages
  → planSubmission() re-validated server-side
  → curation_packages + curation_deals
  → ManualFulfilmentAdapter enqueues curation_fulfilment_tasks
  → /admin/curation/queue reads real rows
  → operator pastes the Deal ID → deal goes active → buyer emailed
```

§17.4's MVP promise — *"you get a working Deal ID without emailing PGAM"* — is
now kept by software rather than by a person remembering to send it. The email
is what makes leaving the page a reasonable thing for a buyer to do, and it is
sent from the deliver action.

New modules, all under `src/lib/curation/`:

| Module | Role |
|---|---|
| `submission.ts` | Orchestration: gate → plan → rows → adapters → package status |
| `fulfilment.ts` | Operator claim and deliver |
| `notify.ts` | The buyer's "your Deal ID is ready" email |
| `registry.ts` | Provider → adapter, and per-tenant candidate narrowing |
| `config-schema.ts` | The parse boundary (zod → `NormalizedDealConfig`) |
| `clearing-price.ts` | Observed supply cost, per provider |
| `ui/queue-data.ts` | The operator queue's projection and stats |

Plus `migrations/0148_curation_activation.sql` (three columns) and
`curation.view` / `curation.write` in `src/lib/rbac.ts`.

## 2. Decisions worth not relitigating

**The client's plan is never an input.** The builder computes a plan to render
its preview; the server recomputes it from the config alone with the same
`planSubmission()`. Because both call one function, rejecting the client's
copy costs nothing — there is no honest client whose plan we needed.

**Double-submit is a schema guarantee, not a code path.** A client-supplied
`submission_key` with a partial unique index means a second click replays the
first package. `uq_curation_tasks_live_per_deal` cannot help here: it keys on
`deal_id`, and a retry carries a new one. Same harm as §9.2's ("two Deal IDs
for one deal, and the buyer billed twice"), one layer up.

**Unknown stays unknown, everywhere.** A provider with no usable delivery
history is *absent* from the clearing-price map rather than priced at zero; its
economics columns are written `NULL` rather than `0`; and the buyer is told
their floor could not be confirmed. The operator console's median build time
was a hardcoded `36 min` from the fixtures — which is the number §6.2 says the
buyer-facing ETA should be calibrated from — and is now measured or a muted
dash.

**Email only, and the console says so.** It previously told the operator that
"email, browser push and their Slack channel have gone out". Two of those do
not exist. `notified_at` is stamped only after a successful send, so a provider
outage leaves the deal eligible for a retry rather than permanently marked as
told; and the operator sees the send result separately from the delivery
result, because a delivery whose email failed needs a human to follow up and
one green tick for both facts is how that never happens.

**The queue's loader is server-only.** It is imported by the admin page
directly rather than re-exported through `ui/loader.ts`, which is shared with
client components. Routing it through there for symmetry dragged the manual
adapter's `node:crypto` import into the browser bundle and broke `next build`
outright. The `server-only` marker makes a repeat fail at the import rather
than at webpack's resolver three files from the cause.

## 3. Still open

- **The buyer builder cannot POST yet.** `BuilderScreen` holds format, tier,
  supply path and floor. A valid `NormalizedDealConfig` also needs an
  advertiser, a deal name, flight dates, a DSP seat and market selection.
  Those controls are the deferred builder work; the server accepts the config
  the moment they exist. **This is the one thing between the current state and
  a buyer using the product unaided.**
- **The read/import path is unchanged** — still blocked on one saved
  `reportingSearch` response, per audit Addendum C. `loadLiveDeals()` returns
  `[]`, for a reason now narrower than when it was written: `curation_deals`
  carries rows, but delivered spend, impressions and attention come from a
  reporting join that does not exist.
- **Slack and web push**, per audit §18 Phase 2.
- **No write API on either SSP.** Unchanged, and the reason the manual
  adapter is on the critical path. When an entitlement lands it is still the
  one-flag change §6.2 describes.

## 4. Permission model

`curation.view` / `curation.write` map onto the existing `dsp` feature
(`view` / `edit`). A curation deal spends the tenant's money on the tenant's
inventory, so the people who may build a campaign may build a deal; a separate
feature would have to be granted to every existing user before the product
worked for anyone, and the first fix for that would be to grant it to
everyone.

The per-**tenant** gate is a different question and already lives in
`tenant_settings.curation_enabled`, checked inside `planSubmission()` — so it
is enforced on the server for every submission, not by hiding a nav item.

The operator surface (`/admin/curation/queue` and both fulfilment routes) is
gated on `admin.view` **and** the PGAM-staff assertion, per audit §12: a tenant
holding it could mark their own deal active with an ID of their choosing and
trigger an email saying so.
