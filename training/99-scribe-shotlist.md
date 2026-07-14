# Scribe Shotlist

Ordered list of screen recordings to make in Scribe. Each entry lists: what to record, start point, end point, who watches this in onboarding.

**How to work through this:** open the target tool, click Scribe extension → Record. Do the flow start-to-end at normal speed. Scribe auto-generates the step-by-step. Rename the Scribe to match the entry title, drop the shareable link into the relevant playbook `.md` next to the topic it documents.

## Priority 1 — the fire-risk SOPs (do these first)

- [ ] **DSP: Wizard → SS campaign build (full flow)**
  Start: DSP dashboard home. End: SS shows demand tag live with correct rate + inventory group.
  Audience: every engineer + trafficker

- [ ] **DSP: QA a wizard payload against the field-mapping drops**
  Start: SS demand tag detail page. End: verified frequency cap uses `frequency_cap_value`, inventory group is DealList, rate is media-cost not gross.
  Audience: trafficker

- [ ] **Monday: close a ticket via CLI + via UI**
  Start: terminal + Monday web tab. End: item in Done column.
  Audience: everyone

- [ ] **Freeze list check before writing to a partner**
  Start: open `core/partner_freeze.py`. End: know how to add a partner to the freeze list and how to verify current freezes.
  Audience: engineer, ops

## Priority 2 — supply platform ops

- [ ] **LL: check current floor for a demand partner**
  Start: LL UI. End: floor value known + noted.

- [ ] **LL: verify an automation change against the ledger**
  Start: scheduler log. End: ledger row + LL UI state cross-checked.

- [ ] **TB: read live state from Neon `pgam_direct`**
  Start: Neon console. End: sample query result showing current TB state.

- [ ] **TB: diagnose a 401 error in job logs**
  Start: scheduler log with 401. End: identified cause (token / IP / endpoint) + fix path.

- [ ] **Partner Revenue Dashboard: walk through daily LL + TB check-in**
  Start: `admin.pgammedia.com`. End: daily numbers vs $10K/$15K targets known.

## Priority 3 — DSP demand ops

- [ ] **DSP: `/ss-marketplace` activation flow (demo env)**
  Start: `demo.dsp.pgammedia.com/ss-marketplace`. End: campaign activated + verified in fixtures.

- [ ] **DSP: enable marketplace prod (flag flip checklist)**
  Start: Vercel env. End: `NEXT_PUBLIC_MARKETPLACE_ACTIVATE_ENABLED=true`, smoke test done.

- [ ] **DSP: buyer agent — read a suggestion and apply/reject**
  Start: buyer agent view. End: lever applied, verified in SS.

- [ ] **DSP: loosen freq cap for an under-pacing campaign**
  Start: campaign underdelivering. End: freq cap raised (e.g., 3/1 → 10/1), pacing improved.

- [ ] **DSP: check a campaign for agency-name / gross-rate leakage in SS**
  Start: SS campaign detail. End: verified clean.

- [ ] **DSP demo env: hand-off to a sales prospect**
  Start: demo URL. End: prospect walked through the UI with fixtures, no live data exposed.

## Priority 4 — engineering ops

- [ ] **Create a new worktree on pgam-dsp-dashboard (with env symlinks)**
  Start: main tree. End: worktree with `node_modules` + `.env.local` symlinks, `next build` passes.

- [ ] **Vercel: roll back a bad deploy on DSP**
  Start: Vercel deployments page. End: previous good deploy promoted.

- [ ] **Vercel: deploy a preview and verify `NEXT_PUBLIC_API_URL` is set correctly**
  Start: PR opened. End: preview URL loads without DOCTYPE errors.

- [ ] **Neon: safe read-only query on prod schema**
  Start: Neon console. End: query returned, verified read-only role used.

## Priority 5 — content ops

- [ ] **BoxingNews: Sanity Studio publish flow**
  Start: Sanity Studio. End: article live on boxingnews.com.

- [ ] **BoxingNews: MSN session recovery**
  Start: 401 error on MSN job. End: session bootstrapped via `msn-bootstrap.yml` workflow_dispatch + confirmed live.

- [ ] **HubSpot: enroll a contact in a cold outbound sequence**
  Start: HubSpot deal. End: contact enrolled in Instantly via handoff.

- [ ] **HubSpot: work the `pgam_outbound_*` pipeline stages**
  Start: pipeline view. End: know each stage's meaning + who moves cards.

## Priority 6 — finance / partner mgmt

- [ ] **QBO: create a partner payout invoice**
  Start: QBO dashboard. End: invoice sent.

- [ ] **Managed Stripe invoicing (`/admin/invoicing/managed`)**
  Start: admin panel. End: client-billable Stripe invoice sent.

- [ ] **Invoca: intake a new advertiser's per-partner API key**
  Start: advertiser onboarding form. End: API key stored, first reading pull successful.

- [ ] **pgam-recon: run monthly SSP reconciliation**
  Start: recon repo. End: month closed, discrepancies flagged.

## Priority 7 — first-day setup (for new hire on their own laptop)

- [ ] **Get 1Password + PGAM vaults access**
- [ ] **Clone the repos you need (`pgam-intelligence`, and role-specific)**
- [ ] **Set up Node v24, Python 3.12, Homebrew**
- [ ] **Configure git with your name + email; add SSH key to GitHub**
- [ ] **Get Vercel org invite accepted**
- [ ] **Get Monday.com invite + set up API token if applicable**
- [ ] **Get SpringServe UI access if applicable (trafficker role)**
- [ ] **Get Neon read-only access (engineer role)**
- [ ] **Get Slack invite + join relevant channels**

---

## Scribe hygiene rules

- Record at normal speed, one flow per Scribe.
- Never record with a gross CPM or agency name visible on screen — pause, blank the field, resume.
- Never record with real advertiser PII visible — use the demo env or fixtures.
- Prefer the demo env (`demo.dsp.pgammedia.com`) for anything that would otherwise show real client data.
- Title = the entry title in this file, verbatim, so linking is easy.
