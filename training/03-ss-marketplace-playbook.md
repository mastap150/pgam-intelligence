# /ss-marketplace Playbook

The self-serve marketplace bridges the DSP UI to canonical ClearLine deal packs. Advertisers pick packs, hit activate, and the flow provisions the campaign side of SpringServe.

## Status

- Marketplace UI: live at `/ss-marketplace` in `pgam-dsp-dashboard`
- ClearLine wiring: shipped 2026-06-23
- **Demo:** works today at `demo.dsp.pgammedia.com/ss-marketplace`
- **Prod:** gated behind `NEXT_PUBLIC_MARKETPLACE_ACTIVATE_ENABLED`
- To flip on for prod, work through the flip checklist below

## Flip checklist (prod enable)

1. Seed real dealIDs into the marketplace pack config (`marketplace/packs/*.ts` or DB, depending on latest impl — verify current source)
2. Confirm SS env (`SPRINGSERVE_API_KEY`, `SPRINGSERVE_BASE_URL`) is prod, not sandbox
3. Smoke test end-to-end: pick a pack → activate → confirm SS demand tag created + inventory group attached + rate correct
4. Verify no gross rate or agency name leaks in the SS record (see security doc)
5. Flip `NEXT_PUBLIC_MARKETPLACE_ACTIVATE_ENABLED=true` in Vercel prod env
6. Redeploy
7. Manual QA on a real advertiser account before announcing

## Known constraints when building packs

- Demand tags require a **DealList** inventory group. If a pack references DomainList or AppBundleList, SS rejects the payload. Default DealList group is `271`.
- Frequency cap payload must use `frequency_cap_value` (SS native field), not `cap`. Anything using `cap` silently no-ops.
- SS `/deals` API isn't exposed publicly — `POST/GET /deals` returns 404. PR #122 auto-deal-list is inert in prod. Real endpoint has to be captured via Chrome DevTools before we can auto-provision new deals from the marketplace. Until then, deals used in packs are pre-seeded manually.

## When a marketplace pack activates

Flow (from advertiser POV):
1. Advertiser browses `/ss-marketplace`
2. Selects a pack (curated bundle of dealIDs + rate + config)
3. Clicks Activate
4. DSP creates a campaign in Neon
5. DSP pushes to SS: campaign + demand tag + inventory group + freq caps
6. Advertiser sees campaign live in their DSP dashboard

If any step fails, the whole activation should roll back cleanly. Check `pushTargetingToClearline` and adjacent code for the current transaction shape.

## Common failure modes

- Frequency cap silently no-ops → check payload field names
- Deal not attached → dealID missing or wrong inventory group type
- Rate wrong in SS → check rate source; must be media-cost CPM, not gross
- Agency name leaks into tag name → hard rule violation, see security doc

## Who to loop in

- Marketplace UI/flow issues → engineering
- Pack curation (which deals, which rates) → Priyesh
- Advertiser-facing packaging (naming, description) → Priyesh
