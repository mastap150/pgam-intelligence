# Discrepancy import dry-run — pre-launch plan

The `/api/discrepancy/import` endpoint accepts CSV statements from DSPs
and reconciles them against our internal `financial_events`. **It has
never been driven with a real DSP statement format**. Each DSP's CSV
schema is different. Before launch, verify that at least the top 3 DSPs
we plan to transact with parse correctly.

## Pre-flight

- [ ] Confirm `/api/discrepancy/import` route is `internal_admin/finance` only
- [ ] Confirm import is idempotent (re-uploading same file = no duplicates)
- [ ] Confirm import surfaces parse errors line-by-line (not silently dropped rows)

## DSP CSV format intake

For each DSP in your **target day-1 demand set** (probably top 3-5,
not all 16 — see #3 audit), get one month of statement and verify:

### Magnite (id=5)
- Format: typically Excel `.xlsx` with sheet "Publisher Detail"
- Columns: `Date | Publisher ID | Domain | Impressions | Spend | eCPM`
- **Required transformation**: convert XLSX → CSV with normalized headers
- Test row: at minimum, paid_impressions, gross_revenue_usd

### PubMatic (id=4)
- Format: API JSON pull or daily emailed CSV
- Columns: `Day | Publisher | Site | Imps | Revenue | CPM`
- **Required transformation**: API auth via OAuth client_credentials
- Test row: paid_impressions, gross_revenue_usd

### Verve (id=1)
- Format: CSV email
- Columns: `Date | Site | App | Impressions | Net Revenue`
- **Required transformation**: header rename (`Net Revenue` → `gross_revenue_usd`)

## Drill steps

1. **Get statements** from each of top-3 DSPs for last month (ask AM contact)
2. **Manually transform** each to canonical CSV:
   ```
   report_date,dsp_id,publisher_id,placement_ref,impressions,gross_revenue_usd
   2026-03-15,5,1,demo_hero,1234,12.34
   ```
3. **POST to** `/api/discrepancy/import` with admin cookie
4. **Verify** `/api/discrepancy/our-counts` returns matching impression count for the same window
5. **Verify** `/api/discrepancy/compare` produces diff payload — ideally diff is < 5% (if > 5%, investigate before declaring this DSP transactable)
6. **Repeat** for each DSP

## Pass condition

- All 3 statements parse without errors
- Diff vs internal counts is < 5% per DSP
- Date-range math holds (no off-by-one on UTC vs partner timezone)

## Likely failures and fixes

- **CSV header case sensitivity**: implement case-insensitive header match
- **UTF-8 BOM**: strip on parse
- **Currency formatting**: `$1,234.56` vs `1234.56` — canonicalize
- **Partner timezone offset**: most DSPs report in their local timezone, not UTC

## Output

Once dry-run passes for top-3 DSPs, write a **per-DSP statement-format
crib sheet** in `docs/runbooks/discrepancy-formats.md` so the next person
running reconciliation doesn't have to rediscover each format.
