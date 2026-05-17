# BoxingNews MSN Insights — operations guide

End-to-end view of the MSN syndication telemetry stack. Scoped to
BoxingNews (partner `AA1lKiff`); the same code can ingest other MSN
partners by swapping the `--partner-id` arg.

## Why this exists

MSN pays BoxingNews $4 CPM on article page views. Partner Hub's UI
only exposes a rolling 24h window with no historical view. There's
a 50–100× spread between the worst- and best-performing articles in
that window. Without our own time-series, we have no way to learn what
makes the high-traffic articles work — i.e. no way to grow MSN PVs
intentionally.

This stack is **Phase 1** of the MSN PV growth plan (memory:
`boxingnews_msn_syndication.md`). It lands the data needed for the
Phase 2 headline A/B work that follows.

## Architecture

```
                MSN Partner Hub (SPA)
                    ↑ user logs in once
                    │
                    │   ┌─── api.msn.com /msn/v0/pages/ugc/insights/content/realtime
                    │   │
            Playwright Chromium
        (~/.pgam/msn-session/ persists)
                    │
                    ▼
   agents/etl/msn_insights_etl.py        every 15 min
       ├─ ensures schema (CREATE IF NOT EXISTS)
       ├─ paginates the 24h rolling window
       ├─ writes pgam_direct.msn_article_snapshots (per snapshot)
       ├─ upserts  pgam_direct.msn_article_meta    (seed pending docs)
       ├─ tries the daily-aggregate endpoint and writes
       │           pgam_direct.msn_daily_totals
       └─ logs to  pgam_direct.msn_pull_runs
                    │
                    ▼
   agents/enrichment/msn_doc_resolver.py  every 30 min
       └─ for each pending docID: fetch the MSN public page, parse
          out the boxingnews.com canonical URL + og:image, update
          pgam_direct.msn_article_meta
                    │
                    ▼
   admin.pgammedia.com  /admin/msn-insights
       ├─ /api/reporting/msn-insights (server/msn-insights.ts)
       └─ Page: cards + daily trend + headline patterns + top articles
```

## Tables

All in schema `pgam_direct` (same DB the existing partner-revenue
dashboard uses).

| Table | Cardinality | Purpose |
|---|---|---|
| `msn_article_snapshots` | ~12K rows/day | Per-snapshot per-article time series |
| `msn_article_meta` | ~120 rows/day new | One row per docID; backfilled canonical URL |
| `msn_daily_totals` | ~30 rows/day | Per (date, contentType) aggregate |
| `msn_pull_runs` | ~96 rows/day | Run log; freshness indicator |
| `msn_article_peak` (view) | — | MAX(read_count) per doc; 30d window |

Full DDL in `migrations/2026_05_16_msn_insights.sql`. The ETL agent
ensures the same schema on every run via `CREATE TABLE IF NOT EXISTS`,
so applying the SQL file manually is optional.

## First-run bootstrap

The very first run needs an interactive browser so the user can complete
the MSN login (and MFA if prompted).

```bash
# 1. Install Playwright Chromium (~300MB, one-time)
pip install -r requirements.txt
python -m playwright install chromium

# 2. Fill .env with MSN_EMAIL and MSN_PASSWORD (already done if you
#    set them via the earlier .env.example workflow).

# 3. First run: visible browser, dry-run so we don't write yet
MSN_HEADLESS=0 python -m agents.etl.msn_insights_etl --dry-run
```

A Chromium window opens, MSAL redirects you to login, you complete it
once. After Partner Hub loads, the script polls the realtime endpoint,
prints a top-10 summary to stdout, and exits without writing to Neon.

```bash
# 4. Validate Neon connectivity (this writes 1 batch of real rows)
python -m agents.etl.msn_insights_etl
# Look for "Upserted N rows" + check pgam_direct.msn_article_snapshots
```

```bash
# 5. Run the resolver to backfill canonical URLs for the docIDs we just saw
python -m agents.enrichment.msn_doc_resolver --batch 50
```

## Recurring operation

### Option A — local (Mac, easy)

Add to scheduler.py (already wired; flip the env flag):

```bash
# .env
PGAM_MSN_PULLER_ENABLED=1
```

When `scheduler.py` is running locally, the puller will tick every
15 min and the resolver every 30 min. Default off in Render because
Chromium can't fit on the free Python tier.

### Option B — GitHub Actions (preferred for prod)

`.github/workflows/msn-insights.yml` runs every 15 min. To enable:

1. Push the repo to a private GitHub repo (or use an existing one).
2. Add three repository secrets:
   - `PGAM_DIRECT_DATABASE_URL` — the Neon DSN
   - `MSN_EMAIL`
   - `MSN_PASSWORD`
3. Bootstrap the session cache once:
   - On a workstation, run the first-run flow above with
     `MSN_SESSION_DIR=$(pwd)/.msn-session MSN_HEADLESS=0`.
   - tar + base64 the session: `tar c .msn-session | base64 | pbcopy`
   - Locally: `gh secret set MSN_SESSION_TAR_B64 < pasted` (or set
     via the web UI). The current workflow doesn't read this secret;
     bootstrap is left as a manual step until we standardise on a
     session-injection mechanism. See `.github/workflows/msn-insights.yml`
     for the structure.

### Option C — separate Render worker

If we ever want long-running Playwright on Render, use a Docker service
(NOT the Python service):

```dockerfile
FROM mcr.microsoft.com/playwright/python:v1.40.0-jammy
COPY . /app
WORKDIR /app
RUN pip install -r requirements.txt
ENV PGAM_MSN_PULLER_ENABLED=1
CMD ["python", "scheduler.py"]
```

## Troubleshooting

### `MSN API returned 401 Unauthorized`

The persistent Playwright profile exists but isn't authenticated.
Re-run with `MSN_HEADLESS=0` once, complete login, then revert.

### `playwright not importable`

Run `pip install playwright && python -m playwright install chromium`.
The agent self-skips with this error message when the import fails,
so the scheduler stays healthy.

### `MSN API /...realtime returned HTTP 403`

Could mean the apikey rotated (we hard-code one captured from DevTools
on 2026-05-16) or the partner account has been suspended. Re-capture
the apikey by re-running the DevTools trace in
`boxingnews_msn_syndication.md` and update `APIKEY` in
`core/msn_partner_hub.py`.

### Aggregate endpoint always 404s

The aggregate endpoint path wasn't confirmed from the user's DevTools
trace. The client tries three candidate paths and the puller continues
on without it (realtime is the load-bearing endpoint). To fix, capture
the request URL when the Partner Hub "Overview" tab loads and add it
to `_URL_CANDIDATES` in `core/msn_partner_hub.py` → `fetch_aggregate`.

### docID resolver finds no boxingnews link

MSN sometimes serves a minimal "preview" page to bot-like UAs. The
resolver uses a real Chrome UA but you may need to capture the
"View original" link selector from a logged-in MSN session and update
`_BOXINGNEWS_HREF_RE` or add an explicit `<a>`-tag parser.

## Tests

`python tests/test_msn_insights.py` — no network, no DB. Validates the
data-transformation layer against fixtures captured on 2026-05-16.

## Useful queries

### Total PVs (and est revenue) in the last 24h

```sql
WITH peak AS (
  SELECT doc_id, MAX(read_count) AS peak
    FROM pgam_direct.msn_article_snapshots
   WHERE snapshot_at > now() - interval '48 hours'
   GROUP BY doc_id
)
SELECT
  SUM(peak)                          AS total_pvs_last_48h,
  ROUND(SUM(peak) * 0.004::numeric, 2) AS est_revenue_usd
FROM peak;
```

### Top 10 articles by peak readCount, with the boxingnews URL if resolved

```sql
SELECT p.peak_read_count, p.latest_msn_title, m.canonical_url
  FROM pgam_direct.msn_article_peak p
  LEFT JOIN pgam_direct.msn_article_meta m USING (doc_id)
 ORDER BY p.peak_read_count DESC
 LIMIT 10;
```

### Pull health — were we silent for any 15-min gap in the last 24h?

```sql
SELECT started_at, ok, error_message
  FROM pgam_direct.msn_pull_runs
 WHERE started_at > now() - interval '24 hours'
 ORDER BY started_at DESC;
```

## Future work (Phase 2 onward)

- **`msnHeadline` Sanity field** — add an optional override on the
  BoxingNews article schema. RSS feed reads it for `<title>` when set,
  falls back to canonical headline. Dashboard already surfaces the
  MSN vs canonical title delta — once the override field exists, the
  dashboard becomes a real A/B harness.
- **Per-article daily endpoint** — there's likely a Partner Hub
  endpoint that exposes per-article-per-day metrics (we currently
  reconstruct this from the rolling 24h snapshots). Capture its
  URL/response from DevTools and add a fetch method.
- **Sanity join** — the BoxingNews articles table lives in a separate
  Neon project (TBD location); once known, join `msn_article_meta.canonical_url`
  against it to enrich the dashboard with publish_time, category,
  fighter tags.
- **Event-driven content generator** — fight-card calendar →
  T-48h/-24h/0/+6h auto-spawn the formats the dashboard shows
  outperform (predictions, LIVE, "what's next", weekend preview
  listicles). Volume × winning-formats = Phase 3 PV ramp.
