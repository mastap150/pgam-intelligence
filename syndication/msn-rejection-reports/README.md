# MSN Rejection Reports — drop directory

MSN Partner Hub → Home → **Resolve content issues** → **Download**
exports one of two CSV variants:

- `*-Overview.csv` — aggregate: reason × failure_type × count. Small
  file, fast to load. Populates `pgam_direct.msn_rejection_report`.
- `*-Details.csv` — per-article: one row per rejected doc with
  Document ID, canonical URL, flagged words / image URLs, appeal
  status. Populates `pgam_direct.msn_rejection_docs` — joinable to
  `msn_article_snapshots` (via doc_id) and `boxingnews.articles`
  (via canonical URL).

**Prefer Details.** It's a strict superset of what Overview tells you.

## How to use

1. Download the CSV from Partner Hub. Save into `inbox/`.
2. `git add`, commit, push to `main`.
3. `msn-rejection-csv-loader` workflow fires, routes by filename to
   the right loader, and moves the processed file to
   `archive/{YYYY-MM-DD}/` under an automated commit.

## Idempotent

Same key tuple UPSERTs with latest fields winning. Safe to re-load any
archived file by copying it back into `inbox/`.
