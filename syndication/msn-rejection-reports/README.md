# MSN Rejection Reports — drop directory

MSN Partner Hub → Home → **Resolve content issues** → **Download** produces a
CSV named like:

    Content Rejection Report-PGAM Media LLC-All brands-{DATE}-Overview.csv

## How to use

1. Download the CSV from Partner Hub. Save into `inbox/`.
2. `git add`, commit, push to `main`.
3. The `msn-rejection-csv-loader` workflow fires on the push, loads
   the rows into `pgam_direct.msn_rejection_report`, and moves the
   processed file into `archive/{YYYY-MM-DD}/` with an automated commit.

## Why not fully automated?

MSN's per-doc rejection endpoint returns empty `failures[]` under our
current auth — the human "Download" button is the only path to real
data. See `docs/msn-moderation-findings.md` for the 2026-05 hunt.

## Idempotent

Same `(partner_id, window_start, window_end, reason, failure_type)` tuple
UPSERTs with latest count winning. Safe to re-process any archived file
by copying it back into `inbox/`.
