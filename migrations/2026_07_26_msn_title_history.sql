-- migrations/2026_07_26_msn_title_history.sql
--
-- Surfaces MSN-title change history from the existing per-snapshot
-- table so we can attribute readCount deltas to specific title edits.
--
-- No new writer needed: msn_article_snapshots already stores (doc_id,
-- snapshot_at, msn_title, read_count). This migration adds two views
-- on top of that data.
--
-- Rationale — as of 2026-07-26 only 1 of 3,369 recent docs had >1
-- title version. That's not because the data is missing; it's because
-- nobody's been editing MSN titles. Building this view is the first
-- step toward using MSN's per-article title-edit lever, which is the
-- primary A/B knob on the platform.

CREATE OR REPLACE VIEW pgam_direct.msn_title_history AS
WITH ranked AS (
  SELECT
    s.doc_id,
    s.msn_title,
    s.snapshot_at,
    s.read_count,
    LAG(s.msn_title) OVER (PARTITION BY s.doc_id ORDER BY s.snapshot_at) AS prev_title
  FROM pgam_direct.msn_article_snapshots s
),
transitions AS (
  SELECT
    doc_id,
    prev_title  AS old_title,
    msn_title   AS new_title,
    snapshot_at AS changed_at,
    read_count  AS read_count_at_change
  FROM ranked
  WHERE prev_title IS NOT NULL
    AND prev_title <> msn_title
)
SELECT
  t.doc_id,
  t.old_title,
  t.new_title,
  t.changed_at,
  t.read_count_at_change,
  (
    SELECT MAX(s2.read_count)
    FROM pgam_direct.msn_article_snapshots s2
    WHERE s2.doc_id      = t.doc_id
      AND s2.snapshot_at < t.changed_at
      AND s2.msn_title   = t.old_title
  ) AS old_title_peak_reads,
  (
    SELECT MAX(s3.read_count)
    FROM pgam_direct.msn_article_snapshots s3
    WHERE s3.doc_id      = t.doc_id
      AND s3.snapshot_at >= t.changed_at
      AND s3.msn_title   = t.new_title
  ) AS new_title_peak_reads
FROM transitions t
ORDER BY t.changed_at DESC;

COMMENT ON VIEW pgam_direct.msn_title_history IS
'Per-(doc_id, title-transition) history reconstructed from msn_article_snapshots. Each row = one MSN title edit with the peak read_count before and after the change, so the weekly review can attribute reads to specific title variants.';

-- Companion view: which currently-live articles are underperforming
-- and are prime candidates for a MSN title rewrite. Anchored to
-- msn_article_peak (the operational rollup). Emits doc_id + title
-- + peak_reads + a proxy percentile within the last-14-day cohort.

CREATE OR REPLACE VIEW pgam_direct.msn_title_change_candidates AS
WITH recent AS (
  SELECT
    partner_id,
    doc_id,
    latest_msn_title,
    peak_read_count,
    first_seen_at,
    last_seen_at
  FROM pgam_direct.msn_article_peak
  WHERE last_seen_at >= now() - INTERVAL '14 days'
),
cohort AS (
  SELECT
    NTILE(4) OVER (ORDER BY peak_read_count DESC)      AS quartile,
    NTILE(100) OVER (ORDER BY peak_read_count DESC)    AS pct_rank,
    r.*
  FROM recent r
),
-- Median PV within cohort for the "underperforming vs cohort" test
med AS (
  SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY peak_read_count) AS cohort_median
  FROM recent
)
SELECT
  c.partner_id,
  c.doc_id,
  c.latest_msn_title,
  c.peak_read_count,
  c.first_seen_at,
  c.last_seen_at,
  c.quartile,
  c.pct_rank,
  m.cohort_median,
  ROUND(EXTRACT(EPOCH FROM (now() - c.first_seen_at)) / 3600.0, 1) AS age_hours,
  CASE
    WHEN c.peak_read_count < m.cohort_median * 0.3 THEN 'severe'
    WHEN c.peak_read_count < m.cohort_median * 0.6 THEN 'moderate'
    ELSE 'not_underperforming'
  END AS underperformance_severity
FROM cohort c CROSS JOIN med m
WHERE c.peak_read_count < m.cohort_median * 0.6
  AND EXTRACT(EPOCH FROM (now() - c.first_seen_at)) / 3600.0 > 6      -- old enough to have had a real shot
ORDER BY (m.cohort_median - c.peak_read_count) DESC;

COMMENT ON VIEW pgam_direct.msn_title_change_candidates IS
'Underperformers (relative to 14d cohort median) that have been live >6h. Consumed by weekly-review to recommend MSN title rewrites via Partner Hub.';
