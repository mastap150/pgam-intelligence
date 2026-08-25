-- destination-news-audit-queries.sql
--
-- Fills the data gaps in docs/destination-news-audit-2026-08-25.md.
-- Every "Unresolved" row in that audit is answered by one query here.
--
-- Target: the destination.com Neon database (news_items table).
-- Run:  psql "$DATABASE_URL" -f docs/destination-news-audit-queries.sql
--
-- DATABASE_URL is NOT in this repo and must not be committed. Read it from
-- the environment or Vercel. See CLAUDE.md, "Secrets are not present in
-- this repo, ever".
--
-- All queries are read-only.

\timing on

-- ── A. Pipeline age + lifetime volume ────────────────────────────────
-- Settles the audit's open conflict: code comments date first ingest to
-- 2026-07-28 (~4 weeks), but scripts/send-briefing.mjs claims the cron has
-- "been running for months". Whichever MIN(published_at) says, wins.
SELECT
  MIN(published_at)::date              AS first_article,
  MAX(published_at)::date              AS latest_article,
  (CURRENT_DATE - MIN(published_at)::date) AS days_running,
  COUNT(*)                             AS total_rows,
  COUNT(*) FILTER (WHERE status = 'live') AS live_rows,
  MAX(id)                              AS max_id
FROM news_items;

-- ── B. Volume for 7 / 30 / 90 days ───────────────────────────────────
-- The core "exact breakdown" ask. per_day is the honest cadence number.
-- NOTE: if days_running from (A) is < 90, the 90d row is a partial window
-- and per_day for it is misleading — compare against days_running, not 90.
SELECT
  w.label,
  w.days                                                   AS window_days,
  COUNT(n.id)                                              AS articles,
  ROUND(COUNT(n.id)::numeric / w.days, 2)                  AS per_day,
  ROUND(COUNT(n.id)::numeric / w.days * 7, 1)              AS per_week,
  ROUND(COUNT(n.id)::numeric / w.days * 30, 1)             AS per_month,
  COUNT(DISTINCT n.published_at::date)                     AS days_with_output,
  w.days - COUNT(DISTINCT n.published_at::date)            AS silent_days
FROM (VALUES ('7d', 7), ('30d', 30), ('90d', 90)) AS w(label, days)
LEFT JOIN news_items n
  ON n.status = 'live'
 AND n.published_at > NOW() - (w.days || ' days')::interval
GROUP BY w.label, w.days
ORDER BY w.days;

-- ── C. Consistency: publish hour + day of week ───────────────────────
-- Answers "are we publishing consistently or sporadically?" and feeds the
-- day-of-week / publish-time half of the NewsBreak performance analysis.
-- Expect weekday-heavy: the nine sources are trade press that doesn't file
-- on weekends.
SELECT
  to_char(published_at, 'Dy')                  AS dow,
  EXTRACT(dow FROM published_at)::int          AS dow_num,
  COUNT(*)                                     AS articles,
  ROUND(AVG(length(body_html)))                AS avg_body_chars
FROM news_items
WHERE status = 'live' AND published_at > NOW() - INTERVAL '90 days'
GROUP BY 1, 2
ORDER BY dow_num;

SELECT
  EXTRACT(hour FROM published_at)::int AS utc_hour,
  COUNT(*)                             AS articles
FROM news_items
WHERE status = 'live' AND published_at > NOW() - INTERVAL '90 days'
GROUP BY 1
ORDER BY 1;

-- ── D. Category + region mix ─────────────────────────────────────────
-- Which categories are we actually publishing, and does `industry`
-- (the trade-press bucket the audit recommends demoting) dominate?
SELECT
  COALESCE(category, '(none)')                      AS category,
  COUNT(*)                                          AS articles,
  ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1) AS pct,
  ROUND(AVG(length(body_html)))                     AS avg_body_chars,
  COUNT(*) FILTER (WHERE hero_image_url IS NOT NULL) AS with_hero
FROM news_items
WHERE status = 'live' AND published_at > NOW() - INTERVAL '30 days'
GROUP BY 1
ORDER BY articles DESC;

SELECT
  COALESCE(region, '(none)')  AS region,
  COALESCE(country, '(none)') AS country,
  COUNT(*)                    AS articles
FROM news_items
WHERE status = 'live' AND published_at > NOW() - INTERVAL '30 days'
GROUP BY 1, 2
ORDER BY articles DESC
LIMIT 30;

-- ── E. Sourcing: where stories come from, and rewrite depth ──────────
-- Confirms the audit's "0% original reporting" finding from the data side.
-- sources_per_item = 1 means single-source rewrite, the weakest case.
SELECT
  unnest(source_names)  AS source_outlet,
  COUNT(*)              AS times_used
FROM news_items
WHERE status = 'live' AND published_at > NOW() - INTERVAL '30 days'
GROUP BY 1
ORDER BY times_used DESC;

SELECT
  array_length(source_urls, 1)                       AS sources_per_item,
  COUNT(*)                                           AS articles,
  ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1) AS pct
FROM news_items
WHERE status = 'live' AND published_at > NOW() - INTERVAL '30 days'
GROUP BY 1
ORDER BY 1;

-- ── F. Quality proxies + the image-rights exposure ───────────────────
-- hero_credit is in the schema but persistNews() never writes it, so
-- with_credit should come back 0. That count IS the exposure: every row
-- with a hero and no credit is a third-party image republished uncredited.
SELECT
  COUNT(*)                                              AS live_articles,
  COUNT(*) FILTER (WHERE hero_image_url IS NOT NULL)    AS with_hero,
  COUNT(*) FILTER (WHERE hero_credit IS NOT NULL)       AS with_credit,
  COUNT(*) FILTER (WHERE hero_image_url IS NOT NULL
                     AND hero_credit IS NULL)           AS hero_uncredited,
  COUNT(*) FILTER (WHERE dek IS NULL OR dek = '')       AS missing_dek,
  ROUND(AVG(length(body_html)))                         AS avg_body_chars,
  percentile_cont(0.5) WITHIN GROUP (ORDER BY length(body_html)) AS median_body_chars,
  COUNT(*) FILTER (WHERE body_html LIKE '%<h2%')        AS with_subheads,
  COUNT(*) FILTER (WHERE body_html LIKE '%<ul%'
                      OR body_html LIKE '%<table%')     AS with_list_or_table
FROM news_items
WHERE status = 'live';

-- Kill rate — how much the (currently retrospective) review actually removes.
SELECT status, COUNT(*) AS rows
FROM news_items
GROUP BY status
ORDER BY rows DESC;

-- Which domains are we hotlinking hero images from? Each distinct host is a
-- separate rights-holder to resolve. Prioritise fix #1 by this list.
SELECT
  split_part(split_part(hero_image_url, '//', 2), '/', 1) AS image_host,
  COUNT(*)                                                AS articles
FROM news_items
WHERE status = 'live' AND hero_image_url IS NOT NULL
GROUP BY 1
ORDER BY articles DESC;

-- ── G. Headline-pattern scoring (the Breeze hypothesis, at corpus scale) ──
-- Tests §2 against every article instead of n=2. Join a NewsBreak analytics
-- export on slug and this becomes the editorial model: does
-- disruption-verb + large-geography actually predict views?
WITH scored AS (
  SELECT
    slug,
    headline,
    category,
    region,
    published_at,
    -- Disruption verbs — the Breeze pattern
    (headline ~* '\y(pause|paus|cancel|cut|suspend|drop|end|halt|delay|ground|strike|close|ban|reduce)')
      AS is_disruption,
    -- Expansion verbs — the Southwest-Nashville pattern
    (headline ~* '\y(launch|add|expand|grow|open|resume|introduce|boost|increase)')
      AS is_expansion,
    -- Large US geography = NewsBreak multi-locale fan-out
    (headline ~* '\y(Florida|Texas|California|New York|Hawaii|Alaska|Georgia|Arizona|Nevada|Carolina|Colorado|Southeast|Midwest|West Coast|East Coast|nationwide|U\.S\.|US)\y')
      AS has_large_geo,
    -- Dated window — "through early October"
    (headline ~* '\y(through|until|starting|begins|from|by)\y.*(January|February|March|April|May|June|July|August|September|October|November|December|spring|summer|fall|winter|20[0-9]{2})')
      AS has_date_window,
    -- Countable specifics — "four routes"
    (headline ~* '\y(one|two|three|four|five|six|seven|eight|nine|ten|[0-9]+)\y')
      AS has_number,
    length(headline) AS headline_chars
  FROM news_items
  WHERE status = 'live'
)
SELECT
  is_disruption,
  has_large_geo,
  COUNT(*)                       AS articles,
  ROUND(AVG(headline_chars))     AS avg_headline_chars,
  COUNT(*) FILTER (WHERE has_date_window) AS with_date_window,
  COUNT(*) FILTER (WHERE has_number)      AS with_number,
  array_agg(slug ORDER BY published_at DESC) FILTER (WHERE true) AS example_slugs
FROM scored
GROUP BY is_disruption, has_large_geo
ORDER BY articles DESC;

-- Full per-article scorecard — export this and join NewsBreak views on slug.
-- This is the seed of the §11 dashboard.
WITH scored AS (
  SELECT
    slug, headline, category, region, country, published_at,
    (headline ~* '\y(pause|paus|cancel|cut|suspend|drop|end|halt|delay|ground|strike|close|ban)') AS is_disruption,
    (headline ~* '\y(Florida|Texas|California|New York|Hawaii|Georgia|Arizona|Nevada|Carolina|Colorado|Southeast|Midwest)\y') AS has_large_geo,
    (headline ~* '\y(one|two|three|four|five|[0-9]+)\y') AS has_number,
    length(headline)      AS headline_chars,
    length(body_html)     AS body_chars,
    hero_image_url IS NOT NULL AS has_hero,
    array_length(source_urls, 1) AS n_sources,
    to_char(published_at, 'Dy HH24:00') AS published_slot
  FROM news_items
  WHERE status = 'live'
)
SELECT
  slug, headline, category, region, published_slot,
  is_disruption, has_large_geo, has_number,
  headline_chars, body_chars, has_hero, n_sources,
  -- Predicted-opportunity score, weights from the audit §7
  ( CASE WHEN has_large_geo  THEN 25 ELSE 0 END
  + CASE WHEN is_disruption  THEN 25 ELSE 0 END
  + CASE WHEN has_number     THEN 10 ELSE 0 END
  + CASE WHEN has_hero       THEN 10 ELSE 0 END
  + CASE WHEN category IN ('aviation','visa-policy') THEN 20 ELSE 0 END
  + CASE WHEN n_sources >= 2 THEN 10 ELSE 0 END
  ) AS opportunity_score
FROM scored
ORDER BY published_at DESC;

-- ── H. The two articles in the brief, side by side ───────────────────
-- Everything §2 had to infer from the slug: real headline, dek, body
-- length, structure, hero, sources, exact publish time.
SELECT
  slug, headline, dek, category, region, country, tags,
  published_at, synthesized_at,
  to_char(published_at, 'Dy DD Mon HH24:MI') AS published_readable,
  length(headline)  AS headline_chars,
  length(body_html) AS body_chars,
  -- ~5.5 chars/word incl. markup; rough but comparable between the two
  ROUND(length(regexp_replace(body_html, '<[^>]+>', '', 'g'))::numeric / 5.5) AS approx_words,
  hero_image_url, hero_image_alt, hero_credit,
  source_names, source_urls,
  (body_html LIKE '%<h2%')    AS has_subheads,
  (body_html LIKE '%<ul%')    AS has_list,
  (body_html LIKE '%<table%') AS has_table,
  (body_html LIKE '%<a %')    AS has_links,
  model
FROM news_items
WHERE slug IN (
  'breeze-airways-pauses-four-florida-routes-through-early-october',
  'southwest-nashville-fastest-growing-hub-2250-quarterly-flights'
);

-- ── I. Duplicate pressure ────────────────────────────────────────────
-- How hard the dedup stack is working. High same-brand clustering means
-- the nine sources are covering the same ground — evidence that the real
-- constraint is source diversity, not the 5-clusters-per-run cap.
SELECT
  lower(split_part(headline, ' ', 1)) AS leading_brand,
  COUNT(*)                            AS articles,
  MIN(published_at)::date             AS first_seen,
  MAX(published_at)::date             AS last_seen
FROM news_items
WHERE status = 'live' AND published_at > NOW() - INTERVAL '30 days'
GROUP BY 1
HAVING COUNT(*) > 1
ORDER BY articles DESC
LIMIT 25;
