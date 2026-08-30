-- healthnation-affiliate-audit-queries.sql
--
-- The four open items from docs/healthnation-amazon-affiliate-review-2026-08-30.md,
-- plus the placement data the P1 recommendations depend on.
--
-- Run against the healthnation Neon DB (healthnation-web's DATABASE_URL).
-- Paste into the Neon SQL editor, or use the wrapper that formats the output:
--
--   export HEALTHNATION_DATABASE_URL=...
--   python3 scripts/healthnation_affiliate_audit.py
--
-- Read-only. Nothing here writes.
--
-- CONTEXT THAT MAKES THE RESULTS READABLE
-- ───────────────────────────────────────
-- tagAmazonLinks() runs on exactly two render paths in healthnation-web:
--   /[hub]/[slug]      (articles)      page.tsx:71
--   /best/[slug]       (buyer guides)  page.tsx:142
-- It does NOT run on /reviews/[slug]. So an Amazon link sitting in a
-- product_review body is rendered untagged and earns nothing — Q3 exists
-- to size that.


-- ── Q1 ────────────────────────────────────────────────────────────────
-- Content inventory. "How much is actually published?"
SELECT 'articles' AS kind, status, count(*) AS n
  FROM healthnation.articles GROUP BY 1, 2
UNION ALL
SELECT 'buyer_guides', status, count(*)
  FROM healthnation.buyer_guides GROUP BY 1, 2
UNION ALL
SELECT 'product_reviews', status, count(*)
  FROM healthnation.product_reviews GROUP BY 1, 2
ORDER BY 1, 2;


-- ── Q2 ────────────────────────────────────────────────────────────────
-- Amazon link surface, by document type and Amazon domain.
-- How wide is the auto-tagger's reach, and how much of it is non-US?
-- Every row whose host is not exactly 'amazon.com' is a dead click: the
-- tagger stamps the US tag on all 18 matched TLDs (finding 4.4).
WITH docs AS (
  SELECT 'article'        AS kind, slug, status, content_html AS html
    FROM healthnation.articles
  UNION ALL
  SELECT 'buyer_guide', slug, status,
         coalesce(intro_html, '') || ' ' || coalesce(body_html, '')
    FROM healthnation.buyer_guides
  UNION ALL
  SELECT 'product_review', slug, status,
         coalesce(summary_html, '') || ' ' || coalesce(body_html, '')
    FROM healthnation.product_reviews
),
links AS (
  SELECT d.kind, d.slug, d.status, lower(m[1]) AS host
    FROM docs d,
         LATERAL regexp_matches(
           d.html, 'https?://(?:www\.)?(amazon\.[a-z.]{2,9})/', 'gi'
         ) AS m
)
SELECT kind, status, host,
       count(*)              AS link_count,
       count(DISTINCT slug)  AS docs
  FROM links
 GROUP BY 1, 2, 3
 ORDER BY link_count DESC;


-- ── Q3 ────────────────────────────────────────────────────────────────
-- Amazon links that are never tagged, because /reviews/[slug] doesn't call
-- tagAmazonLinks. These are pure leakage — the click leaves the site with
-- no Associate tag at all.
SELECT r.slug, r.status, count(*) AS untagged_amazon_links
  FROM healthnation.product_reviews r,
       LATERAL regexp_matches(
         coalesce(r.summary_html, '') || ' ' || coalesce(r.body_html, ''),
         'https?://(?:www\.)?amazon\.[a-z.]{2,9}/', 'gi'
       ) AS m
 GROUP BY 1, 2
 ORDER BY untagged_amazon_links DESC;


-- ── Q4 ────────────────────────────────────────────────────────────────
-- Disclosure gap (finding 4.5). Published articles carrying Amazon links.
-- /[hub]/[slug] runs the tagger but renders no affiliate disclosure block,
-- so every row here is a live compliance exposure under the Amazon
-- Operating Agreement. Buyer guides are excluded — they link to
-- /affiliate-disclosure at best/[slug]/page.tsx:162.
SELECT a.hub, a.slug, a.published_at, a.has_affiliate_links,
       count(*) AS amazon_links
  FROM healthnation.articles a,
       LATERAL regexp_matches(
         a.content_html, 'https?://(?:www\.)?amazon\.[a-z.]{2,9}/', 'gi'
       ) AS m
 WHERE a.status = 'published'
 GROUP BY 1, 2, 3, 4
 ORDER BY amazon_links DESC;


-- ── Q5 ────────────────────────────────────────────────────────────────
-- Flag consistency. articles.has_affiliate_links defaults FALSE and the
-- generator has to set it deliberately; the tagger doesn't touch it. Rows
-- here have Amazon links but claim they don't, which means any future
-- disclosure logic keyed on the flag would skip exactly the wrong pages.
SELECT a.hub, a.slug, a.status, count(*) AS amazon_links
  FROM healthnation.articles a,
       LATERAL regexp_matches(
         a.content_html, 'https?://(?:www\.)?amazon\.[a-z.]{2,9}/', 'gi'
       ) AS m
 WHERE a.has_affiliate_links = false
 GROUP BY 1, 2, 3
 ORDER BY amazon_links DESC;


-- ── Q6 ────────────────────────────────────────────────────────────────
-- Product catalog by network (finding 4.1). Every row on a network with no
-- live integration is a "Check current price" button earning $0.
-- Skimlinks appears nowhere in healthnation-web/src as of 2026-08-30.
SELECT coalesce(affiliate_network, '(null)') AS network,
       status,
       count(*)                                                  AS products,
       count(*) FILTER (WHERE affiliate_url IS NULL)             AS no_url,
       count(*) FILTER (WHERE affiliate_url NOT ILIKE '%?%')     AS url_has_no_query_params
  FROM healthnation.products
 GROUP BY 1, 2
 ORDER BY products DESC;


-- ── Q7 ────────────────────────────────────────────────────────────────
-- Click volume through the /go bouncer, by network and month.
-- Remember: auto-tagged inline Amazon links never reach the bouncer
-- (finding 4.3), so this undercounts real outbound clicks.
SELECT date_trunc('month', created_at)::date AS month,
       coalesce(affiliate_network, '(null)') AS network,
       count(*)                              AS clicks,
       count(DISTINCT session_id)            AS sessions
  FROM healthnation.affiliate_clicks
 GROUP BY 1, 2
 ORDER BY 1 DESC, clicks DESC;


-- ── Q8 ────────────────────────────────────────────────────────────────
-- Clicks by on-page position. This is the dimension the bouncer was built
-- for and the one that should drive placement decisions.
SELECT coalesce(position_on_page, '(null)') AS position,
       count(*)                             AS clicks,
       count(DISTINCT product_slug)         AS products,
       round(100.0 * count(*) / nullif(sum(count(*)) OVER (), 0), 1) AS pct
  FROM healthnation.affiliate_clicks
 GROUP BY 1
 ORDER BY clicks DESC;


-- ── Q9 ────────────────────────────────────────────────────────────────
-- Top products by click volume, last 90 days. This is the list that
-- decides which brands are worth a direct Impact/ShareASale application
-- (review §5.4) — chase the top 5, ignore the tail.
SELECT c.product_slug,
       p.brand,
       coalesce(c.affiliate_network, '(null)') AS network,
       count(*)                                AS clicks_90d,
       count(DISTINCT c.session_id)            AS sessions,
       count(DISTINCT c.source_url)            AS source_pages
  FROM healthnation.affiliate_clicks c
  LEFT JOIN healthnation.products p ON p.slug = c.product_slug
 WHERE c.created_at >= now() - interval '90 days'
 GROUP BY 1, 2, 3
 ORDER BY clicks_90d DESC
 LIMIT 25;


-- ── Q10 ───────────────────────────────────────────────────────────────
-- Highest-earning-potential source pages: which articles and guides are
-- actually driving outbound clicks. Pair with Q4 — a page that both drives
-- clicks and lacks a disclosure is the first one to fix.
SELECT coalesce(source_url, '(direct)') AS source_page,
       count(*)                         AS clicks,
       count(DISTINCT product_slug)     AS products
  FROM healthnation.affiliate_clicks
 WHERE created_at >= now() - interval '90 days'
 GROUP BY 1
 ORDER BY clicks DESC
 LIMIT 25;
