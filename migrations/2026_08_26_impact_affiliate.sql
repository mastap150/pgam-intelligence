-- impact.com affiliate leg — raw action ledger + derived daily rollups.
--
-- Shape differs deliberately from every other ETL in this repo, because
-- affiliate revenue differs in one way that matters: IT GOES BACKWARDS.
--
-- An ad impression is final the moment it is counted. An affiliate action is
-- not: a conversion recorded in March can be REVERSED in June, when the
-- shopper returns the item or the advertiser rejects the sale. The programmatic
-- ETLs (tb_daily_*, tbx_daily_*, ll_daily_*) can therefore write pre-aggregated
-- daily rows and refresh a trailing 14-day window, because nothing older than
-- that window ever changes.
--
-- Doing that here would ratchet revenue permanently upward: the reversal
-- arrives after the window has moved past the action's date, so the day keeps
-- the payout it lost. Slowly, silently, and in the direction that flatters us.
--
-- So this stores one row per ACTION, keyed on the action's own id, and derives
-- the daily rollups as VIEWS. A reversal seen at any age UPSERTs the same row
-- and every rollup corrects itself in the same instant. There is exactly one
-- copy of each number, and it is the vendor's.
--
-- Volume makes that affordable: affiliate conversions are thousands per month,
-- not the 400k rows/hour the LL ETL moves. The cost of a view over a ledger is
-- nothing at this scale, and it buys correctness that a rollup table cannot.
--
-- Written 2026-08-26 without a live account (api.impact.com is unreachable
-- from cloud sessions and no IMPACT_* credential existed yet), which is why
-- `raw` keeps the whole vendor payload: a field this mapping got wrong is then
-- fixable in SQL, from data already landed, instead of by re-pulling months.
-- See docs/impact-affiliate-etl.md.

CREATE SCHEMA IF NOT EXISTS pgam_direct;

CREATE TABLE IF NOT EXISTS pgam_direct.impact_actions (
  -- Vendor id, text not bigint: impact.com action ids are not reliably
  -- numeric across account configurations, and a cast that works today would
  -- fail closed on the first alphanumeric order id.
  action_id          text            NOT NULL,

  campaign_id        bigint,
  campaign_name      text,
  tracker_id         bigint,
  tracker_name       text,

  -- The date the conversion happened, and therefore the date its revenue
  -- belongs to. NOT the date we learned about it — see modification_date.
  event_date         date            NOT NULL,
  creation_date      timestamptz,
  -- When impact.com finalises the action. Past this point it cannot reverse,
  -- which is what makes state='LOCKED' the only invoiceable number.
  locking_date       timestamptz,
  modification_date  timestamptz,
  referring_date     timestamptz,

  -- PENDING | APPROVED | REVERSED | LOCKED (uppercased at write time).
  state              text,

  payout             numeric(14, 4)  NOT NULL DEFAULT 0,
  sale_amount        numeric(14, 4)  NOT NULL DEFAULT 0,
  -- Two currencies, deliberately unconverted. This repo has no FX source, and
  -- a hardcoded rate would turn a reporting question into a silent accounting
  -- error. The views group BY currency so a mixed-currency account cannot
  -- produce a meaningless total.
  currency           text,
  payout_currency    text,

  -- SubId1 is how a publisher with several properties splits revenue by site.
  -- PGAM runs healthnation.com, destination.com, boxingnews, … so this is the
  -- column that answers "which site earned this" — and it is populated only
  -- where the tracking links actually set it. Unset means unattributable, not
  -- zero.
  sub_id1            text,
  sub_id2            text,
  sub_id3            text,

  promo_code         text,
  customer_country   text,
  referring_domain   text,

  -- The vendor payload as received. Cheap at this volume and the reason a
  -- wrong column mapping is a SQL fix rather than a re-pull.
  raw                jsonb           NOT NULL DEFAULT '{}'::jsonb,

  first_seen_at      timestamptz     NOT NULL DEFAULT now(),
  updated_at         timestamptz     NOT NULL DEFAULT now(),

  PRIMARY KEY (action_id)
);

CREATE INDEX IF NOT EXISTS impact_actions_event_date_idx
  ON pgam_direct.impact_actions (event_date DESC);
CREATE INDEX IF NOT EXISTS impact_actions_campaign_date_idx
  ON pgam_direct.impact_actions (campaign_id, event_date DESC);
CREATE INDEX IF NOT EXISTS impact_actions_state_idx
  ON pgam_direct.impact_actions (state);
CREATE INDEX IF NOT EXISTS impact_actions_sub_id1_idx
  ON pgam_direct.impact_actions (sub_id1)
  WHERE sub_id1 IS NOT NULL;
-- Reversal sweeps and freshness checks both key on this.
CREATE INDEX IF NOT EXISTS impact_actions_modified_idx
  ON pgam_direct.impact_actions (modification_date DESC);

COMMENT ON TABLE pgam_direct.impact_actions IS
'One row per impact.com affiliate action (conversion), keyed on the vendor action id. Actions REVERSE months after their event_date, so this is a ledger UPSERTed by id — the daily rollups are views over it, never separate tables. gross affiliate revenue = payout; only state=LOCKED is final.';

COMMENT ON COLUMN pgam_direct.impact_actions.event_date IS
'Date the conversion occurred — the date its revenue belongs to. Distinct from modification_date, which is when the record last changed.';

COMMENT ON COLUMN pgam_direct.impact_actions.state IS
'PENDING and APPROVED can still reverse. LOCKED cannot, and is the only state safe to invoice against.';

COMMENT ON COLUMN pgam_direct.impact_actions.raw IS
'Vendor payload as received. Present because the column mapping was written without a live account; a mis-mapped field is recoverable from here.';


-- ---------------------------------------------------------------------------
-- Derived rollups.
--
-- Views, not tables. A reversal UPSERTs one ledger row and every number below
-- corrects itself — there is no second copy to fall out of date, and no
-- backfill window wide enough to be wrong.
--
-- Every grain carries payout_currency. Summing payouts across currencies is
-- meaningless and this repo has no FX source, so the currency is part of the
-- grain rather than something a consumer can forget to filter on.
-- ---------------------------------------------------------------------------
-- A note on CREATE vs REPLACE: these are DROP + CREATE, not CREATE OR REPLACE.
-- `CREATE OR REPLACE VIEW` refuses to change a view's column list, so the day
-- a column is added or renamed here, every ETL run would fail on the migration
-- it re-applies each pass. The whole file executes inside one transaction
-- (agents/etl/impact_revenue_etl.ensure_tables), so the drop and the create are
-- atomic: a reader sees the old view or the new one, never a missing one.
-- Deliberately NOT `CASCADE` — if something else ever depends on one of these,
-- the drop should fail loudly rather than quietly delete it.
-- ---------------------------------------------------------------------------

DROP VIEW IF EXISTS pgam_direct.impact_daily_campaign_revenue;
CREATE VIEW pgam_direct.impact_daily_campaign_revenue AS
SELECT
    a.event_date                                              AS report_date,
    a.campaign_id,
    max(a.campaign_name)                                      AS campaign_name,
    coalesce(a.payout_currency, a.currency, 'UNKNOWN')        AS payout_currency,
    count(*)                                                  AS actions_total,
    count(*) FILTER (WHERE a.state = 'REVERSED')              AS actions_reversed,
    -- Net of reversals: what we currently believe we earned that day.
    coalesce(sum(a.payout) FILTER (WHERE a.state <> 'REVERSED'), 0)  AS payout_net,
    -- Final. The only column an invoice should reference.
    coalesce(sum(a.payout) FILTER (WHERE a.state = 'LOCKED'), 0)     AS payout_locked,
    coalesce(sum(a.payout) FILTER (WHERE a.state = 'APPROVED'), 0)   AS payout_approved,
    coalesce(sum(a.payout) FILTER (WHERE a.state = 'PENDING'), 0)    AS payout_pending,
    -- Kept visible rather than netted away: a rising reversal rate on a
    -- program is a reason to stop promoting it, and it is invisible if the
    -- only number carried is the net.
    coalesce(sum(a.payout) FILTER (WHERE a.state = 'REVERSED'), 0)   AS payout_reversed,
    coalesce(sum(a.sale_amount) FILTER (WHERE a.state <> 'REVERSED'), 0) AS sale_amount_net,
    max(a.updated_at)                                         AS last_seen_at
FROM pgam_direct.impact_actions a
GROUP BY a.event_date, a.campaign_id,
         coalesce(a.payout_currency, a.currency, 'UNKNOWN');

COMMENT ON VIEW pgam_direct.impact_daily_campaign_revenue IS
'Daily affiliate payout per impact.com program, derived live from impact_actions so reversals of any age are reflected immediately. payout_net is current belief; payout_locked is final. Grain includes payout_currency — do not sum across it.';


DROP VIEW IF EXISTS pgam_direct.impact_daily_property_revenue;
CREATE VIEW pgam_direct.impact_daily_property_revenue AS
SELECT
    a.event_date                                              AS report_date,
    -- '(unset)' rather than NULL: an action whose tracking link carried no
    -- SubId1 is unattributable to a site, and that is a fact worth seeing in
    -- the output instead of a row that disappears from a GROUP BY join.
    coalesce(nullif(trim(a.sub_id1), ''), '(unset)')           AS property,
    coalesce(a.payout_currency, a.currency, 'UNKNOWN')        AS payout_currency,
    count(*)                                                  AS actions_total,
    count(*) FILTER (WHERE a.state = 'REVERSED')              AS actions_reversed,
    coalesce(sum(a.payout) FILTER (WHERE a.state <> 'REVERSED'), 0)  AS payout_net,
    coalesce(sum(a.payout) FILTER (WHERE a.state = 'LOCKED'), 0)     AS payout_locked,
    coalesce(sum(a.payout) FILTER (WHERE a.state = 'REVERSED'), 0)   AS payout_reversed,
    coalesce(sum(a.sale_amount) FILTER (WHERE a.state <> 'REVERSED'), 0) AS sale_amount_net,
    count(DISTINCT a.campaign_id)                             AS campaigns,
    max(a.updated_at)                                         AS last_seen_at
FROM pgam_direct.impact_actions a
GROUP BY a.event_date,
         coalesce(nullif(trim(a.sub_id1), ''), '(unset)'),
         coalesce(a.payout_currency, a.currency, 'UNKNOWN');

COMMENT ON VIEW pgam_direct.impact_daily_property_revenue IS
'Daily affiliate payout per PGAM property, split on SubId1 from the tracking link. property=''(unset)'' is revenue that arrived with no site attribution — a tracking-link gap, not zero revenue.';
