-- TBX (new Teqblaze platform, api.pgammedia.com) daily revenue rollups.
--
-- Deliberately SEPARATE tables from pgam_direct.tb_daily_*, not extra rows in
-- them. The two platforms serve the same marketplace, so a union would double
-- count every impression for as long as both legs run — and both legs run
-- until the reconciliation in docs/teqblaze-new-platform.md §7 tranche 1
-- step 2 passes. Keeping them apart is what makes that comparison possible at
-- all: you cannot reconcile two sources you have already merged.
--
-- Column naming follows the legacy tables (gross_revenue / pub_payout) rather
-- than the platform's own (dsp_price_sum / ssp_price_sum) so that reporting
-- which already reads tb_daily_* can point here with no query rewrite. The
-- mapping was settled against a live dashboard on 2026-08-21 and is recorded
-- in docs/teqblaze-new-platform.md §7:
--
--     dsp_price_sum  ("Demand Spend")   -> gross_revenue
--     ssp_price_sum  ("Supply Revenue") -> pub_payout
--
-- Getting that backwards produces a ~22-31% constant offset that reads
-- exactly like a fee applied at a different stage.
--
-- `requests` is bid requests (requests_sum), which is what the QPS efficiency
-- work needs and the legacy tables never carried.

CREATE TABLE IF NOT EXISTS pgam_direct.tbx_daily_supply_revenue (
  report_date    date            NOT NULL,
  supply_id      bigint          NOT NULL,
  supply_name    text,
  impressions    bigint          NOT NULL DEFAULT 0,
  requests       bigint          NOT NULL DEFAULT 0,
  wins           bigint          NOT NULL DEFAULT 0,
  gross_revenue  numeric(14, 4)  NOT NULL DEFAULT 0,
  pub_payout     numeric(14, 4)  NOT NULL DEFAULT 0,
  updated_at     timestamptz     NOT NULL DEFAULT now(),
  PRIMARY KEY (report_date, supply_id)
);

CREATE TABLE IF NOT EXISTS pgam_direct.tbx_daily_demand_revenue (
  report_date    date            NOT NULL,
  demand_id      bigint          NOT NULL,
  demand_name    text,
  impressions    bigint          NOT NULL DEFAULT 0,
  requests       bigint          NOT NULL DEFAULT 0,
  wins           bigint          NOT NULL DEFAULT 0,
  gross_revenue  numeric(14, 4)  NOT NULL DEFAULT 0,
  pub_payout     numeric(14, 4)  NOT NULL DEFAULT 0,
  updated_at     timestamptz     NOT NULL DEFAULT now(),
  PRIMARY KEY (report_date, demand_id)
);

-- Placement grain exists for one specific reason: Teqblaze confirmed on
-- 2026-08-20 that PLACEMENT ids are unchanged across the two hosts, while
-- inventory ids are new and publisher / demand-source ids were not covered
-- either way (docs/teqblaze-new-platform.md §8.1.10b and §8.1.10d). So this
-- is the only grain where a cross-platform join rests on a vendor commitment
-- rather than an assumption, which makes it the grain the reconciliation
-- should key on.
CREATE TABLE IF NOT EXISTS pgam_direct.tbx_daily_placement_revenue (
  report_date    date            NOT NULL,
  placement_id   bigint          NOT NULL,
  placement_name text,
  impressions    bigint          NOT NULL DEFAULT 0,
  requests       bigint          NOT NULL DEFAULT 0,
  wins           bigint          NOT NULL DEFAULT 0,
  gross_revenue  numeric(14, 4)  NOT NULL DEFAULT 0,
  pub_payout     numeric(14, 4)  NOT NULL DEFAULT 0,
  updated_at     timestamptz     NOT NULL DEFAULT now(),
  PRIMARY KEY (report_date, placement_id)
);

CREATE INDEX IF NOT EXISTS tbx_daily_supply_revenue_date_idx
  ON pgam_direct.tbx_daily_supply_revenue (report_date DESC);
CREATE INDEX IF NOT EXISTS tbx_daily_demand_revenue_date_idx
  ON pgam_direct.tbx_daily_demand_revenue (report_date DESC);
CREATE INDEX IF NOT EXISTS tbx_daily_placement_revenue_date_idx
  ON pgam_direct.tbx_daily_placement_revenue (report_date DESC);

COMMENT ON TABLE pgam_direct.tbx_daily_supply_revenue IS
'Daily supply-source rollup from api.pgammedia.com (TBX). Same marketplace as tb_daily_publisher_revenue — do NOT union the two, they double count. gross_revenue=dsp_price_sum, pub_payout=ssp_price_sum.';

COMMENT ON TABLE pgam_direct.tbx_daily_demand_revenue IS
'Daily demand-source rollup from api.pgammedia.com (TBX). Same marketplace as tb_daily_demand_revenue — do NOT union.';

COMMENT ON TABLE pgam_direct.tbx_daily_placement_revenue IS
'Daily placement rollup from api.pgammedia.com (TBX). Placement ids are stable across the legacy and new hosts per Teqblaze 2026-08-20, so this is the grain to reconcile on.';
