-- =====================================================================
-- ONE-TIME BOOTSTRAP SQL
-- DO NOT SCHEDULE
-- DO NOT RE-RUN AFTER INITIAL CREATION
--
-- Purpose:
--   Creates the Bronze Search Console site-level daily totals table.
--
-- Execution:
--   Run manually in BigQuery UI once per environment.
-- =====================================================================

CREATE TABLE IF NOT EXISTS
  `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_search_console_site_totals_daily`
(
  date DATE,
  property STRING,
  device STRING,
  country STRING,

  clicks INT64,
  impressions INT64,
  ctr FLOAT64,
  position FLOAT64,

  account_name STRING,
  source STRING,
  ingestion_ts TIMESTAMP
)
PARTITION BY date
CLUSTER BY property, device;
