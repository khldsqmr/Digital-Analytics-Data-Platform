-- =====================================================================
-- ONE-TIME BOOTSTRAP SQL
-- DO NOT SCHEDULE
-- DO NOT RE-RUN AFTER INITIAL CREATION
--
-- Purpose:
--   Creates the Bronze Search Console query-level daily table.
--
-- Execution:
--   Run manually in BigQuery UI once per environment.
--
-- Dependency:
--   Required before running incremental MERGE logic.
-- =====================================================================

CREATE TABLE IF NOT EXISTS
  `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_search_console_query_daily`
(
  -- Grain columns
  date DATE,
  property STRING,
  page STRING,
  query STRING,
  device STRING,
  country STRING,

  -- Raw metrics
  clicks INT64,
  impressions INT64,
  ctr FLOAT64,
  position FLOAT64,

  -- Metadata
  account_name STRING,
  source STRING,
  ingestion_ts TIMESTAMP
)
PARTITION BY date
CLUSTER BY property, page, query;
