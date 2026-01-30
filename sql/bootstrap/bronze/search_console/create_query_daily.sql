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

CREATE TABLE IF NOT EXISTS `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_search_console_query_daily`
(
  event_date DATE,
  event_date_yyyymmdd INT64,

  property STRING,
  site_url STRING,
  page STRING,
  query STRING,
  search_type STRING,

  clicks FLOAT64,
  impressions FLOAT64,
  position FLOAT64,
  sum_position FLOAT64,

  -- ingestion metadata
  file_name STRING,
  file_load_datetime TIMESTAMP,
  insert_ts TIMESTAMP,

  -- lineage & control
  source_system STRING,
  record_hash STRING
)
PARTITION BY event_date
CLUSTER BY property, page, query;
