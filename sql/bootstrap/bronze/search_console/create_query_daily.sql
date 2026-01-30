/* ===============================================================================
BRONZE | GOOGLE SEARCH CONSOLE | QUERY LEVEL | ONE-TIME BOOTSTRAP
===============================================================================
-- BOOTSTRAP: Google Search Console – Query + Search Type (Bronze)
-- TABLE: sdi_bronze_search_console_query_daily
--
-- PURPOSE:
--   Stores raw query-level Search Console metrics.
--   Preserves full diagnostic fidelity.
--
-- SOURCE:
--   ds_dbi_improvado_master.google_search_console_query_search_type_tmo
--
-- GRAIN:
--   date × site_url × page × query × search_type
--
-- NOTES:
--   - BOTH `position` and `sum_position` are preserved
--   - No averaging or derivation in Bronze
-- ============================================================
*/
CREATE TABLE IF NOT EXISTS
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_search_console_query_daily`
(
  -- Identifiers
  account_id STRING,
  account_name STRING,
  site_url STRING,

  -- Dimensions
  page STRING,
  query STRING,
  search_type STRING,

  -- Logical event date
  event_date DATE,

  -- Metrics
  clicks FLOAT64,
  impressions FLOAT64,
  position FLOAT64,
  sum_position FLOAT64,

  -- Raw source date fields
  date_yyyymmdd INT64,

  -- Audit metadata
  __insert_date INT64,
  file_load_datetime TIMESTAMP,
  filename STRING
)
PARTITION BY event_date
CLUSTER BY site_url, page, query;