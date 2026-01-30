/*
===============================================================================
BRONZE | GOOGLE SEARCH CONSOLE | SITE TOTALS | ONE-TIME BOOTSTRAP
===============================================================================
-- BOOTSTRAP: Google Search Console – Site Totals (Bronze)
-- TABLE: sdi_bronze_search_console_site_totals_daily
--
-- PURPOSE:
--   Stores raw site-level daily Search Console metrics.
--   Mirrors Improvado source schema exactly.
--
-- SOURCE:
--   ds_dbi_improvado_master.google_search_console_site_totals_tmo
--
-- GRAIN:
--   date × site_url
--
-- NOTES:
--   - `sum_position` is preserved (no `position` exists upstream)
--   - No transformations or derivations in Bronze
-- ============================================================
*/

CREATE TABLE IF NOT EXISTS
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_search_console_site_totals_daily`
(
  -- Identifiers
  account_id STRING,
  account_name STRING,
  site_url STRING,

  -- Logical event date (Search Console date)
  event_date DATE,

  -- Metrics
  clicks FLOAT64,
  impressions FLOAT64,
  sum_position FLOAT64,

  -- Raw source date fields (preserved)
  date_yyyymmdd INT64,

  -- Audit metadata
  __insert_date INT64,
  file_load_datetime TIMESTAMP,
  filename STRING
)
PARTITION BY event_date
CLUSTER BY site_url;