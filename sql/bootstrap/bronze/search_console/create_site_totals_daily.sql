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

  -- Date fields
  date INT64,              -- GSC numeric date (days since epoch)
  date_yyyymmdd INT64,     -- YYYYMMDD representation

  -- Metrics
  clicks FLOAT64,
  impressions FLOAT64,
  sum_position FLOAT64,    -- Aggregated position from source

  -- Metadata / audit
  __insert_date INT64,
  File_Load_datetime TIMESTAMP,
  Filename STRING
)
PARTITION BY
  DATE(PARSE_DATE('%Y%m%d', CAST(date_yyyymmdd AS STRING)))
CLUSTER BY
  site_url;
