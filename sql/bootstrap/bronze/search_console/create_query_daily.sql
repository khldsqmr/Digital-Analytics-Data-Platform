/*

PARTITION BY expression must be _PARTITIONDATE, DATE(_PARTITIONTIME), DATE(<timestamp_column>), DATE(<datetime_column>), DATETIME_TRUNC(<datetime_column>, DAY/HOUR/MONTH/YEAR), a DATE column, TIMESTAMP_TRUNC(<timestamp_column>, DAY/HOUR/MONTH/YEAR), DATE_TRUNC(<date_column>, MONTH/YEAR), or RANGE_BUCKET(<int64_column>, GENERATE_ARRAY(<int64_value>, <int64_value>[, <int64_value>]))

===============================================================================
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

  -- Date fields
  date INT64,
  date_yyyymmdd INT64,

  -- Metrics
  clicks FLOAT64,
  impressions FLOAT64,
  position FLOAT64,        -- Raw average position from source
  sum_position FLOAT64,    -- Aggregated position from source

  -- Metadata / audit
  __insert_date INT64,
  File_Load_datetime TIMESTAMP,
  Filename STRING
)
PARTITION BY
  DATE(PARSE_DATE('%Y%m%d', CAST(date_yyyymmdd AS STRING)))
CLUSTER BY
  site_url, page, query;
