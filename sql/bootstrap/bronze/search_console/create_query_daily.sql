/*
===============================================================================
BOOTSTRAP | BRONZE | GOOGLE SEARCH CONSOLE | QUERY LEVEL
===============================================================================

PURPOSE
- One-time physical table creation for query-level Search Console metrics
- Preserves full source granularity

SOURCE SNAPSHOT ALIGNMENT
Source table:
prj-dbi-prd-1.ds_dbi_improvado_master.google_search_console_query_search_type_tmo

Verified columns include:
- position (FLOAT64)
- sum_position (FLOAT64)
- date_yyyymmdd (INT64)
===============================================================================
*/

CREATE TABLE IF NOT EXISTS
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation
 .sdi_bronze_search_console_query_daily`
(
  -- Identifiers
  account_id STRING,
  account_name STRING,
  site_url STRING,
  page STRING,
  query STRING,
  search_type STRING,

  -- Dates
  date_yyyymmdd INT64,
  date DATE,

  -- Metrics
  clicks FLOAT64,
  impressions FLOAT64,
  position FLOAT64,
  sum_position FLOAT64,

  -- Audit metadata
  __insert_date INT64,
  File_Load_datetime TIMESTAMP,
  Filename STRING
)
PARTITION BY date
CLUSTER BY site_url, page, query;
