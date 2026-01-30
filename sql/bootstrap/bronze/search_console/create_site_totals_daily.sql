/*
===============================================================================
BOOTSTRAP | BRONZE | GOOGLE SEARCH CONSOLE | SITE TOTALS
===============================================================================

PURPOSE
- One-time physical table creation for site-level Search Console metrics
- Must be executed ONCE per environment
- Never scheduled
- Never re-run after creation

SOURCE SNAPSHOT ALIGNMENT
Source table:
prj-dbi-prd-1.ds_dbi_improvado_master.google_search_console_site_totals_tmo

Key columns verified:
- date_yyyymmdd (INT64)
- clicks (FLOAT64)
- impressions (FLOAT64)
- sum_position (FLOAT64)
===============================================================================
*/

CREATE TABLE IF NOT EXISTS
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation
 .sdi_bronze_search_console_site_totals_daily`
(
  -- Identifiers
  account_id STRING,
  account_name STRING,
  site_url STRING,

  -- Dates
  date_yyyymmdd INT64,
  date DATE,

  -- Metrics
  clicks FLOAT64,
  impressions FLOAT64,
  sum_position FLOAT64,

  -- Audit metadata
  __insert_date INT64,
  File_Load_datetime TIMESTAMP,
  Filename STRING
)
PARTITION BY date
CLUSTER BY site_url, account_name;
