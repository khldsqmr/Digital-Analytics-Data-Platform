/*
===============================================================================
BOOTSTRAP | BRONZE | GOOGLE SEARCH CONSOLE | SITE TOTALS (ONE-TIME)
===============================================================================

PURPOSE
- Creates the Bronze site-level Search Console table
- Highest aggregation level from source
- Executed ONCE per environment

SOURCE
- Improvado Search Console site totals

===============================================================================
*/

CREATE TABLE IF NOT EXISTS
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_search_console_site_totals_daily`
(
  account_id STRING,
  account_name STRING,
  site_url STRING,

  -- Source date as STRING (kept for lineage)
  date_yyyymmdd STRING,

  -- Derived analytics date
  date DATE,

  clicks FLOAT64,
  impressions FLOAT64,
  sum_position FLOAT64,

  -- Metadata
  __insert_date INT64,
  file_load_datetime TIMESTAMP,
  filename STRING
)
PARTITION BY date
CLUSTER BY account_name, site_url;
