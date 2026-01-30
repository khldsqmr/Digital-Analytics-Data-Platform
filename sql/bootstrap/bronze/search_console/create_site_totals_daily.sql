/*
===============================================================================
BRONZE | GOOGLE SEARCH CONSOLE | SITE TOTALS | ONE-TIME BOOTSTRAP
===============================================================================

PURPOSE
- Creates the Bronze table for SITE-LEVEL Search Console metrics
- This table represents AGGREGATED metrics per site per day
- Parent table for query-level detail

GRAIN (Natural Key)
- account_name
- site_url
- date

DESIGN PRINCIPLES
- Preserve upstream aggregation exactly as received
- No derived metrics
- Supports executive reporting and trend analysis

PARTITIONING
- date

CLUSTERING
- account_name
- site_url
===============================================================================
*/

CREATE TABLE IF NOT EXISTS
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation
 .sdi_bronze_search_console_site_totals_daily`
(
  -- Source identifiers
  account_id STRING,
  account_name STRING,
  site_url STRING,

  -- Date
  date DATE,

  -- Aggregated performance metrics
  clicks FLOAT64,
  impressions FLOAT64,
  sum_position FLOAT64,
  position FLOAT64,

  -- Audit metadata
  __insert_date INT64,
  file_load_datetime TIMESTAMP,
  filename STRING
)
PARTITION BY date
CLUSTER BY account_name, site_url;
