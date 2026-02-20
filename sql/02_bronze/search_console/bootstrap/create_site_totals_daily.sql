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
  account_id STRING OPTIONS (
    description = 'Search Console account identifier from source system.'
  ),

  account_name STRING OPTIONS (
    description = 'Search Console property name (sc-domain or URL prefix).'
  ),

  site_url STRING OPTIONS (
    description = 'Verified Search Console site URL.'
  ),

  date_yyyymmdd STRING OPTIONS (
    description = 'Source date in YYYYMMDD format, preserved for lineage.'
  ),

  date DATE OPTIONS (
    description = 'Derived analytics date used for partitioning and joins.'
  ),

  clicks FLOAT64 OPTIONS (
    description = 'Total number of clicks for the site on the given day.'
  ),

  impressions FLOAT64 OPTIONS (
    description = 'Total number of impressions for the site on the given day.'
  ),

  sum_position FLOAT64 OPTIONS (
    description = 'Sum of positions across all impressions for the site.'
  ),

  __insert_date INT64 OPTIONS (
    description = 'Epoch timestamp when the record was ingested.'
  ),

  file_load_datetime TIMESTAMP OPTIONS (
    description = 'Timestamp when the source file was loaded.'
  ),

  filename STRING OPTIONS (
    description = 'Source filename delivered by ingestion pipeline.'
  )
)
PARTITION BY date
CLUSTER BY account_name, site_url
OPTIONS (
  description = 'Bronze site-level Google Search Console metrics. One row per site per day.'
);
