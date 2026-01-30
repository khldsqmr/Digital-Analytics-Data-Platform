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
  -- Source identifiers
  account_id            STRING,
  account_name          STRING,
  site_url              STRING,

  -- Dates
  date_yyyymmdd         STRING,   -- Source date (YYYYMMDD)
  date                  DATE,     -- Derived DATE for partitioning

  -- Metrics
  clicks                FLOAT64,
  impressions           FLOAT64,
  sum_position          FLOAT64,  -- Used to recompute avg position

  -- Audit & lineage
  __insert_date         INT64,
  file_load_datetime    TIMESTAMP,
  filename              STRING
)
PARTITION BY date
CLUSTER BY account_name, site_url;
