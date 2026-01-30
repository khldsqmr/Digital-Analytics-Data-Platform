/*
===============================================================================
BRONZE | GOOGLE SEARCH CONSOLE | QUERY LEVEL | ONE-TIME BOOTSTRAP
===============================================================================

PURPOSE
- Creates the Bronze table for query-level Google Search Console data
- This table stores the MOST GRANULAR Search Console metrics available
- Executed ONCE per environment

GRAIN (Natural Key)
- account_name
- site_url
- page
- query
- search_type
- date

DESIGN PRINCIPLES
- Preserve raw source structure
- No aggregations
- No business logic
- Optimized for incremental MERGE operations

PARTITIONING
- date (daily partitioning for cost control)

CLUSTERING
- account_name
- site_url
- query
===============================================================================
*/

CREATE TABLE IF NOT EXISTS
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation
 .sdi_bronze_search_console_query_daily`
(
  -- Source identifiers
  account_id STRING,
  account_name STRING,
  site_url STRING,

  -- Search dimensions
  page STRING,
  query STRING,
  search_type STRING,

  -- Date (derived from YYYYMMDD in source)
  date DATE,

  -- Performance metrics
  clicks FLOAT64,          -- Number of clicks
  impressions FLOAT64,     -- Number of impressions
  sum_position FLOAT64,    -- Sum of average position across impressions
  position FLOAT64,        -- Average search position

  -- Audit & lineage metadata
  __insert_date INT64,     -- Source system insert timestamp
  file_load_datetime TIMESTAMP, -- When file was loaded
  filename STRING          -- Source file name
)
PARTITION BY date
CLUSTER BY account_name, site_url, query;
