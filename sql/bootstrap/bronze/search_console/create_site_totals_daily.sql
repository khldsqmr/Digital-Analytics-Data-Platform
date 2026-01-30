/*
===============================================================================
BOOTSTRAP | BRONZE | GOOGLE SEARCH CONSOLE | QUERY LEVEL (ONE-TIME)
===============================================================================

PURPOSE
- Creates the physical Bronze table for query-level Search Console data
- Executed ONCE per environment
- Must NOT be re-run after creation

DESIGN PRINCIPLES
- Preserve source fidelity
- Keep raw identifiers and metadata
- Use derived DATE for partitioning
- Support incremental MERGE pipelines

SOURCE
- Improvado Search Console (query + search_type)

===============================================================================
*/

CREATE TABLE IF NOT EXISTS
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_search_console_query_daily`
(
  -- Source identifiers
  account_id            STRING,
  account_name          STRING,
  site_url              STRING,

  -- Search dimensions
  page                  STRING,
  query                 STRING,
  search_type           STRING,

  -- Dates
  date_yyyymmdd         STRING,   -- Source date as delivered (YYYYMMDD)
  date                  DATE,     -- Derived date used for partitioning

  -- Metrics
  clicks                FLOAT64,
  impressions           FLOAT64,
  position              FLOAT64,  -- Avg position (query-level only)
  sum_position          FLOAT64,  -- Sum of positions (used for recalcs)

  -- Audit & lineage
  __insert_date         INT64,
  file_load_datetime    TIMESTAMP,
  filename              STRING
)
PARTITION BY date
CLUSTER BY account_name, site_url, query;
