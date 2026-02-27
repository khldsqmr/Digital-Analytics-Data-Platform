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
  account_id STRING OPTIONS (
    description = 'Search Console account identifier from source system.'
  ),

  account_name STRING OPTIONS (
    description = 'Search Console property name (sc-domain or URL prefix).'
  ),

  site_url STRING OPTIONS (
    description = 'Verified Search Console site URL.'
  ),

  page STRING OPTIONS (
    description = 'Landing page URL that appeared in search results.'
  ),

  query STRING OPTIONS (
    description = 'User search query that triggered impressions.'
  ),

  search_type STRING OPTIONS (
    description = 'Search surface type (web, image, video, etc.).'
  ),

  date_yyyymmdd STRING OPTIONS (
    description = 'Source date in YYYYMMDD format, preserved for lineage.'
  ),

  date DATE OPTIONS (
    description = 'Derived analytics date used for partitioning and joins.'
  ),

  clicks FLOAT64 OPTIONS (
    description = 'Number of clicks recorded for the query.'
  ),

  impressions FLOAT64 OPTIONS (
    description = 'Number of impressions recorded for the query.'
  ),

  position FLOAT64 OPTIONS (
    description = 'Average position of the query in search results.'
  ),

  sum_position FLOAT64 OPTIONS (
    description = 'Sum of positions used to compute average position.'
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
CLUSTER BY account_name, site_url, page, query
OPTIONS (
  description = 'Bronze query-level Google Search Console data. One row per site, page, query, search type, and day.'
);

