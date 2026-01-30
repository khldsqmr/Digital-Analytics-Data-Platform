/*
===============================================================================
METADATA | BRONZE | SEARCH CONSOLE | QUERY LEVEL
===============================================================================
Applies table and column descriptions.
Safe to run occasionally.
===============================================================================
*/

ALTER TABLE
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_search_console_query_daily`

SET OPTIONS (
  description = 'Bronze query-level Google Search Console data with full source fidelity. One row per site, page, query, search type, and day.'
)

ALTER COLUMN account_id SET OPTIONS (
  description = 'Search Console account identifier from source system.'
)
ALTER COLUMN account_name SET OPTIONS (
  description = 'Search Console property name (sc-domain or URL prefix).'
)
ALTER COLUMN site_url SET OPTIONS (
  description = 'Verified Search Console site URL.'
)
ALTER COLUMN page SET OPTIONS (
  description = 'Landing page URL that appeared in search results.'
)
ALTER COLUMN query SET OPTIONS (
  description = 'User search query that triggered impressions.'
)
ALTER COLUMN search_type SET OPTIONS (
  description = 'Search surface type (web, image, video, etc.).'
)
ALTER COLUMN date_yyyymmdd SET OPTIONS (
  description = 'Source date as YYYYMMDD string, preserved for lineage.'
)
ALTER COLUMN date SET OPTIONS (
  description = 'Derived analytics date used for partitioning.'
)
ALTER COLUMN clicks SET OPTIONS (
  description = 'Number of clicks recorded for the query.'
)
ALTER COLUMN impressions SET OPTIONS (
  description = 'Number of impressions recorded for the query.'
)
ALTER COLUMN position SET OPTIONS (
  description = 'Average position of the query in search results.'
)
ALTER COLUMN sum_position SET OPTIONS (
  description = 'Sum of positions used to compute average position.'
)
ALTER COLUMN __insert_date SET OPTIONS (
  description = 'Epoch timestamp when record was ingested.'
)
ALTER COLUMN file_load_datetime SET OPTIONS (
  description = 'Timestamp when the source file was loaded.'
)
ALTER COLUMN filename SET OPTIONS (
  description = 'Source filename delivered by ingestion pipeline.'
);
