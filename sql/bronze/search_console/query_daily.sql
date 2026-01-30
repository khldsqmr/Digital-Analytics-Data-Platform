/*
===============================================================================
BOOTSTRAP | BRONZE | GOOGLE SEARCH CONSOLE | QUERY LEVEL
===============================================================================

PURPOSE
- Creates the Bronze table for query-level Google Search Console data
- This table is populated incrementally via MERGE pipelines
- Executed ONCE per environment

DESIGN PRINCIPLES
- Raw fidelity (no business logic)
- Explicit partitioning for cost control
- Deterministic record_hash for idempotent merges
- Audit + lineage preserved

GRAIN
- property + site_url + page + query + search_type + event_date
===============================================================================
*/

CREATE TABLE IF NOT EXISTS
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_search_console_query_daily`
(
  -- ======================
  -- BUSINESS IDENTIFIERS
  -- ======================
  property                STRING,
  account_name            STRING,
  site_url                STRING,
  page                    STRING,
  query                   STRING,
  search_type             STRING,

  -- ======================
  -- DATE FIELDS
  -- ======================
  event_date              DATE,
  event_date_yyyymmdd     INT64,

  -- ======================
  -- METRICS (RAW)
  -- ======================
  clicks                  FLOAT64,
  impressions             FLOAT64,
  position                FLOAT64,
  sum_position             FLOAT64,

  -- ======================
  -- INGESTION METADATA
  -- ======================
  file_name               STRING,
  file_load_datetime      TIMESTAMP,
  insert_ts               TIMESTAMP,

  -- ======================
  -- LINEAGE
  -- ======================
  source_system            STRING,

  -- ======================
  -- MERGE CONTROL
  -- ======================
  record_hash              STRING
)
PARTITION BY event_date
CLUSTER BY property, site_url, query
OPTIONS (
  description = "Bronze raw query-level Google Search Console data"
);
