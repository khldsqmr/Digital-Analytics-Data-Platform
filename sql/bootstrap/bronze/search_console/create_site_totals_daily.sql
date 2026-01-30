/*
===============================================================================
BOOTSTRAP | BRONZE | GOOGLE SEARCH CONSOLE | SITE TOTALS
===============================================================================

PURPOSE
- Creates the Bronze table for site-level Google Search Console metrics
- Used as authoritative daily SEO KPI source
- Executed ONCE per environment

DESIGN PRINCIPLES
- Immutable Bronze foundation
- Partitioned by date for efficient reprocessing
- Deterministic record_hash ensures idempotency

GRAIN
- property + site_url + event_date
===============================================================================
*/

CREATE TABLE IF NOT EXISTS
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_search_console_site_totals_daily`
(
  -- ======================
  -- BUSINESS IDENTIFIERS
  -- ======================
  property                STRING,
  account_name            STRING,
  site_url                STRING,

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
  sum_position             FLOAT64,
  position                FLOAT64,

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
CLUSTER BY property, site_url
OPTIONS (
  description = "Bronze raw site-level Google Search Console data"
);
