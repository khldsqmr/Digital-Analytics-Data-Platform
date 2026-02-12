/*
===============================================================================
FILE: bronze_campaign_entity_reconciliation_tests.sql
LAYER: Bronze
TABLE: sdi_bronze_sa360_campaign_entity

SOURCE:
  prj-dbi-prd-1.ds_dbi_improvado_master.google_search_ads_360_beta_campaign_entity_custom_tmo

PURPOSE:
  Validate that Bronze Campaign Entity correctly reflects source snapshots.

GRAIN:
  account_id + campaign_id + date

===============================================================================
*/

-- ======================================================
-- TEST 1: Row Count Reconciliation (Last 7 Days)
-- ======================================================
-- Why?
-- Bronze snapshot count should match source snapshot count
-- for same date window.
-- What we expect?
-- source_count = bronze_count
-- ======================================================
WITH source_data AS (
  SELECT
    COUNT(*) AS source_count
  FROM
  `prj-dbi-prd-1.ds_dbi_improvado_master.google_search_ads_360_beta_campaign_entity_custom_tmo`
  WHERE
    PARSE_DATE('%Y%m%d', date_yyyymmdd)
      >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
),

bronze_data AS (
  SELECT
    COUNT(*) AS bronze_count
  FROM
  `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_campaign_entity`
  WHERE
    date >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
)

SELECT
  source_count,
  bronze_count,
  source_count - bronze_count AS difference
FROM source_data, bronze_data;

-- ======================================================
-- TEST 2: Missing Snapshot Rows
-- ======================================================
-- Why?
-- Detect campaigns missing in bronze for given snapshot date.
-- What we expect?
-- 0 rows returned.
-- ======================================================
SELECT
  s.account_id,
  s.campaign_id,
  s.date_yyyymmdd
FROM
`prj-dbi-prd-1.ds_dbi_improvado_master.google_search_ads_360_beta_campaign_entity_custom_tmo` s
LEFT JOIN
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_campaign_entity` b
ON
  s.account_id = b.account_id
  AND s.campaign_id = b.campaign_id
  AND s.date_yyyymmdd = b.date_yyyymmdd
WHERE
  b.account_id IS NULL
  AND PARSE_DATE('%Y%m%d', s.date_yyyymmdd)
      >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY);

-- ======================================================
-- TEST 3: Bidding Strategy Type Reconciliation
-- ======================================================
-- Why?
-- Validate dimension mapping integrity.
-- Ensure bronze transformation did not modify strategy type.
-- What we expect?
-- difference = 0
-- ======================================================
WITH source_dist AS (
  SELECT
    bidding_strategy_type,
    COUNT(*) AS source_count
  FROM
  `prj-dbi-prd-1.ds_dbi_improvado_master.google_search_ads_360_beta_campaign_entity_custom_tmo`
  GROUP BY 1
),

bronze_dist AS (
  SELECT
    bidding_strategy_type,
    COUNT(*) AS bronze_count
  FROM
  `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_campaign_entity`
  GROUP BY 1
)

SELECT
  COALESCE(s.bidding_strategy_type, b.bidding_strategy_type) AS bidding_strategy_type,
  source_count,
  bronze_count,
  source_count - bronze_count AS difference
FROM source_dist s
FULL OUTER JOIN bronze_dist b
USING (bidding_strategy_type);

-- ======================================================
-- TEST 4: Snapshot Freshness Monitoring
-- ======================================================
-- Why?
-- Detect stale entity ingestion.
-- What we expect?
-- file_load_datetime recent.
-- ======================================================
SELECT
  MAX(file_load_datetime) AS latest_file_loaded,
  MAX(bronze_inserted_at) AS latest_bronze_insert
FROM
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_campaign_entity`;