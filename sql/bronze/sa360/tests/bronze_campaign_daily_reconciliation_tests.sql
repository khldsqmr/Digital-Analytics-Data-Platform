/*
===============================================================================
FILE: bronze_campaign_daily_reconciliation_tests.sql
LAYER: Bronze
TABLE: sdi_bronze_sa360_campaign_daily

SOURCE:
  prj-dbi-prd-1.ds_dbi_improvado_master.google_search_ads_360_campaigns_tmo

PURPOSE:
  Validate that Bronze Campaign Daily correctly reflects source data.
  Ensures:
    • No missing rows
    • No duplicated rows
    • Metric totals match
    • Date derivation is correct
    • Column renaming did not alter values

GRAIN:
  account_id + campaign_id + date

===============================================================================
*/
-- ======================================================
-- TEST 1: Row Count Reconciliation (Last 7 Days)
-- ======================================================
-- Why?
-- Bronze should contain same number of rows as source
-- for same date range (after lookback filter).
-- What we expect?
-- • source_count = bronze_count
-- • If mismatch → MERGE filtering issue
-- ======================================================
WITH source_data AS (
  SELECT
    COUNT(*) AS source_count
  FROM
  `prj-dbi-prd-1.ds_dbi_improvado_master.google_search_ads_360_campaigns_tmo`
  WHERE
    PARSE_DATE('%Y%m%d', date_yyyymmdd)
      >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
),

bronze_data AS (
  SELECT
    COUNT(*) AS bronze_count
  FROM
  `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_campaign_daily`
  WHERE
    date >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
)

SELECT
  source_count,
  bronze_count,
  source_count - bronze_count AS difference
FROM source_data, bronze_data;

-- ======================================================
-- TEST 2: Grain-Level Missing Rows Detection
-- ======================================================
-- Why?
-- Detect rows present in source but missing in bronze.
-- Most important correctness test.
-- What we expect?
-- 0 rows returned.
-- ======================================================
SELECT
  s.account_id,
  s.campaign_id,
  s.date_yyyymmdd
FROM
`prj-dbi-prd-1.ds_dbi_improvado_master.google_search_ads_360_campaigns_tmo` s
LEFT JOIN
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_campaign_daily` b
ON
  s.account_id = b.account_id
  AND s.campaign_id = b.campaign_id
  AND s.date_yyyymmdd = b.date_yyyymmdd
WHERE
  b.account_id IS NULL
  AND PARSE_DATE('%Y%m%d', s.date_yyyymmdd)
      >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY);

-- ======================================================
-- TEST 3: Metric Total Reconciliation (Cost)
-- ======================================================
-- Why?
-- Validate transformation logic:
-- cost = cost_micros / 1,000,000
-- What we expect?
-- • expected_cost ≈ bronze_cost
-- • difference very close to 0
-- ======================================================
WITH source_totals AS (
  SELECT
    SUM(cost_micros)/1000000 AS expected_cost
  FROM
  `prj-dbi-prd-1.ds_dbi_improvado_master.google_search_ads_360_campaigns_tmo`
  WHERE
    PARSE_DATE('%Y%m%d', date_yyyymmdd)
      >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
),

bronze_totals AS (
  SELECT
    SUM(cost) AS bronze_cost
  FROM
  `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_campaign_daily`
  WHERE
    date >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
)

SELECT
  expected_cost,
  bronze_cost,
  ABS(expected_cost - bronze_cost) AS difference
FROM source_totals, bronze_totals;

-- ======================================================
-- TEST 4: Clicks Reconciliation
-- ======================================================
-- Why?
-- Ensure metric renaming did not change values.
-- What we expect?
-- difference = 0
-- ======================================================
WITH source_totals AS (
  SELECT
    SUM(clicks) AS source_clicks
  FROM
  `prj-dbi-prd-1.ds_dbi_improvado_master.google_search_ads_360_campaigns_tmo`
  WHERE
    PARSE_DATE('%Y%m%d', date_yyyymmdd)
      >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
),

bronze_totals AS (
  SELECT
    SUM(clicks) AS bronze_clicks
  FROM
  `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_campaign_daily`
  WHERE
    date >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
)

SELECT
  source_clicks,
  bronze_clicks,
  source_clicks - bronze_clicks AS difference
FROM source_totals, bronze_totals;

-- ======================================================
-- TEST 5: Date Parsing Accuracy
-- ======================================================
-- Why?
-- Validate that:
-- PARSE_DATE('%Y%m%d', date_yyyymmdd) = date
-- What we expect?
-- 0 rows returned.
-- ======================================================
SELECT
  date_yyyymmdd,
  date,
  PARSE_DATE('%Y%m%d', date_yyyymmdd) AS recalculated_date
FROM
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_campaign_daily`
WHERE
  date != PARSE_DATE('%Y%m%d', date_yyyymmdd);

-- ======================================================
-- TEST 6: Extreme Outlier Detection
-- ======================================================
-- Why?
-- Identify abnormal metric spikes that may indicate duplication.
-- What we expect?
-- Reasonable ranges only.
-- ======================================================
SELECT
  MAX(clicks) AS max_clicks,
  MAX(impressions) AS max_impressions,
  MAX(cost) AS max_cost,
  MAX(all_conversions) AS max_conversions
FROM
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_campaign_daily`;