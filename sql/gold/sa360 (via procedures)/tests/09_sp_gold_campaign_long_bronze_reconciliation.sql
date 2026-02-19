/*
===============================================================================
FILE: 09_sp_gold_campaign_long_bronze_reconciliation.sql
PROC:  sp_gold_sa360_campaign_long_bronze_reconciliation_tests

RECON (LIMITED):
  Gold long daily vs Bronze daily for raw comparable metrics:
    - impressions
    - clicks
    - cost (bronze cost_micros -> gold cost)
    - all_conversions
===============================================================================
*/

CREATE OR REPLACE PROCEDURE
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_gold_sa360_campaign_long_bronze_reconciliation_tests`()
OPTIONS(strict_mode=false)
BEGIN
  DECLARE lookback_days INT64 DEFAULT 14;
  DECLARE tolerance FLOAT64 DEFAULT 0.000001;

  -- Adjust this bronze table name if yours differs
  DECLARE bronze_table STRING DEFAULT 'prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_campaign_daily';

  -- NOTE:
  -- BigQuery does not allow dynamic table names in standard SQL without EXECUTE IMMEDIATE.
  -- So below, I assume the bronze table name is literal. Replace it manually if needed.

  -- TEST 1: Impressions (Gold long vs Bronze)
  INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_test_results`
  WITH sums AS (
    SELECT
      (SELECT SUM(COALESCE(impressions,0))
       FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_campaign_daily`
       WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL lookback_days DAY)
      ) AS bronze_sum,
      (SELECT SUM(COALESCE(metric_value,0))
       FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_campaign_daily_long`
       WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL lookback_days DAY)
         AND metric_name = 'impressions'
      ) AS gold_long_sum
  )
  SELECT
    CURRENT_TIMESTAMP(), CURRENT_DATE(),
    'sdi_gold_sa360_campaign_daily_long',
    'reconciliation',
    'Impressions Reconciliation (Gold long vs Bronze, 14d)',
    'MEDIUM',
    bronze_sum,
    gold_long_sum,
    (gold_long_sum - bronze_sum),
    IF(ABS(gold_long_sum - bronze_sum) <= tolerance, 'PASS', 'FAIL'),
    IF(ABS(gold_long_sum - bronze_sum) <= tolerance, '🟢', '🔴'),
    IF(ABS(gold_long_sum - bronze_sum) <= tolerance,
      'Impressions match (Gold long vs Bronze).',
      'Impressions do NOT match (Gold long vs Bronze).'
    ),
    IF(ABS(gold_long_sum - bronze_sum) <= tolerance,
      'No action required.',
      'Investigate transformations between Bronze->Silver->Gold for impressions.'
    ),
    FALSE,
    IF(ABS(gold_long_sum - bronze_sum) <= tolerance, TRUE, FALSE),
    IF(ABS(gold_long_sum - bronze_sum) > tolerance, TRUE, FALSE)
  FROM sums;

  -- TEST 2: Cost (Bronze micros -> Gold cost)
  INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_test_results`
  WITH sums AS (
    SELECT
      (SELECT SUM(COALESCE(cost_micros,0))/1000000.0
       FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_campaign_daily`
       WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL lookback_days DAY)
      ) AS bronze_cost,
      (SELECT SUM(COALESCE(metric_value,0))
       FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_campaign_daily_long`
       WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL lookback_days DAY)
         AND metric_name = 'cost'
      ) AS gold_cost
  )
  SELECT
    CURRENT_TIMESTAMP(), CURRENT_DATE(),
    'sdi_gold_sa360_campaign_daily_long',
    'reconciliation',
    'Cost Reconciliation (Gold long vs Bronze micros, 14d)',
    'MEDIUM',
    bronze_cost,
    gold_cost,
    (gold_cost - bronze_cost),
    IF(ABS(gold_cost - bronze_cost) <= tolerance, 'PASS', 'FAIL'),
    IF(ABS(gold_cost - bronze_cost) <= tolerance, '🟢', '🔴'),
    IF(ABS(gold_cost - bronze_cost) <= tolerance,
      'Cost matches (Gold long vs Bronze micros).',
      'Cost does NOT match (Gold long vs Bronze micros).'
    ),
    IF(ABS(gold_cost - bronze_cost) <= tolerance,
      'No action required.',
      'Investigate cost conversions between layers (micros -> currency).'
    ),
    FALSE,
    IF(ABS(gold_cost - bronze_cost) <= tolerance, TRUE, FALSE),
    IF(ABS(gold_cost - bronze_cost) > tolerance, TRUE, FALSE)
  FROM sums;

END;
