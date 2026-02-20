/*
===============================================================================
FILE: 02_sp_silver_campaign_daily_reconciliation.sql
LAYER: Silver | QA
PROC:  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_silver_sa360_campaign_daily_reconciliation_tests

RECONCILIATION:
  Silver vs Bronze (same metric values; Silver only enriches dimensions).
  We reconcile metric sums for:
    - cart_start
    - postpaid_pspv
===============================================================================
*/

CREATE OR REPLACE PROCEDURE
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_silver_sa360_campaign_daily_reconciliation_tests`()
OPTIONS(strict_mode=false)
BEGIN
  DECLARE lookback_days INT64 DEFAULT 7;
  DECLARE tolerance FLOAT64 DEFAULT 0.000001;

  -- cart_start sum reconcile
  INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_silver_sa360_test_results`
  WITH sums AS (
    SELECT
      (SELECT SUM(COALESCE(cart_start,0))
       FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_silver_sa360_campaign_daily`
       WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL lookback_days DAY)
      ) AS silver_sum,
      (SELECT SUM(COALESCE(cart_start,0))
       FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_campaign_daily`
       WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL lookback_days DAY)
      ) AS bronze_sum
  )
  SELECT
    CURRENT_TIMESTAMP(), CURRENT_DATE(),
    'sdi_silver_sa360_campaign_daily',
    'reconciliation',
    'Cart Start Reconciliation (Silver vs Bronze, 7d)',
    'HIGH',
    bronze_sum,
    silver_sum,
    (silver_sum - bronze_sum),
    IF(ABS(silver_sum - bronze_sum) <= tolerance, 'PASS', 'FAIL'),
    IF(ABS(silver_sum - bronze_sum) <= tolerance, '🟢', '🔴'),
    IF(ABS(silver_sum - bronze_sum) <= tolerance,
      'Cart Start sums match Silver vs Bronze.',
      'Cart Start sums do NOT match Silver vs Bronze (unexpected).'
    ),
    IF(ABS(silver_sum - bronze_sum) <= tolerance,
      'No action required.',
      'Investigate Silver MERGE; ensure metric columns are 1:1 from Bronze.'
    ),
    IF(ABS(silver_sum - bronze_sum) > tolerance, TRUE, FALSE),
    IF(ABS(silver_sum - bronze_sum) <= tolerance, TRUE, FALSE),
    IF(ABS(silver_sum - bronze_sum) > tolerance, TRUE, FALSE)
  FROM sums;

  -- postpaid_pspv sum reconcile
  INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_silver_sa360_test_results`
  WITH sums AS (
    SELECT
      (SELECT SUM(COALESCE(postpaid_pspv,0))
       FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_silver_sa360_campaign_daily`
       WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL lookback_days DAY)
      ) AS silver_sum,
      (SELECT SUM(COALESCE(postpaid_pspv,0))
       FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_campaign_daily`
       WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL lookback_days DAY)
      ) AS bronze_sum
  )
  SELECT
    CURRENT_TIMESTAMP(), CURRENT_DATE(),
    'sdi_silver_sa360_campaign_daily',
    'reconciliation',
    'Postpaid PSPV Reconciliation (Silver vs Bronze, 7d)',
    'HIGH',
    bronze_sum,
    silver_sum,
    (silver_sum - bronze_sum),
    IF(ABS(silver_sum - bronze_sum) <= tolerance, 'PASS', 'FAIL'),
    IF(ABS(silver_sum - bronze_sum) <= tolerance, '🟢', '🔴'),
    IF(ABS(silver_sum - bronze_sum) <= tolerance,
      'Postpaid PSPV sums match Silver vs Bronze.',
      'Postpaid PSPV sums do NOT match Silver vs Bronze (unexpected).'
    ),
    IF(ABS(silver_sum - bronze_sum) <= tolerance,
      'No action required.',
      'Investigate Silver MERGE; ensure metric columns are 1:1 from Bronze.'
    ),
    IF(ABS(silver_sum - bronze_sum) > tolerance, TRUE, FALSE),
    IF(ABS(silver_sum - bronze_sum) <= tolerance, TRUE, FALSE),
    IF(ABS(silver_sum - bronze_sum) > tolerance, TRUE, FALSE)
  FROM sums;

END;
