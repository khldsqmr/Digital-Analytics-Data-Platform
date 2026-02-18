/*
===============================================================================
FILE: 02_sp_gold_campaign_daily_reconciliation.sql
LAYER: Gold | QA
PROC:  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_gold_sa360_campaign_daily_reconciliation_tests

RECONCILIATION:
  Gold Daily vs Silver Daily (metrics should match, Gold is projection).
  Focus:
    - cart_start
    - postpaid_pspv
===============================================================================
*/

CREATE OR REPLACE PROCEDURE
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_gold_sa360_campaign_daily_reconciliation_tests`()
OPTIONS(strict_mode=false)
BEGIN
  DECLARE lookback_days INT64 DEFAULT 14;
  DECLARE tolerance FLOAT64 DEFAULT 0.000001;

  -- cart_start reconcile
  INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_test_results`
  WITH sums AS (
    SELECT
      (SELECT SUM(COALESCE(cart_start,0))
       FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_campaign_daily`
       WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL lookback_days DAY)
      ) AS gold_sum,
      (SELECT SUM(COALESCE(cart_start,0))
       FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_silver_sa360_campaign_daily`
       WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL lookback_days DAY)
      ) AS silver_sum
  )
  SELECT
    CURRENT_TIMESTAMP(), CURRENT_DATE(),
    'sdi_gold_sa360_campaign_daily',
    'reconciliation',
    'Cart Start Reconciliation (Gold vs Silver, 14d)',
    'HIGH',
    silver_sum,
    gold_sum,
    (gold_sum - silver_sum),
    IF(ABS(gold_sum - silver_sum) <= tolerance, 'PASS', 'FAIL'),
    IF(ABS(gold_sum - silver_sum) <= tolerance, '🟢', '🔴'),
    IF(ABS(gold_sum - silver_sum) <= tolerance,
      'Cart Start sums match Gold vs Silver.',
      'Cart Start sums do NOT match Gold vs Silver (unexpected).'
    ),
    IF(ABS(gold_sum - silver_sum) <= tolerance,
      'No action required.',
      'Investigate Gold daily MERGE projection and column mapping.'
    ),
    IF(ABS(gold_sum - silver_sum) > tolerance, TRUE, FALSE),
    IF(ABS(gold_sum - silver_sum) <= tolerance, TRUE, FALSE),
    IF(ABS(gold_sum - silver_sum) > tolerance, TRUE, FALSE)
  FROM sums;

  -- postpaid_pspv reconcile
  INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_test_results`
  WITH sums AS (
    SELECT
      (SELECT SUM(COALESCE(postpaid_pspv,0))
       FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_campaign_daily`
       WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL lookback_days DAY)
      ) AS gold_sum,
      (SELECT SUM(COALESCE(postpaid_pspv,0))
       FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_silver_sa360_campaign_daily`
       WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL lookback_days DAY)
      ) AS silver_sum
  )
  SELECT
    CURRENT_TIMESTAMP(), CURRENT_DATE(),
    'sdi_gold_sa360_campaign_daily',
    'reconciliation',
    'Postpaid PSPV Reconciliation (Gold vs Silver, 14d)',
    'HIGH',
    silver_sum,
    gold_sum,
    (gold_sum - silver_sum),
    IF(ABS(gold_sum - silver_sum) <= tolerance, 'PASS', 'FAIL'),
    IF(ABS(gold_sum - silver_sum) <= tolerance, '🟢', '🔴'),
    IF(ABS(gold_sum - silver_sum) <= tolerance,
      'Postpaid PSPV sums match Gold vs Silver.',
      'Postpaid PSPV sums do NOT match Gold vs Silver (unexpected).'
    ),
    IF(ABS(gold_sum - silver_sum) <= tolerance,
      'No action required.',
      'Investigate Gold daily MERGE projection and column mapping.'
    ),
    IF(ABS(gold_sum - silver_sum) > tolerance, TRUE, FALSE),
    IF(ABS(gold_sum - silver_sum) <= tolerance, TRUE, FALSE),
    IF(ABS(gold_sum - silver_sum) > tolerance, TRUE, FALSE)
  FROM sums;

END;
