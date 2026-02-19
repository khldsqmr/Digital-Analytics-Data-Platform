/*
===============================================================================
FILE: 06_sp_gold_campaign_long_daily_reconciliation.sql
PROC:  sp_gold_sa360_campaign_long_daily_reconciliation_tests
RECON: Gold long daily vs Gold wide daily (same window)
===============================================================================
*/

CREATE OR REPLACE PROCEDURE
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_gold_sa360_campaign_long_daily_reconciliation_tests`()
OPTIONS(strict_mode=false)
BEGIN
  DECLARE lookback_days INT64 DEFAULT 14;
  DECLARE tolerance FLOAT64 DEFAULT 0.000001;

  -- Reconcile a small set of “must match” metrics (add more if you want)
  -- Pattern: metric_name in long must equal SUM(column) in wide
  -- ---------------------------------------------------------------------------

  -- TEST 1: impressions
  INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_test_results`
  WITH sums AS (
    SELECT
      (SELECT SUM(COALESCE(impressions,0))
       FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_campaign_daily`
       WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL lookback_days DAY)
      ) AS wide_sum,
      (SELECT SUM(COALESCE(metric_value,0))
       FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_campaign_daily_long`
       WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL lookback_days DAY)
         AND metric_name = 'impressions'
      ) AS long_sum
  )
  SELECT
    CURRENT_TIMESTAMP(), CURRENT_DATE(),
    'sdi_gold_sa360_campaign_daily_long',
    'reconciliation',
    'Impressions Reconciliation (Gold long vs Gold wide, 14d)',
    'HIGH',
    wide_sum,
    long_sum,
    (long_sum - wide_sum),
    IF(ABS(long_sum - wide_sum) <= tolerance, 'PASS', 'FAIL'),
    IF(ABS(long_sum - wide_sum) <= tolerance, '🟢', '🔴'),
    IF(ABS(long_sum - wide_sum) <= tolerance,
      'Gold long matches Gold wide for impressions.',
      'Gold long does NOT match Gold wide for impressions.'
    ),
    IF(ABS(long_sum - wide_sum) <= tolerance,
      'No action required.',
      'Fix long build/unpivot logic for impressions.'
    ),
    IF(ABS(long_sum - wide_sum) > tolerance, TRUE, FALSE),
    IF(ABS(long_sum - wide_sum) <= tolerance, TRUE, FALSE),
    IF(ABS(long_sum - wide_sum) > tolerance, TRUE, FALSE)
  FROM sums;

  -- TEST 2: cart_start (example)
  INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_test_results`
  WITH sums AS (
    SELECT
      (SELECT SUM(COALESCE(cart_start,0))
       FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_campaign_daily`
       WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL lookback_days DAY)
      ) AS wide_sum,
      (SELECT SUM(COALESCE(metric_value,0))
       FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_campaign_daily_long`
       WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL lookback_days DAY)
         AND metric_name = 'cart_start'
      ) AS long_sum
  )
  SELECT
    CURRENT_TIMESTAMP(), CURRENT_DATE(),
    'sdi_gold_sa360_campaign_daily_long',
    'reconciliation',
    'Cart Start Reconciliation (Gold long vs Gold wide, 14d)',
    'HIGH',
    wide_sum,
    long_sum,
    (long_sum - wide_sum),
    IF(ABS(long_sum - wide_sum) <= tolerance, 'PASS', 'FAIL'),
    IF(ABS(long_sum - wide_sum) <= tolerance, '🟢', '🔴'),
    IF(ABS(long_sum - wide_sum) <= tolerance,
      'Gold long matches Gold wide for cart_start.',
      'Gold long does NOT match Gold wide for cart_start.'
    ),
    IF(ABS(long_sum - wide_sum) <= tolerance,
      'No action required.',
      'Fix long build/unpivot logic for cart_start.'
    ),
    IF(ABS(long_sum - wide_sum) > tolerance, TRUE, FALSE),
    IF(ABS(long_sum - wide_sum) <= tolerance, TRUE, FALSE),
    IF(ABS(long_sum - wide_sum) > tolerance, TRUE, FALSE)
  FROM sums;

END;
