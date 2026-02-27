/*
===============================================================================
FILE: 06_sp_gold_campaign_long_daily_reconciliation.sql
PROC: sp_gold_sa360_campaign_long_daily_reconciliation_tests

RECON (basic data flow):
  Gold long daily vs Gold wide daily (same window) for ONLY:
    - cart_start
    - postpaid_pspv
===============================================================================
*/

CREATE OR REPLACE PROCEDURE
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_gold_sa360_campaign_long_daily_reconciliation_tests`()
OPTIONS(strict_mode=false)
BEGIN
  DECLARE lookback_days INT64 DEFAULT 14;
  DECLARE tolerance FLOAT64 DEFAULT 0.000001;

  -- ===========================================================================
  -- TEST 1: cart_start (Gold long vs Gold wide)
  -- ===========================================================================
  INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_test_results`
  WITH stats AS (
    SELECT
      (SELECT COUNT(1)
       FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_campaign_daily`
       WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL lookback_days DAY)
      ) AS wide_rows,
      (SELECT COUNT(1)
       FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_campaign_daily_long`
       WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL lookback_days DAY)
         AND metric_name = 'cart_start'
      ) AS long_rows,

      COALESCE((
        SELECT SUM(COALESCE(cart_start,0))
        FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_campaign_daily`
        WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL lookback_days DAY)
      ), 0) AS wide_sum,

      COALESCE((
        SELECT SUM(COALESCE(metric_value,0))
        FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_campaign_daily_long`
        WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL lookback_days DAY)
          AND metric_name = 'cart_start'
      ), 0) AS long_sum
  ),
  eval AS (
    SELECT
      wide_rows,
      long_rows,
      wide_sum,
      long_sum,
      (long_sum - wide_sum) AS diff,
      CASE
        WHEN wide_rows = 0 THEN 'FAIL'
        WHEN long_rows = 0 THEN 'FAIL'
        WHEN ABS(long_sum - wide_sum) <= tolerance THEN 'PASS'
        ELSE 'FAIL'
      END AS status
    FROM stats
  )
  SELECT
    CURRENT_TIMESTAMP(), CURRENT_DATE(),
    'sdi_gold_sa360_campaign_daily_long',
    'reconciliation',
    'Cart Start Reconciliation (Gold long vs Gold wide, 14d)',
    'HIGH',
    CAST(wide_sum AS FLOAT64),
    CAST(long_sum AS FLOAT64),
    CAST(diff AS FLOAT64),
    status,
    IF(status='PASS','🟢','🔴'),
    CASE
      WHEN wide_rows = 0 THEN 'Gold wide daily has 0 rows in lookback window (cannot reconcile).'
      WHEN long_rows = 0 THEN 'Gold long daily has 0 rows for metric=cart_start in lookback window.'
      WHEN status='PASS' THEN 'Gold long matches Gold wide for cart_start.'
      ELSE 'Gold long does NOT match Gold wide for cart_start.'
    END,
    CASE
      WHEN wide_rows = 0 THEN 'Check Gold wide daily build/backfill.'
      WHEN long_rows = 0 THEN 'Check Gold long daily build/unpivot for cart_start.'
      WHEN status='PASS' THEN 'No action required.'
      ELSE 'Fix long build/unpivot logic for cart_start.'
    END,
    (status='FAIL'),
    (status='PASS'),
    (status='FAIL')
  FROM eval;

  -- ===========================================================================
  -- TEST 2: postpaid_pspv (Gold long vs Gold wide)
  -- ===========================================================================
  INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_test_results`
  WITH stats AS (
    SELECT
      (SELECT COUNT(1)
       FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_campaign_daily`
       WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL lookback_days DAY)
      ) AS wide_rows,
      (SELECT COUNT(1)
       FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_campaign_daily_long`
       WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL lookback_days DAY)
         AND metric_name = 'postpaid_pspv'
      ) AS long_rows,

      COALESCE((
        SELECT SUM(COALESCE(postpaid_pspv,0))
        FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_campaign_daily`
        WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL lookback_days DAY)
      ), 0) AS wide_sum,

      COALESCE((
        SELECT SUM(COALESCE(metric_value,0))
        FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_campaign_daily_long`
        WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL lookback_days DAY)
          AND metric_name = 'postpaid_pspv'
      ), 0) AS long_sum
  ),
  eval AS (
    SELECT
      wide_rows,
      long_rows,
      wide_sum,
      long_sum,
      (long_sum - wide_sum) AS diff,
      CASE
        WHEN wide_rows = 0 THEN 'FAIL'
        WHEN long_rows = 0 THEN 'FAIL'
        WHEN ABS(long_sum - wide_sum) <= tolerance THEN 'PASS'
        ELSE 'FAIL'
      END AS status
    FROM stats
  )
  SELECT
    CURRENT_TIMESTAMP(), CURRENT_DATE(),
    'sdi_gold_sa360_campaign_daily_long',
    'reconciliation',
    'Postpaid PSPV Reconciliation (Gold long vs Gold wide, 14d)',
    'HIGH',
    CAST(wide_sum AS FLOAT64),
    CAST(long_sum AS FLOAT64),
    CAST(diff AS FLOAT64),
    status,
    IF(status='PASS','🟢','🔴'),
    CASE
      WHEN wide_rows = 0 THEN 'Gold wide daily has 0 rows in lookback window (cannot reconcile).'
      WHEN long_rows = 0 THEN 'Gold long daily has 0 rows for metric=postpaid_pspv in lookback window.'
      WHEN status='PASS' THEN 'Gold long matches Gold wide for postpaid_pspv.'
      ELSE 'Gold long does NOT match Gold wide for postpaid_pspv.'
    END,
    CASE
      WHEN wide_rows = 0 THEN 'Check Gold wide daily build/backfill.'
      WHEN long_rows = 0 THEN 'Check Gold long daily build/unpivot for postpaid_pspv.'
      WHEN status='PASS' THEN 'No action required.'
      ELSE 'Fix long build/unpivot logic for postpaid_pspv.'
    END,
    (status='FAIL'),
    (status='PASS'),
    (status='FAIL')
  FROM eval;

END;