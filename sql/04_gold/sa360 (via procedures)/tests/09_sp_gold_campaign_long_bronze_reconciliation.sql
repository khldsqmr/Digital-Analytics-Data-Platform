/*
===============================================================================
FILE: 09_sp_gold_campaign_long_bronze_reconciliation.sql
PROC:  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_gold_sa360_campaign_long_bronze_reconciliation_tests

RECON (FOCUSED):
  Gold long daily vs Bronze daily for ONLY:
    - cart_start
    - postpaid_pspv

NOTES:
  - This assumes Bronze contains these columns at campaign daily grain.
  - If Bronze column name differs (e.g. cart__start_), update references below.
===============================================================================
*/

CREATE OR REPLACE PROCEDURE
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_gold_sa360_campaign_long_bronze_reconciliation_tests`()
OPTIONS(strict_mode=false)
BEGIN
  DECLARE lookback_days INT64 DEFAULT 14;
  DECLARE tolerance     FLOAT64 DEFAULT 0.000001;

  -- ===========================================================================
  -- TEST 1: cart_start (Gold long vs Bronze)
  -- ===========================================================================
  INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_test_results`
  WITH stats AS (
    SELECT
      (SELECT COUNT(1)
       FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_campaign_daily`
       WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL lookback_days DAY)
      ) AS bronze_rows,

      (SELECT COUNT(1)
       FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_campaign_daily_long`
       WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL lookback_days DAY)
         AND metric_name = 'cart_start'
      ) AS gold_rows,

      COALESCE((
        SELECT SUM(COALESCE(cart_start,0))  -- <- change to cart__start_ if needed
        FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_campaign_daily`
        WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL lookback_days DAY)
      ), 0) AS bronze_sum,

      COALESCE((
        SELECT SUM(COALESCE(metric_value,0))
        FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_campaign_daily_long`
        WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL lookback_days DAY)
          AND metric_name = 'cart_start'
      ), 0) AS gold_sum
  ),
  eval AS (
    SELECT
      bronze_rows, gold_rows, bronze_sum, gold_sum,
      (gold_sum - bronze_sum) AS diff,
      CASE
        WHEN bronze_rows = 0 THEN 'FAIL'
        WHEN gold_rows   = 0 THEN 'FAIL'
        WHEN ABS(gold_sum - bronze_sum) <= tolerance THEN 'PASS'
        ELSE 'FAIL'
      END AS status
    FROM stats
  )
  SELECT
    CURRENT_TIMESTAMP(), CURRENT_DATE(),
    'sdi_gold_sa360_campaign_daily_long',
    'reconciliation',
    'Cart Start Reconciliation (Gold long vs Bronze, 14d)',
    'MEDIUM',
    CAST(bronze_sum AS FLOAT64),
    CAST(gold_sum   AS FLOAT64),
    CAST(diff       AS FLOAT64),
    status,
    IF(status = 'PASS', '🟢', '🔴'),
    CASE
      WHEN bronze_rows = 0 THEN 'Bronze Daily has 0 rows in lookback window (cannot reconcile).'
      WHEN gold_rows   = 0 THEN 'Gold long daily has 0 rows for metric=cart_start in lookback window.'
      WHEN status = 'PASS' THEN 'cart_start matches (Gold long vs Bronze).'
      ELSE 'cart_start does NOT match (Gold long vs Bronze).'
    END,
    CASE
      WHEN bronze_rows = 0 THEN 'Check Bronze Daily build/backfill for last 14 days.'
      WHEN gold_rows   = 0 THEN 'Check Gold long daily build/unpivot for cart_start.'
      WHEN status = 'PASS' THEN 'No action required.'
      ELSE 'Investigate transformations/filters between Bronze->Silver->Gold for cart_start.'
    END,
    FALSE,
    (status='PASS') AS is_pass,
    (status='FAIL') AS is_fail
  FROM eval;

  -- ===========================================================================
  -- TEST 2: postpaid_pspv (Gold long vs Bronze)
  -- ===========================================================================
  INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_test_results`
  WITH stats AS (
    SELECT
      (SELECT COUNT(1)
       FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_campaign_daily`
       WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL lookback_days DAY)
      ) AS bronze_rows,

      (SELECT COUNT(1)
       FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_campaign_daily_long`
       WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL lookback_days DAY)
         AND metric_name = 'postpaid_pspv'
      ) AS gold_rows,

      COALESCE((
        SELECT SUM(COALESCE(postpaid_pspv,0))
        FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_campaign_daily`
        WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL lookback_days DAY)
      ), 0) AS bronze_sum,

      COALESCE((
        SELECT SUM(COALESCE(metric_value,0))
        FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_campaign_daily_long`
        WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL lookback_days DAY)
          AND metric_name = 'postpaid_pspv'
      ), 0) AS gold_sum
  ),
  eval AS (
    SELECT
      bronze_rows, gold_rows, bronze_sum, gold_sum,
      (gold_sum - bronze_sum) AS diff,
      CASE
        WHEN bronze_rows = 0 THEN 'FAIL'
        WHEN gold_rows   = 0 THEN 'FAIL'
        WHEN ABS(gold_sum - bronze_sum) <= tolerance THEN 'PASS'
        ELSE 'FAIL'
      END AS status
    FROM stats
  )
  SELECT
    CURRENT_TIMESTAMP(), CURRENT_DATE(),
    'sdi_gold_sa360_campaign_daily_long',
    'reconciliation',
    'Postpaid PSPV Reconciliation (Gold long vs Bronze, 14d)',
    'MEDIUM',
    CAST(bronze_sum AS FLOAT64),
    CAST(gold_sum   AS FLOAT64),
    CAST(diff       AS FLOAT64),
    status,
    IF(status = 'PASS', '🟢', '🔴'),
    CASE
      WHEN bronze_rows = 0 THEN 'Bronze Daily has 0 rows in lookback window (cannot reconcile).'
      WHEN gold_rows   = 0 THEN 'Gold long daily has 0 rows for metric=postpaid_pspv in lookback window.'
      WHEN status = 'PASS' THEN 'postpaid_pspv matches (Gold long vs Bronze).'
      ELSE 'postpaid_pspv does NOT match (Gold long vs Bronze).'
    END,
    CASE
      WHEN bronze_rows = 0 THEN 'Check Bronze Daily build/backfill for last 14 days.'
      WHEN gold_rows   = 0 THEN 'Check Gold long daily build/unpivot for postpaid_pspv.'
      WHEN status = 'PASS' THEN 'No action required.'
      ELSE 'Investigate transformations/filters between Bronze->Silver->Gold for postpaid_pspv.'
    END,
    FALSE,
    (status='PASS') AS is_pass,
    (status='FAIL') AS is_fail
  FROM eval;

END;