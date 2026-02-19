/*
===============================================================================
FILE: 02_sp_gold_campaign_daily_reconciliation.sql
LAYER: Gold | QA
PROC:  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_gold_sa360_campaign_daily_reconciliation_tests

RECONCILIATION:
  Gold Daily vs Silver Daily (metrics should match, Gold is projection).

UPDATES:
  - NULL-safe SUM() using COALESCE((SELECT SUM(...)),0)
  - Clean FAIL when either side has 0 rows in lookback window
===============================================================================
*/

CREATE OR REPLACE PROCEDURE
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_gold_sa360_campaign_daily_reconciliation_tests`()
OPTIONS(strict_mode=false)
BEGIN
  DECLARE lookback_days INT64 DEFAULT 14;
  DECLARE tolerance FLOAT64 DEFAULT 0.000001;

  -- ===========================================================================
  -- TEST 1: cart_start reconcile (Gold vs Silver)
  -- ===========================================================================
  INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_test_results`
  WITH stats AS (
    SELECT
      -- Row counts (to detect empty windows)
      (SELECT COUNT(1)
       FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_campaign_daily`
       WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL lookback_days DAY)
      ) AS gold_rows,
      (SELECT COUNT(1)
       FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_silver_sa360_campaign_daily`
       WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL lookback_days DAY)
      ) AS silver_rows,

      -- NULL-safe sums
      COALESCE((
        SELECT SUM(COALESCE(cart_start,0))
        FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_campaign_daily`
        WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL lookback_days DAY)
      ), 0) AS gold_sum,

      COALESCE((
        SELECT SUM(COALESCE(cart_start,0))
        FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_silver_sa360_campaign_daily`
        WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL lookback_days DAY)
      ), 0) AS silver_sum
  ),
  eval AS (
    SELECT
      gold_rows,
      silver_rows,
      gold_sum,
      silver_sum,
      (gold_sum - silver_sum) AS diff,
      -- Fail if either side is empty (comparison not meaningful)
      CASE
        WHEN gold_rows = 0 THEN 'FAIL'
        WHEN silver_rows = 0 THEN 'FAIL'
        WHEN ABS(gold_sum - silver_sum) <= tolerance THEN 'PASS'
        ELSE 'FAIL'
      END AS status
    FROM stats
  )
  SELECT
    CURRENT_TIMESTAMP(), CURRENT_DATE(),
    'sdi_gold_sa360_campaign_daily',
    'reconciliation',
    'Cart Start Reconciliation (Gold vs Silver, 14d)',
    'HIGH',
    CAST(silver_sum AS FLOAT64),
    CAST(gold_sum AS FLOAT64),
    CAST(diff AS FLOAT64),
    status,
    IF(status = 'PASS', '🟢', '🔴'),
    CASE
      WHEN gold_rows = 0 THEN 'Gold Daily has 0 rows in lookback window (cannot reconcile).'
      WHEN silver_rows = 0 THEN 'Silver Daily has 0 rows in lookback window (cannot reconcile).'
      WHEN status = 'PASS' THEN 'Cart Start sums match Gold vs Silver.'
      ELSE 'Cart Start sums do NOT match Gold vs Silver (unexpected).'
    END,
    CASE
      WHEN gold_rows = 0 THEN 'Check Gold Daily build/backfill for the last 14 days.'
      WHEN silver_rows = 0 THEN 'Check Silver Daily build/backfill for the last 14 days.'
      WHEN status = 'PASS' THEN 'No action required.'
      ELSE 'Investigate Gold daily MERGE projection and column mapping.'
    END,
    (status = 'FAIL') AS is_critical_failure,
    (status = 'PASS') AS is_pass,
    (status = 'FAIL') AS is_fail
  FROM eval;

  -- ===========================================================================
  -- TEST 2: postpaid_pspv reconcile (Gold vs Silver)
  -- ===========================================================================
  INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_test_results`
  WITH stats AS (
    SELECT
      (SELECT COUNT(1)
       FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_campaign_daily`
       WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL lookback_days DAY)
      ) AS gold_rows,
      (SELECT COUNT(1)
       FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_silver_sa360_campaign_daily`
       WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL lookback_days DAY)
      ) AS silver_rows,

      COALESCE((
        SELECT SUM(COALESCE(postpaid_pspv,0))
        FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_campaign_daily`
        WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL lookback_days DAY)
      ), 0) AS gold_sum,

      COALESCE((
        SELECT SUM(COALESCE(postpaid_pspv,0))
        FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_silver_sa360_campaign_daily`
        WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL lookback_days DAY)
      ), 0) AS silver_sum
  ),
  eval AS (
    SELECT
      gold_rows,
      silver_rows,
      gold_sum,
      silver_sum,
      (gold_sum - silver_sum) AS diff,
      CASE
        WHEN gold_rows = 0 THEN 'FAIL'
        WHEN silver_rows = 0 THEN 'FAIL'
        WHEN ABS(gold_sum - silver_sum) <= tolerance THEN 'PASS'
        ELSE 'FAIL'
      END AS status
    FROM stats
  )
  SELECT
    CURRENT_TIMESTAMP(), CURRENT_DATE(),
    'sdi_gold_sa360_campaign_daily',
    'reconciliation',
    'Postpaid PSPV Reconciliation (Gold vs Silver, 14d)',
    'HIGH',
    CAST(silver_sum AS FLOAT64),
    CAST(gold_sum AS FLOAT64),
    CAST(diff AS FLOAT64),
    status,
    IF(status = 'PASS', '🟢', '🔴'),
    CASE
      WHEN gold_rows = 0 THEN 'Gold Daily has 0 rows in lookback window (cannot reconcile).'
      WHEN silver_rows = 0 THEN 'Silver Daily has 0 rows in lookback window (cannot reconcile).'
      WHEN status = 'PASS' THEN 'Postpaid PSPV sums match Gold vs Silver.'
      ELSE 'Postpaid PSPV sums do NOT match Gold vs Silver (unexpected).'
    END,
    CASE
      WHEN gold_rows = 0 THEN 'Check Gold Daily build/backfill for the last 14 days.'
      WHEN silver_rows = 0 THEN 'Check Silver Daily build/backfill for the last 14 days.'
      WHEN status = 'PASS' THEN 'No action required.'
      ELSE 'Investigate Gold daily MERGE projection and column mapping.'
    END,
    (status = 'FAIL') AS is_critical_failure,
    (status = 'PASS') AS is_pass,
    (status = 'FAIL') AS is_fail
  FROM eval;

END;
