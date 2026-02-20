/*
===============================================================================
FILE: 06_sp_gold_campaign_long_daily_reconciliation.sql
PROC:  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_gold_sa360_campaign_long_daily_reconciliation_tests
RECON: Gold long daily vs Gold wide daily (same window)

UPDATES:
  - NULL-safe SUM() + row_count checks
  - Clean FAIL when either long or wide side has 0 rows in window
===============================================================================
*/

CREATE OR REPLACE PROCEDURE
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_gold_sa360_campaign_long_daily_reconciliation_tests`()
OPTIONS(strict_mode=false)
BEGIN
  DECLARE lookback_days INT64 DEFAULT 14;
  DECLARE tolerance FLOAT64 DEFAULT 0.000001;

  -- ===========================================================================
  -- TEST 1: impressions (Gold long vs Gold wide)
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
         AND metric_name = 'impressions'
      ) AS long_rows,

      COALESCE((
        SELECT SUM(COALESCE(impressions,0))
        FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_campaign_daily`
        WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL lookback_days DAY)
      ), 0) AS wide_sum,

      COALESCE((
        SELECT SUM(COALESCE(metric_value,0))
        FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_campaign_daily_long`
        WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL lookback_days DAY)
          AND metric_name = 'impressions'
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
    'Impressions Reconciliation (Gold long vs Gold wide, 14d)',
    'HIGH',
    CAST(wide_sum AS FLOAT64),
    CAST(long_sum AS FLOAT64),
    CAST(diff AS FLOAT64),
    status,
    IF(status = 'PASS', '🟢', '🔴'),
    CASE
      WHEN wide_rows = 0 THEN 'Gold wide daily has 0 rows in lookback window (cannot reconcile).'
      WHEN long_rows = 0 THEN 'Gold long daily has 0 rows for metric=impressions in lookback window.'
      WHEN status = 'PASS' THEN 'Gold long matches Gold wide for impressions.'
      ELSE 'Gold long does NOT match Gold wide for impressions.'
    END,
    CASE
      WHEN wide_rows = 0 THEN 'Check Gold wide daily build/backfill.'
      WHEN long_rows = 0 THEN 'Check Gold long daily build/unpivot for impressions.'
      WHEN status = 'PASS' THEN 'No action required.'
      ELSE 'Fix long build/unpivot logic for impressions.'
    END,
    (status = 'FAIL') AS is_critical_failure,
    (status = 'PASS') AS is_pass,
    (status = 'FAIL') AS is_fail
  FROM eval;

  -- ===========================================================================
  -- TEST 2: cart_start (Gold long vs Gold wide)
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
    IF(status = 'PASS', '🟢', '🔴'),
    CASE
      WHEN wide_rows = 0 THEN 'Gold wide daily has 0 rows in lookback window (cannot reconcile).'
      WHEN long_rows = 0 THEN 'Gold long daily has 0 rows for metric=cart_start in lookback window.'
      WHEN status = 'PASS' THEN 'Gold long matches Gold wide for cart_start.'
      ELSE 'Gold long does NOT match Gold wide for cart_start.'
    END,
    CASE
      WHEN wide_rows = 0 THEN 'Check Gold wide daily build/backfill.'
      WHEN long_rows = 0 THEN 'Check Gold long daily build/unpivot for cart_start.'
      WHEN status = 'PASS' THEN 'No action required.'
      ELSE 'Fix long build/unpivot logic for cart_start.'
    END,
    (status = 'FAIL') AS is_critical_failure,
    (status = 'PASS') AS is_pass,
    (status = 'FAIL') AS is_fail
  FROM eval;

END;
