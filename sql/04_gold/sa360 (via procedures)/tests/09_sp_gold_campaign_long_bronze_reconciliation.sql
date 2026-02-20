/*
===============================================================================
FILE: 09_sp_gold_campaign_long_bronze_reconciliation.sql
PROC:  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_gold_sa360_campaign_long_bronze_reconciliation_tests

RECON (LIMITED):
  Gold long daily vs Bronze daily for raw comparable metrics:
    - impressions, clicks, cost (micros->currency), all_conversions

UPDATES:
  - NULL-safe SUM() + row_count checks
  - Clean FAIL if either side has 0 rows in lookback window
===============================================================================
*/

CREATE OR REPLACE PROCEDURE
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_gold_sa360_campaign_long_bronze_reconciliation_tests`()
OPTIONS(strict_mode=false)
BEGIN
  DECLARE lookback_days INT64 DEFAULT 14;
  DECLARE tolerance FLOAT64 DEFAULT 0.000001;

  -- ===========================================================================
  -- TEST 1: Impressions (Gold long vs Bronze)
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
         AND metric_name = 'impressions'
      ) AS gold_rows,

      COALESCE((
        SELECT SUM(COALESCE(impressions,0))
        FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_campaign_daily`
        WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL lookback_days DAY)
      ), 0) AS bronze_sum,

      COALESCE((
        SELECT SUM(COALESCE(metric_value,0))
        FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_campaign_daily_long`
        WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL lookback_days DAY)
          AND metric_name = 'impressions'
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
    'Impressions Reconciliation (Gold long vs Bronze, 14d)',
    'MEDIUM',
    CAST(bronze_sum AS FLOAT64),
    CAST(gold_sum AS FLOAT64),
    CAST(diff AS FLOAT64),
    status,
    IF(status = 'PASS', '🟢', '🔴'),
    CASE
      WHEN bronze_rows = 0 THEN 'Bronze Daily has 0 rows in lookback window (cannot reconcile).'
      WHEN gold_rows   = 0 THEN 'Gold long daily has 0 rows for metric=impressions in lookback window.'
      WHEN status = 'PASS' THEN 'Impressions match (Gold long vs Bronze).'
      ELSE 'Impressions do NOT match (Gold long vs Bronze).'
    END,
    CASE
      WHEN bronze_rows = 0 THEN 'Check Bronze Daily build/backfill for last 14 days.'
      WHEN gold_rows   = 0 THEN 'Check Gold long daily build/unpivot for impressions.'
      WHEN status = 'PASS' THEN 'No action required.'
      ELSE 'Investigate transformations between Bronze->Silver->Gold for impressions.'
    END,
    FALSE,
    (status='PASS') AS is_pass,
    (status='FAIL') AS is_fail
  FROM eval;

  -- ===========================================================================
  -- TEST 2: Clicks (Gold long vs Bronze)
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
         AND metric_name = 'clicks'
      ) AS gold_rows,

      COALESCE((
        SELECT SUM(COALESCE(clicks,0))
        FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_campaign_daily`
        WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL lookback_days DAY)
      ), 0) AS bronze_sum,

      COALESCE((
        SELECT SUM(COALESCE(metric_value,0))
        FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_campaign_daily_long`
        WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL lookback_days DAY)
          AND metric_name = 'clicks'
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
    'Clicks Reconciliation (Gold long vs Bronze, 14d)',
    'MEDIUM',
    CAST(bronze_sum AS FLOAT64),
    CAST(gold_sum AS FLOAT64),
    CAST(diff AS FLOAT64),
    status,
    IF(status = 'PASS', '🟢', '🔴'),
    CASE
      WHEN bronze_rows = 0 THEN 'Bronze Daily has 0 rows in lookback window (cannot reconcile).'
      WHEN gold_rows   = 0 THEN 'Gold long daily has 0 rows for metric=clicks in lookback window.'
      WHEN status = 'PASS' THEN 'Clicks match (Gold long vs Bronze).'
      ELSE 'Clicks do NOT match (Gold long vs Bronze).'
    END,
    CASE
      WHEN bronze_rows = 0 THEN 'Check Bronze Daily build/backfill for last 14 days.'
      WHEN gold_rows   = 0 THEN 'Check Gold long daily build/unpivot for clicks.'
      WHEN status = 'PASS' THEN 'No action required.'
      ELSE 'Investigate transformations between Bronze->Silver->Gold for clicks.'
    END,
    FALSE,
    (status='PASS') AS is_pass,
    (status='FAIL') AS is_fail
  FROM eval;

  -- ===========================================================================
  -- TEST 3: Cost (Bronze micros -> Gold cost)
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
         AND metric_name = 'cost'
      ) AS gold_rows,

      COALESCE((
        SELECT SUM(COALESCE(cost_micros,0))/1000000.0
        FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_campaign_daily`
        WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL lookback_days DAY)
      ), 0) AS bronze_cost,

      COALESCE((
        SELECT SUM(COALESCE(metric_value,0))
        FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_campaign_daily_long`
        WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL lookback_days DAY)
          AND metric_name = 'cost'
      ), 0) AS gold_cost
  ),
  eval AS (
    SELECT
      bronze_rows, gold_rows, bronze_cost, gold_cost,
      (gold_cost - bronze_cost) AS diff,
      CASE
        WHEN bronze_rows = 0 THEN 'FAIL'
        WHEN gold_rows   = 0 THEN 'FAIL'
        WHEN ABS(gold_cost - bronze_cost) <= tolerance THEN 'PASS'
        ELSE 'FAIL'
      END AS status
    FROM stats
  )
  SELECT
    CURRENT_TIMESTAMP(), CURRENT_DATE(),
    'sdi_gold_sa360_campaign_daily_long',
    'reconciliation',
    'Cost Reconciliation (Gold long vs Bronze micros, 14d)',
    'MEDIUM',
    CAST(bronze_cost AS FLOAT64),
    CAST(gold_cost AS FLOAT64),
    CAST(diff AS FLOAT64),
    status,
    IF(status = 'PASS', '🟢', '🔴'),
    CASE
      WHEN bronze_rows = 0 THEN 'Bronze Daily has 0 rows in lookback window (cannot reconcile).'
      WHEN gold_rows   = 0 THEN 'Gold long daily has 0 rows for metric=cost in lookback window.'
      WHEN status = 'PASS' THEN 'Cost matches (Gold long vs Bronze micros).'
      ELSE 'Cost does NOT match (Gold long vs Bronze micros).'
    END,
    CASE
      WHEN bronze_rows = 0 THEN 'Check Bronze Daily build/backfill for last 14 days.'
      WHEN gold_rows   = 0 THEN 'Check Gold long daily build/unpivot for cost.'
      WHEN status = 'PASS' THEN 'No action required.'
      ELSE 'Investigate cost conversions between layers (micros -> currency).'
    END,
    FALSE,
    (status='PASS') AS is_pass,
    (status='FAIL') AS is_fail
  FROM eval;

  -- ===========================================================================
  -- TEST 4: all_conversions (Gold long vs Bronze)
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
         AND metric_name = 'all_conversions'
      ) AS gold_rows,

      COALESCE((
        SELECT SUM(COALESCE(all_conversions,0))
        FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_campaign_daily`
        WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL lookback_days DAY)
      ), 0) AS bronze_sum,

      COALESCE((
        SELECT SUM(COALESCE(metric_value,0))
        FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_campaign_daily_long`
        WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL lookback_days DAY)
          AND metric_name = 'all_conversions'
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
    'All Conversions Reconciliation (Gold long vs Bronze, 14d)',
    'MEDIUM',
    CAST(bronze_sum AS FLOAT64),
    CAST(gold_sum AS FLOAT64),
    CAST(diff AS FLOAT64),
    status,
    IF(status = 'PASS', '🟢', '🔴'),
    CASE
      WHEN bronze_rows = 0 THEN 'Bronze Daily has 0 rows in lookback window (cannot reconcile).'
      WHEN gold_rows   = 0 THEN 'Gold long daily has 0 rows for metric=all_conversions in lookback window.'
      WHEN status = 'PASS' THEN 'All Conversions match (Gold long vs Bronze).'
      ELSE 'All Conversions do NOT match (Gold long vs Bronze).'
    END,
    CASE
      WHEN bronze_rows = 0 THEN 'Check Bronze Daily build/backfill for last 14 days.'
      WHEN gold_rows   = 0 THEN 'Check Gold long daily build/unpivot for all_conversions.'
      WHEN status = 'PASS' THEN 'No action required.'
      ELSE 'Investigate transformations between Bronze->Silver->Gold for all_conversions.'
    END,
    FALSE,
    (status='PASS') AS is_pass,
    (status='FAIL') AS is_fail
  FROM eval;

END;
