/*
===============================================================================
FILE: 02_sp_gold_campaign_daily_reconciliation.sql
LAYER: Gold QA (Reconciliation)

PURPOSE:
  Reconcile Gold Daily against Silver Daily for correctness.

TABLES:
  Silver Daily: prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_silver_sa360_campaign_daily
  Gold Daily:   prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_campaign_daily

GRAIN:
  account_id + campaign_id + date

WINDOW:
  last N days (default 14)

TESTS (HIGH):
  1) Rowcount Match
  2) Missing Gold Keys vs Silver
  3) Core Metric Sums Match (with tolerance)
===============================================================================
*/

CREATE OR REPLACE PROCEDURE
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_gold_campaign_daily_reconciliation`()
OPTIONS(strict_mode=false)
BEGIN

  DECLARE v_table STRING DEFAULT 'sdi_gold_sa360_campaign_daily';
  DECLARE v_now TIMESTAMP DEFAULT CURRENT_TIMESTAMP();

  DECLARE v_window_days INT64 DEFAULT 14;
  DECLARE v_start DATE DEFAULT DATE_SUB(CURRENT_DATE(), INTERVAL v_window_days DAY);

  DECLARE v_cost_tol FLOAT64 DEFAULT 0.01;      -- currency tolerance
  DECLARE v_conv_tol FLOAT64 DEFAULT 0.0001;    -- conversion tolerance (float)

  WITH silver AS (
    SELECT account_id, campaign_id, date, impressions, clicks, cost, all_conversions
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_silver_sa360_campaign_daily`
    WHERE date >= v_start
  ),
  gold AS (
    SELECT account_id, campaign_id, date, impressions, clicks, cost, all_conversions
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_campaign_daily`
    WHERE date >= v_start
  )

  -- ---------------------------------------------------------------------------
  -- TEST 1: Rowcount Match (HIGH)
  -- ---------------------------------------------------------------------------
  INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_test_results`
  WITH counts AS (
    SELECT
      (SELECT COUNT(*) FROM silver) AS expected_rows,
      (SELECT COUNT(*) FROM gold) AS actual_rows
  )
  SELECT
    v_now, CURRENT_DATE(), v_table,
    'reconciliation',
    'Rowcount Match vs Silver (14-day)',
    'HIGH',
    CAST(expected_rows AS FLOAT64),
    CAST(actual_rows AS FLOAT64),
    CAST(actual_rows - expected_rows AS FLOAT64),
    IF(actual_rows = expected_rows, 'PASS', 'FAIL'),
    IF(actual_rows = expected_rows, '🟢', '🔴'),
    IF(actual_rows = expected_rows,
      'Rowcount matches Silver in window.',
      CONCAT('Rowcount differs (Gold=', CAST(actual_rows AS STRING), ', Silver=', CAST(expected_rows AS STRING), ').')
    ),
    IF(actual_rows = expected_rows,
      'No action required.',
      'Check Gold Daily MERGE lookback, filters, or key mismatches.'
    ),
    IF(actual_rows = expected_rows, FALSE, TRUE),
    IF(actual_rows = expected_rows, TRUE, FALSE),
    IF(actual_rows = expected_rows, FALSE, TRUE)
  FROM counts;

  -- ---------------------------------------------------------------------------
  -- TEST 2: Missing Gold Keys vs Silver (HIGH)
  -- ---------------------------------------------------------------------------
  INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_test_results`
  WITH missing AS (
    SELECT COUNT(*) AS missing_cnt
    FROM (
      SELECT DISTINCT account_id, campaign_id, date FROM silver
    ) s
    LEFT JOIN (
      SELECT DISTINCT account_id, campaign_id, date FROM gold
    ) g
    USING (account_id, campaign_id, date)
    WHERE g.account_id IS NULL
  )
  SELECT
    v_now, CURRENT_DATE(), v_table,
    'reconciliation',
    'Missing Gold Daily Keys vs Silver (14-day)',
    'HIGH',
    0.0,
    CAST(missing_cnt AS FLOAT64),
    CAST(missing_cnt AS FLOAT64),
    IF(missing_cnt = 0, 'PASS', 'FAIL'),
    IF(missing_cnt = 0, '🟢', '🔴'),
    IF(missing_cnt = 0,
      'No missing daily keys. Gold covers Silver keys in window.',
      CONCAT('Missing Gold daily keys detected: ', CAST(missing_cnt AS STRING), '.')
    ),
    IF(missing_cnt = 0,
      'No action required.',
      'Investigate Gold MERGE filters and upstream availability.'
    ),
    IF(missing_cnt = 0, FALSE, TRUE),
    IF(missing_cnt = 0, TRUE, FALSE),
    IF(missing_cnt = 0, FALSE, TRUE)
  FROM missing;

  -- ---------------------------------------------------------------------------
  -- TEST 3: Core Metric Sums Match vs Silver (HIGH, tolerant)
  --   impressions/clicks expected integers -> exact
  --   cost/all_conversions -> tolerance
  -- ---------------------------------------------------------------------------
  INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_test_results`
  WITH s AS (
    SELECT
      IFNULL(SUM(impressions),0) AS impressions,
      IFNULL(SUM(clicks),0) AS clicks,
      IFNULL(SUM(cost),0) AS cost,
      IFNULL(SUM(all_conversions),0) AS all_conversions
    FROM silver
  ),
  g AS (
    SELECT
      IFNULL(SUM(impressions),0) AS impressions,
      IFNULL(SUM(clicks),0) AS clicks,
      IFNULL(SUM(cost),0) AS cost,
      IFNULL(SUM(all_conversions),0) AS all_conversions
    FROM gold
  ),
  calc AS (
    SELECT
      (IF(g.impressions = s.impressions, 0, 1) +
       IF(g.clicks = s.clicks, 0, 1) +
       IF(ABS(g.cost - s.cost) <= v_cost_tol, 0, 1) +
       IF(ABS(g.all_conversions - s.all_conversions) <= v_conv_tol, 0, 1)
      ) AS failed_metric_cnt
    FROM s, g
  )
  SELECT
    v_now, CURRENT_DATE(), v_table,
    'reconciliation',
    'Core Metric Sums Match vs Silver (14-day)',
    'HIGH',
    0.0,
    CAST(failed_metric_cnt AS FLOAT64),
    CAST(failed_metric_cnt AS FLOAT64),
    IF(failed_metric_cnt = 0, 'PASS', 'FAIL'),
    IF(failed_metric_cnt = 0, '🟢', '🔴'),
    IF(failed_metric_cnt = 0,
      'Gold core metric sums match Silver (tolerant).',
      'Gold core metric sums differ vs Silver (filtering/duplication/missing rows OR tolerance too strict).'
    ),
    IF(failed_metric_cnt = 0,
      'No action required.',
      'Compare totals by metric; verify types/tolerance; check Gold MERGE selection.'
    ),
    IF(failed_metric_cnt = 0, FALSE, TRUE),
    IF(failed_metric_cnt = 0, TRUE, FALSE),
    IF(failed_metric_cnt = 0, FALSE, TRUE)
  FROM calc;

END;
