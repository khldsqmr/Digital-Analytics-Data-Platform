/*
===============================================================================
FILE: 02_sp_gold_campaign_daily_reconciliation.sql
LAYER: Gold QA (Reconciliation)

PURPOSE:
  Reconcile Gold Daily vs Silver Daily in recent window:
    1) Rowcount match (HIGH)
    2) Missing keys in Gold vs Silver (HIGH)
    3) Core metric sums match (HIGH): impressions, clicks, cost, all_conversions

===============================================================================
*/

CREATE OR REPLACE PROCEDURE
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_gold_campaign_daily_reconciliation`()
OPTIONS(strict_mode=false)
BEGIN
  DECLARE v_now TIMESTAMP DEFAULT CURRENT_TIMESTAMP();
  DECLARE v_table STRING DEFAULT 'sdi_gold_sa360_campaign_daily';

  DECLARE v_lookback_days INT64 DEFAULT 14;
  DECLARE v_start DATE DEFAULT DATE_SUB(CURRENT_DATE(), INTERVAL v_lookback_days DAY);

  -- TEST 1: Rowcount match (HIGH)
  INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_test_results`
  WITH c AS (
    SELECT
      (SELECT COUNT(*) FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_silver_sa360_campaign_daily`
       WHERE date >= v_start) AS expected_rows,
      (SELECT COUNT(*) FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_campaign_daily`
       WHERE date >= v_start) AS actual_rows
  )
  SELECT
    v_now, CURRENT_DATE(), v_table,
    'reconciliation',
    'Rowcount Match vs Silver (14-day)',
    'HIGH',
    CAST(expected_rows AS FLOAT64),
    CAST(actual_rows AS FLOAT64),
    CAST(actual_rows - expected_rows AS FLOAT64),
    IF(actual_rows = expected_rows,'PASS','FAIL'),
    IF(actual_rows = expected_rows,'🟢','🔴'),
    IF(actual_rows = expected_rows,
      'Rowcount matches Silver in window.',
      CONCAT('Rowcount differs (Gold=', CAST(actual_rows AS STRING), ', Silver=', CAST(expected_rows AS STRING), ').')
    ),
    IF(actual_rows = expected_rows,
      'No action required.',
      'Run Gold Daily MERGE; verify lookback; ensure Gold selection includes all Silver rows.'
    ),
    IF(actual_rows = expected_rows,FALSE,TRUE),
    IF(actual_rows = expected_rows,TRUE,FALSE),
    IF(actual_rows = expected_rows,FALSE,TRUE)
  FROM c;

  -- TEST 2: Missing keys (HIGH)
  INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_test_results`
  WITH silver_keys AS (
    SELECT DISTINCT account_id, campaign_id, date
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_silver_sa360_campaign_daily`
    WHERE date >= v_start
  ),
  gold_keys AS (
    SELECT DISTINCT account_id, campaign_id, date
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_campaign_daily`
    WHERE date >= v_start
  ),
  miss AS (
    SELECT COUNT(*) AS missing_cnt
    FROM silver_keys s
    LEFT JOIN gold_keys g
      ON s.account_id = g.account_id
     AND s.campaign_id = g.campaign_id
     AND s.date = g.date
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
    IF(missing_cnt = 0,'PASS','FAIL'),
    IF(missing_cnt = 0,'🟢','🔴'),
    IF(missing_cnt = 0,
      'No missing daily keys. Gold covers Silver keys in window.',
      CONCAT('Missing Gold keys: ', CAST(missing_cnt AS STRING), ' Silver keys absent in Gold.')
    ),
    IF(missing_cnt = 0,
      'No action required.',
      'Fix Gold Daily MERGE inserts; check window; check table name/location.'
    ),
    IF(missing_cnt = 0,FALSE,TRUE),
    IF(missing_cnt = 0,TRUE,FALSE),
    IF(missing_cnt = 0,FALSE,TRUE)
  FROM miss;

  -- TEST 3: Core metric sums match (HIGH)
  INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_test_results`
  WITH silver AS (
    SELECT
      IFNULL(SUM(impressions),0) AS impressions,
      IFNULL(SUM(clicks),0) AS clicks,
      IFNULL(SUM(cost),0) AS cost,
      IFNULL(SUM(all_conversions),0) AS all_conversions
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_silver_sa360_campaign_daily`
    WHERE date >= v_start
  ),
  gold AS (
    SELECT
      IFNULL(SUM(impressions),0) AS impressions,
      IFNULL(SUM(clicks),0) AS clicks,
      IFNULL(SUM(cost),0) AS cost,
      IFNULL(SUM(all_conversions),0) AS all_conversions
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_campaign_daily`
    WHERE date >= v_start
  ),
  calc AS (
    SELECT
      (IF(gold.impressions = silver.impressions, 0, 1) +
       IF(gold.clicks = silver.clicks, 0, 1) +
       IF(gold.cost = silver.cost, 0, 1) +
       IF(gold.all_conversions = silver.all_conversions, 0, 1)) AS failed_metric_cnt
    FROM silver CROSS JOIN gold
  )
  SELECT
    v_now, CURRENT_DATE(), v_table,
    'reconciliation',
    'Core Metric Sums Match vs Silver (14-day)',
    'HIGH',
    0.0,
    CAST(failed_metric_cnt AS FLOAT64),
    CAST(failed_metric_cnt AS FLOAT64),
    IF(failed_metric_cnt = 0,'PASS','FAIL'),
    IF(failed_metric_cnt = 0,'🟢','🔴'),
    IF(failed_metric_cnt = 0,
      'Gold core metric sums match Silver in window.',
      'Gold core metric sums differ vs Silver (filtering/duplication/missing rows).'
    ),
    IF(failed_metric_cnt = 0,
      'No action required.',
      'Check Gold selection and MERGE logic; ensure no columns were dropped/renamed.'
    ),
    IF(failed_metric_cnt = 0,FALSE,TRUE),
    IF(failed_metric_cnt = 0,TRUE,FALSE),
    IF(failed_metric_cnt = 0,FALSE,TRUE)
  FROM calc;

END;
