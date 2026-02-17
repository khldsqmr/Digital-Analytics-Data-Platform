/*
===============================================================================
FILE: 02_sp_gold_campaign_daily_reconciliation.sql
LAYER: Gold QA (Reconciliation)
PURPOSE:
  Reconcile Gold Daily table against Silver Daily (same grain).
  Validates that Gold curation did NOT drop rows or distort key metrics.

TABLES:
  Gold:  sdi-gold-sa360-campaign-daily
  Silver: sdi_silver_sa360_campaign_daily

WINDOW:
  Lookback N days (default 7)

TESTS:
  1) Rowcount match (HIGH)
  2) Missing keys in Gold vs Silver (HIGH)
  3) Metric sum match for core metrics (HIGH): impressions, clicks, cost, all_conversions
===============================================================================
*/

CREATE OR REPLACE PROCEDURE
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_gold_campaign_daily_reconciliation`()
OPTIONS(strict_mode=false)
BEGIN

  DECLARE v_table_name STRING DEFAULT 'sdi-gold-sa360-campaign-daily';
  DECLARE v_now TIMESTAMP DEFAULT CURRENT_TIMESTAMP();

  DECLARE v_lookback_days INT64 DEFAULT 7;
  DECLARE v_window_start DATE DEFAULT DATE_SUB(CURRENT_DATE(), INTERVAL v_lookback_days DAY);

  -- 1) Rowcount Match (HIGH)
  INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_test_results`
  WITH counts AS (
    SELECT
      (SELECT COUNT(*) FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_silver_sa360_campaign_daily`
       WHERE date >= v_window_start) AS silver_rows,
      (SELECT COUNT(*) FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi-gold-sa360-campaign-daily`
       WHERE date >= v_window_start) AS gold_rows
  )
  SELECT
    v_now,
    CURRENT_DATE(),
    v_table_name,
    'reconciliation',
    'Rowcount Match vs Silver (7-day)',
    'HIGH',
    CAST(silver_rows AS FLOAT64) AS expected_value,
    CAST(gold_rows AS FLOAT64) AS actual_value,
    CAST(gold_rows - silver_rows AS FLOAT64) AS variance_value,
    IF(gold_rows = silver_rows, 'PASS', 'FAIL') AS status,
    IF(gold_rows = silver_rows, '🟢', '🔴') AS status_emoji,
    IF(gold_rows = silver_rows,
      'Rowcount matches Silver in the lookback window.',
      CONCAT('Rowcount differs (Gold=', CAST(gold_rows AS STRING), ', Silver=', CAST(silver_rows AS STRING), '). Possible filtering or missing inserts.')
    ) AS failure_reason,
    IF(gold_rows = silver_rows,
      'No action required.',
      'Check Gold MERGE WHERE clause / joins; confirm lookback window and partition filter.'
    ) AS next_step,
    IF(gold_rows = silver_rows, FALSE, TRUE) AS is_critical_failure,
    IF(gold_rows = silver_rows, TRUE, FALSE),
    IF(gold_rows = silver_rows, FALSE, TRUE)
  FROM counts;

  -- 2) Missing Keys in Gold (HIGH)
  --    Count Silver grain keys missing in Gold within lookback.
  INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_test_results`
  WITH silver_keys AS (
    SELECT DISTINCT account_id, campaign_id, date
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_silver_sa360_campaign_daily`
    WHERE date >= v_window_start
  ),
  gold_keys AS (
    SELECT DISTINCT account_id, campaign_id, date
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi-gold-sa360-campaign-daily`
    WHERE date >= v_window_start
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
    v_now,
    CURRENT_DATE(),
    v_table_name,
    'reconciliation',
    'Missing Keys vs Silver (7-day)',
    'HIGH',
    0.0 AS expected_value,
    CAST(missing_cnt AS FLOAT64) AS actual_value,
    CAST(missing_cnt AS FLOAT64) - 0.0 AS variance_value,
    IF(missing_cnt = 0, 'PASS', 'FAIL') AS status,
    IF(missing_cnt = 0, '🟢', '🔴') AS status_emoji,
    IF(missing_cnt = 0,
      'No missing keys. Gold fully covers Silver grain keys in the lookback.',
      CONCAT('Missing keys detected. Silver has ', CAST(missing_cnt AS STRING), ' grain keys absent in Gold.')
    ) AS failure_reason,
    IF(missing_cnt = 0,
      'No action required.',
      'Inspect Gold MERGE source selection; ensure INSERT happens for all Silver rows in window.'
    ) AS next_step,
    IF(missing_cnt = 0, FALSE, TRUE) AS is_critical_failure,
    IF(missing_cnt = 0, TRUE, FALSE),
    IF(missing_cnt = 0, FALSE, TRUE)
  FROM miss;

  -- 3) Metric Sum Match (HIGH) - core metrics
  --    We reconcile sums in window for: impressions, clicks, cost, all_conversions
  INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_test_results`
  WITH silver_sums AS (
    SELECT
      IFNULL(SUM(impressions), 0) AS impressions,
      IFNULL(SUM(clicks), 0) AS clicks,
      IFNULL(SUM(cost), 0) AS cost,
      IFNULL(SUM(all_conversions), 0) AS all_conversions
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_silver_sa360_campaign_daily`
    WHERE date >= v_window_start
  ),
  gold_sums AS (
    SELECT
      IFNULL(SUM(impressions), 0) AS impressions,
      IFNULL(SUM(clicks), 0) AS clicks,
      IFNULL(SUM(cost), 0) AS cost,
      IFNULL(SUM(all_conversions), 0) AS all_conversions
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi-gold-sa360-campaign-daily`
    WHERE date >= v_window_start
  ),
  joined AS (
    SELECT
      s.impressions AS s_impressions, g.impressions AS g_impressions,
      s.clicks AS s_clicks, g.clicks AS g_clicks,
      s.cost AS s_cost, g.cost AS g_cost,
      s.all_conversions AS s_all_conv, g.all_conversions AS g_all_conv
    FROM silver_sums s CROSS JOIN gold_sums g
  ),
  calc AS (
    SELECT
      -- Compute "failed metrics count" as a single numeric for the test row
      (IF(g_impressions = s_impressions, 0, 1) +
       IF(g_clicks = s_clicks, 0, 1) +
       IF(g_cost = s_cost, 0, 1) +
       IF(g_all_conv = s_all_conv, 0, 1)) AS failed_metric_cnt
    FROM joined
  )
  SELECT
    v_now,
    CURRENT_DATE(),
    v_table_name,
    'reconciliation',
    'Core Metric Sums Match vs Silver (7-day)',
    'HIGH',
    0.0 AS expected_value,  -- expecting 0 mismatched metrics
    CAST(failed_metric_cnt AS FLOAT64) AS actual_value,
    CAST(failed_metric_cnt AS FLOAT64) - 0.0 AS variance_value,
    IF(failed_metric_cnt = 0, 'PASS', 'FAIL') AS status,
    IF(failed_metric_cnt = 0, '🟢', '🔴') AS status_emoji,
    IF(failed_metric_cnt = 0,
      'Core metric sums match Silver (impressions/clicks/cost/all_conversions).',
      'One or more core metric sums differ vs Silver in the lookback window.'
    ) AS failure_reason,
    IF(failed_metric_cnt = 0,
      'No action required.',
      'Validate Gold selection did not rename/drop metrics; ensure no filters or duplicate rows were introduced.'
    ) AS next_step,
    IF(failed_metric_cnt = 0, FALSE, TRUE) AS is_critical_failure,
    IF(failed_metric_cnt = 0, TRUE, FALSE),
    IF(failed_metric_cnt = 0, FALSE, TRUE)
  FROM calc;

END;
