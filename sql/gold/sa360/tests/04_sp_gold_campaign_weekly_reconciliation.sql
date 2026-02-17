/*
===============================================================================
FILE: 04_sp_gold_campaign_weekly_reconciliation.sql
LAYER: Gold QA (Reconciliation)

PURPOSE:
  Reconcile Gold Weekly against recomputed rollup from Gold Daily (same QGP logic).

TABLES:
  Gold Daily:  sdi_gold_sa360_campaign_daily
  Gold Weekly: sdi_gold_sa360_campaign_weekly

WINDOW:
  60 days default (covers quarter-end partial buckets)

===============================================================================
*/

CREATE OR REPLACE PROCEDURE
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_gold_campaign_weekly_reconciliation`()
OPTIONS(strict_mode=false)
BEGIN
  DECLARE v_now TIMESTAMP DEFAULT CURRENT_TIMESTAMP();
  DECLARE v_table STRING DEFAULT 'sdi_gold_sa360_campaign_weekly';

  DECLARE v_lookback_days INT64 DEFAULT 60;
  DECLARE v_window_start DATE DEFAULT DATE_SUB(CURRENT_DATE(), INTERVAL v_lookback_days DAY);

  -- TEST 1: Rowcount match (HIGH)
  INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_test_results`
  WITH recomputed_weekly AS (
    WITH base AS (
      SELECT
        d.*,
        DATE_ADD(DATE_TRUNC(d.date, WEEK(SUNDAY)), INTERVAL 6 DAY) AS week_end_saturday,
        DATE_SUB(DATE_ADD(DATE_TRUNC(d.date, QUARTER), INTERVAL 3 MONTH), INTERVAL 1 DAY) AS quarter_end_date,
        DATE_SUB(
          DATE_SUB(DATE_ADD(DATE_TRUNC(d.date, QUARTER), INTERVAL 3 MONTH), INTERVAL 1 DAY),
          INTERVAL MOD(EXTRACT(DAYOFWEEK FROM DATE_SUB(DATE_ADD(DATE_TRUNC(d.date, QUARTER), INTERVAL 3 MONTH), INTERVAL 1 DAY)), 7) DAY
        ) AS last_saturday_before_qe
      FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_campaign_daily` d
      WHERE d.date >= v_window_start
    ),
    bucketed AS (
      SELECT
        *,
        CASE
          WHEN date > last_saturday_before_qe AND date <= quarter_end_date
          THEN quarter_end_date
          ELSE week_end_saturday
        END AS qgp_week
      FROM base
    )
    SELECT
      account_id,
      campaign_id,
      qgp_week,
      SUM(impressions) AS impressions,
      SUM(clicks) AS clicks,
      SUM(cost) AS cost,
      SUM(all_conversions) AS all_conversions
    FROM bucketed
    GROUP BY account_id, campaign_id, qgp_week
  ),
  weekly_table AS (
    SELECT *
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_campaign_weekly`
    WHERE qgp_week >= v_window_start
  ),
  counts AS (
    SELECT
      (SELECT COUNT(*) FROM recomputed_weekly) AS expected_rows,
      (SELECT COUNT(*) FROM weekly_table) AS actual_rows
  )
  SELECT
    v_now,
    CURRENT_DATE(),
    v_table,
    'reconciliation',
    'Rowcount Match vs Recomputed from Gold Daily (60-day)',
    'HIGH',
    CAST(expected_rows AS FLOAT64),
    CAST(actual_rows AS FLOAT64),
    CAST(actual_rows - expected_rows AS FLOAT64),
    IF(actual_rows = expected_rows, 'PASS', 'FAIL'),
    IF(actual_rows = expected_rows, '🟢', '🔴'),
    IF(actual_rows = expected_rows,
      'Rowcount matches recomputed weekly rollup from Gold Daily.',
      CONCAT('Rowcount differs (Weekly=', CAST(actual_rows AS STRING), ', Recomputed=', CAST(expected_rows AS STRING), ').')
    ),
    IF(actual_rows = expected_rows,
      'No action required.',
      'Compare weekly build logic vs recomputation; verify lookback and qgp_week bucketing.'
    ),
    IF(actual_rows = expected_rows, FALSE, TRUE),
    IF(actual_rows = expected_rows, TRUE, FALSE),
    IF(actual_rows = expected_rows, FALSE, TRUE)
  FROM counts;

  -- TEST 2: Missing keys (HIGH)
  INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_test_results`
  WITH recomputed_keys AS (
    WITH base AS (
      SELECT
        d.*,
        DATE_ADD(DATE_TRUNC(d.date, WEEK(SUNDAY)), INTERVAL 6 DAY) AS week_end_saturday,
        DATE_SUB(DATE_ADD(DATE_TRUNC(d.date, QUARTER), INTERVAL 3 MONTH), INTERVAL 1 DAY) AS quarter_end_date,
        DATE_SUB(
          DATE_SUB(DATE_ADD(DATE_TRUNC(d.date, QUARTER), INTERVAL 3 MONTH), INTERVAL 1 DAY),
          INTERVAL MOD(EXTRACT(DAYOFWEEK FROM DATE_SUB(DATE_ADD(DATE_TRUNC(d.date, QUARTER), INTERVAL 3 MONTH), INTERVAL 1 DAY)), 7) DAY
        ) AS last_saturday_before_qe
      FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_campaign_daily` d
      WHERE d.date >= v_window_start
    ),
    bucketed AS (
      SELECT
        *,
        CASE
          WHEN date > last_saturday_before_qe AND date <= quarter_end_date
          THEN quarter_end_date
          ELSE week_end_saturday
        END AS qgp_week
      FROM base
    )
    SELECT DISTINCT account_id, campaign_id, qgp_week
    FROM bucketed
  ),
  weekly_keys AS (
    SELECT DISTINCT account_id, campaign_id, qgp_week
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_campaign_weekly`
    WHERE qgp_week >= v_window_start
  ),
  miss AS (
    SELECT COUNT(*) AS missing_cnt
    FROM recomputed_keys r
    LEFT JOIN weekly_keys w
      ON r.account_id = w.account_id
     AND r.campaign_id = w.campaign_id
     AND r.qgp_week = w.qgp_week
    WHERE w.account_id IS NULL
  )
  SELECT
    v_now,
    CURRENT_DATE(),
    v_table,
    'reconciliation',
    'Missing Weekly Keys vs Recomputed (60-day)',
    'HIGH',
    0.0,
    CAST(missing_cnt AS FLOAT64),
    CAST(missing_cnt AS FLOAT64),
    IF(missing_cnt = 0, 'PASS', 'FAIL'),
    IF(missing_cnt = 0, '🟢', '🔴'),
    IF(missing_cnt = 0,
      'No missing weekly keys.',
      CONCAT('Missing weekly keys detected: ', CAST(missing_cnt AS STRING), '.')
    ),
    IF(missing_cnt = 0,
      'No action required.',
      'Investigate weekly MERGE insert logic; verify lookback covers quarter-end buckets.'
    ),
    IF(missing_cnt = 0, FALSE, TRUE),
    IF(missing_cnt = 0, TRUE, FALSE),
    IF(missing_cnt = 0, FALSE, TRUE)
  FROM miss;

  -- TEST 3: Core sums match (HIGH)
  INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_test_results`
  WITH recomputed_weekly AS (
    WITH base AS (
      SELECT
        d.*,
        DATE_ADD(DATE_TRUNC(d.date, WEEK(SUNDAY)), INTERVAL 6 DAY) AS week_end_saturday,
        DATE_SUB(DATE_ADD(DATE_TRUNC(d.date, QUARTER), INTERVAL 3 MONTH), INTERVAL 1 DAY) AS quarter_end_date,
        DATE_SUB(
          DATE_SUB(DATE_ADD(DATE_TRUNC(d.date, QUARTER), INTERVAL 3 MONTH), INTERVAL 1 DAY),
          INTERVAL MOD(EXTRACT(DAYOFWEEK FROM DATE_SUB(DATE_ADD(DATE_TRUNC(d.date, QUARTER), INTERVAL 3 MONTH), INTERVAL 1 DAY)), 7) DAY
        ) AS last_saturday_before_qe
      FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_campaign_daily` d
      WHERE d.date >= v_window_start
    ),
    bucketed AS (
      SELECT
        *,
        CASE
          WHEN date > last_saturday_before_qe AND date <= quarter_end_date
          THEN quarter_end_date
          ELSE week_end_saturday
        END AS qgp_week
      FROM base
    )
    SELECT
      SUM(impressions) AS impressions,
      SUM(clicks) AS clicks,
      SUM(cost) AS cost,
      SUM(all_conversions) AS all_conversions
    FROM (
      SELECT
        account_id, campaign_id, qgp_week,
        SUM(impressions) AS impressions,
        SUM(clicks) AS clicks,
        SUM(cost) AS cost,
        SUM(all_conversions) AS all_conversions
      FROM bucketed
      GROUP BY 1,2,3
    )
  ),
  weekly_sums AS (
    SELECT
      IFNULL(SUM(impressions),0) AS impressions,
      IFNULL(SUM(clicks),0) AS clicks,
      IFNULL(SUM(cost),0) AS cost,
      IFNULL(SUM(all_conversions),0) AS all_conversions
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_campaign_weekly`
    WHERE qgp_week >= v_window_start
  ),
  calc AS (
    SELECT
      (IF(weekly_sums.impressions = recomputed_weekly.impressions, 0, 1) +
       IF(weekly_sums.clicks = recomputed_weekly.clicks, 0, 1) +
       IF(weekly_sums.cost = recomputed_weekly.cost, 0, 1) +
       IF(weekly_sums.all_conversions = recomputed_weekly.all_conversions, 0, 1)) AS failed_metric_cnt
    FROM weekly_sums CROSS JOIN recomputed_weekly
  )
  SELECT
    v_now,
    CURRENT_DATE(),
    v_table,
    'reconciliation',
    'Core Weekly Metric Sums Match vs Recomputed (60-day)',
    'HIGH',
    0.0,
    CAST(failed_metric_cnt AS FLOAT64),
    CAST(failed_metric_cnt AS FLOAT64),
    IF(failed_metric_cnt = 0, 'PASS', 'FAIL'),
    IF(failed_metric_cnt = 0, '🟢', '🔴'),
    IF(failed_metric_cnt = 0,
      'Weekly core metric sums match recomputed rollup.',
      'Weekly core metric sums differ vs recomputed rollup (bucketing/duplication/filtering issue).'
    ),
    IF(failed_metric_cnt = 0,
      'No action required.',
      'Verify qgp_week computation and MERGE key (account_id,campaign_id,qgp_week).'
    ),
    IF(failed_metric_cnt = 0, FALSE, TRUE),
    IF(failed_metric_cnt = 0, TRUE, FALSE),
    IF(failed_metric_cnt = 0, FALSE, TRUE)
  FROM calc;

END;
