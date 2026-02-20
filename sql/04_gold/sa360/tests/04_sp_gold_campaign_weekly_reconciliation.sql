/*
===============================================================================
FILE: 04_sp_gold_campaign_weekly_reconciliation.sql
LAYER: Gold QA (Reconciliation)

PURPOSE:
  Reconcile Gold Weekly against a recomputed weekly rollup derived from Gold Daily
  using the EXACT SAME QGP week bucketing logic.

TABLES:
  Gold Daily:  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_campaign_daily
  Gold Weekly: prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_campaign_weekly

GRAIN:
  account_id + campaign_id + qgp_week

WINDOW:
  Lookback N days (default 60), filtered by qgp_week >= window_start.
  Uses spill window (window_start - 6 days) to avoid bucket clipping.

TESTS (HIGH):
  1) Rowcount match
  2) Missing keys in Weekly vs recomputed
  3) Unexpected extra keys in Weekly vs recomputed  <-- catches stale keys
  4) Core metric sum match (tolerant)

NOTES:
  - If extra keys exist, core sums will differ.
  - Best practice fix: weekly MERGE should delete stale keys within window:
      WHEN NOT MATCHED BY SOURCE AND T.qgp_week >= window_start THEN DELETE
===============================================================================
*/

CREATE OR REPLACE PROCEDURE
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_gold_campaign_weekly_reconciliation`()
OPTIONS(strict_mode=false)
BEGIN

  DECLARE v_table STRING DEFAULT 'sdi_gold_sa360_campaign_weekly';
  DECLARE v_now TIMESTAMP DEFAULT CURRENT_TIMESTAMP();

  DECLARE v_lookback_days INT64 DEFAULT 60;
  DECLARE v_window_start DATE DEFAULT DATE_SUB(CURRENT_DATE(), INTERVAL v_lookback_days DAY);

  DECLARE v_spill_days INT64 DEFAULT 6;
  DECLARE v_daily_pull_start DATE DEFAULT DATE_SUB(v_window_start, INTERVAL v_spill_days DAY);

  DECLARE v_cost_tol FLOAT64 DEFAULT 0.01;
  DECLARE v_conv_tol FLOAT64 DEFAULT 0.0001;

  -- ---------------------------------------------------------------------------
  -- Recompute weekly from Gold Daily using SAME QGP week bucketing logic
  -- (spill-window safe)
  -- ---------------------------------------------------------------------------
  WITH recomputed_weekly AS (
    WITH base AS (
      SELECT
        d.*,

        -- Saturday end-of-week for Sun->Sat weeks
        DATE_ADD(DATE_TRUNC(d.date, WEEK(SUNDAY)), INTERVAL 6 DAY) AS week_end_saturday,

        -- Quarter end date
        DATE_SUB(DATE_ADD(DATE_TRUNC(d.date, QUARTER), INTERVAL 3 MONTH), INTERVAL 1 DAY) AS quarter_end_date,

        -- Last Saturday on/before quarter end
        DATE_SUB(
          DATE_SUB(DATE_ADD(DATE_TRUNC(d.date, QUARTER), INTERVAL 3 MONTH), INTERVAL 1 DAY),
          INTERVAL MOD(
            EXTRACT(DAYOFWEEK FROM DATE_SUB(DATE_ADD(DATE_TRUNC(d.date, QUARTER), INTERVAL 3 MONTH), INTERVAL 1 DAY)),
            7
          ) DAY
        ) AS last_saturday_before_qe

      FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_campaign_daily` d
      WHERE d.date >= v_daily_pull_start
    ),
    bucketed AS (
      SELECT
        *,

        -- QGP bucket end date:
        --   - usual: Saturday week end
        --   - quarter-end: quarter_end_date captures the partial tail after last Saturday
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

      ANY_VALUE(account_name) AS account_name,
      ANY_VALUE(campaign_name) AS campaign_name,
      ANY_VALUE(lob) AS lob,
      ANY_VALUE(ad_platform) AS ad_platform,
      ANY_VALUE(campaign_type) AS campaign_type,
      ANY_VALUE(advertising_channel_type) AS advertising_channel_type,
      ANY_VALUE(advertising_channel_sub_type) AS advertising_channel_sub_type,
      ANY_VALUE(bidding_strategy_type) AS bidding_strategy_type,
      ANY_VALUE(serving_status) AS serving_status,

      ANY_VALUE(customer_id) AS customer_id,
      ANY_VALUE(customer_name) AS customer_name,
      ANY_VALUE(resource_name) AS resource_name,
      ANY_VALUE(client_manager_id) AS client_manager_id,
      ANY_VALUE(client_manager_name) AS client_manager_name,

      SUM(impressions) AS impressions,
      SUM(clicks) AS clicks,
      SUM(cost) AS cost,
      SUM(all_conversions) AS all_conversions,

      MAX(file_load_datetime) AS file_load_datetime
    FROM bucketed
    WHERE qgp_week >= v_window_start
    GROUP BY account_id, campaign_id, qgp_week
  ),
  weekly_table AS (
    SELECT *
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_campaign_weekly`
    WHERE qgp_week >= v_window_start
  )

  -- ---------------------------------------------------------------------------
  -- TEST 1: Rowcount Match (HIGH)
  -- ---------------------------------------------------------------------------
  INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_test_results`
  WITH counts AS (
    SELECT
      (SELECT COUNT(*) FROM recomputed_weekly) AS expected_rows,
      (SELECT COUNT(*) FROM weekly_table) AS actual_rows
  )
  SELECT
    v_now, CURRENT_DATE(), v_table,
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
      CONCAT('Rowcount differs (Weekly=', CAST(actual_rows AS STRING),
             ', Recomputed=', CAST(expected_rows AS STRING), '). Possible stale keys or bucketing mismatch.')
    ),
    IF(actual_rows = expected_rows,
      'No action required.',
      'Check for extra keys in Weekly (stale MERGE). Consider deleting stale keys within window.'
    ),
    IF(actual_rows = expected_rows, FALSE, TRUE),
    IF(actual_rows = expected_rows, TRUE, FALSE),
    IF(actual_rows = expected_rows, FALSE, TRUE)
  FROM counts;

  -- ---------------------------------------------------------------------------
  -- TEST 2: Missing Keys in Weekly vs Recomputed (HIGH)
  -- ---------------------------------------------------------------------------
  INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_test_results`
  WITH expected_keys AS (
    SELECT DISTINCT account_id, campaign_id, qgp_week
    FROM recomputed_weekly
  ),
  actual_keys AS (
    SELECT DISTINCT account_id, campaign_id, qgp_week
    FROM weekly_table
  ),
  miss AS (
    SELECT COUNT(*) AS missing_cnt
    FROM expected_keys e
    LEFT JOIN actual_keys a
      ON e.account_id = a.account_id
     AND e.campaign_id = a.campaign_id
     AND e.qgp_week = a.qgp_week
    WHERE a.account_id IS NULL
  )
  SELECT
    v_now, CURRENT_DATE(), v_table,
    'reconciliation',
    'Missing Weekly Keys vs Recomputed (60-day)',
    'HIGH',
    0.0,
    CAST(missing_cnt AS FLOAT64),
    CAST(missing_cnt AS FLOAT64),
    IF(missing_cnt = 0, 'PASS', 'FAIL'),
    IF(missing_cnt = 0, '🟢', '🔴'),
    IF(missing_cnt = 0,
      'No missing weekly keys. Weekly fully covers recomputed keys.',
      CONCAT('Missing weekly keys detected: ', CAST(missing_cnt AS STRING), '.')
    ),
    IF(missing_cnt = 0,
      'No action required.',
      'Investigate weekly MERGE window/lookback; verify qgp_week logic matches recomputation.'
    ),
    IF(missing_cnt = 0, FALSE, TRUE),
    IF(missing_cnt = 0, TRUE, FALSE),
    IF(missing_cnt = 0, FALSE, TRUE)
  FROM miss;

  -- ---------------------------------------------------------------------------
  -- TEST 3: Unexpected Extra Keys in Weekly vs Recomputed (HIGH)
  --   This catches your "+4 rows" issue (stale keys not deleted by MERGE).
  -- ---------------------------------------------------------------------------
  INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_test_results`
  WITH expected_keys AS (
    SELECT DISTINCT account_id, campaign_id, qgp_week
    FROM recomputed_weekly
  ),
  actual_keys AS (
    SELECT DISTINCT account_id, campaign_id, qgp_week
    FROM weekly_table
  ),
  extra AS (
    SELECT COUNT(*) AS extra_cnt
    FROM actual_keys a
    LEFT JOIN expected_keys e
      ON a.account_id = e.account_id
     AND a.campaign_id = e.campaign_id
     AND a.qgp_week = e.qgp_week
    WHERE e.account_id IS NULL
  )
  SELECT
    v_now, CURRENT_DATE(), v_table,
    'reconciliation',
    'Unexpected Weekly Keys not in Recomputed (60-day)',
    'HIGH',
    0.0,
    CAST(extra_cnt AS FLOAT64),
    CAST(extra_cnt AS FLOAT64),
    IF(extra_cnt = 0, 'PASS', 'FAIL'),
    IF(extra_cnt = 0, '🟢', '🔴'),
    IF(extra_cnt = 0,
      'No unexpected weekly keys. Weekly keys match recomputed keys.',
      CONCAT('Weekly contains unexpected keys: ', CAST(extra_cnt AS STRING), ' (likely stale MERGE rows).')
    ),
    IF(extra_cnt = 0,
      'No action required.',
      'Fix weekly MERGE to delete stale keys in-window (WHEN NOT MATCHED BY SOURCE ... THEN DELETE) or rebuild weekly for window.'
    ),
    IF(extra_cnt = 0, FALSE, TRUE),
    IF(extra_cnt = 0, TRUE, FALSE),
    IF(extra_cnt = 0, FALSE, TRUE)
  FROM extra;

  -- ---------------------------------------------------------------------------
  -- TEST 4: Core Metric Sums Match (HIGH, tolerant)
  -- ---------------------------------------------------------------------------
  INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_test_results`
  WITH e AS (
    SELECT
      IFNULL(SUM(impressions),0) AS impressions,
      IFNULL(SUM(clicks),0) AS clicks,
      IFNULL(SUM(cost),0) AS cost,
      IFNULL(SUM(all_conversions),0) AS all_conversions
    FROM recomputed_weekly
  ),
  a AS (
    SELECT
      IFNULL(SUM(impressions),0) AS impressions,
      IFNULL(SUM(clicks),0) AS clicks,
      IFNULL(SUM(cost),0) AS cost,
      IFNULL(SUM(all_conversions),0) AS all_conversions
    FROM weekly_table
  ),
  calc AS (
    SELECT
      (IF(a.impressions = e.impressions, 0, 1) +
       IF(a.clicks = e.clicks, 0, 1) +
       IF(ABS(a.cost - e.cost) <= v_cost_tol, 0, 1) +
       IF(ABS(a.all_conversions - e.all_conversions) <= v_conv_tol, 0, 1)
      ) AS failed_metric_cnt
    FROM e, a
  )
  SELECT
    v_now, CURRENT_DATE(), v_table,
    'reconciliation',
    'Core Weekly Metric Sums Match vs Recomputed (60-day)',
    'HIGH',
    0.0,
    CAST(failed_metric_cnt AS FLOAT64),
    CAST(failed_metric_cnt AS FLOAT64),
    IF(failed_metric_cnt = 0, 'PASS', 'FAIL'),
    IF(failed_metric_cnt = 0, '🟢', '🔴'),
    IF(failed_metric_cnt = 0,
      'Weekly core metric sums match recomputed rollup from Gold Daily (tolerant).',
      'Weekly core metric sums differ vs recomputed rollup (extra keys, bucketing mismatch, or tolerance too strict).'
    ),
    IF(failed_metric_cnt = 0,
      'No action required.',
      'Check extra keys test above; fix weekly MERGE delete; validate qgp_week logic and spill window.'
    ),
    IF(failed_metric_cnt = 0, FALSE, TRUE),
    IF(failed_metric_cnt = 0, TRUE, FALSE),
    IF(failed_metric_cnt = 0, FALSE, TRUE)
  FROM calc;

END;
