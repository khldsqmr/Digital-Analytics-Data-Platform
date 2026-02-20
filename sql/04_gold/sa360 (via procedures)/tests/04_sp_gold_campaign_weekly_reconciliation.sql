/*
===============================================================================
FILE: 04_sp_gold_campaign_weekly_reconciliation.sql  (UPDATED)
LAYER: Gold | QA
PROC:  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_gold_sa360_campaign_weekly_reconciliation_tests

CHANGES:
  - Daily bucket derivation uses fn_qgp_week(date) to match build
===============================================================================
*/

CREATE OR REPLACE PROCEDURE
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_gold_sa360_campaign_weekly_reconciliation_tests`()
OPTIONS(strict_mode=false)
BEGIN
  DECLARE sample_weeks INT64 DEFAULT 4;
  DECLARE tolerance    FLOAT64 DEFAULT 0.000001;

  -- ---------------------------------------------------------------------------
  -- TEST 1: cart_start weekly == SUM(daily) per qgp_week (last N qgp_weeks)
  -- ---------------------------------------------------------------------------
  INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_test_results`
  WITH daily_bucketed AS (
    SELECT
      `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.fn_qgp_week`(date) AS qgp_week,
      cart_start
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_campaign_daily`
    WHERE date IS NOT NULL
  ),
  qgp_list AS (
    SELECT qgp_week
    FROM (
      SELECT DISTINCT qgp_week
      FROM daily_bucketed
      WHERE qgp_week IS NOT NULL
    )
    QUALIFY ROW_NUMBER() OVER (ORDER BY qgp_week DESC) <= sample_weeks
  ),
  daily_rollup AS (
    SELECT
      qgp_week,
      SUM(COALESCE(cart_start,0)) AS daily_val
    FROM daily_bucketed
    WHERE qgp_week IN (SELECT qgp_week FROM qgp_list)
    GROUP BY 1
  ),
  weekly_rollup AS (
    SELECT
      qgp_week,
      SUM(COALESCE(cart_start,0)) AS weekly_val
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_campaign_weekly`
    WHERE qgp_week IN (SELECT qgp_week FROM qgp_list)
    GROUP BY 1
  ),
  aligned AS (
    SELECT
      l.qgp_week,
      d.daily_val  AS expected_value,
      w.weekly_val AS actual_value
    FROM qgp_list l
    LEFT JOIN daily_rollup d USING (qgp_week)
    LEFT JOIN weekly_rollup w USING (qgp_week)
  )
  SELECT
    CURRENT_TIMESTAMP() AS test_run_timestamp,
    CURRENT_DATE()      AS test_date,
    'sdi_gold_sa360_campaign_weekly' AS table_name,
    'reconciliation'    AS test_layer,
    CONCAT('Cart Start Weekly == SUM(Daily) | qgp_week=', CAST(qgp_week AS STRING)) AS test_name,
    'HIGH'              AS severity_level,
    CAST(expected_value AS FLOAT64) AS expected_value,
    CAST(actual_value   AS FLOAT64) AS actual_value,
    CAST(actual_value - expected_value AS FLOAT64) AS variance_value,
    CASE
      WHEN expected_value IS NULL THEN 'FAIL'
      WHEN actual_value   IS NULL THEN 'FAIL'
      WHEN ABS(actual_value - expected_value) <= tolerance THEN 'PASS'
      ELSE 'FAIL'
    END AS status,
    CASE
      WHEN expected_value IS NULL THEN '🔴'
      WHEN actual_value   IS NULL THEN '🔴'
      WHEN ABS(actual_value - expected_value) <= tolerance THEN '🟢'
      ELSE '🔴'
    END AS status_emoji,
    CASE
      WHEN expected_value IS NULL THEN 'Daily rollup missing for this qgp_week (unexpected).'
      WHEN actual_value   IS NULL THEN 'Weekly rollup missing for this qgp_week (weekly not built / filtered).'
      WHEN ABS(actual_value - expected_value) <= tolerance THEN 'Weekly equals SUM(Daily) for this qgp_week.'
      ELSE 'Weekly does NOT equal SUM(Daily) for this qgp_week.'
    END AS failure_reason,
    CASE
      WHEN expected_value IS NULL THEN 'Validate Gold Daily availability and qgp_week bucketing.'
      WHEN actual_value   IS NULL THEN 'Validate Gold Weekly build coverage/window; ensure it includes this qgp_week.'
      WHEN ABS(actual_value - expected_value) <= tolerance THEN 'No action required.'
      ELSE 'Verify weekly build is sourced only from Gold Daily and uses the same qgp_week bucketing.'
    END AS next_step,
    CASE
      WHEN expected_value IS NULL THEN TRUE
      WHEN actual_value   IS NULL THEN TRUE
      WHEN ABS(actual_value - expected_value) > tolerance THEN TRUE
      ELSE FALSE
    END AS is_critical_failure,
    CASE
      WHEN expected_value IS NOT NULL
       AND actual_value IS NOT NULL
       AND ABS(actual_value - expected_value) <= tolerance THEN TRUE
      ELSE FALSE
    END AS is_pass,
    CASE
      WHEN expected_value IS NULL THEN TRUE
      WHEN actual_value   IS NULL THEN TRUE
      WHEN ABS(actual_value - expected_value) > tolerance THEN TRUE
      ELSE FALSE
    END AS is_fail
  FROM aligned;

  -- ---------------------------------------------------------------------------
  -- TEST 2: postpaid_pspv weekly == SUM(daily) per qgp_week (last N qgp_weeks)
  -- ---------------------------------------------------------------------------
  INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_test_results`
  WITH daily_bucketed AS (
    SELECT
      `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.fn_qgp_week`(date) AS qgp_week,
      postpaid_pspv
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_campaign_daily`
    WHERE date IS NOT NULL
  ),
  qgp_list AS (
    SELECT qgp_week
    FROM (
      SELECT DISTINCT qgp_week
      FROM daily_bucketed
      WHERE qgp_week IS NOT NULL
    )
    QUALIFY ROW_NUMBER() OVER (ORDER BY qgp_week DESC) <= sample_weeks
  ),
  daily_rollup AS (
    SELECT
      qgp_week,
      SUM(COALESCE(postpaid_pspv,0)) AS daily_val
    FROM daily_bucketed
    WHERE qgp_week IN (SELECT qgp_week FROM qgp_list)
    GROUP BY 1
  ),
  weekly_rollup AS (
    SELECT
      qgp_week,
      SUM(COALESCE(postpaid_pspv,0)) AS weekly_val
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_campaign_weekly`
    WHERE qgp_week IN (SELECT qgp_week FROM qgp_list)
    GROUP BY 1
  ),
  aligned AS (
    SELECT
      l.qgp_week,
      d.daily_val  AS expected_value,
      w.weekly_val AS actual_value
    FROM qgp_list l
    LEFT JOIN daily_rollup d USING (qgp_week)
    LEFT JOIN weekly_rollup w USING (qgp_week)
  )
  SELECT
    CURRENT_TIMESTAMP(),
    CURRENT_DATE(),
    'sdi_gold_sa360_campaign_weekly',
    'reconciliation',
    CONCAT('Postpaid PSPV Weekly == SUM(Daily) | qgp_week=', CAST(qgp_week AS STRING)),
    'HIGH',
    CAST(expected_value AS FLOAT64),
    CAST(actual_value   AS FLOAT64),
    CAST(actual_value - expected_value AS FLOAT64),
    CASE
      WHEN expected_value IS NULL THEN 'FAIL'
      WHEN actual_value   IS NULL THEN 'FAIL'
      WHEN ABS(actual_value - expected_value) <= tolerance THEN 'PASS'
      ELSE 'FAIL'
    END,
    CASE
      WHEN expected_value IS NULL THEN '🔴'
      WHEN actual_value   IS NULL THEN '🔴'
      WHEN ABS(actual_value - expected_value) <= tolerance THEN '🟢'
      ELSE '🔴'
    END,
    CASE
      WHEN expected_value IS NULL THEN 'Daily rollup missing for this qgp_week (unexpected).'
      WHEN actual_value   IS NULL THEN 'Weekly rollup missing for this qgp_week (weekly not built / filtered).'
      WHEN ABS(actual_value - expected_value) <= tolerance THEN 'Weekly equals SUM(Daily) for this qgp_week.'
      ELSE 'Weekly does NOT equal SUM(Daily) for this qgp_week.'
    END,
    CASE
      WHEN expected_value IS NULL THEN 'Validate Gold Daily availability and qgp_week bucketing.'
      WHEN actual_value   IS NULL THEN 'Validate Gold Weekly build coverage/window; ensure it includes this qgp_week.'
      WHEN ABS(actual_value - expected_value) <= tolerance THEN 'No action required.'
      ELSE 'Verify weekly build is sourced only from Gold Daily and uses the same qgp_week bucketing.'
    END,
    CASE
      WHEN expected_value IS NULL THEN TRUE
      WHEN actual_value   IS NULL THEN TRUE
      WHEN ABS(actual_value - expected_value) > tolerance THEN TRUE
      ELSE FALSE
    END,
    CASE
      WHEN expected_value IS NOT NULL
       AND actual_value IS NOT NULL
       AND ABS(actual_value - expected_value) <= tolerance THEN TRUE
      ELSE FALSE
    END,
    CASE
      WHEN expected_value IS NULL THEN TRUE
      WHEN actual_value   IS NULL THEN TRUE
      WHEN ABS(actual_value - expected_value) > tolerance THEN TRUE
      ELSE FALSE
    END
  FROM aligned;

END;