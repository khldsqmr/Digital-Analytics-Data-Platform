/*
===============================================================================
FILE: 04_sp_gold_campaign_weekly_reconciliation.sql
LAYER: Gold | QA
PROC:  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_gold_sa360_campaign_weekly_reconciliation_tests

REAL TESTS (no cover-ups):
  - Picks last N weekend_dates from Gold Daily (WEEK(SATURDAY))
  - Compares Weekly vs SUM(Daily) per weekend_date (pinpoint failures)
  - If either side missing => FAIL with clear reason
===============================================================================
*/

CREATE OR REPLACE PROCEDURE
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_gold_sa360_campaign_weekly_reconciliation_tests`()
OPTIONS(strict_mode=false)
BEGIN
  DECLARE sample_weeks INT64 DEFAULT 4;        -- e.g., 4 recent weeks
  DECLARE tolerance   FLOAT64 DEFAULT 0.000001;

  -- ---------------------------------------------------------------------------
  -- Build an aligned temp table: last N weekend_dates driven by DAILY
  -- NOTE: No COALESCE in sums; SUM() naturally ignores NULLs.
  -- ---------------------------------------------------------------------------
  CREATE TEMP TABLE aligned AS
  WITH daily_week_list AS (
    SELECT weekend_date
    FROM (
      SELECT DISTINCT DATE_TRUNC(date, WEEK(SATURDAY)) AS weekend_date
      FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_campaign_daily`
      WHERE date IS NOT NULL
    )
    ORDER BY weekend_date DESC
    LIMIT sample_weeks
  ),
  daily_rollup AS (
    SELECT
      DATE_TRUNC(date, WEEK(SATURDAY)) AS weekend_date,
      SUM(cart_start)      AS cart_start_daily_sum,
      SUM(postpaid_pspv)   AS postpaid_pspv_daily_sum
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_campaign_daily`
    GROUP BY 1
  ),
  weekly_rollup AS (
    SELECT
      weekend_date,
      SUM(cart_start)      AS cart_start_weekly_sum,
      SUM(postpaid_pspv)   AS postpaid_pspv_weekly_sum
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_campaign_weekly`
    GROUP BY 1
  )
  SELECT
    l.weekend_date,
    d.cart_start_daily_sum,
    w.cart_start_weekly_sum,
    d.postpaid_pspv_daily_sum,
    w.postpaid_pspv_weekly_sum
  FROM daily_week_list l
  LEFT JOIN daily_rollup d USING (weekend_date)
  LEFT JOIN weekly_rollup w USING (weekend_date);

  -- ---------------------------------------------------------------------------
  -- TEST 1: cart_start weekly == SUM(daily) (per weekend_date)
  -- ---------------------------------------------------------------------------
  INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_test_results`
  SELECT
    CURRENT_TIMESTAMP() AS test_run_timestamp,
    CURRENT_DATE()      AS test_date,
    'sdi_gold_sa360_campaign_weekly' AS table_name,
    'reconciliation' AS test_layer,
    CONCAT('Cart Start Weekly == SUM(Daily) | weekend=', CAST(weekend_date AS STRING)) AS test_name,
    'HIGH' AS severity_level,
    CAST(cart_start_daily_sum AS FLOAT64)  AS expected_value,
    CAST(cart_start_weekly_sum AS FLOAT64) AS actual_value,
    CAST(cart_start_weekly_sum - cart_start_daily_sum AS FLOAT64) AS variance_value,
    CASE
      WHEN cart_start_daily_sum IS NULL THEN 'FAIL'
      WHEN cart_start_weekly_sum IS NULL THEN 'FAIL'
      WHEN ABS(cart_start_weekly_sum - cart_start_daily_sum) <= tolerance THEN 'PASS'
      ELSE 'FAIL'
    END AS status,
    CASE
      WHEN cart_start_daily_sum IS NULL THEN '🔴'
      WHEN cart_start_weekly_sum IS NULL THEN '🔴'
      WHEN ABS(cart_start_weekly_sum - cart_start_daily_sum) <= tolerance THEN '🟢'
      ELSE '🔴'
    END AS status_emoji,
    CASE
      WHEN cart_start_daily_sum IS NULL THEN 'Daily rollup missing for this weekend_date (unexpected).'
      WHEN cart_start_weekly_sum IS NULL THEN 'Weekly rollup missing for this weekend_date (weekly not built or filtered out).'
      WHEN ABS(cart_start_weekly_sum - cart_start_daily_sum) <= tolerance THEN 'Weekly equals SUM(Daily) for this weekend_date.'
      ELSE 'Weekly does NOT equal SUM(Daily) for this weekend_date.'
    END AS failure_reason,
    CASE
      WHEN cart_start_daily_sum IS NULL THEN 'Validate daily data and weekend_date derivation (WEEK(SATURDAY)).'
      WHEN cart_start_weekly_sum IS NULL THEN 'Build weekly for this weekend_date; ensure weekly sourced from Gold Daily.'
      WHEN ABS(cart_start_weekly_sum - cart_start_daily_sum) <= tolerance THEN 'No action required.'
      ELSE 'Verify weekly build uses DATE_TRUNC(date, WEEK(SATURDAY)) and no extra filtering vs daily.'
    END AS next_step,
    -- critical flags
    CASE
      WHEN cart_start_daily_sum IS NULL THEN TRUE
      WHEN cart_start_weekly_sum IS NULL THEN TRUE
      WHEN ABS(cart_start_weekly_sum - cart_start_daily_sum) > tolerance THEN TRUE
      ELSE FALSE
    END AS is_critical_failure,
    CASE
      WHEN cart_start_daily_sum IS NOT NULL
       AND cart_start_weekly_sum IS NOT NULL
       AND ABS(cart_start_weekly_sum - cart_start_daily_sum) <= tolerance THEN TRUE
      ELSE FALSE
    END AS is_pass,
    CASE
      WHEN cart_start_daily_sum IS NULL THEN TRUE
      WHEN cart_start_weekly_sum IS NULL THEN TRUE
      WHEN ABS(cart_start_weekly_sum - cart_start_daily_sum) > tolerance THEN TRUE
      ELSE FALSE
    END AS is_fail
  FROM aligned;

  -- ---------------------------------------------------------------------------
  -- TEST 2: postpaid_pspv weekly == SUM(daily) (per weekend_date)
  -- ---------------------------------------------------------------------------
  INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_test_results`
  SELECT
    CURRENT_TIMESTAMP() AS test_run_timestamp,
    CURRENT_DATE()      AS test_date,
    'sdi_gold_sa360_campaign_weekly' AS table_name,
    'reconciliation' AS test_layer,
    CONCAT('Postpaid PSPV Weekly == SUM(Daily) | weekend=', CAST(weekend_date AS STRING)) AS test_name,
    'HIGH' AS severity_level,
    CAST(postpaid_pspv_daily_sum AS FLOAT64)  AS expected_value,
    CAST(postpaid_pspv_weekly_sum AS FLOAT64) AS actual_value,
    CAST(postpaid_pspv_weekly_sum - postpaid_pspv_daily_sum AS FLOAT64) AS variance_value,
    CASE
      WHEN postpaid_pspv_daily_sum IS NULL THEN 'FAIL'
      WHEN postpaid_pspv_weekly_sum IS NULL THEN 'FAIL'
      WHEN ABS(postpaid_pspv_weekly_sum - postpaid_pspv_daily_sum) <= tolerance THEN 'PASS'
      ELSE 'FAIL'
    END AS status,
    CASE
      WHEN postpaid_pspv_daily_sum IS NULL THEN '🔴'
      WHEN postpaid_pspv_weekly_sum IS NULL THEN '🔴'
      WHEN ABS(postpaid_pspv_weekly_sum - postpaid_pspv_daily_sum) <= tolerance THEN '🟢'
      ELSE '🔴'
    END AS status_emoji,
    CASE
      WHEN postpaid_pspv_daily_sum IS NULL THEN 'Daily rollup missing for this weekend_date (unexpected).'
      WHEN postpaid_pspv_weekly_sum IS NULL THEN 'Weekly rollup missing for this weekend_date (weekly not built or filtered out).'
      WHEN ABS(postpaid_pspv_weekly_sum - postpaid_pspv_daily_sum) <= tolerance THEN 'Weekly equals SUM(Daily) for this weekend_date.'
      ELSE 'Weekly does NOT equal SUM(Daily) for this weekend_date.'
    END AS failure_reason,
    CASE
      WHEN postpaid_pspv_daily_sum IS NULL THEN 'Validate daily data and weekend_date derivation (WEEK(SATURDAY)).'
      WHEN postpaid_pspv_weekly_sum IS NULL THEN 'Build weekly for this weekend_date; ensure weekly sourced from Gold Daily.'
      WHEN ABS(postpaid_pspv_weekly_sum - postpaid_pspv_daily_sum) <= tolerance THEN 'No action required.'
      ELSE 'Verify weekly build uses DATE_TRUNC(date, WEEK(SATURDAY)) and no extra filtering vs daily.'
    END AS next_step,
    -- critical flags
    CASE
      WHEN postpaid_pspv_daily_sum IS NULL THEN TRUE
      WHEN postpaid_pspv_weekly_sum IS NULL THEN TRUE
      WHEN ABS(postpaid_pspv_weekly_sum - postpaid_pspv_daily_sum) > tolerance THEN TRUE
      ELSE FALSE
    END AS is_critical_failure,
    CASE
      WHEN postpaid_pspv_daily_sum IS NOT NULL
       AND postpaid_pspv_weekly_sum IS NOT NULL
       AND ABS(postpaid_pspv_weekly_sum - postpaid_pspv_daily_sum) <= tolerance THEN TRUE
      ELSE FALSE
    END AS is_pass,
    CASE
      WHEN postpaid_pspv_daily_sum IS NULL THEN TRUE
      WHEN postpaid_pspv_weekly_sum IS NULL THEN TRUE
      WHEN ABS(postpaid_pspv_weekly_sum - postpaid_pspv_daily_sum) > tolerance THEN TRUE
      ELSE FALSE
    END AS is_fail
  FROM aligned;

END;
