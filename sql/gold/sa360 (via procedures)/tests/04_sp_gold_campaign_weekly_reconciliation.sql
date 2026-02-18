/*
===============================================================================
FILE: 04_sp_gold_campaign_weekly_reconciliation.sql
LAYER: Gold | QA
PROC:  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_gold_sa360_campaign_weekly_reconciliation_tests

Key points:
  - Uses last N complete weekend_dates driven by Daily (so the weeks are real)
  - Compares per weekend_date so you get pinpoint failures
  - No COALESCE “hiding” — if a side is missing you’ll see it because the join fails and expected/actual becomes NULL (that’s a legit FAIL)
===============================================================================
*/
CREATE OR REPLACE PROCEDURE
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_gold_sa360_campaign_weekly_reconciliation_tests`()
OPTIONS(strict_mode=false)
BEGIN
  DECLARE sample_weeks INT64 DEFAULT 4;   -- keep it simple: 4 weeks
  DECLARE tolerance FLOAT64 DEFAULT 0.000001;

  -- Pick the most recent COMPLETE weekend_dates based on Daily
  -- "Complete" here means weekend_date <= current week's weekend (Saturday) and is present in Daily.
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
      SUM(COALESCE(cart_start,0))     AS cart_start_daily_sum,
      SUM(COALESCE(postpaid_pspv,0))  AS postpaid_pspv_daily_sum
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_campaign_daily`
    GROUP BY 1
  ),

  weekly_rollup AS (
    SELECT
      weekend_date,
      SUM(COALESCE(cart_start,0))     AS cart_start_weekly_sum,
      SUM(COALESCE(postpaid_pspv,0))  AS postpaid_pspv_weekly_sum
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_campaign_weekly`
    GROUP BY 1
  ),

  aligned AS (
    SELECT
      l.weekend_date,
      d.cart_start_daily_sum,
      w.cart_start_weekly_sum,
      d.postpaid_pspv_daily_sum,
      w.postpaid_pspv_weekly_sum
    FROM daily_week_list l
    LEFT JOIN daily_rollup d USING (weekend_date)
    LEFT JOIN weekly_rollup w USING (weekend_date)
  )

  -- ---------------------------------------------------------------------------
  -- TEST 1: cart_start weekly == sum(daily) (per weekend)
  -- ---------------------------------------------------------------------------
  INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_test_results`
  SELECT
    CURRENT_TIMESTAMP(), CURRENT_DATE(),
    'sdi_gold_sa360_campaign_weekly',
    'reconciliation',
    CONCAT('Cart Start Weekly == SUM(Daily) | weekend=', CAST(weekend_date AS STRING)) AS test_name,
    'HIGH',
    CAST(cart_start_daily_sum AS FLOAT64) AS expected_value,
    CAST(cart_start_weekly_sum AS FLOAT64) AS actual_value,
    CAST(cart_start_weekly_sum - cart_start_daily_sum AS FLOAT64) AS variance_value,
    IF(
      cart_start_daily_sum IS NOT NULL
      AND cart_start_weekly_sum IS NOT NULL
      AND ABS(cart_start_weekly_sum - cart_start_daily_sum) <= tolerance,
      'PASS','FAIL'
    ) AS status,
    IF(
      cart_start_daily_sum IS NOT NULL
      AND cart_start_weekly_sum IS NOT NULL
      AND ABS(cart_start_weekly_sum - cart_start_daily_sum) <= tolerance,
      '🟢','🔴'
    ) AS status_emoji,
    CASE
      WHEN cart_start_daily_sum IS NULL THEN 'Daily rollup missing for this weekend_date (unexpected).'
      WHEN cart_start_weekly_sum IS NULL THEN 'Weekly rollup missing for this weekend_date (weekly not built or filtered out).'
      WHEN ABS(cart_start_weekly_sum - cart_start_daily_sum) <= tolerance THEN 'Weekly equals SUM(Daily) for this weekend_date.'
      ELSE 'Weekly does NOT equal SUM(Daily) for this weekend_date.'
    END AS failure_reason,
    CASE
      WHEN cart_start_weekly_sum IS NULL THEN 'Build weekly for this weekend_date and validate weekend_date logic.'
      WHEN cart_start_daily_sum IS NULL THEN 'Validate daily availability and weekend_date derivation.'
      WHEN ABS(cart_start_weekly_sum - cart_start_daily_sum) <= tolerance THEN 'No action required.'
      ELSE 'Verify weekly build is sourced only from Gold Daily and uses WEEK(SATURDAY) consistently.'
    END AS next_step,
    IF(
      cart_start_daily_sum IS NULL
      OR cart_start_weekly_sum IS NULL
      OR ABS(cart_start_weekly_sum - cart_start_daily_sum) > tolerance,
      TRUE, FALSE
    ) AS is_critical_failure,
    IF(
      cart_start_daily_sum IS NOT NULL
      AND cart_start_weekly_sum IS NOT NULL
      AND ABS(cart_start_weekly_sum - cart_start_daily_sum) <= tolerance,
      TRUE, FALSE
    ) AS is_pass,
    IF(
      cart_start_daily_sum IS NULL
      OR cart_start_weekly_sum IS NULL
      OR ABS(cart_start_weekly_sum - cart_start_daily_sum) > tolerance,
      TRUE, FALSE
    ) AS is_fail
  FROM aligned;

  -- ---------------------------------------------------------------------------
  -- TEST 2: postpaid_pspv weekly == sum(daily) (per weekend)
  -- ---------------------------------------------------------------------------
  INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_test_results`
  SELECT
    CURRENT_TIMESTAMP(), CURRENT_DATE(),
    'sdi_gold_sa360_campaign_weekly',
    'reconciliation',
    CONCAT('Postpaid PSPV Weekly == SUM(Daily) | weekend=', CAST(weekend_date AS STRING)) AS test_name,
    'HIGH',
    CAST(postpaid_pspv_daily_sum AS FLOAT64) AS expected_value,
    CAST(postpaid_pspv_weekly_sum AS FLOAT64) AS actual_value,
    CAST(postpaid_pspv_weekly_sum - postpaid_pspv_daily_sum AS FLOAT64) AS variance_value,
    IF(
      postpaid_pspv_daily_sum IS NOT NULL
      AND postpaid_pspv_weekly_sum IS NOT NULL
      AND ABS(postpaid_pspv_weekly_sum - postpaid_pspv_daily_sum) <= tolerance,
      'PASS','FAIL'
    ) AS status,
    IF(
      postpaid_pspv_daily_sum IS NOT NULL
      AND postpaid_pspv_weekly_sum IS NOT NULL
      AND ABS(postpaid_pspv_weekly_sum - postpaid_pspv_daily_sum) <= tolerance,
      '🟢','🔴'
    ) AS status_emoji,
    CASE
      WHEN postpaid_pspv_daily_sum IS NULL THEN 'Daily rollup missing for this weekend_date (unexpected).'
      WHEN postpaid_pspv_weekly_sum IS NULL THEN 'Weekly rollup missing for this weekend_date (weekly not built or filtered out).'
      WHEN ABS(postpaid_pspv_weekly_sum - postpaid_pspv_daily_sum) <= tolerance THEN 'Weekly equals SUM(Daily) for this weekend_date.'
      ELSE 'Weekly does NOT equal SUM(Daily) for this weekend_date.'
    END AS failure_reason,
    CASE
      WHEN postpaid_pspv_weekly_sum IS NULL THEN 'Build weekly for this weekend_date and validate weekend_date logic.'
      WHEN postpaid_pspv_daily_sum IS NULL THEN 'Validate daily availability and weekend_date derivation.'
      WHEN ABS(postpaid_pspv_weekly_sum - postpaid_pspv_daily_sum) <= tolerance THEN 'No action required.'
      ELSE 'Verify weekly build is sourced only from Gold Daily and uses WEEK(SATURDAY) consistently.'
    END AS next_step,
    IF(
      postpaid_pspv_daily_sum IS NULL
      OR postpaid_pspv_weekly_sum IS NULL
      OR ABS(postpaid_pspv_weekly_sum - postpaid_pspv_daily_sum) > tolerance,
      TRUE, FALSE
    ) AS is_critical_failure,
    IF(
      postpaid_pspv_daily_sum IS NOT NULL
      AND postpaid_pspv_weekly_sum IS NOT NULL
      AND ABS(postpaid_pspv_weekly_sum - postpaid_pspv_daily_sum) <= tolerance,
      TRUE, FALSE
    ) AS is_pass,
    IF(
      postpaid_pspv_daily_sum IS NULL
      OR postpaid_pspv_weekly_sum IS NULL
      OR ABS(postpaid_pspv_weekly_sum - postpaid_pspv_daily_sum) > tolerance,
      TRUE, FALSE
    ) AS is_fail
  FROM aligned;

END;
