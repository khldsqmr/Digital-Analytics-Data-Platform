/*
===============================================================================
FILE: 04_sp_gold_campaign_weekly_reconciliation.sql
LAYER: Gold | QA
PROC:  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_gold_sa360_campaign_weekly_reconciliation_tests

WHAT THIS TEST DOES (REAL TESTS, apples-to-apples):
  1) Builds the list of most recent N weekend_dates from Gold Daily (driver).
  2) Aggregates Gold Daily -> weekly totals by weekend_date.
  3) Aggregates Gold Weekly -> weekly totals by weekend_date.
  4) Compares per weekend_date (so no giant variance blobs, and no hiding issues).

NOTES:
  - Uses QUALIFY instead of LIMIT variable (BigQuery scripting limitation).
  - Each INSERT is a single statement: "INSERT INTO ... WITH ... SELECT ..."
===============================================================================
*/

CREATE OR REPLACE PROCEDURE
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_gold_sa360_campaign_weekly_reconciliation_tests`()
OPTIONS(strict_mode=false)
BEGIN
  DECLARE sample_weeks INT64 DEFAULT 4;       -- compare last 4 real weeks
  DECLARE tolerance    FLOAT64 DEFAULT 0.000001;

  -- ---------------------------------------------------------------------------
  -- TEST 1: cart_start weekly == SUM(daily) (per weekend_date)
  -- ---------------------------------------------------------------------------
  INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_test_results`
  WITH week_list AS (
    SELECT weekend_date
    FROM (
      SELECT DISTINCT DATE_TRUNC(date, WEEK(SATURDAY)) AS weekend_date
      FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_campaign_daily`
      WHERE date IS NOT NULL
    )
    QUALIFY ROW_NUMBER() OVER (ORDER BY weekend_date DESC) <= sample_weeks
  ),
  daily_rollup AS (
    SELECT
      DATE_TRUNC(date, WEEK(SATURDAY)) AS weekend_date,
      SUM(cart_start) AS daily_val
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_campaign_daily`
    WHERE date IS NOT NULL
    GROUP BY 1
  ),
  weekly_rollup AS (
    SELECT
      weekend_date,
      SUM(cart_start) AS weekly_val
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_campaign_weekly`
    WHERE weekend_date IS NOT NULL
    GROUP BY 1
  ),
  aligned AS (
    SELECT
      l.weekend_date,
      d.daily_val  AS expected_value,
      w.weekly_val AS actual_value
    FROM week_list l
    LEFT JOIN daily_rollup d USING (weekend_date)
    LEFT JOIN weekly_rollup w USING (weekend_date)
  )
  SELECT
    CURRENT_TIMESTAMP() AS test_run_timestamp,
    CURRENT_DATE()      AS test_date,
    'sdi_gold_sa360_campaign_weekly' AS table_name,
    'reconciliation'    AS test_layer,
    CONCAT('Cart Start Weekly == SUM(Daily) | weekend=', CAST(weekend_date AS STRING)) AS test_name,
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
      WHEN expected_value IS NULL THEN 'Daily rollup missing for this weekend_date (unexpected).'
      WHEN actual_value   IS NULL THEN 'Weekly rollup missing for this weekend_date (weekly not built / filtered).'
      WHEN ABS(actual_value - expected_value) <= tolerance THEN 'Weekly equals SUM(Daily) for this weekend_date.'
      ELSE 'Weekly does NOT equal SUM(Daily) for this weekend_date.'
    END AS failure_reason,
    CASE
      WHEN expected_value IS NULL THEN 'Validate Gold Daily availability and weekend_date derivation.'
      WHEN actual_value   IS NULL THEN 'Validate Gold Weekly build coverage/window; ensure it includes this weekend_date.'
      WHEN ABS(actual_value - expected_value) <= tolerance THEN 'No action required.'
      ELSE 'Verify weekly build is sourced only from Gold Daily and uses WEEK(SATURDAY) consistently.'
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
  -- TEST 2: postpaid_pspv weekly == SUM(daily) (per weekend_date)
  -- ---------------------------------------------------------------------------
  INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_test_results`
  WITH week_list AS (
    SELECT weekend_date
    FROM (
      SELECT DISTINCT DATE_TRUNC(date, WEEK(SATURDAY)) AS weekend_date
      FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_campaign_daily`
      WHERE date IS NOT NULL
    )
    QUALIFY ROW_NUMBER() OVER (ORDER BY weekend_date DESC) <= sample_weeks
  ),
  daily_rollup AS (
    SELECT
      DATE_TRUNC(date, WEEK(SATURDAY)) AS weekend_date,
      SUM(postpaid_pspv) AS daily_val
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_campaign_daily`
    WHERE date IS NOT NULL
    GROUP BY 1
  ),
  weekly_rollup AS (
    SELECT
      weekend_date,
      SUM(postpaid_pspv) AS weekly_val
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_campaign_weekly`
    WHERE weekend_date IS NOT NULL
    GROUP BY 1
  ),
  aligned AS (
    SELECT
      l.weekend_date,
      d.daily_val  AS expected_value,
      w.weekly_val AS actual_value
    FROM week_list l
    LEFT JOIN daily_rollup d USING (weekend_date)
    LEFT JOIN weekly_rollup w USING (weekend_date)
  )
  SELECT
    CURRENT_TIMESTAMP(),
    CURRENT_DATE(),
    'sdi_gold_sa360_campaign_weekly',
    'reconciliation',
    CONCAT('Postpaid PSPV Weekly == SUM(Daily) | weekend=', CAST(weekend_date AS STRING)),
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
      WHEN expected_value IS NULL THEN 'Daily rollup missing for this weekend_date (unexpected).'
      WHEN actual_value   IS NULL THEN 'Weekly rollup missing for this weekend_date (weekly not built / filtered).'
      WHEN ABS(actual_value - expected_value) <= tolerance THEN 'Weekly equals SUM(Daily) for this weekend_date.'
      ELSE 'Weekly does NOT equal SUM(Daily) for this weekend_date.'
    END,
    CASE
      WHEN expected_value IS NULL THEN 'Validate Gold Daily availability and weekend_date derivation.'
      WHEN actual_value   IS NULL THEN 'Validate Gold Weekly build coverage/window; ensure it includes this weekend_date.'
      WHEN ABS(actual_value - expected_value) <= tolerance THEN 'No action required.'
      ELSE 'Verify weekly build is sourced only from Gold Daily and uses WEEK(SATURDAY) consistently.'
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
