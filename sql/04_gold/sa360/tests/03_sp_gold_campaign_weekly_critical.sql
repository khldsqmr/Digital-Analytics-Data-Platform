/*
===============================================================================
FILE: 03_sp_gold_campaign_weekly_critical.sql
LAYER: Gold QA (Critical)

PURPOSE:
  Critical (blocking) tests for Gold Weekly.

TABLE:
  Gold Weekly: prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_campaign_weekly

WINDOW:
  last N days (default 90, based on qgp_week)

TESTS (HIGH):
  1) Null Identifier Check
  2) Duplicate Grain Check (account_id, campaign_id, qgp_week)
  3) Freshness vs Gold Daily (bucket-aware)
===============================================================================
*/

CREATE OR REPLACE PROCEDURE
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_gold_campaign_weekly_critical`()
OPTIONS(strict_mode=false)
BEGIN

  DECLARE v_table STRING DEFAULT 'sdi_gold_sa360_campaign_weekly';
  DECLARE v_now TIMESTAMP DEFAULT CURRENT_TIMESTAMP();

  DECLARE v_window_days INT64 DEFAULT 90;
  DECLARE v_start DATE DEFAULT DATE_SUB(CURRENT_DATE(), INTERVAL v_window_days DAY);

  -- ---------------------------------------------------------------------------
  -- TEST 1: Null Identifier Check (HIGH)
  -- ---------------------------------------------------------------------------
  INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_test_results`
  WITH bad AS (
    SELECT COUNT(*) AS bad_cnt
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_campaign_weekly`
    WHERE qgp_week >= v_start
      AND (
        account_id IS NULL OR account_id = ''
        OR campaign_id IS NULL OR campaign_id = ''
        OR qgp_week IS NULL
      )
  )
  SELECT
    v_now, CURRENT_DATE(), v_table,
    'critical',
    'Null Identifier Check (90-day)',
    'HIGH',
    0.0,
    CAST(bad_cnt AS FLOAT64),
    CAST(bad_cnt AS FLOAT64),
    IF(bad_cnt = 0, 'PASS', 'FAIL'),
    IF(bad_cnt = 0, '🟢', '🔴'),
    IF(bad_cnt = 0,
      'All identifiers valid.',
      CONCAT('Found ', CAST(bad_cnt AS STRING), ' rows with null/blank identifiers.')
    ),
    IF(bad_cnt = 0,
      'No action required.',
      'Investigate weekly build/merge logic; ensure qgp_week computed.'
    ),
    IF(bad_cnt = 0, FALSE, TRUE),
    IF(bad_cnt = 0, TRUE, FALSE),
    IF(bad_cnt = 0, FALSE, TRUE)
  FROM bad;

  -- ---------------------------------------------------------------------------
  -- TEST 2: Duplicate Grain Check (HIGH)
  -- ---------------------------------------------------------------------------
  INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_test_results`
  WITH dups AS (
    SELECT COUNT(*) AS dup_cnt
    FROM (
      SELECT account_id, campaign_id, qgp_week
      FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_campaign_weekly`
      WHERE qgp_week >= v_start
      GROUP BY account_id, campaign_id, qgp_week
      HAVING COUNT(*) > 1
    )
  )
  SELECT
    v_now, CURRENT_DATE(), v_table,
    'critical',
    'Duplicate Grain Check (90-day)',
    'HIGH',
    0.0,
    CAST(dup_cnt AS FLOAT64),
    CAST(dup_cnt AS FLOAT64),
    IF(dup_cnt = 0, 'PASS', 'FAIL'),
    IF(dup_cnt = 0, '🟢', '🔴'),
    IF(dup_cnt = 0,
      'No duplicate grain detected.',
      CONCAT('Duplicate weekly grain keys found: ', CAST(dup_cnt AS STRING), '.')
    ),
    IF(dup_cnt = 0,
      'No action required.',
      'Check weekly MERGE key (account_id,campaign_id,qgp_week) and sources.'
    ),
    IF(dup_cnt = 0, FALSE, TRUE),
    IF(dup_cnt = 0, TRUE, FALSE),
    IF(dup_cnt = 0, FALSE, TRUE)
  FROM dups;

  -- ---------------------------------------------------------------------------
  -- TEST 3: Freshness vs Gold Daily (HIGH, bucket-aware)
  --   We compute the expected qgp_week for the max daily date and ensure
  --   weekly has at least that bucket (or later).
  -- ---------------------------------------------------------------------------
  INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_test_results`
  WITH daily_max AS (
    SELECT MAX(date) AS max_daily_date
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_campaign_daily`
  ),
  daily_bucket AS (
    SELECT
      -- Saturday end of week (Sun->Sat)
      DATE_ADD(DATE_TRUNC(max_daily_date, WEEK(SUNDAY)), INTERVAL 6 DAY) AS week_end_saturday,
      DATE_SUB(DATE_ADD(DATE_TRUNC(max_daily_date, QUARTER), INTERVAL 3 MONTH), INTERVAL 1 DAY) AS quarter_end_date,
      DATE_SUB(
        DATE_SUB(DATE_ADD(DATE_TRUNC(max_daily_date, QUARTER), INTERVAL 3 MONTH), INTERVAL 1 DAY),
        INTERVAL MOD(EXTRACT(DAYOFWEEK FROM DATE_SUB(DATE_ADD(DATE_TRUNC(max_daily_date, QUARTER), INTERVAL 3 MONTH), INTERVAL 1 DAY)), 7) DAY
      ) AS last_saturday_before_qe,
      max_daily_date
    FROM daily_max
  ),
  expected AS (
    SELECT
      CASE
        WHEN max_daily_date > last_saturday_before_qe AND max_daily_date <= quarter_end_date
        THEN quarter_end_date
        ELSE week_end_saturday
      END AS expected_min_week_bucket
    FROM daily_bucket
  ),
  weekly_max AS (
    SELECT MAX(qgp_week) AS max_weekly_bucket
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_campaign_weekly`
  ),
  calc AS (
    SELECT
      DATE_DIFF((SELECT expected_min_week_bucket FROM expected),
                (SELECT max_weekly_bucket FROM weekly_max),
                DAY) AS lag_days
    -- Note: lag_days <= 7 means weekly covers the expected bucket (or later).
  )
  SELECT
    v_now, CURRENT_DATE(), v_table,
    'critical',
    'Freshness vs Gold Daily (bucket-aware lag days)',
    'HIGH',
    7.0,
    CAST(lag_days AS FLOAT64),
    CAST(lag_days - 7 AS FLOAT64),
    IF(lag_days <= 7, 'PASS', 'FAIL'),
    IF(lag_days <= 7, '🟢', '🔴'),
    IF(lag_days <= 7,
      CONCAT('Freshness OK. Weekly coverage relative to expected daily bucket = ', CAST(lag_days AS STRING), ' day(s).'),
      CONCAT('Weekly may be stale. Coverage lag = ', CAST(lag_days AS STRING), ' day(s).')
    ),
    IF(lag_days <= 7,
      'No action required.',
      'Run weekly MERGE with adequate lookback; check quarter-end bucketing.'
    ),
    IF(lag_days <= 7, FALSE, TRUE),
    IF(lag_days <= 7, TRUE, FALSE),
    IF(lag_days <= 7, FALSE, TRUE)
  FROM calc;

END;
