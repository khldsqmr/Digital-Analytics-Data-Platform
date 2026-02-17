/*
===============================================================================
FILE: 01_sp_gold_campaign_daily_critical.sql
LAYER: Gold QA (Critical)
PURPOSE:
  Critical (blocking) tests for Gold Daily table:
    1) Null identifier check
    2) Duplicate grain check
    3) Partition freshness sanity (recent coverage)

TABLE:
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi-gold-sa360-campaign-daily

GRAIN:
  account_id + campaign_id + date
===============================================================================
*/

CREATE OR REPLACE PROCEDURE
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_gold_campaign_daily_critical`()
OPTIONS(strict_mode=false)
BEGIN

  DECLARE v_table_name STRING DEFAULT 'sdi-gold-sa360-campaign-daily';
  DECLARE v_now TIMESTAMP DEFAULT CURRENT_TIMESTAMP();

  DECLARE v_lookback_days INT64 DEFAULT 7;
  DECLARE v_window_start DATE DEFAULT DATE_SUB(CURRENT_DATE(), INTERVAL v_lookback_days DAY);

  -- 1) Null Identifier Check (HIGH)
  INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_test_results`
  WITH calc AS (
    SELECT
      COUNTIF(account_id IS NULL OR campaign_id IS NULL OR date IS NULL) AS null_id_cnt
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi-gold-sa360-campaign-daily`
    WHERE date >= v_window_start
  )
  SELECT
    v_now AS test_run_timestamp,
    CURRENT_DATE() AS test_date,
    v_table_name AS table_name,
    'critical' AS test_layer,
    'Null Identifier Check (7-day)' AS test_name,
    'HIGH' AS severity_level,
    0.0 AS expected_value,
    CAST(null_id_cnt AS FLOAT64) AS actual_value,
    CAST(null_id_cnt AS FLOAT64) - 0.0 AS variance_value,
    IF(null_id_cnt = 0, 'PASS', 'FAIL') AS status,
    IF(null_id_cnt = 0, '🟢', '🔴') AS status_emoji,
    IF(null_id_cnt = 0, 'All identifiers valid.', 'Found NULLs in grain identifiers (account_id/campaign_id/date).') AS failure_reason,
    IF(null_id_cnt = 0, 'No action required.', 'Fix upstream ETL or enforce NOT NULL logic before MERGE into Gold.') AS next_step,
    IF(null_id_cnt = 0, FALSE, TRUE) AS is_critical_failure,
    IF(null_id_cnt = 0, TRUE, FALSE) AS is_pass,
    IF(null_id_cnt = 0, FALSE, TRUE) AS is_fail
  FROM calc;

  -- 2) Duplicate Grain Check (HIGH)
  INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_test_results`
  WITH dups AS (
    SELECT
      COUNT(*) AS dup_cnt
    FROM (
      SELECT
        account_id, campaign_id, date,
        COUNT(*) AS c
      FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi-gold-sa360-campaign-daily`
      WHERE date >= v_window_start
      GROUP BY 1,2,3
      HAVING COUNT(*) > 1
    )
  )
  SELECT
    v_now,
    CURRENT_DATE(),
    v_table_name,
    'critical',
    'Duplicate Grain Check (7-day)',
    'HIGH',
    0.0,
    CAST(dup_cnt AS FLOAT64),
    CAST(dup_cnt AS FLOAT64) - 0.0,
    IF(dup_cnt = 0, 'PASS', 'FAIL'),
    IF(dup_cnt = 0, '🟢', '🔴'),
    IF(dup_cnt = 0, 'No duplicate grain detected.', 'Duplicate rows detected at (account_id, campaign_id, date).'),
    IF(dup_cnt = 0, 'No action required.', 'Review MERGE key logic; ensure source query is 1 row per grain.'),
    IF(dup_cnt = 0, FALSE, TRUE),
    IF(dup_cnt = 0, TRUE, FALSE),
    IF(dup_cnt = 0, FALSE, TRUE)
  FROM dups;

  -- 3) Partition Freshness (MEDIUM)
  --   "Lag days" = difference between max(date) in Silver vs max(date) in Gold (lookback-free).
  --   This is NOT blocking unless you want it to be.
  INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_test_results`
  WITH m AS (
    SELECT
      (SELECT MAX(date) FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_silver_sa360_campaign_daily`) AS max_silver_date,
      (SELECT MAX(date) FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi-gold-sa360-campaign-daily`) AS max_gold_date
  ),
  calc AS (
    SELECT
      DATE_DIFF(max_silver_date, max_gold_date, DAY) AS lag_days
    FROM m
  )
  SELECT
    v_now,
    CURRENT_DATE(),
    v_table_name,
    'critical',
    'Partition Freshness vs Silver',
    'MEDIUM',
    2.0 AS expected_value, -- "acceptable lag" threshold displayed only
    CAST(lag_days AS FLOAT64) AS actual_value,
    CAST(lag_days AS FLOAT64) - 2.0 AS variance_value,
    IF(lag_days <= 2, 'PASS', 'FAIL') AS status,
    IF(lag_days <= 2, '🟢', '🔴') AS status_emoji,
    IF(lag_days <= 2,
      CONCAT('Freshness OK. Gold lag vs Silver = ', CAST(lag_days AS STRING), ' day(s).'),
      CONCAT('Gold is stale. Gold lag vs Silver = ', CAST(lag_days AS STRING), ' day(s).')
    ) AS failure_reason,
    IF(lag_days <= 2, 'No action required.', 'Run Gold MERGE; verify lookback window; check upstream load delays.') AS next_step,
    FALSE AS is_critical_failure,
    IF(lag_days <= 2, TRUE, FALSE),
    IF(lag_days <= 2, FALSE, TRUE)
  FROM calc;

END;
