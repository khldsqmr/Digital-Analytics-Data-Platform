/*
===============================================================================
FILE: 03_sp_gold_campaign_weekly_critical.sql
LAYER: Gold QA (Critical)
PURPOSE:
  Critical tests for Gold Weekly table.

TABLE:
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi-gold-sa360-campaign-weekly

GRAIN:
  account_id + campaign_id + qgp_week

TESTS:
  1) Null identifier check (HIGH)
  2) Duplicate grain check (HIGH)
  3) Partition freshness sanity vs Gold Daily (MEDIUM)
===============================================================================
*/

CREATE OR REPLACE PROCEDURE
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_gold_campaign_weekly_critical`()
OPTIONS(strict_mode=false)
BEGIN

  DECLARE v_table_name STRING DEFAULT 'sdi-gold-sa360-campaign-weekly';
  DECLARE v_now TIMESTAMP DEFAULT CURRENT_TIMESTAMP();

  DECLARE v_lookback_days INT64 DEFAULT 60;
  DECLARE v_window_start DATE DEFAULT DATE_SUB(CURRENT_DATE(), INTERVAL v_lookback_days DAY);

  -- 1) Null Identifier Check (HIGH)
  INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_test_results`
  WITH calc AS (
    SELECT
      COUNTIF(account_id IS NULL OR campaign_id IS NULL OR qgp_week IS NULL) AS null_id_cnt
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi-gold-sa360-campaign-weekly`
    WHERE qgp_week >= v_window_start
  )
  SELECT
    v_now,
    CURRENT_DATE(),
    v_table_name,
    'critical',
    'Null Identifier Check (60-day)',
    'HIGH',
    0.0,
    CAST(null_id_cnt AS FLOAT64),
    CAST(null_id_cnt AS FLOAT64) - 0.0,
    IF(null_id_cnt = 0, 'PASS', 'FAIL'),
    IF(null_id_cnt = 0, '🟢', '🔴'),
    IF(null_id_cnt = 0, 'All identifiers valid.', 'Found NULLs in grain identifiers (account_id/campaign_id/qgp_week).'),
    IF(null_id_cnt = 0, 'No action required.', 'Fix upstream weekly build; enforce NOT NULL constraints before MERGE.'),
    IF(null_id_cnt = 0, FALSE, TRUE),
    IF(null_id_cnt = 0, TRUE, FALSE),
    IF(null_id_cnt = 0, FALSE, TRUE)
  FROM calc;

  -- 2) Duplicate Grain Check (HIGH)
  INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_test_results`
  WITH dups AS (
    SELECT
      COUNT(*) AS dup_cnt
    FROM (
      SELECT
        account_id, campaign_id, qgp_week,
        COUNT(*) AS c
      FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi-gold-sa360-campaign-weekly`
      WHERE qgp_week >= v_window_start
      GROUP BY 1,2,3
      HAVING COUNT(*) > 1
    )
  )
  SELECT
    v_now,
    CURRENT_DATE(),
    v_table_name,
    'critical',
    'Duplicate Grain Check (60-day)',
    'HIGH',
    0.0,
    CAST(dup_cnt AS FLOAT64),
    CAST(dup_cnt AS FLOAT64) - 0.0,
    IF(dup_cnt = 0, 'PASS', 'FAIL'),
    IF(dup_cnt = 0, '🟢', '🔴'),
    IF(dup_cnt = 0, 'No duplicate grain detected.', 'Duplicate rows detected at (account_id, campaign_id, qgp_week).'),
    IF(dup_cnt = 0, 'No action required.', 'Review weekly MERGE key logic; ensure 1 row per grain in source rollup.'),
    IF(dup_cnt = 0, FALSE, TRUE),
    IF(dup_cnt = 0, TRUE, FALSE),
    IF(dup_cnt = 0, FALSE, TRUE)
  FROM dups;

  -- 3) Partition Freshness vs Gold Daily (MEDIUM)
  --    Compare max qgp_week (weekly) to max qgp_week derived from daily.
  INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_test_results`
  WITH daily_max AS (
    SELECT
      MAX(
        CASE
          -- Compute qgp_week end date for each daily date using your bucketing rules:
          WHEN date > last_saturday_before_qe AND date <= quarter_end_date THEN quarter_end_date
          ELSE week_end_date
        END
      ) AS max_daily_qgp_week
    FROM (
      SELECT
        d.date,
        DATE_ADD(DATE_TRUNC(d.date, WEEK(SUNDAY)), INTERVAL 6 DAY) AS week_end_date,
        DATE_SUB(DATE_ADD(DATE_TRUNC(d.date, QUARTER), INTERVAL 3 MONTH), INTERVAL 1 DAY) AS quarter_end_date,
        DATE_SUB(
          DATE_SUB(DATE_ADD(DATE_TRUNC(d.date, QUARTER), INTERVAL 3 MONTH), INTERVAL 1 DAY),
          INTERVAL MOD(EXTRACT(DAYOFWEEK FROM DATE_SUB(DATE_ADD(DATE_TRUNC(d.date, QUARTER), INTERVAL 3 MONTH), INTERVAL 1 DAY)), 7) DAY
        ) AS last_saturday_before_qe
      FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi-gold-sa360-campaign-daily` d
    )
  ),
  weekly_max AS (
    SELECT MAX(qgp_week) AS max_weekly_qgp_week
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi-gold-sa360-campaign-weekly`
  ),
  calc AS (
    SELECT
      DATE_DIFF(d.max_daily_qgp_week, w.max_weekly_qgp_week, DAY) AS lag_days
    FROM daily_max d CROSS JOIN weekly_max w
  )
  SELECT
    v_now,
    CURRENT_DATE(),
    v_table_name,
    'critical',
    'Partition Freshness vs Gold Daily (QGP week)',
    'MEDIUM',
    2.0,
    CAST(lag_days AS FLOAT64),
    CAST(lag_days AS FLOAT64) - 2.0,
    IF(lag_days <= 2, 'PASS', 'FAIL'),
    IF(lag_days <= 2, '🟢', '🔴'),
    IF(lag_days <= 2,
      CONCAT('Freshness OK. Weekly lag vs derived Daily QGP week = ', CAST(lag_days AS STRING), ' day(s).'),
      CONCAT('Weekly is stale. Lag vs derived Daily QGP week = ', CAST(lag_days AS STRING), ' day(s).')
    ),
    IF(lag_days <= 2, 'No action required.', 'Run weekly MERGE; verify lookback covers quarter-end partial buckets.'),
    FALSE,
    IF(lag_days <= 2, TRUE, FALSE),
    IF(lag_days <= 2, FALSE, TRUE)
  FROM calc;

END;
