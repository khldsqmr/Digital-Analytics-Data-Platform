/*
===============================================================================
FILE: 01_sp_gold_campaign_daily_critical.sql
LAYER: Gold QA (Critical)

PURPOSE:
  Critical (blocking) tests for Gold Daily.

TABLE:
  Gold Daily: prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_campaign_daily

WINDOW:
  last N days (default 30)

TESTS (HIGH):
  1) Null Identifier Check
  2) Duplicate Grain Check (account_id, campaign_id, date)
  3) Partition Freshness vs Silver (lag in days)
===============================================================================
*/

CREATE OR REPLACE PROCEDURE
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_gold_campaign_daily_critical`()
OPTIONS(strict_mode=false)
BEGIN

  DECLARE v_table STRING DEFAULT 'sdi_gold_sa360_campaign_daily';
  DECLARE v_now TIMESTAMP DEFAULT CURRENT_TIMESTAMP();

  DECLARE v_window_days INT64 DEFAULT 30;
  DECLARE v_start DATE DEFAULT DATE_SUB(CURRENT_DATE(), INTERVAL v_window_days DAY);

  -- ---------------------------------------------------------------------------
  -- TEST 1: Null Identifier Check (HIGH)
  -- ---------------------------------------------------------------------------
  INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_test_results`
  WITH bad AS (
    SELECT COUNT(*) AS bad_cnt
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_campaign_daily`
    WHERE date >= v_start
      AND (
        account_id IS NULL OR account_id = ''
        OR campaign_id IS NULL OR campaign_id = ''
        OR date IS NULL
      )
  )
  SELECT
    v_now, CURRENT_DATE(), v_table,
    'critical',
    'Null Identifier Check (30-day)',
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
      'Investigate upstream Silver rows; ensure MERGE key fields are populated.'
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
      SELECT account_id, campaign_id, date
      FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_campaign_daily`
      WHERE date >= v_start
      GROUP BY account_id, campaign_id, date
      HAVING COUNT(*) > 1
    )
  )
  SELECT
    v_now, CURRENT_DATE(), v_table,
    'critical',
    'Duplicate Grain Check (30-day)',
    'HIGH',
    0.0,
    CAST(dup_cnt AS FLOAT64),
    CAST(dup_cnt AS FLOAT64),
    IF(dup_cnt = 0, 'PASS', 'FAIL'),
    IF(dup_cnt = 0, '🟢', '🔴'),
    IF(dup_cnt = 0,
      'No duplicate grain detected.',
      CONCAT('Duplicate grain keys found: ', CAST(dup_cnt AS STRING), '.')
    ),
    IF(dup_cnt = 0,
      'No action required.',
      'Check Gold Daily MERGE key and upstream Silver uniqueness.'
    ),
    IF(dup_cnt = 0, FALSE, TRUE),
    IF(dup_cnt = 0, TRUE, FALSE),
    IF(dup_cnt = 0, FALSE, TRUE)
  FROM dups;

  -- ---------------------------------------------------------------------------
  -- TEST 3: Partition Freshness vs Silver (HIGH)
  --   Compare max(date) in Gold Daily vs max(date) in Silver Daily
  -- ---------------------------------------------------------------------------
  INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_test_results`
  WITH mx AS (
    SELECT
      (SELECT MAX(date) FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_silver_sa360_campaign_daily`) AS silver_max_date,
      (SELECT MAX(date) FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_campaign_daily`) AS gold_max_date
  ),
  calc AS (
    SELECT
      DATE_DIFF(silver_max_date, gold_max_date, DAY) AS lag_days
    FROM mx
  )
  SELECT
    v_now, CURRENT_DATE(), v_table,
    'critical',
    'Partition Freshness vs Silver (lag days)',
    'HIGH',
    2.0,
    CAST(lag_days AS FLOAT64),
    CAST(lag_days - 2 AS FLOAT64),
    IF(lag_days <= 2, 'PASS', 'FAIL'),
    IF(lag_days <= 2, '🟢', '🔴'),
    IF(lag_days <= 2,
      CONCAT('Freshness OK. Gold lag vs Silver = ', CAST(lag_days AS STRING), ' day(s).'),
      CONCAT('Gold is stale. Lag vs Silver = ', CAST(lag_days AS STRING), ' day(s).')
    ),
    IF(lag_days <= 2,
      'No action required.',
      'Run Gold Daily MERGE / investigate pipeline scheduling & failures.'
    ),
    IF(lag_days <= 2, FALSE, TRUE),
    IF(lag_days <= 2, TRUE, FALSE),
    IF(lag_days <= 2, FALSE, TRUE)
  FROM calc;

END;
