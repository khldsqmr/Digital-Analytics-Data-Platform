/*
===============================================================================
FILE: 01_sp_gold_campaign_daily_critical.sql
LAYER: Gold | QA
PROC:  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_gold_sa360_campaign_daily_critical_tests

UPDATES:
  - Freshness test now FAILS cleanly when table is empty (max(date) is NULL)
===============================================================================
*/

CREATE OR REPLACE PROCEDURE
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_gold_sa360_campaign_daily_critical_tests`()
OPTIONS(strict_mode=false)
BEGIN
  DECLARE lookback_days INT64 DEFAULT 14;
  DECLARE allowed_freshness_delay_days INT64 DEFAULT 2;

  -- ===========================================================================
  -- TEST 1: Duplicate grain (account_id, campaign_id, date)
  -- ===========================================================================
  INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_test_results`
  WITH dup AS (
    SELECT COUNT(1) AS duplicate_groups
    FROM (
      SELECT account_id, campaign_id, date, COUNT(*) c
      FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_campaign_daily`
      WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL lookback_days DAY)
      GROUP BY 1,2,3
      HAVING COUNT(*) > 1
    )
  )
  SELECT
    CURRENT_TIMESTAMP(), CURRENT_DATE(),
    'sdi_gold_sa360_campaign_daily',
    'critical',
    'Duplicate Grain Check (acct,campaign,date)',
    'HIGH',
    0.0,
    CAST(duplicate_groups AS FLOAT64),
    CAST(duplicate_groups AS FLOAT64),
    IF(duplicate_groups = 0, 'PASS', 'FAIL'),
    IF(duplicate_groups = 0, '🟢', '🔴'),
    IF(duplicate_groups = 0, 'No duplicate grain detected.',
       'Duplicate keys found in Gold Daily.'),
    IF(duplicate_groups = 0, 'No action required.',
       'Inspect Gold MERGE key and upstream Silver uniqueness.'),
    IF(duplicate_groups > 0, TRUE, FALSE),
    IF(duplicate_groups = 0, TRUE, FALSE),
    IF(duplicate_groups > 0, TRUE, FALSE)
  FROM dup;

  -- ===========================================================================
  -- TEST 2: Freshness check (max(date) delay days)
  --   - If table empty (max_date is NULL) => force FAIL with huge delay
  -- ===========================================================================
  INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_test_results`
  WITH mx AS (
    SELECT MAX(date) AS max_date
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_campaign_daily`
  ),
  calc AS (
    SELECT
      allowed_freshness_delay_days AS allowed_delay,
      IF(max_date IS NULL, 9999, DATE_DIFF(CURRENT_DATE(), max_date, DAY)) AS days_delay,
      max_date
    FROM mx
  )
  SELECT
    CURRENT_TIMESTAMP(), CURRENT_DATE(),
    'sdi_gold_sa360_campaign_daily',
    'critical',
    'Partition Freshness (max(date) delay days)',
    'HIGH',
    CAST(allowed_delay AS FLOAT64),
    CAST(days_delay AS FLOAT64),
    CAST(days_delay - allowed_delay AS FLOAT64),
    IF(days_delay <= allowed_delay, 'PASS', 'FAIL'),
    IF(days_delay <= allowed_delay, '🟢', '🔴'),
    CASE
      WHEN max_date IS NULL THEN 'Gold Daily is empty (max(date) is NULL).'
      WHEN days_delay <= allowed_delay THEN CONCAT('Freshness OK. Delay days = ', CAST(days_delay AS STRING), '.')
      ELSE CONCAT('Gold Daily is stale. Delay days = ', CAST(days_delay AS STRING), '.')
    END,
    CASE
      WHEN max_date IS NULL THEN 'Check upstream build and backfill. Ensure Gold Daily table is populated.'
      WHEN days_delay <= allowed_delay THEN 'No action required.'
      ELSE 'Check Gold daily merge schedule + upstream Silver readiness.'
    END,
    IF(days_delay > allowed_delay, TRUE, FALSE),
    IF(days_delay <= allowed_delay, TRUE, FALSE),
    IF(days_delay > allowed_delay, TRUE, FALSE)
  FROM calc;

END;
