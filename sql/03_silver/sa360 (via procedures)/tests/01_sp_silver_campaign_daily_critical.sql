/*
===============================================================================
FILE: 01_sp_silver_campaign_daily_critical.sql
LAYER: Silver | QA
PROC:  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_silver_sa360_campaign_daily_critical_tests

TABLE UNDER TEST:
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_silver_sa360_campaign_daily

WHY:
  Silver is your business-ready fact. Must be unique, fresh, and join-safe.
===============================================================================
*/

CREATE OR REPLACE PROCEDURE
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_silver_sa360_campaign_daily_critical_tests`()
OPTIONS(strict_mode=false)
BEGIN
  DECLARE lookback_days INT64 DEFAULT 7;
  DECLARE allowed_freshness_delay_days INT64 DEFAULT 2;

  -- Duplicate grain (account_id, campaign_id, date)
  INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_silver_sa360_test_results`
  WITH dup AS (
    SELECT COUNT(1) AS duplicate_groups
    FROM (
      SELECT account_id, campaign_id, date, COUNT(*) c
      FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_silver_sa360_campaign_daily`
      WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL lookback_days DAY)
      GROUP BY 1,2,3
      HAVING COUNT(*) > 1
    )
  )
  SELECT
    CURRENT_TIMESTAMP(), CURRENT_DATE(),
    'sdi_silver_sa360_campaign_daily',
    'critical',
    'Duplicate Grain Check (acct,campaign,date)',
    'HIGH',
    0.0,
    CAST(duplicate_groups AS FLOAT64),
    CAST(duplicate_groups AS FLOAT64),
    IF(duplicate_groups = 0, 'PASS', 'FAIL'),
    IF(duplicate_groups = 0, '🟢', '🔴'),
    IF(duplicate_groups = 0, 'No duplicate grain detected.',
       'Duplicate keys found in Silver Daily. This breaks reporting grain.'),
    IF(duplicate_groups = 0, 'No action required.',
       'Inspect Silver MERGE key + upstream Bronze uniqueness and entity AS-OF join.'),
    IF(duplicate_groups > 0, TRUE, FALSE),
    IF(duplicate_groups = 0, TRUE, FALSE),
    IF(duplicate_groups > 0, TRUE, FALSE)
  FROM dup;

  -- Null identifier check
  INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_silver_sa360_test_results`
  WITH bad AS (
    SELECT COUNT(1) AS bad_rows
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_silver_sa360_campaign_daily`
    WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL lookback_days DAY)
      AND (account_id IS NULL OR campaign_id IS NULL OR date IS NULL
           OR TRIM(account_id) = '' OR TRIM(campaign_id) = '')
  )
  SELECT
    CURRENT_TIMESTAMP(), CURRENT_DATE(),
    'sdi_silver_sa360_campaign_daily',
    'critical',
    'Null Identifier Check (acct,campaign,date)',
    'HIGH',
    0.0,
    CAST(bad_rows AS FLOAT64),
    CAST(bad_rows AS FLOAT64),
    IF(bad_rows = 0, 'PASS', 'FAIL'),
    IF(bad_rows = 0, '🟢', '🔴'),
    IF(bad_rows = 0, 'All identifiers valid.',
       'Null/blank keys found in Silver. Downstream joins break.'),
    IF(bad_rows = 0, 'No action required.',
       'Validate Bronze inputs and Silver MERGE logic.'),
    IF(bad_rows > 0, TRUE, FALSE),
    IF(bad_rows = 0, TRUE, FALSE),
    IF(bad_rows > 0, TRUE, FALSE)
  FROM bad;

  -- Freshness check
  INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_silver_sa360_test_results`
  WITH mx AS (
    SELECT MAX(date) AS max_date
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_silver_sa360_campaign_daily`
  ),
  calc AS (
    SELECT
      allowed_freshness_delay_days AS allowed_delay,
      DATE_DIFF(CURRENT_DATE(), max_date, DAY) AS days_delay
    FROM mx
  )
  SELECT
    CURRENT_TIMESTAMP(), CURRENT_DATE(),
    'sdi_silver_sa360_campaign_daily',
    'critical',
    'Partition Freshness (max(date) delay days)',
    'HIGH',
    CAST(allowed_delay AS FLOAT64),
    CAST(days_delay AS FLOAT64),
    CAST(days_delay - allowed_delay AS FLOAT64),
    IF(days_delay <= allowed_delay, 'PASS', 'FAIL'),
    IF(days_delay <= allowed_delay, '🟢', '🔴'),
    IF(days_delay <= allowed_delay,
      CONCAT('Freshness OK. Delay days = ', CAST(days_delay AS STRING), '.'),
      CONCAT('Silver table is stale. Delay days = ', CAST(days_delay AS STRING), '.')
    ),
    IF(days_delay <= allowed_delay,
      'No action required.',
      'Check Silver MERGE job schedule + upstream Bronze availability.'
    ),
    IF(days_delay > allowed_delay, TRUE, FALSE),
    IF(days_delay <= allowed_delay, TRUE, FALSE),
    IF(days_delay > allowed_delay, TRUE, FALSE)
  FROM calc;

END;
