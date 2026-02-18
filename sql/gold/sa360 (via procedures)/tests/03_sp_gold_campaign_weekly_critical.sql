/*
===============================================================================
FILE: 03_sp_gold_campaign_weekly_critical.sql
LAYER: Gold | QA
PROC:  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_gold_sa360_campaign_weekly_critical_tests

TABLE UNDER TEST:
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_campaign_weekly

PURPOSE:
  Real critical QA tests for Gold Weekly:
    1) Duplicate grain check (acct,campaign,weekend_date)
    2) Null identifier check
    3) Weekend alignment check (must be Saturday)
    4) Missing weeks (Daily has weekend_date, Weekly missing)   <-- FIXED WINDOW
    5) Extra weeks (Weekly has weekend_date, Daily missing)     <-- FIXED WINDOW

KEY FIX:
  Use Saturday-aligned cutoff_weekend so Daily and Weekly are compared on the
  same "weekend_date" window. This prevents false positives like 2025-11-22
  when the raw lookback date starts mid-week.
===============================================================================
*/

CREATE OR REPLACE PROCEDURE
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_gold_sa360_campaign_weekly_critical_tests`()
OPTIONS(strict_mode=false)
BEGIN
  DECLARE lookback_weeks INT64 DEFAULT 12;

  -- IMPORTANT: Align the window to Saturday to match weekend_date logic
  DECLARE cutoff_weekend DATE DEFAULT DATE_TRUNC(
    DATE_SUB(CURRENT_DATE(), INTERVAL lookback_weeks WEEK),
    WEEK(SATURDAY)
  );

  -- ---------------------------------------------------------------------------
  -- TEST 1: Duplicate grain (account_id, campaign_id, weekend_date)
  -- ---------------------------------------------------------------------------
  INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_test_results`
  WITH dup AS (
    SELECT COUNT(1) AS duplicate_groups
    FROM (
      SELECT account_id, campaign_id, weekend_date, COUNT(*) c
      FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_campaign_weekly`
      WHERE weekend_date >= cutoff_weekend
      GROUP BY 1,2,3
      HAVING COUNT(*) > 1
    )
  )
  SELECT
    CURRENT_TIMESTAMP(), CURRENT_DATE(),
    'sdi_gold_sa360_campaign_weekly',
    'critical',
    'Duplicate Grain Check (acct,campaign,weekend_date)',
    'HIGH',
    0.0,
    CAST(duplicate_groups AS FLOAT64),
    CAST(duplicate_groups AS FLOAT64),
    IF(duplicate_groups = 0, 'PASS', 'FAIL'),
    IF(duplicate_groups = 0, '🟢', '🔴'),
    IF(duplicate_groups = 0,
      'No duplicate weekly grain detected.',
      'Duplicate keys found in Gold Weekly.'
    ),
    IF(duplicate_groups = 0,
      'No action required.',
      'Fix weekly MERGE logic / upstream duplication; verify unique grain.'
    ),
    IF(duplicate_groups > 0, TRUE, FALSE),
    IF(duplicate_groups = 0, TRUE, FALSE),
    IF(duplicate_groups > 0, TRUE, FALSE)
  FROM dup;


  -- ---------------------------------------------------------------------------
  -- TEST 2: Null identifier check (account_id, campaign_id, weekend_date)
  -- ---------------------------------------------------------------------------
  INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_test_results`
  WITH bad AS (
    SELECT COUNT(1) AS bad_rows
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_campaign_weekly`
    WHERE weekend_date >= cutoff_weekend
      AND (
        account_id IS NULL OR
        campaign_id IS NULL OR
        weekend_date IS NULL
      )
  )
  SELECT
    CURRENT_TIMESTAMP(), CURRENT_DATE(),
    'sdi_gold_sa360_campaign_weekly',
    'critical',
    '"Null Identifier Check (acct,campaign,weekend_date)"',
    'HIGH',
    0.0,
    CAST(bad_rows AS FLOAT64),
    CAST(bad_rows AS FLOAT64),
    IF(bad_rows = 0, 'PASS', 'FAIL'),
    IF(bad_rows = 0, '🟢', '🔴'),
    IF(bad_rows = 0,
      'All weekly identifiers are valid.',
      'Null identifier(s) found in Gold Weekly.'
    ),
    IF(bad_rows = 0,
      'No action required.',
      'Fix upstream mapping; ensure identifiers are always populated.'
    ),
    IF(bad_rows > 0, TRUE, FALSE),
    IF(bad_rows = 0, TRUE, FALSE),
    IF(bad_rows > 0, TRUE, FALSE)
  FROM bad;


  -- ---------------------------------------------------------------------------
  -- TEST 3: Weekend Date Alignment (must be Saturday)
  -- ---------------------------------------------------------------------------
  INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_test_results`
  WITH mis AS (
    SELECT COUNT(1) AS misaligned_rows
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_campaign_weekly`
    WHERE weekend_date >= cutoff_weekend
      AND weekend_date != DATE_TRUNC(weekend_date, WEEK(SATURDAY))
  )
  SELECT
    CURRENT_TIMESTAMP(), CURRENT_DATE(),
    'sdi_gold_sa360_campaign_weekly',
    'critical',
    'Weekend Date Alignment (must be Saturday)',
    'HIGH',
    0.0,
    CAST(misaligned_rows AS FLOAT64),
    CAST(misaligned_rows AS FLOAT64),
    IF(misaligned_rows = 0, 'PASS', 'FAIL'),
    IF(misaligned_rows = 0, '🟢', '🔴'),
    IF(misaligned_rows = 0,
      'All weekend_date values align to Saturday (WEEK(SATURDAY)).',
      'One or more weekend_date values are not Saturday-aligned.'
    ),
    IF(misaligned_rows = 0,
      'No action required.',
      'Fix weekend_date derivation. Must use DATE_TRUNC(date, WEEK(SATURDAY)).'
    ),
    IF(misaligned_rows > 0, TRUE, FALSE),
    IF(misaligned_rows = 0, TRUE, FALSE),
    IF(misaligned_rows > 0, TRUE, FALSE)
  FROM mis;


  -- ---------------------------------------------------------------------------
  -- TEST 4: Missing Weeks (Daily has week, Weekly missing)  ✅ FIXED
  -- Uses the SAME weekend_date-aligned window on both sides.
  -- ---------------------------------------------------------------------------
  INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_test_results`
  WITH daily_weeks AS (
    SELECT DISTINCT DATE_TRUNC(date, WEEK(SATURDAY)) AS weekend_date
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_campaign_daily`
    WHERE DATE_TRUNC(date, WEEK(SATURDAY)) >= cutoff_weekend
  ),
  weekly_weeks AS (
    SELECT DISTINCT weekend_date
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_campaign_weekly`
    WHERE weekend_date >= cutoff_weekend
  ),
  missing AS (
    SELECT d.weekend_date
    FROM daily_weeks d
    LEFT JOIN weekly_weeks w USING (weekend_date)
    WHERE w.weekend_date IS NULL
  )
  SELECT
    CURRENT_TIMESTAMP(), CURRENT_DATE(),
    'sdi_gold_sa360_campaign_weekly',
    'critical',
    '"Missing Weeks (Daily has week, Weekly missing)"',
    'HIGH',
    0.0,
    CAST(COUNT(*) AS FLOAT64),
    CAST(COUNT(*) AS FLOAT64),
    IF(COUNT(*) = 0, 'PASS', 'FAIL'),
    IF(COUNT(*) = 0, '🟢', '🔴'),
    IF(COUNT(*) = 0,
      'No missing weekend_dates in Weekly vs Daily (Saturday-aligned window).',
      CONCAT('Weekly is missing weekend_date example: ', CAST(ANY_VALUE(weekend_date) AS STRING))
    ),
    IF(COUNT(*) = 0,
      'No action required.',
      'Check weekly job run window/backfill; ensure weekly is built for all Daily weeks.'
    ),
    IF(COUNT(*) > 0, TRUE, FALSE),
    IF(COUNT(*) = 0, TRUE, FALSE),
    IF(COUNT(*) > 0, TRUE, FALSE)
  FROM missing;


  -- ---------------------------------------------------------------------------
  -- TEST 5: Extra Weeks (Weekly has week, Daily missing) ✅ FIXED
  -- Uses the SAME weekend_date-aligned window on both sides.
  -- ---------------------------------------------------------------------------
  INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_test_results`
  WITH daily_weeks AS (
    SELECT DISTINCT DATE_TRUNC(date, WEEK(SATURDAY)) AS weekend_date
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_campaign_daily`
    WHERE DATE_TRUNC(date, WEEK(SATURDAY)) >= cutoff_weekend
  ),
  weekly_weeks AS (
    SELECT DISTINCT weekend_date
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_campaign_weekly`
    WHERE weekend_date >= cutoff_weekend
  ),
  extra AS (
    SELECT w.weekend_date
    FROM weekly_weeks w
    LEFT JOIN daily_weeks d USING (weekend_date)
    WHERE d.weekend_date IS NULL
  )
  SELECT
    CURRENT_TIMESTAMP(), CURRENT_DATE(),
    'sdi_gold_sa360_campaign_weekly',
    'critical',
    '"Extra Weeks (Weekly has week, Daily missing)"',
    'MEDIUM',
    0.0,
    CAST(COUNT(*) AS FLOAT64),
    CAST(COUNT(*) AS FLOAT64),
    IF(COUNT(*) = 0, 'PASS', 'FAIL'),
    IF(COUNT(*) = 0, '🟢', '🔴'),
    IF(COUNT(*) = 0,
      'No extra weekend_dates in Weekly vs Daily (Saturday-aligned window).',
      CONCAT('Weekly has extra weekend_date example: ', CAST(ANY_VALUE(weekend_date) AS STRING))
    ),
    IF(COUNT(*) = 0,
      'No action required.',
      'Check weekly filters/late arriving data; ensure Daily coverage matches Weekly.'
    ),
    FALSE, -- not critical by default (MEDIUM)
    IF(COUNT(*) = 0, TRUE, FALSE),
    IF(COUNT(*) > 0, TRUE, FALSE)
  FROM extra;

END;
