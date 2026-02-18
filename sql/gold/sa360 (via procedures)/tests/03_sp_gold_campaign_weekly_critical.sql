/*
===============================================================================
FILE: 03_sp_gold_campaign_weekly_critical.sql
LAYER: Gold | QA
PROC:  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_gold_sa360_campaign_weekly_critical_tests

TABLE UNDER TEST:
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_campaign_weekly
===============================================================================
*/
CREATE OR REPLACE PROCEDURE
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_gold_sa360_campaign_weekly_critical_tests`()
OPTIONS(strict_mode=false)
BEGIN
  DECLARE lookback_weeks INT64 DEFAULT 12;

  -- ---------------------------------------------------------------------------
  -- TEST 1: Duplicate grain check (acct,campaign,weekend_date)
  -- Expect: 0 duplicate groups
  -- ---------------------------------------------------------------------------
  INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_test_results`
  WITH dup AS (
    SELECT COUNT(1) AS duplicate_groups
    FROM (
      SELECT account_id, campaign_id, weekend_date, COUNT(*) AS c
      FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_campaign_weekly`
      WHERE weekend_date >= DATE_SUB(CURRENT_DATE(), INTERVAL lookback_weeks WEEK)
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
      'Duplicate weekly keys exist; weekly table is not 1-row-per-(acct,campaign,weekend_date).'
    ),
    IF(duplicate_groups = 0,
      'No action required.',
      'Fix weekly build MERGE/INSERT logic; enforce unique key.'
    ),
    IF(duplicate_groups > 0, TRUE, FALSE),
    IF(duplicate_groups = 0, TRUE, FALSE),
    IF(duplicate_groups > 0, TRUE, FALSE)
  FROM dup;

  -- ---------------------------------------------------------------------------
  -- TEST 2: Null key check
  -- Expect: 0 rows with NULL/blank account_id/campaign_id or NULL weekend_date
  -- ---------------------------------------------------------------------------
  INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_test_results`
  WITH bad AS (
    SELECT COUNT(1) AS bad_rows
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_campaign_weekly`
    WHERE weekend_date >= DATE_SUB(CURRENT_DATE(), INTERVAL lookback_weeks WEEK)
      AND (
        weekend_date IS NULL
        OR account_id IS NULL OR TRIM(CAST(account_id AS STRING)) = ''
        OR campaign_id IS NULL OR TRIM(CAST(campaign_id AS STRING)) = ''
      )
  )
  SELECT
    CURRENT_TIMESTAMP(), CURRENT_DATE(),
    'sdi_gold_sa360_campaign_weekly',
    'critical',
    'Null Identifier Check (acct,campaign,weekend_date)',
    'HIGH',
    0.0,
    CAST(bad_rows AS FLOAT64),
    CAST(bad_rows AS FLOAT64),
    IF(bad_rows = 0, 'PASS', 'FAIL'),
    IF(bad_rows = 0, '🟢', '🔴'),
    IF(bad_rows = 0,
      'All weekly identifiers are valid.',
      'Found rows with NULL/blank identifiers; downstream joins will break.'
    ),
    IF(bad_rows = 0,
      'No action required.',
      'Fix weekly build to ensure keys and weekend_date are always populated.'
    ),
    IF(bad_rows > 0, TRUE, FALSE),
    IF(bad_rows = 0, TRUE, FALSE),
    IF(bad_rows > 0, TRUE, FALSE)
  FROM bad;

  -- ---------------------------------------------------------------------------
  -- TEST 3: weekend_date alignment check for WEEK(SATURDAY)
  -- Expect: 0 rows where weekend_date is not a Saturday
  -- BigQuery: EXTRACT(DAYOFWEEK) -> Sunday=1 ... Saturday=7
  -- ---------------------------------------------------------------------------
  INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_test_results`
  WITH bad AS (
    SELECT COUNT(1) AS bad_rows
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_campaign_weekly`
    WHERE weekend_date >= DATE_SUB(CURRENT_DATE(), INTERVAL lookback_weeks WEEK)
      AND weekend_date IS NOT NULL
      AND EXTRACT(DAYOFWEEK FROM weekend_date) != 7
  )
  SELECT
    CURRENT_TIMESTAMP(), CURRENT_DATE(),
    'sdi_gold_sa360_campaign_weekly',
    'critical',
    'Weekend Date Alignment (must be Saturday)',
    'HIGH',
    0.0,
    CAST(bad_rows AS FLOAT64),
    CAST(bad_rows AS FLOAT64),
    IF(bad_rows = 0, 'PASS', 'FAIL'),
    IF(bad_rows = 0, '🟢', '🔴'),
    IF(bad_rows = 0,
      'All weekend_date values align to Saturday (WEEK(SATURDAY)).',
      'Some weekend_date values are not Saturday; week logic mismatch between Daily and Weekly.'
    ),
    IF(bad_rows = 0,
      'No action required.',
      'Fix weekly derivation to use DATE_TRUNC(date, WEEK(SATURDAY)) consistently.'
    ),
    IF(bad_rows > 0, TRUE, FALSE),
    IF(bad_rows = 0, TRUE, FALSE),
    IF(bad_rows > 0, TRUE, FALSE)
  FROM bad;

  -- ---------------------------------------------------------------------------
  -- TEST 4: Missing weekend_dates (Daily has week but Weekly does not)
  -- Expect: 0 missing weeks in lookback window
  -- NOTE: This is a real completeness test, not a cover-up.
  -- ---------------------------------------------------------------------------
  INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_test_results`
  WITH daily_weeks AS (
    SELECT DISTINCT DATE_TRUNC(date, WEEK(SATURDAY)) AS weekend_date
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_campaign_daily`
    WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL lookback_weeks WEEK)
  ),
  weekly_weeks AS (
    SELECT DISTINCT weekend_date
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_campaign_weekly`
    WHERE weekend_date >= DATE_SUB(CURRENT_DATE(), INTERVAL lookback_weeks WEEK)
  ),
  missing AS (
    SELECT COUNT(1) AS missing_weeks
    FROM daily_weeks d
    LEFT JOIN weekly_weeks w USING (weekend_date)
    WHERE w.weekend_date IS NULL
  )
  SELECT
    CURRENT_TIMESTAMP(), CURRENT_DATE(),
    'sdi_gold_sa360_campaign_weekly',
    'critical',
    'Missing Weeks (Daily has week, Weekly missing)',
    'HIGH',
    0.0,
    CAST(missing_weeks AS FLOAT64),
    CAST(missing_weeks AS FLOAT64),
    IF(missing_weeks = 0, 'PASS', 'FAIL'),
    IF(missing_weeks = 0, '🟢', '🔴'),
    IF(missing_weeks = 0,
      'No missing weekend_dates: Weekly coverage matches Daily.',
      'Weekly is missing one or more weekend_dates that exist in Daily.'
    ),
    IF(missing_weeks = 0,
      'No action required.',
      'Check weekly job run window/filtering; ensure weekly is built for all Daily weeks.'
    ),
    IF(missing_weeks > 0, TRUE, FALSE),
    IF(missing_weeks = 0, TRUE, FALSE),
    IF(missing_weeks > 0, TRUE, FALSE)
  FROM missing;

  -- ---------------------------------------------------------------------------
  -- TEST 5: Extra weekend_dates (Weekly has week but Daily does not)
  -- Expect: 0 extra weeks in lookback window
  -- ---------------------------------------------------------------------------
  INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_test_results`
  WITH daily_weeks AS (
    SELECT DISTINCT DATE_TRUNC(date, WEEK(SATURDAY)) AS weekend_date
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_campaign_daily`
    WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL lookback_weeks WEEK)
  ),
  weekly_weeks AS (
    SELECT DISTINCT weekend_date
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_campaign_weekly`
    WHERE weekend_date >= DATE_SUB(CURRENT_DATE(), INTERVAL lookback_weeks WEEK)
  ),
  extra AS (
    SELECT COUNT(1) AS extra_weeks
    FROM weekly_weeks w
    LEFT JOIN daily_weeks d USING (weekend_date)
    WHERE d.weekend_date IS NULL
  )
  SELECT
    CURRENT_TIMESTAMP(), CURRENT_DATE(),
    'sdi_gold_sa360_campaign_weekly',
    'critical',
    'Extra Weeks (Weekly has week, Daily missing)',
    'MEDIUM',
    0.0,
    CAST(extra_weeks AS FLOAT64),
    CAST(extra_weeks AS FLOAT64),
    IF(extra_weeks = 0, 'PASS', 'FAIL'),
    IF(extra_weeks = 0, '🟢', '🔴'),
    IF(extra_weeks = 0,
      'No extra weekend_dates in Weekly vs Daily.',
      'Weekly contains weekend_dates not present in Daily (mismatched logic or stale rows).'
    ),
    IF(extra_weeks = 0,
      'No action required.',
      'Check weekly build source (must roll up from Gold Daily only) and clean stale weeks.'
    ),
    IF(extra_weeks > 0, FALSE, FALSE),
    IF(extra_weeks = 0, TRUE, FALSE),
    IF(extra_weeks > 0, TRUE, FALSE)
  FROM extra;

END;
