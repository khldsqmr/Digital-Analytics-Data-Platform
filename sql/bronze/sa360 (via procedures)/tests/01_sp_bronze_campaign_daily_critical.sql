/*
===============================================================================
FILE: 01_sp_bronze_campaign_daily_critical.sql
LAYER: Bronze | QA
PROC:  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_bronze_sa360_campaign_daily_critical_tests

TABLE UNDER TEST:
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_campaign_daily

WHY THESE TESTS:
  - Bronze must be structurally sound (unique grain, no null IDs)
  - Freshness ensures pipeline is not stale
  - Metric sanity prevents obvious corruption (negative cart_start / postpaid_pspv)

NOTES:
  - Tests run on a configurable lookback window (default 7 days).
  - Each test writes exactly 1 row into sdi_bronze_sa360_test_results.
===============================================================================
*/

CREATE OR REPLACE PROCEDURE
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_bronze_sa360_campaign_daily_critical_tests`()
OPTIONS(strict_mode=false)
BEGIN
  DECLARE lookback_days INT64 DEFAULT 7;
  DECLARE allowed_freshness_delay_days INT64 DEFAULT 2;

  -- ---------------------------------------------------------------------------
  -- TEST 1: Duplicate grain check (account_id, campaign_id, date_yyyymmdd)
  -- Expect: 0 duplicates in the lookback window
  -- ---------------------------------------------------------------------------
  INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_test_results`
  WITH dup AS (
    SELECT
      COUNT(1) AS duplicate_groups
    FROM (
      SELECT
        account_id, campaign_id, date_yyyymmdd,
        COUNT(*) AS c
      FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_campaign_daily`
      WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL lookback_days DAY)
      GROUP BY 1,2,3
      HAVING COUNT(*) > 1
    )
  )
  SELECT
    CURRENT_TIMESTAMP() AS test_run_timestamp,
    CURRENT_DATE() AS test_date,
    'sdi_bronze_sa360_campaign_daily' AS table_name,
    'critical' AS test_layer,
    'Duplicate Grain Check (acct,campaign,date_yyyymmdd)' AS test_name,
    'HIGH' AS severity_level,
    0.0 AS expected_value,
    CAST(duplicate_groups AS FLOAT64) AS actual_value,
    CAST(duplicate_groups AS FLOAT64) - 0.0 AS variance_value,
    IF(duplicate_groups = 0, 'PASS', 'FAIL') AS status,
    IF(duplicate_groups = 0, '🟢', '🔴') AS status_emoji,
    IF(duplicate_groups = 0,
      'No duplicate grain detected.',
      'Duplicate keys found in Bronze Daily. This breaks 1-row-per-key guarantees.'
    ) AS failure_reason,
    IF(duplicate_groups = 0,
      'No action required.',
      'Investigate MERGE dedupe logic, late files, or upstream raw duplication.'
    ) AS next_step,
    IF(duplicate_groups > 0, TRUE, FALSE) AS is_critical_failure,
    IF(duplicate_groups = 0, TRUE, FALSE) AS is_pass,
    IF(duplicate_groups > 0, TRUE, FALSE) AS is_fail
  FROM dup;

  -- ---------------------------------------------------------------------------
  -- TEST 2: Null identifier check
  -- Expect: 0 rows with null keys (account_id/campaign_id/date_yyyymmdd/date)
  -- ---------------------------------------------------------------------------
  INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_test_results`
  WITH bad AS (
    SELECT COUNT(1) AS bad_rows
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_campaign_daily`
    WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL lookback_days DAY)
      AND (
        account_id IS NULL OR campaign_id IS NULL OR date_yyyymmdd IS NULL OR date IS NULL
        OR TRIM(account_id) = '' OR TRIM(campaign_id) = '' OR TRIM(date_yyyymmdd) = ''
      )
  )
  SELECT
    CURRENT_TIMESTAMP(),
    CURRENT_DATE(),
    'sdi_bronze_sa360_campaign_daily',
    'critical',
    'Null Identifier Check (keys + canonical date)',
    'HIGH',
    0.0,
    CAST(bad_rows AS FLOAT64),
    CAST(bad_rows AS FLOAT64) - 0.0,
    IF(bad_rows = 0, 'PASS', 'FAIL'),
    IF(bad_rows = 0, '🟢', '🔴'),
    IF(bad_rows = 0,
      'All identifiers valid.',
      'Null/blank keys found. This will break joins/aggregations downstream.'
    ),
    IF(bad_rows = 0,
      'No action required.',
      'Trace failing rows back to RAW. Confirm SAFE_CAST/SAFE.PARSE_DATE + filters.'
    ),
    IF(bad_rows > 0, TRUE, FALSE),
    IF(bad_rows = 0, TRUE, FALSE),
    IF(bad_rows > 0, TRUE, FALSE)
  FROM bad;

  -- ---------------------------------------------------------------------------
  -- TEST 3: Partition freshness (max(date) within allowed delay)
  -- Expect: date_diff(CURRENT_DATE, max(date)) <= allowed_freshness_delay_days
  -- ---------------------------------------------------------------------------
  INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_test_results`
  WITH mx AS (
    SELECT MAX(date) AS max_date
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_campaign_daily`
  ),
  calc AS (
    SELECT
      allowed_freshness_delay_days AS allowed_delay,
      DATE_DIFF(CURRENT_DATE(), max_date, DAY) AS days_delay
    FROM mx
  )
  SELECT
    CURRENT_TIMESTAMP(),
    CURRENT_DATE(),
    'sdi_bronze_sa360_campaign_daily',
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
      CONCAT('Table is stale. Delay days = ', CAST(days_delay AS STRING), '.')
    ),
    IF(days_delay <= allowed_delay,
      'No action required.',
      'Check upstream ingestion schedule, MERGE job success, and RAW availability.'
    ),
    IF(days_delay > allowed_delay, TRUE, FALSE),
    IF(days_delay <= allowed_delay, TRUE, FALSE),
    IF(days_delay > allowed_delay, TRUE, FALSE)
  FROM calc;

  -- ---------------------------------------------------------------------------
  -- TEST 4: Negative metric sanity (cart_start)
  -- Expect: 0 negative rows for cart_start in window
  -- ---------------------------------------------------------------------------
  INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_test_results`
  WITH neg AS (
    SELECT COUNT(1) AS neg_rows
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_campaign_daily`
    WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL lookback_days DAY)
      AND cart_start IS NOT NULL
      AND cart_start < 0
  )
  SELECT
    CURRENT_TIMESTAMP(),
    CURRENT_DATE(),
    'sdi_bronze_sa360_campaign_daily',
    'critical',
    'Negative Value Check (cart_start < 0)',
    'HIGH',
    0.0,
    CAST(neg_rows AS FLOAT64),
    CAST(neg_rows AS FLOAT64),
    IF(neg_rows = 0, 'PASS', 'FAIL'),
    IF(neg_rows = 0, '🟢', '🔴'),
    IF(neg_rows = 0,
      'No negative cart_start values detected.',
      'Negative cart_start values detected (should not happen for count metrics).'
    ),
    IF(neg_rows = 0,
      'No action required.',
      'Trace to RAW for the same keys; confirm upstream export rules and type casting.'
    ),
    IF(neg_rows > 0, TRUE, FALSE),
    IF(neg_rows = 0, TRUE, FALSE),
    IF(neg_rows > 0, TRUE, FALSE)
  FROM neg;

  -- ---------------------------------------------------------------------------
  -- TEST 5: Negative metric sanity (postpaid_pspv)
  -- Expect: 0 negative rows for postpaid_pspv in window
  -- ---------------------------------------------------------------------------
  INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_test_results`
  WITH neg AS (
    SELECT COUNT(1) AS neg_rows
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_campaign_daily`
    WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL lookback_days DAY)
      AND postpaid_pspv IS NOT NULL
      AND postpaid_pspv < 0
  )
  SELECT
    CURRENT_TIMESTAMP(),
    CURRENT_DATE(),
    'sdi_bronze_sa360_campaign_daily',
    'critical',
    'Negative Value Check (postpaid_pspv < 0)',
    'HIGH',
    0.0,
    CAST(neg_rows AS FLOAT64),
    CAST(neg_rows AS FLOAT64),
    IF(neg_rows = 0, 'PASS', 'FAIL'),
    IF(neg_rows = 0, '🟢', '🔴'),
    IF(neg_rows = 0,
      'No negative postpaid_pspv values detected.',
      'Negative postpaid_pspv values detected (should not happen for count metrics).'
    ),
    IF(neg_rows = 0,
      'No action required.',
      'Trace to RAW for the same keys; confirm upstream export rules and metric definitions.'
    ),
    IF(neg_rows > 0, TRUE, FALSE),
    IF(neg_rows = 0, TRUE, FALSE),
    IF(neg_rows > 0, TRUE, FALSE)
  FROM neg;

END;
