/*
===============================================================================
FILE: 03_sp_gold_campaign_weekly_critical.sql
LAYER: Gold | QA
PROC:  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_gold_sa360_campaign_weekly_critical_tests

TABLE UNDER TEST:
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_campaign_weekly

PURPOSE:
  Critical QA tests for Gold Weekly (QGP-week):
    1) Duplicate grain check (acct,campaign,qgp_week)
    2) Null identifier check
    3) QGP week alignment check (qgp_week must be Saturday OR quarter-end)
    4) Missing qgp_weeks (Daily has qgp_week, Weekly missing)  <-- FIXED WINDOW + SAME BUCKETING
    5) Extra qgp_weeks (Weekly has qgp_week, Daily missing)    <-- FIXED WINDOW + SAME BUCKETING

KEY FIX:
  Compare Daily vs Weekly on the SAME derived qgp_week logic.
  Cutoff window anchored to Saturday for stable scanning, but qgp_week can be
  either Saturday or quarter-end.
===============================================================================
*/

CREATE OR REPLACE PROCEDURE
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_gold_sa360_campaign_weekly_critical_tests`()
OPTIONS(strict_mode=false)
BEGIN
  DECLARE lookback_weeks INT64 DEFAULT 12;

  -- Anchor cutoff to Saturday so window is stable and prevents mid-week false positives.
  DECLARE cutoff_anchor DATE DEFAULT DATE_TRUNC(
    DATE_SUB(CURRENT_DATE(), INTERVAL lookback_weeks WEEK),
    WEEK(SATURDAY)
  );

  -- ---------------------------------------------------------------------------
  -- TEST 1: Duplicate grain (account_id, campaign_id, qgp_week)
  -- ---------------------------------------------------------------------------
  INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_test_results`
  WITH dup AS (
    SELECT COUNT(1) AS duplicate_groups
    FROM (
      SELECT account_id, campaign_id, qgp_week, COUNT(*) c
      FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_campaign_weekly`
      WHERE qgp_week >= cutoff_anchor
      GROUP BY 1,2,3
      HAVING COUNT(*) > 1
    )
  )
  SELECT
    CURRENT_TIMESTAMP(), CURRENT_DATE(),
    'sdi_gold_sa360_campaign_weekly',
    'critical',
    'Duplicate Grain Check (acct,campaign,qgp_week)',
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
  -- TEST 2: Null identifier check (account_id, campaign_id, qgp_week)
  -- ---------------------------------------------------------------------------
  INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_test_results`
  WITH bad AS (
    SELECT COUNT(1) AS bad_rows
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_campaign_weekly`
    WHERE qgp_week >= cutoff_anchor
      AND (
        account_id IS NULL OR
        campaign_id IS NULL OR
        qgp_week IS NULL
      )
  )
  SELECT
    CURRENT_TIMESTAMP(), CURRENT_DATE(),
    'sdi_gold_sa360_campaign_weekly',
    'critical',
    '"Null Identifier Check (acct,campaign,qgp_week)"',
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
  -- TEST 3: QGP Week Alignment
  -- qgp_week must be EITHER:
  --   - Saturday-aligned (DATE_TRUNC(qgp_week, WEEK(SATURDAY)) == qgp_week)
  --   - OR quarter-end date for its quarter
  -- ---------------------------------------------------------------------------
  INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_test_results`
  WITH mis AS (
    SELECT COUNT(1) AS misaligned_rows
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_campaign_weekly`
    WHERE qgp_week >= cutoff_anchor
      AND (
        qgp_week != DATE_TRUNC(qgp_week, WEEK(SATURDAY))
        AND qgp_week != DATE_SUB(
          DATE_ADD(DATE_TRUNC(qgp_week, QUARTER), INTERVAL 3 MONTH),
          INTERVAL 1 DAY
        )
      )
  )
  SELECT
    CURRENT_TIMESTAMP(), CURRENT_DATE(),
    'sdi_gold_sa360_campaign_weekly',
    'critical',
    'QGP Week Alignment (must be Saturday OR quarter-end)',
    'HIGH',
    0.0,
    CAST(misaligned_rows AS FLOAT64),
    CAST(misaligned_rows AS FLOAT64),
    IF(misaligned_rows = 0, 'PASS', 'FAIL'),
    IF(misaligned_rows = 0, '🟢', '🔴'),
    IF(misaligned_rows = 0,
      'All qgp_week values are valid (Saturday-aligned or quarter-end).',
      'One or more qgp_week values are neither Saturday nor quarter-end.'
    ),
    IF(misaligned_rows = 0,
      'No action required.',
      'Fix qgp_week derivation; must be DATE_TRUNC(date, WEEK(SATURDAY)) or quarter_end for partial weeks.'
    ),
    IF(misaligned_rows > 0, TRUE, FALSE),
    IF(misaligned_rows = 0, TRUE, FALSE),
    IF(misaligned_rows > 0, TRUE, FALSE)
  FROM mis;


  -- ---------------------------------------------------------------------------
  -- TEST 4: Missing QGP Weeks (Daily has qgp_week, Weekly missing)
  -- Uses SAME qgp_week bucketing logic as the weekly build.
  -- ---------------------------------------------------------------------------
  INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_test_results`
  WITH daily_qgp AS (
    SELECT DISTINCT
      CASE
        WHEN quarter_end < week_end_sat AND date <= quarter_end THEN quarter_end
        ELSE week_end_sat
      END AS qgp_week
    FROM (
      SELECT
        date,
        DATE_TRUNC(date, WEEK(SATURDAY)) AS week_end_sat,
        DATE_SUB(
          DATE_ADD(DATE_TRUNC(date, QUARTER), INTERVAL 3 MONTH),
          INTERVAL 1 DAY
        ) AS quarter_end
      FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_campaign_daily`
      WHERE date >= cutoff_anchor
        AND date IS NOT NULL
    )
    WHERE
      CASE
        WHEN quarter_end < week_end_sat AND date <= quarter_end THEN quarter_end
        ELSE week_end_sat
      END >= cutoff_anchor
  ),
  weekly_qgp AS (
    SELECT DISTINCT qgp_week
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_campaign_weekly`
    WHERE qgp_week >= cutoff_anchor
  ),
  missing AS (
    SELECT d.qgp_week
    FROM daily_qgp d
    LEFT JOIN weekly_qgp w USING (qgp_week)
    WHERE w.qgp_week IS NULL
  )
  SELECT
    CURRENT_TIMESTAMP(), CURRENT_DATE(),
    'sdi_gold_sa360_campaign_weekly',
    'critical',
    '"Missing QGP Weeks (Daily has qgp_week, Weekly missing)"',
    'HIGH',
    0.0,
    CAST(COUNT(*) AS FLOAT64),
    CAST(COUNT(*) AS FLOAT64),
    IF(COUNT(*) = 0, 'PASS', 'FAIL'),
    IF(COUNT(*) = 0, '🟢', '🔴'),
    IF(COUNT(*) = 0,
      'No missing qgp_week values in Weekly vs Daily (same bucketing + aligned window).',
      CONCAT('Weekly is missing qgp_week example: ', CAST(ANY_VALUE(qgp_week) AS STRING))
    ),
    IF(COUNT(*) = 0,
      'No action required.',
      'Check weekly job run window/backfill; ensure weekly is built for all Daily qgp_week buckets.'
    ),
    IF(COUNT(*) > 0, TRUE, FALSE),
    IF(COUNT(*) = 0, TRUE, FALSE),
    IF(COUNT(*) > 0, TRUE, FALSE)
  FROM missing;


  -- ---------------------------------------------------------------------------
  -- TEST 5: Extra QGP Weeks (Weekly has qgp_week, Daily missing)
  -- Uses SAME qgp_week bucketing logic as the weekly build.
  -- ---------------------------------------------------------------------------
  INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_test_results`
  WITH daily_qgp AS (
    SELECT DISTINCT
      CASE
        WHEN quarter_end < week_end_sat AND date <= quarter_end THEN quarter_end
        ELSE week_end_sat
      END AS qgp_week
    FROM (
      SELECT
        date,
        DATE_TRUNC(date, WEEK(SATURDAY)) AS week_end_sat,
        DATE_SUB(
          DATE_ADD(DATE_TRUNC(date, QUARTER), INTERVAL 3 MONTH),
          INTERVAL 1 DAY
        ) AS quarter_end
      FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_campaign_daily`
      WHERE date >= cutoff_anchor
        AND date IS NOT NULL
    )
    WHERE
      CASE
        WHEN quarter_end < week_end_sat AND date <= quarter_end THEN quarter_end
        ELSE week_end_sat
      END >= cutoff_anchor
  ),
  weekly_qgp AS (
    SELECT DISTINCT qgp_week
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_campaign_weekly`
    WHERE qgp_week >= cutoff_anchor
  ),
  extra AS (
    SELECT w.qgp_week
    FROM weekly_qgp w
    LEFT JOIN daily_qgp d USING (qgp_week)
    WHERE d.qgp_week IS NULL
  )
  SELECT
    CURRENT_TIMESTAMP(), CURRENT_DATE(),
    'sdi_gold_sa360_campaign_weekly',
    'critical',
    '"Extra QGP Weeks (Weekly has qgp_week, Daily missing)"',
    'MEDIUM',
    0.0,
    CAST(COUNT(*) AS FLOAT64),
    CAST(COUNT(*) AS FLOAT64),
    IF(COUNT(*) = 0, 'PASS', 'FAIL'),
    IF(COUNT(*) = 0, '🟢', '🔴'),
    IF(COUNT(*) = 0,
      'No extra qgp_week values in Weekly vs Daily (same bucketing + aligned window).',
      CONCAT('Weekly has extra qgp_week example: ', CAST(ANY_VALUE(qgp_week) AS STRING))
    ),
    IF(COUNT(*) = 0,
      'No action required.',
      'Check weekly filters/late arriving data; ensure Daily coverage matches Weekly buckets.'
    ),
    FALSE, -- not critical by default (MEDIUM)
    IF(COUNT(*) = 0, TRUE, FALSE),
    IF(COUNT(*) > 0, TRUE, FALSE)
  FROM extra;

END;
