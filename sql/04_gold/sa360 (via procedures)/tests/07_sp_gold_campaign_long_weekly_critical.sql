/*
===============================================================================
FILE: 07_sp_gold_campaign_long_weekly_critical.sql
PROC: sp_gold_sa360_campaign_long_weekly_critical_tests

BASIC correctness:
  - Duplicate grain
  - Null identifiers
  - QGP week validity

FOCUS:
  - focus metrics only: cart_start, postpaid_pspv

CRITICAL UPDATE:
  - Exclude FUTURE qgp_week buckets from test scope (qgp_week > CURRENT_DATE()).
  - Uses max_allowed_qgp_week derived from Gold wide weekly.
===============================================================================
*/

CREATE OR REPLACE PROCEDURE
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_gold_sa360_campaign_long_weekly_critical_tests`()
OPTIONS(strict_mode=false)
BEGIN
  DECLARE lookback_weeks INT64 DEFAULT 12;

  DECLARE cutoff_anchor DATE DEFAULT
    `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.fn_qgp_week`(
      DATE_SUB(CURRENT_DATE(), INTERVAL lookback_weeks WEEK)
    );

  DECLARE metric_focus ARRAY<STRING> DEFAULT ['cart_start','postpaid_pspv'];

  -- IMPORTANT: cap scope to non-future qgp_week buckets
  DECLARE max_allowed_qgp_week DATE DEFAULT (
    SELECT MAX(qgp_week)
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_campaign_weekly`
    WHERE qgp_week <= CURRENT_DATE()
  );

  -- ===========================================================================
  -- TEST 1: Duplicate grain (acct,campaign,qgp_week,metric_name)
  -- ===========================================================================
  INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_test_results`
  WITH dup AS (
    SELECT COUNT(1) AS duplicate_groups
    FROM (
      SELECT account_id, campaign_id, qgp_week, metric_name, COUNT(*) c
      FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_campaign_weekly_long`
      WHERE qgp_week >= cutoff_anchor
        AND qgp_week <= max_allowed_qgp_week
        AND metric_name IN UNNEST(metric_focus)
      GROUP BY 1,2,3,4
      HAVING COUNT(*) > 1
    )
  )
  SELECT
    CURRENT_TIMESTAMP(), CURRENT_DATE(),
    'sdi_gold_sa360_campaign_weekly_long',
    'critical',
    'Duplicate Grain Check (acct,campaign,qgp_week,metric_name) - focus metrics',
    'HIGH',
    0.0,
    CAST(duplicate_groups AS FLOAT64),
    CAST(duplicate_groups AS FLOAT64),
    IF(duplicate_groups = 0, 'PASS', 'FAIL'),
    IF(duplicate_groups = 0, '🟢', '🔴'),
    IF(duplicate_groups = 0, 'No duplicate grain detected for focus metrics.',
       'Duplicate keys found in long weekly for focus metrics.'),
    IF(duplicate_groups = 0, 'No action required.',
       'Fix weekly_long MERGE key/unpivot; ensure uniqueness.'),
    (duplicate_groups > 0),
    (duplicate_groups = 0),
    (duplicate_groups > 0)
  FROM dup;

  -- ===========================================================================
  -- TEST 2: Null identifiers
  -- ===========================================================================
  INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_test_results`
  WITH bad AS (
    SELECT COUNT(1) AS bad_rows
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_campaign_weekly_long`
    WHERE qgp_week >= cutoff_anchor
      AND qgp_week <= max_allowed_qgp_week
      AND metric_name IN UNNEST(metric_focus)
      AND (
        account_id IS NULL OR
        campaign_id IS NULL OR
        qgp_week IS NULL OR
        metric_name IS NULL
      )
  )
  SELECT
    CURRENT_TIMESTAMP(), CURRENT_DATE(),
    'sdi_gold_sa360_campaign_weekly_long',
    'critical',
    'Null Identifier Check (acct,campaign,qgp_week,metric_name) - focus metrics',
    'HIGH',
    0.0,
    CAST(bad_rows AS FLOAT64),
    CAST(bad_rows AS FLOAT64),
    IF(bad_rows = 0, 'PASS', 'FAIL'),
    IF(bad_rows = 0, '🟢', '🔴'),
    IF(bad_rows = 0, 'All identifiers valid for focus metrics.',
       'Null identifiers found in long weekly for focus metrics.'),
    IF(bad_rows = 0, 'No action required.',
       'Fix upstream mapping/build; identifiers must be populated.'),
    (bad_rows > 0),
    (bad_rows = 0),
    (bad_rows > 0)
  FROM bad;

  -- ===========================================================================
  -- TEST 3: QGP week validity (Saturday OR quarter-end)
  -- ===========================================================================
  INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_test_results`
  WITH mis AS (
    SELECT COUNT(1) AS misaligned_rows
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_campaign_weekly_long`
    WHERE qgp_week >= cutoff_anchor
      AND qgp_week <= max_allowed_qgp_week
      AND metric_name IN UNNEST(metric_focus)
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
    'sdi_gold_sa360_campaign_weekly_long',
    'critical',
    'QGP Week Validity (Saturday OR quarter-end) - long weekly focus metrics',
    'HIGH',
    0.0,
    CAST(misaligned_rows AS FLOAT64),
    CAST(misaligned_rows AS FLOAT64),
    IF(misaligned_rows = 0, 'PASS', 'FAIL'),
    IF(misaligned_rows = 0, '🟢', '🔴'),
    IF(misaligned_rows = 0,
      'All qgp_week values valid for focus metrics.',
      'Misaligned qgp_week found for focus metrics (not Saturday/quarter-end).'
    ),
    IF(misaligned_rows = 0,
      'No action required.',
      'Fix qgp_week derivation upstream and/or long weekly build.'
    ),
    (misaligned_rows > 0),
    (misaligned_rows = 0),
    (misaligned_rows > 0)
  FROM mis;

END;