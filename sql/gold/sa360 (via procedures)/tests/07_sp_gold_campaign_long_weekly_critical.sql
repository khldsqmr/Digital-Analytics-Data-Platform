/*
===============================================================================
FILE: 07_sp_gold_campaign_long_weekly_critical.sql
PROC:  sp_gold_sa360_campaign_long_weekly_critical_tests
TABLE: sdi_gold_sa360_campaign_weekly_long
GRAIN: (account_id, campaign_id, qgp_week, metric_name)
===============================================================================
*/

CREATE OR REPLACE PROCEDURE
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_gold_sa360_campaign_long_weekly_critical_tests`()
OPTIONS(strict_mode=false)
BEGIN
  DECLARE lookback_weeks INT64 DEFAULT 12;

  DECLARE cutoff_anchor DATE DEFAULT DATE_TRUNC(
    DATE_SUB(CURRENT_DATE(), INTERVAL lookback_weeks WEEK),
    WEEK(SATURDAY)
  );

  -- TEST 1: Duplicate grain
  INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_test_results`
  WITH dup AS (
    SELECT COUNT(1) AS duplicate_groups
    FROM (
      SELECT account_id, campaign_id, qgp_week, metric_name, COUNT(*) c
      FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_campaign_weekly_long`
      WHERE qgp_week >= cutoff_anchor
      GROUP BY 1,2,3,4
      HAVING COUNT(*) > 1
    )
  )
  SELECT
    CURRENT_TIMESTAMP(), CURRENT_DATE(),
    'sdi_gold_sa360_campaign_weekly_long',
    'critical',
    'Duplicate Grain Check (acct,campaign,qgp_week,metric_name)',
    'HIGH',
    0.0,
    CAST(duplicate_groups AS FLOAT64),
    CAST(duplicate_groups AS FLOAT64),
    IF(duplicate_groups = 0, 'PASS', 'FAIL'),
    IF(duplicate_groups = 0, '🟢', '🔴'),
    IF(duplicate_groups = 0, 'No duplicate long-weekly grain detected.', 'Duplicate keys found in Gold long weekly.'),
    IF(duplicate_groups = 0, 'No action required.', 'Fix long weekly build/unpivot; ensure uniqueness.'),
    IF(duplicate_groups > 0, TRUE, FALSE),
    IF(duplicate_groups = 0, TRUE, FALSE),
    IF(duplicate_groups > 0, TRUE, FALSE)
  FROM dup;

  -- TEST 2: Null identifiers
  INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_test_results`
  WITH bad AS (
    SELECT COUNT(1) AS bad_rows
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_campaign_weekly_long`
    WHERE qgp_week >= cutoff_anchor
      AND (
        account_id IS NULL OR campaign_id IS NULL OR qgp_week IS NULL OR metric_name IS NULL
      )
  )
  SELECT
    CURRENT_TIMESTAMP(), CURRENT_DATE(),
    'sdi_gold_sa360_campaign_weekly_long',
    'critical',
    'Null Identifier Check (acct,campaign,qgp_week,metric_name)',
    'HIGH',
    0.0,
    CAST(bad_rows AS FLOAT64),
    CAST(bad_rows AS FLOAT64),
    IF(bad_rows = 0, 'PASS', 'FAIL'),
    IF(bad_rows = 0, '🟢', '🔴'),
    IF(bad_rows = 0, 'All long-weekly identifiers are valid.', 'Null identifier(s) found in Gold long weekly.'),
    IF(bad_rows = 0, 'No action required.', 'Fix upstream mapping/build; identifiers must be populated.'),
    IF(bad_rows > 0, TRUE, FALSE),
    IF(bad_rows = 0, TRUE, FALSE),
    IF(bad_rows > 0, TRUE, FALSE)
  FROM bad;

END;
