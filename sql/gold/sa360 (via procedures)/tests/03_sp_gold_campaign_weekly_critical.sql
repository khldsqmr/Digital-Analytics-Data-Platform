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

  -- Duplicate grain (account_id, campaign_id, weekend_date)
  INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_test_results`
  WITH dup AS (
    SELECT COUNT(1) AS duplicate_groups
    FROM (
      SELECT account_id, campaign_id, weekend_date, COUNT(*) c
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
      'Duplicate keys found in Gold Weekly.'
    ),
    IF(duplicate_groups = 0,
      'No action required.',
      'Fix weekly MERGE logic (explicit insert list recommended; avoid INSERT ROW).'
    ),
    IF(duplicate_groups > 0, TRUE, FALSE),
    IF(duplicate_groups = 0, TRUE, FALSE),
    IF(duplicate_groups > 0, TRUE, FALSE)
  FROM dup;

END;
