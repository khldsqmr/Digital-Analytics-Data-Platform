/*
===============================================================================
FILE: 03_sp_silver_campaign_daily_business_logic.sql
LAYER: Silver | QA
PROC:  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_silver_sa360_campaign_daily_business_logic_tests

WHY:
  Silver introduces derived business fields:
    - lob
    - ad_platform
    - campaign_type
  We validate these derivations are consistent with your intended rules.

NOTE:
  These are HIGH if they break reporting segmentation.
===============================================================================
*/

CREATE OR REPLACE PROCEDURE
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_silver_sa360_campaign_daily_business_logic_tests`()
OPTIONS(strict_mode=false)
BEGIN
  DECLARE lookback_days INT64 DEFAULT 14;

  -- TEST 1: lob mapping correctness for known account_name values
  INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_silver_sa360_test_results`
  WITH bad AS (
    SELECT COUNT(1) AS bad_rows
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_silver_sa360_campaign_daily`
    WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL lookback_days DAY)
      AND (
        (account_name IN ('Postpaid Google','Postpaid Bing','BTS Google','BTS Bing') AND lob != 'Postpaid')
        OR (account_name IN ('Broadband Google','Broadband Bing') AND lob != 'HSI')
        OR (account_name IN ('Fiber Google','Fiber Bing') AND lob != 'Fiber')
        OR (account_name IN ('Metro Google','Metro Bing') AND lob != 'Metro')
        OR (account_name IN ('TFB Google','TFB Bing') AND lob != 'TFB')
      )
  )
  SELECT
    CURRENT_TIMESTAMP(), CURRENT_DATE(),
    'sdi_silver_sa360_campaign_daily',
    'business_logic',
    'LOB Derivation Check (known account_name mapping)',
    'HIGH',
    0.0,
    CAST(bad_rows AS FLOAT64),
    CAST(bad_rows AS FLOAT64),
    IF(bad_rows = 0, 'PASS', 'FAIL'),
    IF(bad_rows = 0, '🟢', '🔴'),
    IF(bad_rows = 0,
      'LOB mapping matches expected rules.',
      'LOB mapping mismatch found for known account_name values.'
    ),
    IF(bad_rows = 0,
      'No action required.',
      'Fix LOB CASE logic or normalize account_name values before mapping.'
    ),
    IF(bad_rows > 0, TRUE, FALSE),
    IF(bad_rows = 0, TRUE, FALSE),
    IF(bad_rows > 0, TRUE, FALSE)
  FROM bad;

  -- TEST 2: ad_platform should be Google/Bing/Unknown and consistent with account_name
  INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_silver_sa360_test_results`
  WITH bad AS (
    SELECT COUNT(1) AS bad_rows
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_silver_sa360_campaign_daily`
    WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL lookback_days DAY)
      AND (
        ad_platform NOT IN ('Google','Bing','Unknown')
        OR (LOWER(account_name) LIKE '%google%' AND ad_platform != 'Google')
        OR (LOWER(account_name) LIKE '%bing%' AND ad_platform != 'Bing')
      )
  )
  SELECT
    CURRENT_TIMESTAMP(), CURRENT_DATE(),
    'sdi_silver_sa360_campaign_daily',
    'business_logic',
    'Ad Platform Derivation Check (account_name contains google/bing)',
    'MEDIUM',
    0.0,
    CAST(bad_rows AS FLOAT64),
    CAST(bad_rows AS FLOAT64),
    IF(bad_rows = 0, 'PASS', 'FAIL'),
    IF(bad_rows = 0, '🟢', '🔴'),
    IF(bad_rows = 0,
      'Ad platform derivation consistent.',
      'Ad platform derivation mismatch detected.'
    ),
    IF(bad_rows = 0,
      'No action required.',
      'Fix derivation CASE logic; normalize account_name strings.'
    ),
    FALSE,
    IF(bad_rows = 0, TRUE, FALSE),
    IF(bad_rows > 0, TRUE, FALSE)
  FROM bad;

END;
