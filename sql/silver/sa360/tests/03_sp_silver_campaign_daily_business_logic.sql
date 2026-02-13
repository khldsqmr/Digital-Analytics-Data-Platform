/*
===============================================================================
FILE: 03_sp_silver_campaign_daily_business_logic.sql

PURPOSE:
  Validate business logic and classification correctness in Silver.

These tests are LOW/MEDIUM severity.
===============================================================================
*/

CREATE OR REPLACE PROCEDURE
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_silver_campaign_daily_business_logic`()
BEGIN

DECLARE v_expected FLOAT64;
DECLARE v_actual FLOAT64;
DECLARE v_variance FLOAT64;
DECLARE v_status STRING;
DECLARE v_reason STRING;
DECLARE v_next STRING;

-- =====================================================
-- TEST 1: Campaign Type Null Check
-- =====================================================

SET v_expected = 0;

SET v_actual = (
  SELECT COUNT(*)
  FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_silver_sa360_campaign_daily`
  WHERE campaign_type IS NULL
);

SET v_variance = v_actual - v_expected;
SET v_status = IF(v_actual = 0, 'PASS', 'FAIL');

SET v_reason = IF(
  v_status='FAIL',
  'Derived campaign_type contains NULL values.',
  'All campaign_type values populated.'
);

SET v_next = IF(
  v_status='FAIL',
  'Inspect CASE classification logic.',
  'No action required.'
);

INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_silver_sa360_test_results`
VALUES (
  CURRENT_TIMESTAMP(),
  CURRENT_DATE(),
  'sdi_silver_sa360_campaign_daily',
  'business_logic',
  'Campaign Type Null Check',
  'LOW',
  v_expected,
  v_actual,
  v_variance,
  v_status,
  IF(v_status='PASS','🟢','🔴'),
  v_reason,
  v_next,
  FALSE,
  (v_status='PASS'),
  (v_status='FAIL')
);

-- =====================================================
-- TEST 2: Negative Metric Detection
-- =====================================================

SET v_expected = 0;

SET v_actual = (
  SELECT COUNT(*)
  FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_silver_sa360_campaign_daily`
  WHERE clicks < 0
     OR impressions < 0
     OR cost < 0
);

SET v_variance = v_actual - v_expected;
SET v_status = IF(v_actual = 0, 'PASS', 'FAIL');

SET v_reason = IF(
  v_status='FAIL',
  'Negative metric values detected.',
  'No negative metric values found.'
);

SET v_next = IF(
  v_status='FAIL',
  'Inspect upstream Bronze data.',
  'No action required.'
);

INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_silver_sa360_test_results`
VALUES (
  CURRENT_TIMESTAMP(),
  CURRENT_DATE(),
  'sdi_silver_sa360_campaign_daily',
  'business_logic',
  'Negative Metric Detection',
  'MEDIUM',
  v_expected,
  v_actual,
  v_variance,
  v_status,
  IF(v_status='PASS','🟢','🔴'),
  v_reason,
  v_next,
  FALSE,
  (v_status='PASS'),
  (v_status='FAIL')
);

END;
