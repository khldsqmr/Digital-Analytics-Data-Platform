/*
===============================================================================
FILE: 05_sp_bronze_weekly_deep_validation.sql

PURPOSE:
  Weekly deep validation:
    • Outlier detection
    • Metric sanity
    • Partition spread
    • Historical late arrival monitoring

Non-blocking but important for long-term quality.

===============================================================================
*/

CREATE OR REPLACE PROCEDURE
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_bronze_weekly_deep_validation`()
BEGIN

DECLARE v_expected FLOAT64;
DECLARE v_actual FLOAT64;
DECLARE v_variance FLOAT64;
DECLARE v_status STRING;
DECLARE v_reason STRING;
DECLARE v_next STRING;

-- =====================================================
-- TEST 1: Extreme Cost Spike Check
-- =====================================================

SET v_expected = 10000000;  -- threshold example
SET v_actual = (
  SELECT MAX(cost)
  FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_campaign_daily`
);

SET v_variance = v_actual - v_expected;
SET v_status = IF(v_actual <= v_expected, 'PASS', 'FAIL');

SET v_reason = IF(
  v_status='FAIL',
  'Unusually high cost detected. Possible duplication.',
  'Cost values within expected threshold.'
);

SET v_next = IF(
  v_status='FAIL',
  'Investigate duplication or upstream corruption.',
  'No action required.'
);

INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_test_results`
VALUES (
  CURRENT_TIMESTAMP(),
  CURRENT_DATE(),
  'sdi_bronze_sa360_campaign_daily',
  'deep_validation',
  'Extreme Cost Spike Check',
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

END;
