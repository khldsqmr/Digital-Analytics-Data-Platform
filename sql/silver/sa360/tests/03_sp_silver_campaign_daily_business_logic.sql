/*
===============================================================================
FILE: 03_sp_silver_campaign_daily_business_logic.sql
LAYER: Silver QA
TABLE: sdi_silver_sa360_campaign_daily

PURPOSE:
  Validate derived business logic + classification fields in Silver:
    - lob and ad_platform derived from account_name
    - campaign_type derived from campaign_name
    - sanity checks for negatives

NOTES:
  These are LOW/MEDIUM severity (non-blocking), except where you want to enforce.
===============================================================================
*/

CREATE OR REPLACE PROCEDURE
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_silver_campaign_daily_business_logic`()
BEGIN

DECLARE lookback_days INT64 DEFAULT 30;

DECLARE v_expected FLOAT64 DEFAULT 0;
DECLARE v_actual FLOAT64 DEFAULT 0;
DECLARE v_variance FLOAT64 DEFAULT 0;
DECLARE v_status STRING DEFAULT 'PASS';
DECLARE v_reason STRING DEFAULT '';
DECLARE v_next STRING DEFAULT '';

-- =====================================================
-- TEST 1: campaign_type NULL check (should never be NULL; use Unclassified)
-- =====================================================
SET v_expected = 0;

SET v_actual = (
  SELECT COUNT(*)
  FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_silver_sa360_campaign_daily`
  WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL lookback_days DAY)
    AND campaign_type IS NULL
);

SET v_variance = v_actual - v_expected;
SET v_status = IF(v_actual = 0, 'PASS', 'FAIL');

SET v_reason = IF(
  v_status='FAIL',
  'campaign_type contains NULLs. Silver should default to Unclassified when campaign_name missing.',
  'campaign_type populated (or Unclassified) for all rows.'
);

SET v_next = IF(
  v_status='FAIL',
  'Update Silver campaign_type CASE to ensure ELSE Unclassified and handle NULL campaign_name.',
  'No action required.'
);

INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_silver_sa360_test_results`
VALUES (
  CURRENT_TIMESTAMP(), CURRENT_DATE(),
  'sdi_silver_sa360_campaign_daily',
  'business_logic',
  CONCAT('campaign_type NULL Check (', CAST(lookback_days AS STRING), '-day)'),
  'LOW',
  v_expected, v_actual, v_variance,
  v_status, IF(v_status='PASS','🟢','🔴'),
  v_reason, v_next,
  FALSE,
  (v_status='PASS'),
  (v_status='FAIL')
);

-- =====================================================
-- TEST 2: Negative metric detection (core metrics)
-- =====================================================
SET v_expected = 0;

SET v_actual = (
  SELECT COUNT(*)
  FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_silver_sa360_campaign_daily`
  WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL lookback_days DAY)
    AND (
      clicks < 0 OR impressions < 0 OR cost < 0 OR all_conversions < 0
    )
);

SET v_variance = v_actual - v_expected;
SET v_status = IF(v_actual = 0, 'PASS', 'FAIL');

SET v_reason = IF(
  v_status='FAIL',
  'Negative values detected in core metrics (clicks/impressions/cost/all_conversions).',
  'No negative values found in core metrics.'
);

SET v_next = IF(
  v_status='FAIL',
  'Inspect Bronze ingestion (casts) and upstream source anomalies.',
  'No action required.'
);

INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_silver_sa360_test_results`
VALUES (
  CURRENT_TIMESTAMP(), CURRENT_DATE(),
  'sdi_silver_sa360_campaign_daily',
  'business_logic',
  CONCAT('Negative Core Metrics (', CAST(lookback_days AS STRING), '-day)'),
  'MEDIUM',
  v_expected, v_actual, v_variance,
  v_status, IF(v_status='PASS','🟢','🔴'),
  v_reason, v_next,
  FALSE,
  (v_status='PASS'),
  (v_status='FAIL')
);

-- =====================================================
-- TEST 3: LOB mapping validity (must be one of known values)
-- =====================================================
SET v_expected = 0;

SET v_actual = (
  SELECT COUNT(*)
  FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_silver_sa360_campaign_daily`
  WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL lookback_days DAY)
    AND lob NOT IN ('Postpaid','HSI','Fiber','Metro','TFB','Unclassified')
);

SET v_variance = v_actual - v_expected;
SET v_status = IF(v_actual = 0, 'PASS', 'FAIL');

SET v_reason = IF(
  v_status='FAIL',
  'lob contains unexpected values. Mapping from account_name may be incomplete or account names changed.',
  'lob values are within expected set.'
);

SET v_next = IF(
  v_status='FAIL',
  'Review account_name → lob mapping. Add any new account_name variants.',
  'No action required.'
);

INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_silver_sa360_test_results`
VALUES (
  CURRENT_TIMESTAMP(), CURRENT_DATE(),
  'sdi_silver_sa360_campaign_daily',
  'business_logic',
  CONCAT('LOB Domain Check (', CAST(lookback_days AS STRING), '-day)'),
  'LOW',
  v_expected, v_actual, v_variance,
  v_status, IF(v_status='PASS','🟢','🔴'),
  v_reason, v_next,
  FALSE,
  (v_status='PASS'),
  (v_status='FAIL')
);

-- =====================================================
-- TEST 4: Ad platform mapping validity
-- =====================================================
SET v_expected = 0;

SET v_actual = (
  SELECT COUNT(*)
  FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_silver_sa360_campaign_daily`
  WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL lookback_days DAY)
    AND ad_platform NOT IN ('Google','Bing','Unknown')
);

SET v_variance = v_actual - v_expected;
SET v_status = IF(v_actual = 0, 'PASS', 'FAIL');

SET v_reason = IF(
  v_status='FAIL',
  'ad_platform contains unexpected values. account_name parsing may be inconsistent.',
  'ad_platform values are within expected set.'
);

SET v_next = IF(
  v_status='FAIL',
  'Review ad_platform derivation CASE; ensure it handles all account_name patterns.',
  'No action required.'
);

INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_silver_sa360_test_results`
VALUES (
  CURRENT_TIMESTAMP(), CURRENT_DATE(),
  'sdi_silver_sa360_campaign_daily',
  'business_logic',
  CONCAT('Ad Platform Domain Check (', CAST(lookback_days AS STRING), '-day)'),
  'LOW',
  v_expected, v_actual, v_variance,
  v_status, IF(v_status='PASS','🟢','🔴'),
  v_reason, v_next,
  FALSE,
  (v_status='PASS'),
  (v_status='FAIL')
);

END;
