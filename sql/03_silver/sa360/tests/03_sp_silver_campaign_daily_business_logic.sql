/*
===============================================================================
FILE: 03_sp_silver_campaign_daily_business_logic.sql
LAYER: Silver QA
TABLE: sdi_silver_sa360_campaign_daily

PURPOSE:
  Validate derived business logic + enrichments in Silver:
    - campaign_type completeness
    - campaign_name enrichment coverage (from entity join)
    - negative sanity checks (core + high-signal)
    - domain checks (lob, ad_platform)

===============================================================================
*/

CREATE OR REPLACE PROCEDURE
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_silver_campaign_daily_business_logic`()
BEGIN

DECLARE lookback_days INT64 DEFAULT 30;
DECLARE v_window_start DATE DEFAULT DATE_SUB(CURRENT_DATE(), INTERVAL lookback_days DAY);

DECLARE v_expected FLOAT64;
DECLARE v_actual   FLOAT64;
DECLARE v_variance FLOAT64;
DECLARE v_status   STRING;
DECLARE v_reason   STRING;
DECLARE v_next     STRING;
DECLARE v_emoji    STRING;

-- =====================================================
-- TEST 1 (LOW): campaign_type NULL check (prefer Unclassified)
-- =====================================================
SET v_expected = 0;

SET v_actual = (
  SELECT COUNT(*)
  FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_silver_sa360_campaign_daily`
  WHERE date >= v_window_start
    AND campaign_type IS NULL
);

SET v_variance = v_actual - v_expected;
SET v_status   = IF(v_actual = 0, 'PASS', 'FAIL');
SET v_emoji    = IF(v_status='PASS','🟢','🔴');

SET v_reason = IF(
  v_status='FAIL',
  'campaign_type contains NULLs. Silver should default to Unclassified when campaign_name is missing.',
  'campaign_type populated (or Unclassified) for all rows.'
);

SET v_next = IF(
  v_status='FAIL',
  'Update Silver CASE statement: ensure ELSE "Unclassified" and handle NULL campaign_name safely.',
  'No action required.'
);

INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_silver_sa360_test_results`
(
  test_run_timestamp, test_date,
  table_name, test_layer, test_name, severity_level,
  expected_value, actual_value, variance_value,
  status, status_emoji,
  failure_reason, next_step,
  is_critical_failure, is_pass, is_fail
)
SELECT
  CURRENT_TIMESTAMP(), CURRENT_DATE(),
  'sdi_silver_sa360_campaign_daily',
  'business_logic',
  CONCAT('campaign_type NULL Check (', CAST(lookback_days AS STRING), '-day)'),
  'LOW',
  v_expected, v_actual, v_variance,
  v_status, v_emoji,
  v_reason, v_next,
  FALSE, (v_status='PASS'), (v_status='FAIL');

-- =====================================================
-- TEST 2 (MEDIUM): campaign_name enrichment coverage
--   If entity join fails, campaign_name will be NULL widely.
--   Expected: <= 5% NULL rate (tune)
-- =====================================================
SET v_expected = 0.05;  -- allowed NULL rate (5%)

SET v_actual = (
  SELECT
    SAFE_DIVIDE(
      SUM(CASE WHEN campaign_name IS NULL OR TRIM(campaign_name) = '' THEN 1 ELSE 0 END),
      COUNT(*)
    )
  FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_silver_sa360_campaign_daily`
  WHERE date >= v_window_start
);

SET v_actual   = IFNULL(v_actual, 1.0);
SET v_variance = v_actual - v_expected;
SET v_status   = IF(v_actual <= v_expected, 'PASS', 'FAIL');
SET v_emoji    = IF(v_status='PASS','🟢','🔴');

SET v_reason = IF(
  v_status='FAIL',
  CONCAT('High campaign_name NULL rate in last ', CAST(lookback_days AS STRING),
         ' days: ', CAST(ROUND(v_actual*100, 2) AS STRING),
         '%. Threshold ≤ ', CAST(v_expected*100 AS STRING), '%.'),
  CONCAT('campaign_name coverage OK. NULL rate = ', CAST(ROUND(v_actual*100, 2) AS STRING), '%.')
);

SET v_next = IF(
  v_status='FAIL',
  'Check entity join keys + effective-date logic; ensure entity provides one best record per (account_id,campaign_id,date).',
  'No action required.'
);

INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_silver_sa360_test_results`
(
  test_run_timestamp, test_date,
  table_name, test_layer, test_name, severity_level,
  expected_value, actual_value, variance_value,
  status, status_emoji,
  failure_reason, next_step,
  is_critical_failure, is_pass, is_fail
)
SELECT
  CURRENT_TIMESTAMP(), CURRENT_DATE(),
  'sdi_silver_sa360_campaign_daily',
  'business_logic',
  CONCAT('campaign_name NULL Rate (', CAST(lookback_days AS STRING), '-day)'),
  'MEDIUM',
  v_expected, v_actual, v_variance,
  v_status, v_emoji,
  v_reason, v_next,
  FALSE, (v_status='PASS'), (v_status='FAIL');

-- =====================================================
-- TEST 3 (MEDIUM): Negative metrics sanity (core + high-signal)
-- =====================================================
SET v_expected = 0;

SET v_actual = (
  SELECT COUNT(*)
  FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_silver_sa360_campaign_daily`
  WHERE date >= v_window_start
    AND (
      clicks < 0 OR impressions < 0 OR cost < 0 OR all_conversions < 0
      OR CAST(cart_start AS FLOAT64) < 0
      OR CAST(postpaid_pspv AS FLOAT64) < 0
    )
);

SET v_variance = v_actual - v_expected;
SET v_status   = IF(v_actual = 0, 'PASS', 'FAIL');
SET v_emoji    = IF(v_status='PASS','🟢','🔴');

SET v_reason = IF(
  v_status='FAIL',
  'Negative values detected in core/high-signal metrics (clicks, impressions, cost, all_conversions, cart_start, postpaid_pspv).',
  'No negative values found in core/high-signal metrics.'
);

SET v_next = IF(
  v_status='FAIL',
  'Inspect Bronze ingestion casts + source anomalies. Ensure Silver doesn’t apply subtractive transformations.',
  'No action required.'
);

INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_silver_sa360_test_results`
(
  test_run_timestamp, test_date,
  table_name, test_layer, test_name, severity_level,
  expected_value, actual_value, variance_value,
  status, status_emoji,
  failure_reason, next_step,
  is_critical_failure, is_pass, is_fail
)
SELECT
  CURRENT_TIMESTAMP(), CURRENT_DATE(),
  'sdi_silver_sa360_campaign_daily',
  'business_logic',
  CONCAT('Negative Core + High-Signal Metrics (', CAST(lookback_days AS STRING), '-day)'),
  'MEDIUM',
  v_expected, v_actual, v_variance,
  v_status, v_emoji,
  v_reason, v_next,
  FALSE, (v_status='PASS'), (v_status='FAIL');

-- =====================================================
-- TEST 4 (LOW): LOB domain check
-- =====================================================
SET v_expected = 0;

SET v_actual = (
  SELECT COUNT(*)
  FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_silver_sa360_campaign_daily`
  WHERE date >= v_window_start
    AND lob NOT IN ('Postpaid','HSI','Fiber','Metro','TFB','Unclassified')
);

SET v_variance = v_actual - v_expected;
SET v_status   = IF(v_actual = 0, 'PASS', 'FAIL');
SET v_emoji    = IF(v_status='PASS','🟢','🔴');

SET v_reason = IF(
  v_status='FAIL',
  'lob contains unexpected values. account_name mapping may be incomplete or changed.',
  'lob values are within expected set.'
);

SET v_next = IF(
  v_status='FAIL',
  'Review account_name → lob mapping and add new variants.',
  'No action required.'
);

INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_silver_sa360_test_results`
(
  test_run_timestamp, test_date,
  table_name, test_layer, test_name, severity_level,
  expected_value, actual_value, variance_value,
  status, status_emoji,
  failure_reason, next_step,
  is_critical_failure, is_pass, is_fail
)
SELECT
  CURRENT_TIMESTAMP(), CURRENT_DATE(),
  'sdi_silver_sa360_campaign_daily',
  'business_logic',
  CONCAT('LOB Domain Check (', CAST(lookback_days AS STRING), '-day)'),
  'LOW',
  v_expected, v_actual, v_variance,
  v_status, v_emoji,
  v_reason, v_next,
  FALSE, (v_status='PASS'), (v_status='FAIL');

-- =====================================================
-- TEST 5 (LOW): Ad platform domain check
-- =====================================================
SET v_expected = 0;

SET v_actual = (
  SELECT COUNT(*)
  FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_silver_sa360_campaign_daily`
  WHERE date >= v_window_start
    AND ad_platform NOT IN ('Google','Bing','Unknown')
);

SET v_variance = v_actual - v_expected;
SET v_status   = IF(v_actual = 0, 'PASS', 'FAIL');
SET v_emoji    = IF(v_status='PASS','🟢','🔴');

SET v_reason = IF(
  v_status='FAIL',
  'ad_platform contains unexpected values. account_name parsing may be inconsistent.',
  'ad_platform values are within expected set.'
);

SET v_next = IF(
  v_status='FAIL',
  'Review ad_platform derivation CASE; handle all account_name patterns.',
  'No action required.'
);

INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_silver_sa360_test_results`
(
  test_run_timestamp, test_date,
  table_name, test_layer, test_name, severity_level,
  expected_value, actual_value, variance_value,
  status, status_emoji,
  failure_reason, next_step,
  is_critical_failure, is_pass, is_fail
)
SELECT
  CURRENT_TIMESTAMP(), CURRENT_DATE(),
  'sdi_silver_sa360_campaign_daily',
  'business_logic',
  CONCAT('Ad Platform Domain Check (', CAST(lookback_days AS STRING), '-day)'),
  'LOW',
  v_expected, v_actual, v_variance,
  v_status, v_emoji,
  v_reason, v_next,
  FALSE, (v_status='PASS'), (v_status='FAIL');

END;
