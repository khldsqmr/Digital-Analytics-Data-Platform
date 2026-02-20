/*
===============================================================================
FILE: 05_sp_bronze_weekly_deep_validation.sql

PURPOSE:
  Weekly deep validation (non-blocking, LOW severity heuristics):
    - Cart Start sanity + spike checks
    - PSPV sanity + spike checks
    - Partition spread / ingestion health

NOTES:
  - Keep this cheap: restrict to recent rolling window.
  - Thresholds are placeholders; tune using historical distributions.

===============================================================================
*/

CREATE OR REPLACE PROCEDURE
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_bronze_weekly_deep_validation`()
BEGIN

DECLARE v_recent_days  INT64 DEFAULT 30;
DECLARE v_window_start DATE  DEFAULT DATE_SUB(CURRENT_DATE(), INTERVAL v_recent_days DAY);

DECLARE v_expected FLOAT64;
DECLARE v_actual   FLOAT64;
DECLARE v_variance FLOAT64;
DECLARE v_status   STRING;
DECLARE v_reason   STRING;
DECLARE v_next     STRING;
DECLARE v_emoji    STRING;

-- =====================================================
-- TEST 1: Negative Cart Start Sanity (recent window)
-- =====================================================
SET v_expected = 0;

SET v_actual = (
  SELECT COUNT(*)
  FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_campaign_daily`
  WHERE date >= v_window_start
    AND date IS NOT NULL
    AND CAST(cart_start AS FLOAT64) < 0
);

SET v_variance = v_actual - v_expected;
SET v_status   = IF(v_actual = 0, 'PASS', 'FAIL');
SET v_emoji    = IF(v_status='PASS','🟢','🔴');

SET v_reason = IF(
  v_status='FAIL',
  CONCAT('Negative cart_start rows detected in last ', CAST(v_recent_days AS STRING), ' days.'),
  'No negative cart_start rows detected.'
);

SET v_next = IF(
  v_status='FAIL',
  'Inspect source mapping/casting for cart_start; check for upstream correction rows or bad coercion.',
  'No action required.'
);

INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_test_results`
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
  'sdi_bronze_sa360_campaign_daily',
  'deep_validation',
  CONCAT('Negative Cart Start Sanity (', CAST(v_recent_days AS STRING), '-day window)'),
  'LOW',
  v_expected, v_actual, v_variance,
  v_status, v_emoji,
  v_reason, v_next,
  FALSE, (v_status='PASS'), (v_status='FAIL');

-- =====================================================
-- TEST 2: Negative Postpaid PSPV Sanity (recent window)
-- =====================================================
SET v_expected = 0;

SET v_actual = (
  SELECT COUNT(*)
  FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_campaign_daily`
  WHERE date >= v_window_start
    AND date IS NOT NULL
    AND CAST(postpaid_pspv AS FLOAT64) < 0
);

SET v_variance = v_actual - v_expected;
SET v_status   = IF(v_actual = 0, 'PASS', 'FAIL');
SET v_emoji    = IF(v_status='PASS','🟢','🔴');

SET v_reason = IF(
  v_status='FAIL',
  CONCAT('Negative postpaid_pspv rows detected in last ', CAST(v_recent_days AS STRING), ' days.'),
  'No negative postpaid_pspv rows detected.'
);

SET v_next = IF(
  v_status='FAIL',
  'Inspect source mapping/casting for postpaid_pspv; check for upstream correction rows or schema drift.',
  'No action required.'
);

INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_test_results`
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
  'sdi_bronze_sa360_campaign_daily',
  'deep_validation',
  CONCAT('Negative Postpaid PSPV Sanity (', CAST(v_recent_days AS STRING), '-day window)'),
  'LOW',
  v_expected, v_actual, v_variance,
  v_status, v_emoji,
  v_reason, v_next,
  FALSE, (v_status='PASS'), (v_status='FAIL');

-- =====================================================
-- TEST 3: Extreme Daily Cart Start Spike (recent window)
-- =====================================================
-- Placeholder threshold: tune (e.g., P99 * 2) after you observe real values.
SET v_expected = 100000;  -- example threshold

SET v_actual = (
  SELECT MAX(CAST(cart_start AS FLOAT64))
  FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_campaign_daily`
  WHERE date >= v_window_start
    AND date IS NOT NULL
);

SET v_actual   = IFNULL(v_actual, 0);
SET v_variance = v_actual - v_expected;
SET v_status   = IF(v_actual <= v_expected, 'PASS', 'FAIL');
SET v_emoji    = IF(v_status='PASS','🟢','🔴');

SET v_reason = IF(
  v_status='FAIL',
  CONCAT('Unusually high cart_start detected in last ', CAST(v_recent_days AS STRING),
         ' days. Possible duplication or ingestion anomaly.'),
  'Max daily cart_start within expected threshold.'
);

SET v_next = IF(
  v_status='FAIL',
  'Inspect recent loads and dedup ordering; validate cart_start mapping and late-arrival handling.',
  'No action required.'
);

INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_test_results`
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
  'sdi_bronze_sa360_campaign_daily',
  'deep_validation',
  CONCAT('Extreme Daily Cart Start Spike (', CAST(v_recent_days AS STRING), '-day window)'),
  'LOW',
  v_expected, v_actual, v_variance,
  v_status, v_emoji,
  v_reason, v_next,
  FALSE, (v_status='PASS'), (v_status='FAIL');

-- =====================================================
-- TEST 4: Extreme Daily Postpaid PSPV Spike (recent window)
-- =====================================================
SET v_expected = 100000;  -- example threshold

SET v_actual = (
  SELECT MAX(CAST(postpaid_pspv AS FLOAT64))
  FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_campaign_daily`
  WHERE date >= v_window_start
    AND date IS NOT NULL
);

SET v_actual   = IFNULL(v_actual, 0);
SET v_variance = v_actual - v_expected;
SET v_status   = IF(v_actual <= v_expected, 'PASS', 'FAIL');
SET v_emoji    = IF(v_status='PASS','🟢','🔴');

SET v_reason = IF(
  v_status='FAIL',
  CONCAT('Unusually high postpaid_pspv detected in last ', CAST(v_recent_days AS STRING),
         ' days. Possible duplication or ingestion anomaly.'),
  'Max daily postpaid_pspv within expected threshold.'
);

SET v_next = IF(
  v_status='FAIL',
  'Inspect recent loads and dedup ordering; validate postpaid_pspv mapping and late-arrival handling.',
  'No action required.'
);

INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_test_results`
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
  'sdi_bronze_sa360_campaign_daily',
  'deep_validation',
  CONCAT('Extreme Daily Postpaid PSPV Spike (', CAST(v_recent_days AS STRING), '-day window)'),
  'LOW',
  v_expected, v_actual, v_variance,
  v_status, v_emoji,
  v_reason, v_next,
  FALSE, (v_status='PASS'), (v_status='FAIL');

-- =====================================================
-- TEST 5: Recent Partition Spread Check
-- =====================================================
-- Expect at least 7 distinct dates in last 30 days (tune if needed)
SET v_expected = 7;

SET v_actual = (
  SELECT COUNT(DISTINCT date)
  FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_campaign_daily`
  WHERE date >= v_window_start
    AND date IS NOT NULL
);

SET v_variance = v_actual - v_expected;
SET v_status   = IF(v_actual >= v_expected, 'PASS', 'FAIL');
SET v_emoji    = IF(v_status='PASS','🟢','🔴');

SET v_reason = IF(
  v_status='FAIL',
  CONCAT('Low date coverage: only ', CAST(v_actual AS STRING),
         ' distinct dates found in last ', CAST(v_recent_days AS STRING), ' days.'),
  CONCAT('Date coverage OK: ', CAST(v_actual AS STRING),
         ' distinct dates in last ', CAST(v_recent_days AS STRING), ' days.')
);

SET v_next = IF(
  v_status='FAIL',
  'Check scheduler cadence, upstream landing freshness, and incremental lookback logic.',
  'No action required.'
);

INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_test_results`
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
  'sdi_bronze_sa360_campaign_daily',
  'deep_validation',
  CONCAT('Recent Partition Spread (', CAST(v_recent_days AS STRING), '-day window)'),
  'LOW',
  v_expected, v_actual, v_variance,
  v_status, v_emoji,
  v_reason, v_next,
  FALSE, (v_status='PASS'), (v_status='FAIL');

END;
