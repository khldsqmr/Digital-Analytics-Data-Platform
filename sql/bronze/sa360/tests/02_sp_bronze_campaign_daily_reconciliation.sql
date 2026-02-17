/*
===============================================================================
FILE: 02_sp_bronze_campaign_daily_reconciliation.sql

PURPOSE:
  Reconcile Bronze Campaign Daily vs source (lookback window).
  Focus KPIs:
    - cart_start
    - postpaid_pspv

SOURCE:
  prj-dbi-prd-1.ds_dbi_improvado_master.google_search_ads_360_campaigns_tmo

TARGET:
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_campaign_daily

WINDOW:
  last N days (default 7)

TOLERANCE:
  We allow small differences using both:
    - absolute tolerance (abs_tol)
    - relative tolerance (rel_tol)

===============================================================================
*/

CREATE OR REPLACE PROCEDURE
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_bronze_campaign_daily_reconciliation`()
BEGIN

DECLARE v_lookback_days INT64 DEFAULT 7;
DECLARE v_window_start  DATE  DEFAULT DATE_SUB(CURRENT_DATE(), INTERVAL v_lookback_days DAY);

-- tolerance controls (tune as needed)
DECLARE abs_tol FLOAT64 DEFAULT 0.01;   -- absolute tolerance
DECLARE rel_tol FLOAT64 DEFAULT 0.001;  -- 0.1% relative tolerance

DECLARE v_expected FLOAT64;
DECLARE v_actual   FLOAT64;
DECLARE v_variance FLOAT64;
DECLARE v_status   STRING;
DECLARE v_reason   STRING;
DECLARE v_next     STRING;
DECLARE v_emoji    STRING;

-- =====================================================
-- TEST 1: Row Count Reconciliation (lookback window)
-- =====================================================
SET v_expected = (
  SELECT COUNT(*)
  FROM `prj-dbi-prd-1.ds_dbi_improvado_master.google_search_ads_360_campaigns_tmo` s
  WHERE SAFE.PARSE_DATE('%Y%m%d', CAST(s.date_yyyymmdd AS STRING)) >= v_window_start
);

SET v_actual = (
  SELECT COUNT(*)
  FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_campaign_daily` b
  WHERE b.date >= v_window_start
    AND b.date IS NOT NULL
);

SET v_variance = v_actual - v_expected;
SET v_status   = IF(v_variance = 0, 'PASS', 'FAIL');
SET v_emoji    = IF(v_status='PASS','🟢','🔴');

SET v_reason = IF(
  v_status='FAIL',
  CONCAT('Row count mismatch in last ', CAST(v_lookback_days AS STRING),
         ' days between source and bronze.'),
  'Row counts match source.'
);

SET v_next = IF(
  v_status='FAIL',
  'Inspect incremental MERGE filters (lookback window), dedup ordering, and source late-arrivals.',
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
  'reconciliation',
  CONCAT('Row Count Reconciliation (', CAST(v_lookback_days AS STRING), '-day)'),
  'MEDIUM',
  v_expected, v_actual, v_variance,
  v_status, v_emoji,
  v_reason, v_next,
  FALSE, (v_status='PASS'), (v_status='FAIL');

-- =====================================================
-- TEST 2: Cart Start Reconciliation (lookback window)
-- =====================================================
SET v_expected = (
  SELECT IFNULL(SUM(CAST(s.cart__start_ AS FLOAT64)), 0)
  FROM `prj-dbi-prd-1.ds_dbi_improvado_master.google_search_ads_360_campaigns_tmo` s
  WHERE SAFE.PARSE_DATE('%Y%m%d', CAST(s.date_yyyymmdd AS STRING)) >= v_window_start
);

SET v_actual = (
  SELECT IFNULL(SUM(CAST(b.cart_start AS FLOAT64)), 0)
  FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_campaign_daily` b
  WHERE b.date >= v_window_start
    AND b.date IS NOT NULL
);

SET v_variance = v_actual - v_expected;

-- pass if within abs_tol OR within rel_tol * expected
SET v_status = IF(
  ABS(v_variance) <= abs_tol
  OR ABS(v_variance) <= (rel_tol * NULLIF(ABS(v_expected), 0)),
  'PASS',
  'FAIL'
);

SET v_emoji = IF(v_status='PASS','🟢','🔴');

SET v_reason = IF(
  v_status='FAIL',
  CONCAT('Cart Start mismatch in last ', CAST(v_lookback_days AS STRING),
         ' days. Possible mapping/dedup/window issue.'),
  'Cart Start reconciliation successful.'
);

SET v_next = IF(
  v_status='FAIL',
  'Verify cart_start mapping (cart__start_) + dedup ordering; check for late-arriving source rows.',
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
  'reconciliation',
  CONCAT('Cart Start Reconciliation (', CAST(v_lookback_days AS STRING), '-day)'),
  'MEDIUM',
  v_expected, v_actual, v_variance,
  v_status, v_emoji,
  v_reason, v_next,
  FALSE, (v_status='PASS'), (v_status='FAIL');

-- =====================================================
-- TEST 3: Postpaid PSPV Reconciliation (lookback window)
--   NOTE:
--     In your raw columns, PSPV can appear as "postpaid_pspv" in Bronze.
--     In source it may be named differently; use the exact source column name.
--     If the source column is "postpaid__pspv" or similar, replace below.
-- =====================================================

-- >>> IMPORTANT: update the source column if needed <<<
-- I assumed it exists as s.postpaid_pspv (you may need s.postpaid__pspv).
SET v_expected = (
  SELECT IFNULL(SUM(CAST(s.postpaid_pspv_ AS FLOAT64)), 0)
  FROM `prj-dbi-prd-1.ds_dbi_improvado_master.google_search_ads_360_campaigns_tmo` s
  WHERE SAFE.PARSE_DATE('%Y%m%d', CAST(s.date_yyyymmdd AS STRING)) >= v_window_start
);

SET v_actual = (
  SELECT IFNULL(SUM(CAST(b.postpaid_pspv AS FLOAT64)), 0)
  FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_campaign_daily` b
  WHERE b.date >= v_window_start
    AND b.date IS NOT NULL
);

SET v_variance = v_actual - v_expected;

SET v_status = IF(
  ABS(v_variance) <= abs_tol
  OR ABS(v_variance) <= (rel_tol * NULLIF(ABS(v_expected), 0)),
  'PASS',
  'FAIL'
);

SET v_emoji = IF(v_status='PASS','🟢','🔴');

SET v_reason = IF(
  v_status='FAIL',
  CONCAT('Postpaid PSPV mismatch in last ', CAST(v_lookback_days AS STRING),
         ' days. Check source-to-bronze mapping and dedup/window logic.'),
  'Postpaid PSPV reconciliation successful.'
);

SET v_next = IF(
  v_status='FAIL',
  'Confirm the exact PSPV source column name + Bronze mapping; verify dedup ordering (latest file wins).',
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
  'reconciliation',
  CONCAT('Postpaid PSPV Reconciliation (', CAST(v_lookback_days AS STRING), '-day)'),
  'HIGH',
  v_expected, v_actual, v_variance,
  v_status, v_emoji,
  v_reason, v_next,
  FALSE, (v_status='PASS'), (v_status='FAIL');

END;
