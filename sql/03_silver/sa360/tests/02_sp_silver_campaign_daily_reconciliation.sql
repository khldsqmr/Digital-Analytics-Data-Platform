/*
===============================================================================
FILE: 02_sp_silver_campaign_daily_reconciliation.sql
LAYER: Silver QA
TABLE: sdi_silver_sa360_campaign_daily

PURPOSE:
  Inter-layer reconciliation: Silver Campaign Daily vs Bronze Campaign Daily.
  Focus: row coverage + key coverage + high-signal metric totals (cart_start + PSPV).

WINDOW:
  last N days (default 7)

NOTES:
  - Silver enrichments (entity join for campaign_name) must NOT change row-level coverage.
  - Use small tolerances for FLOAT sums.

===============================================================================
*/

CREATE OR REPLACE PROCEDURE
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_silver_campaign_daily_reconciliation`()
BEGIN

DECLARE lookback_days INT64 DEFAULT 7;
DECLARE v_window_start DATE DEFAULT DATE_SUB(CURRENT_DATE(), INTERVAL lookback_days DAY);

DECLARE v_expected FLOAT64;
DECLARE v_actual   FLOAT64;
DECLARE v_variance FLOAT64;
DECLARE v_status   STRING;
DECLARE v_reason   STRING;
DECLARE v_next     STRING;
DECLARE v_emoji    STRING;

-- =====================================================
-- TEST 1: Row Count Match (recent window)
-- =====================================================
SET v_expected = (
  SELECT COUNT(*)
  FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_campaign_daily`
  WHERE date >= v_window_start
);

SET v_actual = (
  SELECT COUNT(*)
  FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_silver_sa360_campaign_daily`
  WHERE date >= v_window_start
);

SET v_variance = v_actual - v_expected;
SET v_status   = IF(v_variance = 0, 'PASS', 'FAIL');
SET v_emoji    = IF(v_status='PASS','🟢','🔴');

SET v_reason = IF(
  v_status='FAIL',
  CONCAT('Row count mismatch Bronze vs Silver in last ', CAST(lookback_days AS STRING), ' days.'),
  'Row counts match Bronze.'
);

SET v_next = IF(
  v_status='FAIL',
  'Check Silver join logic + filters. Silver should not drop/add rows relative to Bronze grain.',
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
  'reconciliation',
  CONCAT('Row Count Reconciliation (', CAST(lookback_days AS STRING), '-day)'),
  'MEDIUM',
  v_expected, v_actual, v_variance,
  v_status, v_emoji,
  v_reason, v_next,
  FALSE, (v_status='PASS'), (v_status='FAIL');

-- =====================================================
-- TEST 2 (HIGH): Bronze keys missing in Silver (anti-join)
-- =====================================================
SET v_expected = 0;

SET v_actual = (
  SELECT COUNT(*)
  FROM (
    SELECT b.account_id, b.campaign_id, b.date
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_campaign_daily` b
    WHERE b.date >= v_window_start
    EXCEPT DISTINCT
    SELECT s.account_id, s.campaign_id, s.date
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_silver_sa360_campaign_daily` s
    WHERE s.date >= v_window_start
  )
);

SET v_variance = v_actual - v_expected;
SET v_status   = IF(v_actual = 0, 'PASS', 'FAIL');
SET v_emoji    = IF(v_status='PASS','🟢','🔴');

SET v_reason = IF(
  v_status='FAIL',
  CONCAT('Silver is missing ', CAST(v_actual AS STRING), ' Bronze key(s) (recent window).'),
  'All Bronze keys are present in Silver (recent window).'
);

SET v_next = IF(
  v_status='FAIL',
  'Inspect Silver MERGE filters/window. Ensure join does not drop rows and merge ON clause matches grain.',
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
  'reconciliation',
  CONCAT('Key Coverage: Bronze minus Silver (', CAST(lookback_days AS STRING), '-day)'),
  'HIGH',
  v_expected, v_actual, v_variance,
  v_status, v_emoji,
  v_reason, v_next,
  (v_status='FAIL'), (v_status='PASS'), (v_status='FAIL');

-- =====================================================
-- TEST 3 (HIGH): Silver keys missing in Bronze (should be 0)
--   If Silver has keys Bronze doesn't, something is wrong (timing or filters).
-- =====================================================
SET v_expected = 0;

SET v_actual = (
  SELECT COUNT(*)
  FROM (
    SELECT s.account_id, s.campaign_id, s.date
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_silver_sa360_campaign_daily` s
    WHERE s.date >= v_window_start
    EXCEPT DISTINCT
    SELECT b.account_id, b.campaign_id, b.date
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_campaign_daily` b
    WHERE b.date >= v_window_start
  )
);

SET v_variance = v_actual - v_expected;
SET v_status   = IF(v_actual = 0, 'PASS', 'FAIL');
SET v_emoji    = IF(v_status='PASS','🟢','🔴');

SET v_reason = IF(
  v_status='FAIL',
  CONCAT('Silver contains ', CAST(v_actual AS STRING), ' key(s) not present in Bronze (recent window).'),
  'Silver keys are fully explainable by Bronze keys (recent window).'
);

SET v_next = IF(
  v_status='FAIL',
  'Confirm orchestration order (Bronze before Silver). Verify Silver is not unioning extra sources or using different date logic.',
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
  'reconciliation',
  CONCAT('Key Coverage: Silver minus Bronze (', CAST(lookback_days AS STRING), '-day)'),
  'HIGH',
  v_expected, v_actual, v_variance,
  v_status, v_emoji,
  v_reason, v_next,
  (v_status='FAIL'), (v_status='PASS'), (v_status='FAIL');

-- =====================================================
-- TEST 4 (MEDIUM): cart_start total match (tolerance)
-- =====================================================
SET v_expected = (
  SELECT IFNULL(SUM(CAST(cart_start AS FLOAT64)), 0)
  FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_campaign_daily`
  WHERE date >= v_window_start
);

SET v_actual = (
  SELECT IFNULL(SUM(CAST(cart_start AS FLOAT64)), 0)
  FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_silver_sa360_campaign_daily`
  WHERE date >= v_window_start
);

SET v_variance = v_actual - v_expected;
SET v_status   = IF(ABS(v_variance) < 0.0001, 'PASS', 'FAIL');
SET v_emoji    = IF(v_status='PASS','🟢','🔴');

SET v_reason = IF(
  v_status='FAIL',
  CONCAT('cart_start mismatch Bronze vs Silver. Variance=', CAST(v_variance AS STRING)),
  'cart_start totals match.'
);

SET v_next = IF(
  v_status='FAIL',
  'Check Silver column mapping for cart_start and ensure no aggregations occur in Silver.',
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
  'reconciliation',
  CONCAT('cart_start Total Reconciliation (', CAST(lookback_days AS STRING), '-day)'),
  'MEDIUM',
  v_expected, v_actual, v_variance,
  v_status, v_emoji,
  v_reason, v_next,
  FALSE, (v_status='PASS'), (v_status='FAIL');

-- =====================================================
-- TEST 5 (MEDIUM): postpaid_pspv total match (tolerance)
-- =====================================================
SET v_expected = (
  SELECT IFNULL(SUM(CAST(postpaid_pspv AS FLOAT64)), 0)
  FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_campaign_daily`
  WHERE date >= v_window_start
);

SET v_actual = (
  SELECT IFNULL(SUM(CAST(postpaid_pspv AS FLOAT64)), 0)
  FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_silver_sa360_campaign_daily`
  WHERE date >= v_window_start
);

SET v_variance = v_actual - v_expected;
SET v_status   = IF(ABS(v_variance) < 0.0001, 'PASS', 'FAIL');
SET v_emoji    = IF(v_status='PASS','🟢','🔴');

SET v_reason = IF(
  v_status='FAIL',
  CONCAT('postpaid_pspv mismatch Bronze vs Silver. Variance=', CAST(v_variance AS STRING)),
  'postpaid_pspv totals match.'
);

SET v_next = IF(
  v_status='FAIL',
  'Check Silver column mapping for postpaid_pspv and ensure no aggregations occur in Silver.',
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
  'reconciliation',
  CONCAT('postpaid_pspv Total Reconciliation (', CAST(lookback_days AS STRING), '-day)'),
  'MEDIUM',
  v_expected, v_actual, v_variance,
  v_status, v_emoji,
  v_reason, v_next,
  FALSE, (v_status='PASS'), (v_status='FAIL');

-- =====================================================
-- TEST 6 (LOW, optional): cost total match (keep or delete)
-- =====================================================
SET v_expected = (
  SELECT IFNULL(SUM(CAST(cost AS FLOAT64)), 0)
  FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_campaign_daily`
  WHERE date >= v_window_start
);

SET v_actual = (
  SELECT IFNULL(SUM(CAST(cost AS FLOAT64)), 0)
  FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_silver_sa360_campaign_daily`
  WHERE date >= v_window_start
);

SET v_variance = v_actual - v_expected;
SET v_status   = IF(ABS(v_variance) < 0.01, 'PASS', 'FAIL');
SET v_emoji    = IF(v_status='PASS','🟢','🔴');

SET v_reason = IF(
  v_status='FAIL',
  CONCAT('cost mismatch Bronze vs Silver. Variance=', CAST(v_variance AS STRING)),
  'cost totals match.'
);

SET v_next = IF(
  v_status='FAIL',
  'Verify Silver is not re-deriving cost differently than Bronze.',
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
  'reconciliation',
  CONCAT('cost Total Reconciliation (', CAST(lookback_days AS STRING), '-day)'),
  'LOW',
  v_expected, v_actual, v_variance,
  v_status, v_emoji,
  v_reason, v_next,
  FALSE, (v_status='PASS'), (v_status='FAIL');

END;
