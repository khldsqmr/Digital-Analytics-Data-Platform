/*
===============================================================================
FILE: 02_sp_silver_campaign_daily_reconciliation.sql
LAYER: Silver QA
TABLE: sdi_silver_sa360_campaign_daily

PURPOSE:
  Cross-table reconciliation: Silver Campaign Daily vs Bronze Campaign Daily.
  Focus: row coverage + metric totals.

WINDOW:
  last 7 days (changeable by lookback_days)

GRAIN:
  account_id + campaign_id + date

NOTES:
  - Silver may LEFT JOIN entity; that must NOT change row counts vs Bronze Daily.
  - Use small tolerances on floating sums to avoid float noise.
===============================================================================
*/

CREATE OR REPLACE PROCEDURE
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_silver_campaign_daily_reconciliation`()
BEGIN

DECLARE lookback_days INT64 DEFAULT 7;

DECLARE v_expected FLOAT64 DEFAULT 0;
DECLARE v_actual FLOAT64 DEFAULT 0;
DECLARE v_variance FLOAT64 DEFAULT 0;
DECLARE v_status STRING DEFAULT 'PASS';
DECLARE v_reason STRING DEFAULT '';
DECLARE v_next STRING DEFAULT '';

-- =====================================================
-- TEST 1: Row Count Match (lookback window)
-- =====================================================
SET v_expected = (
  SELECT COUNT(*)
  FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_campaign_daily`
  WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL lookback_days DAY)
);

SET v_actual = (
  SELECT COUNT(*)
  FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_silver_sa360_campaign_daily`
  WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL lookback_days DAY)
);

SET v_variance = v_actual - v_expected;
SET v_status = IF(v_variance = 0, 'PASS', 'FAIL');

SET v_reason = IF(
  v_status='FAIL',
  CONCAT('Row count mismatch Bronze vs Silver in last ', CAST(lookback_days AS STRING), ' days.'),
  'Row counts match Bronze.'
);

SET v_next = IF(
  v_status='FAIL',
  'Check Silver backfill/merge source filter. Silver should not drop rows when joining entity.',
  'No action required.'
);

INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_silver_sa360_test_results`
VALUES (
  CURRENT_TIMESTAMP(), CURRENT_DATE(),
  'sdi_silver_sa360_campaign_daily',
  'reconciliation',
  CONCAT('Row Count Reconciliation (', CAST(lookback_days AS STRING), '-day)'),
  'MEDIUM',
  v_expected, v_actual, v_variance,
  v_status, IF(v_status='PASS','🟢','🔴'),
  v_reason, v_next,
  FALSE,
  (v_status='PASS'),
  (v_status='FAIL')
);

-- =====================================================
-- TEST 2: Key Coverage (anti-join) Bronze keys missing in Silver
--   Expected = 0 missing keys
-- =====================================================
SET v_expected = 0;

SET v_actual = (
  SELECT COUNT(*)
  FROM (
    SELECT b.account_id, b.campaign_id, b.date
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_campaign_daily` b
    WHERE b.date >= DATE_SUB(CURRENT_DATE(), INTERVAL lookback_days DAY)
    EXCEPT DISTINCT
    SELECT s.account_id, s.campaign_id, s.date
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_silver_sa360_campaign_daily` s
    WHERE s.date >= DATE_SUB(CURRENT_DATE(), INTERVAL lookback_days DAY)
  )
);

SET v_variance = v_actual - v_expected;
SET v_status = IF(v_actual = 0, 'PASS', 'FAIL');

SET v_reason = IF(
  v_status='FAIL',
  CONCAT('Silver is missing ', CAST(v_actual AS STRING), ' key(s) present in Bronze (lookback window).'),
  'All Bronze keys are present in Silver (lookback window).'
);

SET v_next = IF(
  v_status='FAIL',
  'Inspect Silver MERGE ON clause and source SELECT. Verify partition filters and that date_yyyymmdd is not used as key in Silver.',
  'No action required.'
);

INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_silver_sa360_test_results`
VALUES (
  CURRENT_TIMESTAMP(), CURRENT_DATE(),
  'sdi_silver_sa360_campaign_daily',
  'reconciliation',
  CONCAT('Key Coverage: Bronze minus Silver (', CAST(lookback_days AS STRING), '-day)'),
  'HIGH',
  v_expected, v_actual, v_variance,
  v_status, IF(v_status='PASS','🟢','🔴'),
  v_reason, v_next,
  (v_status='FAIL'),
  (v_status='PASS'),
  (v_status='FAIL')
);

-- =====================================================
-- TEST 3: Cost Total Match (tolerance)
-- =====================================================
SET v_expected = (
  SELECT IFNULL(SUM(cost), 0)
  FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_campaign_daily`
  WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL lookback_days DAY)
);

SET v_actual = (
  SELECT IFNULL(SUM(cost), 0)
  FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_silver_sa360_campaign_daily`
  WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL lookback_days DAY)
);

SET v_variance = v_actual - v_expected;

-- tolerance: 0.01 currency units
SET v_status = IF(ABS(v_variance) < 0.01, 'PASS', 'FAIL');

SET v_reason = IF(
  v_status='FAIL',
  CONCAT('Cost mismatch Bronze vs Silver. Variance=', CAST(v_variance AS STRING)),
  'Cost reconciliation successful.'
);

SET v_next = IF(
  v_status='FAIL',
  'Verify Silver is using d.cost (already derived from Bronze) and not re-deriving from micros.',
  'No action required.'
);

INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_silver_sa360_test_results`
VALUES (
  CURRENT_TIMESTAMP(), CURRENT_DATE(),
  'sdi_silver_sa360_campaign_daily',
  'reconciliation',
  CONCAT('Cost Reconciliation (', CAST(lookback_days AS STRING), '-day)'),
  'MEDIUM',
  v_expected, v_actual, v_variance,
  v_status, IF(v_status='PASS','🟢','🔴'),
  v_reason, v_next,
  FALSE,
  (v_status='PASS'),
  (v_status='FAIL')
);

-- =====================================================
-- TEST 4: Sum checks for key conversion families (high-signal metrics)
--   (Keep these few; don't explode into 50 tests.)
-- =====================================================

-- 4A: all_conversions
SET v_expected = (
  SELECT IFNULL(SUM(all_conversions), 0)
  FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_campaign_daily`
  WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL lookback_days DAY)
);

SET v_actual = (
  SELECT IFNULL(SUM(all_conversions), 0)
  FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_silver_sa360_campaign_daily`
  WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL lookback_days DAY)
);

SET v_variance = v_actual - v_expected;
SET v_status = IF(ABS(v_variance) < 0.0001, 'PASS', 'FAIL');

SET v_reason = IF(
  v_status='FAIL',
  CONCAT('all_conversions mismatch Bronze vs Silver. Variance=', CAST(v_variance AS STRING)),
  'all_conversions totals match.'
);

SET v_next = IF(
  v_status='FAIL',
  'Check column mapping in Silver SELECT/UPDATE. Ensure Silver uses d.all_conversions.',
  'No action required.'
);

INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_silver_sa360_test_results`
VALUES (
  CURRENT_TIMESTAMP(), CURRENT_DATE(),
  'sdi_silver_sa360_campaign_daily',
  'reconciliation',
  CONCAT('all_conversions Total Reconciliation (', CAST(lookback_days AS STRING), '-day)'),
  'LOW',
  v_expected, v_actual, v_variance,
  v_status, IF(v_status='PASS','🟢','🔴'),
  v_reason, v_next,
  FALSE,
  (v_status='PASS'),
  (v_status='FAIL')
);

-- 4B: cart_start (cart family)
SET v_expected = (
  SELECT IFNULL(SUM(cart_start), 0)
  FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_campaign_daily`
  WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL lookback_days DAY)
);

SET v_actual = (
  SELECT IFNULL(SUM(cart_start), 0)
  FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_silver_sa360_campaign_daily`
  WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL lookback_days DAY)
);

SET v_variance = v_actual - v_expected;
SET v_status = IF(ABS(v_variance) < 0.0001, 'PASS', 'FAIL');

SET v_reason = IF(
  v_status='FAIL',
  CONCAT('cart_start mismatch Bronze vs Silver. Variance=', CAST(v_variance AS STRING)),
  'cart_start totals match.'
);

SET v_next = IF(
  v_status='FAIL',
  'Check Silver column mapping for cart_start.',
  'No action required.'
);

INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_silver_sa360_test_results`
VALUES (
  CURRENT_TIMESTAMP(), CURRENT_DATE(),
  'sdi_silver_sa360_campaign_daily',
  'reconciliation',
  CONCAT('cart_start Total Reconciliation (', CAST(lookback_days AS STRING), '-day)'),
  'LOW',
  v_expected, v_actual, v_variance,
  v_status, IF(v_status='PASS','🟢','🔴'),
  v_reason, v_next,
  FALSE,
  (v_status='PASS'),
  (v_status='FAIL')
);

END;
