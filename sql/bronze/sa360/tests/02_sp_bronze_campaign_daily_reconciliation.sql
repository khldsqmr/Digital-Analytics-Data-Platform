/*
===============================================================================
FILE: 02_sp_bronze_campaign_daily_reconciliation.sql

PURPOSE:
  Reconcile Bronze Campaign Daily vs source (7-day window).

SOURCE:
  prj-dbi-prd-1.ds_dbi_improvado_master.google_search_ads_360_campaigns_tmo

TARGET:
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_campaign_daily

WINDOW:
  last N days (default 7)

===============================================================================
*/

CREATE OR REPLACE PROCEDURE
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_bronze_campaign_daily_reconciliation`()
BEGIN

DECLARE v_lookback_days INT64 DEFAULT 7;
DECLARE v_window_start  DATE  DEFAULT DATE_SUB(CURRENT_DATE(), INTERVAL v_lookback_days DAY);

DECLARE v_expected FLOAT64;
DECLARE v_actual   FLOAT64;
DECLARE v_variance FLOAT64;
DECLARE v_status   STRING;
DECLARE v_reason   STRING;
DECLARE v_next     STRING;

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

SET v_reason = IF(
  v_status='FAIL',
  CONCAT('Row count mismatch in last ', CAST(v_lookback_days AS STRING), ' days between source and bronze.'),
  'Row counts match source.'
);

SET v_next = IF(
  v_status='FAIL',
  'Inspect incremental MERGE filters (lookback window), dedup ordering, and source late-arrivals.',
  'No action required.'
);

INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_test_results`
VALUES (
  CURRENT_TIMESTAMP(),
  CURRENT_DATE(),
  'sdi_bronze_sa360_campaign_daily',
  'reconciliation',
  CONCAT('Row Count Reconciliation (', CAST(v_lookback_days AS STRING), '-day)'),
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

-- =====================================================
-- TEST 2: Cost Reconciliation (lookback window)
-- =====================================================

SET v_expected = (
  SELECT SAFE_DIVIDE(SUM(CAST(s.cost_micros AS FLOAT64)), 1000000.0)
  FROM `prj-dbi-prd-1.ds_dbi_improvado_master.google_search_ads_360_campaigns_tmo` s
  WHERE SAFE.PARSE_DATE('%Y%m%d', CAST(s.date_yyyymmdd AS STRING)) >= v_window_start
);

SET v_actual = (
  SELECT SUM(CAST(b.cost AS FLOAT64))
  FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_campaign_daily` b
  WHERE b.date >= v_window_start
    AND b.date IS NOT NULL
);

SET v_variance = IFNULL(v_actual, 0) - IFNULL(v_expected, 0);
SET v_status   = IF(ABS(v_variance) < 0.01, 'PASS', 'FAIL');

SET v_reason = IF(
  v_status='FAIL',
  CONCAT('Cost mismatch in last ', CAST(v_lookback_days AS STRING),
         ' days. Potential micros->cost conversion or dedup issue.'),
  'Cost reconciliation successful.'
);

SET v_next = IF(
  v_status='FAIL',
  'Verify cost = cost_micros / 1,000,000 in Bronze and confirm latest-file dedup logic.',
  'No action required.'
);

INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_test_results`
VALUES (
  CURRENT_TIMESTAMP(),
  CURRENT_DATE(),
  'sdi_bronze_sa360_campaign_daily',
  'reconciliation',
  CONCAT('Cost Reconciliation (', CAST(v_lookback_days AS STRING), '-day)'),
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
