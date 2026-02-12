/*
===============================================================================
FILE: 02_sp_bronze_campaign_daily_reconciliation.sql

PURPOSE:
  Validate Bronze Campaign Daily against source.

SOURCE:
  google_search_ads_360_campaigns_tmo

GRAIN:
  account_id + campaign_id + date

These tests are NON-BLOCKING but critical for data correctness.

===============================================================================
*/

CREATE OR REPLACE PROCEDURE
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_bronze_campaign_daily_reconciliation`()
BEGIN

DECLARE v_expected FLOAT64;
DECLARE v_actual FLOAT64;
DECLARE v_variance FLOAT64;
DECLARE v_status STRING;
DECLARE v_reason STRING;
DECLARE v_next STRING;

-- =====================================================
-- TEST 1: Row Count Reconciliation (7-day window)
-- =====================================================

SET v_expected = (
  SELECT COUNT(*)
  FROM `prj-dbi-prd-1.ds_dbi_improvado_master.google_search_ads_360_campaigns_tmo`
  WHERE PARSE_DATE('%Y%m%d', date_yyyymmdd)
        >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
);

SET v_actual = (
  SELECT COUNT(*)
  FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_campaign_daily`
  WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
);

SET v_variance = v_actual - v_expected;
SET v_status = IF(v_variance = 0, 'PASS', 'FAIL');

SET v_reason = IF(
  v_status='FAIL',
  'Row count mismatch between source and bronze.',
  'Row counts match source.'
);

SET v_next = IF(
  v_status='FAIL',
  'Inspect incremental MERGE filters and lookback window.',
  'No action required.'
);

INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_test_results`
VALUES (
  CURRENT_TIMESTAMP(),
  CURRENT_DATE(),
  'sdi_bronze_sa360_campaign_daily',
  'reconciliation',
  'Row Count Reconciliation (7-day)',
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
-- TEST 2: Cost Reconciliation
-- =====================================================

SET v_expected = (
  SELECT SUM(cost_micros)/1000000
  FROM `prj-dbi-prd-1.ds_dbi_improvado_master.google_search_ads_360_campaigns_tmo`
  WHERE PARSE_DATE('%Y%m%d', date_yyyymmdd)
        >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
);

SET v_actual = (
  SELECT SUM(cost)
  FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_campaign_daily`
  WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
);

SET v_variance = IFNULL(v_actual - v_expected, 0);
SET v_status = IF(ABS(v_variance) < 0.01, 'PASS', 'FAIL');

SET v_reason = IF(
  v_status='FAIL',
  'Cost mismatch detected. Micros to cost conversion error possible.',
  'Cost reconciliation successful.'
);

SET v_next = IF(
  v_status='FAIL',
  'Verify cost = cost_micros/1,000,000 transformation logic.',
  'No action required.'
);

INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_test_results`
VALUES (
  CURRENT_TIMESTAMP(),
  CURRENT_DATE(),
  'sdi_bronze_sa360_campaign_daily',
  'reconciliation',
  'Cost Reconciliation',
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
