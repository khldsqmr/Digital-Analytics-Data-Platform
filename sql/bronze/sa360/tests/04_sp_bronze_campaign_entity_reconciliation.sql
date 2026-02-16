/*
===============================================================================
FILE: 04_sp_bronze_campaign_entity_reconciliation.sql

PURPOSE:
  Reconcile Bronze Campaign Entity vs source (7-day window).

SOURCE:
  prj-dbi-prd-1.ds_dbi_improvado_master.google_search_ads_360_beta_campaign_entity_custom_tmo

TARGET:
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_campaign_entity

WINDOW:
  last N days (default 7)

===============================================================================
*/

CREATE OR REPLACE PROCEDURE
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_bronze_campaign_entity_reconciliation`()
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
  FROM `prj-dbi-prd-1.ds_dbi_improvado_master.google_search_ads_360_beta_campaign_entity_custom_tmo` s
  WHERE SAFE.PARSE_DATE('%Y%m%d', CAST(s.date_yyyymmdd AS STRING)) >= v_window_start
);

SET v_actual = (
  SELECT COUNT(*)
  FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_campaign_entity` b
  WHERE b.date >= v_window_start
    AND b.date IS NOT NULL
);

SET v_variance = v_actual - v_expected;
SET v_status   = IF(v_variance = 0, 'PASS', 'FAIL');

SET v_reason = IF(
  v_status='FAIL',
  CONCAT('Entity row count mismatch in last ', CAST(v_lookback_days AS STRING), ' days between source and bronze.'),
  'Entity snapshot row counts match source.'
);

SET v_next = IF(
  v_status='FAIL',
  'Verify entity incremental MERGE lookback window, dedup ordering, and late-arriving snapshots.',
  'No action required.'
);

INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_test_results`
VALUES (
  CURRENT_TIMESTAMP(),
  CURRENT_DATE(),
  'sdi_bronze_sa360_campaign_entity',
  'reconciliation',
  CONCAT('Entity Row Count Reconciliation (', CAST(v_lookback_days AS STRING), '-day)'),
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
-- TEST 2: Bidding Strategy Type Coverage (lookback window)
-- =====================================================

SET v_expected = (
  SELECT COUNT(DISTINCT CAST(s.bidding_strategy_type AS STRING))
  FROM `prj-dbi-prd-1.ds_dbi_improvado_master.google_search_ads_360_beta_campaign_entity_custom_tmo` s
  WHERE SAFE.PARSE_DATE('%Y%m%d', CAST(s.date_yyyymmdd AS STRING)) >= v_window_start
);

SET v_actual = (
  SELECT COUNT(DISTINCT CAST(b.bidding_strategy_type AS STRING))
  FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_campaign_entity` b
  WHERE b.date >= v_window_start
    AND b.date IS NOT NULL
);

SET v_variance = v_actual - v_expected;
SET v_status   = IF(v_variance = 0, 'PASS', 'FAIL');

SET v_reason = IF(
  v_status='FAIL',
  CONCAT('Distinct bidding_strategy_type mismatch in last ', CAST(v_lookback_days AS STRING), ' days.'),
  'Bidding strategy type coverage aligned.'
);

SET v_next = IF(
  v_status='FAIL',
  'Check transformation/mapping for bidding_strategy_type in entity Bronze.',
  'No action required.'
);

INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_test_results`
VALUES (
  CURRENT_TIMESTAMP(),
  CURRENT_DATE(),
  'sdi_bronze_sa360_campaign_entity',
  'reconciliation',
  CONCAT('Bidding Strategy Type Coverage (', CAST(v_lookback_days AS STRING), '-day)'),
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
