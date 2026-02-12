/*
===============================================================================
FILE: 04_sp_bronze_campaign_entity_reconciliation.sql

PURPOSE:
  Reconcile Bronze Campaign Entity against source snapshot table.

SOURCE:
  google_search_ads_360_beta_campaign_entity_custom_tmo

TABLE:
  sdi_bronze_sa360_campaign_entity

GRAIN:
  account_id + campaign_id + date

These are MEDIUM severity (non-blocking) but correctness critical.

===============================================================================
*/

CREATE OR REPLACE PROCEDURE
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_bronze_campaign_entity_reconciliation`()
BEGIN

DECLARE v_expected FLOAT64;
DECLARE v_actual FLOAT64;
DECLARE v_variance FLOAT64;
DECLARE v_status STRING;
DECLARE v_reason STRING;
DECLARE v_next STRING;

-- =====================================================
-- TEST 1: 7-Day Snapshot Row Count Reconciliation
-- =====================================================

SET v_expected = (
  SELECT COUNT(*)
  FROM `prj-dbi-prd-1.ds_dbi_improvado_master.google_search_ads_360_beta_campaign_entity_custom_tmo`
  WHERE PARSE_DATE('%Y%m%d', date_yyyymmdd)
        >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
);

SET v_actual = (
  SELECT COUNT(*)
  FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_campaign_entity`
  WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
);

SET v_variance = v_actual - v_expected;
SET v_status = IF(v_variance = 0, 'PASS', 'FAIL');

SET v_reason = IF(
  v_status='FAIL',
  'Entity row count mismatch between source and bronze.',
  'Entity snapshot row counts match source.'
);

SET v_next = IF(
  v_status='FAIL',
  'Verify entity incremental MERGE logic and snapshot filters.',
  'No action required.'
);

INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_test_results`
VALUES (
  CURRENT_TIMESTAMP(),
  CURRENT_DATE(),
  'sdi_bronze_sa360_campaign_entity',
  'reconciliation',
  'Entity Row Count Reconciliation (7-day)',
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
-- TEST 2: Bidding Strategy Distribution Check
-- =====================================================

SET v_expected = (
  SELECT COUNT(DISTINCT bidding_strategy_type)
  FROM `prj-dbi-prd-1.ds_dbi_improvado_master.google_search_ads_360_beta_campaign_entity_custom_tmo`
);

SET v_actual = (
  SELECT COUNT(DISTINCT bidding_strategy_type)
  FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_campaign_entity`
);

SET v_variance = v_actual - v_expected;
SET v_status = IF(v_variance = 0, 'PASS', 'FAIL');

SET v_reason = IF(
  v_status='FAIL',
  'Bidding strategy distribution mismatch.',
  'Bidding strategy distribution aligned.'
);

SET v_next = IF(
  v_status='FAIL',
  'Check transformation logic for bidding_strategy_type.',
  'No action required.'
);

INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_test_results`
VALUES (
  CURRENT_TIMESTAMP(),
  CURRENT_DATE(),
  'sdi_bronze_sa360_campaign_entity',
  'reconciliation',
  'Bidding Strategy Distribution Check',
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
