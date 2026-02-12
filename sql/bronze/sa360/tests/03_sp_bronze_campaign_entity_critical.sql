/*
===============================================================================
FILE: 03_sp_bronze_campaign_entity_critical.sql

PURPOSE:
  Critical validation for campaign entity snapshot table.

TABLE:
  sdi_bronze_sa360_campaign_entity

GRAIN:
  account_id + campaign_id + date

===============================================================================
*/

CREATE OR REPLACE PROCEDURE
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_bronze_campaign_entity_critical`()
BEGIN

DECLARE v_expected FLOAT64;
DECLARE v_actual FLOAT64;
DECLARE v_variance FLOAT64;
DECLARE v_status STRING;
DECLARE v_reason STRING;
DECLARE v_next STRING;

-- =====================================================
-- TEST 1: Duplicate Snapshot Check
-- =====================================================

SET v_expected = 0;

SET v_actual = (
  SELECT COUNT(*)
  FROM (
    SELECT account_id, campaign_id, date, COUNT(*) c
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_campaign_entity`
    GROUP BY 1,2,3
    HAVING COUNT(*) > 1
  )
);

SET v_variance = v_actual;
SET v_status = IF(v_actual=0,'PASS','FAIL');

SET v_reason = IF(
  v_status='FAIL',
  'Duplicate entity snapshots detected.',
  'No duplicate snapshots found.'
);

SET v_next = IF(
  v_status='FAIL',
  'Investigate entity incremental MERGE logic.',
  'No action required.'
);

INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_test_results`
VALUES (
  CURRENT_TIMESTAMP(),
  CURRENT_DATE(),
  'sdi_bronze_sa360_campaign_entity',
  'critical',
  'Duplicate Snapshot Check',
  'HIGH',
  0,
  v_actual,
  v_variance,
  v_status,
  IF(v_status='PASS','🟢','🔴'),
  v_reason,
  v_next,
  (v_status='FAIL'),
  (v_status='PASS'),
  (v_status='FAIL')
);

END;
