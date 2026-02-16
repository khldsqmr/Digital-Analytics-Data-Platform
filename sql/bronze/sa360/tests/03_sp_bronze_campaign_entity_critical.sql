/*
===============================================================================
FILE: 03_sp_bronze_campaign_entity_critical.sql

PURPOSE:
  Blocking / HIGH severity validation for:
    prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_campaign_entity

GRAIN (must be unique):
  account_id + campaign_id + date_yyyymmdd

===============================================================================
*/

CREATE OR REPLACE PROCEDURE
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_bronze_campaign_entity_critical`()
BEGIN

DECLARE v_expected FLOAT64;
DECLARE v_actual   FLOAT64;
DECLARE v_variance FLOAT64;
DECLARE v_status   STRING;
DECLARE v_reason   STRING;
DECLARE v_next     STRING;

-- =====================================================
-- TEST 1: Duplicate Snapshot Check
-- =====================================================
SET v_expected = 0;

SET v_actual = (
  SELECT COUNT(*)
  FROM (
    SELECT account_id, campaign_id, date_yyyymmdd, COUNT(*) AS c
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_campaign_entity`
    GROUP BY 1,2,3
    HAVING COUNT(*) > 1
  )
);

SET v_variance = v_actual - v_expected;
SET v_status   = IF(v_actual = 0, 'PASS', 'FAIL');

SET v_reason = IF(
  v_status='FAIL',
  'Duplicate entity snapshots detected (account_id, campaign_id, date_yyyymmdd).',
  'No duplicate snapshots found.'
);

SET v_next = IF(
  v_status='FAIL',
  'Investigate entity incremental/backfill MERGE keys and dedup ordering.',
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
  v_expected,
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

-- =====================================================
-- TEST 2: Null Identifier Check (entity)
-- =====================================================
SET v_expected = 0;

SET v_actual = (
  SELECT COUNT(*)
  FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_campaign_entity`
  WHERE account_id IS NULL
     OR campaign_id IS NULL
     OR date_yyyymmdd IS NULL
     OR date IS NULL
);

SET v_variance = v_actual - v_expected;
SET v_status   = IF(v_actual = 0, 'PASS', 'FAIL');

SET v_reason = IF(
  v_status='FAIL',
  'Primary identifiers contain NULL values (account_id/campaign_id/date_yyyymmdd/date).',
  'All identifiers valid.'
);

SET v_next = IF(
  v_status='FAIL',
  'Inspect date parsing and source ingestion mapping for entity table.',
  'No action required.'
);

INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_test_results`
VALUES (
  CURRENT_TIMESTAMP(),
  CURRENT_DATE(),
  'sdi_bronze_sa360_campaign_entity',
  'critical',
  'Null Identifier Check',
  'HIGH',
  v_expected,
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
