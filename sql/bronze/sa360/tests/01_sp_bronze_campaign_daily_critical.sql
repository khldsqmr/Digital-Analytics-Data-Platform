/*
===============================================================================
FILE: 01_sp_bronze_campaign_daily_critical.sql

PURPOSE:
  Critical structural validation for:
    sdi_bronze_sa360_campaign_daily

  These tests are blocking.
  If any HIGH severity FAIL exists, orchestration stops.

GRAIN:
  account_id + campaign_id + date

===============================================================================
*/

CREATE OR REPLACE PROCEDURE
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_bronze_campaign_daily_critical`()
BEGIN

DECLARE v_expected FLOAT64;
DECLARE v_actual FLOAT64;
DECLARE v_variance FLOAT64;
DECLARE v_status STRING;
DECLARE v_reason STRING;
DECLARE v_next STRING;

-- =====================================================
-- TEST 1: Duplicate Grain Check
-- =====================================================

SET v_expected = 0;

SET v_actual = (
  SELECT COUNT(*)
  FROM (
    SELECT account_id, campaign_id, date, COUNT(*) c
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_campaign_daily`
    GROUP BY 1,2,3
    HAVING COUNT(*) > 1
  )
);

SET v_variance = v_actual - v_expected;
SET v_status = IF(v_actual = 0, 'PASS', 'FAIL');

SET v_reason = IF(
  v_status='FAIL',
  'Duplicate grain detected. MERGE or upstream duplication issue.',
  'No duplicate grain detected.'
);

SET v_next = IF(
  v_status='FAIL',
  'Inspect incremental MERGE and deduplication logic.',
  'No action required.'
);

INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_test_results`
VALUES (
  CURRENT_TIMESTAMP(),
  CURRENT_DATE(),
  'sdi_bronze_sa360_campaign_daily',
  'critical',
  'Duplicate Grain Check',
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
-- TEST 2: Null Identifier Check
-- =====================================================

SET v_expected = 0;

SET v_actual = (
  SELECT COUNT(*)
  FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_campaign_daily`
  WHERE account_id IS NULL
     OR campaign_id IS NULL
     OR date IS NULL
     OR date_yyyymmdd IS NULL
);

SET v_variance = v_actual - v_expected;
SET v_status = IF(v_actual = 0, 'PASS', 'FAIL');

SET v_reason = IF(
  v_status='FAIL',
  'Primary identifier contains NULL values.',
  'All identifiers valid.'
);

SET v_next = IF(
  v_status='FAIL',
  'Inspect source ingestion and column mapping.',
  'No action required.'
);

INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_test_results`
VALUES (
  CURRENT_TIMESTAMP(),
  CURRENT_DATE(),
  'sdi_bronze_sa360_campaign_daily',
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

-- =====================================================
-- TEST 3: Partition Freshness Check
-- =====================================================

-- Allow max 2-day delay
SET v_expected = 2;

SET v_actual = (
  SELECT DATE_DIFF(CURRENT_DATE(), MAX(date), DAY)
  FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_campaign_daily`
);

SET v_variance = v_actual - v_expected;
SET v_status = IF(v_actual <= v_expected, 'PASS', 'FAIL');

SET v_reason = IF(
  v_status='FAIL',
  CONCAT('Partition stale by ', CAST(v_actual AS STRING), ' days. Threshold ≤ 2 days.'),
  CONCAT('Partition freshness OK (', CAST(v_actual AS STRING), ' days delay).')
);

SET v_next = IF(
  v_status='FAIL',
  'Check incremental ingestion pipeline and upstream data availability.',
  'No action required.'
);

INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_test_results`
VALUES (
  CURRENT_TIMESTAMP(),
  CURRENT_DATE(),
  'sdi_bronze_sa360_campaign_daily',
  'critical',
  'Partition Freshness',
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
