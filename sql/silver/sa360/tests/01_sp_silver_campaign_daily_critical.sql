/*
===============================================================================
FILE: 01_sp_silver_campaign_daily_critical.sql
LAYER: Silver QA
TABLE: sdi_silver_sa360_campaign_daily

PURPOSE:
  Critical structural validation for Silver Campaign Daily.

  These tests are BLOCKING.
  If any HIGH FAIL exists → orchestration halts.

GRAIN (Silver):
  account_id + campaign_id + date

SOURCE TABLES:
  - Silver: sdi_silver_sa360_campaign_daily
  - Bronze Daily: sdi_bronze_sa360_campaign_daily
===============================================================================
*/

CREATE OR REPLACE PROCEDURE
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_silver_campaign_daily_critical`()
BEGIN

DECLARE v_expected FLOAT64 DEFAULT 0;
DECLARE v_actual FLOAT64 DEFAULT 0;
DECLARE v_variance FLOAT64 DEFAULT 0;
DECLARE v_status STRING DEFAULT 'PASS';
DECLARE v_reason STRING DEFAULT '';
DECLARE v_next STRING DEFAULT '';

-- Helper macro-like pattern: insert one row
-- (BigQuery doesn't support macros here, so repeated inserts follow same shape)

-- =====================================================
-- TEST 1: Duplicate Grain Check
-- =====================================================
SET v_expected = 0;

SET v_actual = (
  SELECT COUNT(*)
  FROM (
    SELECT account_id, campaign_id, date, COUNT(*) c
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_silver_sa360_campaign_daily`
    GROUP BY 1,2,3
    HAVING COUNT(*) > 1
  )
);

SET v_variance = v_actual - v_expected;
SET v_status = IF(v_actual = 0, 'PASS', 'FAIL');

SET v_reason = IF(
  v_status='FAIL',
  'Duplicate grain detected (account_id+campaign_id+date). Silver backfill/merge logic is not idempotent.',
  'No duplicate grain detected.'
);

SET v_next = IF(
  v_status='FAIL',
  'Check Silver backfill and MERGE keys; confirm source dedup (Bronze) and MERGE ON clause matches grain.',
  'No action required.'
);

INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_silver_sa360_test_results`
VALUES (
  CURRENT_TIMESTAMP(), CURRENT_DATE(),
  'sdi_silver_sa360_campaign_daily',
  'critical',
  'Duplicate Grain Check',
  'HIGH',
  v_expected, v_actual, v_variance,
  v_status, IF(v_status='PASS','🟢','🔴'),
  v_reason, v_next,
  (v_status='FAIL'),
  (v_status='PASS'),
  (v_status='FAIL')
);

-- =====================================================
-- TEST 2: Null Identifier Check (grain columns)
-- =====================================================
SET v_expected = 0;

SET v_actual = (
  SELECT COUNT(*)
  FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_silver_sa360_campaign_daily`
  WHERE account_id IS NULL
     OR campaign_id IS NULL
     OR date IS NULL
);

SET v_variance = v_actual - v_expected;
SET v_status = IF(v_actual = 0, 'PASS', 'FAIL');

SET v_reason = IF(
  v_status='FAIL',
  'Primary identifiers contain NULL values (account_id/campaign_id/date).',
  'All identifiers valid.'
);

SET v_next = IF(
  v_status='FAIL',
  'Check upstream Bronze keys and Silver SELECT; ensure date parse and joins do not drop keys.',
  'No action required.'
);

INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_silver_sa360_test_results`
VALUES (
  CURRENT_TIMESTAMP(), CURRENT_DATE(),
  'sdi_silver_sa360_campaign_daily',
  'critical',
  'Null Identifier Check',
  'HIGH',
  v_expected, v_actual, v_variance,
  v_status, IF(v_status='PASS','🟢','🔴'),
  v_reason, v_next,
  (v_status='FAIL'),
  (v_status='PASS'),
  (v_status='FAIL')
);

-- =====================================================
-- TEST 3: Partition Freshness (Silver vs Bronze Daily)
--   Expect Silver max(date) == Bronze max(date) OR within 1-2 days (late arrivals).
-- =====================================================
SET v_expected = 2;

SET v_actual = (
  SELECT DATE_DIFF(
    (SELECT MAX(date) FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_campaign_daily`),
    (SELECT MAX(date) FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_silver_sa360_campaign_daily`),
    DAY
  )
);

-- If Silver is ahead (shouldn't happen), treat as 0 delay
SET v_actual = IF(v_actual < 0, 0, v_actual);

SET v_variance = v_actual - v_expected;
SET v_status = IF(v_actual <= v_expected, 'PASS', 'FAIL');

SET v_reason = IF(
  v_status='FAIL',
  CONCAT('Silver is stale vs Bronze by ', CAST(v_actual AS STRING), ' day(s). Threshold ≤ ', CAST(v_expected AS STRING), '.'),
  CONCAT('Freshness OK. Silver lag vs Bronze = ', CAST(v_actual AS STRING), ' day(s).')
);

SET v_next = IF(
  v_status='FAIL',
  'Check Silver incremental MERGE schedule/window and upstream Bronze ingestion timeliness.',
  'No action required.'
);

INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_silver_sa360_test_results`
VALUES (
  CURRENT_TIMESTAMP(), CURRENT_DATE(),
  'sdi_silver_sa360_campaign_daily',
  'critical',
  'Partition Freshness vs Bronze',
  'HIGH',
  v_expected, v_actual, v_variance,
  v_status, IF(v_status='PASS','🟢','🔴'),
  v_reason, v_next,
  (v_status='FAIL'),
  (v_status='PASS'),
  (v_status='FAIL')
);

END;
