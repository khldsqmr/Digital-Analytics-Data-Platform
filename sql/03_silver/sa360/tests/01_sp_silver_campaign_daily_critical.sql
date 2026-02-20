/*
===============================================================================
FILE: 01_sp_silver_campaign_daily_critical.sql
LAYER: Silver QA
TABLE: sdi_silver_sa360_campaign_daily

PURPOSE:
  Critical structural validation for Silver Campaign Daily.
  These tests are BLOCKING (HIGH severity).

SILVER GRAIN:
  account_id + campaign_id + date

INTER-LAYER CONTEXT:
  - Silver is built from Bronze Daily plus enrichments (e.g., entity join for campaign_name).
  - Enrichment must not create/destroy rows relative to Bronze grain.

===============================================================================
*/

CREATE OR REPLACE PROCEDURE
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_silver_campaign_daily_critical`()
BEGIN

DECLARE v_expected FLOAT64;
DECLARE v_actual   FLOAT64;
DECLARE v_variance FLOAT64;
DECLARE v_status   STRING;
DECLARE v_reason   STRING;
DECLARE v_next     STRING;
DECLARE v_emoji    STRING;

DECLARE lookback_days INT64 DEFAULT 7;
DECLARE v_window_start DATE DEFAULT DATE_SUB(CURRENT_DATE(), INTERVAL lookback_days DAY);

-- =====================================================
-- TEST 1: Duplicate Grain Check (Silver)
-- =====================================================
SET v_expected = 0;

SET v_actual = (
  SELECT COUNT(*)
  FROM (
    SELECT account_id, campaign_id, date
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_silver_sa360_campaign_daily`
    GROUP BY 1,2,3
    HAVING COUNT(*) > 1
  )
);

SET v_variance = v_actual - v_expected;
SET v_status   = IF(v_actual = 0, 'PASS', 'FAIL');
SET v_emoji    = IF(v_status='PASS','🟢','🔴');

SET v_reason = IF(
  v_status='FAIL',
  'Duplicate grain detected (account_id, campaign_id, date). Silver MERGE/backfill not idempotent OR join created duplicates.',
  'No duplicate grain detected.'
);

SET v_next = IF(
  v_status='FAIL',
  'Verify Silver MERGE ON keys match grain and entity join returns max 1 row per (account_id, campaign_id, date).',
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
  'critical',
  'Duplicate Grain Check',
  'HIGH',
  v_expected, v_actual, v_variance,
  v_status, v_emoji,
  v_reason, v_next,
  (v_status='FAIL'), (v_status='PASS'), (v_status='FAIL');

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
SET v_status   = IF(v_actual = 0, 'PASS', 'FAIL');
SET v_emoji    = IF(v_status='PASS','🟢','🔴');

SET v_reason = IF(
  v_status='FAIL',
  'Primary identifiers contain NULL values (account_id/campaign_id/date).',
  'All identifiers valid.'
);

SET v_next = IF(
  v_status='FAIL',
  'Check upstream Bronze keys and Silver join logic. Ensure date derivation is stable and not nullifying joins.',
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
  'critical',
  'Null Identifier Check',
  'HIGH',
  v_expected, v_actual, v_variance,
  v_status, v_emoji,
  v_reason, v_next,
  (v_status='FAIL'), (v_status='PASS'), (v_status='FAIL');

-- =====================================================
-- TEST 3: Partition Freshness (Silver lag vs Bronze)
--   Threshold: Silver can lag Bronze by <= 2 days
-- =====================================================
SET v_expected = 2;

SET v_actual = (
  SELECT
    IFNULL(
      GREATEST(
        0,
        DATE_DIFF(
          (SELECT MAX(date) FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_campaign_daily`),
          (SELECT MAX(date) FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_silver_sa360_campaign_daily`),
          DAY
        )
      ),
      9999
    )
);

SET v_variance = v_actual - v_expected;
SET v_status   = IF(v_actual <= v_expected, 'PASS', 'FAIL');
SET v_emoji    = IF(v_status='PASS','🟢','🔴');

SET v_reason = IF(
  v_status='FAIL',
  CONCAT('Silver is stale vs Bronze by ', CAST(v_actual AS STRING),
         ' day(s). Threshold ≤ ', CAST(v_expected AS STRING), '.'),
  CONCAT('Freshness OK. Silver lag vs Bronze = ', CAST(v_actual AS STRING), ' day(s).')
);

SET v_next = IF(
  v_status='FAIL',
  'Check Silver incremental window/schedule and ensure Silver is built AFTER Bronze completes.',
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
  'critical',
  'Partition Freshness vs Bronze',
  'HIGH',
  v_expected, v_actual, v_variance,
  v_status, v_emoji,
  v_reason, v_next,
  (v_status='FAIL'), (v_status='PASS'), (v_status='FAIL');

-- =====================================================
-- TEST 4 (HIGH): Join Explosion Detection (Silver vs Bronze row count, recent window)
--   If entity join multiplies rows, rowcount will exceed Bronze.
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
  CONCAT('Rowcount differs in last ', CAST(lookback_days AS STRING),
         ' days (Silver=', CAST(v_actual AS STRING),
         ', Bronze=', CAST(v_expected AS STRING), '). Possible join explosion or filtering.'),
  'Rowcount matches Bronze in the recent window.'
);

SET v_next = IF(
  v_status='FAIL',
  'Validate Silver entity join returns exactly one match per Bronze key. Ensure no WHERE filters are applied after joins.',
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
  'critical',
  CONCAT('Join Explosion Detection (', CAST(lookback_days AS STRING), '-day)'),
  'HIGH',
  v_expected, v_actual, v_variance,
  v_status, v_emoji,
  v_reason, v_next,
  (v_status='FAIL'), (v_status='PASS'), (v_status='FAIL');

END;
