/*
===============================================================================
FILE: 01_sp_bronze_campaign_daily_critical.sql

PURPOSE:
  Blocking / HIGH severity structural validation for:
    sdi_bronze_sa360_campaign_daily

GRAIN (must be unique):
  account_id + campaign_id + date_yyyymmdd

===============================================================================
*/

CREATE OR REPLACE PROCEDURE
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_bronze_campaign_daily_critical`()
BEGIN

DECLARE v_expected FLOAT64;
DECLARE v_actual   FLOAT64;
DECLARE v_variance FLOAT64;
DECLARE v_status   STRING;
DECLARE v_reason   STRING;
DECLARE v_next     STRING;

-- Small helper for emoji
DECLARE v_emoji STRING;

-- =====================================================
-- TEST 1: Duplicate Grain Check
-- =====================================================
SET v_expected = 0;

SET v_actual = (
  SELECT COUNT(*)
  FROM (
    SELECT account_id, campaign_id, date_yyyymmdd
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_campaign_daily`
    GROUP BY 1,2,3
    HAVING COUNT(*) > 1
  )
);

SET v_variance = v_actual - v_expected;
SET v_status   = IF(v_actual = 0, 'PASS', 'FAIL');
SET v_emoji    = IF(v_status='PASS','🟢','🔴');

SET v_reason = IF(
  v_status = 'FAIL',
  'Duplicate grain detected (account_id, campaign_id, date_yyyymmdd). MERGE/dedup issue.',
  'No duplicate grain detected.'
);

SET v_next = IF(
  v_status = 'FAIL',
  'Inspect incremental/backfill MERGE keys and ROW_NUMBER() dedup ordering.',
  'No action required.'
);

INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_test_results`
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
  'sdi_bronze_sa360_campaign_daily', 'critical', 'Duplicate Grain Check', 'HIGH',
  v_expected, v_actual, v_variance,
  v_status, v_emoji,
  v_reason, v_next,
  (v_status='FAIL'), (v_status='PASS'), (v_status='FAIL');

-- =====================================================
-- TEST 2: Null Identifier Check
-- =====================================================
SET v_expected = 0;

SET v_actual = (
  SELECT COUNT(*)
  FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_campaign_daily`
  WHERE account_id    IS NULL
     OR campaign_id   IS NULL
     OR date_yyyymmdd IS NULL
     OR date          IS NULL
);

SET v_variance = v_actual - v_expected;
SET v_status   = IF(v_actual = 0, 'PASS', 'FAIL');
SET v_emoji    = IF(v_status='PASS','🟢','🔴');

SET v_reason = IF(
  v_status='FAIL',
  'Primary identifiers contain NULL values (account_id/campaign_id/date_yyyymmdd/date).',
  'All identifiers valid.'
);

SET v_next = IF(
  v_status='FAIL',
  'Inspect parsing of date_yyyymmdd -> date and source ingestion mapping.',
  'No action required.'
);

INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_test_results`
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
  'sdi_bronze_sa360_campaign_daily', 'critical', 'Null Identifier Check', 'HIGH',
  v_expected, v_actual, v_variance,
  v_status, v_emoji,
  v_reason, v_next,
  (v_status='FAIL'), (v_status='PASS'), (v_status='FAIL');

-- =====================================================
-- TEST 3: Partition Freshness Check (max 2-day delay)
-- =====================================================
-- expected: data should be no more than 2 days behind today
SET v_expected = 2;

SET v_actual = (
  SELECT DATE_DIFF(CURRENT_DATE(), MAX(date), DAY)
  FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_campaign_daily`
  WHERE date IS NOT NULL
);

-- if table is empty, MAX(date) is NULL -> v_actual NULL; treat as huge delay
SET v_actual   = IFNULL(v_actual, 9999);

SET v_variance = v_actual - v_expected;
SET v_status   = IF(v_actual <= v_expected, 'PASS', 'FAIL');
SET v_emoji    = IF(v_status='PASS','🟢','🔴');

SET v_reason = IF(
  v_status='FAIL',
  CONCAT('Partition stale by ', CAST(v_actual AS STRING), ' days. Threshold <= 2 days.'),
  CONCAT('Partition freshness OK (', CAST(v_actual AS STRING), ' days delay).')
);

SET v_next = IF(
  v_status='FAIL',
  'Check upstream landing, incremental scheduling, and lookback logic.',
  'No action required.'
);

INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_test_results`
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
  'sdi_bronze_sa360_campaign_daily', 'critical', 'Partition Freshness', 'HIGH',
  v_expected, v_actual, v_variance,
  v_status, v_emoji,
  v_reason, v_next,
  (v_status='FAIL'), (v_status='PASS'), (v_status='FAIL');

END;
