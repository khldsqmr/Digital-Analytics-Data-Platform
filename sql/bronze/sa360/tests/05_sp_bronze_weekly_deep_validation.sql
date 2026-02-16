/*
===============================================================================
FILE: 05_sp_bronze_weekly_deep_validation.sql

PURPOSE:
  Weekly deep validation (non-blocking but important):
    • Outlier detection
    • Metric sanity
    • Partition spread
    • Historical late arrival monitoring

NOTE:
  Keep this cheap. We restrict to a recent rolling window for most checks.

===============================================================================
*/

CREATE OR REPLACE PROCEDURE
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_bronze_weekly_deep_validation`()
BEGIN

DECLARE v_recent_days INT64 DEFAULT 30;
DECLARE v_window_start DATE DEFAULT DATE_SUB(CURRENT_DATE(), INTERVAL v_recent_days DAY);

DECLARE v_expected FLOAT64;
DECLARE v_actual   FLOAT64;
DECLARE v_variance FLOAT64;
DECLARE v_status   STRING;
DECLARE v_reason   STRING;
DECLARE v_next     STRING;

-- =====================================================
-- TEST 1: Extreme Daily Cost Spike Check (recent window)
-- =====================================================

/*
Rationale:
  A single-day max cost that is abnormally high often indicates:
    - duplication
    - bad micros conversion
    - upstream ingestion anomaly

This is a LOW severity heuristic check (non-blocking).
*/

SET v_expected = 10000000;  -- example threshold; tune per business context (currency units)
SET v_actual = (
  SELECT MAX(CAST(cost AS FLOAT64))
  FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_campaign_daily`
  WHERE date >= v_window_start
    AND date IS NOT NULL
);

SET v_variance = IFNULL(v_actual, 0) - v_expected;
SET v_status   = IF(IFNULL(v_actual, 0) <= v_expected, 'PASS', 'FAIL');

SET v_reason = IF(
  v_status='FAIL',
  CONCAT('Unusually high daily cost detected in last ',
         CAST(v_recent_days AS STRING),
         ' days. Possible duplication or ingestion anomaly.'),
  'Max daily cost within expected threshold.'
);

SET v_next = IF(
  v_status='FAIL',
  'Inspect recent loads (File_Load_datetime/Filename), and validate dedup + cost conversion.',
  'No action required.'
);

INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_test_results`
VALUES (
  CURRENT_TIMESTAMP(),
  CURRENT_DATE(),
  'sdi_bronze_sa360_campaign_daily',
  'deep_validation',
  CONCAT('Extreme Daily Cost Spike (', CAST(v_recent_days AS STRING), '-day window)'),
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

-- =====================================================
-- TEST 2: Negative Cost Sanity Check (recent window)
-- =====================================================

/*
Rationale:
  Negative cost is generally invalid for SA360 cost exports.
  If present, it may indicate bad casting, upstream adjustments, or corrupted rows.
*/

SET v_expected = 0;

SET v_actual = (
  SELECT COUNT(*)
  FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_campaign_daily`
  WHERE date >= v_window_start
    AND date IS NOT NULL
    AND CAST(cost AS FLOAT64) < 0
);

SET v_variance = v_actual - v_expected;
SET v_status   = IF(v_actual = 0, 'PASS', 'FAIL');

SET v_reason = IF(
  v_status='FAIL',
  CONCAT('Negative cost rows detected in last ',
         CAST(v_recent_days AS STRING), ' days.'),
  'No negative cost rows detected.'
);

SET v_next = IF(
  v_status='FAIL',
  'Inspect upstream source cost_micros and Bronze casting/conversion logic.',
  'No action required.'
);

INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_test_results`
VALUES (
  CURRENT_TIMESTAMP(),
  CURRENT_DATE(),
  'sdi_bronze_sa360_campaign_daily',
  'deep_validation',
  CONCAT('Negative Cost Sanity (', CAST(v_recent_days AS STRING), '-day window)'),
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

-- =====================================================
-- TEST 3: Recent Partition Spread Check
-- =====================================================

/*
Rationale:
  Ensures we actually have data across multiple dates in the recent window.
  A very narrow spread can indicate that daily ingestion is stuck or only one date is loading.
*/

SET v_expected = 7;  -- expect at least 7 distinct dates in last 30 days (tune as needed)

SET v_actual = (
  SELECT COUNT(DISTINCT date)
  FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_campaign_daily`
  WHERE date >= v_window_start
    AND date IS NOT NULL
);

SET v_variance = v_actual - v_expected;
SET v_status   = IF(v_actual >= v_expected, 'PASS', 'FAIL');

SET v_reason = IF(
  v_status='FAIL',
  CONCAT('Low date coverage: only ',
         CAST(v_actual AS STRING),
         ' distinct dates found in last ',
         CAST(v_recent_days AS STRING),
         ' days.'),
  CONCAT('Date coverage OK: ',
         CAST(v_actual AS STRING),
         ' distinct dates in last ',
         CAST(v_recent_days AS STRING),
         ' days.')
);

SET v_next = IF(
  v_status='FAIL',
  'Check incremental scheduler, landing freshness, and source export cadence.',
  'No action required.'
);

INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_test_results`
VALUES (
  CURRENT_TIMESTAMP(),
  CURRENT_DATE(),
  'sdi_bronze_sa360_campaign_daily',
  'deep_validation',
  CONCAT('Recent Partition Spread (', CAST(v_recent_days AS STRING), '-day window)'),
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
