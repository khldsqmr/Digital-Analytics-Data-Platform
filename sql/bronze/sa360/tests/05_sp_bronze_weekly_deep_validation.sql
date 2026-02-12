/*
===============================================================================
FILE: 05_sp_bronze_weekly_deep_validation.sql

PURPOSE:
  Weekly deep validation for Bronze SA360 tables.
  Non-blocking but critical for long-term health monitoring.

SEVERITY:
  MEDIUM / LOW

SCHEDULE:
  Weekly (Sunday)

===============================================================================
*/

CREATE OR REPLACE PROCEDURE
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_bronze_weekly_deep_validation`()
BEGIN

DECLARE negative_count INT64;
DECLARE cost_difference FLOAT64;
DECLARE outlier_cost FLOAT64;
DECLARE late_update_count INT64;

-- ======================================================
-- TEST 1: Negative Metrics (Daily Table)
-- ======================================================

SET negative_count = (
  SELECT COUNT(*)
  FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_campaign_daily`
  WHERE clicks < 0 OR impressions < 0 OR cost < 0
);

INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_test_results`
SELECT
  CURRENT_TIMESTAMP(),
  CURRENT_DATE(),
  'sdi_bronze_sa360_campaign_daily',
  'deep_validation',
  'Negative Metric Detection',
  'MEDIUM',
  0,
  negative_count,
  negative_count,
  IF(negative_count=0,'PASS','FAIL'),
  IF(negative_count=0,'🟢','🔴'),
  IF(negative_count=0,
     'No negative metrics.',
     'Negative metrics detected.'
  ),
  IF(negative_count=0,
     'No action required.',
     'Inspect ingestion overflow or source corruption.'
  );

-- ======================================================
-- TEST 2: Cost Conversion Validation
-- ======================================================

SET cost_difference = (
  SELECT ABS(SUM(cost) - SUM(cost_micros)/1000000)
  FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_campaign_daily`
);

INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_test_results`
SELECT
  CURRENT_TIMESTAMP(),
  CURRENT_DATE(),
  'sdi_bronze_sa360_campaign_daily',
  'deep_validation',
  'Micros to Cost Conversion',
  'MEDIUM',
  0,
  cost_difference,
  cost_difference,
  IF(cost_difference < 0.01,'PASS','FAIL'),
  IF(cost_difference < 0.01,'🟢','🔴'),
  IF(cost_difference < 0.01,
     'Cost conversion accurate.',
     'Cost conversion mismatch detected.'
  ),
  IF(cost_difference < 0.01,
     'No action required.',
     'Review transformation logic.'
  );

-- ======================================================
-- TEST 3: Extreme Outlier Detection
-- ======================================================

SET outlier_cost = (
  SELECT MAX(cost)
  FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_campaign_daily`
);

INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_test_results`
SELECT
  CURRENT_TIMESTAMP(),
  CURRENT_DATE(),
  'sdi_bronze_sa360_campaign_daily',
  'deep_validation',
  'Extreme Cost Outlier',
  'LOW',
  10000000,
  outlier_cost,
  outlier_cost - 10000000,
  IF(outlier_cost < 10000000,'PASS','FAIL'),
  IF(outlier_cost < 10000000,'🟢','🔴'),
  IF(outlier_cost < 10000000,
     'Cost values within expected range.',
     'Abnormally high cost detected.'
  ),
  IF(outlier_cost < 10000000,
     'No action required.',
     'Inspect for duplication or file corruption.'
  );

-- ======================================================
-- TEST 4: Late Arrival Monitoring
-- ======================================================

SET late_update_count = (
  SELECT COUNT(*)
  FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_campaign_daily`
  WHERE date < DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
    AND bronze_inserted_at > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)
);

INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_test_results`
SELECT
  CURRENT_TIMESTAMP(),
  CURRENT_DATE(),
  'sdi_bronze_sa360_campaign_daily',
  'deep_validation',
  'Late Partition Update',
  'LOW',
  0,
  late_update_count,
  late_update_count,
  IF(late_update_count=0,'PASS','WARN'),
  IF(late_update_count=0,'🟢','🟡'),
  IF(late_update_count=0,
     'No unexpected late updates.',
     'Old partitions updated recently.'
  ),
  IF(late_update_count=0,
     'No action required.',
     'Verify upstream late file loads.'
  );

END;
