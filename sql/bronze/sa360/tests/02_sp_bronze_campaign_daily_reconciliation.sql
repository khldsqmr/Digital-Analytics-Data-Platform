/*
===============================================================================
FILE: 02_sp_bronze_campaign_daily_reconciliation.sql
TABLE: sdi_bronze_sa360_campaign_daily

SOURCE:
  google_search_ads_360_campaigns_tmo

PURPOSE:
  Validate Bronze table accurately reflects source data.

GRAIN:
  account_id + campaign_id + date

BLOCKING:
  YES for major mismatches
  WARNING for small variance

===============================================================================
*/

CREATE OR REPLACE PROCEDURE
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_bronze_campaign_daily_reconciliation`()
BEGIN

DECLARE source_count INT64;
DECLARE bronze_count INT64;
DECLARE missing_rows INT64;
DECLARE cost_diff FLOAT64;
DECLARE clicks_diff FLOAT64;
DECLARE date_mismatch INT64;

-- ======================================================
-- TEST 1: Row Count Reconciliation (Last 7 Days)
-- ======================================================

SET source_count = (
  SELECT COUNT(*)
  FROM `prj-dbi-prd-1.ds_dbi_improvado_master.google_search_ads_360_campaigns_tmo`
  WHERE PARSE_DATE('%Y%m%d', date_yyyymmdd)
        >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
);

SET bronze_count = (
  SELECT COUNT(*)
  FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_campaign_daily`
  WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
);

INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_test_results`
SELECT
  CURRENT_TIMESTAMP(),
  CURRENT_DATE(),
  'sdi_bronze_sa360_campaign_daily',
  'reconciliation',
  'Row Count Reconciliation',
  'HIGH',
  source_count,
  bronze_count,
  source_count - bronze_count,
  IF(source_count = bronze_count,'PASS','FAIL'),
  IF(source_count = bronze_count,'🟢','🔴'),
  IF(source_count = bronze_count,
     'Row counts match.',
     'Row mismatch detected between source and bronze.'
  ),
  IF(source_count = bronze_count,
     'No action required.',
     'Check MERGE filtering logic and incremental window.'
  );

-- ======================================================
-- TEST 2: Missing Grain Rows
-- ======================================================

SET missing_rows = (
  SELECT COUNT(*)
  FROM (
    SELECT s.account_id, s.campaign_id, s.date_yyyymmdd
    FROM `prj-dbi-prd-1.ds_dbi_improvado_master.google_search_ads_360_campaigns_tmo` s
    LEFT JOIN `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_campaign_daily` b
      ON s.account_id = b.account_id
     AND s.campaign_id = b.campaign_id
     AND s.date_yyyymmdd = b.date_yyyymmdd
    WHERE b.account_id IS NULL
      AND PARSE_DATE('%Y%m%d', s.date_yyyymmdd)
          >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
  )
);

INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_test_results`
SELECT
  CURRENT_TIMESTAMP(),
  CURRENT_DATE(),
  'sdi_bronze_sa360_campaign_daily',
  'reconciliation',
  'Missing Grain Rows',
  'HIGH',
  0,
  missing_rows,
  missing_rows,
  IF(missing_rows=0,'PASS','FAIL'),
  IF(missing_rows=0,'🟢','🔴'),
  IF(missing_rows=0,
     'No missing rows.',
     'Some source rows missing in bronze.'
  ),
  IF(missing_rows=0,
     'No action required.',
     'Re-run incremental merge for affected dates.'
  );

-- ======================================================
-- TEST 3: Cost Conversion Validation
-- ======================================================

SET cost_diff = (
  WITH source_totals AS (
    SELECT SUM(cost_micros)/1000000 AS expected_cost
    FROM `prj-dbi-prd-1.ds_dbi_improvado_master.google_search_ads_360_campaigns_tmo`
    WHERE PARSE_DATE('%Y%m%d', date_yyyymmdd)
          >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
  ),
  bronze_totals AS (
    SELECT SUM(cost) AS bronze_cost
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_campaign_daily`
    WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
  )
  SELECT ABS(expected_cost - bronze_cost)
  FROM source_totals, bronze_totals
);

INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_test_results`
SELECT
  CURRENT_TIMESTAMP(),
  CURRENT_DATE(),
  'sdi_bronze_sa360_campaign_daily',
  'reconciliation',
  'Cost Conversion Accuracy',
  'HIGH',
  0,
  cost_diff,
  cost_diff,
  IF(cost_diff < 0.01,'PASS','FAIL'),
  IF(cost_diff < 0.01,'🟢','🔴'),
  IF(cost_diff < 0.01,
     'Cost transformation valid.',
     'Cost micros to cost mismatch.'
  ),
  IF(cost_diff < 0.01,
     'No action required.',
     'Inspect cost transformation logic.'
  );

-- ======================================================
-- TEST 4: Clicks Reconciliation
-- ======================================================

SET clicks_diff = (
  WITH source_totals AS (
    SELECT SUM(clicks) AS source_clicks
    FROM `prj-dbi-prd-1.ds_dbi_improvado_master.google_search_ads_360_campaigns_tmo`
    WHERE PARSE_DATE('%Y%m%d', date_yyyymmdd)
          >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
  ),
  bronze_totals AS (
    SELECT SUM(clicks) AS bronze_clicks
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_campaign_daily`
    WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
  )
  SELECT ABS(source_clicks - bronze_clicks)
  FROM source_totals, bronze_totals
);

INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_test_results`
SELECT
  CURRENT_TIMESTAMP(),
  CURRENT_DATE(),
  'sdi_bronze_sa360_campaign_daily',
  'reconciliation',
  'Clicks Reconciliation',
  'HIGH',
  0,
  clicks_diff,
  clicks_diff,
  IF(clicks_diff=0,'PASS','FAIL'),
  IF(clicks_diff=0,'🟢','🔴'),
  IF(clicks_diff=0,
     'Clicks match source.',
     'Clicks mismatch detected.'
  ),
  IF(clicks_diff=0,
     'No action required.',
     'Inspect ingestion and renaming logic.'
  );

-- ======================================================
-- TEST 5: Date Parsing Validation
-- ======================================================

SET date_mismatch = (
  SELECT COUNT(*)
  FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_campaign_daily`
  WHERE date != PARSE_DATE('%Y%m%d', date_yyyymmdd)
);

INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_test_results`
SELECT
  CURRENT_TIMESTAMP(),
  CURRENT_DATE(),
  'sdi_bronze_sa360_campaign_daily',
  'reconciliation',
  'Date Parsing Accuracy',
  'HIGH',
  0,
  date_mismatch,
  date_mismatch,
  IF(date_mismatch=0,'PASS','FAIL'),
  IF(date_mismatch=0,'🟢','🔴'),
  IF(date_mismatch=0,
     'Date parsing correct.',
     'Date parsing mismatch detected.'
  ),
  IF(date_mismatch=0,
     'No action required.',
     'Review PARSE_DATE transformation.'
  );

END;
