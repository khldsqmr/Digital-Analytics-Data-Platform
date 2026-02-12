/*
===============================================================================
FILE: 01_sp_bronze_campaign_daily_critical.sql
TABLE: sdi_bronze_sa360_campaign_daily

PURPOSE:
  Run CRITICAL structural integrity tests.

GRAIN:
  account_id + campaign_id + date

BLOCKING:
  YES (HIGH severity failures)

===============================================================================
*/

CREATE OR REPLACE PROCEDURE
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_bronze_campaign_daily_critical`()
BEGIN

DECLARE duplicate_count INT64;
DECLARE null_count INT64;
DECLARE freshness_gap INT64;

-- ======================================================
-- TEST 1: Duplicate Grain
-- ======================================================

SET duplicate_count = (
  SELECT COUNT(*)
  FROM (
    SELECT account_id, campaign_id, date
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_campaign_daily`
    GROUP BY 1,2,3
    HAVING COUNT(*) > 1
  )
);

INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_test_results`
SELECT
  CURRENT_TIMESTAMP(),
  CURRENT_DATE(),
  'sdi_bronze_sa360_campaign_daily',
  'critical',
  'Duplicate Grain Check',
  'HIGH',
  0,
  duplicate_count,
  duplicate_count,
  IF(duplicate_count=0,'PASS','FAIL'),
  IF(duplicate_count=0,'🟢','🔴'),
  IF(duplicate_count=0,
     'No duplicates found.',
     'Duplicate grain detected. MERGE logic likely broken.'
  ),
  IF(duplicate_count=0,
     'No action required.',
     'Inspect incremental MERGE logic and upstream duplication.'
  );


-- ======================================================
-- TEST 2: Null Identifier Check
-- ======================================================

SET null_count = (
  SELECT COUNT(*)
  FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_campaign_daily`
  WHERE account_id IS NULL
     OR campaign_id IS NULL
     OR date IS NULL
);

INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_test_results`
SELECT
  CURRENT_TIMESTAMP(),
  CURRENT_DATE(),
  'sdi_bronze_sa360_campaign_daily',
  'critical',
  'Null Identifier Check',
  'HIGH',
  0,
  null_count,
  null_count,
  IF(null_count=0,'PASS','FAIL'),
  IF(null_count=0,'🟢','🔴'),
  IF(null_count=0,
     'Primary keys valid.',
     'Null primary keys found. Downstream joins will break.'
  ),
  IF(null_count=0,
     'No action required.',
     'Investigate source ingestion & partition logic.'
  );


-- ======================================================
-- TEST 3: Partition Freshness
-- ======================================================

SET freshness_gap = (
  SELECT DATE_DIFF(CURRENT_DATE(), MAX(date), DAY)
  FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_campaign_daily`
);

INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_test_results`
SELECT
  CURRENT_TIMESTAMP(),
  CURRENT_DATE(),
  'sdi_bronze_sa360_campaign_daily',
  'critical',
  'Partition Freshness',
  'HIGH',
  2,
  freshness_gap,
  freshness_gap - 2,
  IF(freshness_gap<=2,'PASS','FAIL'),
  IF(freshness_gap<=2,'🟢','🔴'),
  IF(freshness_gap<=2,
     'Data is fresh.',
     'Latest partition too old. Incremental ingestion likely failed.'
  ),
  IF(freshness_gap<=2,
     'No action required.',
     'Check ingestion scheduler and source arrival.'
  );

END;
