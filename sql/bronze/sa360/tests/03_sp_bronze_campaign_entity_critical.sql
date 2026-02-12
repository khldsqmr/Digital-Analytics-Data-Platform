/*
===============================================================================
FILE: 03_sp_bronze_campaign_entity_critical.sql
TABLE: sdi_bronze_sa360_campaign_entity

PURPOSE:
  Validate structural integrity and freshness of Bronze Campaign Entity table.

GRAIN:
  account_id + campaign_id + date

BLOCKING:
  YES (HIGH severity failures)

===============================================================================
*/

CREATE OR REPLACE PROCEDURE
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_bronze_campaign_entity_critical`()
BEGIN

DECLARE duplicate_count INT64;
DECLARE null_count INT64;
DECLARE freshness_gap INT64;

-- ======================================================
-- TEST 1: Duplicate Snapshot Grain
-- ======================================================

SET duplicate_count = (
  SELECT COUNT(*)
  FROM (
    SELECT account_id, campaign_id, date
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_campaign_entity`
    GROUP BY 1,2,3
    HAVING COUNT(*) > 1
  )
);

INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_test_results`
SELECT
  CURRENT_TIMESTAMP(),
  CURRENT_DATE(),
  'sdi_bronze_sa360_campaign_entity',
  'critical',
  'Duplicate Snapshot Grain',
  'HIGH',
  0,
  duplicate_count,
  duplicate_count,
  IF(duplicate_count=0,'PASS','FAIL'),
  IF(duplicate_count=0,'🟢','🔴'),
  IF(duplicate_count=0,
     'No duplicate snapshots.',
     'Duplicate snapshot rows detected.'
  ),
  IF(duplicate_count=0,
     'No action required.',
     'Review incremental merge logic and snapshot keys.'
  );

-- ======================================================
-- TEST 2: Null Identifier Check
-- ======================================================

SET null_count = (
  SELECT COUNT(*)
  FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_campaign_entity`
  WHERE account_id IS NULL
     OR campaign_id IS NULL
     OR date IS NULL
);

INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_test_results`
SELECT
  CURRENT_TIMESTAMP(),
  CURRENT_DATE(),
  'sdi_bronze_sa360_campaign_entity',
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
     'Null primary identifiers found.'
  ),
  IF(null_count=0,
     'No action required.',
     'Inspect ingestion and date parsing logic.'
  );

-- ======================================================
-- TEST 3: Partition Freshness
-- ======================================================

SET freshness_gap = (
  SELECT DATE_DIFF(CURRENT_DATE(), MAX(date), DAY)
  FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_campaign_entity`
);

INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_test_results`
SELECT
  CURRENT_TIMESTAMP(),
  CURRENT_DATE(),
  'sdi_bronze_sa360_campaign_entity',
  'critical',
  'Partition Freshness',
  'HIGH',
  2,
  freshness_gap,
  freshness_gap - 2,
  IF(freshness_gap<=2,'PASS','FAIL'),
  IF(freshness_gap<=2,'🟢','🔴'),
  IF(freshness_gap<=2,
     'Entity data is fresh.',
     'Entity partition too old.'
  ),
  IF(freshness_gap<=2,
     'No action required.',
     'Check snapshot ingestion scheduler.'
  );

END;
