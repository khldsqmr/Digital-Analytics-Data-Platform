/*
===============================================================================
FILE: bronze_campaign_entity_tests.sql
LAYER: Bronze
TABLE: sdi_bronze_sa360_campaign_entity

PURPOSE:
  Validate structural integrity, partitioning, grain consistency,
  snapshot behavior, and metadata sanity for Bronze Campaign Entity table.

GRAIN:
  account_id + campaign_id + date

PARTITION:
  date

===============================================================================
*/

-- ======================================================
-- TEST 1: Partition Range Validation
-- ======================================================
-- Why?
-- Entity table is snapshot-based and partitioned by date.
-- Missing partitions indicate incremental failure.
-- What we expect?
-- • max_partition_date is recent
-- • partition_count steadily increasing
-- ======================================================
SELECT
  MIN(date) AS min_partition_date,
  MAX(date) AS max_partition_date,
  COUNT(DISTINCT date) AS partition_count
FROM
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_campaign_entity`;

-- ======================================================
-- TEST 2: Duplicate Snapshot Detection
-- ======================================================
-- Why?
-- Grain = account_id + campaign_id + date
-- Multiple rows for same grain = snapshot duplication
-- What we expect?
-- 0 rows returned.
-- ======================================================
SELECT
  account_id,
  campaign_id,
  date,
  COUNT(*) AS record_count
FROM
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_campaign_entity`
GROUP BY 1,2,3
HAVING COUNT(*) > 1;

-- ======================================================
-- TEST 3: Critical Identifier Null Check
-- ======================================================
-- Why?
-- Identifiers must never be NULL.
-- NULLs break downstream joins.
-- What we expect?
-- All values = 0.
-- ======================================================
SELECT
  COUNTIF(account_id IS NULL) AS null_account_id,
  COUNTIF(campaign_id IS NULL) AS null_campaign_id,
  COUNTIF(date IS NULL) AS null_date,
  COUNTIF(date_yyyymmdd IS NULL) AS null_date_yyyymmdd
FROM
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_campaign_entity`;

-- ======================================================
-- TEST 4: Date Parsing Accuracy
-- ======================================================
-- Why?
-- Validate correct parsing of date_yyyymmdd.
-- What we expect?
-- 0 rows returned.
-- ======================================================
SELECT
  date_yyyymmdd,
  date,
  PARSE_DATE('%Y%m%d', date_yyyymmdd) AS recalculated_date
FROM
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_campaign_entity`
WHERE
  date != PARSE_DATE('%Y%m%d', date_yyyymmdd);

-- ======================================================
-- TEST 5: Campaign Status Distribution
-- ======================================================
-- Why?
-- Sanity check on campaign status values.
-- Unexpected values indicate mapping issue.
-- What we expect?
-- Valid statuses like:
-- ENABLED, PAUSED, REMOVED, SERVING
-- ======================================================
SELECT
  status,
  COUNT(*) AS row_count
FROM
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_campaign_entity`
GROUP BY 1
ORDER BY 2 DESC;

-- ======================================================
-- TEST 6: Serving Status Distribution
-- ======================================================
-- Why?
-- Ensure serving_status field is populated correctly.
-- What we expect?
-- Values like SERVING, NOT_ELIGIBLE, PAUSED
-- ======================================================
SELECT
  serving_status,
  COUNT(*) AS row_count
FROM
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_campaign_entity`
GROUP BY 1
ORDER BY 2 DESC;