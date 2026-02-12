/*
===============================================================================
FILE: bronze_campaign_daily_tests.sql
LAYER: Bronze
TABLE: sdi_bronze_sa360_campaign_daily

PURPOSE:
  Validate structural integrity, grain uniqueness, partition health,
  transformation correctness, freshness, and metric sanity
  for Bronze SA360 Campaign Daily table.

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
-- We partition by date.
-- If partitions are missing or stale, downstream dashboards will break.
-- Poor partition health also increases scan cost.

-- What we expect?
-- • max_partition_date = recent date
-- • partition_count steadily increasing
-- • If max_partition_date is old → incremental pipeline failure.
-- ======================================================
SELECT
  MIN(date) AS min_partition_date,
  MAX(date) AS max_partition_date,
  COUNT(DISTINCT date) AS partition_count
FROM
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_campaign_daily`;

-- ======================================================
-- TEST 2: Duplicate Grain Detection
-- ======================================================
-- Why?
-- Bronze grain must be:
-- account_id + campaign_id + date
-- Duplicates indicate:
-- • MERGE logic failure
-- • Incremental conflict
-- • Source duplication

-- What we expect?
-- 0 rows returned.
-- ======================================================
SELECT
  account_id,
  campaign_id,
  date,
  COUNT(*) AS record_count
FROM
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_campaign_daily`
GROUP BY 1,2,3
HAVING COUNT(*) > 1;

-- ======================================================
-- TEST 3: Critical Identifier Null Check
-- ======================================================
-- Why?
-- Primary identifiers and partition keys must never be NULL.
-- If NULL exists:
-- • Downstream joins break
-- • Aggregations become inaccurate
-- • Partitioning fails

-- What we expect?
-- All returned values = 0.
-- ======================================================
SELECT
  COUNTIF(account_id IS NULL) AS null_account_id,
  COUNTIF(campaign_id IS NULL) AS null_campaign_id,
  COUNTIF(date IS NULL) AS null_date,
  COUNTIF(date_yyyymmdd IS NULL) AS null_date_yyyymmdd
FROM
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_campaign_daily`;

-- ======================================================
-- TEST 4: Micros to Cost Conversion Validation
-- ======================================================
-- Why?
-- We derive:
-- cost = cost_micros / 1,000,000
-- This must preserve totals.

-- What we expect?
-- • difference ≈ 0
-- • Minor floating rounding allowed
-- • Large difference indicates transformation bug
-- ======================================================
SELECT
  SUM(cost_micros)/1000000 AS expected_cost,
  SUM(cost) AS bronze_cost,
  ABS(SUM(cost) - SUM(cost_micros)/1000000) AS difference
FROM
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_campaign_daily`;

-- ======================================================
-- TEST 5: Negative Metric Detection
-- ======================================================
-- Why?
-- Clicks, impressions, cost should never be negative.
-- Negative values indicate:
-- • Upstream corruption
-- • Bad ingestion
-- • Data type overflow

-- What we expect?
-- All values = 0.
-- ======================================================
SELECT
  COUNTIF(clicks < 0) AS negative_clicks,
  COUNTIF(impressions < 0) AS negative_impressions,
  COUNTIF(cost < 0) AS negative_cost
FROM
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_campaign_daily`;

-- ======================================================
-- TEST 6: 7-Day Freshness Check
-- ======================================================
-- Why?
-- Ensure incremental ingestion is functioning daily.

-- What we expect?
-- • Rows present for last 7 days
-- • No missing dates
-- • Stable daily counts

-- If a date is missing → pipeline gap.
-- ======================================================
SELECT
  date,
  COUNT(*) AS daily_row_count
FROM
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_campaign_daily`
WHERE
  date >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
GROUP BY 1
ORDER BY 1 DESC;

-- ======================================================
-- TEST 7: Late Arrival Monitoring
-- ======================================================
-- Why?
-- Detect unexpected updates to older partitions.
-- Helps monitor late file ingestion.

-- What we expect?
-- • Recent partitions updated recently
-- • Older partitions rarely updated

-- Frequent old updates → investigate upstream changes.
-- ======================================================
SELECT
  date,
  MAX(bronze_inserted_at) AS latest_ingestion_time
FROM
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_campaign_daily`
GROUP BY 1
ORDER BY 1 DESC
LIMIT 10;

