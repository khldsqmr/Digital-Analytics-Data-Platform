/*
===============================================================================
FILE: 03_sp_bronze_campaign_entity_critical.sql
LAYER: Bronze | QA
PROC:  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_bronze_sa360_campaign_entity_critical_tests

TABLE UNDER TEST:
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_campaign_entity

WHY:
  Entity snapshot is used for AS-OF enrichment in Silver.
  If entity has duplicates or null keys, Silver joins become unstable.
===============================================================================
*/

CREATE OR REPLACE PROCEDURE
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_bronze_sa360_campaign_entity_critical_tests`()
OPTIONS(strict_mode=false)
BEGIN
  DECLARE lookback_days INT64 DEFAULT 7;
  DECLARE allowed_freshness_delay_days INT64 DEFAULT 7;

  -- Duplicate grain check
  INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_test_results`
  WITH dup AS (
    SELECT COUNT(1) AS duplicate_groups
    FROM (
      SELECT account_id, campaign_id, date_yyyymmdd, COUNT(*) AS c
      FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_campaign_entity`
      WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL lookback_days DAY)
      GROUP BY 1,2,3
      HAVING COUNT(*) > 1
    )
  )
  SELECT
    CURRENT_TIMESTAMP(), CURRENT_DATE(),
    'sdi_bronze_sa360_campaign_entity',
    'critical',
    'Duplicate Grain Check (acct,campaign,date_yyyymmdd)',
    'HIGH',
    0.0,
    CAST(duplicate_groups AS FLOAT64),
    CAST(duplicate_groups AS FLOAT64),
    IF(duplicate_groups = 0, 'PASS', 'FAIL'),
    IF(duplicate_groups = 0, '🟢', '🔴'),
    IF(duplicate_groups = 0,
      'No duplicate grain detected.',
      'Duplicate keys found in Bronze Entity snapshot.'
    ),
    IF(duplicate_groups = 0,
      'No action required.',
      'Investigate entity MERGE dedupe ordering (file_load_datetime/filename) and RAW duplication.'
    ),
    IF(duplicate_groups > 0, TRUE, FALSE),
    IF(duplicate_groups = 0, TRUE, FALSE),
    IF(duplicate_groups > 0, TRUE, FALSE)
  FROM dup;

  -- Null identifier check
  INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_test_results`
  WITH bad AS (
    SELECT COUNT(1) AS bad_rows
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_campaign_entity`
    WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL lookback_days DAY)
      AND (
        account_id IS NULL OR campaign_id IS NULL OR date_yyyymmdd IS NULL OR date IS NULL
        OR TRIM(account_id) = '' OR TRIM(campaign_id) = '' OR TRIM(date_yyyymmdd) = ''
      )
  )
  SELECT
    CURRENT_TIMESTAMP(), CURRENT_DATE(),
    'sdi_bronze_sa360_campaign_entity',
    'critical',
    'Null Identifier Check (keys + canonical date)',
    'HIGH',
    0.0,
    CAST(bad_rows AS FLOAT64),
    CAST(bad_rows AS FLOAT64),
    IF(bad_rows = 0, 'PASS', 'FAIL'),
    IF(bad_rows = 0, '🟢', '🔴'),
    IF(bad_rows = 0,
      'All identifiers valid.',
      'Null/blank keys found in entity snapshot.'
    ),
    IF(bad_rows = 0,
      'No action required.',
      'Trace failing keys back to RAW; confirm SAFE.PARSE_DATE filter and casts.'
    ),
    IF(bad_rows > 0, TRUE, FALSE),
    IF(bad_rows = 0, TRUE, FALSE),
    IF(bad_rows > 0, TRUE, FALSE)
  FROM bad;

  -- Freshness check (entity can be less frequent, so allow 7 days)
  INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_test_results`
  WITH mx AS (
    SELECT MAX(date) AS max_date
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_campaign_entity`
  ),
  calc AS (
    SELECT
      allowed_freshness_delay_days AS allowed_delay,
      DATE_DIFF(CURRENT_DATE(), max_date, DAY) AS days_delay
    FROM mx
  )
  SELECT
    CURRENT_TIMESTAMP(), CURRENT_DATE(),
    'sdi_bronze_sa360_campaign_entity',
    'critical',
    'Partition Freshness (max(date) delay days)',
    'MEDIUM',
    CAST(allowed_delay AS FLOAT64),
    CAST(days_delay AS FLOAT64),
    CAST(days_delay - allowed_delay AS FLOAT64),
    IF(days_delay <= allowed_delay, 'PASS', 'FAIL'),
    IF(days_delay <= allowed_delay, '🟢', '🔴'),
    IF(days_delay <= allowed_delay,
      CONCAT('Freshness OK. Delay days = ', CAST(days_delay AS STRING), '.'),
      CONCAT('Entity snapshot is stale. Delay days = ', CAST(days_delay AS STRING), '.')
    ),
    IF(days_delay <= allowed_delay,
      'No action required.',
      'Check entity ingestion cadence / upstream extract schedule.'
    ),
    IF(days_delay > allowed_delay, FALSE, FALSE), -- not critical by default
    IF(days_delay <= allowed_delay, TRUE, FALSE),
    IF(days_delay > allowed_delay, TRUE, FALSE)
  FROM calc;

END;
