/*
===============================================================================
FILE: 04_sp_bronze_campaign_entity_reconciliation.sql
LAYER: Bronze | QA
PROC:  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_bronze_sa360_campaign_entity_reconciliation_tests

RECONCILIATION:
  Bronze Entity vs RAW Entity table (deduped with same logic).

RAW SOURCE:
  prj-dbi-prd-1.ds_dbi_improvado_master.google_search_ads_360_beta_campaign_entity_custom_tmo
===============================================================================
*/

CREATE OR REPLACE PROCEDURE
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_bronze_sa360_campaign_entity_reconciliation_tests`()
OPTIONS(strict_mode=false)
BEGIN
  DECLARE lookback_days INT64 DEFAULT 7;

  -- Row-count reconciliation (Entity Bronze vs RAW-dedup)
  INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_test_results`
  WITH raw_src AS (
    SELECT
      SAFE_CAST(account_id AS STRING) AS account_id,
      SAFE_CAST(campaign_id AS STRING) AS campaign_id,
      SAFE_CAST(date_yyyymmdd AS STRING) AS date_yyyymmdd,
      SAFE.PARSE_DATE('%Y%m%d', SAFE_CAST(date_yyyymmdd AS STRING)) AS date,
      SAFE_CAST(File_Load_datetime AS DATETIME) AS file_load_datetime,
      NULLIF(TRIM(SAFE_CAST(Filename AS STRING)), '') AS filename
    FROM `prj-dbi-prd-1.ds_dbi_improvado_master.google_search_ads_360_beta_campaign_entity_custom_tmo`
    WHERE SAFE.PARSE_DATE('%Y%m%d', SAFE_CAST(date_yyyymmdd AS STRING))
          >= DATE_SUB(CURRENT_DATE(), INTERVAL lookback_days DAY)
  ),
  raw_clean AS (SELECT * FROM raw_src WHERE date IS NOT NULL),
  raw_dedup AS (
    SELECT * EXCEPT(rn)
    FROM (
      SELECT
        raw_clean.*,
        ROW_NUMBER() OVER (
          PARTITION BY account_id, campaign_id, date_yyyymmdd
          ORDER BY file_load_datetime DESC, filename DESC
        ) AS rn
      FROM raw_clean
    )
    WHERE rn = 1
  ),
  counts AS (
    SELECT
      (SELECT COUNT(1)
       FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_campaign_entity`
       WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL lookback_days DAY)
      ) AS bronze_cnt,
      (SELECT COUNT(1) FROM raw_dedup) AS raw_cnt
  )
  SELECT
    CURRENT_TIMESTAMP(),
    CURRENT_DATE(),
    'sdi_bronze_sa360_campaign_entity',
    'reconciliation',
    'Row Count Reconciliation (Entity Bronze vs RAW-dedup, 7d)',
    'HIGH',
    CAST(raw_cnt AS FLOAT64),
    CAST(bronze_cnt AS FLOAT64),
    CAST(bronze_cnt - raw_cnt AS FLOAT64),
    IF(bronze_cnt = raw_cnt, 'PASS', 'FAIL'),
    IF(bronze_cnt = raw_cnt, '🟢', '🔴'),
    IF(bronze_cnt = raw_cnt,
      'Row counts match exactly.',
      'Row counts do NOT match. Bronze entity may be missing rows or duplicating rows vs RAW-dedup.'
    ),
    IF(bronze_cnt = raw_cnt,
      'No action required.',
      'Compare missing keys and validate MERGE filters + date parsing.'
    ),
    IF(bronze_cnt != raw_cnt, TRUE, FALSE),
    IF(bronze_cnt = raw_cnt, TRUE, FALSE),
    IF(bronze_cnt != raw_cnt, TRUE, FALSE)
  FROM counts;

END;
