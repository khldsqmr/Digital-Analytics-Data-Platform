/*
===============================================================================
FILE: 04_sp_bronze_campaign_entity_reconciliation.sql  (UPDATED)
LAYER: Bronze | QA
PROC:  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_bronze_sa360_campaign_entity_reconciliation_tests

RECONCILIATION:
  Bronze Entity vs RAW Entity table (deduped with same grain).

IMPORTANT FIX:
  - Scope BOTH RAW and BRONZE by ARRIVAL time (file_load_datetime), not date_yyyymmdd.
  - Entity feeds often have stale/lagged date_yyyymmdd, which causes false FAILs.

RAW SOURCE:
  prj-dbi-prd-1.ds_dbi_improvado_master.google_search_ads_360_beta_campaign_entity_custom_tmo

GRAIN (DEDUP):
  (account_id, campaign_id, date_yyyymmdd) keep latest file_load_datetime (+ filename tie-break)
===============================================================================
*/

CREATE OR REPLACE PROCEDURE
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_bronze_sa360_campaign_entity_reconciliation_tests`()
OPTIONS(strict_mode=false)
BEGIN
  DECLARE lookback_days INT64 DEFAULT 7;

  INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_test_results`
  WITH raw_src AS (
    SELECT
      SAFE_CAST(account_id AS STRING) AS account_id,
      SAFE_CAST(campaign_id AS STRING) AS campaign_id,
      SAFE_CAST(date_yyyymmdd AS STRING) AS date_yyyymmdd,
      SAFE.PARSE_DATE('%Y%m%d', SAFE_CAST(date_yyyymmdd AS STRING)) AS date,

      -- RAW arrives as DATETIME (Improvado); keep both forms
      SAFE_CAST(File_Load_datetime AS DATETIME) AS file_load_datetime_dt,
      CAST(SAFE_CAST(File_Load_datetime AS DATETIME) AS TIMESTAMP) AS file_load_datetime_ts,

      NULLIF(TRIM(SAFE_CAST(Filename AS STRING)), '') AS filename
    FROM `prj-dbi-prd-1.ds_dbi_improvado_master.google_search_ads_360_beta_campaign_entity_custom_tmo`
    WHERE CAST(SAFE_CAST(File_Load_datetime AS DATETIME) AS TIMESTAMP)
          >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL lookback_days DAY)
  ),

  raw_clean AS (
    SELECT *
    FROM raw_src
    WHERE account_id IS NOT NULL
      AND campaign_id IS NOT NULL
      AND date_yyyymmdd IS NOT NULL
      AND file_load_datetime_ts IS NOT NULL
      -- date can be NULL in entity feeds; do NOT filter it out for row-count recon
  ),

  raw_dedup AS (
    SELECT * EXCEPT(rn)
    FROM (
      SELECT
        raw_clean.*,
        ROW_NUMBER() OVER (
          PARTITION BY account_id, campaign_id, date_yyyymmdd
          ORDER BY file_load_datetime_ts DESC, filename DESC
        ) AS rn
      FROM raw_clean
    )
    WHERE rn = 1
  ),

  bronze_scoped AS (
    SELECT *
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_campaign_entity`
    WHERE CAST(file_load_datetime AS TIMESTAMP)
          >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL lookback_days DAY)
  ),

  counts AS (
    SELECT
      (SELECT COUNT(1) FROM bronze_scoped) AS bronze_cnt,
      (SELECT COUNT(1) FROM raw_dedup)     AS raw_cnt
  )

  SELECT
    CURRENT_TIMESTAMP() AS test_run_timestamp,
    CURRENT_DATE()      AS test_date,
    'sdi_bronze_sa360_campaign_entity' AS table_name,
    'reconciliation'    AS test_layer,
    CONCAT('Row Count Reconciliation (Entity Bronze vs RAW-dedup, arrival window ', CAST(lookback_days AS STRING), 'd)') AS test_name,
    'HIGH'              AS severity_level,

    CAST(raw_cnt AS FLOAT64)    AS expected_value,
    CAST(bronze_cnt AS FLOAT64) AS actual_value,
    IF(bronze_cnt = raw_cnt, 0.0, CAST(bronze_cnt - raw_cnt AS FLOAT64)) AS variance_value,

    IF(bronze_cnt = raw_cnt, 'PASS', 'FAIL') AS status,
    IF(bronze_cnt = raw_cnt, '🟢', '🔴')      AS status_emoji,

    IF(bronze_cnt = raw_cnt,
      'Row counts match (scoped by arrival time).',
      'Row counts do NOT match when scoped by arrival time. This is likely a true pipeline mismatch (missing/extra keys) vs RAW-dedup.'
    ) AS failure_reason,

    IF(bronze_cnt = raw_cnt,
      'No action required.',
      'Run a key-diff on (account_id,campaign_id,date_yyyymmdd) between Bronze scoped and RAW-dedup scoped. Validate Bronze MERGE filters and key casting.'
    ) AS next_step,

    IF(bronze_cnt != raw_cnt, TRUE, FALSE) AS is_critical_failure,
    IF(bronze_cnt = raw_cnt, TRUE, FALSE)  AS is_pass,
    IF(bronze_cnt != raw_cnt, TRUE, FALSE) AS is_fail
  FROM counts;

END;