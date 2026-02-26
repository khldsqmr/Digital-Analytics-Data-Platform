/*
===============================================================================
FILE: 04_sp_bronze_campaign_entity_reconciliation.sql  (COMPACT, 1 ROW OUTPUT)
LAYER: Bronze | QA
PROC:  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_bronze_sa360_campaign_entity_reconciliation_tests

RECONCILIATION:
  Bronze Entity vs RAW Entity, scoped by ARRIVAL window (last N days),
  deduped consistently on (account_id, campaign_id, date_yyyymmdd).

WHY ARRIVAL WINDOW:
  Entity snapshots can arrive late (new file today for older date_yyyymmdd).
===============================================================================
*/

CREATE OR REPLACE PROCEDURE
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_bronze_sa360_campaign_entity_reconciliation_tests`()
OPTIONS(strict_mode=false)
BEGIN
  DECLARE lookback_days INT64 DEFAULT 7;
  DECLARE window_start_ts TIMESTAMP DEFAULT TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL lookback_days DAY);

  INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_test_results`
  WITH
  raw_src AS (
    SELECT
      SAFE_CAST(account_id AS STRING) AS account_id,
      SAFE_CAST(campaign_id AS STRING) AS campaign_id,
      SAFE_CAST(date_yyyymmdd AS STRING) AS date_yyyymmdd,
      SAFE.PARSE_DATE('%Y%m%d', SAFE_CAST(date_yyyymmdd AS STRING)) AS date,
      SAFE_CAST(File_Load_datetime AS DATETIME) AS file_load_datetime,
      TIMESTAMP(SAFE_CAST(File_Load_datetime AS DATETIME)) AS file_load_ts,
      NULLIF(TRIM(SAFE_CAST(Filename AS STRING)), '') AS filename
    FROM `prj-dbi-prd-1.ds_dbi_improvado_master.google_search_ads_360_beta_campaign_entity_custom_tmo`
    WHERE TIMESTAMP(SAFE_CAST(File_Load_datetime AS DATETIME)) >= window_start_ts
  ),
  raw_clean AS (
    SELECT * FROM raw_src
    WHERE date IS NOT NULL AND account_id IS NOT NULL AND campaign_id IS NOT NULL AND date_yyyymmdd IS NOT NULL
  ),
  raw_dedup AS (
    SELECT * EXCEPT(rn)
    FROM (
      SELECT
        r.*,
        ROW_NUMBER() OVER (
          PARTITION BY account_id, campaign_id, date_yyyymmdd
          ORDER BY file_load_ts DESC NULLS LAST, filename DESC NULLS LAST
        ) AS rn
      FROM raw_clean r
    )
    WHERE rn = 1
  ),

  bronze_src AS (
    SELECT
      SAFE_CAST(account_id AS STRING) AS account_id,
      SAFE_CAST(campaign_id AS STRING) AS campaign_id,
      SAFE_CAST(date_yyyymmdd AS STRING) AS date_yyyymmdd,
      date,
      SAFE_CAST(file_load_datetime AS DATETIME) AS file_load_datetime,
      TIMESTAMP(SAFE_CAST(file_load_datetime AS DATETIME)) AS file_load_ts
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_campaign_entity`
    WHERE TIMESTAMP(SAFE_CAST(file_load_datetime AS DATETIME)) >= window_start_ts
  ),
  bronze_clean AS (
    SELECT * FROM bronze_src
    WHERE date IS NOT NULL AND account_id IS NOT NULL AND campaign_id IS NOT NULL AND date_yyyymmdd IS NOT NULL
  ),
  -- Defensive: reconcile on latest key state inside the arrival window (same intent as RAW)
  bronze_dedup AS (
    SELECT * EXCEPT(rn)
    FROM (
      SELECT
        b.*,
        ROW_NUMBER() OVER (
          PARTITION BY account_id, campaign_id, date_yyyymmdd
          ORDER BY file_load_ts DESC NULLS LAST
        ) AS rn
      FROM bronze_clean b
    )
    WHERE rn = 1
  ),

  counts AS (
    SELECT
      (SELECT COUNT(1) FROM raw_dedup) AS raw_cnt,
      (SELECT COUNT(1) FROM bronze_dedup) AS bronze_cnt
  )
  SELECT
    CURRENT_TIMESTAMP(),
    CURRENT_DATE(),
    'sdi_bronze_sa360_campaign_entity',
    'reconciliation',
    'Row Count Reconciliation (Entity Bronze vs RAW-dedup, arrival window 7d)',
    'HIGH',
    CAST(raw_cnt AS FLOAT64) AS expected_value,
    CAST(bronze_cnt AS FLOAT64) AS actual_value,
    IF(bronze_cnt = raw_cnt, 0.0, CAST(bronze_cnt - raw_cnt AS FLOAT64)) AS variance_value,
    IF(bronze_cnt = raw_cnt, 'PASS', 'FAIL') AS status,
    IF(bronze_cnt = raw_cnt, '🟢', '🔴') AS status_emoji,
    IF(bronze_cnt = raw_cnt,
      'Row counts match exactly when scoped by arrival time (last 7 days) using consistent dedupe.',
      'Row counts do NOT match when scoped by arrival time. This indicates missing/extra keys within the arrival window.'
    ) AS failure_reason,
    IF(bronze_cnt = raw_cnt,
      'No action required.',
      'Run a separate one-off key diff query (not stored in test_results) for (account_id,campaign_id,date_yyyymmdd).'
    ) AS next_step,
    IF(bronze_cnt != raw_cnt, TRUE, FALSE) AS is_critical_failure,
    IF(bronze_cnt = raw_cnt, TRUE, FALSE) AS is_pass,
    IF(bronze_cnt != raw_cnt, TRUE, FALSE) AS is_fail
  FROM counts;

END;