/*
===============================================================================
FILE: 04_sp_bronze_campaign_entity_reconciliation.sql
LAYER: Bronze | QA
PROC:  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_bronze_sa360_campaign_entity_reconciliation_tests

WHAT THIS TEST DOES (7d window, by BUSINESS DATE):
  - Compares Bronze Entity vs RAW Entity (deduped) using the SAME business-date window.
  - Adds diagnostics WITHOUT adding extra result rows:
      * missing_in_raw_cnt     = keys present in Bronze but absent in RAW-dedup
      * missing_in_bronze_cnt  = keys present in RAW-dedup but absent in Bronze
      * small sample keys inline (limit 10) to debug quickly

WHY THIS IS NEEDED:
  - Your RAW source is not guaranteed append-only (rows can disappear after ingestion).
  - So a simple count mismatch is not enough; we need "which direction" and examples.

RAW SOURCE:
  prj-dbi-prd-1.ds_dbi_improvado_master.google_search_ads_360_beta_campaign_entity_custom_tmo
===============================================================================
*/

CREATE OR REPLACE PROCEDURE
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_bronze_sa360_campaign_entity_reconciliation_tests`()
OPTIONS(strict_mode=false)
BEGIN
  DECLARE lookback_days INT64 DEFAULT 7;
  DECLARE window_start DATE DEFAULT DATE_SUB(CURRENT_DATE(), INTERVAL lookback_days DAY);

  INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_test_results`
  WITH
  -- ----------------------------
  -- RAW (deduped)
  -- ----------------------------
  raw_src AS (
    SELECT
      SAFE_CAST(account_id AS STRING) AS account_id,
      SAFE_CAST(campaign_id AS STRING) AS campaign_id,
      SAFE_CAST(date_yyyymmdd AS STRING) AS date_yyyymmdd,
      SAFE.PARSE_DATE('%Y%m%d', SAFE_CAST(date_yyyymmdd AS STRING)) AS date,

      -- robust "arrival" timestamp for ordering only (do NOT filter on this)
      COALESCE(
        SAFE_CAST(File_Load_datetime AS TIMESTAMP),
        TIMESTAMP(SAFE_CAST(File_Load_datetime AS DATETIME))
      ) AS file_load_ts,

      NULLIF(TRIM(SAFE_CAST(Filename AS STRING)), '') AS filename
    FROM `prj-dbi-prd-1.ds_dbi_improvado_master.google_search_ads_360_beta_campaign_entity_custom_tmo`
    WHERE SAFE.PARSE_DATE('%Y%m%d', SAFE_CAST(date_yyyymmdd AS STRING)) >= window_start
  ),
  raw_clean AS (
    SELECT * FROM raw_src WHERE date IS NOT NULL
  ),
  raw_dedup AS (
    SELECT * EXCEPT(rn)
    FROM (
      SELECT
        r.*,
        ROW_NUMBER() OVER (
          PARTITION BY account_id, campaign_id, date_yyyymmdd
          ORDER BY file_load_ts DESC NULLS LAST, filename DESC
        ) AS rn
      FROM raw_clean r
    )
    WHERE rn = 1
  ),

  -- ----------------------------
  -- BRONZE (same business-date window)
  -- ----------------------------
  bronze_scoped AS (
    SELECT
      account_id,
      campaign_id,
      date_yyyymmdd,
      date,
      COALESCE(
        SAFE_CAST(file_load_datetime AS TIMESTAMP),
        TIMESTAMP(SAFE_CAST(file_load_datetime AS DATETIME))
      ) AS file_load_ts
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_campaign_entity`
    WHERE date >= window_start
  ),

  -- ----------------------------
  -- Counts + diagnostics
  -- ----------------------------
  counts AS (
    SELECT
      (SELECT COUNT(1) FROM raw_dedup) AS raw_cnt,
      (SELECT COUNT(1) FROM bronze_scoped) AS bronze_cnt,

      -- keys present in Bronze but missing in RAW
      (SELECT COUNT(1)
       FROM (SELECT account_id, campaign_id, date_yyyymmdd FROM bronze_scoped) b
       LEFT JOIN (SELECT account_id, campaign_id, date_yyyymmdd FROM raw_dedup) r
       USING (account_id, campaign_id, date_yyyymmdd)
       WHERE r.account_id IS NULL
      ) AS missing_in_raw_cnt,

      -- keys present in RAW but missing in Bronze
      (SELECT COUNT(1)
       FROM (SELECT account_id, campaign_id, date_yyyymmdd FROM raw_dedup) r
       LEFT JOIN (SELECT account_id, campaign_id, date_yyyymmdd FROM bronze_scoped) b
       USING (account_id, campaign_id, date_yyyymmdd)
       WHERE b.account_id IS NULL
      ) AS missing_in_bronze_cnt,

      -- sample keys (Bronze-only)
      (SELECT STRING_AGG(CONCAT(account_id,'|',campaign_id,'|',date_yyyymmdd), ', ' ORDER BY account_id, campaign_id, date_yyyymmdd)
       FROM (
         SELECT b.account_id, b.campaign_id, b.date_yyyymmdd
         FROM (SELECT account_id, campaign_id, date_yyyymmdd FROM bronze_scoped) b
         LEFT JOIN (SELECT account_id, campaign_id, date_yyyymmdd FROM raw_dedup) r
         USING (account_id, campaign_id, date_yyyymmdd)
         WHERE r.account_id IS NULL
         LIMIT 10
       )
      ) AS sample_missing_in_raw,

      -- sample keys (RAW-only)
      (SELECT STRING_AGG(CONCAT(account_id,'|',campaign_id,'|',date_yyyymmdd), ', ' ORDER BY account_id, campaign_id, date_yyyymmdd)
       FROM (
         SELECT r.account_id, r.campaign_id, r.date_yyyymmdd
         FROM (SELECT account_id, campaign_id, date_yyyymmdd FROM raw_dedup) r
         LEFT JOIN (SELECT account_id, campaign_id, date_yyyymmdd FROM bronze_scoped) b
         USING (account_id, campaign_id, date_yyyymmdd)
         WHERE b.account_id IS NULL
         LIMIT 10
       )
      ) AS sample_missing_in_bronze
  )

  SELECT
    CURRENT_TIMESTAMP() AS test_run_timestamp,
    CURRENT_DATE() AS test_date,
    'sdi_bronze_sa360_campaign_entity' AS table_name,
    'reconciliation' AS test_layer,
    'Row Count Reconciliation (Entity Bronze vs RAW-dedup, 7d by business date)' AS test_name,
    'HIGH' AS severity_level,

    CAST(raw_cnt AS FLOAT64) AS expected_value,
    CAST(bronze_cnt AS FLOAT64) AS actual_value,
    CAST(bronze_cnt - raw_cnt AS FLOAT64) AS variance_value,

    IF(bronze_cnt = raw_cnt, 'PASS', 'FAIL') AS status,
    IF(bronze_cnt = raw_cnt, '🟢', '🔴') AS status_emoji,

    IF(bronze_cnt = raw_cnt,
      'Row counts match exactly.',
      CONCAT(
        'Row counts do NOT match. ',
        'missing_in_raw_cnt=', CAST(missing_in_raw_cnt AS STRING),
        ', missing_in_bronze_cnt=', CAST(missing_in_bronze_cnt AS STRING),
        '. Bronze-only sample (up to 10)=', COALESCE(sample_missing_in_raw,'<none>'),
        '. RAW-only sample (up to 10)=', COALESCE(sample_missing_in_bronze,'<none>')
      )
    ) AS failure_reason,

    IF(bronze_cnt = raw_cnt,
      'No action required.',
      'If missing_in_raw_cnt > 0, RAW may be non-append-only OR you are reconciling against the wrong RAW source. If missing_in_bronze_cnt > 0, Bronze merge/backfill may be filtering out keys. Use the samples above to investigate.'
    ) AS next_step,

    IF(bronze_cnt != raw_cnt, TRUE, FALSE) AS is_critical_failure,
    IF(bronze_cnt = raw_cnt, TRUE, FALSE) AS is_pass,
    IF(bronze_cnt != raw_cnt, TRUE, FALSE) AS is_fail
  FROM counts;

END;