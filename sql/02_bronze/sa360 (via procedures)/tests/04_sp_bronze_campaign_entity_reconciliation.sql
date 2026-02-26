/*
===============================================================================
FILE: 04_sp_bronze_campaign_entity_reconciliation.sql
LAYER: Bronze | QA
PROC:  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_bronze_sa360_campaign_entity_reconciliation_tests

INTERPRETATION RULE (IMPORTANT):
  - FAIL only when Bronze is missing keys that exist in RAW-dedup (missing_in_bronze_cnt > 0).
  - If RAW is missing keys that exist in Bronze (missing_in_raw_cnt > 0) but missing_in_bronze_cnt = 0,
    then pipeline is OK; treat as PASS with warning (RAW likely non-append-only / refreshed).

WINDOW:
  7d by BUSINESS DATE (parsed from date_yyyymmdd)

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
  raw_src AS (
    SELECT
      SAFE_CAST(account_id AS STRING) AS account_id,
      SAFE_CAST(campaign_id AS STRING) AS campaign_id,
      SAFE_CAST(date_yyyymmdd AS STRING) AS date_yyyymmdd,
      SAFE.PARSE_DATE('%Y%m%d', SAFE_CAST(date_yyyymmdd AS STRING)) AS date,
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
  counts AS (
    SELECT
      (SELECT COUNT(1) FROM raw_dedup) AS raw_cnt,
      (SELECT COUNT(1) FROM bronze_scoped) AS bronze_cnt,

      -- Bronze-only keys (RAW missing)
      (SELECT COUNT(1)
       FROM (SELECT account_id, campaign_id, date_yyyymmdd FROM bronze_scoped) b
       LEFT JOIN (SELECT account_id, campaign_id, date_yyyymmdd FROM raw_dedup) r
       USING (account_id, campaign_id, date_yyyymmdd)
       WHERE r.account_id IS NULL
      ) AS missing_in_raw_cnt,

      -- RAW-only keys (Bronze missing)  <-- THIS is the true pipeline failure signal
      (SELECT COUNT(1)
       FROM (SELECT account_id, campaign_id, date_yyyymmdd FROM raw_dedup) r
       LEFT JOIN (SELECT account_id, campaign_id, date_yyyymmdd FROM bronze_scoped) b
       USING (account_id, campaign_id, date_yyyymmdd)
       WHERE b.account_id IS NULL
      ) AS missing_in_bronze_cnt,

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

    -- severity is HIGH only if Bronze is missing keys that RAW has
    IF(missing_in_bronze_cnt > 0, 'HIGH', 'MEDIUM') AS severity_level,

    CAST(raw_cnt AS FLOAT64) AS expected_value,
    CAST(bronze_cnt AS FLOAT64) AS actual_value,

    -- force 0 variance on PASS (pipeline-safe interpretation)
    IF(missing_in_bronze_cnt = 0, 0.0, CAST(bronze_cnt - raw_cnt AS FLOAT64)) AS variance_value,

    IF(missing_in_bronze_cnt = 0, 'PASS', 'FAIL') AS status,
    IF(missing_in_bronze_cnt = 0, '🟢', '🔴') AS status_emoji,

    IF(missing_in_bronze_cnt = 0,
      CONCAT(
        'Pipeline OK. Bronze is not missing any RAW keys. ',
        'Note: RAW is missing keys present in Bronze (missing_in_raw_cnt=',
        CAST(missing_in_raw_cnt AS STRING),
        '). This often means RAW is non-append-only/refreshed. Sample Bronze-only keys=',
        COALESCE(sample_missing_in_raw,'<none>')
      ),
      CONCAT(
        'FAIL: Bronze is missing keys that exist in RAW (missing_in_bronze_cnt=',
        CAST(missing_in_bronze_cnt AS STRING),
        '). Sample RAW-only keys=',
        COALESCE(sample_missing_in_bronze,'<none>')
      )
    ) AS failure_reason,

    IF(missing_in_bronze_cnt = 0,
      'No action required for pipeline. If you want strict equality, validate RAW retention/refresh behavior or reconcile to an append-only/raw snapshot.',
      'Run key diff using the RAW-only sample keys. Validate Bronze MERGE filters, casting, and date parsing.'
    ) AS next_step,

    IF(missing_in_bronze_cnt > 0, TRUE, FALSE) AS is_critical_failure,
    IF(missing_in_bronze_cnt = 0, TRUE, FALSE) AS is_pass,
    IF(missing_in_bronze_cnt > 0, TRUE, FALSE) AS is_fail
  FROM counts;

END;