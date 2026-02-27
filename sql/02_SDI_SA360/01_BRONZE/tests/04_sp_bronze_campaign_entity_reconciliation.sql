/*
===============================================================================
FILE: 04_sp_bronze_campaign_entity_reconciliation.sql
LAYER: Bronze | QA
PROC:  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_bronze_sa360_campaign_entity_reconciliation_tests

RECONCILIATION CONTRACT (ENTITY):
  - CRITICAL: Bronze must NOT be missing keys that exist in RAW (same business-date window).
  - INFO/WARN: RAW missing Bronze keys is allowed (RAW may be non-append-only / may drop keys).
  - Output must NOT explode: exactly ONE row inserted into test_results.

WINDOW:
  Business-date window (parsed from date_yyyymmdd) over lookback_days.

RAW SOURCE:
  prj-dbi-prd-1.ds_dbi_improvado_master.google_search_ads_360_beta_campaign_entity_custom_tmo
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

      -- Normalize to TIMESTAMP in QA too (same as Bronze logic)
      COALESCE(
        SAFE_CAST(File_Load_datetime AS TIMESTAMP),
        TIMESTAMP(SAFE_CAST(File_Load_datetime AS DATETIME))
      ) AS file_load_datetime,

      NULLIF(TRIM(SAFE_CAST(Filename AS STRING)), '') AS filename
    FROM `prj-dbi-prd-1.ds_dbi_improvado_master.google_search_ads_360_beta_campaign_entity_custom_tmo`
  ),

  raw_clean AS (
    SELECT *
    FROM raw_src
    WHERE date IS NOT NULL
      AND date >= DATE_SUB(CURRENT_DATE(), INTERVAL lookback_days DAY)
  ),

  raw_dedup AS (
    SELECT * EXCEPT(rn)
    FROM (
      SELECT
        raw_clean.*,
        ROW_NUMBER() OVER (
          PARTITION BY account_id, campaign_id, date_yyyymmdd
          ORDER BY file_load_datetime DESC NULLS LAST, filename DESC
        ) AS rn
      FROM raw_clean
    )
    WHERE rn = 1
  ),

  bronze_scoped AS (
    SELECT
      account_id,
      campaign_id,
      date_yyyymmdd
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_campaign_entity`
    WHERE date IS NOT NULL
      AND date >= DATE_SUB(CURRENT_DATE(), INTERVAL lookback_days DAY)
  ),

  raw_keys AS (
    SELECT account_id, campaign_id, date_yyyymmdd FROM raw_dedup
  ),
  bronze_keys AS (
    SELECT account_id, campaign_id, date_yyyymmdd FROM bronze_scoped
  ),

  missing_in_bronze AS (
    SELECT r.*
    FROM raw_keys r
    LEFT JOIN bronze_keys b
      USING (account_id, campaign_id, date_yyyymmdd)
    WHERE b.account_id IS NULL
  ),

  missing_in_raw AS (
    SELECT b.*
    FROM bronze_keys b
    LEFT JOIN raw_keys r
      USING (account_id, campaign_id, date_yyyymmdd)
    WHERE r.account_id IS NULL
  ),

  counts AS (
    SELECT
      (SELECT COUNT(1) FROM raw_dedup) AS raw_cnt,
      (SELECT COUNT(1) FROM bronze_scoped) AS bronze_cnt,
      (SELECT COUNT(1) FROM missing_in_bronze) AS missing_in_bronze_cnt,
      (SELECT COUNT(1) FROM missing_in_raw) AS missing_in_raw_cnt,

      ARRAY_TO_STRING(
        ARRAY(
          SELECT CONCAT(account_id,'|',campaign_id,'|',date_yyyymmdd)
          FROM missing_in_bronze
          ORDER BY account_id, campaign_id, date_yyyymmdd
          LIMIT 10
        ),
        ', '
      ) AS sample_missing_in_bronze,

      ARRAY_TO_STRING(
        ARRAY(
          SELECT CONCAT(account_id,'|',campaign_id,'|',date_yyyymmdd)
          FROM missing_in_raw
          ORDER BY account_id, campaign_id, date_yyyymmdd
          LIMIT 10
        ),
        ', '
      ) AS sample_missing_in_raw
  )

  SELECT
    CURRENT_TIMESTAMP() AS test_run_timestamp,
    CURRENT_DATE() AS test_date,
    'sdi_bronze_sa360_campaign_entity' AS table_name,
    'reconciliation' AS test_layer,
    CONCAT('Row Count Reconciliation (Entity Bronze vs RAW-dedup, ', CAST(lookback_days AS STRING), 'd by business date)') AS test_name,
    'HIGH' AS severity_level,

    CAST(raw_cnt AS FLOAT64) AS expected_value,
    CAST(bronze_cnt AS FLOAT64) AS actual_value,
    CAST(bronze_cnt - raw_cnt AS FLOAT64) AS variance_value,

    -- FAIL only if RAW has keys missing in Bronze (true pipeline issue)
    IF(missing_in_bronze_cnt > 0, 'FAIL', 'PASS') AS status,
    IF(missing_in_bronze_cnt > 0, '🔴', '🟢') AS status_emoji,

    CONCAT(
      IF(missing_in_bronze_cnt > 0,
        'Bronze is missing RAW keys in the business-date window. ',
        'Bronze contains all RAW keys in the business-date window. '
      ),
      'missing_in_bronze_cnt=', CAST(missing_in_bronze_cnt AS STRING),
      ', missing_in_raw_cnt=', CAST(missing_in_raw_cnt AS STRING),
      IF(missing_in_bronze_cnt > 0,
        CONCAT('. Missing-in-Bronze sample (<=10)=', IFNULL(NULLIF(sample_missing_in_bronze,''), '<none>')),
        ''
      ),
      IF(missing_in_raw_cnt > 0,
        CONCAT('. Bronze-only sample (<=10)=', IFNULL(NULLIF(sample_missing_in_raw,''), '<none>')),
        ''
      )
    ) AS failure_reason,

    IF(missing_in_bronze_cnt > 0,
      'Investigate the sample keys: confirm RAW has them (same business date window), then validate Bronze MERGE filter + dedupe ordering + key casting.',
      IF(missing_in_raw_cnt > 0,
        'RAW appears non-append-only (drops/overwrites). This is informational unless you require Bronze to mirror RAW deletes.',
        'No action required.'
      )
    ) AS next_step,

    IF(missing_in_bronze_cnt > 0, TRUE, FALSE) AS is_critical_failure,
    IF(missing_in_bronze_cnt = 0, TRUE, FALSE) AS is_pass,
    IF(missing_in_bronze_cnt > 0, TRUE, FALSE) AS is_fail

  FROM counts;

END;
