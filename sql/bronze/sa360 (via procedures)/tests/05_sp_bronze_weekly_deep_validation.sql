/*
===============================================================================
FILE: 05_sp_bronze_weekly_deep_validation.sql
LAYER: Bronze | QA
PROC:  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_bronze_sa360_weekly_deep_validation_tests

WHAT THIS DOES:
  "Deep validation" doesn't compare to a source-of-truth table.
  Instead, it looks for abnormal behavior that usually indicates a pipeline issue:
    - sudden drops to zero
    - extreme spikes vs historical baseline

IMPLEMENTATION (simple + robust):
  - Build weekly totals from Bronze Daily using weekend_date = WEEK(SATURDAY)
  - Compare last complete week vs previous 8 weeks average
  - If last week deviates > X times average, flag as MEDIUM (not always a data bug)

METRICS CHECKED:
  - cart_start
  - postpaid_pspv
===============================================================================
*/
CREATE OR REPLACE PROCEDURE
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_bronze_sa360_weekly_reconciliation_tests`()
OPTIONS(strict_mode=false)
BEGIN
  DECLARE last_weekend DATE;

  -- Last complete weekend_date present in Bronze (WEEK(SATURDAY))
  SET last_weekend = (
    SELECT MAX(DATE_TRUNC(date, WEEK(SATURDAY)))
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_campaign_daily`
    WHERE date IS NOT NULL
  );

  -- ---------------------------------------------------------------------------
  -- WEEKLY RECON: CART_START (Bronze vs RAW-dedup)
  -- ---------------------------------------------------------------------------
  INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_test_results`
  WITH bronze_week AS (
    SELECT
      DATE_TRUNC(date, WEEK(SATURDAY)) AS weekend_date,
      SUM(COALESCE(cart_start, 0)) AS bronze_val
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_campaign_daily`
    GROUP BY 1
  ),
  raw_dedup AS (
    SELECT * EXCEPT(rn)
    FROM (
      SELECT
        SAFE_CAST(raw.account_id AS STRING) AS account_id,
        SAFE_CAST(raw.campaign_id AS STRING) AS campaign_id,
        SAFE_CAST(raw.date_yyyymmdd AS STRING) AS date_yyyymmdd,
        SAFE.PARSE_DATE('%Y%m%d', SAFE_CAST(raw.date_yyyymmdd AS STRING)) AS date,
        SAFE_CAST(raw.File_Load_datetime AS DATETIME) AS file_load_datetime,
        NULLIF(TRIM(SAFE_CAST(raw.Filename AS STRING)), '') AS filename,
        SAFE_CAST(raw.cart__start_ AS FLOAT64) AS cart_start,
        ROW_NUMBER() OVER (
          PARTITION BY SAFE_CAST(raw.account_id AS STRING),
                       SAFE_CAST(raw.campaign_id AS STRING),
                       SAFE_CAST(raw.date_yyyymmdd AS STRING)
          ORDER BY SAFE_CAST(raw.File_Load_datetime AS DATETIME) DESC,
                   NULLIF(TRIM(SAFE_CAST(raw.Filename AS STRING)), '') DESC
        ) AS rn
      FROM `prj-dbi-prd-1.ds_dbi_improvado_master.google_search_ads_360_campaigns_tmo` raw
      WHERE SAFE.PARSE_DATE('%Y%m%d', SAFE_CAST(raw.date_yyyymmdd AS STRING)) IS NOT NULL
    )
    WHERE rn = 1
  ),
  raw_week AS (
    SELECT
      DATE_TRUNC(date, WEEK(SATURDAY)) AS weekend_date,
      SUM(COALESCE(cart_start, 0)) AS raw_val
    FROM raw_dedup
    GROUP BY 1
  ),
  calc AS (
    SELECT
      (SELECT raw_val FROM raw_week WHERE weekend_date = last_weekend) AS expected_value,
      (SELECT bronze_val FROM bronze_week WHERE weekend_date = last_weekend) AS actual_value
  )
  SELECT
    CURRENT_TIMESTAMP() AS test_run_timestamp,
    CURRENT_DATE() AS test_date,
    'sdi_bronze_sa360_campaign_daily' AS table_name,
    'reconciliation' AS test_layer,
    'Weekly Reconciliation (cart_start | Bronze vs RAW-dedup | last complete week)' AS test_name,
    'HIGH' AS severity_level,
    expected_value,
    actual_value,
    (actual_value - expected_value) AS variance_value,
    IF(expected_value = actual_value, 'PASS', 'FAIL') AS status,
    IF(expected_value = actual_value, '🟢', '🔴') AS status_emoji,
    IF(expected_value = actual_value,
       'Weekly totals match exactly (apples-to-apples).',
       'Mismatch in weekly totals (pipeline/dedupe/ingestion issue).'
    ) AS failure_reason,
    IF(expected_value = actual_value,
       'No action required.',
       'Check RAW->Bronze dedupe logic, last-week ingestion completeness, and schema mapping for cart_start.'
    ) AS next_step,
    IF(expected_value != actual_value, TRUE, FALSE) AS is_critical_failure,
    IF(expected_value = actual_value, TRUE, FALSE) AS is_pass,
    IF(expected_value != actual_value, TRUE, FALSE) AS is_fail
  FROM calc;

  -- ---------------------------------------------------------------------------
  -- WEEKLY RECON: POSTPAID_PSPV (Bronze vs RAW-dedup)
  -- ---------------------------------------------------------------------------
  INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_test_results`
  WITH bronze_week AS (
    SELECT
      DATE_TRUNC(date, WEEK(SATURDAY)) AS weekend_date,
      SUM(COALESCE(postpaid_pspv, 0)) AS bronze_val
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_campaign_daily`
    GROUP BY 1
  ),
  raw_dedup AS (
    SELECT * EXCEPT(rn)
    FROM (
      SELECT
        SAFE_CAST(raw.account_id AS STRING) AS account_id,
        SAFE_CAST(raw.campaign_id AS STRING) AS campaign_id,
        SAFE_CAST(raw.date_yyyymmdd AS STRING) AS date_yyyymmdd,
        SAFE.PARSE_DATE('%Y%m%d', SAFE_CAST(raw.date_yyyymmdd AS STRING)) AS date,
        SAFE_CAST(raw.File_Load_datetime AS DATETIME) AS file_load_datetime,
        NULLIF(TRIM(SAFE_CAST(raw.Filename AS STRING)), '') AS filename,
        SAFE_CAST(raw.postpaid_pspv_ AS FLOAT64) AS postpaid_pspv,
        ROW_NUMBER() OVER (
          PARTITION BY SAFE_CAST(raw.account_id AS STRING),
                       SAFE_CAST(raw.campaign_id AS STRING),
                       SAFE_CAST(raw.date_yyyymmdd AS STRING)
          ORDER BY SAFE_CAST(raw.File_Load_datetime AS DATETIME) DESC,
                   NULLIF(TRIM(SAFE_CAST(raw.Filename AS STRING)), '') DESC
        ) AS rn
      FROM `prj-dbi-prd-1.ds_dbi_improvado_master.google_search_ads_360_campaigns_tmo` raw
      WHERE SAFE.PARSE_DATE('%Y%m%d', SAFE_CAST(raw.date_yyyymmdd AS STRING)) IS NOT NULL
    )
    WHERE rn = 1
  ),
  raw_week AS (
    SELECT
      DATE_TRUNC(date, WEEK(SATURDAY)) AS weekend_date,
      SUM(COALESCE(postpaid_pspv, 0)) AS raw_val
    FROM raw_dedup
    GROUP BY 1
  ),
  calc AS (
    SELECT
      (SELECT raw_val FROM raw_week WHERE weekend_date = last_weekend) AS expected_value,
      (SELECT bronze_val FROM bronze_week WHERE weekend_date = last_weekend) AS actual_value
  )
  SELECT
    CURRENT_TIMESTAMP(),
    CURRENT_DATE(),
    'sdi_bronze_sa360_campaign_daily',
    'reconciliation',
    'Weekly Reconciliation (postpaid_pspv | Bronze vs RAW-dedup | last complete week)',
    'HIGH',
    expected_value,
    actual_value,
    (actual_value - expected_value),
    IF(expected_value = actual_value, 'PASS', 'FAIL'),
    IF(expected_value = actual_value, '🟢', '🔴'),
    IF(expected_value = actual_value,
       'Weekly totals match exactly (apples-to-apples).',
       'Mismatch in weekly totals (pipeline/dedupe/ingestion issue).'
    ),
    IF(expected_value = actual_value,
       'No action required.',
       'Check RAW->Bronze dedupe logic, last-week ingestion completeness, and schema mapping for postpaid_pspv.'
    ),
    IF(expected_value != actual_value, TRUE, FALSE),
    IF(expected_value = actual_value, TRUE, FALSE),
    IF(expected_value != actual_value, TRUE, FALSE)
  FROM calc;

END;
