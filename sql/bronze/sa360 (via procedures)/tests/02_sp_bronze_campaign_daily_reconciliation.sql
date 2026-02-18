/*
===============================================================================
FILE: 02_sp_bronze_campaign_daily_reconciliation.sql
LAYER: Bronze | QA
PROC:  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_bronze_sa360_campaign_daily_reconciliation_tests

TABLE UNDER TEST:
  Bronze Daily:
    prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_campaign_daily

RECONCILIATION SOURCE (RAW):
  prj-dbi-prd-1.ds_dbi_improvado_master.google_search_ads_360_campaigns_tmo

WHY RECONCILE BRONZE VS RAW:
  Bronze is a cleaned + typed representation of RAW.
  We must ensure no values are lost/duplicated after:
    - date parsing
    - dedup in merge key
    - metric casting

IMPORTANT:
  To be apples-to-apples, we dedupe RAW using the SAME rules as Bronze MERGE:
    key = (account_id, campaign_id, date_yyyymmdd)
    tie-breaker = file_load_datetime DESC, filename DESC
===============================================================================
*/

CREATE OR REPLACE PROCEDURE
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_bronze_sa360_campaign_daily_reconciliation_tests`()
OPTIONS(strict_mode=false)
BEGIN
  DECLARE lookback_days INT64 DEFAULT 7;
  DECLARE tolerance FLOAT64 DEFAULT 0.000001;

  -- ---------------------------------------------------------------------------
  -- Helper CTE: RAW deduped in-window (mirror Bronze MERGE dedupe)
  -- ---------------------------------------------------------------------------
  -- TEST 1: Row-count reconciliation (Bronze vs RAW-dedup)
  -- Expect: counts match exactly
  INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_test_results`
  WITH raw_src AS (
    SELECT
      SAFE_CAST(account_id AS STRING) AS account_id,
      SAFE_CAST(campaign_id AS STRING) AS campaign_id,
      SAFE_CAST(date_yyyymmdd AS STRING) AS date_yyyymmdd,
      SAFE.PARSE_DATE('%Y%m%d', SAFE_CAST(date_yyyymmdd AS STRING)) AS date,
      SAFE_CAST(File_Load_datetime AS DATETIME) AS file_load_datetime,
      NULLIF(TRIM(SAFE_CAST(Filename AS STRING)), '') AS filename
    FROM `prj-dbi-prd-1.ds_dbi_improvado_master.google_search_ads_360_campaigns_tmo`
    WHERE SAFE.PARSE_DATE('%Y%m%d', SAFE_CAST(date_yyyymmdd AS STRING))
          >= DATE_SUB(CURRENT_DATE(), INTERVAL lookback_days DAY)
  ),
  raw_clean AS (
    SELECT * FROM raw_src WHERE date IS NOT NULL
  ),
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
       FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_campaign_daily`
       WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL lookback_days DAY)
      ) AS bronze_cnt,
      (SELECT COUNT(1) FROM raw_dedup) AS raw_cnt
  )
  SELECT
    CURRENT_TIMESTAMP(),
    CURRENT_DATE(),
    'sdi_bronze_sa360_campaign_daily',
    'reconciliation',
    'Row Count Reconciliation (Bronze vs RAW-dedup, 7d)',
    'HIGH',
    CAST(raw_cnt AS FLOAT64) AS expected_value,
    CAST(bronze_cnt AS FLOAT64) AS actual_value,
    CAST(bronze_cnt - raw_cnt AS FLOAT64) AS variance_value,
    IF(bronze_cnt = raw_cnt, 'PASS', 'FAIL') AS status,
    IF(bronze_cnt = raw_cnt, '🟢', '🔴') AS status_emoji,
    IF(bronze_cnt = raw_cnt,
      'Row counts match exactly.',
      'Row counts do NOT match. Bronze may be missing rows or duplicating rows vs RAW-dedup.'
    ) AS failure_reason,
    IF(bronze_cnt = raw_cnt,
      'No action required.',
      'Compare missing keys (account_id,campaign_id,date_yyyymmdd). Validate MERGE filters and date parsing.'
    ) AS next_step,
    IF(bronze_cnt != raw_cnt, TRUE, FALSE) AS is_critical_failure,
    IF(bronze_cnt = raw_cnt, TRUE, FALSE) AS is_pass,
    IF(bronze_cnt != raw_cnt, TRUE, FALSE) AS is_fail
  FROM counts;

  -- ---------------------------------------------------------------------------
  -- TEST 2: Metric reconciliation (cart_start) over 7d (Bronze vs RAW-dedup)
  -- Expect: sums match within tolerance
  -- ---------------------------------------------------------------------------
  INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_test_results`
  WITH raw_src AS (
    SELECT
      SAFE_CAST(account_id AS STRING) AS account_id,
      SAFE_CAST(campaign_id AS STRING) AS campaign_id,
      SAFE_CAST(date_yyyymmdd AS STRING) AS date_yyyymmdd,
      SAFE.PARSE_DATE('%Y%m%d', SAFE_CAST(date_yyyymmdd AS STRING)) AS date,
      SAFE_CAST(File_Load_datetime AS DATETIME) AS file_load_datetime,
      NULLIF(TRIM(SAFE_CAST(Filename AS STRING)), '') AS filename,
      SAFE_CAST(cart__start_ AS FLOAT64) AS cart_start_raw
    FROM `prj-dbi-prd-1.ds_dbi_improvado_master.google_search_ads_360_campaigns_tmo`
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
  sums AS (
    SELECT
      (SELECT SUM(COALESCE(cart_start,0))
       FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_campaign_daily`
       WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL lookback_days DAY)
      ) AS bronze_sum,
      (SELECT SUM(COALESCE(cart_start_raw,0)) FROM raw_dedup) AS raw_sum
  )
  SELECT
    CURRENT_TIMESTAMP(),
    CURRENT_DATE(),
    'sdi_bronze_sa360_campaign_daily',
    'reconciliation',
    'Cart Start Reconciliation (Bronze vs RAW-dedup, 7d)',
    'HIGH',
    raw_sum AS expected_value,
    bronze_sum AS actual_value,
    (bronze_sum - raw_sum) AS variance_value,
    IF(ABS(bronze_sum - raw_sum) <= tolerance, 'PASS', 'FAIL') AS status,
    IF(ABS(bronze_sum - raw_sum) <= tolerance, '🟢', '🔴') AS status_emoji,
    IF(ABS(bronze_sum - raw_sum) <= tolerance,
      'Cart Start sums reconcile successfully.',
      'Cart Start sums do NOT reconcile (beyond tolerance).'
    ) AS failure_reason,
    IF(ABS(bronze_sum - raw_sum) <= tolerance,
      'No action required.',
      'Validate dedupe logic + ensure RAW cart__start_ mapped to Bronze cart_start correctly.'
    ) AS next_step,
    IF(ABS(bronze_sum - raw_sum) > tolerance, TRUE, FALSE) AS is_critical_failure,
    IF(ABS(bronze_sum - raw_sum) <= tolerance, TRUE, FALSE) AS is_pass,
    IF(ABS(bronze_sum - raw_sum) > tolerance, TRUE, FALSE) AS is_fail
  FROM sums;

  -- ---------------------------------------------------------------------------
  -- TEST 3: Metric reconciliation (postpaid_pspv) over 7d
  -- Expect: sums match within tolerance
  -- ---------------------------------------------------------------------------
  INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_test_results`
  WITH raw_src AS (
    SELECT
      SAFE_CAST(account_id AS STRING) AS account_id,
      SAFE_CAST(campaign_id AS STRING) AS campaign_id,
      SAFE_CAST(date_yyyymmdd AS STRING) AS date_yyyymmdd,
      SAFE.PARSE_DATE('%Y%m%d', SAFE_CAST(date_yyyymmdd AS STRING)) AS date,
      SAFE_CAST(File_Load_datetime AS DATETIME) AS file_load_datetime,
      NULLIF(TRIM(SAFE_CAST(Filename AS STRING)), '') AS filename,
      SAFE_CAST(postpaid_pspv_ AS FLOAT64) AS postpaid_pspv_raw
    FROM `prj-dbi-prd-1.ds_dbi_improvado_master.google_search_ads_360_campaigns_tmo`
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
  sums AS (
    SELECT
      (SELECT SUM(COALESCE(postpaid_pspv,0))
       FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_campaign_daily`
       WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL lookback_days DAY)
      ) AS bronze_sum,
      (SELECT SUM(COALESCE(postpaid_pspv_raw,0)) FROM raw_dedup) AS raw_sum
  )
  SELECT
    CURRENT_TIMESTAMP(),
    CURRENT_DATE(),
    'sdi_bronze_sa360_campaign_daily',
    'reconciliation',
    'Postpaid PSPV Reconciliation (Bronze vs RAW-dedup, 7d)',
    'HIGH',
    raw_sum AS expected_value,
    bronze_sum AS actual_value,
    (bronze_sum - raw_sum) AS variance_value,
    IF(ABS(bronze_sum - raw_sum) <= tolerance, 'PASS', 'FAIL') AS status,
    IF(ABS(bronze_sum - raw_sum) <= tolerance, '🟢', '🔴') AS status_emoji,
    IF(ABS(bronze_sum - raw_sum) <= tolerance,
      'Postpaid PSPV sums reconcile successfully.',
      'Postpaid PSPV sums do NOT reconcile (beyond tolerance).'
    ) AS failure_reason,
    IF(ABS(bronze_sum - raw_sum) <= tolerance,
      'No action required.',
      'Validate mapping raw.postpaid_pspv_ -> bronze.postpaid_pspv and dedupe logic.'
    ) AS next_step,
    IF(ABS(bronze_sum - raw_sum) > tolerance, TRUE, FALSE) AS is_critical_failure,
    IF(ABS(bronze_sum - raw_sum) <= tolerance, TRUE, FALSE) AS is_pass,
    IF(ABS(bronze_sum - raw_sum) > tolerance, TRUE, FALSE) AS is_fail
  FROM sums;

END;
