
/*
===============================================================================
FILE: 01_sp_qa_sdi_profound_bronze_visibility_asset_daily.sql
LAYER: Bronze | QA
DATASET: prj-dbi-prd-1.ds_dbi_digitalmedia_automation
PROC:  sp_qa_sdi_profound_bronze_visibility_asset_daily

BRONZE TABLE:
  sdi_profound_bronze_visibility_asset_daily

RAW TABLE:
  prj-dbi-prd-1.ds_dbi_improvado_master.sdi_seo_profound_visibility_asset_daily_tmo

GRAIN:
  account_id + asset_id + date_yyyymmdd

TESTS (5 rows only):
  CRITICAL (3):
    1) freshness_rows_in_last_hours
    2) duplicate_grain_groups_last_N_days
    3) null_key_or_date_rows_last_N_days
  RECONCILIATION (2):
    4) raw_dedup_vs_bronze_row_count_last_N_days
    5) raw_dedup_vs_bronze_metric_sums_last_N_days
===============================================================================
*/
CREATE OR REPLACE PROCEDURE
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_qa_sdi_profound_bronze_visibility_asset_daily`()
OPTIONS(strict_mode=false)
BEGIN
  DECLARE lookback_days INT64 DEFAULT 60;
  DECLARE freshness_hours INT64 DEFAULT 36;

  -- 1) CRITICAL: Freshness (expect >= 1 row in last X hours)
  INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_profound_bronze_test_results`
  WITH s AS (
    SELECT COUNT(1) AS actual_rows
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_profound_bronze_visibility_asset_daily`
    WHERE file_load_datetime >= DATETIME_SUB(CURRENT_DATETIME(), INTERVAL freshness_hours HOUR)
  )
  SELECT
    CURRENT_TIMESTAMP(), CURRENT_DATE(),
    'sdi_profound_bronze_visibility_asset_daily',
    'critical',
    'freshness_rows_in_last_hours',
    'HIGH',
    1,
    CAST(actual_rows AS FLOAT64),
    CAST(actual_rows AS FLOAT64) - 1,
    IF(actual_rows >= 1, 'PASS', 'FAIL'),
    IF(actual_rows >= 1, '🟢', '🔴'),
    IF(actual_rows >= 1, 'Recent Bronze loads exist.', 'No Bronze rows in freshness window.'),
    'Check raw ingestion + Bronze merge job schedule/lookback.',
    IF(actual_rows < 1, TRUE, FALSE),
    IF(actual_rows >= 1, TRUE, FALSE),
    IF(actual_rows < 1, TRUE, FALSE)
  FROM s;

  -- 2) CRITICAL: Duplicate grain groups in last N days (expect 0)
  INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_profound_bronze_test_results`
  WITH d AS (
    SELECT COUNT(1) AS actual_dup_groups
    FROM (
      SELECT account_id, asset_id, date_yyyymmdd, COUNT(*) AS cnt
      FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_profound_bronze_visibility_asset_daily`
      WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL lookback_days DAY)
      GROUP BY 1,2,3
      HAVING cnt > 1
    )
  )
  SELECT
    CURRENT_TIMESTAMP(), CURRENT_DATE(),
    'sdi_profound_bronze_visibility_asset_daily',
    'critical',
    'duplicate_grain_groups_last_N_days',
    'HIGH',
    0,
    CAST(actual_dup_groups AS FLOAT64),
    CAST(actual_dup_groups AS FLOAT64),
    IF(actual_dup_groups = 0, 'PASS', 'FAIL'),
    IF(actual_dup_groups = 0, '🟢', '🔴'),
    IF(actual_dup_groups = 0, 'No duplicate grain groups detected.', 'Duplicate grain groups found in Bronze.'),
    'Check Bronze MERGE key + dedup ORDER BY tie-breakers.',
    IF(actual_dup_groups > 0, TRUE, FALSE),
    IF(actual_dup_groups = 0, TRUE, FALSE),
    IF(actual_dup_groups > 0, TRUE, FALSE)
  FROM d;

  -- 3) CRITICAL: Null key/date rows in last N days (expect 0)
  INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_profound_bronze_test_results`
  WITH n AS (
    SELECT COUNT(1) AS actual_bad_rows
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_profound_bronze_visibility_asset_daily`
    WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL lookback_days DAY)
      AND (
        account_id IS NULL OR asset_id IS NULL OR date_yyyymmdd IS NULL OR date IS NULL
      )
  )
  SELECT
    CURRENT_TIMESTAMP(), CURRENT_DATE(),
    'sdi_profound_bronze_visibility_asset_daily',
    'critical',
    'null_key_or_date_rows_last_N_days',
    'HIGH',
    0,
    CAST(actual_bad_rows AS FLOAT64),
    CAST(actual_bad_rows AS FLOAT64),
    IF(actual_bad_rows = 0, 'PASS', 'FAIL'),
    IF(actual_bad_rows = 0, '🟢', '🔴'),
    IF(actual_bad_rows = 0, 'No null key/date rows found.', 'Null key/date rows found in Bronze.'),
    'Check raw data quality + SAFE.PARSE_DATE + TRIM/NULLIF rules.',
    IF(actual_bad_rows > 0, TRUE, FALSE),
    IF(actual_bad_rows = 0, TRUE, FALSE),
    IF(actual_bad_rows > 0, TRUE, FALSE)
  FROM n;

  -- Reconciliation uses RAW_DEDUP with SAME tie-breakers as Bronze merge
  -- 4) RECON: Row count raw_dedup vs Bronze (expect 0 variance)
  INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_profound_bronze_test_results`
  WITH raw_src AS (
    SELECT
      SAFE_CAST(account_id AS STRING) AS account_id,
      SAFE_CAST(asset_id AS STRING) AS asset_id,
      SAFE_CAST(date_yyyymmdd AS STRING) AS date_yyyymmdd,
      SAFE.PARSE_DATE('%Y%m%d', SAFE_CAST(date_yyyymmdd AS STRING)) AS date,
      SAFE_CAST(__insert_date AS INT64) AS insert_date,
      SAFE_CAST(File_Load_datetime AS DATETIME) AS file_load_datetime,
      SAFE_CAST(Filename AS STRING) AS filename
    FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_seo_profound_visibility_asset_daily_tmo`
    WHERE SAFE.PARSE_DATE('%Y%m%d', SAFE_CAST(date_yyyymmdd AS STRING))
      >= DATE_SUB(CURRENT_DATE(), INTERVAL lookback_days DAY)
  ),
  raw_dedup AS (
    SELECT * EXCEPT(rn)
    FROM (
      SELECT
        r.*,
        ROW_NUMBER() OVER (
          PARTITION BY account_id, asset_id, date_yyyymmdd
          ORDER BY file_load_datetime DESC, filename DESC, insert_date DESC
        ) AS rn
      FROM raw_src r
      WHERE date IS NOT NULL AND account_id IS NOT NULL AND asset_id IS NOT NULL AND date_yyyymmdd IS NOT NULL
    )
    WHERE rn = 1
  ),
  agg_raw AS (SELECT COUNT(1) AS expected_rows FROM raw_dedup),
  agg_bronze AS (
    SELECT COUNT(1) AS actual_rows
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_profound_bronze_visibility_asset_daily`
    WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL lookback_days DAY)
  ),
  f AS (SELECT * FROM agg_raw CROSS JOIN agg_bronze)
  SELECT
    CURRENT_TIMESTAMP(), CURRENT_DATE(),
    'sdi_profound_bronze_visibility_asset_daily',
    'reconciliation',
    'raw_dedup_vs_bronze_row_count_last_N_days',
    'HIGH',
    CAST(expected_rows AS FLOAT64),
    CAST(actual_rows AS FLOAT64),
    CAST(actual_rows AS FLOAT64) - CAST(expected_rows AS FLOAT64),
    IF(expected_rows = actual_rows, 'PASS', 'FAIL'),
    IF(expected_rows = actual_rows, '🟢', '🔴'),
    CONCAT('expected(raw_dedup)=',CAST(expected_rows AS STRING),', actual(bronze)=',CAST(actual_rows AS STRING)),
    'If FAIL: confirm Bronze lookback covers late files; check dedup ordering + MERGE key.',
    IF(expected_rows <> actual_rows, TRUE, FALSE),
    IF(expected_rows = actual_rows, TRUE, FALSE),
    IF(expected_rows <> actual_rows, TRUE, FALSE)
  FROM f;

  -- 5) RECON: Metric sums raw_dedup vs Bronze (expect 0 variance)
  INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_profound_bronze_test_results`
  WITH raw_src AS (
    SELECT
      SAFE_CAST(account_id AS STRING) AS account_id,
      SAFE_CAST(asset_id AS STRING) AS asset_id,
      SAFE_CAST(date_yyyymmdd AS STRING) AS date_yyyymmdd,
      SAFE.PARSE_DATE('%Y%m%d', SAFE_CAST(date_yyyymmdd AS STRING)) AS date,
      SAFE_CAST(executions AS FLOAT64) AS executions,
      SAFE_CAST(mentions_count AS FLOAT64) AS mentions_count,
      SAFE_CAST(share_of_voice AS FLOAT64) AS share_of_voice,
      SAFE_CAST(visibility_score AS FLOAT64) AS visibility_score,
      SAFE_CAST(__insert_date AS INT64) AS insert_date,
      SAFE_CAST(File_Load_datetime AS DATETIME) AS file_load_datetime,
      SAFE_CAST(Filename AS STRING) AS filename
    FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_seo_profound_visibility_asset_daily_tmo`
    WHERE SAFE.PARSE_DATE('%Y%m%d', SAFE_CAST(date_yyyymmdd AS STRING))
      >= DATE_SUB(CURRENT_DATE(), INTERVAL lookback_days DAY)
  ),
  raw_dedup AS (
    SELECT * EXCEPT(rn)
    FROM (
      SELECT
        r.*,
        ROW_NUMBER() OVER (
          PARTITION BY account_id, asset_id, date_yyyymmdd
          ORDER BY file_load_datetime DESC, filename DESC, insert_date DESC
        ) AS rn
      FROM raw_src r
      WHERE date IS NOT NULL AND account_id IS NOT NULL AND asset_id IS NOT NULL AND date_yyyymmdd IS NOT NULL
    )
    WHERE rn = 1
  ),
  agg_raw AS (
    SELECT
      IFNULL(SUM(executions),0) AS sum_exec,
      IFNULL(SUM(mentions_count),0) AS sum_mentions,
      IFNULL(SUM(share_of_voice),0) AS sum_sov,
      IFNULL(SUM(visibility_score),0) AS sum_vis
    FROM raw_dedup
  ),
  agg_bronze AS (
    SELECT
      IFNULL(SUM(executions),0) AS sum_exec,
      IFNULL(SUM(mentions_count),0) AS sum_mentions,
      IFNULL(SUM(share_of_voice),0) AS sum_sov,
      IFNULL(SUM(visibility_score),0) AS sum_vis
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_profound_bronze_visibility_asset_daily`
    WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL lookback_days DAY)
  ),
  f AS (
    SELECT
      r.sum_exec AS exp_exec, b.sum_exec AS act_exec,
      r.sum_mentions AS exp_mentions, b.sum_mentions AS act_mentions,
      r.sum_sov AS exp_sov, b.sum_sov AS act_sov,
      r.sum_vis AS exp_vis, b.sum_vis AS act_vis
    FROM agg_raw r CROSS JOIN agg_bronze b
  )
  SELECT
    CURRENT_TIMESTAMP(), CURRENT_DATE(),
    'sdi_profound_bronze_visibility_asset_daily',
    'reconciliation',
    'raw_dedup_vs_bronze_metric_sums_last_N_days',
    'HIGH',
    (exp_exec + exp_mentions + exp_sov + exp_vis),
    (act_exec + act_mentions + act_sov + act_vis),
    (act_exec + act_mentions + act_sov + act_vis) - (exp_exec + exp_mentions + exp_sov + exp_vis),
    IF(exp_exec=act_exec AND exp_mentions=act_mentions AND exp_sov=act_sov AND exp_vis=act_vis, 'PASS', 'FAIL'),
    IF(exp_exec=act_exec AND exp_mentions=act_mentions AND exp_sov=act_sov AND exp_vis=act_vis, '🟢', '🔴'),
    CONCAT(
      'exec exp=',CAST(exp_exec AS STRING),', act=',CAST(act_exec AS STRING),
      ' | mentions exp=',CAST(exp_mentions AS STRING),', act=',CAST(act_mentions AS STRING),
      ' | sov exp=',CAST(exp_sov AS STRING),', act=',CAST(act_sov AS STRING),
      ' | vis exp=',CAST(exp_vis AS STRING),', act=',CAST(act_vis AS STRING)
    ),
    'If FAIL: validate dedup ordering, check for type casting differences, confirm Bronze source columns match raw.',
    IF(NOT(exp_exec=act_exec AND exp_mentions=act_mentions AND exp_sov=act_sov AND exp_vis=act_vis), TRUE, FALSE),
    IF(exp_exec=act_exec AND exp_mentions=act_mentions AND exp_sov=act_sov AND exp_vis=act_vis, TRUE, FALSE),
    IF(NOT(exp_exec=act_exec AND exp_mentions=act_mentions AND exp_sov=act_sov AND exp_vis=act_vis), TRUE, FALSE)
  FROM f;

END;

