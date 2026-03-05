/*===============================================================================
FILE: 01_sp_qa_sdi_profound_bronze_visibility_asset_daily.sql
LAYER: Bronze | QA
DATASET: prj-dbi-prd-1.ds_dbi_digitalmedia_automation
PROC:  sp_qa_sdi_profound_bronze_visibility_asset_daily

PURPOSE:
  3 critical + 2 reconciliation tests for:
    sdi_profound_bronze_visibility_asset_daily

ALIGNMENT:
  - RAW source + filter matches merge SP (File_Load_datetime lookback).
  - RAW dedup grain + ORDER BY matches merge SP:
      PARTITION BY (account_id, asset_id, date_yyyymmdd)
      ORDER BY file_load_datetime DESC, filename DESC, insert_date DESC
  - Bronze comparison window matches merge SP lookback (file_load_datetime).

OUTPUT:
  Exactly 5 rows inserted into sdi_profound_bronze_test_results per run.
===============================================================================*/
CREATE OR REPLACE PROCEDURE
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_qa_sdi_profound_bronze_visibility_asset_daily`()
OPTIONS(strict_mode=false)
BEGIN
  DECLARE lookback_days INT64 DEFAULT 60;
  DECLARE freshness_days INT64 DEFAULT 2;

  INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_profound_bronze_test_results`
  (
    test_run_timestamp, test_date,
    table_name, test_layer, test_name, severity_level,
    expected_value, actual_value, variance_value,
    status, status_emoji, failure_reason, next_step,
    is_critical_failure, is_pass, is_fail
  )
  WITH
  params AS (
    SELECT
      CURRENT_TIMESTAMP() AS run_ts,
      CURRENT_DATE() AS run_date,
      DATETIME_SUB(CURRENT_DATETIME(), INTERVAL lookback_days DAY) AS window_start_dt,
      DATETIME_SUB(CURRENT_DATETIME(), INTERVAL freshness_days DAY) AS freshness_start_dt
  ),
  bronze_window AS (
    SELECT *
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_profound_bronze_visibility_asset_daily`
    WHERE file_load_datetime IS NOT NULL
      AND file_load_datetime >= (SELECT window_start_dt FROM params)
  ),
  -- RAW filtered exactly like merge SP
  raw_src AS (
    SELECT
      SAFE_CAST(raw.account_id AS STRING) AS account_id,
      NULLIF(TRIM(SAFE_CAST(raw.account_name AS STRING)), '') AS account_name,
      NULLIF(TRIM(SAFE_CAST(raw.asset_id AS STRING)), '') AS asset_id,
      NULLIF(TRIM(SAFE_CAST(raw.asset_name AS STRING)), '') AS asset_name,
      SAFE_CAST(raw.date_yyyymmdd AS STRING) AS date_yyyymmdd,
      SAFE.PARSE_DATE('%Y%m%d', SAFE_CAST(raw.date_yyyymmdd AS STRING)) AS date,
      SAFE_CAST(raw.date AS INT64) AS raw_date_int64,
      SAFE_CAST(raw.executions AS FLOAT64) AS executions,
      SAFE_CAST(raw.mentions_count AS FLOAT64) AS mentions_count,
      SAFE_CAST(raw.share_of_voice AS FLOAT64) AS share_of_voice,
      SAFE_CAST(raw.visibility_score AS FLOAT64) AS visibility_score,
      SAFE_CAST(raw.__insert_date AS INT64) AS insert_date,
      SAFE_CAST(raw.File_Load_datetime AS DATETIME) AS file_load_datetime,
      NULLIF(TRIM(SAFE_CAST(raw.Filename AS STRING)), '') AS filename
    FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_seo_profound_visibility_asset_daily_tmo` raw
    WHERE SAFE_CAST(raw.File_Load_datetime AS DATETIME) IS NOT NULL
      AND SAFE_CAST(raw.File_Load_datetime AS DATETIME) >= (SELECT window_start_dt FROM params)
  ),
  raw_cleaned AS (
    SELECT *
    FROM raw_src
    WHERE date IS NOT NULL
      AND account_id IS NOT NULL
      AND asset_id IS NOT NULL
      AND date_yyyymmdd IS NOT NULL
  ),
  raw_dedup AS (
    SELECT * EXCEPT(rn)
    FROM (
      SELECT
        c.*,
        ROW_NUMBER() OVER (
          PARTITION BY account_id, asset_id, date_yyyymmdd
          ORDER BY file_load_datetime DESC, filename DESC, insert_date DESC
        ) AS rn
      FROM raw_cleaned c
    )
    WHERE rn = 1
  ),
  -- -------------------------
  -- Critical metrics
  -- -------------------------
  crit_dup AS (
    SELECT COUNT(*) AS dup_groups
    FROM (
      SELECT account_id, asset_id, date_yyyymmdd, COUNT(*) AS cnt
      FROM bronze_window
      GROUP BY 1,2,3
      HAVING COUNT(*) > 1
    )
  ),
  crit_nulls AS (
    SELECT COUNT(*) AS null_bad_rows
    FROM bronze_window
    WHERE account_id IS NULL
       OR asset_id IS NULL
       OR date_yyyymmdd IS NULL
       OR date IS NULL
  ),
  crit_fresh AS (
    SELECT
      MAX(file_load_datetime) AS max_fldt
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_profound_bronze_visibility_asset_daily`
  ),
  -- -------------------------
  -- Reconciliation metrics
  -- -------------------------
  rec_rowcnt AS (
    SELECT
      (SELECT COUNT(*) FROM raw_dedup) AS expected_rows,
      (SELECT COUNT(*) FROM bronze_window) AS actual_rows
  ),
  rec_metrics AS (
    SELECT
      (SELECT COALESCE(SUM(executions),0) FROM raw_dedup)
    + (SELECT COALESCE(SUM(mentions_count),0) FROM raw_dedup)
    + (SELECT COALESCE(SUM(share_of_voice),0) FROM raw_dedup)
    + (SELECT COALESCE(SUM(visibility_score),0) FROM raw_dedup) AS expected_sum,
      (SELECT COALESCE(SUM(executions),0) FROM bronze_window)
    + (SELECT COALESCE(SUM(mentions_count),0) FROM bronze_window)
    + (SELECT COALESCE(SUM(share_of_voice),0) FROM bronze_window)
    + (SELECT COALESCE(SUM(visibility_score),0) FROM bronze_window) AS actual_sum,
      (SELECT COALESCE(SUM(executions),0) FROM raw_dedup) AS exp_exec,
      (SELECT COALESCE(SUM(executions),0) FROM bronze_window) AS act_exec,
      (SELECT COALESCE(SUM(mentions_count),0) FROM raw_dedup) AS exp_mentions,
      (SELECT COALESCE(SUM(mentions_count),0) FROM bronze_window) AS act_mentions,
      (SELECT COALESCE(SUM(share_of_voice),0) FROM raw_dedup) AS exp_sov,
      (SELECT COALESCE(SUM(share_of_voice),0) FROM bronze_window) AS act_sov,
      (SELECT COALESCE(SUM(visibility_score),0) FROM raw_dedup) AS exp_vis,
      (SELECT COALESCE(SUM(visibility_score),0) FROM bronze_window) AS act_vis
  )
  -- =====================================================================
  -- Insert exactly 5 rows
  -- =====================================================================
  SELECT
    (SELECT run_ts FROM params) AS test_run_timestamp,
    (SELECT run_date FROM params) AS test_date,
    'sdi_profound_bronze_visibility_asset_daily' AS table_name,
    'critical' AS test_layer,
    'duplicate_grain_groups_last_N_days' AS test_name,
    'HIGH' AS severity_level,
    0.0 AS expected_value,
    CAST((SELECT dup_groups FROM crit_dup) AS FLOAT64) AS actual_value,
    CAST((SELECT dup_groups FROM crit_dup) AS FLOAT64) - 0.0 AS variance_value,
    IF((SELECT dup_groups FROM crit_dup)=0,'PASS','FAIL') AS status,
    IF((SELECT dup_groups FROM crit_dup)=0,'🟢','🔴') AS status_emoji,
    IF((SELECT dup_groups FROM crit_dup)=0,
      'No duplicate grain groups detected.',
      'Duplicate grain groups found in Bronze window.'
    ) AS failure_reason,
    'If FAIL: check MERGE key + dedup ORDER BY tie-breakers.' AS next_step,
    IF((SELECT dup_groups FROM crit_dup)=0,FALSE,TRUE) AS is_critical_failure,
    IF((SELECT dup_groups FROM crit_dup)=0,TRUE,FALSE) AS is_pass,
    IF((SELECT dup_groups FROM crit_dup)=0,FALSE,TRUE) AS is_fail

  UNION ALL
  SELECT
    (SELECT run_ts FROM params),
    (SELECT run_date FROM params),
    'sdi_profound_bronze_visibility_asset_daily',
    'critical',
    'null_key_or_date_rows_last_N_days',
    'HIGH',
    0.0,
    CAST((SELECT null_bad_rows FROM crit_nulls) AS FLOAT64),
    CAST((SELECT null_bad_rows FROM crit_nulls) AS FLOAT64) - 0.0,
    IF((SELECT null_bad_rows FROM crit_nulls)=0,'PASS','FAIL'),
    IF((SELECT null_bad_rows FROM crit_nulls)=0,'🟢','🔴'),
    IF((SELECT null_bad_rows FROM crit_nulls)=0,
      'No null key/date rows found.',
      'Null key/date rows found in Bronze window.'
    ),
    'If FAIL: verify TRIM/NULLIF + SAFE.PARSE_DATE + key filters in merge.',
    IF((SELECT null_bad_rows FROM crit_nulls)=0,FALSE,TRUE),
    IF((SELECT null_bad_rows FROM crit_nulls)=0,TRUE,FALSE),
    IF((SELECT null_bad_rows FROM crit_nulls)=0,FALSE,TRUE)

  UNION ALL
  SELECT
    (SELECT run_ts FROM params),
    (SELECT run_date FROM params),
    'sdi_profound_bronze_visibility_asset_daily',
    'critical',
    'freshness_max_file_load_datetime_within_last_days',
    'HIGH',
    1.0,
    IF(
      (SELECT max_fldt FROM crit_fresh) IS NOT NULL
      AND (SELECT max_fldt FROM crit_fresh) >= (SELECT freshness_start_dt FROM params),
      1.0, 0.0
    ),
    IF(
      (SELECT max_fldt FROM crit_fresh) IS NOT NULL
      AND (SELECT max_fldt FROM crit_fresh) >= (SELECT freshness_start_dt FROM params),
      0.0, -1.0
    ),
    IF(
      (SELECT max_fldt FROM crit_fresh) IS NOT NULL
      AND (SELECT max_fldt FROM crit_fresh) >= (SELECT freshness_start_dt FROM params),
      'PASS','FAIL'
    ),
    IF(
      (SELECT max_fldt FROM crit_fresh) IS NOT NULL
      AND (SELECT max_fldt FROM crit_fresh) >= (SELECT freshness_start_dt FROM params),
      '🟢','🔴'
    ),
    IF(
      (SELECT max_fldt FROM crit_fresh) IS NOT NULL
      AND (SELECT max_fldt FROM crit_fresh) >= (SELECT freshness_start_dt FROM params),
      'Recent loads exist (MAX(file_load_datetime) is fresh).',
      'No fresh loads: MAX(file_load_datetime) is older than freshness window (or NULL).'
    ),
    'If FAIL: check raw ingestion, merge schedule, and file_load_datetime population.',
    IF(
      (SELECT max_fldt FROM crit_fresh) IS NOT NULL
      AND (SELECT max_fldt FROM crit_fresh) >= (SELECT freshness_start_dt FROM params),
      FALSE, TRUE
    ),
    IF(
      (SELECT max_fldt FROM crit_fresh) IS NOT NULL
      AND (SELECT max_fldt FROM crit_fresh) >= (SELECT freshness_start_dt FROM params),
      TRUE, FALSE
    ),
    IF(
      (SELECT max_fldt FROM crit_fresh) IS NOT NULL
      AND (SELECT max_fldt FROM crit_fresh) >= (SELECT freshness_start_dt FROM params),
      FALSE, TRUE
    )

  UNION ALL
  SELECT
    (SELECT run_ts FROM params),
    (SELECT run_date FROM params),
    'sdi_profound_bronze_visibility_asset_daily',
    'reconciliation',
    'raw_dedup_vs_bronze_row_count_last_N_days',
    'HIGH',
    CAST((SELECT expected_rows FROM rec_rowcnt) AS FLOAT64),
    CAST((SELECT actual_rows FROM rec_rowcnt) AS FLOAT64),
    CAST((SELECT actual_rows FROM rec_rowcnt) - (SELECT expected_rows FROM rec_rowcnt) AS FLOAT64),
    IF((SELECT expected_rows FROM rec_rowcnt) = (SELECT actual_rows FROM rec_rowcnt),'PASS','FAIL'),
    IF((SELECT expected_rows FROM rec_rowcnt) = (SELECT actual_rows FROM rec_rowcnt),'🟢','🔴'),
    CONCAT('"expected(raw_dedup)=', CAST((SELECT expected_rows FROM rec_rowcnt) AS STRING),
           ', actual(bronze)=', CAST((SELECT actual_rows FROM rec_rowcnt) AS STRING), '"'),
    'If FAIL: Bronze missing rows in lookback window; run backfill or re-run merge with lookback_days.',
    IF((SELECT expected_rows FROM rec_rowcnt) = (SELECT actual_rows FROM rec_rowcnt),FALSE,TRUE),
    IF((SELECT expected_rows FROM rec_rowcnt) = (SELECT actual_rows FROM rec_rowcnt),TRUE,FALSE),
    IF((SELECT expected_rows FROM rec_rowcnt) = (SELECT actual_rows FROM rec_rowcnt),FALSE,TRUE)

  UNION ALL
  SELECT
    (SELECT run_ts FROM params),
    (SELECT run_date FROM params),
    'sdi_profound_bronze_visibility_asset_daily',
    'reconciliation',
    'raw_dedup_vs_bronze_metric_sums_last_N_days',
    'HIGH',
    (SELECT expected_sum FROM rec_metrics),
    (SELECT actual_sum FROM rec_metrics),
    (SELECT actual_sum FROM rec_metrics) - (SELECT expected_sum FROM rec_metrics),
    IF((SELECT expected_sum FROM rec_metrics) = (SELECT actual_sum FROM rec_metrics),'PASS','FAIL'),
    IF((SELECT expected_sum FROM rec_metrics) = (SELECT actual_sum FROM rec_metrics),'🟢','🔴'),
    CONCAT(
      '"exec exp=', CAST((SELECT exp_exec FROM rec_metrics) AS STRING), ', act=', CAST((SELECT act_exec FROM rec_metrics) AS STRING),
      ' | mentions exp=', CAST((SELECT exp_mentions FROM rec_metrics) AS STRING), ', act=', CAST((SELECT act_mentions FROM rec_metrics) AS STRING),
      ' | sov exp=', CAST((SELECT exp_sov FROM rec_metrics) AS STRING), ', act=', CAST((SELECT act_sov FROM rec_metrics) AS STRING),
      ' | vis exp=', CAST((SELECT exp_vis FROM rec_metrics) AS STRING), ', act=', CAST((SELECT act_vis FROM rec_metrics) AS STRING),
      '"'
    ),
    'If FAIL: Bronze missing rows or dedup mismatch vs raw; verify lookback + ordering.',
    IF((SELECT expected_sum FROM rec_metrics) = (SELECT actual_sum FROM rec_metrics),FALSE,TRUE),
    IF((SELECT expected_sum FROM rec_metrics) = (SELECT actual_sum FROM rec_metrics),TRUE,FALSE),
    IF((SELECT expected_sum FROM rec_metrics) = (SELECT actual_sum FROM rec_metrics),FALSE,TRUE);
END;


