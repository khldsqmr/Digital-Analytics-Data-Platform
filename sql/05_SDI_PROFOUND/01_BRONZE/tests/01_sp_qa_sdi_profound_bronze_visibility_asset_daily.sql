/*===============================================================================
FILE: 01_sp_qa_sdi_profound_bronze_visibility_asset_daily.sql
LAYER: Bronze | QA
DATASET: prj-dbi-prd-1.ds_dbi_digitalmedia_automation
PROC:  sp_qa_sdi_profound_bronze_visibility_asset_daily

PURPOSE:
  3 critical + 2 reconciliation tests for:
    prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_profound_bronze_visibility_asset_daily

ALIGNMENT (matches merge SP exactly):
  - RAW source: prj-dbi-prd-1.ds_dbi_improvado_master.sdi_seo_profound_visibility_asset_daily_tmo
  - RAW canonical date: SAFE.PARSE_DATE('%Y%m%d', raw.date_yyyymmdd)
  - RAW dedup grain: (account_id, asset_id, date_yyyymmdd)
  - RAW dedup ORDER BY: file_load_datetime DESC, filename DESC, insert_date DESC
  - Reconciliation window: last completed ISO week (Mon-Sun) by event date (NOT ingestion)

OUTPUT:
  Exactly 5 rows inserted into sdi_profound_bronze_test_results per run.
===============================================================================*/
CREATE OR REPLACE PROCEDURE
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_qa_sdi_profound_bronze_visibility_asset_daily`()
OPTIONS(strict_mode=false)
BEGIN
  -- -----------------------------
  -- Parameters (keep stable)
  -- -----------------------------
  DECLARE recon_week_offset INT64 DEFAULT 1;         -- 1 = last completed ISO week; set 2 if late arrivals are common
  DECLARE freshness_days INT64 DEFAULT 2;            -- "pipeline alive" check
  DECLARE test_date DATE DEFAULT CURRENT_DATE();
  DECLARE run_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP();

  -- Last completed ISO week (Mon..Sun) offset by recon_week_offset
  DECLARE week_start DATE DEFAULT DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL recon_week_offset WEEK), WEEK(MONDAY));
  DECLARE week_end   DATE DEFAULT DATE_ADD(week_start, INTERVAL 6 DAY);

  -- Table identifiers (for logging consistency)
  DECLARE bronze_table STRING DEFAULT 'sdi_profound_bronze_visibility_asset_daily';

  -- -----------------------------
  -- Common CTEs (inline per test)
  -- NOTE: No temp tables (prevents _raw_dedup collisions / qualification errors)
  -- -----------------------------

  -- ======================================================================
  -- TEST 1 (CRITICAL): Duplicate grain groups in the reconciliation week
  -- Grain: account_id + asset_id + date_yyyymmdd (same as merge key)
  -- Expect: 0 duplicate groups
  -- ======================================================================
  INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_profound_bronze_test_results`
  WITH dup AS (
    SELECT
      COUNT(*) AS duplicate_groups
    FROM (
      SELECT
        account_id,
        asset_id,
        date_yyyymmdd,
        COUNT(*) AS c
      FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_profound_bronze_visibility_asset_daily`
      WHERE date BETWEEN week_start AND week_end
      GROUP BY 1,2,3
      HAVING c > 1
    )
  )
  SELECT
    run_ts AS test_run_timestamp,
    test_date AS test_date,
    bronze_table AS table_name,
    'critical' AS test_layer,
    'duplicate_grain_groups_iso_week' AS test_name,
    'HIGH' AS severity_level,
    0.0 AS expected_value,
    CAST(duplicate_groups AS FLOAT64) AS actual_value,
    CAST(duplicate_groups AS FLOAT64) - 0.0 AS variance_value,
    IF(duplicate_groups = 0, 'PASS', 'FAIL') AS status,
    IF(duplicate_groups = 0, '🟢', '🔴') AS status_emoji,
    IF(duplicate_groups = 0,
      'No duplicate grain groups detected.',
      'Duplicate grain groups found in Bronze for the ISO week.'
    ) AS failure_reason,
    'If FAIL: check MERGE key + dedup ORDER BY tie-breakers.' AS next_step,
    IF(duplicate_groups = 0, FALSE, TRUE) AS is_critical_failure,
    IF(duplicate_groups = 0, TRUE, FALSE) AS is_pass,
    IF(duplicate_groups = 0, FALSE, TRUE) AS is_fail
  FROM dup;

  -- ======================================================================
  -- TEST 2 (CRITICAL): Null key/date rows in the reconciliation week
  -- Expect: 0 rows with null keys/date/date_yyyymmdd
  -- ======================================================================
  INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_profound_bronze_test_results`
  WITH bad AS (
    SELECT
      COUNT(*) AS bad_rows
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_profound_bronze_visibility_asset_daily`
    WHERE date BETWEEN week_start AND week_end
      AND (
        account_id IS NULL OR asset_id IS NULL OR date_yyyymmdd IS NULL OR date IS NULL
      )
  )
  SELECT
    run_ts,
    test_date,
    bronze_table,
    'critical',
    'null_key_or_date_rows_iso_week',
    'HIGH',
    0.0,
    CAST(bad_rows AS FLOAT64),
    CAST(bad_rows AS FLOAT64) - 0.0,
    IF(bad_rows = 0, 'PASS', 'FAIL'),
    IF(bad_rows = 0, '🟢', '🔴'),
    IF(bad_rows = 0,
      'No null key/date rows found.',
      'Null key/date rows found in Bronze for the ISO week.'
    ),
    'If FAIL: verify TRIM/NULLIF + SAFE.PARSE_DATE + key filters in merge.',
    IF(bad_rows = 0, FALSE, TRUE),
    IF(bad_rows = 0, TRUE, FALSE),
    IF(bad_rows = 0, FALSE, TRUE)
  FROM bad;

  -- ======================================================================
  -- TEST 3 (CRITICAL): Freshness (pipeline alive)
  -- Check: MAX(file_load_datetime) within last freshness_days
  -- Expect: 1 (true) if fresh, 0 if stale
  -- ======================================================================
  INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_profound_bronze_test_results`
  WITH f AS (
    SELECT
      MAX(file_load_datetime) AS max_fld
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_profound_bronze_visibility_asset_daily`
  ),
  flags AS (
    SELECT
      IF(max_fld IS NULL, 0,
         IF(max_fld >= DATETIME_SUB(CURRENT_DATETIME(), INTERVAL freshness_days DAY), 1, 0)
      ) AS is_fresh,
      max_fld
    FROM f
  )
  SELECT
    run_ts,
    test_date,
    bronze_table,
    'critical',
    'freshness_max_file_load_datetime_within_last_days',
    'HIGH',
    1.0,
    CAST(is_fresh AS FLOAT64),
    CAST(is_fresh AS FLOAT64) - 1.0,
    IF(is_fresh = 1, 'PASS', 'FAIL'),
    IF(is_fresh = 1, '🟢', '🔴'),
    IF(is_fresh = 1,
      'Recent loads exist (MAX(file_load_datetime) is fresh).',
      'No recent loads (MAX(file_load_datetime) is stale or NULL).'
    ),
    'If FAIL: check raw ingestion, merge schedule, and file_load_datetime population.',
    IF(is_fresh = 1, FALSE, TRUE),
    IF(is_fresh = 1, TRUE, FALSE),
    IF(is_fresh = 1, FALSE, TRUE)
  FROM flags;

  -- ======================================================================
  -- Reconciliation helpers (RAW dedup exactly like merge)
  -- Note: Filter by event date (parsed from date_yyyymmdd) in the ISO week
  -- ======================================================================

  -- ======================================================================
  -- TEST 4 (RECONCILIATION): Raw-dedup vs Bronze row count (ISO week)
  -- Expect: equality
  -- ======================================================================
  INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_profound_bronze_test_results`
  WITH raw_src AS (
    SELECT
      SAFE_CAST(account_id AS STRING) AS account_id,
      NULLIF(TRIM(SAFE_CAST(asset_id AS STRING)), '') AS asset_id,
      SAFE_CAST(date_yyyymmdd AS STRING) AS date_yyyymmdd,
      SAFE.PARSE_DATE('%Y%m%d', SAFE_CAST(date_yyyymmdd AS STRING)) AS date,
      SAFE_CAST(File_Load_datetime AS DATETIME) AS file_load_datetime,
      NULLIF(TRIM(SAFE_CAST(Filename AS STRING)), '') AS filename,
      SAFE_CAST(__insert_date AS INT64) AS insert_date
    FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_seo_profound_visibility_asset_daily_tmo`
    WHERE SAFE.PARSE_DATE('%Y%m%d', SAFE_CAST(date_yyyymmdd AS STRING)) BETWEEN week_start AND week_end
      AND SAFE_CAST(File_Load_datetime AS DATETIME) IS NOT NULL
  ),
  raw_clean AS (
    SELECT *
    FROM raw_src
    WHERE date IS NOT NULL AND account_id IS NOT NULL AND asset_id IS NOT NULL AND date_yyyymmdd IS NOT NULL
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
      FROM raw_clean r
    )
    WHERE rn = 1
  ),
  expected AS (SELECT COUNT(*) AS expected_rows FROM raw_dedup),
  actual AS (
    SELECT COUNT(*) AS actual_rows
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_profound_bronze_visibility_asset_daily`
    WHERE date BETWEEN week_start AND week_end
  )
  SELECT
    run_ts,
    test_date,
    bronze_table,
    'reconciliation',
    'raw_dedup_vs_bronze_row_count_iso_week',
    'HIGH',
    CAST(expected_rows AS FLOAT64),
    CAST(actual_rows AS FLOAT64),
    CAST(actual_rows AS FLOAT64) - CAST(expected_rows AS FLOAT64),
    IF(actual_rows = expected_rows, 'PASS', 'FAIL'),
    IF(actual_rows = expected_rows, '🟢', '🔴'),
    FORMAT('expected(raw_dedup)=%d, actual(bronze)=%d', expected_rows, actual_rows),
    'If FAIL: Bronze missing/extra rows for the ISO week; check merge lookback + run backfill if table was recreated.',
    IF(actual_rows = expected_rows, FALSE, TRUE),
    IF(actual_rows = expected_rows, TRUE, FALSE),
    IF(actual_rows = expected_rows, FALSE, TRUE)
  FROM expected, actual;

  -- ======================================================================
  -- TEST 5 (RECONCILIATION): Raw-dedup vs Bronze metric sums (ISO week)
  -- Metrics: executions, mentions_count, share_of_voice, visibility_score
  -- Expect: equality (exact) since Bronze is direct cast + dedup
  -- ======================================================================
  INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_profound_bronze_test_results`
  WITH raw_src AS (
    SELECT
      SAFE_CAST(account_id AS STRING) AS account_id,
      NULLIF(TRIM(SAFE_CAST(asset_id AS STRING)), '') AS asset_id,
      SAFE_CAST(date_yyyymmdd AS STRING) AS date_yyyymmdd,
      SAFE.PARSE_DATE('%Y%m%d', SAFE_CAST(date_yyyymmdd AS STRING)) AS date,
      SAFE_CAST(executions AS FLOAT64) AS executions,
      SAFE_CAST(mentions_count AS FLOAT64) AS mentions_count,
      SAFE_CAST(share_of_voice AS FLOAT64) AS share_of_voice,
      SAFE_CAST(visibility_score AS FLOAT64) AS visibility_score,
      SAFE_CAST(File_Load_datetime AS DATETIME) AS file_load_datetime,
      NULLIF(TRIM(SAFE_CAST(Filename AS STRING)), '') AS filename,
      SAFE_CAST(__insert_date AS INT64) AS insert_date
    FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_seo_profound_visibility_asset_daily_tmo`
    WHERE SAFE.PARSE_DATE('%Y%m%d', SAFE_CAST(date_yyyymmdd AS STRING)) BETWEEN week_start AND week_end
      AND SAFE_CAST(File_Load_datetime AS DATETIME) IS NOT NULL
  ),
  raw_clean AS (
    SELECT *
    FROM raw_src
    WHERE date IS NOT NULL AND account_id IS NOT NULL AND asset_id IS NOT NULL AND date_yyyymmdd IS NOT NULL
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
      FROM raw_clean r
    )
    WHERE rn = 1
  ),
  exp AS (
    SELECT
      SUM(COALESCE(executions,0)) AS exp_exec,
      SUM(COALESCE(mentions_count,0)) AS exp_mentions,
      SUM(COALESCE(share_of_voice,0)) AS exp_sov,
      SUM(COALESCE(visibility_score,0)) AS exp_vis
    FROM raw_dedup
  ),
  act AS (
    SELECT
      SUM(COALESCE(executions,0)) AS act_exec,
      SUM(COALESCE(mentions_count,0)) AS act_mentions,
      SUM(COALESCE(share_of_voice,0)) AS act_sov,
      SUM(COALESCE(visibility_score,0)) AS act_vis
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_profound_bronze_visibility_asset_daily`
    WHERE date BETWEEN week_start AND week_end
  ),
  cmp AS (
    SELECT
      (exp_exec + exp_mentions + exp_sov + exp_vis) AS expected_total,
      (act_exec + act_mentions + act_sov + act_vis) AS actual_total,
      FORMAT(
        'exec exp=%.0f, act=%.0f | mentions exp=%.0f, act=%.0f | sov exp=%.6f, act=%.6f | vis exp=%.6f, act=%.6f',
        exp_exec, act_exec, exp_mentions, act_mentions, exp_sov, act_sov, exp_vis, act_vis
      ) AS detail_msg
    FROM exp, act
  )
  SELECT
    run_ts,
    test_date,
    bronze_table,
    'reconciliation',
    'raw_dedup_vs_bronze_metric_sums_iso_week',
    'HIGH',
    CAST(expected_total AS FLOAT64),
    CAST(actual_total AS FLOAT64),
    CAST(actual_total AS FLOAT64) - CAST(expected_total AS FLOAT64),
    IF(ABS(actual_total - expected_total) < 0.000001, 'PASS', 'FAIL'),
    IF(ABS(actual_total - expected_total) < 0.000001, '🟢', '🔴'),
    detail_msg,
    'If FAIL: mismatch indicates missing/extra rows or dedup mismatch; verify raw filter + dedup ORDER BY + merge lookback.',
    IF(ABS(actual_total - expected_total) < 0.000001, FALSE, TRUE),
    IF(ABS(actual_total - expected_total) < 0.000001, TRUE, FALSE),
    IF(ABS(actual_total - expected_total) < 0.000001, FALSE, TRUE)
  FROM cmp;

END;


