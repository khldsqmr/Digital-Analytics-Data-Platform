/*===============================================================================
FILE: 01_sp_qa_sdi_profound_bronze_visibility_asset_daily.sql
LAYER: Bronze | QA
DATASET: prj-dbi-prd-1.ds_dbi_digitalmedia_automation
PROC:  sp_qa_sdi_profound_bronze_visibility_asset_daily

PURPOSE:
  3 critical + 2 reconciliation tests for:
    sdi_profound_bronze_visibility_asset_daily

ALIGNMENT (matches merge SP):
  - RAW source:
      prj-dbi-prd-1.ds_dbi_improvado_master.sdi_seo_profound_visibility_asset_daily_tmo
  - RAW filter:
      File_Load_datetime IS NOT NULL
      File_Load_datetime >= NOW - lookback_days
  - RAW dedup grain + ORDER BY:
      PARTITION BY (account_id, asset_id, date_yyyymmdd)
      ORDER BY file_load_datetime DESC, filename DESC, insert_date DESC
  - Reconciliation window:
      BOTH raw_dedup and bronze filtered to:
        file_load_datetime >= NOW - lookback_days
        date >= CURRENT_DATE - recon_days

OUTPUT:
  Exactly 5 rows inserted into sdi_profound_bronze_test_results per run.
===============================================================================*/

CREATE OR REPLACE PROCEDURE
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_qa_sdi_profound_bronze_visibility_asset_daily`()
OPTIONS(strict_mode=false)
BEGIN
  DECLARE lookback_days   INT64 DEFAULT 60;
  DECLARE recon_days      INT64 DEFAULT 14;
  DECLARE freshness_days  INT64 DEFAULT 3;

  DECLARE run_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP();
  DECLARE test_dt DATE DEFAULT CURRENT_DATE();

  DECLARE tbl STRING DEFAULT 'sdi_profound_bronze_visibility_asset_daily';

  -- ===========================================================================
  -- CTE: raw_dedup (EXACT merge logic)
  -- ===========================================================================
  -- NOTE: We keep this as a CTE pattern inside each test so we don’t create
  --       temp tables (avoids "Already Exists" / qualification errors).
  -- ===========================================================================

  -- ---------------------------------------------------------------------------
  -- TEST 1 (CRITICAL): duplicate grain groups (Bronze) in reconciliation window
  -- ---------------------------------------------------------------------------
  INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_profound_bronze_test_results`
  WITH bronze_scope AS (
    SELECT *
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_profound_bronze_visibility_asset_daily`
    WHERE file_load_datetime IS NOT NULL
      AND file_load_datetime >= DATETIME_SUB(CURRENT_DATETIME(), INTERVAL lookback_days DAY)
      AND date >= DATE_SUB(CURRENT_DATE(), INTERVAL recon_days DAY)
  ),
  dup_groups AS (
    SELECT COUNT(*) AS dup_group_count
    FROM (
      SELECT account_id, asset_id, date_yyyymmdd, COUNT(*) AS c
      FROM bronze_scope
      GROUP BY 1,2,3
      HAVING c > 1
    )
  )
  SELECT
    run_ts AS test_run_timestamp,
    test_dt AS test_date,
    tbl AS table_name,
    'critical' AS test_layer,
    'duplicate_grain_groups_recon_window' AS test_name,
    'HIGH' AS severity_level,
    0.0 AS expected_value,
    CAST(dup_group_count AS FLOAT64) AS actual_value,
    CAST(dup_group_count AS FLOAT64) - 0.0 AS variance_value,
    IF(dup_group_count = 0, 'PASS', 'FAIL') AS status,
    IF(dup_group_count = 0, '🟢', '🔴') AS status_emoji,
    IF(dup_group_count = 0,
      'No duplicate grain groups detected.',
      'Duplicate grain groups detected in Bronze for (account_id, asset_id, date_yyyymmdd).'
    ) AS failure_reason,
    'If FAIL: check MERGE ON keys + dedup ORDER BY; confirm no upstream hidden dimension.' AS next_step,
    IF(dup_group_count = 0, FALSE, TRUE) AS is_critical_failure,
    IF(dup_group_count = 0, TRUE, FALSE) AS is_pass,
    IF(dup_group_count = 0, FALSE, TRUE) AS is_fail
  FROM dup_groups;

  -- ---------------------------------------------------------------------------
  -- TEST 2 (CRITICAL): null key/date rows (Bronze) in reconciliation window
  -- ---------------------------------------------------------------------------
  INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_profound_bronze_test_results`
  WITH bronze_scope AS (
    SELECT *
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_profound_bronze_visibility_asset_daily`
    WHERE file_load_datetime IS NOT NULL
      AND file_load_datetime >= DATETIME_SUB(CURRENT_DATETIME(), INTERVAL lookback_days DAY)
      AND date >= DATE_SUB(CURRENT_DATE(), INTERVAL recon_days DAY)
  ),
  bad AS (
    SELECT COUNT(*) AS bad_rows
    FROM bronze_scope
    WHERE account_id IS NULL
       OR asset_id IS NULL
       OR date_yyyymmdd IS NULL
       OR date IS NULL
  )
  SELECT
    run_ts, test_dt, tbl,
    'critical',
    'null_key_or_date_rows_recon_window',
    'HIGH',
    0.0,
    CAST(bad_rows AS FLOAT64),
    CAST(bad_rows AS FLOAT64) - 0.0,
    IF(bad_rows = 0, 'PASS', 'FAIL'),
    IF(bad_rows = 0, '🟢', '🔴'),
    IF(bad_rows = 0,
      'No null key/date rows found.',
      'Found rows with NULL in (account_id, asset_id, date_yyyymmdd, date).'
    ),
    'If FAIL: verify TRIM/NULLIF + SAFE.PARSE_DATE and merge cleaned filters.',
    IF(bad_rows = 0, FALSE, TRUE),
    IF(bad_rows = 0, TRUE, FALSE),
    IF(bad_rows = 0, FALSE, TRUE)
  FROM bad;

  -- ---------------------------------------------------------------------------
  -- TEST 3 (CRITICAL): freshness of Bronze loads (MAX(file_load_datetime))
  -- ---------------------------------------------------------------------------
  INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_profound_bronze_test_results`
  WITH mx AS (
    SELECT MAX(file_load_datetime) AS max_fld
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_profound_bronze_visibility_asset_daily`
  ),
  flag AS (
    SELECT
      IF(max_fld IS NOT NULL
         AND max_fld >= DATETIME_SUB(CURRENT_DATETIME(), INTERVAL freshness_days DAY), 1.0, 0.0) AS ok
    FROM mx
  )
  SELECT
    run_ts, test_dt, tbl,
    'critical',
    'freshness_max_file_load_datetime_within_last_days',
    'HIGH',
    1.0,
    ok,
    ok - 1.0,
    IF(ok = 1.0, 'PASS', 'FAIL'),
    IF(ok = 1.0, '🟢', '🔴'),
    IF(ok = 1.0,
      'Recent loads exist (MAX(file_load_datetime) is fresh).',
      'MAX(file_load_datetime) is stale or NULL.'
    ),
    'If FAIL: check raw ingestion schedule + merge job schedule + file_load_datetime population.',
    IF(ok = 1.0, FALSE, TRUE),
    IF(ok = 1.0, TRUE, FALSE),
    IF(ok = 1.0, FALSE, TRUE)
  FROM flag;

  -- ---------------------------------------------------------------------------
  -- TEST 4 (RECON): raw_dedup vs bronze row count (aligned to merge scope)
  -- ---------------------------------------------------------------------------
  INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_profound_bronze_test_results`
  WITH raw_src AS (
    SELECT
      SAFE_CAST(raw.account_id AS STRING) AS account_id,
      NULLIF(TRIM(SAFE_CAST(raw.asset_id AS STRING)), '') AS asset_id,
      SAFE_CAST(raw.date_yyyymmdd AS STRING) AS date_yyyymmdd,
      SAFE.PARSE_DATE('%Y%m%d', SAFE_CAST(raw.date_yyyymmdd AS STRING)) AS date,
      SAFE_CAST(raw.__insert_date AS INT64) AS insert_date,
      SAFE_CAST(raw.File_Load_datetime AS DATETIME) AS file_load_datetime,
      NULLIF(TRIM(SAFE_CAST(raw.Filename AS STRING)), '') AS filename
    FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_seo_profound_visibility_asset_daily_tmo` raw
    WHERE SAFE_CAST(raw.File_Load_datetime AS DATETIME) IS NOT NULL
      AND SAFE_CAST(raw.File_Load_datetime AS DATETIME)
        >= DATETIME_SUB(CURRENT_DATETIME(), INTERVAL lookback_days DAY)
  ),
  raw_clean AS (
    SELECT *
    FROM raw_src
    WHERE date IS NOT NULL
      AND account_id IS NOT NULL
      AND asset_id IS NOT NULL
      AND date_yyyymmdd IS NOT NULL
      AND date >= DATE_SUB(CURRENT_DATE(), INTERVAL recon_days DAY)
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
  bronze_scope AS (
    SELECT account_id, asset_id, date_yyyymmdd
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_profound_bronze_visibility_asset_daily`
    WHERE file_load_datetime IS NOT NULL
      AND file_load_datetime >= DATETIME_SUB(CURRENT_DATETIME(), INTERVAL lookback_days DAY)
      AND date >= DATE_SUB(CURRENT_DATE(), INTERVAL recon_days DAY)
  ),
  counts AS (
    SELECT
      (SELECT COUNT(*) FROM raw_dedup) AS expected_cnt,
      (SELECT COUNT(*) FROM bronze_scope) AS actual_cnt
  )
  SELECT
    run_ts, test_dt, tbl,
    'reconciliation',
    'raw_dedup_vs_bronze_row_count_recon_window',
    'HIGH',
    CAST(expected_cnt AS FLOAT64),
    CAST(actual_cnt AS FLOAT64),
    CAST(actual_cnt - expected_cnt AS FLOAT64),
    IF(expected_cnt = actual_cnt, 'PASS', 'FAIL'),
    IF(expected_cnt = actual_cnt, '🟢', '🔴'),
    FORMAT('expected(raw_dedup)=%d, actual(bronze)=%d', expected_cnt, actual_cnt),
    'If FAIL: Bronze missing/extra rows within MERGE scope. Verify merge lookback + run backfill if table was recreated.',
    IF(expected_cnt = actual_cnt, FALSE, TRUE),
    IF(expected_cnt = actual_cnt, TRUE, FALSE),
    IF(expected_cnt = actual_cnt, FALSE, TRUE)
  FROM counts;

  -- ---------------------------------------------------------------------------
  -- TEST 5 (RECON): raw_dedup vs bronze metric sums (aligned to merge scope)
  --   We store a single scalar expected/actual by summing all numeric metrics.
  --   Breakdown is placed in failure_reason for clarity.
  -- ---------------------------------------------------------------------------
  INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_profound_bronze_test_results`
  WITH raw_src AS (
    SELECT
      SAFE_CAST(raw.account_id AS STRING) AS account_id,
      NULLIF(TRIM(SAFE_CAST(raw.asset_id AS STRING)), '') AS asset_id,
      SAFE_CAST(raw.date_yyyymmdd AS STRING) AS date_yyyymmdd,
      SAFE.PARSE_DATE('%Y%m%d', SAFE_CAST(raw.date_yyyymmdd AS STRING)) AS date,
      SAFE_CAST(raw.executions AS FLOAT64) AS executions,
      SAFE_CAST(raw.mentions_count AS FLOAT64) AS mentions_count,
      SAFE_CAST(raw.share_of_voice AS FLOAT64) AS share_of_voice,
      SAFE_CAST(raw.visibility_score AS FLOAT64) AS visibility_score,
      SAFE_CAST(raw.__insert_date AS INT64) AS insert_date,
      SAFE_CAST(raw.File_Load_datetime AS DATETIME) AS file_load_datetime,
      NULLIF(TRIM(SAFE_CAST(raw.Filename AS STRING)), '') AS filename
    FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_seo_profound_visibility_asset_daily_tmo` raw
    WHERE SAFE_CAST(raw.File_Load_datetime AS DATETIME) IS NOT NULL
      AND SAFE_CAST(raw.File_Load_datetime AS DATETIME)
        >= DATETIME_SUB(CURRENT_DATETIME(), INTERVAL lookback_days DAY)
  ),
  raw_clean AS (
    SELECT *
    FROM raw_src
    WHERE date IS NOT NULL
      AND account_id IS NOT NULL
      AND asset_id IS NOT NULL
      AND date_yyyymmdd IS NOT NULL
      AND date >= DATE_SUB(CURRENT_DATE(), INTERVAL recon_days DAY)
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
  raw_agg AS (
    SELECT
      ROUND(IFNULL(SUM(executions),0), 6) AS exec_sum,
      ROUND(IFNULL(SUM(mentions_count),0), 6) AS mentions_sum,
      ROUND(IFNULL(SUM(share_of_voice),0), 6) AS sov_sum,
      ROUND(IFNULL(SUM(visibility_score),0), 6) AS vis_sum
    FROM raw_dedup
  ),
  bronze_scope AS (
    SELECT
      executions,
      mentions_count,
      share_of_voice,
      visibility_score
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_profound_bronze_visibility_asset_daily`
    WHERE file_load_datetime IS NOT NULL
      AND file_load_datetime >= DATETIME_SUB(CURRENT_DATETIME(), INTERVAL lookback_days DAY)
      AND date >= DATE_SUB(CURRENT_DATE(), INTERVAL recon_days DAY)
  ),
  bronze_agg AS (
    SELECT
      ROUND(IFNULL(SUM(executions),0), 6) AS exec_sum,
      ROUND(IFNULL(SUM(mentions_count),0), 6) AS mentions_sum,
      ROUND(IFNULL(SUM(share_of_voice),0), 6) AS sov_sum,
      ROUND(IFNULL(SUM(visibility_score),0), 6) AS vis_sum
    FROM bronze_scope
  ),
  final AS (
    SELECT
      (r.exec_sum + r.mentions_sum + r.sov_sum + r.vis_sum) AS expected_total,
      (b.exec_sum + b.mentions_sum + b.sov_sum + b.vis_sum) AS actual_total,
      r.exec_sum AS exp_exec, b.exec_sum AS act_exec,
      r.mentions_sum AS exp_mentions, b.mentions_sum AS act_mentions,
      r.sov_sum AS exp_sov, b.sov_sum AS act_sov,
      r.vis_sum AS exp_vis, b.vis_sum AS act_vis
    FROM raw_agg r CROSS JOIN bronze_agg b
  )
  SELECT
    run_ts, test_dt, tbl,
    'reconciliation',
    'raw_dedup_vs_bronze_metric_sums_recon_window',
    'HIGH',
    expected_total,
    actual_total,
    actual_total - expected_total,
    IF(expected_total = actual_total, 'PASS', 'FAIL'),
    IF(expected_total = actual_total, '🟢', '🔴'),
    FORMAT(
      'exec exp=%g, act=%g | mentions exp=%g, act=%g | sov exp=%g, act=%g | vis exp=%g, act=%g',
      exp_exec, act_exec, exp_mentions, act_mentions, exp_sov, act_sov, exp_vis, act_vis
    ),
    'If FAIL: mismatch means missing/extra rows within MERGE scope or dedup mismatch. Verify dedup ORDER BY + merge lookback.',
    IF(expected_total = actual_total, FALSE, TRUE),
    IF(expected_total = actual_total, TRUE, FALSE),
    IF(expected_total = actual_total, FALSE, TRUE)
  FROM final;

END;