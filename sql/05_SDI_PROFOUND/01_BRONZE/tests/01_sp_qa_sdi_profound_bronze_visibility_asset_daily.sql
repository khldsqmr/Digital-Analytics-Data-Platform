/*===============================================================================
FILE: 01_sp_qa_sdi_profound_bronze_visibility_asset_daily.sql
LAYER: Bronze | QA
DATASET: prj-dbi-prd-1.ds_dbi_digitalmedia_automation
PROC:  sp_qa_sdi_profound_bronze_visibility_asset_daily

PURPOSE:
  3 critical + 2 reconciliation tests for:
    sdi_profound_bronze_visibility_asset_daily

ALIGNMENT:
  - RAW source + filter matches merge SP (File_Load_datetime lookback)
  - RAW dedup grain + ORDER BY matches merge SP
  - Bronze comparison window matches merge SP lookback (file_load_datetime)

OUTPUT:
  Exactly 5 rows inserted into sdi_profound_bronze_test_results per run.
===============================================================================*/
CREATE OR REPLACE PROCEDURE
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_qa_sdi_profound_bronze_visibility_asset_daily`()
OPTIONS(strict_mode=false)
BEGIN
  -- ---------------------------------------------------------------------------
  -- Parameters (keep aligned with the merge SP)
  -- ---------------------------------------------------------------------------
  DECLARE lookback_days INT64 DEFAULT 60;
  DECLARE freshness_days INT64 DEFAULT 7;

  -- ---------------------------------------------------------------------------
  -- Run metadata
  -- ---------------------------------------------------------------------------
  DECLARE run_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP();
  DECLARE run_dt DATE DEFAULT CURRENT_DATE();
  DECLARE tbl STRING DEFAULT 'sdi_profound_bronze_visibility_asset_daily';

  -- ===========================================================================
  -- 0) Build RAW expected dataset (deduped) using SAME logic as merge SP
  --     - filter by File_Load_datetime lookback
  --     - SAFE.PARSE_DATE on date_yyyymmdd
  --     - required key filters
  --     - dedup order: file_load_datetime DESC, filename DESC, insert_date DESC
  -- ===========================================================================
  CREATE OR REPLACE TEMP TABLE _raw_dedup AS
  WITH src AS (
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
      AND SAFE_CAST(raw.File_Load_datetime AS DATETIME) >= DATETIME_SUB(CURRENT_DATETIME(), INTERVAL lookback_days DAY)
  ),
  cleaned AS (
    SELECT *
    FROM src
    WHERE date IS NOT NULL
      AND account_id IS NOT NULL
      AND asset_id IS NOT NULL
      AND date_yyyymmdd IS NOT NULL
  ),
  dedup AS (
    SELECT * EXCEPT(rn)
    FROM (
      SELECT
        c.*,
        ROW_NUMBER() OVER (
          PARTITION BY account_id, asset_id, date_yyyymmdd
          ORDER BY file_load_datetime DESC, filename DESC, insert_date DESC
        ) AS rn
      FROM cleaned c
    )
    WHERE rn = 1
  )
  SELECT * FROM dedup;

  -- ===========================================================================
  -- 1) Bronze comparison window (same “ingestion lookback” concept as merge SP)
  -- ===========================================================================
  CREATE OR REPLACE TEMP TABLE _bronze_window AS
  SELECT *
  FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_profound_bronze_visibility_asset_daily`
  WHERE file_load_datetime IS NOT NULL
    AND file_load_datetime >= DATETIME_SUB(CURRENT_DATETIME(), INTERVAL lookback_days DAY);

  -- ===========================================================================
  -- CRITICAL TEST 1: Freshness (binary)
  --   Pass if MAX(file_load_datetime) is within freshness_days
  -- ===========================================================================
  INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_profound_bronze_test_results`
  WITH x AS (
    SELECT MAX(file_load_datetime) AS max_fldt
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_profound_bronze_visibility_asset_daily`
  ),
  y AS (
    SELECT
      1.0 AS expected_value,
      IF(max_fldt >= DATETIME_SUB(CURRENT_DATETIME(), INTERVAL freshness_days DAY), 1.0, 0.0) AS actual_value,
      max_fldt
    FROM x
  )
  SELECT
    run_ts, run_dt,
    tbl,
    'critical',
    'freshness_max_file_load_datetime_within_last_days',
    'HIGH',
    expected_value,
    actual_value,
    actual_value - expected_value,
    IF(actual_value = expected_value, 'PASS', 'FAIL'),
    IF(actual_value = expected_value, '🟢', '🔴'),
    IF(actual_value = expected_value,
       'Recent loads exist (MAX(file_load_datetime) is fresh).',
       CONCAT('No loads within ', CAST(freshness_days AS STRING), ' days. max(file_load_datetime)=', CAST(max_fldt AS STRING))
    ),
    'If FAIL: check raw ingestion, merge schedule, and file_load_datetime population.',
    IF(actual_value != expected_value, TRUE, FALSE),
    IF(actual_value = expected_value, TRUE, FALSE),
    IF(actual_value != expected_value, TRUE, FALSE)
  FROM y;

  -- ===========================================================================
  -- CRITICAL TEST 2: Null key/date rows in Bronze window
  -- ===========================================================================
  INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_profound_bronze_test_results`
  WITH z AS (
    SELECT COUNTIF(account_id IS NULL OR asset_id IS NULL OR date_yyyymmdd IS NULL OR date IS NULL) AS bad_rows
    FROM _bronze_window
  )
  SELECT
    run_ts, run_dt,
    tbl,
    'critical',
    'null_key_or_date_rows_last_N_days',
    'HIGH',
    0.0,
    CAST(bad_rows AS FLOAT64),
    CAST(bad_rows AS FLOAT64),
    IF(bad_rows = 0, 'PASS', 'FAIL'),
    IF(bad_rows = 0, '🟢', '🔴'),
    IF(bad_rows = 0, 'No null key/date rows found.', CONCAT('Found ', CAST(bad_rows AS STRING), ' rows with null key/date.')),
    'If FAIL: verify TRIM/NULLIF + SAFE.PARSE_DATE + key filters in merge.',
    IF(bad_rows > 0, TRUE, FALSE),
    IF(bad_rows = 0, TRUE, FALSE),
    IF(bad_rows > 0, TRUE, FALSE)
  FROM z;

  -- ===========================================================================
  -- CRITICAL TEST 3: Duplicate grain groups in Bronze window
  --   Grain: account_id + asset_id + date_yyyymmdd
  -- ===========================================================================
  INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_profound_bronze_test_results`
  WITH d AS (
    SELECT COUNT(*) AS dup_groups
    FROM (
      SELECT account_id, asset_id, date_yyyymmdd
      FROM _bronze_window
      GROUP BY 1,2,3
      HAVING COUNT(*) > 1
    )
  )
  SELECT
    run_ts, run_dt,
    tbl,
    'critical',
    'duplicate_grain_groups_last_N_days',
    'HIGH',
    0.0,
    CAST(dup_groups AS FLOAT64),
    CAST(dup_groups AS FLOAT64),
    IF(dup_groups = 0, 'PASS', 'FAIL'),
    IF(dup_groups = 0, '🟢', '🔴'),
    IF(dup_groups = 0, 'No duplicate grain groups detected.', CONCAT('Duplicate grain groups found: ', CAST(dup_groups AS STRING))),
    'If FAIL: check MERGE key + dedup ORDER BY tie-breakers.',
    IF(dup_groups > 0, TRUE, FALSE),
    IF(dup_groups = 0, TRUE, FALSE),
    IF(dup_groups > 0, TRUE, FALSE)
  FROM d;

  -- ===========================================================================
  -- RECON TEST 1: Raw dedup vs Bronze row count (same lookback window concept)
  -- ===========================================================================
  INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_profound_bronze_test_results`
  WITH a AS (SELECT COUNT(*) AS exp_cnt FROM _raw_dedup),
       b AS (SELECT COUNT(*) AS act_cnt FROM _bronze_window)
  SELECT
    run_ts, run_dt,
    tbl,
    'reconciliation',
    'raw_dedup_vs_bronze_row_count_last_N_days',
    'HIGH',
    CAST(exp_cnt AS FLOAT64),
    CAST(act_cnt AS FLOAT64),
    CAST(act_cnt - exp_cnt AS FLOAT64),
    IF(exp_cnt = act_cnt, 'PASS', 'FAIL'),
    IF(exp_cnt = act_cnt, '🟢', '🔴'),
    IF(exp_cnt = act_cnt,
       'Row counts match (raw_dedup vs bronze window).',
       CONCAT('expected(raw_dedup)=', CAST(exp_cnt AS STRING), ', actual(bronze)=', CAST(act_cnt AS STRING))
    ),
    'If FAIL: Bronze missing rows in lookback window; run backfill or re-run merge with lookback_days.',
    IF(exp_cnt != act_cnt, TRUE, FALSE),
    IF(exp_cnt = act_cnt, TRUE, FALSE),
    IF(exp_cnt != act_cnt, TRUE, FALSE)
  FROM a,b;

  -- ===========================================================================
  -- RECON TEST 2: Raw dedup vs Bronze metric sums
  --   Visibility checksum = executions + mentions_count + share_of_voice + visibility_score
  -- ===========================================================================
  INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_profound_bronze_test_results`
  WITH r AS (
    SELECT
      SUM(executions) AS exec_sum,
      SUM(mentions_count) AS mentions_sum,
      SUM(share_of_voice) AS sov_sum,
      SUM(visibility_score) AS vis_sum
    FROM _raw_dedup
  ),
  s AS (
    SELECT
      SUM(executions) AS exec_sum,
      SUM(mentions_count) AS mentions_sum,
      SUM(share_of_voice) AS sov_sum,
      SUM(visibility_score) AS vis_sum
    FROM _bronze_window
  ),
  t AS (
    SELECT
      (COALESCE(r.exec_sum,0)+COALESCE(r.mentions_sum,0)+COALESCE(r.sov_sum,0)+COALESCE(r.vis_sum,0)) AS expected_value,
      (COALESCE(s.exec_sum,0)+COALESCE(s.mentions_sum,0)+COALESCE(s.sov_sum,0)+COALESCE(s.vis_sum,0)) AS actual_value,
      r.exec_sum AS exp_exec, s.exec_sum AS act_exec,
      r.mentions_sum AS exp_mentions, s.mentions_sum AS act_mentions,
      r.sov_sum AS exp_sov, s.sov_sum AS act_sov,
      r.vis_sum AS exp_vis, s.vis_sum AS act_vis
    FROM r,s
  )
  SELECT
    run_ts, run_dt,
    tbl,
    'reconciliation',
    'raw_dedup_vs_bronze_metric_sums_last_N_days',
    'HIGH',
    expected_value,
    actual_value,
    actual_value - expected_value,
    IF(ABS(actual_value - expected_value) < 0.000001, 'PASS', 'FAIL'),
    IF(ABS(actual_value - expected_value) < 0.000001, '🟢', '🔴'),
    IF(ABS(actual_value - expected_value) < 0.000001,
       'Metric sums match (raw_dedup vs bronze window).',
       CONCAT('exec exp=', CAST(exp_exec AS STRING), ', act=', CAST(act_exec AS STRING),
              ' | mentions exp=', CAST(exp_mentions AS STRING), ', act=', CAST(act_mentions AS STRING),
              ' | sov exp=', CAST(exp_sov AS STRING), ', act=', CAST(act_sov AS STRING),
              ' | vis exp=', CAST(exp_vis AS STRING), ', act=', CAST(act_vis AS STRING))
    ),
    'If FAIL: Bronze missing rows or dedup mismatch vs raw; verify lookback + ordering.',
    IF(ABS(actual_value - expected_value) >= 0.000001, TRUE, FALSE),
    IF(ABS(actual_value - expected_value) < 0.000001, TRUE, FALSE),
    IF(ABS(actual_value - expected_value) >= 0.000001, TRUE, FALSE)
  FROM t;

END;