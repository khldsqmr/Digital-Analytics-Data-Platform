
/* =================================================================================================
FILE: 04_sp_qa_sdi_profound_bronze_visibility_topic_tag_daily.sql
LAYER: Bronze | QA
PROC:  sp_qa_sdi_profound_bronze_visibility_topic_tag_daily
TARGET: sdi_profound_bronze_visibility_topic_tag_daily
RAW:    prj-dbi-prd-1.ds_dbi_improvado_master.sdi_seo_profound_visibility_topic_tag_daily_tmo
GRAIN:  account_id + asset_name + topic + tag + date_yyyymmdd
================================================================================================= */

CREATE OR REPLACE PROCEDURE
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_qa_sdi_profound_bronze_visibility_topic_tag_daily`()
OPTIONS(strict_mode=false)
BEGIN
  -- ✅ prevent "Already Exists" temp table collisions (same script/session reruns)
  DROP TABLE IF EXISTS _raw_dedup;
  DROP TABLE IF EXISTS _bronze_window;
  
  DECLARE lookback_days INT64 DEFAULT 60;
  DECLARE freshness_hours INT64 DEFAULT 24;

  DECLARE run_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP();
  DECLARE run_date DATE DEFAULT CURRENT_DATE();

  DECLARE table_name STRING DEFAULT 'sdi_profound_bronze_visibility_topic_tag_daily';

  CREATE TEMP TABLE _raw_dedup AS
  WITH src AS (
    SELECT
      SAFE_CAST(raw.account_id AS STRING) AS account_id,
      NULLIF(TRIM(SAFE_CAST(raw.asset_name AS STRING)), '') AS asset_name,
      NULLIF(TRIM(SAFE_CAST(raw.topic AS STRING)), '') AS topic,
      NULLIF(TRIM(SAFE_CAST(raw.tag AS STRING)), '') AS tag,
      SAFE_CAST(raw.date_yyyymmdd AS STRING) AS date_yyyymmdd,
      SAFE.PARSE_DATE('%Y%m%d', SAFE_CAST(raw.date_yyyymmdd AS STRING)) AS date,

      SAFE_CAST(raw.executions AS FLOAT64) AS executions,
      SAFE_CAST(raw.mentions_count AS FLOAT64) AS mentions_count,
      SAFE_CAST(raw.share_of_voice AS FLOAT64) AS share_of_voice,
      SAFE_CAST(raw.visibility_score AS FLOAT64) AS visibility_score,

      SAFE_CAST(raw.__insert_date AS INT64) AS insert_date,
      SAFE_CAST(raw.File_Load_datetime AS DATETIME) AS file_load_datetime,
      NULLIF(TRIM(SAFE_CAST(raw.Filename AS STRING)), '') AS filename
    FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_seo_profound_visibility_topic_tag_daily_tmo` raw
    WHERE SAFE_CAST(raw.File_Load_datetime AS DATETIME) IS NOT NULL
      AND SAFE_CAST(raw.File_Load_datetime AS DATETIME) >= DATETIME_SUB(CURRENT_DATETIME(), INTERVAL lookback_days DAY)
  ),
  cleaned AS (
    SELECT *
    FROM src
    WHERE date IS NOT NULL
      AND account_id IS NOT NULL
      AND asset_name IS NOT NULL
      AND topic IS NOT NULL
      AND tag IS NOT NULL
      AND date_yyyymmdd IS NOT NULL
  ),
  dedup AS (
    SELECT * EXCEPT(rn)
    FROM (
      SELECT
        c.*,
        ROW_NUMBER() OVER (
          PARTITION BY account_id, asset_name, topic, tag, date_yyyymmdd
          ORDER BY file_load_datetime DESC, filename DESC, insert_date DESC
        ) AS rn
      FROM cleaned c
    )
    WHERE rn = 1
  )
  SELECT * FROM dedup;

  CREATE TEMP TABLE _bronze_window AS
  SELECT
    account_id, asset_name, topic, tag, date_yyyymmdd, date,
    executions, mentions_count, share_of_voice, visibility_score,
    file_load_datetime
  FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_profound_bronze_visibility_topic_tag_daily`
  WHERE file_load_datetime >= DATETIME_SUB(CURRENT_DATETIME(), INTERVAL lookback_days DAY);

  -- (1) freshness
  INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_profound_bronze_test_results`
  SELECT
    run_ts, run_date, table_name,
    'critical', 'freshness_rows_in_last_hours', 'HIGH',
    1.0,
    CAST(COUNT(1) AS FLOAT64),
    CAST(COUNT(1) AS FLOAT64) - 1.0,
    IF(COUNT(1) >= 1, 'PASS', 'FAIL'),
    IF(COUNT(1) >= 1, '🟢', '🔴'),
    IF(COUNT(1) >= 1, 'Recent Bronze loads exist.', 'No Bronze loads found in the freshness window.'),
    'Check ingestion + merge schedule/lookback.',
    IF(COUNT(1) < 1, TRUE, FALSE),
    IF(COUNT(1) >= 1, TRUE, FALSE),
    IF(COUNT(1) < 1, TRUE, FALSE)
  FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_profound_bronze_visibility_topic_tag_daily`
  WHERE file_load_datetime >= DATETIME_SUB(CURRENT_DATETIME(), INTERVAL freshness_hours HOUR);

  -- (2) null keys/date
  INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_profound_bronze_test_results`
  WITH bad AS (
    SELECT COUNT(1) AS bad_rows
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_profound_bronze_visibility_topic_tag_daily`
    WHERE file_load_datetime >= DATETIME_SUB(CURRENT_DATETIME(), INTERVAL lookback_days DAY)
      AND (account_id IS NULL OR asset_name IS NULL OR topic IS NULL OR tag IS NULL OR date_yyyymmdd IS NULL OR date IS NULL)
  )
  SELECT
    run_ts, run_date, table_name,
    'critical', 'null_key_or_date_rows_last_N_days', 'HIGH',
    0.0,
    CAST(bad_rows AS FLOAT64),
    CAST(bad_rows AS FLOAT64),
    IF(bad_rows = 0, 'PASS', 'FAIL'),
    IF(bad_rows = 0, '🟢', '🔴'),
    IF(bad_rows = 0, 'No null key/date rows found.', 'Null key/date rows found in Bronze window.'),
    'Check parsing + TRIM/NULLIF rules.',
    IF(bad_rows > 0, TRUE, FALSE),
    IF(bad_rows = 0, TRUE, FALSE),
    IF(bad_rows > 0, TRUE, FALSE)
  FROM bad;

  -- (3) duplicates
  INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_profound_bronze_test_results`
  WITH dup AS (
    SELECT COUNT(1) AS dup_groups
    FROM (
      SELECT account_id, asset_name, topic, tag, date_yyyymmdd
      FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_profound_bronze_visibility_topic_tag_daily`
      WHERE file_load_datetime >= DATETIME_SUB(CURRENT_DATETIME(), INTERVAL lookback_days DAY)
      GROUP BY 1,2,3,4,5
      HAVING COUNT(1) > 1
    )
  )
  SELECT
    run_ts, run_date, table_name,
    'critical', 'duplicate_grain_groups_last_N_days', 'HIGH',
    0.0,
    CAST(dup_groups AS FLOAT64),
    CAST(dup_groups AS FLOAT64),
    IF(dup_groups = 0, 'PASS', 'FAIL'),
    IF(dup_groups = 0, '🟢', '🔴'),
    IF(dup_groups = 0, 'No duplicate grain groups detected.', 'Duplicate grain groups found in Bronze window.'),
    'Check MERGE key + dedup ordering.',
    IF(dup_groups > 0, TRUE, FALSE),
    IF(dup_groups = 0, TRUE, FALSE),
    IF(dup_groups > 0, TRUE, FALSE)
  FROM dup;

  -- (4) recon row count
  INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_profound_bronze_test_results`
  WITH exp AS (SELECT COUNT(1) AS expected_cnt FROM _raw_dedup),
       act AS (SELECT COUNT(1) AS actual_cnt FROM _bronze_window)
  SELECT
    run_ts, run_date, table_name,
    'reconciliation', 'raw_dedup_vs_bronze_row_count_last_N_days', 'HIGH',
    CAST(expected_cnt AS FLOAT64),
    CAST(actual_cnt AS FLOAT64),
    CAST(actual_cnt AS FLOAT64) - CAST(expected_cnt AS FLOAT64),
    IF(expected_cnt = actual_cnt, 'PASS', 'FAIL'),
    IF(expected_cnt = actual_cnt, '🟢', '🔴'),
    CONCAT('expected(raw_dedup)=', CAST(expected_cnt AS STRING), ', actual(bronze)=', CAST(actual_cnt AS STRING)),
    'If FAIL: check lookback coverage + dedup ordering + whether Bronze was recreated without backfill.',
    IF(expected_cnt != actual_cnt, TRUE, FALSE),
    IF(expected_cnt = actual_cnt, TRUE, FALSE),
    IF(expected_cnt != actual_cnt, TRUE, FALSE)
  FROM exp, act;

  -- (5) recon metric sums
  INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_profound_bronze_test_results`
  WITH exp AS (
    SELECT
      SUM(COALESCE(executions,0)) AS exp_exec,
      SUM(COALESCE(mentions_count,0)) AS exp_mentions,
      SUM(COALESCE(share_of_voice,0)) AS exp_sov,
      SUM(COALESCE(visibility_score,0)) AS exp_vis
    FROM _raw_dedup
  ),
  act AS (
    SELECT
      SUM(COALESCE(executions,0)) AS act_exec,
      SUM(COALESCE(mentions_count,0)) AS act_mentions,
      SUM(COALESCE(share_of_voice,0)) AS act_sov,
      SUM(COALESCE(visibility_score,0)) AS act_vis
    FROM _bronze_window
  ),
  packed AS (
    SELECT
      (exp_exec + exp_mentions + exp_sov + exp_vis) AS expected_value,
      (act_exec + act_mentions + act_sov + act_vis) AS actual_value,
      CONCAT(
        'exec exp=', CAST(exp_exec AS STRING), ', act=', CAST(act_exec AS STRING),
        ' | mentions exp=', CAST(exp_mentions AS STRING), ', act=', CAST(act_mentions AS STRING),
        ' | sov exp=', CAST(exp_sov AS STRING), ', act=', CAST(act_sov AS STRING),
        ' | vis exp=', CAST(exp_vis AS STRING), ', act=', CAST(act_vis AS STRING)
      ) AS msg,
      IF(exp_exec = act_exec AND exp_mentions = act_mentions AND exp_sov = act_sov AND exp_vis = act_vis, TRUE, FALSE) AS is_match
    FROM exp, act
  )
  SELECT
    run_ts, run_date, table_name,
    'reconciliation', 'raw_dedup_vs_bronze_metric_sums_last_N_days', 'HIGH',
    CAST(expected_value AS FLOAT64),
    CAST(actual_value AS FLOAT64),
    CAST(actual_value AS FLOAT64) - CAST(expected_value AS FLOAT64),
    IF(is_match, 'PASS', 'FAIL'),
    IF(is_match, '🟢', '🔴'),
    msg,
    'If FAIL: validate dedup ordering + casting + lookback alignment (File_Load_datetime).',
    IF(NOT is_match, TRUE, FALSE),
    IF(is_match, TRUE, FALSE),
    IF(NOT is_match, TRUE, FALSE)
  FROM packed;

END;
