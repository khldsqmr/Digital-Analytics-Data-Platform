/*===============================================================================
FILE: 01_sp_qa_sdi_profound_bronze_visibility_asset_daily.sql
LAYER: Bronze | QA
DATASET: prj-dbi-prd-1.ds_dbi_digitalmedia_automation
PROC:  sp_qa_sdi_profound_bronze_visibility_asset_daily

PURPOSE:
  3 critical + 2 reconciliation tests for:
    sdi_profound_bronze_visibility_asset_daily

ALIGNMENT (matches MERGE SP):
  - RAW source: ds_dbi_improvado_master.sdi_seo_profound_visibility_asset_daily_tmo
  - RAW filter: File_Load_datetime IS NOT NULL AND >= NOW - lookback_days
  - Canonical date: SAFE.PARSE_DATE('%Y%m%d', date_yyyymmdd)
  - RAW cleaned keys: account_id, asset_id, date_yyyymmdd non-null; date non-null
  - RAW dedup: (account_id, asset_id, date_yyyymmdd)
      ORDER BY file_load_datetime DESC, filename DESC, insert_date DESC
  - Bronze reconciliation scope: last completed ISO week AND file_load_datetime within lookback_days

OUTPUT:
  Exactly 5 rows inserted into sdi_profound_bronze_test_results per run.
===============================================================================*/

CREATE OR REPLACE PROCEDURE
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_qa_sdi_profound_bronze_visibility_asset_daily`()
OPTIONS(strict_mode=false)
BEGIN
  DECLARE lookback_days INT64 DEFAULT 60;
  DECLARE freshness_days INT64 DEFAULT 3;       -- freshness expectation window
  DECLARE recon_week_offset INT64 DEFAULT 1;    -- 1 = last completed ISO week

  DECLARE week_start DATE;
  DECLARE week_end   DATE;

  SET week_start = DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL recon_week_offset WEEK), WEEK(MONDAY));
  SET week_end   = DATE_ADD(week_start, INTERVAL 6 DAY);

  -- ---------------------------------------------------------------------------
  -- Base CTEs used by multiple tests (scope aligned to MERGE)
  -- ---------------------------------------------------------------------------
  WITH
  bronze_scope AS (
    SELECT *
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_profound_bronze_visibility_asset_daily`
    WHERE date BETWEEN week_start AND week_end
      AND file_load_datetime >= DATETIME_SUB(CURRENT_DATETIME(), INTERVAL lookback_days DAY)
  ),
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
      AND SAFE_CAST(raw.File_Load_datetime AS DATETIME)
        >= DATETIME_SUB(CURRENT_DATETIME(), INTERVAL lookback_days DAY)
      AND SAFE.PARSE_DATE('%Y%m%d', SAFE_CAST(raw.date_yyyymmdd AS STRING))
        BETWEEN week_start AND week_end
  ),
  raw_clean AS (
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
        r.*,
        ROW_NUMBER() OVER (
          PARTITION BY account_id, asset_id, date_yyyymmdd
          ORDER BY file_load_datetime DESC, filename DESC, insert_date DESC
        ) AS rn
      FROM raw_clean r
    )
    WHERE rn = 1
  ),
  agg_raw AS (
    SELECT
      COUNT(1) AS row_cnt,
      SUM(executions) AS sum_executions,
      SUM(mentions_count) AS sum_mentions,
      SUM(share_of_voice) AS sum_sov,
      SUM(visibility_score) AS sum_vis
    FROM raw_dedup
  ),
  agg_bronze AS (
    SELECT
      COUNT(1) AS row_cnt,
      SUM(executions) AS sum_executions,
      SUM(mentions_count) AS sum_mentions,
      SUM(share_of_voice) AS sum_sov,
      SUM(visibility_score) AS sum_vis
    FROM bronze_scope
  ),
  crit_duplicates AS (
    SELECT COUNT(1) AS dup_groups
    FROM (
      SELECT account_id, asset_id, date_yyyymmdd, COUNT(*) AS c
      FROM bronze_scope
      GROUP BY 1,2,3
      HAVING c > 1
    )
  ),
  crit_nulls AS (
    SELECT COUNT(1) AS bad_rows
    FROM bronze_scope
    WHERE account_id IS NULL
       OR asset_id IS NULL
       OR date_yyyymmdd IS NULL
       OR date IS NULL
  ),
  crit_fresh AS (
    SELECT
      CASE
        WHEN MAX(file_load_datetime) >= DATETIME_SUB(CURRENT_DATETIME(), INTERVAL freshness_days DAY) THEN 1
        ELSE 0
      END AS is_fresh
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_profound_bronze_visibility_asset_daily`
  )

  -- ---------------------------------------------------------------------------
  -- Insert EXACTLY 5 rows (3 critical + 2 reconciliation)
  -- ---------------------------------------------------------------------------
  INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_profound_bronze_test_results`
  (
    test_run_timestamp, test_date,
    table_name, test_layer, test_name, severity_level,
    expected_value, actual_value, variance_value,
    status, status_emoji,
    failure_reason, next_step,
    is_critical_failure, is_pass, is_fail
  )
  SELECT
    CURRENT_TIMESTAMP(), CURRENT_DATE(),
    'sdi_profound_bronze_visibility_asset_daily',
    test_layer, test_name, severity_level,
    expected_value, actual_value, actual_value - expected_value,
    IF(actual_value = expected_value, 'PASS', 'FAIL'),
    IF(actual_value = expected_value, '🟢', '🔴'),
    failure_reason,
    next_step,
    (severity_level='HIGH' AND actual_value != expected_value),
    (actual_value = expected_value),
    (actual_value != expected_value)
  FROM (
    -- 1) Duplicates (expected 0)
    SELECT
      'critical' AS test_layer,
      'duplicate_grain_groups_iso_week' AS test_name,
      'HIGH' AS severity_level,
      0.0 AS expected_value,
      CAST((SELECT dup_groups FROM crit_duplicates) AS FLOAT64) AS actual_value,
      'No duplicate grain groups detected.' AS failure_reason,
      'If FAIL: check MERGE key + dedup ORDER BY tie-breakers.' AS next_step

    UNION ALL
    -- 2) Null keys/date (expected 0)
    SELECT
      'critical',
      'null_key_or_date_rows_iso_week',
      'HIGH',
      0.0,
      CAST((SELECT bad_rows FROM crit_nulls) AS FLOAT64),
      'No null key/date rows found.' ,
      'If FAIL: verify TRIM/NULLIF + SAFE.PARSE_DATE + key filters in merge.' 

    UNION ALL
    -- 3) Freshness MAX(file_load_datetime) in last freshness_days (expected 1)
    SELECT
      'critical',
      'freshness_max_file_load_datetime_within_last_days',
      'HIGH',
      1.0,
      CAST((SELECT is_fresh FROM crit_fresh) AS FLOAT64),
      'Recent loads exist (MAX(file_load_datetime) is fresh).',
      'If FAIL: check raw ingestion, merge schedule, and file_load_datetime population.'

    UNION ALL
    -- 4) Reconciliation row count (RAW dedup vs Bronze) (expected==actual)
    SELECT
      'reconciliation',
      'raw_dedup_vs_bronze_row_count_iso_week',
      'HIGH',
      CAST((SELECT row_cnt FROM agg_raw) AS FLOAT64),
      CAST((SELECT row_cnt FROM agg_bronze) AS FLOAT64),
      CONCAT(
        'expected(raw_dedup)=', CAST((SELECT row_cnt FROM agg_raw) AS STRING),
        ', actual(bronze)=', CAST((SELECT row_cnt FROM agg_bronze) AS STRING)
      ),
      'If FAIL: populations differ; run key-diff (anti-join) to find extra/missing keys.'

    UNION ALL
    -- 5) Reconciliation metric sums (packed into one numeric to keep 1 row)
    -- We use a "total sum" = sum(exec)+sum(mentions)+sum(sov)+sum(vis).
    -- The failure_reason prints each metric breakdown.
    SELECT
      'reconciliation',
      'raw_dedup_vs_bronze_metric_sums_iso_week',
      'HIGH',
      CAST(
        (SELECT IFNULL(sum_executions,0)+IFNULL(sum_mentions,0)+IFNULL(sum_sov,0)+IFNULL(sum_vis,0) FROM agg_raw)
        AS FLOAT64
      ),
      CAST(
        (SELECT IFNULL(sum_executions,0)+IFNULL(sum_mentions,0)+IFNULL(sum_sov,0)+IFNULL(sum_vis,0) FROM agg_bronze)
        AS FLOAT64
      ),
      CONCAT(
        'exec exp=', CAST((SELECT IFNULL(sum_executions,0) FROM agg_raw) AS STRING),
        ', act=', CAST((SELECT IFNULL(sum_executions,0) FROM agg_bronze) AS STRING),
        ' | mentions exp=', CAST((SELECT IFNULL(sum_mentions,0) FROM agg_raw) AS STRING),
        ', act=', CAST((SELECT IFNULL(sum_mentions,0) FROM agg_bronze) AS STRING),
        ' | sov exp=', CAST((SELECT IFNULL(sum_sov,0) FROM agg_raw) AS STRING),
        ', act=', CAST((SELECT IFNULL(sum_sov,0) FROM agg_bronze) AS STRING),
        ' | vis exp=', CAST((SELECT IFNULL(sum_vis,0) FROM agg_raw) AS STRING),
        ', act=', CAST((SELECT IFNULL(sum_vis,0) FROM agg_bronze) AS STRING)
      ),
      'If FAIL: populations differ or dedup differs; confirm RAW ORDER BY matches MERGE and run key-diff.'
  );

END;