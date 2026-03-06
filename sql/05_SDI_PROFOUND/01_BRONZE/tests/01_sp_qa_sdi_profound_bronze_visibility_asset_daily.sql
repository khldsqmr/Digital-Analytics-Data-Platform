/*===============================================================================
FILE: 01_sp_qa_sdi_profound_bronze_visibility_asset_daily.sql
LAYER: Bronze | QA
DATASET: prj-dbi-prd-1.ds_dbi_digitalmedia_automation
PROC:  sp_qa_sdi_profound_bronze_visibility_asset_daily

PURPOSE:
  Daily unattended QA for:
    sdi_profound_bronze_visibility_asset_daily

VALIDATION STRATEGY:
  This procedure is designed to run automatically every day after Bronze loads finish.
  It does NOT require input dates.

  Instead, it validates a small, stable, complete scope:
    1) latest complete week (Sunday to Saturday)
    2) same aligned week last year (364-day offset to preserve weekday alignment)

WHY THIS DESIGN:
  - avoids incomplete current-week noise
  - gives a strong health signal for recent data
  - gives a comparable year-over-year check
  - stays lightweight enough for daily execution

ALIGNMENT TO BRONZE LOGIC:
  Raw comparison uses the same logical grain and same dedup ORDER BY used in Bronze:
    grain:    account_id + asset_id + date_yyyymmdd
    order by: file_load_datetime DESC, filename DESC, insert_date DESC

TESTS WRITTEN:
  1. duplicate_grain_groups_validation_weeks
  2. null_key_or_date_rows_validation_weeks
  3. freshness_max_file_load_datetime_within_last_days
  4. raw_dedup_vs_bronze_key_reconciliation_validation_weeks
  5. raw_dedup_vs_bronze_metric_reconciliation_validation_weeks

OUTPUT:
  Exactly 5 rows inserted into sdi_profound_bronze_test_results per run.
===============================================================================*/

CREATE OR REPLACE PROCEDURE
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_qa_sdi_profound_bronze_visibility_asset_daily`()
OPTIONS(strict_mode=false)
BEGIN
  DECLARE freshness_days INT64 DEFAULT 3;
  DECLARE metric_tolerance FLOAT64 DEFAULT 0.000001;

  DECLARE run_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP();
  DECLARE test_dt DATE DEFAULT CURRENT_DATE();
  DECLARE tbl STRING DEFAULT 'sdi_profound_bronze_visibility_asset_daily';

  DECLARE ty_week_end DATE DEFAULT DATE_SUB(DATE_TRUNC(CURRENT_DATE(), WEEK(SUNDAY)), INTERVAL 1 DAY);
  DECLARE ty_week_start DATE DEFAULT DATE_SUB(ty_week_end, INTERVAL 6 DAY);

  DECLARE ly_week_end DATE DEFAULT DATE_SUB(ty_week_end, INTERVAL 364 DAY);
  DECLARE ly_week_start DATE DEFAULT DATE_SUB(ty_week_start, INTERVAL 364 DAY);

  -- TEST 1: duplicate grain groups in Bronze across validation weeks
  INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_profound_bronze_test_results`
  WITH bronze_scope AS (
    SELECT *
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_profound_bronze_visibility_asset_daily`
    WHERE (date BETWEEN ty_week_start AND ty_week_end)
       OR (date BETWEEN ly_week_start AND ly_week_end)
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
    run_ts,
    test_dt,
    tbl,
    'critical',
    'duplicate_grain_groups_validation_weeks',
    'HIGH',
    0.0,
    CAST(dup_group_count AS FLOAT64),
    CAST(dup_group_count AS FLOAT64),
    IF(dup_group_count = 0, 'PASS', 'FAIL'),
    IF(dup_group_count = 0, '🟢', '🔴'),
    IF(
      dup_group_count = 0,
      FORMAT('No duplicate grain groups detected for TY week %s to %s and LY-aligned week %s to %s.',
        CAST(ty_week_start AS STRING), CAST(ty_week_end AS STRING), CAST(ly_week_start AS STRING), CAST(ly_week_end AS STRING)),
      'Duplicate grain groups detected for (account_id, asset_id, date_yyyymmdd).'
    ),
    'If FAIL: inspect Bronze uniqueness and confirm the declared business grain is valid.',
    IF(dup_group_count = 0, FALSE, TRUE),
    IF(dup_group_count = 0, TRUE, FALSE),
    IF(dup_group_count = 0, FALSE, TRUE)
  FROM dup_groups;

  -- TEST 2: null key/date rows in Bronze across validation weeks
  INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_profound_bronze_test_results`
  WITH bronze_scope AS (
    SELECT *
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_profound_bronze_visibility_asset_daily`
    WHERE (date BETWEEN ty_week_start AND ty_week_end)
       OR (date BETWEEN ly_week_start AND ly_week_end)
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
    run_ts,
    test_dt,
    tbl,
    'critical',
    'null_key_or_date_rows_validation_weeks',
    'HIGH',
    0.0,
    CAST(bad_rows AS FLOAT64),
    CAST(bad_rows AS FLOAT64),
    IF(bad_rows = 0, 'PASS', 'FAIL'),
    IF(bad_rows = 0, '🟢', '🔴'),
    IF(
      bad_rows = 0,
      'No NULL key/date rows found in Bronze for the validation weeks.',
      'Found NULL values in (account_id, asset_id, date_yyyymmdd, date).'
    ),
    'If FAIL: verify Bronze cleaning filters, NULLIF/TRIM normalization, and DATE parsing logic.',
    IF(bad_rows = 0, FALSE, TRUE),
    IF(bad_rows = 0, TRUE, FALSE),
    IF(bad_rows = 0, FALSE, TRUE)
  FROM bad;

  -- TEST 3: freshness of Bronze loads
  INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_profound_bronze_test_results`
  WITH mx AS (
    SELECT MAX(file_load_datetime) AS max_fld
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_profound_bronze_visibility_asset_daily`
  ),
  flag AS (
    SELECT
      IF(
        max_fld IS NOT NULL
        AND max_fld >= DATETIME_SUB(CURRENT_DATETIME(), INTERVAL freshness_days DAY),
        1.0,
        0.0
      ) AS ok
    FROM mx
  )
  SELECT
    run_ts,
    test_dt,
    tbl,
    'critical',
    'freshness_max_file_load_datetime_within_last_days',
    'HIGH',
    1.0,
    ok,
    ok - 1.0,
    IF(ok = 1.0, 'PASS', 'FAIL'),
    IF(ok = 1.0, '🟢', '🔴'),
    IF(ok = 1.0, 'MAX(file_load_datetime) is fresh.', 'MAX(file_load_datetime) is stale or NULL.'),
    'If FAIL: check source delivery timing, Bronze load schedule, and orchestration dependencies.',
    IF(ok = 1.0, FALSE, TRUE),
    IF(ok = 1.0, TRUE, FALSE),
    IF(ok = 1.0, FALSE, TRUE)
  FROM flag;

  -- TEST 4: key reconciliation raw_dedup vs Bronze across validation weeks
  INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_profound_bronze_test_results`
  WITH raw_src AS (
    SELECT
      NULLIF(TRIM(SAFE_CAST(raw.account_id AS STRING)), '') AS account_id,
      NULLIF(TRIM(SAFE_CAST(raw.asset_id AS STRING)), '') AS asset_id,
      NULLIF(TRIM(SAFE_CAST(raw.date_yyyymmdd AS STRING)), '') AS date_yyyymmdd,
      SAFE.PARSE_DATE('%Y%m%d', NULLIF(TRIM(SAFE_CAST(raw.date_yyyymmdd AS STRING)), '')) AS date,
      SAFE_CAST(raw.__insert_date AS INT64) AS insert_date,
      SAFE_CAST(raw.File_Load_datetime AS DATETIME) AS file_load_datetime,
      NULLIF(TRIM(SAFE_CAST(raw.Filename AS STRING)), '') AS filename
    FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_seo_profound_visibility_asset_daily_tmo` raw
  ),
  scoped_raw AS (
    SELECT *
    FROM raw_src
    WHERE (date BETWEEN ty_week_start AND ty_week_end)
       OR (date BETWEEN ly_week_start AND ly_week_end)
  ),
  raw_clean AS (
    SELECT *
    FROM scoped_raw
    WHERE account_id IS NOT NULL
      AND asset_id IS NOT NULL
      AND date_yyyymmdd IS NOT NULL
      AND date IS NOT NULL
  ),
  raw_dedup AS (
    SELECT * EXCEPT (rn)
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
  raw_keys AS (
    SELECT account_id, asset_id, date_yyyymmdd
    FROM raw_dedup
  ),
  bronze_keys AS (
    SELECT account_id, asset_id, date_yyyymmdd
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_profound_bronze_visibility_asset_daily`
    WHERE (date BETWEEN ty_week_start AND ty_week_end)
       OR (date BETWEEN ly_week_start AND ly_week_end)
  ),
  missing_in_bronze AS (
    SELECT COUNT(*) AS cnt
    FROM (
      SELECT * FROM raw_keys
      EXCEPT DISTINCT
      SELECT * FROM bronze_keys
    )
  ),
  extra_in_bronze AS (
    SELECT COUNT(*) AS cnt
    FROM (
      SELECT * FROM bronze_keys
      EXCEPT DISTINCT
      SELECT * FROM raw_keys
    )
  ),
  final AS (
    SELECT
      m.cnt AS missing_cnt,
      e.cnt AS extra_cnt,
      m.cnt + e.cnt AS issue_cnt
    FROM missing_in_bronze m
    CROSS JOIN extra_in_bronze e
  )
  SELECT
    run_ts,
    test_dt,
    tbl,
    'reconciliation',
    'raw_dedup_vs_bronze_key_reconciliation_validation_weeks',
    'HIGH',
    0.0,
    CAST(issue_cnt AS FLOAT64),
    CAST(issue_cnt AS FLOAT64),
    IF(issue_cnt = 0, 'PASS', 'FAIL'),
    IF(issue_cnt = 0, '🟢', '🔴'),
    FORMAT('missing_in_bronze=%d | extra_in_bronze=%d | TY week=%s to %s | LY-aligned week=%s to %s',
      missing_cnt, extra_cnt,
      CAST(ty_week_start AS STRING), CAST(ty_week_end AS STRING),
      CAST(ly_week_start AS STRING), CAST(ly_week_end AS STRING)),
    'If FAIL: inspect unmatched business keys and verify Bronze merge/backfill consistency.',
    IF(issue_cnt = 0, FALSE, TRUE),
    IF(issue_cnt = 0, TRUE, FALSE),
    IF(issue_cnt = 0, FALSE, TRUE)
  FROM final;

  -- TEST 5: metric reconciliation raw_dedup vs Bronze across validation weeks
  INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_profound_bronze_test_results`
  WITH raw_src AS (
    SELECT
      NULLIF(TRIM(SAFE_CAST(raw.account_id AS STRING)), '') AS account_id,
      NULLIF(TRIM(SAFE_CAST(raw.asset_id AS STRING)), '') AS asset_id,
      NULLIF(TRIM(SAFE_CAST(raw.date_yyyymmdd AS STRING)), '') AS date_yyyymmdd,
      SAFE.PARSE_DATE('%Y%m%d', NULLIF(TRIM(SAFE_CAST(raw.date_yyyymmdd AS STRING)), '')) AS date,
      SAFE_CAST(raw.executions AS FLOAT64) AS executions,
      SAFE_CAST(raw.mentions_count AS FLOAT64) AS mentions_count,
      SAFE_CAST(raw.share_of_voice AS FLOAT64) AS share_of_voice,
      SAFE_CAST(raw.visibility_score AS FLOAT64) AS visibility_score,
      SAFE_CAST(raw.__insert_date AS INT64) AS insert_date,
      SAFE_CAST(raw.File_Load_datetime AS DATETIME) AS file_load_datetime,
      NULLIF(TRIM(SAFE_CAST(raw.Filename AS STRING)), '') AS filename
    FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_seo_profound_visibility_asset_daily_tmo` raw
  ),
  scoped_raw AS (
    SELECT *
    FROM raw_src
    WHERE (date BETWEEN ty_week_start AND ty_week_end)
       OR (date BETWEEN ly_week_start AND ly_week_end)
  ),
  raw_clean AS (
    SELECT *
    FROM scoped_raw
    WHERE account_id IS NOT NULL
      AND asset_id IS NOT NULL
      AND date_yyyymmdd IS NOT NULL
      AND date IS NOT NULL
  ),
  raw_dedup AS (
    SELECT * EXCEPT (rn)
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
      ROUND(IFNULL(SUM(executions), 0), 6) AS exec_sum,
      ROUND(IFNULL(SUM(mentions_count), 0), 6) AS mentions_sum,
      ROUND(IFNULL(SUM(share_of_voice), 0), 6) AS sov_sum,
      ROUND(IFNULL(SUM(visibility_score), 0), 6) AS vis_sum
    FROM raw_dedup
  ),
  bronze_agg AS (
    SELECT
      ROUND(IFNULL(SUM(executions), 0), 6) AS exec_sum,
      ROUND(IFNULL(SUM(mentions_count), 0), 6) AS mentions_sum,
      ROUND(IFNULL(SUM(share_of_voice), 0), 6) AS sov_sum,
      ROUND(IFNULL(SUM(visibility_score), 0), 6) AS vis_sum
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_profound_bronze_visibility_asset_daily`
    WHERE (date BETWEEN ty_week_start AND ty_week_end)
       OR (date BETWEEN ly_week_start AND ly_week_end)
  ),
  final AS (
    SELECT
      ABS(r.exec_sum - b.exec_sum) AS exec_diff,
      ABS(r.mentions_sum - b.mentions_sum) AS mentions_diff,
      ABS(r.sov_sum - b.sov_sum) AS sov_diff,
      ABS(r.vis_sum - b.vis_sum) AS vis_diff,
      GREATEST(
        ABS(r.exec_sum - b.exec_sum),
        ABS(r.mentions_sum - b.mentions_sum),
        ABS(r.sov_sum - b.sov_sum),
        ABS(r.vis_sum - b.vis_sum)
      ) AS max_metric_diff,
      r.exec_sum AS exp_exec, b.exec_sum AS act_exec,
      r.mentions_sum AS exp_mentions, b.mentions_sum AS act_mentions,
      r.sov_sum AS exp_sov, b.sov_sum AS act_sov,
      r.vis_sum AS exp_vis, b.vis_sum AS act_vis
    FROM raw_agg r
    CROSS JOIN bronze_agg b
  )
  SELECT
    run_ts,
    test_dt,
    tbl,
    'reconciliation',
    'raw_dedup_vs_bronze_metric_reconciliation_validation_weeks',
    'HIGH',
    0.0,
    max_metric_diff,
    max_metric_diff,
    IF(
      exec_diff <= metric_tolerance
      AND mentions_diff <= metric_tolerance
      AND sov_diff <= metric_tolerance
      AND vis_diff <= metric_tolerance,
      'PASS', 'FAIL'
    ),
    IF(
      exec_diff <= metric_tolerance
      AND mentions_diff <= metric_tolerance
      AND sov_diff <= metric_tolerance
      AND vis_diff <= metric_tolerance,
      '🟢', '🔴'
    ),
    FORMAT(
      'exec exp=%g act=%g diff=%g | mentions exp=%g act=%g diff=%g | sov exp=%g act=%g diff=%g | vis exp=%g act=%g diff=%g',
      exp_exec, act_exec, exec_diff,
      exp_mentions, act_mentions, mentions_diff,
      exp_sov, act_sov, sov_diff,
      exp_vis, act_vis, vis_diff
    ),
    'If FAIL: inspect metric deltas by date and business key, then validate grain and dedup assumptions.',
    IF(
      exec_diff <= metric_tolerance
      AND mentions_diff <= metric_tolerance
      AND sov_diff <= metric_tolerance
      AND vis_diff <= metric_tolerance,
      FALSE, TRUE
    ),
    IF(
      exec_diff <= metric_tolerance
      AND mentions_diff <= metric_tolerance
      AND sov_diff <= metric_tolerance
      AND vis_diff <= metric_tolerance,
      TRUE, FALSE
    ),
    IF(
      exec_diff <= metric_tolerance
      AND mentions_diff <= metric_tolerance
      AND sov_diff <= metric_tolerance
      AND vis_diff <= metric_tolerance,
      FALSE, TRUE
    )
  FROM final;

END;