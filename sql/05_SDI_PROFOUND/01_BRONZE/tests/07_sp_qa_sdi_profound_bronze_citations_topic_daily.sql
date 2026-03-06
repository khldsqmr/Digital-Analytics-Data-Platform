/*===============================================================================
FILE: 07_sp_qa_sdi_profound_bronze_citations_topic_daily.sql
LAYER: Bronze | QA
DATASET: prj-dbi-prd-1.ds_dbi_digitalmedia_automation
PROC:  sp_qa_sdi_profound_bronze_citations_topic_daily

PURPOSE:
  3 critical + 2 reconciliation tests for:
    sdi_profound_bronze_citations_topic_daily

ALIGNMENT:
  - RAW filter matches merge SP (File_Load_datetime lookback)
  - RAW dedup grain + ORDER BY matches merge SP:
      (account_id, root_domain, topic, date_yyyymmdd)
      ORDER BY file_load_datetime DESC, filename DESC, insert_date DESC
  - Reconciliation compares raw_dedup vs bronze within SAME merge scope

OUTPUT:
  Exactly 5 rows inserted into sdi_profound_bronze_test_results per run.
===============================================================================*/

CREATE OR REPLACE PROCEDURE
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_qa_sdi_profound_bronze_citations_topic_daily`()
OPTIONS(strict_mode=false)
BEGIN
  DECLARE lookback_days   INT64 DEFAULT 60;
  DECLARE recon_days      INT64 DEFAULT 14;
  DECLARE freshness_days  INT64 DEFAULT 3;

  DECLARE run_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP();
  DECLARE test_dt DATE DEFAULT CURRENT_DATE();

  DECLARE tbl STRING DEFAULT 'sdi_profound_bronze_citations_topic_daily';

  -- TEST 1: duplicates
  INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_profound_bronze_test_results`
  WITH bronze_scope AS (
    SELECT *
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_profound_bronze_citations_topic_daily`
    WHERE file_load_datetime IS NOT NULL
      AND file_load_datetime >= DATETIME_SUB(CURRENT_DATETIME(), INTERVAL lookback_days DAY)
      AND date >= DATE_SUB(CURRENT_DATE(), INTERVAL recon_days DAY)
  ),
  dup_groups AS (
    SELECT COUNT(*) AS dup_group_count
    FROM (
      SELECT account_id, root_domain, topic, date_yyyymmdd, COUNT(*) AS c
      FROM bronze_scope
      GROUP BY 1,2,3,4
      HAVING c > 1
    )
  )
  SELECT
    run_ts, test_dt, tbl,
    'critical', 'duplicate_grain_groups_recon_window', 'HIGH',
    0.0, CAST(dup_group_count AS FLOAT64), CAST(dup_group_count AS FLOAT64),
    IF(dup_group_count = 0, 'PASS', 'FAIL'),
    IF(dup_group_count = 0, '🟢', '🔴'),
    IF(dup_group_count = 0, 'No duplicate grain groups detected.',
       'Duplicate grain groups detected in Bronze for (account_id, root_domain, topic, date_yyyymmdd).'),
    'If FAIL: check MERGE keys + dedup ORDER BY; confirm grain is correct.',
    IF(dup_group_count = 0, FALSE, TRUE),
    IF(dup_group_count = 0, TRUE, FALSE),
    IF(dup_group_count = 0, FALSE, TRUE)
  FROM dup_groups;

  -- TEST 2: null keys/dates
  INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_profound_bronze_test_results`
  WITH bronze_scope AS (
    SELECT *
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_profound_bronze_citations_topic_daily`
    WHERE file_load_datetime IS NOT NULL
      AND file_load_datetime >= DATETIME_SUB(CURRENT_DATETIME(), INTERVAL lookback_days DAY)
      AND date >= DATE_SUB(CURRENT_DATE(), INTERVAL recon_days DAY)
  ),
  bad AS (
    SELECT COUNT(*) AS bad_rows
    FROM bronze_scope
    WHERE account_id IS NULL
       OR root_domain IS NULL
       OR topic IS NULL
       OR date_yyyymmdd IS NULL
       OR date IS NULL
  )
  SELECT
    run_ts, test_dt, tbl,
    'critical', 'null_key_or_date_rows_recon_window', 'HIGH',
    0.0, CAST(bad_rows AS FLOAT64), CAST(bad_rows AS FLOAT64),
    IF(bad_rows = 0, 'PASS', 'FAIL'),
    IF(bad_rows = 0, '🟢', '🔴'),
    IF(bad_rows = 0, 'No null key/date rows found.',
       'Found rows with NULL in (account_id, root_domain, topic, date_yyyymmdd, date).'),
    'If FAIL: verify TRIM/NULLIF + SAFE.PARSE_DATE and merge cleaned filters.',
    IF(bad_rows = 0, FALSE, TRUE),
    IF(bad_rows = 0, TRUE, FALSE),
    IF(bad_rows = 0, FALSE, TRUE)
  FROM bad;

  -- TEST 3: freshness
  INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_profound_bronze_test_results`
  WITH mx AS (
    SELECT MAX(file_load_datetime) AS max_fld
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_profound_bronze_citations_topic_daily`
  ),
  flag AS (
    SELECT IF(max_fld IS NOT NULL
              AND max_fld >= DATETIME_SUB(CURRENT_DATETIME(), INTERVAL freshness_days DAY), 1.0, 0.0) AS ok
    FROM mx
  )
  SELECT
    run_ts, test_dt, tbl,
    'critical', 'freshness_max_file_load_datetime_within_last_days', 'HIGH',
    1.0, ok, ok - 1.0,
    IF(ok = 1.0, 'PASS', 'FAIL'),
    IF(ok = 1.0, '🟢', '🔴'),
    IF(ok = 1.0, 'Recent loads exist (MAX(file_load_datetime) is fresh).', 'MAX(file_load_datetime) is stale or NULL.'),
    'If FAIL: check raw ingestion schedule + merge schedule + file_load_datetime population.',
    IF(ok = 1.0, FALSE, TRUE),
    IF(ok = 1.0, TRUE, FALSE),
    IF(ok = 1.0, FALSE, TRUE)
  FROM flag;

  -- TEST 4: row-count reconciliation
  INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_profound_bronze_test_results`
  WITH raw_src AS (
    SELECT
      SAFE_CAST(raw.account_id AS STRING) AS account_id,
      NULLIF(TRIM(SAFE_CAST(raw.root_domain AS STRING)), '') AS root_domain,
      NULLIF(TRIM(SAFE_CAST(raw.topic AS STRING)), '') AS topic,
      SAFE_CAST(raw.date_yyyymmdd AS STRING) AS date_yyyymmdd,
      SAFE.PARSE_DATE('%Y%m%d', SAFE_CAST(raw.date_yyyymmdd AS STRING)) AS date,
      SAFE_CAST(raw.__insert_date AS INT64) AS insert_date,
      SAFE_CAST(raw.File_Load_datetime AS DATETIME) AS file_load_datetime,
      NULLIF(TRIM(SAFE_CAST(raw.Filename AS STRING)), '') AS filename
    FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_seo_profound_citations_topic_daily_tmo` raw
    WHERE SAFE_CAST(raw.File_Load_datetime AS DATETIME) IS NOT NULL
      AND SAFE_CAST(raw.File_Load_datetime AS DATETIME)
        >= DATETIME_SUB(CURRENT_DATETIME(), INTERVAL lookback_days DAY)
  ),
  raw_clean AS (
    SELECT *
    FROM raw_src
    WHERE date IS NOT NULL
      AND account_id IS NOT NULL
      AND root_domain IS NOT NULL
      AND topic IS NOT NULL
      AND date_yyyymmdd IS NOT NULL
      AND date >= DATE_SUB(CURRENT_DATE(), INTERVAL recon_days DAY)
  ),
  raw_dedup AS (
    SELECT * EXCEPT(rn)
    FROM (
      SELECT
        r.*,
        ROW_NUMBER() OVER (
          PARTITION BY account_id, root_domain, topic, date_yyyymmdd
          ORDER BY file_load_datetime DESC, filename DESC, insert_date DESC
        ) AS rn
      FROM raw_clean r
    )
    WHERE rn = 1
  ),
  bronze_scope AS (
    SELECT account_id, root_domain, topic, date_yyyymmdd
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_profound_bronze_citations_topic_daily`
    WHERE file_load_datetime IS NOT NULL
      AND file_load_datetime >= DATETIME_SUB(CURRENT_DATETIME(), INTERVAL lookback_days DAY)
      AND date >= DATE_SUB(CURRENT_DATE(), INTERVAL recon_days DAY)
  ),
  counts AS (
    SELECT (SELECT COUNT(*) FROM raw_dedup) AS expected_cnt,
           (SELECT COUNT(*) FROM bronze_scope) AS actual_cnt
  )
  SELECT
    run_ts, test_dt, tbl,
    'reconciliation', 'raw_dedup_vs_bronze_row_count_recon_window', 'HIGH',
    CAST(expected_cnt AS FLOAT64),
    CAST(actual_cnt AS FLOAT64),
    CAST(actual_cnt - expected_cnt AS FLOAT64),
    IF(expected_cnt = actual_cnt, 'PASS', 'FAIL'),
    IF(expected_cnt = actual_cnt, '🟢', '🔴'),
    FORMAT('expected(raw_dedup)=%d, actual(bronze)=%d', expected_cnt, actual_cnt),
    'If FAIL: Bronze missing/extra rows within MERGE scope. Verify merge lookback + run backfill if recreated.',
    IF(expected_cnt = actual_cnt, FALSE, TRUE),
    IF(expected_cnt = actual_cnt, TRUE, FALSE),
    IF(expected_cnt = actual_cnt, FALSE, TRUE)
  FROM counts;

  -- TEST 5: metric sums reconciliation (count + share_of_voice)
  INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_profound_bronze_test_results`
  WITH raw_src AS (
    SELECT
      SAFE_CAST(raw.account_id AS STRING) AS account_id,
      NULLIF(TRIM(SAFE_CAST(raw.root_domain AS STRING)), '') AS root_domain,
      NULLIF(TRIM(SAFE_CAST(raw.topic AS STRING)), '') AS topic,
      SAFE_CAST(raw.date_yyyymmdd AS STRING) AS date_yyyymmdd,
      SAFE.PARSE_DATE('%Y%m%d', SAFE_CAST(raw.date_yyyymmdd AS STRING)) AS date,
      SAFE_CAST(raw.count AS FLOAT64) AS count_metric,
      SAFE_CAST(raw.share_of_voice AS FLOAT64) AS share_of_voice,
      SAFE_CAST(raw.__insert_date AS INT64) AS insert_date,
      SAFE_CAST(raw.File_Load_datetime AS DATETIME) AS file_load_datetime,
      NULLIF(TRIM(SAFE_CAST(raw.Filename AS STRING)), '') AS filename
    FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_seo_profound_citations_topic_daily_tmo` raw
    WHERE SAFE_CAST(raw.File_Load_datetime AS DATETIME) IS NOT NULL
      AND SAFE_CAST(raw.File_Load_datetime AS DATETIME)
        >= DATETIME_SUB(CURRENT_DATETIME(), INTERVAL lookback_days DAY)
  ),
  raw_clean AS (
    SELECT *
    FROM raw_src
    WHERE date IS NOT NULL
      AND account_id IS NOT NULL
      AND root_domain IS NOT NULL
      AND topic IS NOT NULL
      AND date_yyyymmdd IS NOT NULL
      AND date >= DATE_SUB(CURRENT_DATE(), INTERVAL recon_days DAY)
  ),
  raw_dedup AS (
    SELECT * EXCEPT(rn)
    FROM (
      SELECT
        r.*,
        ROW_NUMBER() OVER (
          PARTITION BY account_id, root_domain, topic, date_yyyymmdd
          ORDER BY file_load_datetime DESC, filename DESC, insert_date DESC
        ) AS rn
      FROM raw_clean r
    )
    WHERE rn = 1
  ),
  raw_agg AS (
    SELECT
      ROUND(IFNULL(SUM(count_metric),0), 6) AS cnt_sum,
      ROUND(IFNULL(SUM(share_of_voice),0), 6) AS sov_sum
    FROM raw_dedup
  ),
  bronze_scope AS (
    SELECT count AS count_metric, share_of_voice
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_profound_bronze_citations_topic_daily`
    WHERE file_load_datetime IS NOT NULL
      AND file_load_datetime >= DATETIME_SUB(CURRENT_DATETIME(), INTERVAL lookback_days DAY)
      AND date >= DATE_SUB(CURRENT_DATE(), INTERVAL recon_days DAY)
  ),
  bronze_agg AS (
    SELECT
      ROUND(IFNULL(SUM(count_metric),0), 6) AS cnt_sum,
      ROUND(IFNULL(SUM(share_of_voice),0), 6) AS sov_sum
    FROM bronze_scope
  ),
  final AS (
    SELECT
      (r.cnt_sum + r.sov_sum) AS expected_total,
      (b.cnt_sum + b.sov_sum) AS actual_total,
      r.cnt_sum AS exp_cnt, b.cnt_sum AS act_cnt,
      r.sov_sum AS exp_sov, b.sov_sum AS act_sov
    FROM raw_agg r CROSS JOIN bronze_agg b
  )
  SELECT
    run_ts, test_dt, tbl,
    'reconciliation', 'raw_dedup_vs_bronze_metric_sums_recon_window', 'HIGH',
    expected_total, actual_total, actual_total - expected_total,
    IF(expected_total = actual_total, 'PASS', 'FAIL'),
    IF(expected_total = actual_total, '🟢', '🔴'),
    FORMAT('count exp=%g, act=%g | sov exp=%g, act=%g', exp_cnt, act_cnt, exp_sov, act_sov),
    'If FAIL: mismatch means missing/extra rows within MERGE scope or dedup mismatch. Verify dedup ORDER BY + merge lookback.',
    IF(expected_total = actual_total, FALSE, TRUE),
    IF(expected_total = actual_total, TRUE, FALSE),
    IF(expected_total = actual_total, FALSE, TRUE)
  FROM final;

END;