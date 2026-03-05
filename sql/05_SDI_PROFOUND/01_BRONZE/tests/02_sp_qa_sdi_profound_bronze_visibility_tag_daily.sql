
/*
===============================================================================
FILE: 02_sp_qa_sdi_profound_bronze_visibility_tag_daily.sql
LAYER: Bronze | QA
PROC:  sp_qa_sdi_profound_bronze_visibility_tag_daily
BRONZE: sdi_profound_bronze_visibility_tag_daily
RAW:    prj-dbi-prd-1.ds_dbi_improvado_master.sdi_seo_profound_visibility_tag_daily_tmo
GRAIN:  account_id + asset_name + tag + date_yyyymmdd
TESTS:  3 critical + 2 reconciliation (5 rows total)
===============================================================================
*/
CREATE OR REPLACE PROCEDURE
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_qa_sdi_profound_bronze_visibility_tag_daily`()
OPTIONS(strict_mode=false)
BEGIN
  DECLARE lookback_days INT64 DEFAULT 60;
  DECLARE freshness_hours INT64 DEFAULT 36;

  -- (1) Freshness
  INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_profound_bronze_test_results`
  WITH s AS (
    SELECT COUNT(1) AS actual_rows
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_profound_bronze_visibility_tag_daily`
    WHERE file_load_datetime >= DATETIME_SUB(CURRENT_DATETIME(), INTERVAL freshness_hours HOUR)
  )
  SELECT CURRENT_TIMESTAMP(), CURRENT_DATE(),
    'sdi_profound_bronze_visibility_tag_daily','critical','freshness_rows_in_last_hours','HIGH',
    1, CAST(actual_rows AS FLOAT64), CAST(actual_rows AS FLOAT64) - 1,
    IF(actual_rows>=1,'PASS','FAIL'), IF(actual_rows>=1,'🟢','🔴'),
    IF(actual_rows>=1,'Recent Bronze loads exist.','No Bronze rows in freshness window.'),
    'Check raw ingestion + Bronze merge schedule/lookback.',
    IF(actual_rows<1,TRUE,FALSE), IF(actual_rows>=1,TRUE,FALSE), IF(actual_rows<1,TRUE,FALSE)
  FROM s;

  -- (2) Duplicate grain
  INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_profound_bronze_test_results`
  WITH d AS (
    SELECT COUNT(1) AS actual_dup_groups
    FROM (
      SELECT account_id, asset_name, tag, date_yyyymmdd, COUNT(*) cnt
      FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_profound_bronze_visibility_tag_daily`
      WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL lookback_days DAY)
      GROUP BY 1,2,3,4
      HAVING cnt > 1
    )
  )
  SELECT CURRENT_TIMESTAMP(), CURRENT_DATE(),
    'sdi_profound_bronze_visibility_tag_daily','critical','duplicate_grain_groups_last_N_days','HIGH',
    0, CAST(actual_dup_groups AS FLOAT64), CAST(actual_dup_groups AS FLOAT64),
    IF(actual_dup_groups=0,'PASS','FAIL'), IF(actual_dup_groups=0,'🟢','🔴'),
    IF(actual_dup_groups=0,'No duplicate grain groups detected.','Duplicate grain groups found in Bronze.'),
    'Check Bronze MERGE key + dedup ORDER BY tie-breakers.',
    IF(actual_dup_groups>0,TRUE,FALSE), IF(actual_dup_groups=0,TRUE,FALSE), IF(actual_dup_groups>0,TRUE,FALSE)
  FROM d;

  -- (3) Null key/date rows
  INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_profound_bronze_test_results`
  WITH n AS (
    SELECT COUNT(1) AS actual_bad_rows
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_profound_bronze_visibility_tag_daily`
    WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL lookback_days DAY)
      AND (account_id IS NULL OR asset_name IS NULL OR tag IS NULL OR date_yyyymmdd IS NULL OR date IS NULL)
  )
  SELECT CURRENT_TIMESTAMP(), CURRENT_DATE(),
    'sdi_profound_bronze_visibility_tag_daily','critical','null_key_or_date_rows_last_N_days','HIGH',
    0, CAST(actual_bad_rows AS FLOAT64), CAST(actual_bad_rows AS FLOAT64),
    IF(actual_bad_rows=0,'PASS','FAIL'), IF(actual_bad_rows=0,'🟢','🔴'),
    IF(actual_bad_rows=0,'No null key/date rows found.','Null key/date rows found in Bronze.'),
    'Check raw data quality + SAFE.PARSE_DATE + TRIM/NULLIF rules.',
    IF(actual_bad_rows>0,TRUE,FALSE), IF(actual_bad_rows=0,TRUE,FALSE), IF(actual_bad_rows>0,TRUE,FALSE)
  FROM n;

  -- (4) RECON: Row count
  INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_profound_bronze_test_results`
  WITH raw_src AS (
    SELECT
      SAFE_CAST(account_id AS STRING) AS account_id,
      SAFE_CAST(asset_name AS STRING) AS asset_name,
      SAFE_CAST(tag AS STRING) AS tag,
      SAFE_CAST(date_yyyymmdd AS STRING) AS date_yyyymmdd,
      SAFE.PARSE_DATE('%Y%m%d', SAFE_CAST(date_yyyymmdd AS STRING)) AS date,
      SAFE_CAST(__insert_date AS INT64) AS insert_date,
      SAFE_CAST(File_Load_datetime AS DATETIME) AS file_load_datetime,
      SAFE_CAST(Filename AS STRING) AS filename
    FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_seo_profound_visibility_tag_daily_tmo`
    WHERE SAFE.PARSE_DATE('%Y%m%d', SAFE_CAST(date_yyyymmdd AS STRING))
      >= DATE_SUB(CURRENT_DATE(), INTERVAL lookback_days DAY)
  ),
  raw_dedup AS (
    SELECT * EXCEPT(rn)
    FROM (
      SELECT r.*,
        ROW_NUMBER() OVER (
          PARTITION BY account_id, asset_name, tag, date_yyyymmdd
          ORDER BY file_load_datetime DESC, filename DESC, insert_date DESC
        ) rn
      FROM raw_src r
      WHERE date IS NOT NULL AND account_id IS NOT NULL AND asset_name IS NOT NULL AND tag IS NOT NULL AND date_yyyymmdd IS NOT NULL
    )
    WHERE rn=1
  ),
  exp AS (SELECT COUNT(1) expected_rows FROM raw_dedup),
  act AS (
    SELECT COUNT(1) actual_rows
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_profound_bronze_visibility_tag_daily`
    WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL lookback_days DAY)
  ),
  f AS (SELECT * FROM exp CROSS JOIN act)
  SELECT CURRENT_TIMESTAMP(), CURRENT_DATE(),
    'sdi_profound_bronze_visibility_tag_daily','reconciliation','raw_dedup_vs_bronze_row_count_last_N_days','HIGH',
    CAST(expected_rows AS FLOAT64), CAST(actual_rows AS FLOAT64), CAST(actual_rows AS FLOAT64)-CAST(expected_rows AS FLOAT64),
    IF(expected_rows=actual_rows,'PASS','FAIL'),
    IF(expected_rows=actual_rows,'🟢','🔴'),
    CONCAT('expected(raw_dedup)=',CAST(expected_rows AS STRING),', actual(bronze)=',CAST(actual_rows AS STRING)),
    'If FAIL: check lookback coverage + dedup ordering.',
    IF(expected_rows<>actual_rows,TRUE,FALSE),
    IF(expected_rows=actual_rows,TRUE,FALSE),
    IF(expected_rows<>actual_rows,TRUE,FALSE)
  FROM f;

  -- (5) RECON: Metric sums
  INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_profound_bronze_test_results`
  WITH raw_src AS (
    SELECT
      SAFE_CAST(account_id AS STRING) AS account_id,
      SAFE_CAST(asset_name AS STRING) AS asset_name,
      SAFE_CAST(tag AS STRING) AS tag,
      SAFE_CAST(date_yyyymmdd AS STRING) AS date_yyyymmdd,
      SAFE.PARSE_DATE('%Y%m%d', SAFE_CAST(date_yyyymmdd AS STRING)) AS date,
      SAFE_CAST(executions AS FLOAT64) AS executions,
      SAFE_CAST(mentions_count AS FLOAT64) AS mentions_count,
      SAFE_CAST(share_of_voice AS FLOAT64) AS share_of_voice,
      SAFE_CAST(visibility_score AS FLOAT64) AS visibility_score,
      SAFE_CAST(__insert_date AS INT64) AS insert_date,
      SAFE_CAST(File_Load_datetime AS DATETIME) AS file_load_datetime,
      SAFE_CAST(Filename AS STRING) AS filename
    FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_seo_profound_visibility_tag_daily_tmo`
    WHERE SAFE.PARSE_DATE('%Y%m%d', SAFE_CAST(date_yyyymmdd AS STRING))
      >= DATE_SUB(CURRENT_DATE(), INTERVAL lookback_days DAY)
  ),
  raw_dedup AS (
    SELECT * EXCEPT(rn)
    FROM (
      SELECT r.*,
        ROW_NUMBER() OVER (
          PARTITION BY account_id, asset_name, tag, date_yyyymmdd
          ORDER BY file_load_datetime DESC, filename DESC, insert_date DESC
        ) rn
      FROM raw_src r
      WHERE date IS NOT NULL AND account_id IS NOT NULL AND asset_name IS NOT NULL AND tag IS NOT NULL AND date_yyyymmdd IS NOT NULL
    )
    WHERE rn=1
  ),
  r AS (
    SELECT
      IFNULL(SUM(executions),0) exec,
      IFNULL(SUM(mentions_count),0) mentions,
      IFNULL(SUM(share_of_voice),0) sov,
      IFNULL(SUM(visibility_score),0) vis
    FROM raw_dedup
  ),
  b AS (
    SELECT
      IFNULL(SUM(executions),0) exec,
      IFNULL(SUM(mentions_count),0) mentions,
      IFNULL(SUM(share_of_voice),0) sov,
      IFNULL(SUM(visibility_score),0) vis
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_profound_bronze_visibility_tag_daily`
    WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL lookback_days DAY)
  ),
  f AS (
    SELECT r.exec exp_exec, b.exec act_exec,
           r.mentions exp_mentions, b.mentions act_mentions,
           r.sov exp_sov, b.sov act_sov,
           r.vis exp_vis, b.vis act_vis
    FROM r CROSS JOIN b
  )
  SELECT CURRENT_TIMESTAMP(), CURRENT_DATE(),
    'sdi_profound_bronze_visibility_tag_daily','reconciliation','raw_dedup_vs_bronze_metric_sums_last_N_days','HIGH',
    (exp_exec+exp_mentions+exp_sov+exp_vis),
    (act_exec+act_mentions+act_sov+act_vis),
    (act_exec+act_mentions+act_sov+act_vis)-(exp_exec+exp_mentions+exp_sov+exp_vis),
    IF(exp_exec=act_exec AND exp_mentions=act_mentions AND exp_sov=act_sov AND exp_vis=act_vis,'PASS','FAIL'),
    IF(exp_exec=act_exec AND exp_mentions=act_mentions AND exp_sov=act_sov AND exp_vis=act_vis,'🟢','🔴'),
    CONCAT(
      'exec exp=',CAST(exp_exec AS STRING),', act=',CAST(act_exec AS STRING),
      ' | mentions exp=',CAST(exp_mentions AS STRING),', act=',CAST(act_mentions AS STRING),
      ' | sov exp=',CAST(exp_sov AS STRING),', act=',CAST(act_sov AS STRING),
      ' | vis exp=',CAST(exp_vis AS STRING),', act=',CAST(act_vis AS STRING)
    ),
    'If FAIL: validate dedup ordering + casting + lookback.',
    IF(NOT(exp_exec=act_exec AND exp_mentions=act_mentions AND exp_sov=act_sov AND exp_vis=act_vis),TRUE,FALSE),
    IF(exp_exec=act_exec AND exp_mentions=act_mentions AND exp_sov=act_sov AND exp_vis=act_vis,TRUE,FALSE),
    IF(NOT(exp_exec=act_exec AND exp_mentions=act_mentions AND exp_sov=act_sov AND exp_vis=act_vis),TRUE,FALSE)
  FROM f;

END;

