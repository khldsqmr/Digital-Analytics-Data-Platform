/*===============================================================================
FILE: 02_sp_qa_sdi_profound_bronze_visibility_tag_daily.sql
LAYER: Bronze | QA
DATASET: prj-dbi-prd-1.ds_dbi_digitalmedia_automation
PROC:  sp_qa_sdi_profound_bronze_visibility_tag_daily

PURPOSE:
  3 critical + 2 reconciliation tests for:
    sdi_profound_bronze_visibility_tag_daily

ALIGNMENT:
  - RAW source + filter matches merge SP (File_Load_datetime lookback)
  - RAW dedup grain + ORDER BY matches merge SP
  - Bronze comparison window matches merge SP lookback (file_load_datetime)

OUTPUT:
  Exactly 5 rows inserted into sdi_profound_bronze_test_results per run.
===============================================================================*/
CREATE OR REPLACE PROCEDURE
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_qa_sdi_profound_bronze_visibility_tag_daily`()
OPTIONS(strict_mode=false)
BEGIN
  DECLARE lookback_days INT64 DEFAULT 60;
  DECLARE freshness_days INT64 DEFAULT 7;

  DECLARE run_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP();
  DECLARE run_dt DATE DEFAULT CURRENT_DATE();
  DECLARE tbl STRING DEFAULT 'sdi_profound_bronze_visibility_tag_daily';

  -- 0) RAW expected (dedup) aligned to merge SP
  CREATE OR REPLACE TEMP TABLE _raw_dedup AS
  WITH src AS (
    SELECT
      SAFE_CAST(raw.account_id AS STRING) AS account_id,
      NULLIF(TRIM(SAFE_CAST(raw.asset_name AS STRING)), '') AS asset_name,
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
    FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_seo_profound_visibility_tag_daily_tmo` raw
    WHERE SAFE_CAST(raw.File_Load_datetime AS DATETIME) IS NOT NULL
      AND SAFE_CAST(raw.File_Load_datetime AS DATETIME) >= DATETIME_SUB(CURRENT_DATETIME(), INTERVAL lookback_days DAY)
  ),
  cleaned AS (
    SELECT * FROM src
    WHERE date IS NOT NULL AND account_id IS NOT NULL AND asset_name IS NOT NULL AND tag IS NOT NULL AND date_yyyymmdd IS NOT NULL
  ),
  dedup AS (
    SELECT * EXCEPT(rn)
    FROM (
      SELECT c.*,
        ROW_NUMBER() OVER (
          PARTITION BY account_id, asset_name, tag, date_yyyymmdd
          ORDER BY file_load_datetime DESC, filename DESC, insert_date DESC
        ) rn
      FROM cleaned c
    )
    WHERE rn = 1
  )
  SELECT * FROM dedup;

  -- 1) Bronze window aligned to ingestion lookback
  CREATE OR REPLACE TEMP TABLE _bronze_window AS
  SELECT *
  FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_profound_bronze_visibility_tag_daily`
  WHERE file_load_datetime IS NOT NULL
    AND file_load_datetime >= DATETIME_SUB(CURRENT_DATETIME(), INTERVAL lookback_days DAY);

  -- CRITICAL 1) Freshness
  INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_profound_bronze_test_results`
  WITH x AS (SELECT MAX(file_load_datetime) max_fldt FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_profound_bronze_visibility_tag_daily`),
       y AS (SELECT 1.0 expected_value, IF(max_fldt >= DATETIME_SUB(CURRENT_DATETIME(), INTERVAL freshness_days DAY),1.0,0.0) actual_value, max_fldt FROM x)
  SELECT run_ts, run_dt, tbl,'critical','freshness_max_file_load_datetime_within_last_days','HIGH',
         expected_value, actual_value, actual_value-expected_value,
         IF(actual_value=expected_value,'PASS','FAIL'), IF(actual_value=expected_value,'🟢','🔴'),
         IF(actual_value=expected_value,'Recent loads exist (MAX(file_load_datetime) is fresh).',
            CONCAT('No loads within ',CAST(freshness_days AS STRING),' days. max(file_load_datetime)=',CAST(max_fldt AS STRING))),
         'If FAIL: check raw ingestion, merge schedule, and file_load_datetime population.',
         IF(actual_value!=expected_value,TRUE,FALSE), IF(actual_value=expected_value,TRUE,FALSE), IF(actual_value!=expected_value,TRUE,FALSE)
  FROM y;

  -- CRITICAL 2) Null key/date
  INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_profound_bronze_test_results`
  WITH z AS (SELECT COUNTIF(account_id IS NULL OR asset_name IS NULL OR tag IS NULL OR date_yyyymmdd IS NULL OR date IS NULL) bad_rows FROM _bronze_window)
  SELECT run_ts, run_dt, tbl,'critical','null_key_or_date_rows_last_N_days','HIGH',
         0.0, CAST(bad_rows AS FLOAT64), CAST(bad_rows AS FLOAT64),
         IF(bad_rows=0,'PASS','FAIL'), IF(bad_rows=0,'🟢','🔴'),
         IF(bad_rows=0,'No null key/date rows found.', CONCAT('Found ',CAST(bad_rows AS STRING),' rows with null key/date.')),
         'If FAIL: verify TRIM/NULLIF + SAFE.PARSE_DATE + key filters in merge.',
         IF(bad_rows>0,TRUE,FALSE), IF(bad_rows=0,TRUE,FALSE), IF(bad_rows>0,TRUE,FALSE)
  FROM z;

  -- CRITICAL 3) Duplicate grain
  INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_profound_bronze_test_results`
  WITH d AS (
    SELECT COUNT(*) dup_groups
    FROM (SELECT account_id, asset_name, tag, date_yyyymmdd FROM _bronze_window GROUP BY 1,2,3,4 HAVING COUNT(*)>1)
  )
  SELECT run_ts, run_dt, tbl,'critical','duplicate_grain_groups_last_N_days','HIGH',
         0.0, CAST(dup_groups AS FLOAT64), CAST(dup_groups AS FLOAT64),
         IF(dup_groups=0,'PASS','FAIL'), IF(dup_groups=0,'🟢','🔴'),
         IF(dup_groups=0,'No duplicate grain groups detected.', CONCAT('Duplicate grain groups found: ',CAST(dup_groups AS STRING))),
         'If FAIL: check MERGE key + dedup ORDER BY tie-breakers.',
         IF(dup_groups>0,TRUE,FALSE), IF(dup_groups=0,TRUE,FALSE), IF(dup_groups>0,TRUE,FALSE)
  FROM d;

  -- RECON 1) Row count
  INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_profound_bronze_test_results`
  WITH a AS (SELECT COUNT(*) exp_cnt FROM _raw_dedup),
       b AS (SELECT COUNT(*) act_cnt FROM _bronze_window)
  SELECT run_ts, run_dt, tbl,'reconciliation','raw_dedup_vs_bronze_row_count_last_N_days','HIGH',
         CAST(exp_cnt AS FLOAT64), CAST(act_cnt AS FLOAT64), CAST(act_cnt-exp_cnt AS FLOAT64),
         IF(exp_cnt=act_cnt,'PASS','FAIL'), IF(exp_cnt=act_cnt,'🟢','🔴'),
         IF(exp_cnt=act_cnt,'Row counts match (raw_dedup vs bronze window).', CONCAT('expected(raw_dedup)=',exp_cnt,', actual(bronze)=',act_cnt)),
         'If FAIL: Bronze missing rows in lookback window; run backfill or re-run merge with lookback_days.',
         IF(exp_cnt!=act_cnt,TRUE,FALSE), IF(exp_cnt=act_cnt,TRUE,FALSE), IF(exp_cnt!=act_cnt,TRUE,FALSE)
  FROM a,b;

  -- RECON 2) Metric sums (visibility checksum)
  INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_profound_bronze_test_results`
  WITH r AS (SELECT SUM(executions) exec_sum,SUM(mentions_count) mentions_sum,SUM(share_of_voice) sov_sum,SUM(visibility_score) vis_sum FROM _raw_dedup),
       s AS (SELECT SUM(executions) exec_sum,SUM(mentions_count) mentions_sum,SUM(share_of_voice) sov_sum,SUM(visibility_score) vis_sum FROM _bronze_window),
       t AS (
         SELECT
           (COALESCE(r.exec_sum,0)+COALESCE(r.mentions_sum,0)+COALESCE(r.sov_sum,0)+COALESCE(r.vis_sum,0)) expected_value,
           (COALESCE(s.exec_sum,0)+COALESCE(s.mentions_sum,0)+COALESCE(s.sov_sum,0)+COALESCE(s.vis_sum,0)) actual_value,
           r.exec_sum exp_exec,s.exec_sum act_exec,r.mentions_sum exp_mentions,s.mentions_sum act_mentions,r.sov_sum exp_sov,s.sov_sum act_sov,r.vis_sum exp_vis,s.vis_sum act_vis
         FROM r,s
       )
  SELECT run_ts, run_dt, tbl,'reconciliation','raw_dedup_vs_bronze_metric_sums_last_N_days','HIGH',
         expected_value, actual_value, actual_value-expected_value,
         IF(ABS(actual_value-expected_value)<0.000001,'PASS','FAIL'), IF(ABS(actual_value-expected_value)<0.000001,'🟢','🔴'),
         IF(ABS(actual_value-expected_value)<0.000001,'Metric sums match (raw_dedup vs bronze window).',
            CONCAT('exec exp=',exp_exec,', act=',act_exec,' | mentions exp=',exp_mentions,', act=',act_mentions,' | sov exp=',exp_sov,', act=',act_sov,' | vis exp=',exp_vis,', act=',act_vis)),
         'If FAIL: Bronze missing rows or dedup mismatch vs raw; verify lookback + ordering.',
         IF(ABS(actual_value-expected_value)>=0.000001,TRUE,FALSE), IF(ABS(actual_value-expected_value)<0.000001,TRUE,FALSE), IF(ABS(actual_value-expected_value)>=0.000001,TRUE,FALSE)
  FROM t;

END;