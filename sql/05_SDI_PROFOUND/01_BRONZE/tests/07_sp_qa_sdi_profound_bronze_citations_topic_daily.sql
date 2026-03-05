
/*
===============================================================================
FILE: 07_sp_qa_sdi_profound_bronze_citations_topic_daily.sql
LAYER: Bronze | QA
PROC:  sp_qa_sdi_profound_bronze_citations_topic_daily
BRONZE: sdi_profound_bronze_citations_topic_daily
RAW:    prj-dbi-prd-1.ds_dbi_improvado_master.sdi_seo_profound_citations_topic_daily_tmo
GRAIN:  account_id + root_domain + topic + date_yyyymmdd
METRICS: count, share_of_voice
TESTS:  3 critical + 2 reconciliation
===============================================================================
*/
CREATE OR REPLACE PROCEDURE
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_qa_sdi_profound_bronze_citations_topic_daily`()
OPTIONS(strict_mode=false)
BEGIN
  DECLARE lookback_days INT64 DEFAULT 60;
  DECLARE freshness_hours INT64 DEFAULT 36;

  -- (1) Freshness
  INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_profound_bronze_test_results`
  WITH s AS (
    SELECT COUNT(1) actual_rows
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_profound_bronze_citations_topic_daily`
    WHERE file_load_datetime >= DATETIME_SUB(CURRENT_DATETIME(), INTERVAL freshness_hours HOUR)
  )
  SELECT CURRENT_TIMESTAMP(), CURRENT_DATE(),
    'sdi_profound_bronze_citations_topic_daily','critical','freshness_rows_in_last_hours','HIGH',
    1, CAST(actual_rows AS FLOAT64), CAST(actual_rows AS FLOAT64)-1,
    IF(actual_rows>=1,'PASS','FAIL'), IF(actual_rows>=1,'🟢','🔴'),
    IF(actual_rows>=1,'Recent Bronze loads exist.','No Bronze rows in freshness window.'),
    'Check ingestion + merge schedule/lookback.',
    IF(actual_rows<1,TRUE,FALSE), IF(actual_rows>=1,TRUE,FALSE), IF(actual_rows<1,TRUE,FALSE)
  FROM s;

  -- (2) Duplicate grain
  INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_profound_bronze_test_results`
  WITH d AS (
    SELECT COUNT(1) actual_dup_groups
    FROM (
      SELECT account_id, root_domain, topic, date_yyyymmdd, COUNT(*) cnt
      FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_profound_bronze_citations_topic_daily`
      WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL lookback_days DAY)
      GROUP BY 1,2,3,4
      HAVING cnt>1
    )
  )
  SELECT CURRENT_TIMESTAMP(), CURRENT_DATE(),
    'sdi_profound_bronze_citations_topic_daily','critical','duplicate_grain_groups_last_N_days','HIGH',
    0, CAST(actual_dup_groups AS FLOAT64), CAST(actual_dup_groups AS FLOAT64),
    IF(actual_dup_groups=0,'PASS','FAIL'), IF(actual_dup_groups=0,'🟢','🔴'),
    IF(actual_dup_groups=0,'No duplicate grain groups detected.','Duplicate grain groups found in Bronze.'),
    'Check MERGE key + dedup ordering.',
    IF(actual_dup_groups>0,TRUE,FALSE), IF(actual_dup_groups=0,TRUE,FALSE), IF(actual_dup_groups>0,TRUE,FALSE)
  FROM d;

  -- (3) Null key/date rows
  INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_profound_bronze_test_results`
  WITH n AS (
    SELECT COUNT(1) actual_bad_rows
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_profound_bronze_citations_topic_daily`
    WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL lookback_days DAY)
      AND (account_id IS NULL OR root_domain IS NULL OR topic IS NULL OR date_yyyymmdd IS NULL OR date IS NULL)
  )
  SELECT CURRENT_TIMESTAMP(), CURRENT_DATE(),
    'sdi_profound_bronze_citations_topic_daily','critical','null_key_or_date_rows_last_N_days','HIGH',
    0, CAST(actual_bad_rows AS FLOAT64), CAST(actual_bad_rows AS FLOAT64),
    IF(actual_bad_rows=0,'PASS','FAIL'), IF(actual_bad_rows=0,'🟢','🔴'),
    IF(actual_bad_rows=0,'No null key/date rows found.','Null key/date rows found in Bronze.'),
    'Check parsing + TRIM/NULLIF rules.',
    IF(actual_bad_rows>0,TRUE,FALSE), IF(actual_bad_rows=0,TRUE,FALSE), IF(actual_bad_rows>0,TRUE,FALSE)
  FROM n;

  -- (4) RECON: Row count
  INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_profound_bronze_test_results`
  WITH raw_src AS (
    SELECT
      SAFE_CAST(account_id AS STRING) account_id,
      SAFE_CAST(root_domain AS STRING) root_domain,
      SAFE_CAST(topic AS STRING) topic,
      SAFE_CAST(date_yyyymmdd AS STRING) date_yyyymmdd,
      SAFE.PARSE_DATE('%Y%m%d', SAFE_CAST(date_yyyymmdd AS STRING)) date,
      SAFE_CAST(__insert_date AS INT64) insert_date,
      SAFE_CAST(File_Load_datetime AS DATETIME) file_load_datetime,
      SAFE_CAST(Filename AS STRING) filename
    FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_seo_profound_citations_topic_daily_tmo`
    WHERE SAFE.PARSE_DATE('%Y%m%d', SAFE_CAST(date_yyyymmdd AS STRING))
      >= DATE_SUB(CURRENT_DATE(), INTERVAL lookback_days DAY)
  ),
  raw_dedup AS (
    SELECT * EXCEPT(rn) FROM (
      SELECT r.*,
        ROW_NUMBER() OVER (
          PARTITION BY account_id, root_domain, topic, date_yyyymmdd
          ORDER BY file_load_datetime DESC, filename DESC, insert_date DESC
        ) rn
      FROM raw_src r
      WHERE date IS NOT NULL AND account_id IS NOT NULL AND root_domain IS NOT NULL AND topic IS NOT NULL AND date_yyyymmdd IS NOT NULL
    ) WHERE rn=1
  ),
  exp AS (SELECT COUNT(1) expected_rows FROM raw_dedup),
  act AS (
    SELECT COUNT(1) actual_rows
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_profound_bronze_citations_topic_daily`
    WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL lookback_days DAY)
  ),
  f AS (SELECT * FROM exp CROSS JOIN act)
  SELECT CURRENT_TIMESTAMP(), CURRENT_DATE(),
    'sdi_profound_bronze_citations_topic_daily','reconciliation','raw_dedup_vs_bronze_row_count_last_N_days','HIGH',
    CAST(expected_rows AS FLOAT64), CAST(actual_rows AS FLOAT64), CAST(actual_rows AS FLOAT64)-CAST(expected_rows AS FLOAT64),
    IF(expected_rows=actual_rows,'PASS','FAIL'),
    IF(expected_rows=actual_rows,'🟢','🔴'),
    CONCAT('expected=',CAST(expected_rows AS STRING),', actual=',CAST(actual_rows AS STRING)),
    'If FAIL: check lookback coverage + dedup ordering.',
    IF(expected_rows<>actual_rows,TRUE,FALSE),
    IF(expected_rows=actual_rows,TRUE,FALSE),
    IF(expected_rows<>actual_rows,TRUE,FALSE)
  FROM f;

  -- (5) RECON: Metric sums
  INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_profound_bronze_test_results`
  WITH raw_src AS (
    SELECT
      SAFE_CAST(account_id AS STRING) account_id,
      SAFE_CAST(root_domain AS STRING) root_domain,
      SAFE_CAST(topic AS STRING) topic,
      SAFE_CAST(date_yyyymmdd AS STRING) date_yyyymmdd,
      SAFE.PARSE_DATE('%Y%m%d', SAFE_CAST(date_yyyymmdd AS STRING)) date,
      SAFE_CAST(count AS FLOAT64) cnt,
      SAFE_CAST(share_of_voice AS FLOAT64) sov,
      SAFE_CAST(__insert_date AS INT64) insert_date,
      SAFE_CAST(File_Load_datetime AS DATETIME) file_load_datetime,
      SAFE_CAST(Filename AS STRING) filename
    FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_seo_profound_citations_topic_daily_tmo`
    WHERE SAFE.PARSE_DATE('%Y%m%d', SAFE_CAST(date_yyyymmdd AS STRING))
      >= DATE_SUB(CURRENT_DATE(), INTERVAL lookback_days DAY)
  ),
  raw_dedup AS (
    SELECT * EXCEPT(rn) FROM (
      SELECT r.*,
        ROW_NUMBER() OVER (
          PARTITION BY account_id, root_domain, topic, date_yyyymmdd
          ORDER BY file_load_datetime DESC, filename DESC, insert_date DESC
        ) rn
      FROM raw_src r
      WHERE date IS NOT NULL AND account_id IS NOT NULL AND root_domain IS NOT NULL AND topic IS NOT NULL AND date_yyyymmdd IS NOT NULL
    ) WHERE rn=1
  ),
  r AS (SELECT IFNULL(SUM(cnt),0) exp_cnt, IFNULL(SUM(sov),0) exp_sov FROM raw_dedup),
  b AS (
    SELECT IFNULL(SUM(count),0) act_cnt, IFNULL(SUM(share_of_voice),0) act_sov
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_profound_bronze_citations_topic_daily`
    WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL lookback_days DAY)
  ),
  f AS (SELECT * FROM r CROSS JOIN b)
  SELECT CURRENT_TIMESTAMP(), CURRENT_DATE(),
    'sdi_profound_bronze_citations_topic_daily','reconciliation','raw_dedup_vs_bronze_metric_sums_last_N_days','HIGH',
    (exp_cnt+exp_sov), (act_cnt+act_sov), (act_cnt+act_sov)-(exp_cnt+exp_sov),
    IF(exp_cnt=act_cnt AND exp_sov=act_sov,'PASS','FAIL'),
    IF(exp_cnt=act_cnt AND exp_sov=act_sov,'🟢','🔴'),
    CONCAT('count exp=',CAST(exp_cnt AS STRING),', act=',CAST(act_cnt AS STRING),
           ' | sov exp=',CAST(exp_sov AS STRING),', act=',CAST(act_sov AS STRING)),
    'If FAIL: validate dedup ordering + casting + lookback.',
    IF(NOT(exp_cnt=act_cnt AND exp_sov=act_sov),TRUE,FALSE),
    IF(exp_cnt=act_cnt AND exp_sov=act_sov,TRUE,FALSE),
    IF(NOT(exp_cnt=act_cnt AND exp_sov=act_sov),TRUE,FALSE)
  FROM f;

END;

