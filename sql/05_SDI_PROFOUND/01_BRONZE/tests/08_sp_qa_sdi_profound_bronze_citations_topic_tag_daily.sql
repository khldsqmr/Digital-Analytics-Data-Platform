/*===============================================================================
FILE: 08_sp_qa_sdi_profound_bronze_citations_topic_tag_daily.sql
LAYER: Bronze | QA
PROC:  sp_qa_sdi_profound_bronze_citations_topic_tag_daily

PURPOSE:
  Daily unattended QA for sdi_profound_bronze_citations_topic_tag_daily.

DEDUP ALIGNMENT:
  grain:    account_id + root_domain + topic + tag + date_yyyymmdd
  order by: file_load_datetime DESC, filename DESC, insert_date DESC
===============================================================================*/

CREATE OR REPLACE PROCEDURE
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_qa_sdi_profound_bronze_citations_topic_tag_daily`()
OPTIONS(strict_mode=false)
BEGIN
  DECLARE freshness_days INT64 DEFAULT 3;
  DECLARE metric_tolerance FLOAT64 DEFAULT 0.000001;
  DECLARE run_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP();
  DECLARE test_dt DATE DEFAULT CURRENT_DATE();
  DECLARE tbl STRING DEFAULT 'sdi_profound_bronze_citations_topic_tag_daily';

  DECLARE ty_week_end DATE DEFAULT DATE_SUB(DATE_TRUNC(CURRENT_DATE(), WEEK(SUNDAY)), INTERVAL 1 DAY);
  DECLARE ty_week_start DATE DEFAULT DATE_SUB(ty_week_end, INTERVAL 6 DAY);
  DECLARE ly_week_end DATE DEFAULT DATE_SUB(ty_week_end, INTERVAL 364 DAY);
  DECLARE ly_week_start DATE DEFAULT DATE_SUB(ty_week_start, INTERVAL 364 DAY);

  INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_profound_bronze_test_results`
  WITH bronze_scope AS (
    SELECT * FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_profound_bronze_citations_topic_tag_daily`
    WHERE (date BETWEEN ty_week_start AND ty_week_end) OR (date BETWEEN ly_week_start AND ly_week_end)
  ),
  dup_groups AS (
    SELECT COUNT(*) AS dup_group_count
    FROM (
      SELECT account_id, root_domain, topic, tag, date_yyyymmdd, COUNT(*) c
      FROM bronze_scope GROUP BY 1,2,3,4,5 HAVING c > 1
    )
  )
  SELECT run_ts, test_dt, tbl, 'critical', 'duplicate_grain_groups_validation_weeks', 'HIGH',
         0.0, CAST(dup_group_count AS FLOAT64), CAST(dup_group_count AS FLOAT64),
         IF(dup_group_count=0,'PASS','FAIL'), IF(dup_group_count=0,'🟢','🔴'),
         IF(dup_group_count=0,'No duplicate grain groups detected in Bronze.',
            'Duplicate grain groups detected for (account_id, root_domain, topic, tag, date_yyyymmdd).'),
         'If FAIL: inspect Bronze uniqueness and confirm the declared business grain is valid.',
         IF(dup_group_count=0,FALSE,TRUE), IF(dup_group_count=0,TRUE,FALSE), IF(dup_group_count=0,FALSE,TRUE)
  FROM dup_groups;

  INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_profound_bronze_test_results`
  WITH bronze_scope AS (
    SELECT * FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_profound_bronze_citations_topic_tag_daily`
    WHERE (date BETWEEN ty_week_start AND ty_week_end) OR (date BETWEEN ly_week_start AND ly_week_end)
  ),
  bad AS (
    SELECT COUNT(*) AS bad_rows
    FROM bronze_scope
    WHERE account_id IS NULL OR root_domain IS NULL OR topic IS NULL OR tag IS NULL OR date_yyyymmdd IS NULL OR date IS NULL
  )
  SELECT run_ts, test_dt, tbl, 'critical', 'null_key_or_date_rows_validation_weeks', 'HIGH',
         0.0, CAST(bad_rows AS FLOAT64), CAST(bad_rows AS FLOAT64),
         IF(bad_rows=0,'PASS','FAIL'), IF(bad_rows=0,'🟢','🔴'),
         IF(bad_rows=0,'No NULL key/date rows found in Bronze for the validation weeks.',
            'Found NULL values in (account_id, root_domain, topic, tag, date_yyyymmdd, date).'),
         'If FAIL: verify Bronze cleaning filters and DATE parsing logic.',
         IF(bad_rows=0,FALSE,TRUE), IF(bad_rows=0,TRUE,FALSE), IF(bad_rows=0,FALSE,TRUE)
  FROM bad;

  INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_profound_bronze_test_results`
  WITH mx AS (
    SELECT MAX(file_load_datetime) AS max_fld
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_profound_bronze_citations_topic_tag_daily`
  ),
  flag AS (
    SELECT IF(max_fld IS NOT NULL AND max_fld >= DATETIME_SUB(CURRENT_DATETIME(), INTERVAL freshness_days DAY),1.0,0.0) AS ok
    FROM mx
  )
  SELECT run_ts, test_dt, tbl, 'critical', 'freshness_max_file_load_datetime_within_last_days', 'HIGH',
         1.0, ok, ok-1.0,
         IF(ok=1.0,'PASS','FAIL'), IF(ok=1.0,'🟢','🔴'),
         IF(ok=1.0,'MAX(file_load_datetime) is fresh.','MAX(file_load_datetime) is stale or NULL.'),
         'If FAIL: check source delivery timing, Bronze load schedule, and orchestration dependencies.',
         IF(ok=1.0,FALSE,TRUE), IF(ok=1.0,TRUE,FALSE), IF(ok=1.0,FALSE,TRUE)
  FROM flag;

  INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_profound_bronze_test_results`
  WITH raw_src AS (
    SELECT
      NULLIF(TRIM(SAFE_CAST(raw.account_id AS STRING)), '') AS account_id,
      NULLIF(TRIM(SAFE_CAST(raw.root_domain AS STRING)), '') AS root_domain,
      NULLIF(TRIM(SAFE_CAST(raw.topic AS STRING)), '') AS topic,
      NULLIF(TRIM(SAFE_CAST(raw.tag AS STRING)), '') AS tag,
      NULLIF(TRIM(SAFE_CAST(raw.date_yyyymmdd AS STRING)), '') AS date_yyyymmdd,
      SAFE.PARSE_DATE('%Y%m%d', NULLIF(TRIM(SAFE_CAST(raw.date_yyyymmdd AS STRING)), '')) AS date,
      SAFE_CAST(raw.__insert_date AS INT64) AS insert_date,
      SAFE_CAST(raw.File_Load_datetime AS DATETIME) AS file_load_datetime,
      NULLIF(TRIM(SAFE_CAST(raw.Filename AS STRING)), '') AS filename
    FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_seo_profound_citations_topic_tag_daily_tmo` raw
  ),
  scoped_raw AS (
    SELECT * FROM raw_src
    WHERE (date BETWEEN ty_week_start AND ty_week_end) OR (date BETWEEN ly_week_start AND ly_week_end)
  ),
  raw_clean AS (
    SELECT * FROM scoped_raw
    WHERE account_id IS NOT NULL AND root_domain IS NOT NULL AND topic IS NOT NULL AND tag IS NOT NULL AND date_yyyymmdd IS NOT NULL AND date IS NOT NULL
  ),
  raw_dedup AS (
    SELECT * EXCEPT(rn) FROM (
      SELECT r.*,
             ROW_NUMBER() OVER (
               PARTITION BY account_id, root_domain, topic, tag, date_yyyymmdd
               ORDER BY file_load_datetime DESC, filename DESC, insert_date DESC
             ) AS rn
      FROM raw_clean r
    ) WHERE rn = 1
  ),
  raw_keys AS (SELECT account_id, root_domain, topic, tag, date_yyyymmdd FROM raw_dedup),
  bronze_keys AS (
    SELECT account_id, root_domain, topic, tag, date_yyyymmdd
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_profound_bronze_citations_topic_tag_daily`
    WHERE (date BETWEEN ty_week_start AND ty_week_end) OR (date BETWEEN ly_week_start AND ly_week_end)
  ),
  missing_in_bronze AS (
    SELECT COUNT(*) AS cnt FROM (SELECT * FROM raw_keys EXCEPT DISTINCT SELECT * FROM bronze_keys)
  ),
  extra_in_bronze AS (
    SELECT COUNT(*) AS cnt FROM (SELECT * FROM bronze_keys EXCEPT DISTINCT SELECT * FROM raw_keys)
  ),
  final AS (
    SELECT m.cnt AS missing_cnt, e.cnt AS extra_cnt, m.cnt + e.cnt AS issue_cnt
    FROM missing_in_bronze m CROSS JOIN extra_in_bronze e
  )
  SELECT run_ts, test_dt, tbl, 'reconciliation', 'raw_dedup_vs_bronze_key_reconciliation_validation_weeks', 'HIGH',
         0.0, CAST(issue_cnt AS FLOAT64), CAST(issue_cnt AS FLOAT64),
         IF(issue_cnt=0,'PASS','FAIL'), IF(issue_cnt=0,'🟢','🔴'),
         FORMAT('missing_in_bronze=%d | extra_in_bronze=%d', missing_cnt, extra_cnt),
         'If FAIL: inspect unmatched business keys and verify Bronze merge/backfill consistency.',
         IF(issue_cnt=0,FALSE,TRUE), IF(issue_cnt=0,TRUE,FALSE), IF(issue_cnt=0,FALSE,TRUE)
  FROM final;

  INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_profound_bronze_test_results`
  WITH raw_src AS (
    SELECT
      NULLIF(TRIM(SAFE_CAST(raw.account_id AS STRING)), '') AS account_id,
      NULLIF(TRIM(SAFE_CAST(raw.root_domain AS STRING)), '') AS root_domain,
      NULLIF(TRIM(SAFE_CAST(raw.topic AS STRING)), '') AS topic,
      NULLIF(TRIM(SAFE_CAST(raw.tag AS STRING)), '') AS tag,
      NULLIF(TRIM(SAFE_CAST(raw.date_yyyymmdd AS STRING)), '') AS date_yyyymmdd,
      SAFE.PARSE_DATE('%Y%m%d', NULLIF(TRIM(SAFE_CAST(raw.date_yyyymmdd AS STRING)), '')) AS date,
      SAFE_CAST(raw.count AS FLOAT64) AS count_metric,
      SAFE_CAST(raw.share_of_voice AS FLOAT64) AS share_of_voice,
      SAFE_CAST(raw.__insert_date AS INT64) AS insert_date,
      SAFE_CAST(raw.File_Load_datetime AS DATETIME) AS file_load_datetime,
      NULLIF(TRIM(SAFE_CAST(raw.Filename AS STRING)), '') AS filename
    FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_seo_profound_citations_topic_tag_daily_tmo` raw
  ),
  scoped_raw AS (
    SELECT * FROM raw_src
    WHERE (date BETWEEN ty_week_start AND ty_week_end) OR (date BETWEEN ly_week_start AND ly_week_end)
  ),
  raw_clean AS (
    SELECT * FROM scoped_raw
    WHERE account_id IS NOT NULL AND root_domain IS NOT NULL AND topic IS NOT NULL AND tag IS NOT NULL AND date_yyyymmdd IS NOT NULL AND date IS NOT NULL
  ),
  raw_dedup AS (
    SELECT * EXCEPT(rn) FROM (
      SELECT r.*,
             ROW_NUMBER() OVER (
               PARTITION BY account_id, root_domain, topic, tag, date_yyyymmdd
               ORDER BY file_load_datetime DESC, filename DESC, insert_date DESC
             ) AS rn
      FROM raw_clean r
    ) WHERE rn = 1
  ),
  raw_agg AS (
    SELECT ROUND(IFNULL(SUM(count_metric),0),6) AS cnt_sum,
           ROUND(IFNULL(SUM(share_of_voice),0),6) AS sov_sum
    FROM raw_dedup
  ),
  bronze_agg AS (
    SELECT ROUND(IFNULL(SUM(count),0),6) AS cnt_sum,
           ROUND(IFNULL(SUM(share_of_voice),0),6) AS sov_sum
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_profound_bronze_citations_topic_tag_daily`
    WHERE (date BETWEEN ty_week_start AND ty_week_end) OR (date BETWEEN ly_week_start AND ly_week_end)
  ),
  final AS (
    SELECT
      ABS(r.cnt_sum - b.cnt_sum) AS cnt_diff,
      ABS(r.sov_sum - b.sov_sum) AS sov_diff,
      GREATEST(ABS(r.cnt_sum - b.cnt_sum), ABS(r.sov_sum - b.sov_sum)) AS max_metric_diff,
      r.cnt_sum AS exp_cnt, b.cnt_sum AS act_cnt,
      r.sov_sum AS exp_sov, b.sov_sum AS act_sov
    FROM raw_agg r CROSS JOIN bronze_agg b
  )
  SELECT run_ts, test_dt, tbl, 'reconciliation', 'raw_dedup_vs_bronze_metric_reconciliation_validation_weeks', 'HIGH',
         0.0, max_metric_diff, max_metric_diff,
         IF(cnt_diff <= metric_tolerance AND sov_diff <= metric_tolerance,'PASS','FAIL'),
         IF(cnt_diff <= metric_tolerance AND sov_diff <= metric_tolerance,'🟢','🔴'),
         FORMAT('count exp=%g act=%g diff=%g | sov exp=%g act=%g diff=%g',
                exp_cnt, act_cnt, cnt_diff, exp_sov, act_sov, sov_diff),
         'If FAIL: inspect metric deltas by date and business key, then validate grain and dedup assumptions.',
         IF(cnt_diff <= metric_tolerance AND sov_diff <= metric_tolerance,FALSE,TRUE),
         IF(cnt_diff <= metric_tolerance AND sov_diff <= metric_tolerance,TRUE,FALSE),
         IF(cnt_diff <= metric_tolerance AND sov_diff <= metric_tolerance,FALSE,TRUE)
  FROM final;

END;