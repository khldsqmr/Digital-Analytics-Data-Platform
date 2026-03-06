/* =================================================================================================
FILE: 04_sp_merge_sdi_profound_bronze_visibility_topic_tag_daily.sql
LAYER: Bronze
DATASET: prj-dbi-prd-1.ds_dbi_digitalmedia_automation
PROC:  sp_merge_sdi_profound_bronze_visibility_topic_tag_daily
TARGET TABLE: sdi_profound_bronze_visibility_topic_tag_daily

PURPOSE:
  Daily incremental upsert of ProFound Visibility Topic+Tag Daily into Bronze.

SOURCE TABLE:
  prj-dbi-prd-1.ds_dbi_improvado_master.sdi_seo_profound_visibility_topic_tag_daily_tmo

TARGET TABLE:
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_profound_bronze_visibility_topic_tag_daily

DECLARED BUSINESS GRAIN:
  account_id + asset_name + topic + tag + date_yyyymmdd
================================================================================================= */

CREATE OR REPLACE PROCEDURE
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_merge_sdi_profound_bronze_visibility_topic_tag_daily`()
OPTIONS(strict_mode=false)
BEGIN

  DECLARE lookback_days INT64 DEFAULT 60;

  MERGE `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_profound_bronze_visibility_topic_tag_daily` AS T
  USING (
    WITH src AS (
      SELECT
        NULLIF(TRIM(SAFE_CAST(raw.account_id AS STRING)), '') AS account_id,
        NULLIF(TRIM(SAFE_CAST(raw.account_name AS STRING)), '') AS account_name,
        NULLIF(TRIM(SAFE_CAST(raw.asset_name AS STRING)), '') AS asset_name,
        NULLIF(TRIM(SAFE_CAST(raw.asset_id AS STRING)), '') AS asset_id,
        NULLIF(TRIM(SAFE_CAST(raw.topic AS STRING)), '') AS topic,
        NULLIF(TRIM(SAFE_CAST(raw.tag AS STRING)), '') AS tag,

        NULLIF(TRIM(SAFE_CAST(raw.date_yyyymmdd AS STRING)), '') AS date_yyyymmdd,
        SAFE.PARSE_DATE('%Y%m%d', NULLIF(TRIM(SAFE_CAST(raw.date_yyyymmdd AS STRING)), '')) AS date,
        SAFE_CAST(raw.date AS INT64) AS raw_date_int64,

        SAFE_CAST(raw.executions AS FLOAT64) AS executions,
        SAFE_CAST(raw.mentions_count AS FLOAT64) AS mentions_count,
        SAFE_CAST(raw.share_of_voice AS FLOAT64) AS share_of_voice,
        SAFE_CAST(raw.visibility_score AS FLOAT64) AS visibility_score,

        SAFE_CAST(raw.__insert_date AS INT64) AS insert_date,
        SAFE_CAST(raw.File_Load_datetime AS DATETIME) AS file_load_datetime,
        NULLIF(TRIM(SAFE_CAST(raw.Filename AS STRING)), '') AS filename
      FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_seo_profound_visibility_topic_tag_daily_tmo` AS raw
      WHERE SAFE_CAST(raw.File_Load_datetime AS DATETIME) IS NOT NULL
        AND SAFE_CAST(raw.File_Load_datetime AS DATETIME)
            >= DATETIME_SUB(CURRENT_DATETIME(), INTERVAL lookback_days DAY)
    ),

    cleaned AS (
      SELECT *
      FROM src
      WHERE account_id IS NOT NULL
        AND asset_name IS NOT NULL
        AND topic IS NOT NULL
        AND tag IS NOT NULL
        AND date_yyyymmdd IS NOT NULL
        AND date IS NOT NULL
    ),

    dedup AS (
      SELECT * EXCEPT (rn)
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

    SELECT
      account_id,
      account_name,
      asset_name,
      topic,
      tag,
      date_yyyymmdd,
      date,
      raw_date_int64,
      executions,
      mentions_count,
      share_of_voice,
      visibility_score,
      insert_date,
      file_load_datetime,
      filename
    FROM dedup
  ) AS S
  ON  T.account_id    = S.account_id
  AND T.asset_name    = S.asset_name
  AND T.topic         = S.topic
  AND T.tag           = S.tag
  AND T.date_yyyymmdd = S.date_yyyymmdd

  WHEN MATCHED THEN
    UPDATE SET
      account_name       = S.account_name,
      date               = S.date,
      raw_date_int64     = S.raw_date_int64,
      executions         = S.executions,
      mentions_count     = S.mentions_count,
      share_of_voice     = S.share_of_voice,
      visibility_score   = S.visibility_score,
      insert_date        = S.insert_date,
      file_load_datetime = S.file_load_datetime,
      filename           = S.filename

  WHEN NOT MATCHED THEN
    INSERT (
      account_id,
      account_name,
      asset_name,
      topic,
      tag,
      date_yyyymmdd,
      date,
      raw_date_int64,
      executions,
      mentions_count,
      share_of_voice,
      visibility_score,
      insert_date,
      file_load_datetime,
      filename
    )
    VALUES (
      S.account_id,
      S.account_name,
      S.asset_name,
      S.topic,
      S.tag,
      S.date_yyyymmdd,
      S.date,
      S.raw_date_int64,
      S.executions,
      S.mentions_count,
      S.share_of_voice,
      S.visibility_score,
      S.insert_date,
      S.file_load_datetime,
      S.filename
    );

END;