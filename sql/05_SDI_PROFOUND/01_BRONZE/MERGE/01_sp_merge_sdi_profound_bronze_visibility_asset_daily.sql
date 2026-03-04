/* =================================================================================================
FILE: 01_sp_merge_sdi_bronze_seo_profound_visibility_asset_daily.sql
LAYER: Bronze
DATASET: prj-dbi-prd-1.ds_dbi_digitalmedia_automation
PROC:  sp_merge_bronze_seo_profound_visibility_asset_daily
TARGET TABLE: sdi_bronze_seo_profound_visibility_asset_daily

PURPOSE:
  Incrementally upsert ProFound Visibility Asset Daily into Bronze:
    - filter by File_Load_datetime lookback window (late arriving files)
    - parse canonical DATE from date_yyyymmdd
    - dedupe within window using latest (file_load_datetime, filename, insert_date)

GRAIN:
  account_id + asset_id + date_yyyymmdd

PARTITION / CLUSTER:
  PARTITION BY date
  CLUSTER BY account_id, asset_id
================================================================================================= */

CREATE OR REPLACE PROCEDURE
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_merge_bronze_seo_profound_visibility_asset_daily`()
OPTIONS(strict_mode=false)
BEGIN
  DECLARE lookback_days INT64 DEFAULT 60;

  MERGE `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_seo_profound_visibility_asset_daily` T
  USING (
    WITH src AS (
      SELECT
        SAFE_CAST(account_id AS STRING) AS account_id,
        NULLIF(TRIM(SAFE_CAST(account_name AS STRING)), '') AS account_name,
        NULLIF(TRIM(SAFE_CAST(asset_id AS STRING)), '') AS asset_id,
        NULLIF(TRIM(SAFE_CAST(asset_name AS STRING)), '') AS asset_name,

        SAFE_CAST(date_yyyymmdd AS STRING) AS date_yyyymmdd,
        SAFE.PARSE_DATE('%Y%m%d', SAFE_CAST(date_yyyymmdd AS STRING)) AS date,
        SAFE_CAST(date AS INT64) AS raw_date_int64,

        SAFE_CAST(executions AS FLOAT64) AS executions,
        SAFE_CAST(mentions_count AS FLOAT64) AS mentions_count,
        SAFE_CAST(share_of_voice AS FLOAT64) AS share_of_voice,
        SAFE_CAST(visibility_score AS FLOAT64) AS visibility_score,

        SAFE_CAST(__insert_date AS INT64) AS insert_date,
        SAFE_CAST(File_Load_datetime AS DATETIME) AS file_load_datetime,
        NULLIF(TRIM(SAFE_CAST(Filename AS STRING)), '') AS filename
      FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_seo_profound_visibility_asset_daily_tmo`
      WHERE SAFE_CAST(File_Load_datetime AS DATETIME)
        >= DATETIME_SUB(CURRENT_DATETIME(), INTERVAL lookback_days DAY)
    ),
    cleaned AS (
      SELECT * FROM src
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
    SELECT * FROM dedup
  ) S
  ON  T.account_id   = S.account_id
  AND T.asset_id     = S.asset_id
  AND T.date_yyyymmdd = S.date_yyyymmdd

  WHEN MATCHED THEN UPDATE SET
    account_name = S.account_name,
    asset_name = S.asset_name,
    date = S.date,
    raw_date_int64 = S.raw_date_int64,
    executions = S.executions,
    mentions_count = S.mentions_count,
    share_of_voice = S.share_of_voice,
    visibility_score = S.visibility_score,
    insert_date = S.insert_date,
    file_load_datetime = S.file_load_datetime,
    filename = S.filename

  WHEN NOT MATCHED THEN INSERT ROW;

END;

