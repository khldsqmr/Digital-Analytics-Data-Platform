
/* =================================================================================================
FILE: 03_backfill_sdi_profound_bronze_visibility_topic_daily.sql
LAYER: Bronze (One-time / On-demand Backfill)
DATASET: prj-dbi-prd-1.ds_dbi_digitalmedia_automation
TARGET TABLE: sdi_profound_bronze_visibility_topic_daily
SOURCE (RAW):
  prj-dbi-prd-1.ds_dbi_improvado_master.sdi_seo_profound_visibility_topic_daily_tmo

PURPOSE:
  Backfill historical ProFound Visibility Topic Daily into Bronze using the SAME logic as incremental,
  processed in safe chunks.

GRAIN:
  account_id + asset_name + topic + date_yyyymmdd

PARTITION / CLUSTER:
  PARTITION BY date
  CLUSTER BY account_id, topic
================================================================================================= */

DECLARE backfill_start_date DATE DEFAULT DATE('2024-01-01');  -- <-- change
DECLARE backfill_end_date   DATE DEFAULT CURRENT_DATE();      -- <-- change
DECLARE chunk_days          INT64 DEFAULT 14;

DECLARE chunk_start DATE;
DECLARE chunk_end   DATE;

SET chunk_start = backfill_start_date;

LOOP
  IF chunk_start > backfill_end_date THEN LEAVE; END IF;

  SET chunk_end = LEAST(DATE_ADD(chunk_start, INTERVAL chunk_days - 1 DAY), backfill_end_date);

  MERGE `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_profound_bronze_visibility_topic_daily` T
  USING (
    WITH src AS (
      SELECT
        SAFE_CAST(raw.account_id AS STRING) AS account_id,
        NULLIF(TRIM(SAFE_CAST(raw.account_name AS STRING)), '') AS account_name,
        NULLIF(TRIM(SAFE_CAST(raw.asset_name AS STRING)), '') AS asset_name,
        NULLIF(TRIM(SAFE_CAST(raw.topic AS STRING)), '') AS topic,

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
      FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_seo_profound_visibility_topic_daily_tmo` raw
      WHERE SAFE.PARSE_DATE('%Y%m%d', SAFE_CAST(raw.date_yyyymmdd AS STRING))
        BETWEEN chunk_start AND chunk_end
    ),
    cleaned AS (
      SELECT *
      FROM src
      WHERE date IS NOT NULL
        AND account_id IS NOT NULL
        AND asset_name IS NOT NULL
        AND topic IS NOT NULL
        AND date_yyyymmdd IS NOT NULL
    ),
    dedup AS (
      SELECT * EXCEPT(rn)
      FROM (
        SELECT
          c.*,
          ROW_NUMBER() OVER (
            PARTITION BY account_id, asset_name, topic, date_yyyymmdd
            ORDER BY file_load_datetime DESC, filename DESC, insert_date DESC
          ) AS rn
        FROM cleaned c
      )
      WHERE rn = 1
    )
    SELECT * FROM dedup
  ) S
  ON  T.account_id    = S.account_id
  AND T.asset_name    = S.asset_name
  AND T.topic         = S.topic
  AND T.date_yyyymmdd = S.date_yyyymmdd

  WHEN MATCHED THEN UPDATE SET
    account_name = S.account_name,
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

  SET chunk_start = DATE_ADD(chunk_end, INTERVAL 1 DAY);
END LOOP;

