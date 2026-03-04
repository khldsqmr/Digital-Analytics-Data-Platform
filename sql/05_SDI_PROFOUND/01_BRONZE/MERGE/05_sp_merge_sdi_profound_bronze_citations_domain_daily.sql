
/* =================================================================================================
FILE: 05_sp_merge_sdi_profound_bronze_citations_domain_daily.sql
LAYER: Bronze
DATASET: prj-dbi-prd-1.ds_dbi_digitalmedia_automation
PROC:  sp_merge_sdi_profound_bronze_citations_domain_daily
TARGET TABLE: sdi_profound_bronze_citations_domain_daily

PURPOSE:
  Incrementally upsert ProFound Citations Domain Daily into Bronze:
    - filter by File_Load_datetime lookback window
    - parse canonical DATE from date_yyyymmdd
    - keep raw date INT64 from source column 'date'
    - dedupe per grain using latest (file_load_datetime, filename, insert_date)

GRAIN:
  account_id + root_domain + date_yyyymmdd

PARTITION / CLUSTER:
  PARTITION BY date
  CLUSTER BY account_id, root_domain
================================================================================================= */

CREATE OR REPLACE PROCEDURE
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_merge_sdi_profound_bronze_citations_domain_daily`()
OPTIONS(strict_mode=false)
BEGIN
  DECLARE lookback_days INT64 DEFAULT 60;

  MERGE `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_profound_bronze_citations_domain_daily` T
  USING (
    WITH src AS (
      SELECT
        SAFE_CAST(raw.account_id AS STRING) AS account_id,
        NULLIF(TRIM(SAFE_CAST(raw.account_name AS STRING)), '') AS account_name,
        NULLIF(TRIM(SAFE_CAST(raw.root_domain AS STRING)), '') AS root_domain,

        SAFE_CAST(raw.date_yyyymmdd AS STRING) AS date_yyyymmdd,
        SAFE.PARSE_DATE('%Y%m%d', SAFE_CAST(raw.date_yyyymmdd AS STRING)) AS date,
        SAFE_CAST(raw.date AS INT64) AS raw_date_int64,

        SAFE_CAST(raw.count AS FLOAT64) AS count,
        SAFE_CAST(raw.share_of_voice AS FLOAT64) AS share_of_voice,

        SAFE_CAST(raw.__insert_date AS INT64) AS insert_date,
        SAFE_CAST(raw.File_Load_datetime AS DATETIME) AS file_load_datetime,
        NULLIF(TRIM(SAFE_CAST(raw.Filename AS STRING)), '') AS filename
      FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_seo_profound_citations_domain_daily_tmo` raw
      WHERE SAFE_CAST(raw.File_Load_datetime AS DATETIME) IS NOT NULL
        AND SAFE_CAST(raw.File_Load_datetime AS DATETIME)
          >= DATETIME_SUB(CURRENT_DATETIME(), INTERVAL lookback_days DAY)
    ),
    cleaned AS (
      SELECT *
      FROM src
      WHERE date IS NOT NULL
        AND account_id IS NOT NULL
        AND root_domain IS NOT NULL
        AND date_yyyymmdd IS NOT NULL
    ),
    dedup AS (
      SELECT * EXCEPT(rn)
      FROM (
        SELECT
          c.*,
          ROW_NUMBER() OVER (
            PARTITION BY account_id, root_domain, date_yyyymmdd
            ORDER BY file_load_datetime DESC, filename DESC, insert_date DESC
          ) AS rn
        FROM cleaned c
      )
      WHERE rn = 1
    )
    SELECT * FROM dedup
  ) S
  ON  T.account_id    = S.account_id
  AND T.root_domain   = S.root_domain
  AND T.date_yyyymmdd = S.date_yyyymmdd

  WHEN MATCHED THEN UPDATE SET
    account_name = S.account_name,
    date = S.date,
    raw_date_int64 = S.raw_date_int64,
    count = S.count,
    share_of_voice = S.share_of_voice,
    insert_date = S.insert_date,
    file_load_datetime = S.file_load_datetime,
    filename = S.filename

  WHEN NOT MATCHED THEN INSERT ROW;

END;

