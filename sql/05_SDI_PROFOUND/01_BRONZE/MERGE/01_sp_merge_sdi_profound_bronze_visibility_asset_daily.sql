/* =================================================================================================
FILE: 01_sp_merge_sdi_profound_bronze_visibility_asset_daily.sql
LAYER: Bronze
DATASET: prj-dbi-prd-1.ds_dbi_digitalmedia_automation
PROC:  sp_merge_sdi_profound_bronze_visibility_asset_daily
TARGET TABLE: sdi_profound_bronze_visibility_asset_daily

PURPOSE:
  Daily incremental upsert of ProFound Visibility Asset Daily into Bronze.

WHAT THIS PROCEDURE DOES:
  1. Reads raw rows loaded within the configured File_Load_datetime lookback window.
  2. Standardizes business keys and lineage fields.
  3. Parses canonical DATE from date_yyyymmdd.
  4. Filters out invalid rows with NULL business keys or invalid dates.
  5. Dedupes raw rows to the declared Bronze grain using the latest delivered record.
  6. MERGEs deduped rows into Bronze:
       - UPDATE matched rows
       - INSERT new rows

WHY THIS PROCEDURE EXISTS:
  This is the daily incremental Bronze loader.
  It is designed for operational scheduling and late-arriving files.
  Historical full-scope correction should be handled separately by reconcile/backfill procedures.

SOURCE TABLE:
  prj-dbi-prd-1.ds_dbi_improvado_master.sdi_seo_profound_visibility_asset_daily_tmo

TARGET TABLE:
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_profound_bronze_visibility_asset_daily

DECLARED BUSINESS GRAIN:
  account_id + asset_id + date_yyyymmdd

DEDUP WINNER RULE:
  Latest row by:
    file_load_datetime DESC,
    filename DESC,
    insert_date DESC

SCHEDULING:
  Intended to run daily after raw landing is complete.
================================================================================================= */

CREATE OR REPLACE PROCEDURE
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_merge_sdi_profound_bronze_visibility_asset_daily`()
OPTIONS(strict_mode=false)
BEGIN

  -- -----------------------------------------------------------------------------------------------
  -- Configurable incremental lookback window.
  -- This should be large enough to catch late-arriving source files.
  -- -----------------------------------------------------------------------------------------------
  DECLARE lookback_days INT64 DEFAULT 60;

  MERGE `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_profound_bronze_visibility_asset_daily` AS T
  USING (
    WITH src AS (
      -- -------------------------------------------------------------------------------------------
      -- Step 1: Read raw rows in the incremental source window and standardize data types.
      -- -------------------------------------------------------------------------------------------
      SELECT
        NULLIF(TRIM(SAFE_CAST(raw.account_id AS STRING)), '') AS account_id,
        NULLIF(TRIM(SAFE_CAST(raw.account_name AS STRING)), '') AS account_name,
        NULLIF(TRIM(SAFE_CAST(raw.asset_id AS STRING)), '') AS asset_id,
        NULLIF(TRIM(SAFE_CAST(raw.asset_name AS STRING)), '') AS asset_name,

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
      FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_seo_profound_visibility_asset_daily_tmo` AS raw
      WHERE SAFE_CAST(raw.File_Load_datetime AS DATETIME) IS NOT NULL
        AND SAFE_CAST(raw.File_Load_datetime AS DATETIME)
            >= DATETIME_SUB(CURRENT_DATETIME(), INTERVAL lookback_days DAY)
    ),

    cleaned AS (
      -- -------------------------------------------------------------------------------------------
      -- Step 2: Keep only rows with valid business keys and valid canonical date.
      -- -------------------------------------------------------------------------------------------
      SELECT *
      FROM src
      WHERE account_id IS NOT NULL
        AND asset_id IS NOT NULL
        AND date_yyyymmdd IS NOT NULL
        AND date IS NOT NULL
    ),

    dedup AS (
      -- -------------------------------------------------------------------------------------------
      -- Step 3: Deduplicate raw rows to the declared Bronze grain.
      -- The latest delivered row wins within the incremental window.
      -- -------------------------------------------------------------------------------------------
      SELECT * EXCEPT (rn)
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

    -- ---------------------------------------------------------------------------------------------
    -- Step 4: Final source dataset for MERGE.
    -- ---------------------------------------------------------------------------------------------
    SELECT
      account_id,
      account_name,
      asset_id,
      asset_name,
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
  AND T.asset_id      = S.asset_id
  AND T.date_yyyymmdd = S.date_yyyymmdd

  WHEN MATCHED THEN
    UPDATE SET
      account_name       = S.account_name,
      asset_name         = S.asset_name,
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
      asset_id,
      asset_name,
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
      S.asset_id,
      S.asset_name,
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