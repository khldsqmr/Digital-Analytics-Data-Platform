/* =================================================================================================
FILE: 01_merge_sdi_profound_bronze_visibility_asset_weekly.sql
LAYER: Bronze
DATASET: prj-dbi-prd-1.ds_dbi_digitalmedia_automation
TARGET TABLE: sdi_profound_bronze_visibility_asset_weekly

SOURCE (RAW):
  prj-dbi-prd-1.ds_dbi_improvado_master.sdi_seo_profound_vis_asset_weekly_tmo

PURPOSE:
  Refresh Bronze ProFound Visibility Asset Weekly using date-scoped delete + insert.

WHY THIS APPROACH:
  This is safer than update/insert-only MERGE for snapshot-style raw files because it correctly
  handles rows that disappear from raw on refreshed dates.

REFRESH LOGIC:
  1) Identify affected date_yyyymmdd values from raw
  2) Delete target rows for only those dates
  3) Insert latest deduped rows for those dates

BUSINESS GRAIN:
  account_id + asset_id + asset_name + date_yyyymmdd
================================================================================================= */

BEGIN TRANSACTION;

DELETE FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_profound_bronze_visibility_asset_weekly`
WHERE date_yyyymmdd IN (
  SELECT DISTINCT date_yyyymmdd
  FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_seo_profound_vis_asset_weekly_tmo`
  WHERE SAFE.PARSE_DATE('%Y%m%d', date_yyyymmdd) IS NOT NULL
);

INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_profound_bronze_visibility_asset_weekly`
(
  account_id,
  account_name,
  asset_id,
  asset_name,
  date,
  date_yyyymmdd,
  raw_date_int64,
  executions,
  mentions_count,
  share_of_voice,
  visibility_score,
  insert_date,
  file_load_datetime,
  filename
)
WITH ranked AS (
  SELECT
    src.account_id,
    src.account_name,
    src.asset_id,
    src.asset_name,
    PARSE_DATE('%Y%m%d', src.date_yyyymmdd) AS date,
    src.date_yyyymmdd,
    src.date AS raw_date_int64,
    src.executions,
    src.mentions_count,
    src.share_of_voice,
    src.visibility_score,
    src.__insert_date AS insert_date,
    src.File_Load_datetime AS file_load_datetime,
    src.Filename AS filename,
    ROW_NUMBER() OVER (
      PARTITION BY
        src.account_id,
        src.asset_id,
        src.asset_name,
        src.date_yyyymmdd
      ORDER BY
        src.File_Load_datetime DESC,
        src.__insert_date DESC,
        src.Filename DESC
    ) AS rn
  FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_seo_profound_vis_asset_weekly_tmo` src
  WHERE SAFE.PARSE_DATE('%Y%m%d', src.date_yyyymmdd) IS NOT NULL
)
SELECT
  account_id,
  account_name,
  asset_id,
  asset_name,
  date,
  date_yyyymmdd,
  raw_date_int64,
  executions,
  mentions_count,
  share_of_voice,
  visibility_score,
  insert_date,
  file_load_datetime,
  filename
FROM ranked
WHERE rn = 1;

COMMIT TRANSACTION;