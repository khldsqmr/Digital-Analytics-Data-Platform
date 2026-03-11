/* =================================================================================================
FILE: 07_sp_merge_sdi_profound_bronze_visibility_asset_monthly.sql
LAYER: Bronze
DATASET: prj-dbi-prd-1.ds_dbi_digitalmedia_automation
PROCEDURE: sp_merge_sdi_profound_bronze_visibility_asset_monthly
================================================================================================= */

CREATE OR REPLACE PROCEDURE
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_merge_sdi_profound_bronze_visibility_asset_monthly`()
OPTIONS(strict_mode=false)
BEGIN

  BEGIN TRANSACTION;

  DELETE FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_profound_bronze_visibility_asset_monthly`
  WHERE date_yyyymmdd IN (
    SELECT DISTINCT date_yyyymmdd
    FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_seo_profound_vis_asset_monthly_tmo`
    WHERE SAFE.PARSE_DATE('%Y%m%d', date_yyyymmdd) IS NOT NULL
  );

  INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_profound_bronze_visibility_asset_monthly`
  (
    account_id, account_name, asset_id, asset_name,
    date, date_yyyymmdd, raw_date_int64,
    executions, mentions_count, share_of_voice, visibility_score,
    insert_date, file_load_datetime, filename
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
        PARTITION BY src.account_id, src.asset_id, src.asset_name, src.date_yyyymmdd
        ORDER BY src.File_Load_datetime DESC, src.__insert_date DESC, src.Filename DESC
      ) AS rn
    FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_seo_profound_vis_asset_monthly_tmo` src
    WHERE SAFE.PARSE_DATE('%Y%m%d', src.date_yyyymmdd) IS NOT NULL
  )
  SELECT
    account_id, account_name, asset_id, asset_name,
    date, date_yyyymmdd, raw_date_int64,
    executions, mentions_count, share_of_voice, visibility_score,
    insert_date, file_load_datetime, filename
  FROM ranked
  WHERE rn = 1;

  COMMIT TRANSACTION;

END;