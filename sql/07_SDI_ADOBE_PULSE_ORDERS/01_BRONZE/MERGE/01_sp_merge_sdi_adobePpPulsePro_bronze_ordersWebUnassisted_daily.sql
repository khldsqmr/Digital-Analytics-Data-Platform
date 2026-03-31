/* =================================================================================================
FILE: 01_sp_merge_sdi_adobePpPulsePro_bronze_ordersWebUnassisted_daily.sql
LAYER: Bronze
DATASET: prj-dbi-prd-1.ds_dbi_digitalmedia_automation
PROCEDURE: sp_merge_sdi_adobePpPulsePro_bronze_ordersWebUnassisted_daily

SOURCE (RAW):
  prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_pulse_pro_orders_unassisted_daily_tmo

TARGET:
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_adobePpPulsePro_bronze_ordersWebUnassisted_daily

PURPOSE:
  Refresh Bronze Adobe PP Pulse Pro Web Unassisted Orders Daily by replacing affected dates
  from source and retaining the latest row per business grain.

BUSINESS GRAIN:
  account_id + last_touch_channel + segments_id + date_yyyymmdd
================================================================================================= */

CREATE OR REPLACE PROCEDURE
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_merge_sdi_adobePpPulsePro_bronze_ordersWebUnassisted_daily`()
OPTIONS(strict_mode=false)
BEGIN

  BEGIN TRANSACTION;

  DELETE FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_adobePpPulsePro_bronze_ordersWebUnassisted_daily`
  WHERE date_yyyymmdd IN (
    SELECT DISTINCT src.date_yyyymmdd
    FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_pulse_pro_orders_unassisted_daily_tmo` src
    WHERE SAFE.PARSE_DATE('%Y%m%d', src.date_yyyymmdd) IS NOT NULL
  );

  INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_adobePpPulsePro_bronze_ordersWebUnassisted_daily`
  (
    account_id,
    account_name,
    last_touch_channel,
    segments_id,
    date,
    date_yyyymmdd,
    raw_date_int64,
    orders,
    insert_date,
    file_load_datetime,
    filename
  )
  WITH ranked AS (
    SELECT
      src.account_id,
      src.account_name,
      src.last_touch_channel,
      src.segments_id,
      SAFE.PARSE_DATE('%Y%m%d', src.date_yyyymmdd) AS date,
      src.date_yyyymmdd,
      src.date AS raw_date_int64,
      src.orders,
      src.__insert_date AS insert_date,
      src.File_Load_datetime AS file_load_datetime,
      src.Filename AS filename,
      ROW_NUMBER() OVER (
        PARTITION BY
          src.account_id,
          src.last_touch_channel,
          COALESCE(src.segments_id, '__NULL__'),
          src.date_yyyymmdd
        ORDER BY
          src.File_Load_datetime DESC,
          src.__insert_date DESC,
          src.Filename DESC
      ) AS rn
    FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_pulse_pro_orders_unassisted_daily_tmo` src
    WHERE SAFE.PARSE_DATE('%Y%m%d', src.date_yyyymmdd) IS NOT NULL
  )
  SELECT
    account_id,
    account_name,
    last_touch_channel,
    segments_id,
    date,
    date_yyyymmdd,
    raw_date_int64,
    orders,
    insert_date,
    file_load_datetime,
    filename
  FROM ranked
  WHERE rn = 1;

  COMMIT TRANSACTION;

END;