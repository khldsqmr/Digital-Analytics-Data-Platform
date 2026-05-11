/* =================================================================================================
FILE: 05_vw_sdi_adobe_bronze_uvnbTotalByChannel_Weekly.sql
LAYER: Bronze View
DATASET: prj-dbi-prd-1.ds_dbi_digitalmedia_automation
VIEW: vw_sdi_adobe_bronze_uvnbTotalByChannel_Weekly

SOURCE:
  prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_pro_lt_uvnb_weekly_tmo

DESTINATION:
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_adobe_bronze_uvnbTotalByChannel_Weekly

PURPOSE:
  Canonical Bronze weekly Adobe total UVNB source at LAST_TOUCH_CHANNEL granularity.

BUSINESS GRAIN:
  One row per:
      WeekSunSat
      LastTouchChannel

BUSINESS RULES:
  - WeekSunSat is derived from date_yyyymmdd by adding 6 days because raw date_yyyymmdd is week-starting Sunday.
  - DataGranularity is fixed as LAST_TOUCH_CHANNEL.
  - LastTouchChannel is standardized as UPPER(TRIM(last_touch_channel)).
  - UvnbTotalAdobe is sourced directly from the total UVNB by last-touch-channel raw table.
  - UvnbTotalAdobe is not calculated from UvnbPostpaid + UvnbHsi + UvnbByod.
  - Missing values remain NULL.

KEY DEDUPE RULE:
  - Deduplicate using latest:
      File_Load_datetime DESC
      Filename DESC
      __insert_date DESC

================================================================================================= */

CREATE OR REPLACE VIEW
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_adobe_bronze_uvnbTotalByChannel_Weekly`
AS

WITH RawBase AS (
  SELECT
    DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY) AS WeekSunSat,
    'LAST_TOUCH_CHANNEL' AS DataGranularity,
    UPPER(TRIM(last_touch_channel)) AS LastTouchChannel,
    CAST(NULL AS STRING) AS LtcGroup,
    SAFE_CAST(visitors AS FLOAT64) AS UvnbTotalAdobe,
    'sdi_raw_adobe_pp_pro_lt_uvnb_weekly_tmo' AS SourceTable,
    __insert_date AS InsertDate,
    File_Load_datetime AS FileLoadDatetime,
    Filename
  FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_pro_lt_uvnb_weekly_tmo`
),

Deduped AS (
  SELECT *
  FROM RawBase
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY
      WeekSunSat,
      DataGranularity,
      LastTouchChannel,
      SourceTable
    ORDER BY
      FileLoadDatetime DESC,
      Filename DESC,
      InsertDate DESC
  ) = 1
)

SELECT
  WeekSunSat,
  DataGranularity,
  LastTouchChannel,
  LtcGroup,
  UvnbTotalAdobe,
  SourceTable AS SourceTablesUsed,
  FileLoadDatetime AS MaxFileLoadDatetime,
  Filename AS FilenamesUsed
FROM Deduped;