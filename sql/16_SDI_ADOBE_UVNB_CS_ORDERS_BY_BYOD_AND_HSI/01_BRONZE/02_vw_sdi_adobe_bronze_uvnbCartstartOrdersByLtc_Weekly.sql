/* =================================================================================================
FILE: 02_vw_sdi_adobe_bronze_uvnbCartstartOrdersByLtc_Weekly.sql
LAYER: Bronze View
DATASET: prj-dbi-prd-1.ds_dbi_digitalmedia_automation
VIEW: vw_sdi_adobe_bronze_uvnbCartstartOrdersByLtc_Weekly

SOURCES:
  prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_ltc_uvnb_postpaid_flow_visitors_weekly_tmo
  prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_ltc_uvnb_hsi_flow_visitors_weekly_tmo
  prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_ltc_uvnb_byod_flow_weekly_tmo
  prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_ltc_postpaid_cartstart_visits_weekly_tmo
  prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_ltc_hsi_cartstart_visits_weekly_tmo
  prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_ltc_byod_cartstart_visits_weekly_tmo
  prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_ltc_postpaid_order_weekly_tmo
  prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_ltc_hsi_order_weekly_tmo
  prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_ltc_byod_order_weekly_tmo

DESTINATION:
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_adobe_bronze_uvnbCartstartOrdersByLtc_Weekly

PURPOSE:
  Canonical Bronze weekly Adobe UVNB, Cartstart, and Orders source mart at LAST_TOUCH_CHANNEL granularity.

BUSINESS GRAIN:
  One row per:
      WeekSunSat
      LastTouchChannel

BUSINESS RULES:
  - WeekSunSat is derived from date_yyyymmdd by adding 6 days because raw date_yyyymmdd is week-starting Sunday.
  - DataGranularity is fixed as LAST_TOUCH_CHANNEL.
  - LastTouchChannel is standardized as UPPER(TRIM(last_touch_channel)).
  - LtcGroup is NULL.
  - Missing metric values remain NULL.
  - MetricName / MetricValue are used internally only and are not exposed in final output.

KEY DEDUPE RULE:
  - Deduplicate each source table at weekly + LastTouchChannel grain using latest:
      File_Load_datetime DESC
      Filename DESC
      __insert_date DESC

NOTE:
  - sdi_raw_adobe_pp_pro_lt_uvnb_weekly_tmo is not used here to avoid duplicate UVNB logic.
  - This view uses the explicit ltc_uvnb_postpaid_flow_visitors table for Uvnb.

================================================================================================= */

CREATE OR REPLACE VIEW
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_adobe_bronze_uvnbCartstartOrdersByLtc_Weekly`
AS

WITH RawUnion AS (

  SELECT
    DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY) AS WeekSunSat,
    'LAST_TOUCH_CHANNEL' AS DataGranularity,
    UPPER(TRIM(last_touch_channel)) AS LastTouchChannel,
    CAST(NULL AS STRING) AS LtcGroup,
    'Uvnb' AS MetricName,
    SAFE_CAST(visitors AS FLOAT64) AS MetricValue,
    'sdi_raw_adobe_pp_uvnb_ltc_uvnb_postpaid_flow_visitors_weekly_tmo' AS SourceTable,
    __insert_date AS InsertDate,
    File_Load_datetime AS FileLoadDatetime,
    Filename
  FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_ltc_uvnb_postpaid_flow_visitors_weekly_tmo`

  UNION ALL
  SELECT
    DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY),
    'LAST_TOUCH_CHANNEL',
    UPPER(TRIM(last_touch_channel)),
    CAST(NULL AS STRING),
    'UvnbHsi',
    SAFE_CAST(visitors AS FLOAT64),
    'sdi_raw_adobe_pp_uvnb_ltc_uvnb_hsi_flow_visitors_weekly_tmo',
    __insert_date,
    File_Load_datetime,
    Filename
  FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_ltc_uvnb_hsi_flow_visitors_weekly_tmo`

  UNION ALL
  SELECT
    DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY),
    'LAST_TOUCH_CHANNEL',
    UPPER(TRIM(last_touch_channel)),
    CAST(NULL AS STRING),
    'UvnbByod',
    SAFE_CAST(visitors AS FLOAT64),
    'sdi_raw_adobe_pp_uvnb_ltc_uvnb_byod_flow_weekly_tmo',
    __insert_date,
    File_Load_datetime,
    Filename
  FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_ltc_uvnb_byod_flow_weekly_tmo`

  UNION ALL
  SELECT
    DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY),
    'LAST_TOUCH_CHANNEL',
    UPPER(TRIM(last_touch_channel)),
    CAST(NULL AS STRING),
    'Cartstart',
    SAFE_CAST(visits AS FLOAT64),
    'sdi_raw_adobe_pp_uvnb_ltc_postpaid_cartstart_visits_weekly_tmo',
    __insert_date,
    File_Load_datetime,
    Filename
  FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_ltc_postpaid_cartstart_visits_weekly_tmo`

  UNION ALL
  SELECT
    DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY),
    'LAST_TOUCH_CHANNEL',
    UPPER(TRIM(last_touch_channel)),
    CAST(NULL AS STRING),
    'CartstartHsi',
    SAFE_CAST(visits AS FLOAT64),
    'sdi_raw_adobe_pp_uvnb_ltc_hsi_cartstart_visits_weekly_tmo',
    __insert_date,
    File_Load_datetime,
    Filename
  FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_ltc_hsi_cartstart_visits_weekly_tmo`

  UNION ALL
  SELECT
    DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY),
    'LAST_TOUCH_CHANNEL',
    UPPER(TRIM(last_touch_channel)),
    CAST(NULL AS STRING),
    'CartstartByod',
    SAFE_CAST(visits AS FLOAT64),
    'sdi_raw_adobe_pp_uvnb_ltc_byod_cartstart_visits_weekly_tmo',
    __insert_date,
    File_Load_datetime,
    Filename
  FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_ltc_byod_cartstart_visits_weekly_tmo`

  UNION ALL
  SELECT
    DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY),
    'LAST_TOUCH_CHANNEL',
    UPPER(TRIM(last_touch_channel)),
    CAST(NULL AS STRING),
    'OrdersAll',
    SAFE_CAST(orders AS FLOAT64),
    'sdi_raw_adobe_pp_uvnb_ltc_postpaid_order_weekly_tmo',
    __insert_date,
    File_Load_datetime,
    Filename
  FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_ltc_postpaid_order_weekly_tmo`

  UNION ALL
  SELECT
    DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY),
    'LAST_TOUCH_CHANNEL',
    UPPER(TRIM(last_touch_channel)),
    CAST(NULL AS STRING),
    'OrdersHsi',
    SAFE_CAST(orders AS FLOAT64),
    'sdi_raw_adobe_pp_uvnb_ltc_hsi_order_weekly_tmo',
    __insert_date,
    File_Load_datetime,
    Filename
  FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_ltc_hsi_order_weekly_tmo`

  UNION ALL
  SELECT
    DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY),
    'LAST_TOUCH_CHANNEL',
    UPPER(TRIM(last_touch_channel)),
    CAST(NULL AS STRING),
    'OrdersByod',
    SAFE_CAST(orders AS FLOAT64),
    'sdi_raw_adobe_pp_uvnb_ltc_byod_order_weekly_tmo',
    __insert_date,
    File_Load_datetime,
    Filename
  FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_ltc_byod_order_weekly_tmo`
),

Deduped AS (
  SELECT *
  FROM RawUnion
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY
      WeekSunSat,
      DataGranularity,
      LastTouchChannel,
      MetricName,
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

  SUM(IF(MetricName = 'Uvnb', MetricValue, NULL)) AS Uvnb,
  SUM(IF(MetricName = 'UvnbHsi', MetricValue, NULL)) AS UvnbHsi,
  SUM(IF(MetricName = 'UvnbByod', MetricValue, NULL)) AS UvnbByod,

  SUM(IF(MetricName = 'Cartstart', MetricValue, NULL)) AS Cartstart,
  SUM(IF(MetricName = 'CartstartHsi', MetricValue, NULL)) AS CartstartHsi,
  SUM(IF(MetricName = 'CartstartByod', MetricValue, NULL)) AS CartstartByod,

  SUM(IF(MetricName = 'OrdersAll', MetricValue, NULL)) AS OrdersAll,
  SUM(IF(MetricName = 'OrdersHsi', MetricValue, NULL)) AS OrdersHsi,
  SUM(IF(MetricName = 'OrdersByod', MetricValue, NULL)) AS OrdersByod,

  STRING_AGG(DISTINCT SourceTable, ', ' ORDER BY SourceTable) AS SourceTablesUsed,
  MAX(FileLoadDatetime) AS MaxFileLoadDatetime,
  STRING_AGG(DISTINCT Filename, ', ' ORDER BY Filename) AS FilenamesUsed

FROM Deduped
GROUP BY
  WeekSunSat,
  DataGranularity,
  LastTouchChannel,
  LtcGroup;