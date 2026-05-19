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

  Orders are split into:
    - Unassisted (digital web unassisted orders) — renamed from original OrdersPostpaid/Hsi/Byod
    - Assisted — NULL PLACEHOLDERS at this grain pending ingestion of LTC-level assisted tables:
        sdi_raw_adobe_pp_uvnb_ltc_postpaid_order_assisted_weekly_tmo  (not yet available)
        sdi_raw_adobe_pp_uvnb_ltc_hsi_order_assisted_weekly_tmo       (not yet available)
        sdi_raw_adobe_pp_uvnb_ltc_byod_order_assisted_weekly_tmo      (not yet available)

  UvnbFlowTotal — NULL PLACEHOLDER at this grain:
    sdi_raw_adobe_pp_uvnb_ltc_flow_total_visitors_weekly_tmo          (not yet available)
    When available, add to RawUnion following same pattern as Bronze 01 and Bronze 03.

BUSINESS GRAIN:
  One row per:
      WeekSunSat
      LastTouchChannel

BUSINESS RULES:
  - WeekSunSat is derived from date_yyyymmdd by adding 6 days because raw date_yyyymmdd is week-starting Sunday.
  - DataGranularity is fixed as LAST_TOUCH_CHANNEL.
  - LastTouchChannel is standardized as UPPER(TRIM(last_touch_channel)).
  - LtcGroup is NULL.
  - Postpaid / HSI / BYOD columns are separate metrics and are not summed together.
  - Missing metric values remain NULL.
  - MetricName / MetricValue are used internally only and are not exposed in final output.

COLUMN CHANGES vs PREVIOUS VERSION:
  - UvnbFlowTotal  ADDED as NULL placeholder

KEY DEDUPE RULE:
  - Deduplicate each source table at weekly + LastTouchChannel grain using latest:
      File_Load_datetime DESC
      Filename DESC
      __insert_date DESC
================================================================================================= */

CREATE OR REPLACE VIEW
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_adobe_bronze_uvnbCartstartOrdersByLtc_Weekly`
AS

WITH RawUnion AS (

  -- UVNB Postpaid Flow
  SELECT
    DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY) AS WeekSunSat,
    'LAST_TOUCH_CHANNEL' AS DataGranularity,
    UPPER(TRIM(last_touch_channel)) AS LastTouchChannel,
    CAST(NULL AS STRING) AS LtcGroup,
    'UvnbPostpaid' AS MetricName,
    SAFE_CAST(visitors AS FLOAT64) AS MetricValue,
    'sdi_raw_adobe_pp_uvnb_ltc_uvnb_postpaid_flow_visitors_weekly_tmo' AS SourceTable,
    __insert_date AS InsertDate,
    File_Load_datetime AS FileLoadDatetime,
    Filename
  FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_ltc_uvnb_postpaid_flow_visitors_weekly_tmo`

  UNION ALL

  -- UVNB HSI Flow
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

  -- UVNB BYOD Flow
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

  -- Cartstart Postpaid
  SELECT
    DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY),
    'LAST_TOUCH_CHANNEL',
    UPPER(TRIM(last_touch_channel)),
    CAST(NULL AS STRING),
    'CartstartPostpaid',
    SAFE_CAST(visits AS FLOAT64),
    'sdi_raw_adobe_pp_uvnb_ltc_postpaid_cartstart_visits_weekly_tmo',
    __insert_date,
    File_Load_datetime,
    Filename
  FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_ltc_postpaid_cartstart_visits_weekly_tmo`

  UNION ALL

  -- Cartstart HSI
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

  -- Cartstart BYOD
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

  -- Orders Unassisted Postpaid
  SELECT
    DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY),
    'LAST_TOUCH_CHANNEL',
    UPPER(TRIM(last_touch_channel)),
    CAST(NULL AS STRING),
    'OrdersUnassistedPostpaid',
    SAFE_CAST(orders AS FLOAT64),
    'sdi_raw_adobe_pp_uvnb_ltc_postpaid_order_weekly_tmo',
    __insert_date,
    File_Load_datetime,
    Filename
  FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_ltc_postpaid_order_weekly_tmo`

  UNION ALL

  -- Orders Unassisted HSI
  SELECT
    DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY),
    'LAST_TOUCH_CHANNEL',
    UPPER(TRIM(last_touch_channel)),
    CAST(NULL AS STRING),
    'OrdersUnassistedHsi',
    SAFE_CAST(orders AS FLOAT64),
    'sdi_raw_adobe_pp_uvnb_ltc_hsi_order_weekly_tmo',
    __insert_date,
    File_Load_datetime,
    Filename
  FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_ltc_hsi_order_weekly_tmo`

  UNION ALL

  -- Orders Unassisted BYOD
  SELECT
    DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY),
    'LAST_TOUCH_CHANNEL',
    UPPER(TRIM(last_touch_channel)),
    CAST(NULL AS STRING),
    'OrdersUnassistedByod',
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

  -- UVNB flows
  MAX(IF(MetricName = 'UvnbPostpaid',             MetricValue, NULL)) AS UvnbPostpaid,
  MAX(IF(MetricName = 'UvnbHsi',                  MetricValue, NULL)) AS UvnbHsi,
  MAX(IF(MetricName = 'UvnbByod',                 MetricValue, NULL)) AS UvnbByod,

  -- UVNB Flow Total — NULL placeholder until LTC flow total table is ingested
  -- TODO: replace when sdi_raw_adobe_pp_uvnb_ltc_flow_total_visitors_weekly_tmo is available
  CAST(NULL AS FLOAT64) AS UvnbFlowTotal,

  -- Cartstart
  MAX(IF(MetricName = 'CartstartPostpaid',         MetricValue, NULL)) AS CartstartPostpaid,
  MAX(IF(MetricName = 'CartstartHsi',              MetricValue, NULL)) AS CartstartHsi,
  MAX(IF(MetricName = 'CartstartByod',             MetricValue, NULL)) AS CartstartByod,

  -- Orders Unassisted
  MAX(IF(MetricName = 'OrdersUnassistedPostpaid',  MetricValue, NULL)) AS OrdersUnassistedPostpaid,
  MAX(IF(MetricName = 'OrdersUnassistedHsi',       MetricValue, NULL)) AS OrdersUnassistedHsi,
  MAX(IF(MetricName = 'OrdersUnassistedByod',      MetricValue, NULL)) AS OrdersUnassistedByod,

  -- Orders Assisted — NULL placeholders until LTC assisted tables are ingested
  -- TODO: replace with real source tables when available:
  --   sdi_raw_adobe_pp_uvnb_ltc_postpaid_order_assisted_weekly_tmo
  --   sdi_raw_adobe_pp_uvnb_ltc_hsi_order_assisted_weekly_tmo
  --   sdi_raw_adobe_pp_uvnb_ltc_byod_order_assisted_weekly_tmo
  CAST(NULL AS FLOAT64) AS OrdersAssistedPostpaid,
  CAST(NULL AS FLOAT64) AS OrdersAssistedHsi,
  CAST(NULL AS FLOAT64) AS OrdersAssistedByod,

  STRING_AGG(DISTINCT SourceTable, ', ' ORDER BY SourceTable) AS SourceTablesUsed,
  MAX(FileLoadDatetime) AS MaxFileLoadDatetime,
  STRING_AGG(DISTINCT Filename, ', ' ORDER BY Filename) AS FilenamesUsed

FROM Deduped
GROUP BY
  WeekSunSat,
  DataGranularity,
  LastTouchChannel,
  LtcGroup;