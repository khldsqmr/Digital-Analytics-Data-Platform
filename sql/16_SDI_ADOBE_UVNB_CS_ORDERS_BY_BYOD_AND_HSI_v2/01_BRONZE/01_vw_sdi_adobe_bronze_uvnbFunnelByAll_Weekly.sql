/* =================================================================================================
FILE: 01_vw_sdi_adobe_bronze_uvnbFunnelByAll_Weekly.sql
LAYER: Bronze View
DATASET: prj-dbi-prd-1.ds_dbi_digitalmedia_automation
VIEW: vw_sdi_adobe_bronze_uvnbFunnelByAll_Weekly

RENAMED FROM: vw_sdi_adobe_bronze_uvnbCartstartOrdersByAll_Weekly

SOURCES:
  prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_all_uvnb_postpaid_flow_visitors_weekly_tmo
  prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_all_uvnb_hsi_flow_visitors_weekly_tmo
  prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_all_uvnb_byod_flow_visitors_weekly_tmo
  prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_all_flow_total_visitors_weekly_tmo
  prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_all_postpaid_cartstart_visits_weekly_tmo
  prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_all_hsi_cartstart_visits_weekly_tmo
  prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_all_byod_cartstart_visits_weekly_tmo
  prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_ec_all_ec_completed_weekly_tmo
  prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_ec_all_ec_successful_visits_weekly_tmo
  prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_all_postpaid_order_weekly_tmo
  prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_all_hsi_order_weekly_tmo
  prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_all_byod_order_weekly_tmo
  prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_all_postpaid_order_assisted_weekly_tmo
  prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_all_hsi_order_assisted_weekly_tmo
  prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_all_byod_order_assisted_weekly_tmo

DESTINATION:
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_adobe_bronze_uvnbFunnelByAll_Weekly

PURPOSE:
  Canonical Bronze weekly Adobe UVNB, Cartstart, EC, and Orders source mart at ALL_CHANNELS granularity.
  This view uses Adobe ALL-channel source tables directly and does not calculate ALL from channel rows.

  Orders are split into:
    - Unassisted (digital web unassisted orders)
    - Assisted (digital web assisted orders)

  UvnbFlowTotal is sourced directly from the Adobe all-flow total visitors table.
  It is the canonical sum of all LOB flows and is not calculated from UvnbPostpaid + UvnbHsi + UvnbByod.

  EcCompleted and EcSuccessful are Adobe Experience Cloud metrics, added after the UVNB
  and Cartstart metrics (before Orders). EcCompleted is sourced from the ec_completed
  page_events tables; EcSuccessful is sourced from the ec_successful_visits visits
  tables. Both are independent of the UVNB/Cartstart/Orders funnel metrics above — not derived
  from them.

BUSINESS GRAIN:
  One row per:
      WeekSunSat

BUSINESS RULES:
  - WeekSunSat is derived from date_yyyymmdd by adding 6 days because raw date_yyyymmdd is week-starting Sunday.
  - DataGranularity is fixed as ALL_CHANNELS.
  - LastTouchChannel is NULL.
  - LtcGroup is NULL.
  - Postpaid / HSI / BYOD columns are separate metrics and are not summed together.
  - Missing metric values remain NULL.
  - MetricName / MetricValue are used internally only and are not exposed in final output.

KEY DEDUPE RULE:
  - Deduplicate each source table at weekly grain using latest:
      File_Load_datetime DESC
      Filename DESC
      __insert_date DESC
================================================================================================= */

CREATE OR REPLACE VIEW
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_adobe_bronze_uvnbFunnelByAll_Weekly`
AS

WITH RawUnion AS (

  -- UPV Postpaid Flow
  SELECT
    DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY) AS WeekSunSat,
    'ALL_CHANNELS' AS DataGranularity,
    CAST(NULL AS STRING) AS LastTouchChannel,
    CAST(NULL AS STRING) AS LtcGroup,
    'UvnbPostpaid' AS MetricName,
    SAFE_CAST(visitors AS FLOAT64) AS MetricValue,
    'sdi_raw_adobe_pp_uvnb_all_uvnb_postpaid_flow_visitors_weekly_tmo' AS SourceTable,
    __insert_date AS InsertDate,
    File_Load_datetime AS FileLoadDatetime,
    Filename
  FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_all_uvnb_postpaid_flow_visitors_weekly_tmo`

  UNION ALL

  -- UPV HSI Flow
  SELECT
    DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY),
    'ALL_CHANNELS',
    CAST(NULL AS STRING),
    CAST(NULL AS STRING),
    'UvnbHsi',
    SAFE_CAST(visitors AS FLOAT64),
    'sdi_raw_adobe_pp_uvnb_all_uvnb_hsi_flow_visitors_weekly_tmo',
    __insert_date,
    File_Load_datetime,
    Filename
  FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_all_uvnb_hsi_flow_visitors_weekly_tmo`

  UNION ALL

  -- UPV BYOD Flow
  SELECT
    DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY),
    'ALL_CHANNELS',
    CAST(NULL AS STRING),
    CAST(NULL AS STRING),
    'UvnbByod',
    SAFE_CAST(visitors AS FLOAT64),
    'sdi_raw_adobe_pp_uvnb_all_uvnb_byod_flow_visitors_weekly_tmo',
    __insert_date,
    File_Load_datetime,
    Filename
  FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_all_uvnb_byod_flow_visitors_weekly_tmo`

  UNION ALL

  -- UPV Flow Total
  -- Sourced directly from Adobe all-flow total table.
  -- Not calculated from UvnbPostpaid + UvnbHsi + UvnbByod.
  SELECT
    DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY),
    'ALL_CHANNELS',
    CAST(NULL AS STRING),
    CAST(NULL AS STRING),
    'UvnbFlowTotal',
    SAFE_CAST(visitors AS FLOAT64),
    'sdi_raw_adobe_pp_uvnb_all_flow_total_visitors_weekly_tmo',
    __insert_date,
    File_Load_datetime,
    Filename
  FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_all_flow_total_visitors_weekly_tmo`

  UNION ALL

  -- Cartstart Postpaid
  SELECT
    DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY),
    'ALL_CHANNELS',
    CAST(NULL AS STRING),
    CAST(NULL AS STRING),
    'CartstartPostpaid',
    SAFE_CAST(visits AS FLOAT64),
    'sdi_raw_adobe_pp_uvnb_all_postpaid_cartstart_visits_weekly_tmo',
    __insert_date,
    File_Load_datetime,
    Filename
  FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_all_postpaid_cartstart_visits_weekly_tmo`

  UNION ALL

  -- Cartstart HSI
  SELECT
    DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY),
    'ALL_CHANNELS',
    CAST(NULL AS STRING),
    CAST(NULL AS STRING),
    'CartstartHsi',
    SAFE_CAST(visits AS FLOAT64),
    'sdi_raw_adobe_pp_uvnb_all_hsi_cartstart_visits_weekly_tmo',
    __insert_date,
    File_Load_datetime,
    Filename
  FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_all_hsi_cartstart_visits_weekly_tmo`

  UNION ALL

  -- Cartstart BYOD
  SELECT
    DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY),
    'ALL_CHANNELS',
    CAST(NULL AS STRING),
    CAST(NULL AS STRING),
    'CartstartByod',
    SAFE_CAST(visits AS FLOAT64),
    'sdi_raw_adobe_pp_uvnb_all_byod_cartstart_visits_weekly_tmo',
    __insert_date,
    File_Load_datetime,
    Filename
  FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_all_byod_cartstart_visits_weekly_tmo`

  UNION ALL

  -- EC Completed
  SELECT
    DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY),
    'ALL_CHANNELS',
    CAST(NULL AS STRING),
    CAST(NULL AS STRING),
    'EcCompleted',
    SAFE_CAST(page_events AS FLOAT64),
    'sdi_raw_adobe_pp_ec_all_ec_completed_weekly_tmo',
    __insert_date,
    File_Load_datetime,
    Filename
  FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_ec_all_ec_completed_weekly_tmo`

  UNION ALL

  -- EC Successful Visits
  SELECT
    DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY),
    'ALL_CHANNELS',
    CAST(NULL AS STRING),
    CAST(NULL AS STRING),
    'EcSuccessful',
    SAFE_CAST(visits AS FLOAT64),
    'sdi_raw_adobe_pp_ec_all_ec_successful_visits_weekly_tmo',
    __insert_date,
    File_Load_datetime,
    Filename
  FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_ec_all_ec_successful_visits_weekly_tmo`

  UNION ALL

  -- Orders Unassisted Postpaid
  SELECT
    DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY),
    'ALL_CHANNELS',
    CAST(NULL AS STRING),
    CAST(NULL AS STRING),
    'OrdersUnassistedPostpaid',
    SAFE_CAST(orders AS FLOAT64),
    'sdi_raw_adobe_pp_uvnb_all_postpaid_order_weekly_tmo',
    __insert_date,
    File_Load_datetime,
    Filename
  FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_all_postpaid_order_weekly_tmo`

  UNION ALL

  -- Orders Unassisted HSI
  SELECT
    DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY),
    'ALL_CHANNELS',
    CAST(NULL AS STRING),
    CAST(NULL AS STRING),
    'OrdersUnassistedHsi',
    SAFE_CAST(orders AS FLOAT64),
    'sdi_raw_adobe_pp_uvnb_all_hsi_order_weekly_tmo',
    __insert_date,
    File_Load_datetime,
    Filename
  FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_all_hsi_order_weekly_tmo`

  UNION ALL

  -- Orders Unassisted BYOD
  SELECT
    DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY),
    'ALL_CHANNELS',
    CAST(NULL AS STRING),
    CAST(NULL AS STRING),
    'OrdersUnassistedByod',
    SAFE_CAST(orders AS FLOAT64),
    'sdi_raw_adobe_pp_uvnb_all_byod_order_weekly_tmo',
    __insert_date,
    File_Load_datetime,
    Filename
  FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_all_byod_order_weekly_tmo`

  UNION ALL

  -- Orders Assisted Postpaid
  SELECT
    DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY),
    'ALL_CHANNELS',
    CAST(NULL AS STRING),
    CAST(NULL AS STRING),
    'OrdersAssistedPostpaid',
    SAFE_CAST(orders AS FLOAT64),
    'sdi_raw_adobe_pp_uvnb_all_postpaid_order_assisted_weekly_tmo',
    __insert_date,
    File_Load_datetime,
    Filename
  FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_all_postpaid_order_assisted_weekly_tmo`

  UNION ALL

  -- Orders Assisted HSI
  SELECT
    DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY),
    'ALL_CHANNELS',
    CAST(NULL AS STRING),
    CAST(NULL AS STRING),
    'OrdersAssistedHsi',
    SAFE_CAST(orders AS FLOAT64),
    'sdi_raw_adobe_pp_uvnb_all_hsi_order_assisted_weekly_tmo',
    __insert_date,
    File_Load_datetime,
    Filename
  FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_all_hsi_order_assisted_weekly_tmo`

  UNION ALL

  -- Orders Assisted BYOD
  SELECT
    DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY),
    'ALL_CHANNELS',
    CAST(NULL AS STRING),
    CAST(NULL AS STRING),
    'OrdersAssistedByod',
    SAFE_CAST(orders AS FLOAT64),
    'sdi_raw_adobe_pp_uvnb_all_byod_order_assisted_weekly_tmo',
    __insert_date,
    File_Load_datetime,
    Filename
  FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_all_byod_order_assisted_weekly_tmo`
),

Deduped AS (
  SELECT *
  FROM RawUnion
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY
      WeekSunSat,
      DataGranularity,
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

  -- UPV flows
  MAX(IF(MetricName = 'UvnbPostpaid',             MetricValue, NULL)) AS UvnbPostpaid,
  MAX(IF(MetricName = 'UvnbHsi',                  MetricValue, NULL)) AS UvnbHsi,
  MAX(IF(MetricName = 'UvnbByod',                 MetricValue, NULL)) AS UvnbByod,

  -- UPV Flow Total
  MAX(IF(MetricName = 'UvnbFlowTotal',            MetricValue, NULL)) AS UvnbFlowTotal,

  -- Cartstart
  MAX(IF(MetricName = 'CartstartPostpaid',         MetricValue, NULL)) AS CartstartPostpaid,
  MAX(IF(MetricName = 'CartstartHsi',              MetricValue, NULL)) AS CartstartHsi,
  MAX(IF(MetricName = 'CartstartByod',             MetricValue, NULL)) AS CartstartByod,

  -- Adobe Experience Cloud (EC) — added after UPV and Cartstart metrics
  MAX(IF(MetricName = 'EcCompleted',             MetricValue, NULL)) AS EcCompleted,
  MAX(IF(MetricName = 'EcSuccessful',      MetricValue, NULL)) AS EcSuccessful,

  -- Orders Unassisted
  MAX(IF(MetricName = 'OrdersUnassistedPostpaid',  MetricValue, NULL)) AS OrdersUnassistedPostpaid,
  MAX(IF(MetricName = 'OrdersUnassistedHsi',       MetricValue, NULL)) AS OrdersUnassistedHsi,
  MAX(IF(MetricName = 'OrdersUnassistedByod',      MetricValue, NULL)) AS OrdersUnassistedByod,

  -- Orders Assisted
  MAX(IF(MetricName = 'OrdersAssistedPostpaid',    MetricValue, NULL)) AS OrdersAssistedPostpaid,
  MAX(IF(MetricName = 'OrdersAssistedHsi',         MetricValue, NULL)) AS OrdersAssistedHsi,
  MAX(IF(MetricName = 'OrdersAssistedByod',        MetricValue, NULL)) AS OrdersAssistedByod,

  STRING_AGG(DISTINCT SourceTable, ', ' ORDER BY SourceTable) AS SourceTablesUsed,
  MAX(FileLoadDatetime) AS MaxFileLoadDatetime,
  STRING_AGG(DISTINCT Filename, ', ' ORDER BY Filename) AS FilenamesUsed

FROM Deduped
GROUP BY
  WeekSunSat,
  DataGranularity,
  LastTouchChannel,
  LtcGroup;