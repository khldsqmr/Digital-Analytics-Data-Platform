/* =================================================================================================
FILE: 01_sdi_tbl_adobeFunnel_bronze_upvFunnelByAllChannel_weekly.sql
LAYER: Bronze Table (via Stored Procedure)
DATASET: prdrzranalytics.lab42
TABLE: sdi_tbl_adobeFunnel_bronze_upvFunnelByAllChannel_weekly
PROCEDURE: sdi_sp_adobeFunnel_bronze_upvFunnelByAllChannel_weekly

RENAMED FROM: vw_sdi_adobe_bronze_uvnbCartstartOrdersByAll_Weekly

SOURCES:
  prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_all_uvnb_postpaid_flow_visitors_weekly_tmo
  prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_all_uvnb_hsi_flow_visitors_weekly_tmo
  prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_all_uvnb_byod_flow_visitors_weekly_tmo
  prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_all_flow_total_visitors_weekly_tmo
  prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_all_postpaid_cartstart_visits_weekly_tmo
  prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_all_hsi_cartstart_visits_weekly_tmo
  prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_all_byod_cartstart_visits_weekly_tmo
  prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_all_postpaid_order_weekly_tmo
  prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_all_hsi_order_weekly_tmo
  prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_all_byod_order_weekly_tmo
  prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_all_postpaid_order_assisted_weekly_tmo
  prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_all_hsi_order_assisted_weekly_tmo
  prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_all_byod_order_assisted_weekly_tmo
  prd_dbi_analytics.improvado.sdi_raw_adobe_pp_ec_all_ec_completed_weekly_tmo
  prd_dbi_analytics.improvado.sdi_raw_adobe_pp_ec_all_ec_successful_visits_weekly_tmo

DESTINATION:
  prdrzranalytics.lab42.sdi_tbl_adobeFunnel_bronze_upvFunnelByAllChannel_weekly

PURPOSE:
  Canonical Bronze weekly Adobe UPV, Cartstart, and Orders source mart at ALL_CHANNELS granularity.
  This view uses Adobe ALL-channel source tables directly and does not calculate ALL from channel rows.

  Orders are split into:
    - Unassisted (digital web unassisted orders)
    - Assisted (digital web assisted orders)

  UpvFlowTotal is sourced directly from the Adobe all-flow total visitors table.
  It is the canonical sum of all LOB flows and is not calculated from UpvPostpaid + UpvHsi + UpvByod.

  EcCompleted and EcSuccessful are Adobe Experience Cloud metrics, added after the UPV
  and Cartstart metrics (before Orders). EcCompleted is sourced from the ec_completed
  page_events tables; EcSuccessful is sourced from the ec_successful_visits visits
  tables. Both are independent of the UPV/Cartstart/Orders funnel metrics above — not derived
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

CREATE OR REPLACE PROCEDURE
prdrzranalytics.lab42.sdi_sp_adobeFunnel_bronze_upvFunnelByAllChannel_weekly()
LANGUAGE SQL
SQL SECURITY INVOKER
MODIFIES SQL DATA
AS
BEGIN

  CREATE OR REPLACE TABLE
  prdrzranalytics.lab42.sdi_tbl_adobeFunnel_bronze_upvFunnelByAllChannel_weekly
  USING DELTA
  AS

WITH RawUnion AS (

  -- UPV Postpaid Flow
  SELECT
    DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6) AS WeekSunSat,
    'ALL_CHANNELS' AS DataGranularity,
    CAST(NULL AS STRING) AS LastTouchChannel,
    CAST(NULL AS STRING) AS LtcGroup,
    'UpvPostpaid' AS MetricName,
    TRY_CAST(visitors AS DOUBLE) AS MetricValue,
    'sdi_raw_adobe_pp_uvnb_all_uvnb_postpaid_flow_visitors_weekly_tmo' AS SourceTable,
    __insert_date AS InsertDate,
    File_Load_datetime AS FileLoadDatetime,
    Filename
  FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_all_uvnb_postpaid_flow_visitors_weekly_tmo

  UNION ALL

  -- UPV HSI Flow
  SELECT
    DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6),
    'ALL_CHANNELS',
    CAST(NULL AS STRING),
    CAST(NULL AS STRING),
    'UpvHsi',
    TRY_CAST(visitors AS DOUBLE),
    'sdi_raw_adobe_pp_uvnb_all_uvnb_hsi_flow_visitors_weekly_tmo',
    __insert_date,
    File_Load_datetime,
    Filename
  FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_all_uvnb_hsi_flow_visitors_weekly_tmo

  UNION ALL

  -- UPV BYOD Flow
  SELECT
    DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6),
    'ALL_CHANNELS',
    CAST(NULL AS STRING),
    CAST(NULL AS STRING),
    'UpvByod',
    TRY_CAST(visitors AS DOUBLE),
    'sdi_raw_adobe_pp_uvnb_all_uvnb_byod_flow_visitors_weekly_tmo',
    __insert_date,
    File_Load_datetime,
    Filename
  FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_all_uvnb_byod_flow_visitors_weekly_tmo

  UNION ALL

  -- UPV Flow Total
  -- Sourced directly from Adobe all-flow total table.
  -- Not calculated from UpvPostpaid + UpvHsi + UpvByod.
  SELECT
    DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6),
    'ALL_CHANNELS',
    CAST(NULL AS STRING),
    CAST(NULL AS STRING),
    'UpvFlowTotal',
    TRY_CAST(visitors AS DOUBLE),
    'sdi_raw_adobe_pp_uvnb_all_flow_total_visitors_weekly_tmo',
    __insert_date,
    File_Load_datetime,
    Filename
  FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_all_flow_total_visitors_weekly_tmo

  UNION ALL

  -- Cartstart Postpaid
  SELECT
    DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6),
    'ALL_CHANNELS',
    CAST(NULL AS STRING),
    CAST(NULL AS STRING),
    'CartstartPostpaid',
    TRY_CAST(visits AS DOUBLE),
    'sdi_raw_adobe_pp_uvnb_all_postpaid_cartstart_visits_weekly_tmo',
    __insert_date,
    File_Load_datetime,
    Filename
  FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_all_postpaid_cartstart_visits_weekly_tmo

  UNION ALL

  -- Cartstart HSI
  SELECT
    DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6),
    'ALL_CHANNELS',
    CAST(NULL AS STRING),
    CAST(NULL AS STRING),
    'CartstartHsi',
    TRY_CAST(visits AS DOUBLE),
    'sdi_raw_adobe_pp_uvnb_all_hsi_cartstart_visits_weekly_tmo',
    __insert_date,
    File_Load_datetime,
    Filename
  FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_all_hsi_cartstart_visits_weekly_tmo

  UNION ALL

  -- Cartstart BYOD
  SELECT
    DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6),
    'ALL_CHANNELS',
    CAST(NULL AS STRING),
    CAST(NULL AS STRING),
    'CartstartByod',
    TRY_CAST(visits AS DOUBLE),
    'sdi_raw_adobe_pp_uvnb_all_byod_cartstart_visits_weekly_tmo',
    __insert_date,
    File_Load_datetime,
    Filename
  FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_all_byod_cartstart_visits_weekly_tmo

  UNION ALL

  -- EC Completed
  SELECT
    DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6),
    'ALL_CHANNELS',
    CAST(NULL AS STRING),
    CAST(NULL AS STRING),
    'EcCompleted',
    TRY_CAST(page_events AS DOUBLE),
    'sdi_raw_adobe_pp_ec_all_ec_completed_weekly_tmo',
    __insert_date,
    File_Load_datetime,
    Filename
  FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_ec_all_ec_completed_weekly_tmo

  UNION ALL

  -- EC Successful Visits
  SELECT
    DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6),
    'ALL_CHANNELS',
    CAST(NULL AS STRING),
    CAST(NULL AS STRING),
    'EcSuccessful',
    TRY_CAST(visits AS DOUBLE),
    'sdi_raw_adobe_pp_ec_all_ec_successful_visits_weekly_tmo',
    __insert_date,
    File_Load_datetime,
    Filename
  FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_ec_all_ec_successful_visits_weekly_tmo

  UNION ALL

  -- Orders Unassisted Postpaid
  SELECT
    DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6),
    'ALL_CHANNELS',
    CAST(NULL AS STRING),
    CAST(NULL AS STRING),
    'OrdersUnassistedPostpaid',
    TRY_CAST(orders AS DOUBLE),
    'sdi_raw_adobe_pp_uvnb_all_postpaid_order_weekly_tmo',
    __insert_date,
    File_Load_datetime,
    Filename
  FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_all_postpaid_order_weekly_tmo

  UNION ALL

  -- Orders Unassisted HSI
  SELECT
    DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6),
    'ALL_CHANNELS',
    CAST(NULL AS STRING),
    CAST(NULL AS STRING),
    'OrdersUnassistedHsi',
    TRY_CAST(orders AS DOUBLE),
    'sdi_raw_adobe_pp_uvnb_all_hsi_order_weekly_tmo',
    __insert_date,
    File_Load_datetime,
    Filename
  FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_all_hsi_order_weekly_tmo

  UNION ALL

  -- Orders Unassisted BYOD
  SELECT
    DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6),
    'ALL_CHANNELS',
    CAST(NULL AS STRING),
    CAST(NULL AS STRING),
    'OrdersUnassistedByod',
    TRY_CAST(orders AS DOUBLE),
    'sdi_raw_adobe_pp_uvnb_all_byod_order_weekly_tmo',
    __insert_date,
    File_Load_datetime,
    Filename
  FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_all_byod_order_weekly_tmo

  UNION ALL

  -- Orders Assisted Postpaid
  SELECT
    DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6),
    'ALL_CHANNELS',
    CAST(NULL AS STRING),
    CAST(NULL AS STRING),
    'OrdersAssistedPostpaid',
    TRY_CAST(orders AS DOUBLE),
    'sdi_raw_adobe_pp_uvnb_all_postpaid_order_assisted_weekly_tmo',
    __insert_date,
    File_Load_datetime,
    Filename
  FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_all_postpaid_order_assisted_weekly_tmo

  UNION ALL

  -- Orders Assisted HSI
  SELECT
    DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6),
    'ALL_CHANNELS',
    CAST(NULL AS STRING),
    CAST(NULL AS STRING),
    'OrdersAssistedHsi',
    TRY_CAST(orders AS DOUBLE),
    'sdi_raw_adobe_pp_uvnb_all_hsi_order_assisted_weekly_tmo',
    __insert_date,
    File_Load_datetime,
    Filename
  FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_all_hsi_order_assisted_weekly_tmo

  UNION ALL

  -- Orders Assisted BYOD
  SELECT
    DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6),
    'ALL_CHANNELS',
    CAST(NULL AS STRING),
    CAST(NULL AS STRING),
    'OrdersAssistedByod',
    TRY_CAST(orders AS DOUBLE),
    'sdi_raw_adobe_pp_uvnb_all_byod_order_assisted_weekly_tmo',
    __insert_date,
    File_Load_datetime,
    Filename
  FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_all_byod_order_assisted_weekly_tmo
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
  MAX(IF(MetricName = 'UpvPostpaid',             MetricValue, NULL)) AS UpvPostpaid,
  MAX(IF(MetricName = 'UpvHsi',                  MetricValue, NULL)) AS UpvHsi,
  MAX(IF(MetricName = 'UpvByod',                 MetricValue, NULL)) AS UpvByod,

  -- UPV Flow Total
  MAX(IF(MetricName = 'UpvFlowTotal',            MetricValue, NULL)) AS UpvFlowTotal,

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

  ARRAY_JOIN(SORT_ARRAY(COLLECT_SET(SourceTable)), ', ') AS SourceTablesUsed,
  MAX(FileLoadDatetime) AS MaxFileLoadDatetime,
  ARRAY_JOIN(SORT_ARRAY(COLLECT_SET(Filename)), ', ') AS FilenamesUsed

FROM Deduped
GROUP BY
  WeekSunSat,
  DataGranularity,
  LastTouchChannel,
  LtcGroup;

END;