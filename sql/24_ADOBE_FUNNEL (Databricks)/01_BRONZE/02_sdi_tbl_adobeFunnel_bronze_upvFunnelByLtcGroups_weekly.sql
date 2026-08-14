/* =================================================================================================
FILE: 02_sdi_tbl_adobeFunnel_bronze_upvFunnelByLtcGroups_weekly.sql
LAYER: Bronze Table (via Stored Procedure)
DATASET: prdrzranalytics.lab42
TABLE: sdi_tbl_adobeFunnel_bronze_upvFunnelByLtcGroups_weekly
PROCEDURE: sdi_sp_adobeFunnel_bronze_upvFunnelByLtcGroups_weekly

RENAMED FROM: vw_sdi_adobe_bronze_uvnbCartstartOrdersByLtcGroups_Weekly

SOURCES:
  Adobe weekly UPV, UPV Flow Total, Cartstart, Unassisted Orders, and Assisted Orders raw tables
  for these LTC groups: DIRECT, ORGANIC SEARCH, OTHER, PAID SEARCH, PROGRAMMATIC, SOCIAL

  UPV Flow Total sources:
    sdi_raw_adobe_pp_uvnb_{group}_flow_total_visitors_weekly_tmo
  where {group} = direct, organic_search, other, paid_search, programmatic, social

DESTINATION:
  prdrzranalytics.lab42.sdi_tbl_adobeFunnel_bronze_upvFunnelByLtcGroups_weekly

PURPOSE:
  Canonical Bronze weekly Adobe UPV, Cartstart, and Orders source mart at LTC_GROUPS granularity.

  UpvFlowTotal is sourced directly from the Adobe group-level flow total visitors tables.
  It is not calculated from UpvPostpaid + UpvHsi + UpvByod.

BUSINESS GRAIN:
  One row per:
      WeekSunSat
      LtcGroup

KEY DEDUPE RULE:
  - Deduplicate each source table at weekly + LtcGroup grain using latest:
      File_Load_datetime DESC
      Filename DESC
      __insert_date DESC
================================================================================================= */

CREATE OR REPLACE PROCEDURE
`prdrzranalytics.lab42.sdi_sp_adobeFunnel_bronze_upvFunnelByLtcGroups_weekly`()
LANGUAGE SQL
MODIFIES SQL DATA
AS
BEGIN

  CREATE OR REPLACE TABLE
  `prdrzranalytics.lab42.sdi_tbl_adobeFunnel_bronze_upvFunnelByLtcGroups_weekly`
  AS

WITH RawUnion AS (

  /* ------------------------------------------------------------------ DIRECT ------------------------------------------------------------------ */
  SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6) AS WeekSunSat, 'LTC_GROUPS' AS DataGranularity, CAST(NULL AS STRING) AS LastTouchChannel, 'DIRECT' AS LtcGroup, 'UpvPostpaid' AS MetricName, TRY_CAST(visitors AS DOUBLE) AS MetricValue, 'sdi_raw_adobe_pp_uvnb_direct_uvnb_postpaid_flow_visitors_weekly_tmo' AS SourceTable, __insert_date AS InsertDate, File_Load_datetime AS FileLoadDatetime, Filename FROM `prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_direct_uvnb_postpaid_flow_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'DIRECT', 'UpvHsi', TRY_CAST(visitors AS DOUBLE), 'sdi_raw_adobe_pp_uvnb_direct_uvnb_hsi_flow_visitors_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_direct_uvnb_hsi_flow_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'DIRECT', 'UpvByod', TRY_CAST(visitors AS DOUBLE), 'sdi_raw_adobe_pp_uvnb_direct_uvnb_byod_flow_visitors_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_direct_uvnb_byod_flow_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'DIRECT', 'UpvFlowTotal', TRY_CAST(visitors AS DOUBLE), 'sdi_raw_adobe_pp_uvnb_direct_flow_total_visitors_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_direct_flow_total_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'DIRECT', 'CartstartPostpaid', TRY_CAST(visits AS DOUBLE), 'sdi_raw_adobe_pp_uvnb_direct_postpaid_cartstart_visits_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_direct_postpaid_cartstart_visits_weekly_tmo`
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'DIRECT', 'CartstartHsi', TRY_CAST(visits AS DOUBLE), 'sdi_raw_adobe_pp_uvnb_direct_hsi_cartstart_visits_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_direct_hsi_cartstart_visits_weekly_tmo`
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'DIRECT', 'CartstartByod', TRY_CAST(visits AS DOUBLE), 'sdi_raw_adobe_pp_uvnb_direct_byod_cartstart_visits_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_direct_byod_cartstart_visits_weekly_tmo`
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'DIRECT', 'OrdersUnassistedPostpaid', TRY_CAST(orders AS DOUBLE), 'sdi_raw_adobe_pp_uvnb_direct_postpaid_order_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_direct_postpaid_order_weekly_tmo`
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'DIRECT', 'OrdersUnassistedHsi', TRY_CAST(orders AS DOUBLE), 'sdi_raw_adobe_pp_uvnb_direct_hsi_order_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_direct_hsi_order_weekly_tmo`
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'DIRECT', 'OrdersUnassistedByod', TRY_CAST(orders AS DOUBLE), 'sdi_raw_adobe_pp_uvnb_direct_byod_order_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_direct_byod_order_weekly_tmo`
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'DIRECT', 'OrdersAssistedPostpaid', TRY_CAST(orders AS DOUBLE), 'sdi_raw_adobe_pp_uvnb_direct_postpaid_order_assisted_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_direct_postpaid_order_assisted_weekly_tmo`
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'DIRECT', 'OrdersAssistedHsi', TRY_CAST(orders AS DOUBLE), 'sdi_raw_adobe_pp_uvnb_direct_hsi_order_assisted_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_direct_hsi_order_assisted_weekly_tmo`
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'DIRECT', 'OrdersAssistedByod', TRY_CAST(orders AS DOUBLE), 'sdi_raw_adobe_pp_uvnb_direct_byod_order_assisted_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_direct_byod_order_assisted_weekly_tmo`

  /* ------------------------------------------------------------------ ORGANIC SEARCH ------------------------------------------------------------------ */
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'ORGANIC SEARCH', 'UpvPostpaid', TRY_CAST(visitors AS DOUBLE), 'sdi_raw_adobe_pp_uvnb_organic_search_uvnb_postpaid_flow_visitors_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_organic_search_uvnb_postpaid_flow_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'ORGANIC SEARCH', 'UpvHsi', TRY_CAST(visitors AS DOUBLE), 'sdi_raw_adobe_pp_uvnb_organic_search_uvnb_hsi_flow_visitors_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_organic_search_uvnb_hsi_flow_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'ORGANIC SEARCH', 'UpvByod', TRY_CAST(visitors AS DOUBLE), 'sdi_raw_adobe_pp_uvnb_organic_search_uvnb_byod_flow_visitors_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_organic_search_uvnb_byod_flow_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'ORGANIC SEARCH', 'UpvFlowTotal', TRY_CAST(visitors AS DOUBLE), 'sdi_raw_adobe_pp_uvnb_organic_search_flow_total_visitors_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_organic_search_flow_total_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'ORGANIC SEARCH', 'CartstartPostpaid', TRY_CAST(visits AS DOUBLE), 'sdi_raw_adobe_pp_uvnb_organic_search_postpaid_cartstart_visits_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_organic_search_postpaid_cartstart_visits_weekly_tmo`
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'ORGANIC SEARCH', 'CartstartHsi', TRY_CAST(visits AS DOUBLE), 'sdi_raw_adobe_pp_uvnb_organic_search_hsi_cartstart_visits_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_organic_search_hsi_cartstart_visits_weekly_tmo`
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'ORGANIC SEARCH', 'CartstartByod', TRY_CAST(visits AS DOUBLE), 'sdi_raw_adobe_pp_uvnb_organic_search_byod_cartstart_visits_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_organic_search_byod_cartstart_visits_weekly_tmo`
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'ORGANIC SEARCH', 'OrdersUnassistedPostpaid', TRY_CAST(orders AS DOUBLE), 'sdi_raw_adobe_pp_uvnb_organic_search_postpaid_order_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_organic_search_postpaid_order_weekly_tmo`
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'ORGANIC SEARCH', 'OrdersUnassistedHsi', TRY_CAST(orders AS DOUBLE), 'sdi_raw_adobe_pp_uvnb_organic_search_hsi_order_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_organic_search_hsi_order_weekly_tmo`
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'ORGANIC SEARCH', 'OrdersUnassistedByod', TRY_CAST(orders AS DOUBLE), 'sdi_raw_adobe_pp_uvnb_organic_search_byod_order_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_organic_search_byod_order_weekly_tmo`
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'ORGANIC SEARCH', 'OrdersAssistedPostpaid', TRY_CAST(orders AS DOUBLE), 'sdi_raw_adobe_pp_uvnb_organic_search_postpaid_order_assisted_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_organic_search_postpaid_order_assisted_weekly_tmo`
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'ORGANIC SEARCH', 'OrdersAssistedHsi', TRY_CAST(orders AS DOUBLE), 'sdi_raw_adobe_pp_uvnb_organic_search_hsi_order_assisted_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_organic_search_hsi_order_assisted_weekly_tmo`
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'ORGANIC SEARCH', 'OrdersAssistedByod', TRY_CAST(orders AS DOUBLE), 'sdi_raw_adobe_pp_uvnb_organic_search_byod_order_assisted_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_organic_search_byod_order_assisted_weekly_tmo`

  /* ------------------------------------------------------------------ OTHER ------------------------------------------------------------------ */
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'OTHER', 'UpvPostpaid', TRY_CAST(visitors AS DOUBLE), 'sdi_raw_adobe_pp_uvnb_other_uvnb_postpaid_flow_visitors_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_other_uvnb_postpaid_flow_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'OTHER', 'UpvHsi', TRY_CAST(visitors AS DOUBLE), 'sdi_raw_adobe_pp_uvnb_other_uvnb_hsi_flow_visitors_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_other_uvnb_hsi_flow_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'OTHER', 'UpvByod', TRY_CAST(visitors AS DOUBLE), 'sdi_raw_adobe_pp_uvnb_other_uvnb_byod_flow_visitors_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_other_uvnb_byod_flow_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'OTHER', 'UpvFlowTotal', TRY_CAST(visitors AS DOUBLE), 'sdi_raw_adobe_pp_uvnb_other_flow_total_visitors_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_other_flow_total_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'OTHER', 'CartstartPostpaid', TRY_CAST(visits AS DOUBLE), 'sdi_raw_adobe_pp_uvnb_other_postpaid_cartstart_visits_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_other_postpaid_cartstart_visits_weekly_tmo`
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'OTHER', 'CartstartHsi', TRY_CAST(visits AS DOUBLE), 'sdi_raw_adobe_pp_uvnb_other_hsi_cartstart_visits_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_other_hsi_cartstart_visits_weekly_tmo`
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'OTHER', 'CartstartByod', TRY_CAST(visits AS DOUBLE), 'sdi_raw_adobe_pp_uvnb_other_byod_cartstart_visits_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_other_byod_cartstart_visits_weekly_tmo`
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'OTHER', 'OrdersUnassistedPostpaid', TRY_CAST(orders AS DOUBLE), 'sdi_raw_adobe_pp_uvnb_other_postpaid_order_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_other_postpaid_order_weekly_tmo`
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'OTHER', 'OrdersUnassistedHsi', TRY_CAST(orders AS DOUBLE), 'sdi_raw_adobe_pp_uvnb_other_hsi_order_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_other_hsi_order_weekly_tmo`
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'OTHER', 'OrdersUnassistedByod', TRY_CAST(orders AS DOUBLE), 'sdi_raw_adobe_pp_uvnb_other_byod_order_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_other_byod_order_weekly_tmo`
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'OTHER', 'OrdersAssistedPostpaid', TRY_CAST(orders AS DOUBLE), 'sdi_raw_adobe_pp_uvnb_other_postpaid_order_assisted_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_other_postpaid_order_assisted_weekly_tmo`
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'OTHER', 'OrdersAssistedHsi', TRY_CAST(orders AS DOUBLE), 'sdi_raw_adobe_pp_uvnb_other_hsi_order_assisted_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_other_hsi_order_assisted_weekly_tmo`
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'OTHER', 'OrdersAssistedByod', TRY_CAST(orders AS DOUBLE), 'sdi_raw_adobe_pp_uvnb_other_byod_order_assisted_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_other_byod_order_assisted_weekly_tmo`

  /* ------------------------------------------------------------------ PAID SEARCH ------------------------------------------------------------------ */
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'PAID SEARCH', 'UpvPostpaid', TRY_CAST(visitors AS DOUBLE), 'sdi_raw_adobe_pp_uvnb_paid_search_uvnb_postpaid_flow_visitors_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_paid_search_uvnb_postpaid_flow_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'PAID SEARCH', 'UpvHsi', TRY_CAST(visitors AS DOUBLE), 'sdi_raw_adobe_pp_uvnb_paid_search_uvnb_hsi_flow_visitors_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_paid_search_uvnb_hsi_flow_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'PAID SEARCH', 'UpvByod', TRY_CAST(visitors AS DOUBLE), 'sdi_raw_adobe_pp_uvnb_paid_search_uvnb_byod_flow_visitors_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_paid_search_uvnb_byod_flow_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'PAID SEARCH', 'UpvFlowTotal', TRY_CAST(visitors AS DOUBLE), 'sdi_raw_adobe_pp_uvnb_paid_search_flow_total_visitors_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_paid_search_flow_total_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'PAID SEARCH', 'CartstartPostpaid', TRY_CAST(visits AS DOUBLE), 'sdi_raw_adobe_pp_uvnb_paid_search_postpaid_cartstart_visits_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_paid_search_postpaid_cartstart_visits_weekly_tmo`
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'PAID SEARCH', 'CartstartHsi', TRY_CAST(visits AS DOUBLE), 'sdi_raw_adobe_pp_uvnb_paid_search_hsi_cartstart_visits_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_paid_search_hsi_cartstart_visits_weekly_tmo`
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'PAID SEARCH', 'CartstartByod', TRY_CAST(visits AS DOUBLE), 'sdi_raw_adobe_pp_uvnb_paid_search_byod_cartstart_visits_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_paid_search_byod_cartstart_visits_weekly_tmo`
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'PAID SEARCH', 'OrdersUnassistedPostpaid', TRY_CAST(orders AS DOUBLE), 'sdi_raw_adobe_pp_uvnb_paid_search_postpaid_order_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_paid_search_postpaid_order_weekly_tmo`
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'PAID SEARCH', 'OrdersUnassistedHsi', TRY_CAST(orders AS DOUBLE), 'sdi_raw_adobe_pp_uvnb_paid_search_hsi_order_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_paid_search_hsi_order_weekly_tmo`
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'PAID SEARCH', 'OrdersUnassistedByod', TRY_CAST(orders AS DOUBLE), 'sdi_raw_adobe_pp_uvnb_paid_search_byod_order_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_paid_search_byod_order_weekly_tmo`
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'PAID SEARCH', 'OrdersAssistedPostpaid', TRY_CAST(orders AS DOUBLE), 'sdi_raw_adobe_pp_uvnb_paid_search_postpaid_order_assisted_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_paid_search_postpaid_order_assisted_weekly_tmo`
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'PAID SEARCH', 'OrdersAssistedHsi', TRY_CAST(orders AS DOUBLE), 'sdi_raw_adobe_pp_uvnb_paid_search_hsi_order_assisted_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_paid_search_hsi_order_assisted_weekly_tmo`
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'PAID SEARCH', 'OrdersAssistedByod', TRY_CAST(orders AS DOUBLE), 'sdi_raw_adobe_pp_uvnb_paid_search_byod_order_assisted_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_paid_search_byod_order_assisted_weekly_tmo`

  /* ------------------------------------------------------------------ PROGRAMMATIC ------------------------------------------------------------------ */
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'PROGRAMMATIC', 'UpvPostpaid', TRY_CAST(visitors AS DOUBLE), 'sdi_raw_adobe_pp_uvnb_programmatic_uvnb_postpaid_flow_visitors_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_programmatic_uvnb_postpaid_flow_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'PROGRAMMATIC', 'UpvHsi', TRY_CAST(visitors AS DOUBLE), 'sdi_raw_adobe_pp_uvnb_programmatic_uvnb_hsi_flow_visitors_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_programmatic_uvnb_hsi_flow_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'PROGRAMMATIC', 'UpvByod', TRY_CAST(visitors AS DOUBLE), 'sdi_raw_adobe_pp_uvnb_programmatic_uvnb_byod_flow_visitors_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_programmatic_uvnb_byod_flow_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'PROGRAMMATIC', 'UpvFlowTotal', TRY_CAST(visitors AS DOUBLE), 'sdi_raw_adobe_pp_uvnb_programmatic_flow_total_visitors_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_programmatic_flow_total_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'PROGRAMMATIC', 'CartstartPostpaid', TRY_CAST(visits AS DOUBLE), 'sdi_raw_adobe_pp_uvnb_programmatic_postpaid_cartstart_visits_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_programmatic_postpaid_cartstart_visits_weekly_tmo`
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'PROGRAMMATIC', 'CartstartHsi', TRY_CAST(visits AS DOUBLE), 'sdi_raw_adobe_pp_uvnb_programmatic_hsi_cartstart_visits_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_programmatic_hsi_cartstart_visits_weekly_tmo`
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'PROGRAMMATIC', 'CartstartByod', TRY_CAST(visits AS DOUBLE), 'sdi_raw_adobe_pp_uvnb_programmatic_byod_cartstart_visits_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_programmatic_byod_cartstart_visits_weekly_tmo`
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'PROGRAMMATIC', 'OrdersUnassistedPostpaid', TRY_CAST(orders AS DOUBLE), 'sdi_raw_adobe_pp_uvnb_programmatic_postpaid_order_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_programmatic_postpaid_order_weekly_tmo`
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'PROGRAMMATIC', 'OrdersUnassistedHsi', TRY_CAST(orders AS DOUBLE), 'sdi_raw_adobe_pp_uvnb_programmatic_hsi_order_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_programmatic_hsi_order_weekly_tmo`
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'PROGRAMMATIC', 'OrdersUnassistedByod', TRY_CAST(orders AS DOUBLE), 'sdi_raw_adobe_pp_uvnb_programmatic_byod_order_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_programmatic_byod_order_weekly_tmo`
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'PROGRAMMATIC', 'OrdersAssistedPostpaid', TRY_CAST(orders AS DOUBLE), 'sdi_raw_adobe_pp_uvnb_programmatic_postpaid_order_assisted_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_programmatic_postpaid_order_assisted_weekly_tmo`
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'PROGRAMMATIC', 'OrdersAssistedHsi', TRY_CAST(orders AS DOUBLE), 'sdi_raw_adobe_pp_uvnb_programmatic_hsi_order_assisted_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_programmatic_hsi_order_assisted_weekly_tmo`
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'PROGRAMMATIC', 'OrdersAssistedByod', TRY_CAST(orders AS DOUBLE), 'sdi_raw_adobe_pp_uvnb_programmatic_byod_order_assisted_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_programmatic_byod_order_assisted_weekly_tmo`

  /* ------------------------------------------------------------------ SOCIAL ------------------------------------------------------------------ */
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'SOCIAL', 'UpvPostpaid', TRY_CAST(visitors AS DOUBLE), 'sdi_raw_adobe_pp_uvnb_social_uvnb_postpaid_flow_visitors_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_social_uvnb_postpaid_flow_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'SOCIAL', 'UpvHsi', TRY_CAST(visitors AS DOUBLE), 'sdi_raw_adobe_pp_uvnb_social_uvnb_hsi_flow_visitors_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_social_uvnb_hsi_flow_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'SOCIAL', 'UpvByod', TRY_CAST(visitors AS DOUBLE), 'sdi_raw_adobe_pp_uvnb_social_uvnb_byod_flow_visitors_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_social_uvnb_byod_flow_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'SOCIAL', 'UpvFlowTotal', TRY_CAST(visitors AS DOUBLE), 'sdi_raw_adobe_pp_uvnb_social_flow_total_visitors_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_social_flow_total_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'SOCIAL', 'CartstartPostpaid', TRY_CAST(visits AS DOUBLE), 'sdi_raw_adobe_pp_uvnb_social_postpaid_cartstart_visits_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_social_postpaid_cartstart_visits_weekly_tmo`
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'SOCIAL', 'CartstartHsi', TRY_CAST(visits AS DOUBLE), 'sdi_raw_adobe_pp_uvnb_social_hsi_cartstart_visits_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_social_hsi_cartstart_visits_weekly_tmo`
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'SOCIAL', 'CartstartByod', TRY_CAST(visits AS DOUBLE), 'sdi_raw_adobe_pp_uvnb_social_byod_cartstart_visits_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_social_byod_cartstart_visits_weekly_tmo`
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'SOCIAL', 'OrdersUnassistedPostpaid', TRY_CAST(orders AS DOUBLE), 'sdi_raw_adobe_pp_uvnb_social_postpaid_order_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_social_postpaid_order_weekly_tmo`
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'SOCIAL', 'OrdersUnassistedHsi', TRY_CAST(orders AS DOUBLE), 'sdi_raw_adobe_pp_uvnb_social_hsi_order_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_social_hsi_order_weekly_tmo`
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'SOCIAL', 'OrdersUnassistedByod', TRY_CAST(orders AS DOUBLE), 'sdi_raw_adobe_pp_uvnb_social_byod_order_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_social_byod_order_weekly_tmo`
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'SOCIAL', 'OrdersAssistedPostpaid', TRY_CAST(orders AS DOUBLE), 'sdi_raw_adobe_pp_uvnb_social_postpaid_order_assisted_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_social_postpaid_order_assisted_weekly_tmo`
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'SOCIAL', 'OrdersAssistedHsi', TRY_CAST(orders AS DOUBLE), 'sdi_raw_adobe_pp_uvnb_social_hsi_order_assisted_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_social_hsi_order_assisted_weekly_tmo`
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'SOCIAL', 'OrdersAssistedByod', TRY_CAST(orders AS DOUBLE), 'sdi_raw_adobe_pp_uvnb_social_byod_order_assisted_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_social_byod_order_assisted_weekly_tmo`
),

Deduped AS (
  SELECT *
  FROM RawUnion
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY
      WeekSunSat,
      DataGranularity,
      LtcGroup,
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

END;