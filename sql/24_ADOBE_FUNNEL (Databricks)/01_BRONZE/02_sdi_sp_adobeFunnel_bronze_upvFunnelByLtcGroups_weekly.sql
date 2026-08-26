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

  Adobe Experience Cloud (EC) sources, added after the UPV and Cartstart metrics (before Orders):
    sdi_raw_adobe_pp_ec_{group}_ec_completed_weekly_tmo          (page_events -> EcCompleted)
    sdi_raw_adobe_pp_ec_{group}_ec_successful_visits_weekly_tmo  (visits -> EcSuccessful)
  where {group} = direct, organic_search, other, paid_search, programmatic, social
  Independent of the UPV/Cartstart/Orders funnel metrics — not derived from them.

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
prdrzranalytics.lab42.sdi_sp_adobeFunnel_bronze_upvFunnelByLtcGroups_weekly()
LANGUAGE SQL
SQL SECURITY INVOKER
MODIFIES SQL DATA
AS
BEGIN

  CREATE OR REPLACE TABLE
  prdrzranalytics.lab42.sdi_tbl_adobeFunnel_bronze_upvFunnelByLtcGroups_weekly
  USING DELTA
  AS

WITH RawUnion AS (

  /* ------------------------------------------------------------------ DIRECT ------------------------------------------------------------------ */
  SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6) AS WeekSunSat, 'LTC_GROUPS' AS DataGranularity, CAST(NULL AS STRING) AS LastTouchChannel, 'Direct' AS LtcGroup, 'UpvPostpaid' AS MetricName, TRY_CAST(visitors AS DOUBLE) AS MetricValue, 'sdi_raw_adobe_pp_uvnb_direct_uvnb_postpaid_flow_visitors_weekly_tmo' AS SourceTable, __insert_date AS InsertDate, File_Load_datetime AS FileLoadDatetime, Filename FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_direct_uvnb_postpaid_flow_visitors_weekly_tmo
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'Direct', 'UpvHsi', TRY_CAST(visitors AS DOUBLE), 'sdi_raw_adobe_pp_uvnb_direct_uvnb_hsi_flow_visitors_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_direct_uvnb_hsi_flow_visitors_weekly_tmo
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'Direct', 'UpvByod', TRY_CAST(visitors AS DOUBLE), 'sdi_raw_adobe_pp_uvnb_direct_uvnb_byod_flow_visitors_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_direct_uvnb_byod_flow_visitors_weekly_tmo
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'Direct', 'UpvFlowTotal', TRY_CAST(visitors AS DOUBLE), 'sdi_raw_adobe_pp_uvnb_direct_flow_total_visitors_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_direct_flow_total_visitors_weekly_tmo
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'Direct', 'CartstartPostpaid', TRY_CAST(visits AS DOUBLE), 'sdi_raw_adobe_pp_uvnb_direct_postpaid_cartstart_visits_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_direct_postpaid_cartstart_visits_weekly_tmo
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'Direct', 'CartstartHsi', TRY_CAST(visits AS DOUBLE), 'sdi_raw_adobe_pp_uvnb_direct_hsi_cartstart_visits_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_direct_hsi_cartstart_visits_weekly_tmo
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'Direct', 'CartstartByod', TRY_CAST(visits AS DOUBLE), 'sdi_raw_adobe_pp_uvnb_direct_byod_cartstart_visits_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_direct_byod_cartstart_visits_weekly_tmo
-- COMMENTED OUT (EC raw source not yet active):   UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'Direct', 'EcCompleted', TRY_CAST(page_events AS DOUBLE), 'sdi_raw_adobe_pp_ec_direct_ec_completed_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_ec_direct_ec_completed_weekly_tmo
-- COMMENTED OUT (EC raw source not yet active):   UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'Direct', 'EcSuccessful', TRY_CAST(visits AS DOUBLE), 'sdi_raw_adobe_pp_ec_direct_ec_successful_visits_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_ec_direct_ec_successful_visits_weekly_tmo
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'Direct', 'OrdersUnassistedPostpaid', TRY_CAST(orders AS DOUBLE), 'sdi_raw_adobe_pp_uvnb_direct_postpaid_order_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_direct_postpaid_order_weekly_tmo
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'Direct', 'OrdersUnassistedHsi', TRY_CAST(orders AS DOUBLE), 'sdi_raw_adobe_pp_uvnb_direct_hsi_order_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_direct_hsi_order_weekly_tmo
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'Direct', 'OrdersUnassistedByod', TRY_CAST(orders AS DOUBLE), 'sdi_raw_adobe_pp_uvnb_direct_byod_order_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_direct_byod_order_weekly_tmo
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'Direct', 'OrdersAssistedPostpaid', TRY_CAST(orders AS DOUBLE), 'sdi_raw_adobe_pp_uvnb_direct_postpaid_order_assisted_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_direct_postpaid_order_assisted_weekly_tmo
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'Direct', 'OrdersAssistedHsi', TRY_CAST(orders AS DOUBLE), 'sdi_raw_adobe_pp_uvnb_direct_hsi_order_assisted_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_direct_hsi_order_assisted_weekly_tmo
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'Direct', 'OrdersAssistedByod', TRY_CAST(orders AS DOUBLE), 'sdi_raw_adobe_pp_uvnb_direct_byod_order_assisted_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_direct_byod_order_assisted_weekly_tmo

  /* ------------------------------------------------------------------ ORGANIC SEARCH ------------------------------------------------------------------ */
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'Organic Search', 'UpvPostpaid', TRY_CAST(visitors AS DOUBLE), 'sdi_raw_adobe_pp_uvnb_organic_search_uvnb_postpaid_flow_visitors_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_organic_search_uvnb_postpaid_flow_visitors_weekly_tmo
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'Organic Search', 'UpvHsi', TRY_CAST(visitors AS DOUBLE), 'sdi_raw_adobe_pp_uvnb_organic_search_uvnb_hsi_flow_visitors_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_organic_search_uvnb_hsi_flow_visitors_weekly_tmo
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'Organic Search', 'UpvByod', TRY_CAST(visitors AS DOUBLE), 'sdi_raw_adobe_pp_uvnb_organic_search_uvnb_byod_flow_visitors_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_organic_search_uvnb_byod_flow_visitors_weekly_tmo
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'Organic Search', 'UpvFlowTotal', TRY_CAST(visitors AS DOUBLE), 'sdi_raw_adobe_pp_uvnb_organic_search_flow_total_visitors_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_organic_search_flow_total_visitors_weekly_tmo
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'Organic Search', 'CartstartPostpaid', TRY_CAST(visits AS DOUBLE), 'sdi_raw_adobe_pp_uvnb_organic_search_postpaid_cartstart_visits_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_organic_search_postpaid_cartstart_visits_weekly_tmo
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'Organic Search', 'CartstartHsi', TRY_CAST(visits AS DOUBLE), 'sdi_raw_adobe_pp_uvnb_organic_search_hsi_cartstart_visits_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_organic_search_hsi_cartstart_visits_weekly_tmo
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'Organic Search', 'CartstartByod', TRY_CAST(visits AS DOUBLE), 'sdi_raw_adobe_pp_uvnb_organic_search_byod_cartstart_visits_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_organic_search_byod_cartstart_visits_weekly_tmo
-- COMMENTED OUT (EC raw source not yet active):   UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'Organic Search', 'EcCompleted', TRY_CAST(page_events AS DOUBLE), 'sdi_raw_adobe_pp_ec_organic_search_ec_completed_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_ec_organic_search_ec_completed_weekly_tmo
-- COMMENTED OUT (EC raw source not yet active):   UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'Organic Search', 'EcSuccessful', TRY_CAST(visits AS DOUBLE), 'sdi_raw_adobe_pp_ec_organic_search_ec_successful_visits_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_ec_organic_search_ec_successful_visits_weekly_tmo
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'Organic Search', 'OrdersUnassistedPostpaid', TRY_CAST(orders AS DOUBLE), 'sdi_raw_adobe_pp_uvnb_organic_search_postpaid_order_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_organic_search_postpaid_order_weekly_tmo
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'Organic Search', 'OrdersUnassistedHsi', TRY_CAST(orders AS DOUBLE), 'sdi_raw_adobe_pp_uvnb_organic_search_hsi_order_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_organic_search_hsi_order_weekly_tmo
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'Organic Search', 'OrdersUnassistedByod', TRY_CAST(orders AS DOUBLE), 'sdi_raw_adobe_pp_uvnb_organic_search_byod_order_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_organic_search_byod_order_weekly_tmo
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'Organic Search', 'OrdersAssistedPostpaid', TRY_CAST(orders AS DOUBLE), 'sdi_raw_adobe_pp_uvnb_organic_search_postpaid_order_assisted_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_organic_search_postpaid_order_assisted_weekly_tmo
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'Organic Search', 'OrdersAssistedHsi', TRY_CAST(orders AS DOUBLE), 'sdi_raw_adobe_pp_uvnb_organic_search_hsi_order_assisted_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_organic_search_hsi_order_assisted_weekly_tmo
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'Organic Search', 'OrdersAssistedByod', TRY_CAST(orders AS DOUBLE), 'sdi_raw_adobe_pp_uvnb_organic_search_byod_order_assisted_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_organic_search_byod_order_assisted_weekly_tmo

  /* ------------------------------------------------------------------ OTHER ------------------------------------------------------------------ */
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'Other', 'UpvPostpaid', TRY_CAST(visitors AS DOUBLE), 'sdi_raw_adobe_pp_uvnb_other_uvnb_postpaid_flow_visitors_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_other_uvnb_postpaid_flow_visitors_weekly_tmo
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'Other', 'UpvHsi', TRY_CAST(visitors AS DOUBLE), 'sdi_raw_adobe_pp_uvnb_other_uvnb_hsi_flow_visitors_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_other_uvnb_hsi_flow_visitors_weekly_tmo
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'Other', 'UpvByod', TRY_CAST(visitors AS DOUBLE), 'sdi_raw_adobe_pp_uvnb_other_uvnb_byod_flow_visitors_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_other_uvnb_byod_flow_visitors_weekly_tmo
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'Other', 'UpvFlowTotal', TRY_CAST(visitors AS DOUBLE), 'sdi_raw_adobe_pp_uvnb_other_flow_total_visitors_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_other_flow_total_visitors_weekly_tmo
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'Other', 'CartstartPostpaid', TRY_CAST(visits AS DOUBLE), 'sdi_raw_adobe_pp_uvnb_other_postpaid_cartstart_visits_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_other_postpaid_cartstart_visits_weekly_tmo
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'Other', 'CartstartHsi', TRY_CAST(visits AS DOUBLE), 'sdi_raw_adobe_pp_uvnb_other_hsi_cartstart_visits_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_other_hsi_cartstart_visits_weekly_tmo
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'Other', 'CartstartByod', TRY_CAST(visits AS DOUBLE), 'sdi_raw_adobe_pp_uvnb_other_byod_cartstart_visits_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_other_byod_cartstart_visits_weekly_tmo
-- COMMENTED OUT (EC raw source not yet active):   UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'Other', 'EcCompleted', TRY_CAST(page_events AS DOUBLE), 'sdi_raw_adobe_pp_ec_other_ec_completed_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_ec_other_ec_completed_weekly_tmo
-- COMMENTED OUT (EC raw source not yet active):   UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'Other', 'EcSuccessful', TRY_CAST(visits AS DOUBLE), 'sdi_raw_adobe_pp_ec_other_ec_successful_visits_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_ec_other_ec_successful_visits_weekly_tmo
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'Other', 'OrdersUnassistedPostpaid', TRY_CAST(orders AS DOUBLE), 'sdi_raw_adobe_pp_uvnb_other_postpaid_order_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_other_postpaid_order_weekly_tmo
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'Other', 'OrdersUnassistedHsi', TRY_CAST(orders AS DOUBLE), 'sdi_raw_adobe_pp_uvnb_other_hsi_order_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_other_hsi_order_weekly_tmo
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'Other', 'OrdersUnassistedByod', TRY_CAST(orders AS DOUBLE), 'sdi_raw_adobe_pp_uvnb_other_byod_order_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_other_byod_order_weekly_tmo
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'Other', 'OrdersAssistedPostpaid', TRY_CAST(orders AS DOUBLE), 'sdi_raw_adobe_pp_uvnb_other_postpaid_order_assisted_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_other_postpaid_order_assisted_weekly_tmo
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'Other', 'OrdersAssistedHsi', TRY_CAST(orders AS DOUBLE), 'sdi_raw_adobe_pp_uvnb_other_hsi_order_assisted_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_other_hsi_order_assisted_weekly_tmo
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'Other', 'OrdersAssistedByod', TRY_CAST(orders AS DOUBLE), 'sdi_raw_adobe_pp_uvnb_other_byod_order_assisted_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_other_byod_order_assisted_weekly_tmo

  /* ------------------------------------------------------------------ PAID SEARCH ------------------------------------------------------------------ */
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'Paid Search', 'UpvPostpaid', TRY_CAST(visitors AS DOUBLE), 'sdi_raw_adobe_pp_uvnb_paid_search_uvnb_postpaid_flow_visitors_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_paid_search_uvnb_postpaid_flow_visitors_weekly_tmo
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'Paid Search', 'UpvHsi', TRY_CAST(visitors AS DOUBLE), 'sdi_raw_adobe_pp_uvnb_paid_search_uvnb_hsi_flow_visitors_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_paid_search_uvnb_hsi_flow_visitors_weekly_tmo
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'Paid Search', 'UpvByod', TRY_CAST(visitors AS DOUBLE), 'sdi_raw_adobe_pp_uvnb_paid_search_uvnb_byod_flow_visitors_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_paid_search_uvnb_byod_flow_visitors_weekly_tmo
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'Paid Search', 'UpvFlowTotal', TRY_CAST(visitors AS DOUBLE), 'sdi_raw_adobe_pp_uvnb_paid_search_flow_total_visitors_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_paid_search_flow_total_visitors_weekly_tmo
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'Paid Search', 'CartstartPostpaid', TRY_CAST(visits AS DOUBLE), 'sdi_raw_adobe_pp_uvnb_paid_search_postpaid_cartstart_visits_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_paid_search_postpaid_cartstart_visits_weekly_tmo
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'Paid Search', 'CartstartHsi', TRY_CAST(visits AS DOUBLE), 'sdi_raw_adobe_pp_uvnb_paid_search_hsi_cartstart_visits_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_paid_search_hsi_cartstart_visits_weekly_tmo
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'Paid Search', 'CartstartByod', TRY_CAST(visits AS DOUBLE), 'sdi_raw_adobe_pp_uvnb_paid_search_byod_cartstart_visits_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_paid_search_byod_cartstart_visits_weekly_tmo
-- COMMENTED OUT (EC raw source not yet active):   UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'Paid Search', 'EcCompleted', TRY_CAST(page_events AS DOUBLE), 'sdi_raw_adobe_pp_ec_paid_search_ec_completed_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_ec_paid_search_ec_completed_weekly_tmo
-- COMMENTED OUT (EC raw source not yet active):   UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'Paid Search', 'EcSuccessful', TRY_CAST(visits AS DOUBLE), 'sdi_raw_adobe_pp_ec_paid_search_ec_successful_visits_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_ec_paid_search_ec_successful_visits_weekly_tmo
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'Paid Search', 'OrdersUnassistedPostpaid', TRY_CAST(orders AS DOUBLE), 'sdi_raw_adobe_pp_uvnb_paid_search_postpaid_order_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_paid_search_postpaid_order_weekly_tmo
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'Paid Search', 'OrdersUnassistedHsi', TRY_CAST(orders AS DOUBLE), 'sdi_raw_adobe_pp_uvnb_paid_search_hsi_order_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_paid_search_hsi_order_weekly_tmo
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'Paid Search', 'OrdersUnassistedByod', TRY_CAST(orders AS DOUBLE), 'sdi_raw_adobe_pp_uvnb_paid_search_byod_order_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_paid_search_byod_order_weekly_tmo
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'Paid Search', 'OrdersAssistedPostpaid', TRY_CAST(orders AS DOUBLE), 'sdi_raw_adobe_pp_uvnb_paid_search_postpaid_order_assisted_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_paid_search_postpaid_order_assisted_weekly_tmo
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'Paid Search', 'OrdersAssistedHsi', TRY_CAST(orders AS DOUBLE), 'sdi_raw_adobe_pp_uvnb_paid_search_hsi_order_assisted_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_paid_search_hsi_order_assisted_weekly_tmo
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'Paid Search', 'OrdersAssistedByod', TRY_CAST(orders AS DOUBLE), 'sdi_raw_adobe_pp_uvnb_paid_search_byod_order_assisted_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_paid_search_byod_order_assisted_weekly_tmo

  /* ------------------------------------------------------------------ PROGRAMMATIC ------------------------------------------------------------------ */
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'Programmatic', 'UpvPostpaid', TRY_CAST(visitors AS DOUBLE), 'sdi_raw_adobe_pp_uvnb_programmatic_uvnb_postpaid_flow_visitors_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_programmatic_uvnb_postpaid_flow_visitors_weekly_tmo
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'Programmatic', 'UpvHsi', TRY_CAST(visitors AS DOUBLE), 'sdi_raw_adobe_pp_uvnb_programmatic_uvnb_hsi_flow_visitors_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_programmatic_uvnb_hsi_flow_visitors_weekly_tmo
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'Programmatic', 'UpvByod', TRY_CAST(visitors AS DOUBLE), 'sdi_raw_adobe_pp_uvnb_programmatic_uvnb_byod_flow_visitors_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_programmatic_uvnb_byod_flow_visitors_weekly_tmo
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'Programmatic', 'UpvFlowTotal', TRY_CAST(visitors AS DOUBLE), 'sdi_raw_adobe_pp_uvnb_programmatic_flow_total_visitors_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_programmatic_flow_total_visitors_weekly_tmo
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'Programmatic', 'CartstartPostpaid', TRY_CAST(visits AS DOUBLE), 'sdi_raw_adobe_pp_uvnb_programmatic_postpaid_cartstart_visits_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_programmatic_postpaid_cartstart_visits_weekly_tmo
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'Programmatic', 'CartstartHsi', TRY_CAST(visits AS DOUBLE), 'sdi_raw_adobe_pp_uvnb_programmatic_hsi_cartstart_visits_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_programmatic_hsi_cartstart_visits_weekly_tmo
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'Programmatic', 'CartstartByod', TRY_CAST(visits AS DOUBLE), 'sdi_raw_adobe_pp_uvnb_programmatic_byod_cartstart_visits_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_programmatic_byod_cartstart_visits_weekly_tmo
-- COMMENTED OUT (EC raw source not yet active):   UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'Programmatic', 'EcCompleted', TRY_CAST(page_events AS DOUBLE), 'sdi_raw_adobe_pp_ec_programmatic_ec_completed_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_ec_programmatic_ec_completed_weekly_tmo
-- COMMENTED OUT (EC raw source not yet active):   UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'Programmatic', 'EcSuccessful', TRY_CAST(visits AS DOUBLE), 'sdi_raw_adobe_pp_ec_programmatic_ec_successful_visits_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_ec_programmatic_ec_successful_visits_weekly_tmo
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'Programmatic', 'OrdersUnassistedPostpaid', TRY_CAST(orders AS DOUBLE), 'sdi_raw_adobe_pp_uvnb_programmatic_postpaid_order_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_programmatic_postpaid_order_weekly_tmo
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'Programmatic', 'OrdersUnassistedHsi', TRY_CAST(orders AS DOUBLE), 'sdi_raw_adobe_pp_uvnb_programmatic_hsi_order_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_programmatic_hsi_order_weekly_tmo
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'Programmatic', 'OrdersUnassistedByod', TRY_CAST(orders AS DOUBLE), 'sdi_raw_adobe_pp_uvnb_programmatic_byod_order_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_programmatic_byod_order_weekly_tmo
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'Programmatic', 'OrdersAssistedPostpaid', TRY_CAST(orders AS DOUBLE), 'sdi_raw_adobe_pp_uvnb_programmatic_postpaid_order_assisted_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_programmatic_postpaid_order_assisted_weekly_tmo
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'Programmatic', 'OrdersAssistedHsi', TRY_CAST(orders AS DOUBLE), 'sdi_raw_adobe_pp_uvnb_programmatic_hsi_order_assisted_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_programmatic_hsi_order_assisted_weekly_tmo
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'Programmatic', 'OrdersAssistedByod', TRY_CAST(orders AS DOUBLE), 'sdi_raw_adobe_pp_uvnb_programmatic_byod_order_assisted_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_programmatic_byod_order_assisted_weekly_tmo

  /* ------------------------------------------------------------------ SOCIAL ------------------------------------------------------------------ */
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'Paid Social', 'UpvPostpaid', TRY_CAST(visitors AS DOUBLE), 'sdi_raw_adobe_pp_uvnb_social_uvnb_postpaid_flow_visitors_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_social_uvnb_postpaid_flow_visitors_weekly_tmo
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'Paid Social', 'UpvHsi', TRY_CAST(visitors AS DOUBLE), 'sdi_raw_adobe_pp_uvnb_social_uvnb_hsi_flow_visitors_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_social_uvnb_hsi_flow_visitors_weekly_tmo
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'Paid Social', 'UpvByod', TRY_CAST(visitors AS DOUBLE), 'sdi_raw_adobe_pp_uvnb_social_uvnb_byod_flow_visitors_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_social_uvnb_byod_flow_visitors_weekly_tmo
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'Paid Social', 'UpvFlowTotal', TRY_CAST(visitors AS DOUBLE), 'sdi_raw_adobe_pp_uvnb_social_flow_total_visitors_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_social_flow_total_visitors_weekly_tmo
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'Paid Social', 'CartstartPostpaid', TRY_CAST(visits AS DOUBLE), 'sdi_raw_adobe_pp_uvnb_social_postpaid_cartstart_visits_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_social_postpaid_cartstart_visits_weekly_tmo
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'Paid Social', 'CartstartHsi', TRY_CAST(visits AS DOUBLE), 'sdi_raw_adobe_pp_uvnb_social_hsi_cartstart_visits_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_social_hsi_cartstart_visits_weekly_tmo
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'Paid Social', 'CartstartByod', TRY_CAST(visits AS DOUBLE), 'sdi_raw_adobe_pp_uvnb_social_byod_cartstart_visits_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_social_byod_cartstart_visits_weekly_tmo
-- COMMENTED OUT (EC raw source not yet active):   UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'Paid Social', 'EcCompleted', TRY_CAST(page_events AS DOUBLE), 'sdi_raw_adobe_pp_ec_social_ec_completed_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_ec_social_ec_completed_weekly_tmo
-- COMMENTED OUT (EC raw source not yet active):   UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'Paid Social', 'EcSuccessful', TRY_CAST(visits AS DOUBLE), 'sdi_raw_adobe_pp_ec_social_ec_successful_visits_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_ec_social_ec_successful_visits_weekly_tmo
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'Paid Social', 'OrdersUnassistedPostpaid', TRY_CAST(orders AS DOUBLE), 'sdi_raw_adobe_pp_uvnb_social_postpaid_order_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_social_postpaid_order_weekly_tmo
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'Paid Social', 'OrdersUnassistedHsi', TRY_CAST(orders AS DOUBLE), 'sdi_raw_adobe_pp_uvnb_social_hsi_order_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_social_hsi_order_weekly_tmo
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'Paid Social', 'OrdersUnassistedByod', TRY_CAST(orders AS DOUBLE), 'sdi_raw_adobe_pp_uvnb_social_byod_order_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_social_byod_order_weekly_tmo
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'Paid Social', 'OrdersAssistedPostpaid', TRY_CAST(orders AS DOUBLE), 'sdi_raw_adobe_pp_uvnb_social_postpaid_order_assisted_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_social_postpaid_order_assisted_weekly_tmo
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'Paid Social', 'OrdersAssistedHsi', TRY_CAST(orders AS DOUBLE), 'sdi_raw_adobe_pp_uvnb_social_hsi_order_assisted_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_social_hsi_order_assisted_weekly_tmo
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'Paid Social', 'OrdersAssistedByod', TRY_CAST(orders AS DOUBLE), 'sdi_raw_adobe_pp_uvnb_social_byod_order_assisted_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_social_byod_order_assisted_weekly_tmo
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

  -- Adobe Experience Cloud (EC) — COMMENTED OUT: raw source tables not yet active in
  -- Databricks. Uncomment once the ec_completed / ec_successful_visits raw tables are live.
  -- MAX(IF(MetricName = 'EcCompleted',             MetricValue, NULL)) AS EcCompleted,
  -- MAX(IF(MetricName = 'EcSuccessful',      MetricValue, NULL)) AS EcSuccessful,

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