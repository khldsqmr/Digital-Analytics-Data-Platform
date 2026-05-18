/* =================================================================================================
FILE: 03_vw_sdi_adobe_bronze_uvnbCartstartOrdersByLtcGroups_Weekly.sql
LAYER: Bronze View
DATASET: prj-dbi-prd-1.ds_dbi_digitalmedia_automation
VIEW: vw_sdi_adobe_bronze_uvnbCartstartOrdersByLtcGroups_Weekly

SOURCES:
  Adobe weekly UVNB, Cartstart, Unassisted Orders, and Assisted Orders raw tables
  for these LTC groups: DIRECT, ORGANIC SEARCH, OTHER, PAID SEARCH, PROGRAMMATIC, SOCIAL

  Assisted order sources (NEW):
    sdi_raw_adobe_pp_uvnb_{group}_postpaid_order_assisted_weekly_tmo
    sdi_raw_adobe_pp_uvnb_{group}_hsi_order_assisted_weekly_tmo
    sdi_raw_adobe_pp_uvnb_{group}_byod_order_assisted_weekly_tmo
  where {group} = direct, organic_search, other, paid_search, programmatic, social

DESTINATION:
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_adobe_bronze_uvnbCartstartOrdersByLtcGroups_Weekly

PURPOSE:
  Canonical Bronze weekly Adobe UVNB, Cartstart, and Orders source mart at LTC_GROUPS granularity.
  This view uses Adobe LTC group source tables directly and does not calculate group values from LTC rows.

  Orders are split into:
    - Unassisted (digital web unassisted orders) — renamed from original OrdersPostpaid/Hsi/Byod
    - Assisted (digital web assisted orders)     — new as of this version

BUSINESS GRAIN:
  One row per:
      WeekSunSat
      LtcGroup

BUSINESS RULES:
  - WeekSunSat is derived from date_yyyymmdd by adding 6 days because raw date_yyyymmdd is week-starting Sunday.
  - DataGranularity is fixed as LTC_GROUPS.
  - LastTouchChannel is NULL.
  - LtcGroup is derived from the raw table family.
  - Postpaid / HSI / BYOD columns are separate metrics and are not summed together.
  - Missing metric values remain NULL.
  - MetricName / MetricValue are used internally only and are not exposed in final output.

COLUMN CHANGES vs PREVIOUS VERSION:
  - OrdersPostpaid  renamed to  OrdersUnassistedPostpaid
  - OrdersHsi       renamed to  OrdersUnassistedHsi
  - OrdersByod      renamed to  OrdersUnassistedByod
  - OrdersAssistedPostpaid  ADDED (new)
  - OrdersAssistedHsi       ADDED (new)
  - OrdersAssistedByod      ADDED (new)

KEY DEDUPE RULE:
  - Deduplicate each source table at weekly + LtcGroup grain using latest:
      File_Load_datetime DESC
      Filename DESC
      __insert_date DESC
================================================================================================= */

CREATE OR REPLACE VIEW
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_adobe_bronze_uvnbCartstartOrdersByLtcGroups_Weekly`
AS

WITH RawUnion AS (

  /* ------------------------------------------------------------------ DIRECT ------------------------------------------------------------------ */
  SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY) AS WeekSunSat, 'LTC_GROUPS' AS DataGranularity, CAST(NULL AS STRING) AS LastTouchChannel, 'DIRECT' AS LtcGroup, 'UvnbPostpaid' AS MetricName, SAFE_CAST(visitors AS FLOAT64) AS MetricValue, 'sdi_raw_adobe_pp_uvnb_direct_uvnb_postpaid_flow_visitors_weekly_tmo' AS SourceTable, __insert_date AS InsertDate, File_Load_datetime AS FileLoadDatetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_direct_uvnb_postpaid_flow_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'LTC_GROUPS', CAST(NULL AS STRING), 'DIRECT', 'UvnbHsi', SAFE_CAST(visitors AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_direct_uvnb_hsi_flow_visitors_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_direct_uvnb_hsi_flow_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'LTC_GROUPS', CAST(NULL AS STRING), 'DIRECT', 'UvnbByod', SAFE_CAST(visitors AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_direct_uvnb_byod_flow_visitors_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_direct_uvnb_byod_flow_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'LTC_GROUPS', CAST(NULL AS STRING), 'DIRECT', 'CartstartPostpaid', SAFE_CAST(visits AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_direct_postpaid_cartstart_visits_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_direct_postpaid_cartstart_visits_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'LTC_GROUPS', CAST(NULL AS STRING), 'DIRECT', 'CartstartHsi', SAFE_CAST(visits AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_direct_hsi_cartstart_visits_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_direct_hsi_cartstart_visits_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'LTC_GROUPS', CAST(NULL AS STRING), 'DIRECT', 'CartstartByod', SAFE_CAST(visits AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_direct_byod_cartstart_visits_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_direct_byod_cartstart_visits_weekly_tmo`
  -- Orders Unassisted
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'LTC_GROUPS', CAST(NULL AS STRING), 'DIRECT', 'OrdersUnassistedPostpaid', SAFE_CAST(orders AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_direct_postpaid_order_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_direct_postpaid_order_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'LTC_GROUPS', CAST(NULL AS STRING), 'DIRECT', 'OrdersUnassistedHsi', SAFE_CAST(orders AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_direct_hsi_order_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_direct_hsi_order_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'LTC_GROUPS', CAST(NULL AS STRING), 'DIRECT', 'OrdersUnassistedByod', SAFE_CAST(orders AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_direct_byod_order_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_direct_byod_order_weekly_tmo`
  -- Orders Assisted (NEW)
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'LTC_GROUPS', CAST(NULL AS STRING), 'DIRECT', 'OrdersAssistedPostpaid', SAFE_CAST(orders AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_direct_postpaid_order_assisted_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_direct_postpaid_order_assisted_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'LTC_GROUPS', CAST(NULL AS STRING), 'DIRECT', 'OrdersAssistedHsi', SAFE_CAST(orders AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_direct_hsi_order_assisted_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_direct_hsi_order_assisted_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'LTC_GROUPS', CAST(NULL AS STRING), 'DIRECT', 'OrdersAssistedByod', SAFE_CAST(orders AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_direct_byod_order_assisted_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_direct_byod_order_assisted_weekly_tmo`

  /* ------------------------------------------------------------------ ORGANIC SEARCH ------------------------------------------------------------------ */
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'LTC_GROUPS', CAST(NULL AS STRING), 'ORGANIC SEARCH', 'UvnbPostpaid', SAFE_CAST(visitors AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_organic_search_uvnb_postpaid_flow_visitors_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_organic_search_uvnb_postpaid_flow_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'LTC_GROUPS', CAST(NULL AS STRING), 'ORGANIC SEARCH', 'UvnbHsi', SAFE_CAST(visitors AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_organic_search_uvnb_hsi_flow_visitors_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_organic_search_uvnb_hsi_flow_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'LTC_GROUPS', CAST(NULL AS STRING), 'ORGANIC SEARCH', 'UvnbByod', SAFE_CAST(visitors AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_organic_search_uvnb_byod_flow_visitors_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_organic_search_uvnb_byod_flow_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'LTC_GROUPS', CAST(NULL AS STRING), 'ORGANIC SEARCH', 'CartstartPostpaid', SAFE_CAST(visits AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_organic_search_postpaid_cartstart_visits_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_organic_search_postpaid_cartstart_visits_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'LTC_GROUPS', CAST(NULL AS STRING), 'ORGANIC SEARCH', 'CartstartHsi', SAFE_CAST(visits AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_organic_search_hsi_cartstart_visits_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_organic_search_hsi_cartstart_visits_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'LTC_GROUPS', CAST(NULL AS STRING), 'ORGANIC SEARCH', 'CartstartByod', SAFE_CAST(visits AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_organic_search_byod_cartstart_visits_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_organic_search_byod_cartstart_visits_weekly_tmo`
  -- Orders Unassisted
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'LTC_GROUPS', CAST(NULL AS STRING), 'ORGANIC SEARCH', 'OrdersUnassistedPostpaid', SAFE_CAST(orders AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_organic_search_postpaid_order_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_organic_search_postpaid_order_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'LTC_GROUPS', CAST(NULL AS STRING), 'ORGANIC SEARCH', 'OrdersUnassistedHsi', SAFE_CAST(orders AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_organic_search_hsi_order_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_organic_search_hsi_order_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'LTC_GROUPS', CAST(NULL AS STRING), 'ORGANIC SEARCH', 'OrdersUnassistedByod', SAFE_CAST(orders AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_organic_search_byod_order_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_organic_search_byod_order_weekly_tmo`
  -- Orders Assisted (NEW)
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'LTC_GROUPS', CAST(NULL AS STRING), 'ORGANIC SEARCH', 'OrdersAssistedPostpaid', SAFE_CAST(orders AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_organic_search_postpaid_order_assisted_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_organic_search_postpaid_order_assisted_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'LTC_GROUPS', CAST(NULL AS STRING), 'ORGANIC SEARCH', 'OrdersAssistedHsi', SAFE_CAST(orders AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_organic_search_hsi_order_assisted_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_organic_search_hsi_order_assisted_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'LTC_GROUPS', CAST(NULL AS STRING), 'ORGANIC SEARCH', 'OrdersAssistedByod', SAFE_CAST(orders AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_organic_search_byod_order_assisted_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_organic_search_byod_order_assisted_weekly_tmo`

  /* ------------------------------------------------------------------ OTHER ------------------------------------------------------------------ */
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'LTC_GROUPS', CAST(NULL AS STRING), 'OTHER', 'UvnbPostpaid', SAFE_CAST(visitors AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_other_uvnb_postpaid_flow_visitors_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_other_uvnb_postpaid_flow_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'LTC_GROUPS', CAST(NULL AS STRING), 'OTHER', 'UvnbHsi', SAFE_CAST(visitors AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_other_uvnb_hsi_flow_visitors_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_other_uvnb_hsi_flow_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'LTC_GROUPS', CAST(NULL AS STRING), 'OTHER', 'UvnbByod', SAFE_CAST(visitors AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_other_uvnb_byod_flow_visitors_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_other_uvnb_byod_flow_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'LTC_GROUPS', CAST(NULL AS STRING), 'OTHER', 'CartstartPostpaid', SAFE_CAST(visits AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_other_postpaid_cartstart_visits_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_other_postpaid_cartstart_visits_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'LTC_GROUPS', CAST(NULL AS STRING), 'OTHER', 'CartstartHsi', SAFE_CAST(visits AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_other_hsi_cartstart_visits_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_other_hsi_cartstart_visits_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'LTC_GROUPS', CAST(NULL AS STRING), 'OTHER', 'CartstartByod', SAFE_CAST(visits AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_other_byod_cartstart_visits_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_other_byod_cartstart_visits_weekly_tmo`
  -- Orders Unassisted
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'LTC_GROUPS', CAST(NULL AS STRING), 'OTHER', 'OrdersUnassistedPostpaid', SAFE_CAST(orders AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_other_postpaid_order_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_other_postpaid_order_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'LTC_GROUPS', CAST(NULL AS STRING), 'OTHER', 'OrdersUnassistedHsi', SAFE_CAST(orders AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_other_hsi_order_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_other_hsi_order_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'LTC_GROUPS', CAST(NULL AS STRING), 'OTHER', 'OrdersUnassistedByod', SAFE_CAST(orders AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_other_byod_order_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_other_byod_order_weekly_tmo`
  -- Orders Assisted (NEW)
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'LTC_GROUPS', CAST(NULL AS STRING), 'OTHER', 'OrdersAssistedPostpaid', SAFE_CAST(orders AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_other_postpaid_order_assisted_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_other_postpaid_order_assisted_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'LTC_GROUPS', CAST(NULL AS STRING), 'OTHER', 'OrdersAssistedHsi', SAFE_CAST(orders AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_other_hsi_order_assisted_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_other_hsi_order_assisted_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'LTC_GROUPS', CAST(NULL AS STRING), 'OTHER', 'OrdersAssistedByod', SAFE_CAST(orders AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_other_byod_order_assisted_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_other_byod_order_assisted_weekly_tmo`

  /* ------------------------------------------------------------------ PAID SEARCH ------------------------------------------------------------------ */
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'LTC_GROUPS', CAST(NULL AS STRING), 'PAID SEARCH', 'UvnbPostpaid', SAFE_CAST(visitors AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_paid_search_uvnb_postpaid_flow_visitors_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_paid_search_uvnb_postpaid_flow_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'LTC_GROUPS', CAST(NULL AS STRING), 'PAID SEARCH', 'UvnbHsi', SAFE_CAST(visitors AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_paid_search_uvnb_hsi_flow_visitors_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_paid_search_uvnb_hsi_flow_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'LTC_GROUPS', CAST(NULL AS STRING), 'PAID SEARCH', 'UvnbByod', SAFE_CAST(visitors AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_paid_search_uvnb_byod_flow_visitors_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_paid_search_uvnb_byod_flow_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'LTC_GROUPS', CAST(NULL AS STRING), 'PAID SEARCH', 'CartstartPostpaid', SAFE_CAST(visits AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_paid_search_postpaid_cartstart_visits_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_paid_search_postpaid_cartstart_visits_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'LTC_GROUPS', CAST(NULL AS STRING), 'PAID SEARCH', 'CartstartHsi', SAFE_CAST(visits AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_paid_search_hsi_cartstart_visits_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_paid_search_hsi_cartstart_visits_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'LTC_GROUPS', CAST(NULL AS STRING), 'PAID SEARCH', 'CartstartByod', SAFE_CAST(visits AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_paid_search_byod_cartstart_visits_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_paid_search_byod_cartstart_visits_weekly_tmo`
  -- Orders Unassisted
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'LTC_GROUPS', CAST(NULL AS STRING), 'PAID SEARCH', 'OrdersUnassistedPostpaid', SAFE_CAST(orders AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_paid_search_postpaid_order_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_paid_search_postpaid_order_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'LTC_GROUPS', CAST(NULL AS STRING), 'PAID SEARCH', 'OrdersUnassistedHsi', SAFE_CAST(orders AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_paid_search_hsi_order_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_paid_search_hsi_order_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'LTC_GROUPS', CAST(NULL AS STRING), 'PAID SEARCH', 'OrdersUnassistedByod', SAFE_CAST(orders AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_paid_search_byod_order_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_paid_search_byod_order_weekly_tmo`
  -- Orders Assisted (NEW)
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'LTC_GROUPS', CAST(NULL AS STRING), 'PAID SEARCH', 'OrdersAssistedPostpaid', SAFE_CAST(orders AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_paid_search_postpaid_order_assisted_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_paid_search_postpaid_order_assisted_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'LTC_GROUPS', CAST(NULL AS STRING), 'PAID SEARCH', 'OrdersAssistedHsi', SAFE_CAST(orders AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_paid_search_hsi_order_assisted_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_paid_search_hsi_order_assisted_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'LTC_GROUPS', CAST(NULL AS STRING), 'PAID SEARCH', 'OrdersAssistedByod', SAFE_CAST(orders AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_paid_search_byod_order_assisted_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_paid_search_byod_order_assisted_weekly_tmo`

  /* ------------------------------------------------------------------ PROGRAMMATIC ------------------------------------------------------------------ */
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'LTC_GROUPS', CAST(NULL AS STRING), 'PROGRAMMATIC', 'UvnbPostpaid', SAFE_CAST(visitors AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_programmatic_uvnb_postpaid_flow_visitors_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_programmatic_uvnb_postpaid_flow_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'LTC_GROUPS', CAST(NULL AS STRING), 'PROGRAMMATIC', 'UvnbHsi', SAFE_CAST(visitors AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_programmatic_uvnb_hsi_flow_visitors_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_programmatic_uvnb_hsi_flow_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'LTC_GROUPS', CAST(NULL AS STRING), 'PROGRAMMATIC', 'UvnbByod', SAFE_CAST(visitors AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_programmatic_uvnb_byod_flow_visitors_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_programmatic_uvnb_byod_flow_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'LTC_GROUPS', CAST(NULL AS STRING), 'PROGRAMMATIC', 'CartstartPostpaid', SAFE_CAST(visits AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_programmatic_postpaid_cartstart_visits_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_programmatic_postpaid_cartstart_visits_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'LTC_GROUPS', CAST(NULL AS STRING), 'PROGRAMMATIC', 'CartstartHsi', SAFE_CAST(visits AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_programmatic_hsi_cartstart_visits_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_programmatic_hsi_cartstart_visits_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'LTC_GROUPS', CAST(NULL AS STRING), 'PROGRAMMATIC', 'CartstartByod', SAFE_CAST(visits AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_programmatic_byod_cartstart_visits_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_programmatic_byod_cartstart_visits_weekly_tmo`
  -- Orders Unassisted
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'LTC_GROUPS', CAST(NULL AS STRING), 'PROGRAMMATIC', 'OrdersUnassistedPostpaid', SAFE_CAST(orders AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_programmatic_postpaid_order_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_programmatic_postpaid_order_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'LTC_GROUPS', CAST(NULL AS STRING), 'PROGRAMMATIC', 'OrdersUnassistedHsi', SAFE_CAST(orders AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_programmatic_hsi_order_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_programmatic_hsi_order_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'LTC_GROUPS', CAST(NULL AS STRING), 'PROGRAMMATIC', 'OrdersUnassistedByod', SAFE_CAST(orders AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_programmatic_byod_order_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_programmatic_byod_order_weekly_tmo`
  -- Orders Assisted (NEW)
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'LTC_GROUPS', CAST(NULL AS STRING), 'PROGRAMMATIC', 'OrdersAssistedPostpaid', SAFE_CAST(orders AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_programmatic_postpaid_order_assisted_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_programmatic_postpaid_order_assisted_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'LTC_GROUPS', CAST(NULL AS STRING), 'PROGRAMMATIC', 'OrdersAssistedHsi', SAFE_CAST(orders AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_programmatic_hsi_order_assisted_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_programmatic_hsi_order_assisted_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'LTC_GROUPS', CAST(NULL AS STRING), 'PROGRAMMATIC', 'OrdersAssistedByod', SAFE_CAST(orders AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_programmatic_byod_order_assisted_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_programmatic_byod_order_assisted_weekly_tmo`

  /* ------------------------------------------------------------------ SOCIAL ------------------------------------------------------------------ */
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'LTC_GROUPS', CAST(NULL AS STRING), 'SOCIAL', 'UvnbPostpaid', SAFE_CAST(visitors AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_social_uvnb_postpaid_flow_visitors_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_social_uvnb_postpaid_flow_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'LTC_GROUPS', CAST(NULL AS STRING), 'SOCIAL', 'UvnbHsi', SAFE_CAST(visitors AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_social_uvnb_hsi_flow_visitors_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_social_uvnb_hsi_flow_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'LTC_GROUPS', CAST(NULL AS STRING), 'SOCIAL', 'UvnbByod', SAFE_CAST(visitors AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_social_uvnb_byod_flow_visitors_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_social_uvnb_byod_flow_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'LTC_GROUPS', CAST(NULL AS STRING), 'SOCIAL', 'CartstartPostpaid', SAFE_CAST(visits AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_social_postpaid_cartstart_visits_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_social_postpaid_cartstart_visits_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'LTC_GROUPS', CAST(NULL AS STRING), 'SOCIAL', 'CartstartHsi', SAFE_CAST(visits AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_social_hsi_cartstart_visits_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_social_hsi_cartstart_visits_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'LTC_GROUPS', CAST(NULL AS STRING), 'SOCIAL', 'CartstartByod', SAFE_CAST(visits AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_social_byod_cartstart_visits_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_social_byod_cartstart_visits_weekly_tmo`
  -- Orders Unassisted
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'LTC_GROUPS', CAST(NULL AS STRING), 'SOCIAL', 'OrdersUnassistedPostpaid', SAFE_CAST(orders AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_social_postpaid_order_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_social_postpaid_order_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'LTC_GROUPS', CAST(NULL AS STRING), 'SOCIAL', 'OrdersUnassistedHsi', SAFE_CAST(orders AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_social_hsi_order_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_social_hsi_order_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'LTC_GROUPS', CAST(NULL AS STRING), 'SOCIAL', 'OrdersUnassistedByod', SAFE_CAST(orders AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_social_byod_order_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_social_byod_order_weekly_tmo`
  -- Orders Assisted (NEW)
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'LTC_GROUPS', CAST(NULL AS STRING), 'SOCIAL', 'OrdersAssistedPostpaid', SAFE_CAST(orders AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_social_postpaid_order_assisted_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_social_postpaid_order_assisted_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'LTC_GROUPS', CAST(NULL AS STRING), 'SOCIAL', 'OrdersAssistedHsi', SAFE_CAST(orders AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_social_hsi_order_assisted_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_social_hsi_order_assisted_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'LTC_GROUPS', CAST(NULL AS STRING), 'SOCIAL', 'OrdersAssistedByod', SAFE_CAST(orders AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_social_byod_order_assisted_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_social_byod_order_assisted_weekly_tmo`
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

  -- UVNB flows (unchanged)
  MAX(IF(MetricName = 'UvnbPostpaid',             MetricValue, NULL)) AS UvnbPostpaid,
  MAX(IF(MetricName = 'UvnbHsi',                  MetricValue, NULL)) AS UvnbHsi,
  MAX(IF(MetricName = 'UvnbByod',                 MetricValue, NULL)) AS UvnbByod,

  -- Cartstart (unchanged)
  MAX(IF(MetricName = 'CartstartPostpaid',         MetricValue, NULL)) AS CartstartPostpaid,
  MAX(IF(MetricName = 'CartstartHsi',             MetricValue, NULL)) AS CartstartHsi,
  MAX(IF(MetricName = 'CartstartByod',            MetricValue, NULL)) AS CartstartByod,

  -- Orders Unassisted (renamed from OrdersPostpaid/Hsi/Byod)
  MAX(IF(MetricName = 'OrdersUnassistedPostpaid', MetricValue, NULL)) AS OrdersUnassistedPostpaid,
  MAX(IF(MetricName = 'OrdersUnassistedHsi',      MetricValue, NULL)) AS OrdersUnassistedHsi,
  MAX(IF(MetricName = 'OrdersUnassistedByod',     MetricValue, NULL)) AS OrdersUnassistedByod,

  -- Orders Assisted (new)
  MAX(IF(MetricName = 'OrdersAssistedPostpaid',   MetricValue, NULL)) AS OrdersAssistedPostpaid,
  MAX(IF(MetricName = 'OrdersAssistedHsi',        MetricValue, NULL)) AS OrdersAssistedHsi,
  MAX(IF(MetricName = 'OrdersAssistedByod',       MetricValue, NULL)) AS OrdersAssistedByod,

  STRING_AGG(DISTINCT SourceTable, ', ' ORDER BY SourceTable) AS SourceTablesUsed,
  MAX(FileLoadDatetime) AS MaxFileLoadDatetime,
  STRING_AGG(DISTINCT Filename, ', ' ORDER BY Filename) AS FilenamesUsed

FROM Deduped
GROUP BY
  WeekSunSat,
  DataGranularity,
  LastTouchChannel,
  LtcGroup;