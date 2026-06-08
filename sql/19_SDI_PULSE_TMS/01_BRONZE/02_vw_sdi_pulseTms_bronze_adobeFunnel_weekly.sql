/* =================================================================================================
FILE:         02_vw_sdi_pulseTms_bronze_adobeFunnel_weekly.sql
LAYER:        Bronze View
DATASET:      prj-dbi-prd-1.ds_dbi_digitalmedia_automation
VIEW NAME:    vw_sdi_pulseTms_bronze_adobeFunnel_weekly

RAW SOURCES:
  -- ALL_CHANNELS grain:
  prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_all_uvnb_postpaid_flow_visitors_weekly_tmo
  prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_all_uvnb_hsi_flow_visitors_weekly_tmo
  prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_all_uvnb_byod_flow_visitors_weekly_tmo
  prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_all_flow_total_visitors_weekly_tmo
  prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_pp_pro_uvnb_weekly_tmo
  prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_all_postpaid_cartstart_visits_weekly_tmo
  prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_all_hsi_cartstart_visits_weekly_tmo
  prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_all_byod_cartstart_visits_weekly_tmo
  prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_all_postpaid_order_weekly_tmo
  prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_all_hsi_order_weekly_tmo
  prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_all_byod_order_weekly_tmo
  prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_all_postpaid_order_assisted_weekly_tmo
  prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_all_hsi_order_assisted_weekly_tmo
  prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_all_byod_order_assisted_weekly_tmo

  -- LTC_GROUPS grain ({group} = direct | organic_search | other | paid_search | programmatic | social):
  prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_{group}_uvnb_postpaid_flow_visitors_weekly_tmo
  prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_{group}_uvnb_hsi_flow_visitors_weekly_tmo
  prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_{group}_uvnb_byod_flow_visitors_weekly_tmo
  prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_{group}_flow_total_visitors_weekly_tmo
  prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_{group}_uvnb_visitors_weekly_tmo
  prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_{group}_postpaid_cartstart_visits_weekly_tmo
  prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_{group}_hsi_cartstart_visits_weekly_tmo
  prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_{group}_byod_cartstart_visits_weekly_tmo
  prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_{group}_postpaid_order_weekly_tmo
  prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_{group}_hsi_order_weekly_tmo
  prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_{group}_byod_order_weekly_tmo
  prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_{group}_postpaid_order_assisted_weekly_tmo
  prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_{group}_hsi_order_assisted_weekly_tmo
  prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_{group}_byod_order_assisted_weekly_tmo

DESTINATION:
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseTms_bronze_adobeFunnel_weekly

PURPOSE:
  Source-close Bronze view for Adobe UPV funnel metrics in the PulseTMS pipeline.
  Reads directly from raw Improvado tables.

  Produces one wide row per week_sun_sat x channel_group with all funnel metrics
  as named columns. No QGP date logic, no camelCase conversion, no WoW/YoY —
  all of that happens in Silver.

  metric_name values use camelCase throughout (Bronze → Silver → Gold).
  No conversion needed in Silver — metric names flow through as-is.

BUSINESS GRAIN:
  One row per: week_sun_sat x channel_group

CHANNEL GROUPS (raw values, passed through as-is):
  ALL, DIRECT, ORGANIC SEARCH, PAID SEARCH, PROGRAMMATIC, SOCIAL, OTHER
  'ALL' is renamed to 'All Channels' in Silver.

DATE CONVENTION:
  week_sun_sat = date_yyyymmdd (week-starting Sunday) + 6 days = week-ending Saturday.
  QGP alignment handled in Silver.

DEDUPLICATION:
  Each raw source deduplicated at week_sun_sat x channel_group x metric_name grain
  using: File_Load_datetime DESC, Filename DESC, __insert_date DESC.

BUSINESS RULES:
  - upv_flow_total: sourced directly from Adobe flow total tables (NOT sum of LOBs).
  - upv_total_adobe: from separate total UVNB raw table (NOT computed from flows).
  - NULL metric values remain NULL — no COALESCE.
  - Derived totals computed in Silver, not here.

DOWNSTREAM:
  05_vw_sdi_pulseTms_silver_adobeFunnel_weekly
================================================================================================= */

CREATE OR REPLACE VIEW
  `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseTms_bronze_adobeFunnel_weekly`
AS

WITH RawUnion AS (

  /* ============================================================ ALL ============================================================ */
  SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY) AS week_sun_sat, 'ALL' AS channel_group, 'upvPostpaid' AS metric_name, SAFE_CAST(visitors AS FLOAT64) AS metric_value, 'sdi_raw_adobe_pp_uvnb_all_uvnb_postpaid_flow_visitors_weekly_tmo' AS source_table, __insert_date AS insert_date, File_Load_datetime AS file_load_datetime, Filename AS filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_all_uvnb_postpaid_flow_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'ALL', 'upvHsi', SAFE_CAST(visitors AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_all_uvnb_hsi_flow_visitors_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_all_uvnb_hsi_flow_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'ALL', 'upvByod', SAFE_CAST(visitors AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_all_uvnb_byod_flow_visitors_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_all_uvnb_byod_flow_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'ALL', 'upvFlowTotal', SAFE_CAST(visitors AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_all_flow_total_visitors_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_all_flow_total_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'ALL', 'upvTotalAdobe', SAFE_CAST(visitors AS FLOAT64), 'sdi_raw_pp_pro_uvnb_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_pp_pro_uvnb_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'ALL', 'cartstartPostpaid', SAFE_CAST(visits AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_all_postpaid_cartstart_visits_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_all_postpaid_cartstart_visits_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'ALL', 'cartstartHsi', SAFE_CAST(visits AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_all_hsi_cartstart_visits_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_all_hsi_cartstart_visits_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'ALL', 'cartstartByod', SAFE_CAST(visits AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_all_byod_cartstart_visits_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_all_byod_cartstart_visits_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'ALL', 'ordersUnassistedPostpaid', SAFE_CAST(orders AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_all_postpaid_order_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_all_postpaid_order_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'ALL', 'ordersUnassistedHsi', SAFE_CAST(orders AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_all_hsi_order_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_all_hsi_order_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'ALL', 'ordersUnassistedByod', SAFE_CAST(orders AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_all_byod_order_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_all_byod_order_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'ALL', 'ordersAssistedPostpaid', SAFE_CAST(orders AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_all_postpaid_order_assisted_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_all_postpaid_order_assisted_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'ALL', 'ordersAssistedHsi', SAFE_CAST(orders AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_all_hsi_order_assisted_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_all_hsi_order_assisted_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'ALL', 'ordersAssistedByod', SAFE_CAST(orders AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_all_byod_order_assisted_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_all_byod_order_assisted_weekly_tmo`

  /* ============================================================ DIRECT ============================================================ */
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'DIRECT', 'upvPostpaid', SAFE_CAST(visitors AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_direct_uvnb_postpaid_flow_visitors_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_direct_uvnb_postpaid_flow_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'DIRECT', 'upvHsi', SAFE_CAST(visitors AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_direct_uvnb_hsi_flow_visitors_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_direct_uvnb_hsi_flow_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'DIRECT', 'upvByod', SAFE_CAST(visitors AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_direct_uvnb_byod_flow_visitors_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_direct_uvnb_byod_flow_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'DIRECT', 'upvFlowTotal', SAFE_CAST(visitors AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_direct_flow_total_visitors_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_direct_flow_total_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'DIRECT', 'upvTotalAdobe', SAFE_CAST(visitors AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_direct_uvnb_visitors_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_direct_uvnb_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'DIRECT', 'cartstartPostpaid', SAFE_CAST(visits AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_direct_postpaid_cartstart_visits_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_direct_postpaid_cartstart_visits_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'DIRECT', 'cartstartHsi', SAFE_CAST(visits AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_direct_hsi_cartstart_visits_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_direct_hsi_cartstart_visits_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'DIRECT', 'cartstartByod', SAFE_CAST(visits AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_direct_byod_cartstart_visits_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_direct_byod_cartstart_visits_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'DIRECT', 'ordersUnassistedPostpaid', SAFE_CAST(orders AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_direct_postpaid_order_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_direct_postpaid_order_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'DIRECT', 'ordersUnassistedHsi', SAFE_CAST(orders AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_direct_hsi_order_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_direct_hsi_order_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'DIRECT', 'ordersUnassistedByod', SAFE_CAST(orders AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_direct_byod_order_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_direct_byod_order_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'DIRECT', 'ordersAssistedPostpaid', SAFE_CAST(orders AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_direct_postpaid_order_assisted_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_direct_postpaid_order_assisted_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'DIRECT', 'ordersAssistedHsi', SAFE_CAST(orders AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_direct_hsi_order_assisted_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_direct_hsi_order_assisted_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'DIRECT', 'ordersAssistedByod', SAFE_CAST(orders AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_direct_byod_order_assisted_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_direct_byod_order_assisted_weekly_tmo`

  /* ============================================================ ORGANIC SEARCH ============================================================ */
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'ORGANIC SEARCH', 'upvPostpaid', SAFE_CAST(visitors AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_organic_search_uvnb_postpaid_flow_visitors_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_organic_search_uvnb_postpaid_flow_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'ORGANIC SEARCH', 'upvHsi', SAFE_CAST(visitors AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_organic_search_uvnb_hsi_flow_visitors_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_organic_search_uvnb_hsi_flow_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'ORGANIC SEARCH', 'upvByod', SAFE_CAST(visitors AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_organic_search_uvnb_byod_flow_visitors_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_organic_search_uvnb_byod_flow_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'ORGANIC SEARCH', 'upvFlowTotal', SAFE_CAST(visitors AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_organic_search_flow_total_visitors_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_organic_search_flow_total_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'ORGANIC SEARCH', 'upvTotalAdobe', SAFE_CAST(visitors AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_organic_search_uvnb_visitors_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_organic_search_uvnb_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'ORGANIC SEARCH', 'cartstartPostpaid', SAFE_CAST(visits AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_organic_search_postpaid_cartstart_visits_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_organic_search_postpaid_cartstart_visits_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'ORGANIC SEARCH', 'cartstartHsi', SAFE_CAST(visits AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_organic_search_hsi_cartstart_visits_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_organic_search_hsi_cartstart_visits_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'ORGANIC SEARCH', 'cartstartByod', SAFE_CAST(visits AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_organic_search_byod_cartstart_visits_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_organic_search_byod_cartstart_visits_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'ORGANIC SEARCH', 'ordersUnassistedPostpaid', SAFE_CAST(orders AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_organic_search_postpaid_order_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_organic_search_postpaid_order_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'ORGANIC SEARCH', 'ordersUnassistedHsi', SAFE_CAST(orders AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_organic_search_hsi_order_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_organic_search_hsi_order_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'ORGANIC SEARCH', 'ordersUnassistedByod', SAFE_CAST(orders AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_organic_search_byod_order_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_organic_search_byod_order_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'ORGANIC SEARCH', 'ordersAssistedPostpaid', SAFE_CAST(orders AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_organic_search_postpaid_order_assisted_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_organic_search_postpaid_order_assisted_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'ORGANIC SEARCH', 'ordersAssistedHsi', SAFE_CAST(orders AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_organic_search_hsi_order_assisted_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_organic_search_hsi_order_assisted_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'ORGANIC SEARCH', 'ordersAssistedByod', SAFE_CAST(orders AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_organic_search_byod_order_assisted_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_organic_search_byod_order_assisted_weekly_tmo`

  /* ============================================================ OTHER ============================================================ */
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'OTHER', 'upvPostpaid', SAFE_CAST(visitors AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_other_uvnb_postpaid_flow_visitors_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_other_uvnb_postpaid_flow_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'OTHER', 'upvHsi', SAFE_CAST(visitors AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_other_uvnb_hsi_flow_visitors_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_other_uvnb_hsi_flow_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'OTHER', 'upvByod', SAFE_CAST(visitors AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_other_uvnb_byod_flow_visitors_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_other_uvnb_byod_flow_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'OTHER', 'upvFlowTotal', SAFE_CAST(visitors AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_other_flow_total_visitors_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_other_flow_total_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'OTHER', 'upvTotalAdobe', SAFE_CAST(visitors AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_other_uvnb_visitors_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_other_uvnb_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'OTHER', 'cartstartPostpaid', SAFE_CAST(visits AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_other_postpaid_cartstart_visits_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_other_postpaid_cartstart_visits_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'OTHER', 'cartstartHsi', SAFE_CAST(visits AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_other_hsi_cartstart_visits_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_other_hsi_cartstart_visits_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'OTHER', 'cartstartByod', SAFE_CAST(visits AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_other_byod_cartstart_visits_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_other_byod_cartstart_visits_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'OTHER', 'ordersUnassistedPostpaid', SAFE_CAST(orders AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_other_postpaid_order_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_other_postpaid_order_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'OTHER', 'ordersUnassistedHsi', SAFE_CAST(orders AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_other_hsi_order_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_other_hsi_order_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'OTHER', 'ordersUnassistedByod', SAFE_CAST(orders AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_other_byod_order_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_other_byod_order_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'OTHER', 'ordersAssistedPostpaid', SAFE_CAST(orders AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_other_postpaid_order_assisted_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_other_postpaid_order_assisted_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'OTHER', 'ordersAssistedHsi', SAFE_CAST(orders AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_other_hsi_order_assisted_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_other_hsi_order_assisted_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'OTHER', 'ordersAssistedByod', SAFE_CAST(orders AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_other_byod_order_assisted_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_other_byod_order_assisted_weekly_tmo`

  /* ============================================================ PAID SEARCH ============================================================ */
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'PAID SEARCH', 'upvPostpaid', SAFE_CAST(visitors AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_paid_search_uvnb_postpaid_flow_visitors_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_paid_search_uvnb_postpaid_flow_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'PAID SEARCH', 'upvHsi', SAFE_CAST(visitors AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_paid_search_uvnb_hsi_flow_visitors_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_paid_search_uvnb_hsi_flow_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'PAID SEARCH', 'upvByod', SAFE_CAST(visitors AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_paid_search_uvnb_byod_flow_visitors_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_paid_search_uvnb_byod_flow_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'PAID SEARCH', 'upvFlowTotal', SAFE_CAST(visitors AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_paid_search_flow_total_visitors_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_paid_search_flow_total_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'PAID SEARCH', 'upvTotalAdobe', SAFE_CAST(visitors AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_paid_search_uvnb_visitors_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_paid_search_uvnb_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'PAID SEARCH', 'cartstartPostpaid', SAFE_CAST(visits AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_paid_search_postpaid_cartstart_visits_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_paid_search_postpaid_cartstart_visits_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'PAID SEARCH', 'cartstartHsi', SAFE_CAST(visits AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_paid_search_hsi_cartstart_visits_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_paid_search_hsi_cartstart_visits_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'PAID SEARCH', 'cartstartByod', SAFE_CAST(visits AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_paid_search_byod_cartstart_visits_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_paid_search_byod_cartstart_visits_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'PAID SEARCH', 'ordersUnassistedPostpaid', SAFE_CAST(orders AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_paid_search_postpaid_order_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_paid_search_postpaid_order_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'PAID SEARCH', 'ordersUnassistedHsi', SAFE_CAST(orders AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_paid_search_hsi_order_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_paid_search_hsi_order_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'PAID SEARCH', 'ordersUnassistedByod', SAFE_CAST(orders AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_paid_search_byod_order_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_paid_search_byod_order_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'PAID SEARCH', 'ordersAssistedPostpaid', SAFE_CAST(orders AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_paid_search_postpaid_order_assisted_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_paid_search_postpaid_order_assisted_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'PAID SEARCH', 'ordersAssistedHsi', SAFE_CAST(orders AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_paid_search_hsi_order_assisted_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_paid_search_hsi_order_assisted_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'PAID SEARCH', 'ordersAssistedByod', SAFE_CAST(orders AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_paid_search_byod_order_assisted_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_paid_search_byod_order_assisted_weekly_tmo`

  /* ============================================================ PROGRAMMATIC ============================================================ */
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'PROGRAMMATIC', 'upvPostpaid', SAFE_CAST(visitors AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_programmatic_uvnb_postpaid_flow_visitors_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_programmatic_uvnb_postpaid_flow_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'PROGRAMMATIC', 'upvHsi', SAFE_CAST(visitors AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_programmatic_uvnb_hsi_flow_visitors_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_programmatic_uvnb_hsi_flow_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'PROGRAMMATIC', 'upvByod', SAFE_CAST(visitors AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_programmatic_uvnb_byod_flow_visitors_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_programmatic_uvnb_byod_flow_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'PROGRAMMATIC', 'upvFlowTotal', SAFE_CAST(visitors AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_programmatic_flow_total_visitors_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_programmatic_flow_total_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'PROGRAMMATIC', 'upvTotalAdobe', SAFE_CAST(visitors AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_programmatic_uvnb_visitors_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_programmatic_uvnb_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'PROGRAMMATIC', 'cartstartPostpaid', SAFE_CAST(visits AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_programmatic_postpaid_cartstart_visits_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_programmatic_postpaid_cartstart_visits_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'PROGRAMMATIC', 'cartstartHsi', SAFE_CAST(visits AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_programmatic_hsi_cartstart_visits_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_programmatic_hsi_cartstart_visits_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'PROGRAMMATIC', 'cartstartByod', SAFE_CAST(visits AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_programmatic_byod_cartstart_visits_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_programmatic_byod_cartstart_visits_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'PROGRAMMATIC', 'ordersUnassistedPostpaid', SAFE_CAST(orders AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_programmatic_postpaid_order_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_programmatic_postpaid_order_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'PROGRAMMATIC', 'ordersUnassistedHsi', SAFE_CAST(orders AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_programmatic_hsi_order_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_programmatic_hsi_order_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'PROGRAMMATIC', 'ordersUnassistedByod', SAFE_CAST(orders AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_programmatic_byod_order_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_programmatic_byod_order_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'PROGRAMMATIC', 'ordersAssistedPostpaid', SAFE_CAST(orders AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_programmatic_postpaid_order_assisted_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_programmatic_postpaid_order_assisted_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'PROGRAMMATIC', 'ordersAssistedHsi', SAFE_CAST(orders AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_programmatic_hsi_order_assisted_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_programmatic_hsi_order_assisted_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'PROGRAMMATIC', 'ordersAssistedByod', SAFE_CAST(orders AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_programmatic_byod_order_assisted_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_programmatic_byod_order_assisted_weekly_tmo`

  /* ============================================================ SOCIAL ============================================================ */
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'SOCIAL', 'upvPostpaid', SAFE_CAST(visitors AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_social_uvnb_postpaid_flow_visitors_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_social_uvnb_postpaid_flow_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'SOCIAL', 'upvHsi', SAFE_CAST(visitors AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_social_uvnb_hsi_flow_visitors_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_social_uvnb_hsi_flow_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'SOCIAL', 'upvByod', SAFE_CAST(visitors AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_social_uvnb_byod_flow_visitors_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_social_uvnb_byod_flow_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'SOCIAL', 'upvFlowTotal', SAFE_CAST(visitors AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_social_flow_total_visitors_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_social_flow_total_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'SOCIAL', 'upvTotalAdobe', SAFE_CAST(visitors AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_social_uvnb_visitors_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_social_uvnb_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'SOCIAL', 'cartstartPostpaid', SAFE_CAST(visits AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_social_postpaid_cartstart_visits_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_social_postpaid_cartstart_visits_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'SOCIAL', 'cartstartHsi', SAFE_CAST(visits AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_social_hsi_cartstart_visits_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_social_hsi_cartstart_visits_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'SOCIAL', 'cartstartByod', SAFE_CAST(visits AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_social_byod_cartstart_visits_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_social_byod_cartstart_visits_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'SOCIAL', 'ordersUnassistedPostpaid', SAFE_CAST(orders AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_social_postpaid_order_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_social_postpaid_order_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'SOCIAL', 'ordersUnassistedHsi', SAFE_CAST(orders AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_social_hsi_order_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_social_hsi_order_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'SOCIAL', 'ordersUnassistedByod', SAFE_CAST(orders AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_social_byod_order_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_social_byod_order_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'SOCIAL', 'ordersAssistedPostpaid', SAFE_CAST(orders AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_social_postpaid_order_assisted_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_social_postpaid_order_assisted_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'SOCIAL', 'ordersAssistedHsi', SAFE_CAST(orders AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_social_hsi_order_assisted_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_social_hsi_order_assisted_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'SOCIAL', 'ordersAssistedByod', SAFE_CAST(orders AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_social_byod_order_assisted_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_social_byod_order_assisted_weekly_tmo`
),

Deduped AS (
  SELECT *
  FROM RawUnion
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY week_sun_sat, channel_group, metric_name, source_table
    ORDER BY file_load_datetime DESC, filename DESC, insert_date DESC
  ) = 1
)

SELECT
  week_sun_sat,
  channel_group,
  MAX(IF(metric_name = 'upvPostpaid',               metric_value, NULL)) AS upvPostpaid,
  MAX(IF(metric_name = 'upvHsi',                    metric_value, NULL)) AS upvHsi,
  MAX(IF(metric_name = 'upvByod',                   metric_value, NULL)) AS upvByod,
  MAX(IF(metric_name = 'upvFlowTotal',             metric_value, NULL)) AS upvFlowTotal,
  MAX(IF(metric_name = 'upvTotalAdobe',            metric_value, NULL)) AS upvTotalAdobe,
  MAX(IF(metric_name = 'cartstartPostpaid',         metric_value, NULL)) AS cartstartPostpaid,
  MAX(IF(metric_name = 'cartstartHsi',              metric_value, NULL)) AS cartstartHsi,
  MAX(IF(metric_name = 'cartstartByod',             metric_value, NULL)) AS cartstartByod,
  MAX(IF(metric_name = 'ordersUnassistedPostpaid', metric_value, NULL)) AS ordersUnassistedPostpaid,
  MAX(IF(metric_name = 'ordersUnassistedHsi',      metric_value, NULL)) AS ordersUnassistedHsi,
  MAX(IF(metric_name = 'ordersUnassistedByod',     metric_value, NULL)) AS ordersUnassistedByod,
  MAX(IF(metric_name = 'ordersAssistedPostpaid',   metric_value, NULL)) AS ordersAssistedPostpaid,
  MAX(IF(metric_name = 'ordersAssistedHsi',        metric_value, NULL)) AS ordersAssistedHsi,
  MAX(IF(metric_name = 'ordersAssistedByod',       metric_value, NULL)) AS ordersAssistedByod,
  STRING_AGG(DISTINCT source_table, ', ' ORDER BY source_table)           AS source_tables_used,
  MAX(file_load_datetime)                                                  AS max_file_load_datetime,
  STRING_AGG(DISTINCT filename,     ', ' ORDER BY filename)               AS filenames_used
FROM Deduped
GROUP BY week_sun_sat, channel_group
;