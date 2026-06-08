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

  metric_name values use snake_case here. camelCase conversion is applied
  in Silver during the unpivot step.

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
  SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY) AS week_sun_sat, 'ALL' AS channel_group, 'upv_postpaid' AS metric_name, SAFE_CAST(visitors AS FLOAT64) AS metric_value, 'sdi_raw_adobe_pp_uvnb_all_uvnb_postpaid_flow_visitors_weekly_tmo' AS source_table, __insert_date AS insert_date, File_Load_datetime AS file_load_datetime, Filename AS filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_all_uvnb_postpaid_flow_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'ALL', 'upv_hsi', SAFE_CAST(visitors AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_all_uvnb_hsi_flow_visitors_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_all_uvnb_hsi_flow_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'ALL', 'upv_byod', SAFE_CAST(visitors AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_all_uvnb_byod_flow_visitors_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_all_uvnb_byod_flow_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'ALL', 'upv_flow_total', SAFE_CAST(visitors AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_all_flow_total_visitors_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_all_flow_total_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'ALL', 'upv_total_adobe', SAFE_CAST(visitors AS FLOAT64), 'sdi_raw_pp_pro_uvnb_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_pp_pro_uvnb_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'ALL', 'cartstart_postpaid', SAFE_CAST(visits AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_all_postpaid_cartstart_visits_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_all_postpaid_cartstart_visits_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'ALL', 'cartstart_hsi', SAFE_CAST(visits AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_all_hsi_cartstart_visits_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_all_hsi_cartstart_visits_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'ALL', 'cartstart_byod', SAFE_CAST(visits AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_all_byod_cartstart_visits_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_all_byod_cartstart_visits_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'ALL', 'orders_unassisted_postpaid', SAFE_CAST(orders AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_all_postpaid_order_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_all_postpaid_order_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'ALL', 'orders_unassisted_hsi', SAFE_CAST(orders AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_all_hsi_order_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_all_hsi_order_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'ALL', 'orders_unassisted_byod', SAFE_CAST(orders AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_all_byod_order_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_all_byod_order_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'ALL', 'orders_assisted_postpaid', SAFE_CAST(orders AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_all_postpaid_order_assisted_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_all_postpaid_order_assisted_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'ALL', 'orders_assisted_hsi', SAFE_CAST(orders AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_all_hsi_order_assisted_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_all_hsi_order_assisted_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'ALL', 'orders_assisted_byod', SAFE_CAST(orders AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_all_byod_order_assisted_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_all_byod_order_assisted_weekly_tmo`

  /* ============================================================ DIRECT ============================================================ */
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'DIRECT', 'upv_postpaid', SAFE_CAST(visitors AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_direct_uvnb_postpaid_flow_visitors_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_direct_uvnb_postpaid_flow_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'DIRECT', 'upv_hsi', SAFE_CAST(visitors AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_direct_uvnb_hsi_flow_visitors_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_direct_uvnb_hsi_flow_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'DIRECT', 'upv_byod', SAFE_CAST(visitors AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_direct_uvnb_byod_flow_visitors_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_direct_uvnb_byod_flow_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'DIRECT', 'upv_flow_total', SAFE_CAST(visitors AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_direct_flow_total_visitors_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_direct_flow_total_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'DIRECT', 'upv_total_adobe', SAFE_CAST(visitors AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_direct_uvnb_visitors_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_direct_uvnb_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'DIRECT', 'cartstart_postpaid', SAFE_CAST(visits AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_direct_postpaid_cartstart_visits_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_direct_postpaid_cartstart_visits_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'DIRECT', 'cartstart_hsi', SAFE_CAST(visits AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_direct_hsi_cartstart_visits_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_direct_hsi_cartstart_visits_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'DIRECT', 'cartstart_byod', SAFE_CAST(visits AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_direct_byod_cartstart_visits_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_direct_byod_cartstart_visits_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'DIRECT', 'orders_unassisted_postpaid', SAFE_CAST(orders AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_direct_postpaid_order_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_direct_postpaid_order_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'DIRECT', 'orders_unassisted_hsi', SAFE_CAST(orders AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_direct_hsi_order_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_direct_hsi_order_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'DIRECT', 'orders_unassisted_byod', SAFE_CAST(orders AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_direct_byod_order_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_direct_byod_order_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'DIRECT', 'orders_assisted_postpaid', SAFE_CAST(orders AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_direct_postpaid_order_assisted_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_direct_postpaid_order_assisted_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'DIRECT', 'orders_assisted_hsi', SAFE_CAST(orders AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_direct_hsi_order_assisted_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_direct_hsi_order_assisted_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'DIRECT', 'orders_assisted_byod', SAFE_CAST(orders AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_direct_byod_order_assisted_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_direct_byod_order_assisted_weekly_tmo`

  /* ============================================================ ORGANIC SEARCH ============================================================ */
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'ORGANIC SEARCH', 'upv_postpaid', SAFE_CAST(visitors AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_organic_search_uvnb_postpaid_flow_visitors_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_organic_search_uvnb_postpaid_flow_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'ORGANIC SEARCH', 'upv_hsi', SAFE_CAST(visitors AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_organic_search_uvnb_hsi_flow_visitors_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_organic_search_uvnb_hsi_flow_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'ORGANIC SEARCH', 'upv_byod', SAFE_CAST(visitors AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_organic_search_uvnb_byod_flow_visitors_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_organic_search_uvnb_byod_flow_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'ORGANIC SEARCH', 'upv_flow_total', SAFE_CAST(visitors AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_organic_search_flow_total_visitors_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_organic_search_flow_total_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'ORGANIC SEARCH', 'upv_total_adobe', SAFE_CAST(visitors AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_organic_search_uvnb_visitors_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_organic_search_uvnb_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'ORGANIC SEARCH', 'cartstart_postpaid', SAFE_CAST(visits AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_organic_search_postpaid_cartstart_visits_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_organic_search_postpaid_cartstart_visits_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'ORGANIC SEARCH', 'cartstart_hsi', SAFE_CAST(visits AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_organic_search_hsi_cartstart_visits_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_organic_search_hsi_cartstart_visits_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'ORGANIC SEARCH', 'cartstart_byod', SAFE_CAST(visits AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_organic_search_byod_cartstart_visits_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_organic_search_byod_cartstart_visits_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'ORGANIC SEARCH', 'orders_unassisted_postpaid', SAFE_CAST(orders AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_organic_search_postpaid_order_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_organic_search_postpaid_order_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'ORGANIC SEARCH', 'orders_unassisted_hsi', SAFE_CAST(orders AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_organic_search_hsi_order_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_organic_search_hsi_order_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'ORGANIC SEARCH', 'orders_unassisted_byod', SAFE_CAST(orders AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_organic_search_byod_order_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_organic_search_byod_order_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'ORGANIC SEARCH', 'orders_assisted_postpaid', SAFE_CAST(orders AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_organic_search_postpaid_order_assisted_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_organic_search_postpaid_order_assisted_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'ORGANIC SEARCH', 'orders_assisted_hsi', SAFE_CAST(orders AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_organic_search_hsi_order_assisted_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_organic_search_hsi_order_assisted_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'ORGANIC SEARCH', 'orders_assisted_byod', SAFE_CAST(orders AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_organic_search_byod_order_assisted_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_organic_search_byod_order_assisted_weekly_tmo`

  /* ============================================================ OTHER ============================================================ */
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'OTHER', 'upv_postpaid', SAFE_CAST(visitors AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_other_uvnb_postpaid_flow_visitors_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_other_uvnb_postpaid_flow_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'OTHER', 'upv_hsi', SAFE_CAST(visitors AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_other_uvnb_hsi_flow_visitors_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_other_uvnb_hsi_flow_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'OTHER', 'upv_byod', SAFE_CAST(visitors AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_other_uvnb_byod_flow_visitors_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_other_uvnb_byod_flow_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'OTHER', 'upv_flow_total', SAFE_CAST(visitors AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_other_flow_total_visitors_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_other_flow_total_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'OTHER', 'upv_total_adobe', SAFE_CAST(visitors AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_other_uvnb_visitors_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_other_uvnb_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'OTHER', 'cartstart_postpaid', SAFE_CAST(visits AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_other_postpaid_cartstart_visits_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_other_postpaid_cartstart_visits_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'OTHER', 'cartstart_hsi', SAFE_CAST(visits AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_other_hsi_cartstart_visits_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_other_hsi_cartstart_visits_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'OTHER', 'cartstart_byod', SAFE_CAST(visits AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_other_byod_cartstart_visits_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_other_byod_cartstart_visits_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'OTHER', 'orders_unassisted_postpaid', SAFE_CAST(orders AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_other_postpaid_order_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_other_postpaid_order_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'OTHER', 'orders_unassisted_hsi', SAFE_CAST(orders AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_other_hsi_order_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_other_hsi_order_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'OTHER', 'orders_unassisted_byod', SAFE_CAST(orders AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_other_byod_order_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_other_byod_order_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'OTHER', 'orders_assisted_postpaid', SAFE_CAST(orders AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_other_postpaid_order_assisted_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_other_postpaid_order_assisted_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'OTHER', 'orders_assisted_hsi', SAFE_CAST(orders AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_other_hsi_order_assisted_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_other_hsi_order_assisted_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'OTHER', 'orders_assisted_byod', SAFE_CAST(orders AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_other_byod_order_assisted_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_other_byod_order_assisted_weekly_tmo`

  /* ============================================================ PAID SEARCH ============================================================ */
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'PAID SEARCH', 'upv_postpaid', SAFE_CAST(visitors AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_paid_search_uvnb_postpaid_flow_visitors_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_paid_search_uvnb_postpaid_flow_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'PAID SEARCH', 'upv_hsi', SAFE_CAST(visitors AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_paid_search_uvnb_hsi_flow_visitors_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_paid_search_uvnb_hsi_flow_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'PAID SEARCH', 'upv_byod', SAFE_CAST(visitors AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_paid_search_uvnb_byod_flow_visitors_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_paid_search_uvnb_byod_flow_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'PAID SEARCH', 'upv_flow_total', SAFE_CAST(visitors AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_paid_search_flow_total_visitors_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_paid_search_flow_total_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'PAID SEARCH', 'upv_total_adobe', SAFE_CAST(visitors AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_paid_search_uvnb_visitors_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_paid_search_uvnb_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'PAID SEARCH', 'cartstart_postpaid', SAFE_CAST(visits AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_paid_search_postpaid_cartstart_visits_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_paid_search_postpaid_cartstart_visits_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'PAID SEARCH', 'cartstart_hsi', SAFE_CAST(visits AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_paid_search_hsi_cartstart_visits_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_paid_search_hsi_cartstart_visits_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'PAID SEARCH', 'cartstart_byod', SAFE_CAST(visits AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_paid_search_byod_cartstart_visits_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_paid_search_byod_cartstart_visits_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'PAID SEARCH', 'orders_unassisted_postpaid', SAFE_CAST(orders AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_paid_search_postpaid_order_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_paid_search_postpaid_order_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'PAID SEARCH', 'orders_unassisted_hsi', SAFE_CAST(orders AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_paid_search_hsi_order_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_paid_search_hsi_order_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'PAID SEARCH', 'orders_unassisted_byod', SAFE_CAST(orders AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_paid_search_byod_order_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_paid_search_byod_order_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'PAID SEARCH', 'orders_assisted_postpaid', SAFE_CAST(orders AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_paid_search_postpaid_order_assisted_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_paid_search_postpaid_order_assisted_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'PAID SEARCH', 'orders_assisted_hsi', SAFE_CAST(orders AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_paid_search_hsi_order_assisted_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_paid_search_hsi_order_assisted_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'PAID SEARCH', 'orders_assisted_byod', SAFE_CAST(orders AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_paid_search_byod_order_assisted_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_paid_search_byod_order_assisted_weekly_tmo`

  /* ============================================================ PROGRAMMATIC ============================================================ */
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'PROGRAMMATIC', 'upv_postpaid', SAFE_CAST(visitors AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_programmatic_uvnb_postpaid_flow_visitors_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_programmatic_uvnb_postpaid_flow_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'PROGRAMMATIC', 'upv_hsi', SAFE_CAST(visitors AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_programmatic_uvnb_hsi_flow_visitors_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_programmatic_uvnb_hsi_flow_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'PROGRAMMATIC', 'upv_byod', SAFE_CAST(visitors AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_programmatic_uvnb_byod_flow_visitors_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_programmatic_uvnb_byod_flow_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'PROGRAMMATIC', 'upv_flow_total', SAFE_CAST(visitors AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_programmatic_flow_total_visitors_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_programmatic_flow_total_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'PROGRAMMATIC', 'upv_total_adobe', SAFE_CAST(visitors AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_programmatic_uvnb_visitors_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_programmatic_uvnb_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'PROGRAMMATIC', 'cartstart_postpaid', SAFE_CAST(visits AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_programmatic_postpaid_cartstart_visits_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_programmatic_postpaid_cartstart_visits_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'PROGRAMMATIC', 'cartstart_hsi', SAFE_CAST(visits AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_programmatic_hsi_cartstart_visits_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_programmatic_hsi_cartstart_visits_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'PROGRAMMATIC', 'cartstart_byod', SAFE_CAST(visits AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_programmatic_byod_cartstart_visits_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_programmatic_byod_cartstart_visits_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'PROGRAMMATIC', 'orders_unassisted_postpaid', SAFE_CAST(orders AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_programmatic_postpaid_order_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_programmatic_postpaid_order_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'PROGRAMMATIC', 'orders_unassisted_hsi', SAFE_CAST(orders AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_programmatic_hsi_order_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_programmatic_hsi_order_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'PROGRAMMATIC', 'orders_unassisted_byod', SAFE_CAST(orders AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_programmatic_byod_order_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_programmatic_byod_order_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'PROGRAMMATIC', 'orders_assisted_postpaid', SAFE_CAST(orders AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_programmatic_postpaid_order_assisted_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_programmatic_postpaid_order_assisted_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'PROGRAMMATIC', 'orders_assisted_hsi', SAFE_CAST(orders AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_programmatic_hsi_order_assisted_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_programmatic_hsi_order_assisted_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'PROGRAMMATIC', 'orders_assisted_byod', SAFE_CAST(orders AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_programmatic_byod_order_assisted_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_programmatic_byod_order_assisted_weekly_tmo`

  /* ============================================================ SOCIAL ============================================================ */
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'SOCIAL', 'upv_postpaid', SAFE_CAST(visitors AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_social_uvnb_postpaid_flow_visitors_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_social_uvnb_postpaid_flow_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'SOCIAL', 'upv_hsi', SAFE_CAST(visitors AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_social_uvnb_hsi_flow_visitors_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_social_uvnb_hsi_flow_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'SOCIAL', 'upv_byod', SAFE_CAST(visitors AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_social_uvnb_byod_flow_visitors_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_social_uvnb_byod_flow_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'SOCIAL', 'upv_flow_total', SAFE_CAST(visitors AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_social_flow_total_visitors_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_social_flow_total_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'SOCIAL', 'upv_total_adobe', SAFE_CAST(visitors AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_social_uvnb_visitors_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_social_uvnb_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'SOCIAL', 'cartstart_postpaid', SAFE_CAST(visits AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_social_postpaid_cartstart_visits_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_social_postpaid_cartstart_visits_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'SOCIAL', 'cartstart_hsi', SAFE_CAST(visits AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_social_hsi_cartstart_visits_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_social_hsi_cartstart_visits_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'SOCIAL', 'cartstart_byod', SAFE_CAST(visits AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_social_byod_cartstart_visits_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_social_byod_cartstart_visits_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'SOCIAL', 'orders_unassisted_postpaid', SAFE_CAST(orders AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_social_postpaid_order_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_social_postpaid_order_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'SOCIAL', 'orders_unassisted_hsi', SAFE_CAST(orders AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_social_hsi_order_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_social_hsi_order_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'SOCIAL', 'orders_unassisted_byod', SAFE_CAST(orders AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_social_byod_order_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_social_byod_order_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'SOCIAL', 'orders_assisted_postpaid', SAFE_CAST(orders AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_social_postpaid_order_assisted_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_social_postpaid_order_assisted_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'SOCIAL', 'orders_assisted_hsi', SAFE_CAST(orders AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_social_hsi_order_assisted_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_social_hsi_order_assisted_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'SOCIAL', 'orders_assisted_byod', SAFE_CAST(orders AS FLOAT64), 'sdi_raw_adobe_pp_uvnb_social_byod_order_assisted_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_social_byod_order_assisted_weekly_tmo`
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
  MAX(IF(metric_name = 'upv_postpaid',               metric_value, NULL)) AS upv_postpaid,
  MAX(IF(metric_name = 'upv_hsi',                    metric_value, NULL)) AS upv_hsi,
  MAX(IF(metric_name = 'upv_byod',                   metric_value, NULL)) AS upv_byod,
  MAX(IF(metric_name = 'upv_flow_total',             metric_value, NULL)) AS upv_flow_total,
  MAX(IF(metric_name = 'upv_total_adobe',            metric_value, NULL)) AS upv_total_adobe,
  MAX(IF(metric_name = 'cartstart_postpaid',         metric_value, NULL)) AS cartstart_postpaid,
  MAX(IF(metric_name = 'cartstart_hsi',              metric_value, NULL)) AS cartstart_hsi,
  MAX(IF(metric_name = 'cartstart_byod',             metric_value, NULL)) AS cartstart_byod,
  MAX(IF(metric_name = 'orders_unassisted_postpaid', metric_value, NULL)) AS orders_unassisted_postpaid,
  MAX(IF(metric_name = 'orders_unassisted_hsi',      metric_value, NULL)) AS orders_unassisted_hsi,
  MAX(IF(metric_name = 'orders_unassisted_byod',     metric_value, NULL)) AS orders_unassisted_byod,
  MAX(IF(metric_name = 'orders_assisted_postpaid',   metric_value, NULL)) AS orders_assisted_postpaid,
  MAX(IF(metric_name = 'orders_assisted_hsi',        metric_value, NULL)) AS orders_assisted_hsi,
  MAX(IF(metric_name = 'orders_assisted_byod',       metric_value, NULL)) AS orders_assisted_byod,
  STRING_AGG(DISTINCT source_table, ', ' ORDER BY source_table)           AS source_tables_used,
  MAX(file_load_datetime)                                                  AS max_file_load_datetime,
  STRING_AGG(DISTINCT filename,     ', ' ORDER BY filename)               AS filenames_used
FROM Deduped
GROUP BY week_sun_sat, channel_group
;