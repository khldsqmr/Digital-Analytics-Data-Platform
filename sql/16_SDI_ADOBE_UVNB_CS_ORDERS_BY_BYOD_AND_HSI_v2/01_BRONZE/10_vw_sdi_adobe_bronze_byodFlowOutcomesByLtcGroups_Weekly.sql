/* =================================================================================================
FILE:         10_vw_sdi_adobe_bronze_byodFlowOutcomesByLtcGroups_Weekly.sql
LAYER:        Bronze View
DATASET:      prj-dbi-prd-1.ds_dbi_digitalmedia_automation
VIEW:         vw_sdi_adobe_bronze_byodFlowOutcomesByLtcGroups_Weekly

SOURCES (36 tables — 6 channel groups × 6 outcome metrics):
  Channel groups: PAID SEARCH, ORGANIC SEARCH, DIRECT, PROGRAMMATIC, SOCIAL, OTHER
  Metrics per group:
    byod_vr_chat_visitors, byod_call_visitors, byod_store_locator_visitors,
    byod_internaltmo_visitors, byod_bouncers_visitors, byod_orders

DESTINATION:
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_adobe_bronze_byodFlowOutcomesByLtcGroups_Weekly

PURPOSE:
  Bronze view for BYOD funnel outcome metrics at LTC_GROUPS granularity.
  Structurally mirrors Bronze 09 but broken out by channel group.
  Same 6 outcome metrics per channel group.
  ByodOrders uses the `orders` column; all other metrics use `visitors`.

BUSINESS GRAIN:
  One row per:
      WeekSunSat
      ChannelGroup

BUSINESS RULES:
  - WeekSunSat is derived from date_yyyymmdd by adding 6 days (raw is week-starting Sunday)
  - ChannelGroup is hardcoded per source table family
  - Visitor metrics: SAFE_CAST(visitors AS FLOAT64)
  - Orders metric:   SAFE_CAST(orders AS FLOAT64)
  - Missing metric values remain NULL — no fake zeroes
  - MetricName / MetricValue used internally only, not exposed in final output

KEY DEDUPE RULE:
  Deduplicate each source table at weekly + ChannelGroup grain using latest:
    File_Load_datetime DESC
    Filename DESC
    __insert_date DESC

DOWNSTREAM:
  Silver 04: vw_sdi_adobe_silver_byodFlowOutcomesByChannelGroupsPlusAll_Weekly
================================================================================================= */

CREATE OR REPLACE VIEW
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_adobe_bronze_byodFlowOutcomesByLtcGroups_Weekly`
AS

WITH RawUnion AS (

  /* ------------------------------------------------------------------ PAID SEARCH ------------------------------------------------------------------ */
  SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY) AS WeekSunSat, 'PAID SEARCH' AS ChannelGroup, 'ByodVrChatVisitors'      AS MetricName, SAFE_CAST(visitors AS FLOAT64) AS MetricValue, 'sdi_raw_adobe_byod_flow_paid_search_byod_vr_chat_visitors_weekly_tmo'       AS SourceTable, __insert_date AS InsertDate, File_Load_datetime AS FileLoadDatetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_byod_flow_paid_search_byod_vr_chat_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'PAID SEARCH', 'ByodCallVisitors',         SAFE_CAST(visitors AS FLOAT64), 'sdi_raw_adobe_byod_flow_paid_search_byod_call_visitors_weekly_tmo',          __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_byod_flow_paid_search_byod_call_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'PAID SEARCH', 'ByodStoreLocatorVisitors', SAFE_CAST(visitors AS FLOAT64), 'sdi_raw_adobe_byod_flow_paid_search_byod_store_locator_visitors_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_byod_flow_paid_search_byod_store_locator_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'PAID SEARCH', 'ByodInternalTmoVisitors',  SAFE_CAST(visitors AS FLOAT64), 'sdi_raw_adobe_byod_flow_paid_search_byod_internaltmo_visitors_weekly_tmo',   __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_byod_flow_paid_search_byod_internaltmo_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'PAID SEARCH', 'ByodBouncersVisitors',     SAFE_CAST(visitors AS FLOAT64), 'sdi_raw_adobe_byod_flow_paid_search_byod_bouncers_visitors_weekly_tmo',      __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_byod_flow_paid_search_byod_bouncers_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'PAID SEARCH', 'ByodOrders',               SAFE_CAST(orders   AS FLOAT64), 'sdi_raw_adobe_byod_flow_paid_search_byod_orders_weekly_tmo',                __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_byod_flow_paid_search_byod_orders_weekly_tmo`

  /* ------------------------------------------------------------------ ORGANIC SEARCH ------------------------------------------------------------------ */
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'ORGANIC SEARCH', 'ByodVrChatVisitors',      SAFE_CAST(visitors AS FLOAT64), 'sdi_raw_adobe_byod_flow_organic_search_byod_vr_chat_visitors_weekly_tmo',       __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_byod_flow_organic_search_byod_vr_chat_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'ORGANIC SEARCH', 'ByodCallVisitors',         SAFE_CAST(visitors AS FLOAT64), 'sdi_raw_adobe_byod_flow_organic_search_byod_call_visitors_weekly_tmo',          __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_byod_flow_organic_search_byod_call_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'ORGANIC SEARCH', 'ByodStoreLocatorVisitors', SAFE_CAST(visitors AS FLOAT64), 'sdi_raw_adobe_byod_flow_organic_search_byod_store_locator_visitors_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_byod_flow_organic_search_byod_store_locator_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'ORGANIC SEARCH', 'ByodInternalTmoVisitors',  SAFE_CAST(visitors AS FLOAT64), 'sdi_raw_adobe_byod_flow_organic_search_byod_internaltmo_visitors_weekly_tmo',   __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_byod_flow_organic_search_byod_internaltmo_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'ORGANIC SEARCH', 'ByodBouncersVisitors',     SAFE_CAST(visitors AS FLOAT64), 'sdi_raw_adobe_byod_flow_organic_search_byod_bouncers_visitors_weekly_tmo',      __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_byod_flow_organic_search_byod_bouncers_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'ORGANIC SEARCH', 'ByodOrders',               SAFE_CAST(orders   AS FLOAT64), 'sdi_raw_adobe_byod_flow_organic_search_byod_orders_weekly_tmo',                __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_byod_flow_organic_search_byod_orders_weekly_tmo`

  /* ------------------------------------------------------------------ DIRECT ------------------------------------------------------------------ */
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'DIRECT', 'ByodVrChatVisitors',      SAFE_CAST(visitors AS FLOAT64), 'sdi_raw_adobe_byod_flow_direct_byod_vr_chat_visitors_weekly_tmo',       __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_byod_flow_direct_byod_vr_chat_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'DIRECT', 'ByodCallVisitors',         SAFE_CAST(visitors AS FLOAT64), 'sdi_raw_adobe_byod_flow_direct_byod_call_visitors_weekly_tmo',          __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_byod_flow_direct_byod_call_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'DIRECT', 'ByodStoreLocatorVisitors', SAFE_CAST(visitors AS FLOAT64), 'sdi_raw_adobe_byod_flow_direct_byod_store_locator_visitors_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_byod_flow_direct_byod_store_locator_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'DIRECT', 'ByodInternalTmoVisitors',  SAFE_CAST(visitors AS FLOAT64), 'sdi_raw_adobe_byod_flow_direct_byod_internaltmo_visitors_weekly_tmo',   __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_byod_flow_direct_byod_internaltmo_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'DIRECT', 'ByodBouncersVisitors',     SAFE_CAST(visitors AS FLOAT64), 'sdi_raw_adobe_byod_flow_direct_byod_bouncers_visitors_weekly_tmo',      __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_byod_flow_direct_byod_bouncers_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'DIRECT', 'ByodOrders',               SAFE_CAST(orders   AS FLOAT64), 'sdi_raw_adobe_byod_flow_direct_byod_orders_weekly_tmo',                __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_byod_flow_direct_byod_orders_weekly_tmo`

  /* ------------------------------------------------------------------ PROGRAMMATIC ------------------------------------------------------------------ */
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'PROGRAMMATIC', 'ByodVrChatVisitors',      SAFE_CAST(visitors AS FLOAT64), 'sdi_raw_adobe_byod_flow_programmatic_byod_vr_chat_visitors_weekly_tmo',       __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_byod_flow_programmatic_byod_vr_chat_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'PROGRAMMATIC', 'ByodCallVisitors',         SAFE_CAST(visitors AS FLOAT64), 'sdi_raw_adobe_byod_flow_programmatic_byod_call_visitors_weekly_tmo',          __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_byod_flow_programmatic_byod_call_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'PROGRAMMATIC', 'ByodStoreLocatorVisitors', SAFE_CAST(visitors AS FLOAT64), 'sdi_raw_adobe_byod_flow_programmatic_byod_store_locator_visitors_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_byod_flow_programmatic_byod_store_locator_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'PROGRAMMATIC', 'ByodInternalTmoVisitors',  SAFE_CAST(visitors AS FLOAT64), 'sdi_raw_adobe_byod_flow_programmatic_byod_internaltmo_visitors_weekly_tmo',   __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_byod_flow_programmatic_byod_internaltmo_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'PROGRAMMATIC', 'ByodBouncersVisitors',     SAFE_CAST(visitors AS FLOAT64), 'sdi_raw_adobe_byod_flow_programmatic_byod_bouncers_visitors_weekly_tmo',      __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_byod_flow_programmatic_byod_bouncers_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'PROGRAMMATIC', 'ByodOrders',               SAFE_CAST(orders   AS FLOAT64), 'sdi_raw_adobe_byod_flow_programmatic_byod_orders_weekly_tmo',                __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_byod_flow_programmatic_byod_orders_weekly_tmo`

  /* ------------------------------------------------------------------ SOCIAL ------------------------------------------------------------------ */
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'SOCIAL', 'ByodVrChatVisitors',      SAFE_CAST(visitors AS FLOAT64), 'sdi_raw_adobe_byod_flow_social_byod_vr_chat_visitors_weekly_tmo',       __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_byod_flow_social_byod_vr_chat_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'SOCIAL', 'ByodCallVisitors',         SAFE_CAST(visitors AS FLOAT64), 'sdi_raw_adobe_byod_flow_social_byod_call_visitors_weekly_tmo',          __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_byod_flow_social_byod_call_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'SOCIAL', 'ByodStoreLocatorVisitors', SAFE_CAST(visitors AS FLOAT64), 'sdi_raw_adobe_byod_flow_social_byod_store_locator_visitors_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_byod_flow_social_byod_store_locator_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'SOCIAL', 'ByodInternalTmoVisitors',  SAFE_CAST(visitors AS FLOAT64), 'sdi_raw_adobe_byod_flow_social_byod_internaltmo_visitors_weekly_tmo',   __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_byod_flow_social_byod_internaltmo_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'SOCIAL', 'ByodBouncersVisitors',     SAFE_CAST(visitors AS FLOAT64), 'sdi_raw_adobe_byod_flow_social_byod_bouncers_visitors_weekly_tmo',      __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_byod_flow_social_byod_bouncers_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'SOCIAL', 'ByodOrders',               SAFE_CAST(orders   AS FLOAT64), 'sdi_raw_adobe_byod_flow_social_byod_orders_weekly_tmo',                __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_byod_flow_social_byod_orders_weekly_tmo`

  /* ------------------------------------------------------------------ OTHER ------------------------------------------------------------------ */
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'OTHER', 'ByodVrChatVisitors',      SAFE_CAST(visitors AS FLOAT64), 'sdi_raw_adobe_byod_flow_other_byod_vr_chat_visitors_weekly_tmo',       __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_byod_flow_other_byod_vr_chat_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'OTHER', 'ByodCallVisitors',         SAFE_CAST(visitors AS FLOAT64), 'sdi_raw_adobe_byod_flow_other_byod_call_visitors_weekly_tmo',          __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_byod_flow_other_byod_call_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'OTHER', 'ByodStoreLocatorVisitors', SAFE_CAST(visitors AS FLOAT64), 'sdi_raw_adobe_byod_flow_other_byod_store_locator_visitors_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_byod_flow_other_byod_store_locator_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'OTHER', 'ByodInternalTmoVisitors',  SAFE_CAST(visitors AS FLOAT64), 'sdi_raw_adobe_byod_flow_other_byod_internaltmo_visitors_weekly_tmo',   __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_byod_flow_other_byod_internaltmo_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'OTHER', 'ByodBouncersVisitors',     SAFE_CAST(visitors AS FLOAT64), 'sdi_raw_adobe_byod_flow_other_byod_bouncers_visitors_weekly_tmo',      __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_byod_flow_other_byod_bouncers_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'OTHER', 'ByodOrders',               SAFE_CAST(orders   AS FLOAT64), 'sdi_raw_adobe_byod_flow_other_byod_orders_weekly_tmo',                __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_byod_flow_other_byod_orders_weekly_tmo`
),

Deduped AS (
  SELECT *
  FROM RawUnion
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY
      WeekSunSat,
      ChannelGroup,
      MetricName,
      SourceTable
    ORDER BY
      FileLoadDatetime DESC,
      Filename        DESC,
      InsertDate      DESC
  ) = 1
)

SELECT
  WeekSunSat,
  ChannelGroup,

  -- Outcome metrics
  MAX(IF(MetricName = 'ByodVrChatVisitors',       MetricValue, NULL)) AS ByodVrChatVisitors,
  MAX(IF(MetricName = 'ByodCallVisitors',          MetricValue, NULL)) AS ByodCallVisitors,
  MAX(IF(MetricName = 'ByodStoreLocatorVisitors',  MetricValue, NULL)) AS ByodStoreLocatorVisitors,
  MAX(IF(MetricName = 'ByodInternalTmoVisitors',   MetricValue, NULL)) AS ByodInternalTmoVisitors,
  MAX(IF(MetricName = 'ByodBouncersVisitors',      MetricValue, NULL)) AS ByodBouncersVisitors,
  MAX(IF(MetricName = 'ByodOrders',                MetricValue, NULL)) AS ByodOrders,

  STRING_AGG(DISTINCT SourceTable, ', ' ORDER BY SourceTable) AS SourceTablesUsed,
  MAX(FileLoadDatetime)                                        AS MaxFileLoadDatetime,
  STRING_AGG(DISTINCT Filename, ', ' ORDER BY Filename)        AS FilenamesUsed

FROM Deduped
GROUP BY
  WeekSunSat,
  ChannelGroup;