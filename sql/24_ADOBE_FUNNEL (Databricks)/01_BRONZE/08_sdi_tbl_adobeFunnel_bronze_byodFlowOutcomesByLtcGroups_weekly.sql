/* =================================================================================================
FILE:         08_sdi_tbl_adobeFunnel_bronze_byodFlowOutcomesByLtcGroups_weekly.sql
LAYER:        Bronze Table (via Stored Procedure)
DATASET:      prdrzranalytics.lab42
TABLE:         sdi_tbl_adobeFunnel_bronze_byodFlowOutcomesByLtcGroups_weekly
PROCEDURE:         sdi_sp_adobeFunnel_bronze_byodFlowOutcomesByLtcGroups_weekly

SOURCES (36 tables — 6 channel groups × 6 outcome metrics):
  Channel groups: PAID SEARCH, ORGANIC SEARCH, DIRECT, PROGRAMMATIC, SOCIAL, OTHER
  Metrics per group:
    byod_vr_chat_visitors, byod_call_visitors, byod_store_locator_visitors,
    byod_internaltmo_visitors, byod_bouncers_visitors, byod_orders

DESTINATION:
  prdrzranalytics.lab42.sdi_tbl_adobeFunnel_bronze_byodFlowOutcomesByLtcGroups_weekly

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
  - Visitor metrics: TRY_CAST(visitors AS DOUBLE)
  - Orders metric:   TRY_CAST(orders AS DOUBLE)
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

CREATE OR REPLACE PROCEDURE
`prdrzranalytics.lab42.sdi_sp_adobeFunnel_bronze_byodFlowOutcomesByLtcGroups_weekly`()
LANGUAGE SQL
MODIFIES SQL DATA
AS
BEGIN

  CREATE OR REPLACE TABLE
  `prdrzranalytics.lab42.sdi_tbl_adobeFunnel_bronze_byodFlowOutcomesByLtcGroups_weekly`
  AS

WITH RawUnion AS (

  /* ------------------------------------------------------------------ PAID SEARCH ------------------------------------------------------------------ */
  SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6) AS WeekSunSat, 'PAID SEARCH' AS ChannelGroup, 'ByodVrChatVisitors'      AS MetricName, TRY_CAST(visitors AS DOUBLE) AS MetricValue, 'sdi_raw_adobe_byod_flow_paid_search_byod_vr_chat_visitors_weekly_tmo'       AS SourceTable, __insert_date AS InsertDate, File_Load_datetime AS FileLoadDatetime, Filename FROM `prd_dbi_analytics.improvado.sdi_raw_adobe_byod_flow_paid_search_byod_vr_chat_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'PAID SEARCH', 'ByodCallVisitors',         TRY_CAST(visitors AS DOUBLE), 'sdi_raw_adobe_byod_flow_paid_search_byod_call_visitors_weekly_tmo',          __insert_date, File_Load_datetime, Filename FROM `prd_dbi_analytics.improvado.sdi_raw_adobe_byod_flow_paid_search_byod_call_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'PAID SEARCH', 'ByodStoreLocatorVisitors', TRY_CAST(visitors AS DOUBLE), 'sdi_raw_adobe_byod_flow_paid_search_byod_store_locator_visitors_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prd_dbi_analytics.improvado.sdi_raw_adobe_byod_flow_paid_search_byod_store_locator_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'PAID SEARCH', 'ByodInternalTmoVisitors',  TRY_CAST(visitors AS DOUBLE), 'sdi_raw_adobe_byod_flow_paid_search_byod_internaltmo_visitors_weekly_tmo',   __insert_date, File_Load_datetime, Filename FROM `prd_dbi_analytics.improvado.sdi_raw_adobe_byod_flow_paid_search_byod_internaltmo_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'PAID SEARCH', 'ByodBouncersVisitors',     TRY_CAST(visitors AS DOUBLE), 'sdi_raw_adobe_byod_flow_paid_search_byod_bouncers_visitors_weekly_tmo',      __insert_date, File_Load_datetime, Filename FROM `prd_dbi_analytics.improvado.sdi_raw_adobe_byod_flow_paid_search_byod_bouncers_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'PAID SEARCH', 'ByodOrders',               TRY_CAST(orders AS DOUBLE), 'sdi_raw_adobe_byod_flow_paid_search_byod_orders_weekly_tmo',                __insert_date, File_Load_datetime, Filename FROM `prd_dbi_analytics.improvado.sdi_raw_adobe_byod_flow_paid_search_byod_orders_weekly_tmo`

  /* ------------------------------------------------------------------ ORGANIC SEARCH ------------------------------------------------------------------ */
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'ORGANIC SEARCH', 'ByodVrChatVisitors',      TRY_CAST(visitors AS DOUBLE), 'sdi_raw_adobe_byod_flow_organic_search_byod_vr_chat_visitors_weekly_tmo',       __insert_date, File_Load_datetime, Filename FROM `prd_dbi_analytics.improvado.sdi_raw_adobe_byod_flow_organic_search_byod_vr_chat_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'ORGANIC SEARCH', 'ByodCallVisitors',         TRY_CAST(visitors AS DOUBLE), 'sdi_raw_adobe_byod_flow_organic_search_byod_call_visitors_weekly_tmo',          __insert_date, File_Load_datetime, Filename FROM `prd_dbi_analytics.improvado.sdi_raw_adobe_byod_flow_organic_search_byod_call_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'ORGANIC SEARCH', 'ByodStoreLocatorVisitors', TRY_CAST(visitors AS DOUBLE), 'sdi_raw_adobe_byod_flow_organic_search_byod_store_locator_visitors_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prd_dbi_analytics.improvado.sdi_raw_adobe_byod_flow_organic_search_byod_store_locator_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'ORGANIC SEARCH', 'ByodInternalTmoVisitors',  TRY_CAST(visitors AS DOUBLE), 'sdi_raw_adobe_byod_flow_organic_search_byod_internaltmo_visitors_weekly_tmo',   __insert_date, File_Load_datetime, Filename FROM `prd_dbi_analytics.improvado.sdi_raw_adobe_byod_flow_organic_search_byod_internaltmo_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'ORGANIC SEARCH', 'ByodBouncersVisitors',     TRY_CAST(visitors AS DOUBLE), 'sdi_raw_adobe_byod_flow_organic_search_byod_bouncers_visitors_weekly_tmo',      __insert_date, File_Load_datetime, Filename FROM `prd_dbi_analytics.improvado.sdi_raw_adobe_byod_flow_organic_search_byod_bouncers_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'ORGANIC SEARCH', 'ByodOrders',               TRY_CAST(orders AS DOUBLE), 'sdi_raw_adobe_byod_flow_organic_search_byod_orders_weekly_tmo',                __insert_date, File_Load_datetime, Filename FROM `prd_dbi_analytics.improvado.sdi_raw_adobe_byod_flow_organic_search_byod_orders_weekly_tmo`

  /* ------------------------------------------------------------------ DIRECT ------------------------------------------------------------------ */
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'DIRECT', 'ByodVrChatVisitors',      TRY_CAST(visitors AS DOUBLE), 'sdi_raw_adobe_byod_flow_direct_byod_vr_chat_visitors_weekly_tmo',       __insert_date, File_Load_datetime, Filename FROM `prd_dbi_analytics.improvado.sdi_raw_adobe_byod_flow_direct_byod_vr_chat_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'DIRECT', 'ByodCallVisitors',         TRY_CAST(visitors AS DOUBLE), 'sdi_raw_adobe_byod_flow_direct_byod_call_visitors_weekly_tmo',          __insert_date, File_Load_datetime, Filename FROM `prd_dbi_analytics.improvado.sdi_raw_adobe_byod_flow_direct_byod_call_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'DIRECT', 'ByodStoreLocatorVisitors', TRY_CAST(visitors AS DOUBLE), 'sdi_raw_adobe_byod_flow_direct_byod_store_locator_visitors_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prd_dbi_analytics.improvado.sdi_raw_adobe_byod_flow_direct_byod_store_locator_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'DIRECT', 'ByodInternalTmoVisitors',  TRY_CAST(visitors AS DOUBLE), 'sdi_raw_adobe_byod_flow_direct_byod_internaltmo_visitors_weekly_tmo',   __insert_date, File_Load_datetime, Filename FROM `prd_dbi_analytics.improvado.sdi_raw_adobe_byod_flow_direct_byod_internaltmo_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'DIRECT', 'ByodBouncersVisitors',     TRY_CAST(visitors AS DOUBLE), 'sdi_raw_adobe_byod_flow_direct_byod_bouncers_visitors_weekly_tmo',      __insert_date, File_Load_datetime, Filename FROM `prd_dbi_analytics.improvado.sdi_raw_adobe_byod_flow_direct_byod_bouncers_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'DIRECT', 'ByodOrders',               TRY_CAST(orders AS DOUBLE), 'sdi_raw_adobe_byod_flow_direct_byod_orders_weekly_tmo',                __insert_date, File_Load_datetime, Filename FROM `prd_dbi_analytics.improvado.sdi_raw_adobe_byod_flow_direct_byod_orders_weekly_tmo`

  /* ------------------------------------------------------------------ PROGRAMMATIC ------------------------------------------------------------------ */
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'PROGRAMMATIC', 'ByodVrChatVisitors',      TRY_CAST(visitors AS DOUBLE), 'sdi_raw_adobe_byod_flow_programmatic_byod_vr_chat_visitors_weekly_tmo',       __insert_date, File_Load_datetime, Filename FROM `prd_dbi_analytics.improvado.sdi_raw_adobe_byod_flow_programmatic_byod_vr_chat_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'PROGRAMMATIC', 'ByodCallVisitors',         TRY_CAST(visitors AS DOUBLE), 'sdi_raw_adobe_byod_flow_programmatic_byod_call_visitors_weekly_tmo',          __insert_date, File_Load_datetime, Filename FROM `prd_dbi_analytics.improvado.sdi_raw_adobe_byod_flow_programmatic_byod_call_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'PROGRAMMATIC', 'ByodStoreLocatorVisitors', TRY_CAST(visitors AS DOUBLE), 'sdi_raw_adobe_byod_flow_programmatic_byod_store_locator_visitors_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prd_dbi_analytics.improvado.sdi_raw_adobe_byod_flow_programmatic_byod_store_locator_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'PROGRAMMATIC', 'ByodInternalTmoVisitors',  TRY_CAST(visitors AS DOUBLE), 'sdi_raw_adobe_byod_flow_programmatic_byod_internaltmo_visitors_weekly_tmo',   __insert_date, File_Load_datetime, Filename FROM `prd_dbi_analytics.improvado.sdi_raw_adobe_byod_flow_programmatic_byod_internaltmo_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'PROGRAMMATIC', 'ByodBouncersVisitors',     TRY_CAST(visitors AS DOUBLE), 'sdi_raw_adobe_byod_flow_programmatic_byod_bouncers_visitors_weekly_tmo',      __insert_date, File_Load_datetime, Filename FROM `prd_dbi_analytics.improvado.sdi_raw_adobe_byod_flow_programmatic_byod_bouncers_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'PROGRAMMATIC', 'ByodOrders',               TRY_CAST(orders AS DOUBLE), 'sdi_raw_adobe_byod_flow_programmatic_byod_orders_weekly_tmo',                __insert_date, File_Load_datetime, Filename FROM `prd_dbi_analytics.improvado.sdi_raw_adobe_byod_flow_programmatic_byod_orders_weekly_tmo`

  /* ------------------------------------------------------------------ SOCIAL ------------------------------------------------------------------ */
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'SOCIAL', 'ByodVrChatVisitors',      TRY_CAST(visitors AS DOUBLE), 'sdi_raw_adobe_byod_flow_social_byod_vr_chat_visitors_weekly_tmo',       __insert_date, File_Load_datetime, Filename FROM `prd_dbi_analytics.improvado.sdi_raw_adobe_byod_flow_social_byod_vr_chat_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'SOCIAL', 'ByodCallVisitors',         TRY_CAST(visitors AS DOUBLE), 'sdi_raw_adobe_byod_flow_social_byod_call_visitors_weekly_tmo',          __insert_date, File_Load_datetime, Filename FROM `prd_dbi_analytics.improvado.sdi_raw_adobe_byod_flow_social_byod_call_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'SOCIAL', 'ByodStoreLocatorVisitors', TRY_CAST(visitors AS DOUBLE), 'sdi_raw_adobe_byod_flow_social_byod_store_locator_visitors_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prd_dbi_analytics.improvado.sdi_raw_adobe_byod_flow_social_byod_store_locator_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'SOCIAL', 'ByodInternalTmoVisitors',  TRY_CAST(visitors AS DOUBLE), 'sdi_raw_adobe_byod_flow_social_byod_internaltmo_visitors_weekly_tmo',   __insert_date, File_Load_datetime, Filename FROM `prd_dbi_analytics.improvado.sdi_raw_adobe_byod_flow_social_byod_internaltmo_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'SOCIAL', 'ByodBouncersVisitors',     TRY_CAST(visitors AS DOUBLE), 'sdi_raw_adobe_byod_flow_social_byod_bouncers_visitors_weekly_tmo',      __insert_date, File_Load_datetime, Filename FROM `prd_dbi_analytics.improvado.sdi_raw_adobe_byod_flow_social_byod_bouncers_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'SOCIAL', 'ByodOrders',               TRY_CAST(orders AS DOUBLE), 'sdi_raw_adobe_byod_flow_social_byod_orders_weekly_tmo',                __insert_date, File_Load_datetime, Filename FROM `prd_dbi_analytics.improvado.sdi_raw_adobe_byod_flow_social_byod_orders_weekly_tmo`

  /* ------------------------------------------------------------------ OTHER ------------------------------------------------------------------ */
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'OTHER', 'ByodVrChatVisitors',      TRY_CAST(visitors AS DOUBLE), 'sdi_raw_adobe_byod_flow_other_byod_vr_chat_visitors_weekly_tmo',       __insert_date, File_Load_datetime, Filename FROM `prd_dbi_analytics.improvado.sdi_raw_adobe_byod_flow_other_byod_vr_chat_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'OTHER', 'ByodCallVisitors',         TRY_CAST(visitors AS DOUBLE), 'sdi_raw_adobe_byod_flow_other_byod_call_visitors_weekly_tmo',          __insert_date, File_Load_datetime, Filename FROM `prd_dbi_analytics.improvado.sdi_raw_adobe_byod_flow_other_byod_call_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'OTHER', 'ByodStoreLocatorVisitors', TRY_CAST(visitors AS DOUBLE), 'sdi_raw_adobe_byod_flow_other_byod_store_locator_visitors_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prd_dbi_analytics.improvado.sdi_raw_adobe_byod_flow_other_byod_store_locator_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'OTHER', 'ByodInternalTmoVisitors',  TRY_CAST(visitors AS DOUBLE), 'sdi_raw_adobe_byod_flow_other_byod_internaltmo_visitors_weekly_tmo',   __insert_date, File_Load_datetime, Filename FROM `prd_dbi_analytics.improvado.sdi_raw_adobe_byod_flow_other_byod_internaltmo_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'OTHER', 'ByodBouncersVisitors',     TRY_CAST(visitors AS DOUBLE), 'sdi_raw_adobe_byod_flow_other_byod_bouncers_visitors_weekly_tmo',      __insert_date, File_Load_datetime, Filename FROM `prd_dbi_analytics.improvado.sdi_raw_adobe_byod_flow_other_byod_bouncers_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'OTHER', 'ByodOrders',               TRY_CAST(orders AS DOUBLE), 'sdi_raw_adobe_byod_flow_other_byod_orders_weekly_tmo',                __insert_date, File_Load_datetime, Filename FROM `prd_dbi_analytics.improvado.sdi_raw_adobe_byod_flow_other_byod_orders_weekly_tmo`
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

END;