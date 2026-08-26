/* =================================================================================================
FILE:         09_vw_sdi_adobe_bronze_byodFlowOutcomesByAll_Weekly.sql
LAYER:        Bronze View
DATASET:      prj-dbi-prd-1.ds_dbi_digitalmedia_automation
VIEW:         vw_sdi_adobe_bronze_byodFlowOutcomesByAll_Weekly

SOURCES (6 tables — ALL channel):
  prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_byod_flow_all_byod_vr_chat_visitors_weekly_tmo
  prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_byod_flow_all_byod_call_visitors_weekly_tmo
  prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_byod_flow_all_byod_store_locator_visitors_weekly_tmo
  prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_byod_flow_all_byod_internaltmo_visitors_weekly_tmo
  prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_byod_flow_all_byod_bouncers_visitors_weekly_tmo
  prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_byod_flow_all_byod_orders_weekly_tmo

DESTINATION:
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_adobe_bronze_byodFlowOutcomesByAll_Weekly

PURPOSE:
  Bronze view for BYOD funnel outcome metrics at ALL_CHANNELS granularity.
  Tracks what actions BYOD visitors took after entering the site.

  Outcome metrics:
    ByodVrChatVisitors      — engaged via virtual/chat support
    ByodCallVisitors        — engaged via phone call
    ByodStoreLocatorVisitors — used store locator
    ByodInternalTmoVisitors — identified as internal T-Mobile traffic
    ByodBouncersVisitors    — bounced without meaningful engagement
    ByodOrders              — completed a BYOD order (count of orders, not visitors)

  Note: ByodBouncersVisitors is classified as an outcome — it represents visitors who
  entered the BYOD funnel but left without any engagement action.

  Note: ByodOrders uses the `orders` column from the source table, not `visitors`.
  All other metrics use the `visitors` column.

BUSINESS GRAIN:
  One row per:
      WeekSunSat

BUSINESS RULES:
  - WeekSunSat is derived from date_yyyymmdd by adding 6 days (raw is week-starting Sunday)
  - ChannelGroup is fixed as 'ALL'
  - Visitor metrics: SAFE_CAST(visitors AS FLOAT64)
  - Orders metric:   SAFE_CAST(orders AS FLOAT64)
  - Missing metric values remain NULL — no fake zeroes
  - MetricName / MetricValue used internally only, not exposed in final output

KEY DEDUPE RULE:
  Deduplicate each source table at weekly grain using latest:
    File_Load_datetime DESC
    Filename DESC
    __insert_date DESC

DOWNSTREAM:
  Silver 04: vw_sdi_adobe_silver_byodFlowOutcomesByChannelGroupsPlusAll_Weekly
================================================================================================= */

CREATE OR REPLACE VIEW
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_adobe_bronze_byodFlowOutcomesByAll_Weekly`
AS

WITH RawUnion AS (

  -- VR / Chat visitors
  SELECT
    DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY) AS WeekSunSat,
    'ALL'                                                           AS ChannelGroup,
    'ByodVrChatVisitors'                                            AS MetricName,
    SAFE_CAST(visitors AS FLOAT64)                                  AS MetricValue,
    'sdi_raw_adobe_byod_flow_all_byod_vr_chat_visitors_weekly_tmo'  AS SourceTable,
    __insert_date                                                   AS InsertDate,
    File_Load_datetime                                              AS FileLoadDatetime,
    Filename
  FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_byod_flow_all_byod_vr_chat_visitors_weekly_tmo`

  UNION ALL

  -- Call visitors
  SELECT
    DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY),
    'ALL',
    'ByodCallVisitors',
    SAFE_CAST(visitors AS FLOAT64),
    'sdi_raw_adobe_byod_flow_all_byod_call_visitors_weekly_tmo',
    __insert_date,
    File_Load_datetime,
    Filename
  FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_byod_flow_all_byod_call_visitors_weekly_tmo`

  UNION ALL

  -- Store locator visitors
  SELECT
    DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY),
    'ALL',
    'ByodStoreLocatorVisitors',
    SAFE_CAST(visitors AS FLOAT64),
    'sdi_raw_adobe_byod_flow_all_byod_store_locator_visitors_weekly_tmo',
    __insert_date,
    File_Load_datetime,
    Filename
  FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_byod_flow_all_byod_store_locator_visitors_weekly_tmo`

  UNION ALL

  -- Internal T-Mobile visitors
  SELECT
    DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY),
    'ALL',
    'ByodInternalTmoVisitors',
    SAFE_CAST(visitors AS FLOAT64),
    'sdi_raw_adobe_byod_flow_all_byod_internaltmo_visitors_weekly_tmo',
    __insert_date,
    File_Load_datetime,
    Filename
  FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_byod_flow_all_byod_internaltmo_visitors_weekly_tmo`

  UNION ALL

  -- Bouncers visitors
  SELECT
    DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY),
    'ALL',
    'ByodBouncersVisitors',
    SAFE_CAST(visitors AS FLOAT64),
    'sdi_raw_adobe_byod_flow_all_byod_bouncers_visitors_weekly_tmo',
    __insert_date,
    File_Load_datetime,
    Filename
  FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_byod_flow_all_byod_bouncers_visitors_weekly_tmo`

  UNION ALL

  -- Orders (uses orders column, not visitors)
  SELECT
    DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY),
    'ALL',
    'ByodOrders',
    SAFE_CAST(orders AS FLOAT64),
    'sdi_raw_adobe_byod_flow_all_byod_orders_weekly_tmo',
    __insert_date,
    File_Load_datetime,
    Filename
  FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_byod_flow_all_byod_orders_weekly_tmo`
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