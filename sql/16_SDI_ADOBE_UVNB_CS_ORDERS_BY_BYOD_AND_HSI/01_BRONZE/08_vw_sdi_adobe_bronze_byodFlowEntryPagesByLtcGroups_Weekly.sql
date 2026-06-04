/* =================================================================================================
FILE:         08_vw_sdi_adobe_bronze_byodFlowEntryPagesByLtcGroups_Weekly.sql
LAYER:        Bronze View
DATASET:      prj-dbi-prd-1.ds_dbi_digitalmedia_automation
VIEW:         vw_sdi_adobe_bronze_byodFlowEntryPagesByLtcGroups_Weekly

SOURCES (36 tables — 6 channel groups × 6 entry metrics):
  Channel groups: PAID SEARCH, ORGANIC SEARCH, DIRECT, PROGRAMMATIC, SOCIAL, OTHER
  Metrics per group:
    byod_uvnb_visitors, entry_byodpage_visitors, entry_homepage_visitors,
    entry_devicepage_visitors, entry_planspage_visitors, entry_otherpage_visitors

DESTINATION:
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_adobe_bronze_byodFlowEntryPagesByLtcGroups_Weekly

PURPOSE:
  Bronze view for BYOD funnel entry page visitor metrics at LTC_GROUPS granularity.
  Structurally mirrors Bronze 07 but broken out by channel group.
  Same 6 entry page metrics per channel group.

BUSINESS GRAIN:
  One row per:
      WeekSunSat
      ChannelGroup

BUSINESS RULES:
  - WeekSunSat is derived from date_yyyymmdd by adding 6 days (raw is week-starting Sunday)
  - ChannelGroup is hardcoded per source table family
  - All metrics are visitor counts (SAFE_CAST visitors AS FLOAT64)
  - Missing metric values remain NULL — no fake zeroes
  - MetricName / MetricValue used internally only, not exposed in final output

KEY DEDUPE RULE:
  Deduplicate each source table at weekly + ChannelGroup grain using latest:
    File_Load_datetime DESC
    Filename DESC
    __insert_date DESC

DOWNSTREAM:
  Silver 03: vw_sdi_adobe_silver_byodFlowEntryPagesByChannelGroupsPlusAll_Weekly
================================================================================================= */

CREATE OR REPLACE VIEW
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_adobe_bronze_byodFlowEntryPagesByLtcGroups_Weekly`
AS

WITH RawUnion AS (

  /* ------------------------------------------------------------------ PAID SEARCH ------------------------------------------------------------------ */
  SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY) AS WeekSunSat, 'PAID SEARCH' AS ChannelGroup, 'ByodUvnbVisitors' AS MetricName, SAFE_CAST(visitors AS FLOAT64) AS MetricValue, 'sdi_raw_adobe_byod_flow_paid_search_byod_uvnb_visitors_weekly_tmo' AS SourceTable, __insert_date AS InsertDate, File_Load_datetime AS FileLoadDatetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_byod_flow_paid_search_byod_uvnb_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'PAID SEARCH', 'ByodEntryByodPageVisitors',    SAFE_CAST(visitors AS FLOAT64), 'sdi_raw_adobe_byod_flow_paid_search_byod_entry_byodpage_visitors_weekly_tmo',    __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_byod_flow_paid_search_byod_entry_byodpage_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'PAID SEARCH', 'ByodEntryHomePageVisitors',    SAFE_CAST(visitors AS FLOAT64), 'sdi_raw_adobe_byod_flow_paid_search_byod_entry_homepage_visitors_weekly_tmo',    __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_byod_flow_paid_search_byod_entry_homepage_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'PAID SEARCH', 'ByodEntryDevicePageVisitors',  SAFE_CAST(visitors AS FLOAT64), 'sdi_raw_adobe_byod_flow_paid_search_byod_entry_devicepage_visitors_weekly_tmo',  __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_byod_flow_paid_search_byod_entry_devicepage_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'PAID SEARCH', 'ByodEntryPlansPageVisitors',   SAFE_CAST(visitors AS FLOAT64), 'sdi_raw_adobe_byod_flow_paid_search_byod_entry_planspage_visitors_weekly_tmo',   __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_byod_flow_paid_search_byod_entry_planspage_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'PAID SEARCH', 'ByodEntryOtherPageVisitors',   SAFE_CAST(visitors AS FLOAT64), 'sdi_raw_adobe_byod_flow_paid_search_byod_entry_otherpage_visitors_weekly_tmo',   __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_byod_flow_paid_search_byod_entry_otherpage_visitors_weekly_tmo`

  /* ------------------------------------------------------------------ ORGANIC SEARCH ------------------------------------------------------------------ */
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'ORGANIC SEARCH', 'ByodUvnbVisitors',            SAFE_CAST(visitors AS FLOAT64), 'sdi_raw_adobe_byod_flow_organic_search_byod_uvnb_visitors_weekly_tmo',            __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_byod_flow_organic_search_byod_uvnb_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'ORGANIC SEARCH', 'ByodEntryByodPageVisitors',   SAFE_CAST(visitors AS FLOAT64), 'sdi_raw_adobe_byod_flow_organic_search_byod_entry_byodpage_visitors_weekly_tmo',  __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_byod_flow_organic_search_byod_entry_byodpage_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'ORGANIC SEARCH', 'ByodEntryHomePageVisitors',   SAFE_CAST(visitors AS FLOAT64), 'sdi_raw_adobe_byod_flow_organic_search_byod_entry_homepage_visitors_weekly_tmo',  __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_byod_flow_organic_search_byod_entry_homepage_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'ORGANIC SEARCH', 'ByodEntryDevicePageVisitors', SAFE_CAST(visitors AS FLOAT64), 'sdi_raw_adobe_byod_flow_organic_search_byod_entry_devicepage_visitors_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_byod_flow_organic_search_byod_entry_devicepage_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'ORGANIC SEARCH', 'ByodEntryPlansPageVisitors',  SAFE_CAST(visitors AS FLOAT64), 'sdi_raw_adobe_byod_flow_organic_search_byod_entry_planspage_visitors_weekly_tmo',  __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_byod_flow_organic_search_byod_entry_planspage_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'ORGANIC SEARCH', 'ByodEntryOtherPageVisitors',  SAFE_CAST(visitors AS FLOAT64), 'sdi_raw_adobe_byod_flow_organic_search_byod_entry_otherpage_visitors_weekly_tmo',  __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_byod_flow_organic_search_byod_entry_otherpage_visitors_weekly_tmo`

  /* ------------------------------------------------------------------ DIRECT ------------------------------------------------------------------ */
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'DIRECT', 'ByodUvnbVisitors',            SAFE_CAST(visitors AS FLOAT64), 'sdi_raw_adobe_byod_flow_direct_byod_uvnb_visitors_weekly_tmo',            __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_byod_flow_direct_byod_uvnb_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'DIRECT', 'ByodEntryByodPageVisitors',   SAFE_CAST(visitors AS FLOAT64), 'sdi_raw_adobe_byod_flow_direct_byod_entry_byodpage_visitors_weekly_tmo',   __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_byod_flow_direct_byod_entry_byodpage_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'DIRECT', 'ByodEntryHomePageVisitors',   SAFE_CAST(visitors AS FLOAT64), 'sdi_raw_adobe_byod_flow_direct_byod_entry_homepage_visitors_weekly_tmo',   __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_byod_flow_direct_byod_entry_homepage_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'DIRECT', 'ByodEntryDevicePageVisitors', SAFE_CAST(visitors AS FLOAT64), 'sdi_raw_adobe_byod_flow_direct_byod_entry_devicepage_visitors_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_byod_flow_direct_byod_entry_devicepage_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'DIRECT', 'ByodEntryPlansPageVisitors',  SAFE_CAST(visitors AS FLOAT64), 'sdi_raw_adobe_byod_flow_direct_byod_entry_planspage_visitors_weekly_tmo',  __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_byod_flow_direct_byod_entry_planspage_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'DIRECT', 'ByodEntryOtherPageVisitors',  SAFE_CAST(visitors AS FLOAT64), 'sdi_raw_adobe_byod_flow_direct_byod_entry_otherpage_visitors_weekly_tmo',  __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_byod_flow_direct_byod_entry_otherpage_visitors_weekly_tmo`

  /* ------------------------------------------------------------------ PROGRAMMATIC ------------------------------------------------------------------ */
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'PROGRAMMATIC', 'ByodUvnbVisitors',            SAFE_CAST(visitors AS FLOAT64), 'sdi_raw_adobe_byod_flow_programmatic_byod_uvnb_visitors_weekly_tmo',            __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_byod_flow_programmatic_byod_uvnb_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'PROGRAMMATIC', 'ByodEntryByodPageVisitors',   SAFE_CAST(visitors AS FLOAT64), 'sdi_raw_adobe_byod_flow_programmatic_byod_entry_byodpage_visitors_weekly_tmo',   __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_byod_flow_programmatic_byod_entry_byodpage_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'PROGRAMMATIC', 'ByodEntryHomePageVisitors',   SAFE_CAST(visitors AS FLOAT64), 'sdi_raw_adobe_byod_flow_programmatic_byod_entry_homepage_visitors_weekly_tmo',   __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_byod_flow_programmatic_byod_entry_homepage_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'PROGRAMMATIC', 'ByodEntryDevicePageVisitors', SAFE_CAST(visitors AS FLOAT64), 'sdi_raw_adobe_byod_flow_programmatic_byod_entry_devicepage_visitors_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_byod_flow_programmatic_byod_entry_devicepage_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'PROGRAMMATIC', 'ByodEntryPlansPageVisitors',  SAFE_CAST(visitors AS FLOAT64), 'sdi_raw_adobe_byod_flow_programmatic_byod_entry_planspage_visitors_weekly_tmo',  __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_byod_flow_programmatic_byod_entry_planspage_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'PROGRAMMATIC', 'ByodEntryOtherPageVisitors',  SAFE_CAST(visitors AS FLOAT64), 'sdi_raw_adobe_byod_flow_programmatic_byod_entry_otherpage_visitors_weekly_tmo',  __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_byod_flow_programmatic_byod_entry_otherpage_visitors_weekly_tmo`

  /* ------------------------------------------------------------------ SOCIAL ------------------------------------------------------------------ */
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'SOCIAL', 'ByodUvnbVisitors',            SAFE_CAST(visitors AS FLOAT64), 'sdi_raw_adobe_byod_flow_social_byod_uvnb_visitors_weekly_tmo',            __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_byod_flow_social_byod_uvnb_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'SOCIAL', 'ByodEntryByodPageVisitors',   SAFE_CAST(visitors AS FLOAT64), 'sdi_raw_adobe_byod_flow_social_byod_entry_byodpage_visitors_weekly_tmo',   __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_byod_flow_social_byod_entry_byodpage_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'SOCIAL', 'ByodEntryHomePageVisitors',   SAFE_CAST(visitors AS FLOAT64), 'sdi_raw_adobe_byod_flow_social_byod_entry_homepage_visitors_weekly_tmo',   __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_byod_flow_social_byod_entry_homepage_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'SOCIAL', 'ByodEntryDevicePageVisitors', SAFE_CAST(visitors AS FLOAT64), 'sdi_raw_adobe_byod_flow_social_byod_entry_devicepage_visitors_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_byod_flow_social_byod_entry_devicepage_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'SOCIAL', 'ByodEntryPlansPageVisitors',  SAFE_CAST(visitors AS FLOAT64), 'sdi_raw_adobe_byod_flow_social_byod_entry_planspage_visitors_weekly_tmo',  __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_byod_flow_social_byod_entry_planspage_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'SOCIAL', 'ByodEntryOtherPageVisitors',  SAFE_CAST(visitors AS FLOAT64), 'sdi_raw_adobe_byod_flow_social_byod_entry_otherpage_visitors_weekly_tmo',  __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_byod_flow_social_byod_entry_otherpage_visitors_weekly_tmo`

  /* ------------------------------------------------------------------ OTHER ------------------------------------------------------------------ */
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'OTHER', 'ByodUvnbVisitors',            SAFE_CAST(visitors AS FLOAT64), 'sdi_raw_adobe_byod_flow_other_byod_uvnb_visitors_weekly_tmo',            __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_byod_flow_other_byod_uvnb_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'OTHER', 'ByodEntryByodPageVisitors',   SAFE_CAST(visitors AS FLOAT64), 'sdi_raw_adobe_byod_flow_other_byod_entry_byodpage_visitors_weekly_tmo',   __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_byod_flow_other_byod_entry_byodpage_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'OTHER', 'ByodEntryHomePageVisitors',   SAFE_CAST(visitors AS FLOAT64), 'sdi_raw_adobe_byod_flow_other_byod_entry_homepage_visitors_weekly_tmo',   __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_byod_flow_other_byod_entry_homepage_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'OTHER', 'ByodEntryDevicePageVisitors', SAFE_CAST(visitors AS FLOAT64), 'sdi_raw_adobe_byod_flow_other_byod_entry_devicepage_visitors_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_byod_flow_other_byod_entry_devicepage_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'OTHER', 'ByodEntryPlansPageVisitors',  SAFE_CAST(visitors AS FLOAT64), 'sdi_raw_adobe_byod_flow_other_byod_entry_planspage_visitors_weekly_tmo',  __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_byod_flow_other_byod_entry_planspage_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY), 'OTHER', 'ByodEntryOtherPageVisitors',  SAFE_CAST(visitors AS FLOAT64), 'sdi_raw_adobe_byod_flow_other_byod_entry_otherpage_visitors_weekly_tmo',  __insert_date, File_Load_datetime, Filename FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_byod_flow_other_byod_entry_otherpage_visitors_weekly_tmo`
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

  -- Entry page metrics
  MAX(IF(MetricName = 'ByodUvnbVisitors',            MetricValue, NULL)) AS ByodUvnbVisitors,
  MAX(IF(MetricName = 'ByodEntryByodPageVisitors',   MetricValue, NULL)) AS ByodEntryByodPageVisitors,
  MAX(IF(MetricName = 'ByodEntryHomePageVisitors',   MetricValue, NULL)) AS ByodEntryHomePageVisitors,
  MAX(IF(MetricName = 'ByodEntryDevicePageVisitors', MetricValue, NULL)) AS ByodEntryDevicePageVisitors,
  MAX(IF(MetricName = 'ByodEntryPlansPageVisitors',  MetricValue, NULL)) AS ByodEntryPlansPageVisitors,
  MAX(IF(MetricName = 'ByodEntryOtherPageVisitors',  MetricValue, NULL)) AS ByodEntryOtherPageVisitors,

  STRING_AGG(DISTINCT SourceTable, ', ' ORDER BY SourceTable) AS SourceTablesUsed,
  MAX(FileLoadDatetime)                                        AS MaxFileLoadDatetime,
  STRING_AGG(DISTINCT Filename, ', ' ORDER BY Filename)        AS FilenamesUsed

FROM Deduped
GROUP BY
  WeekSunSat,
  ChannelGroup;