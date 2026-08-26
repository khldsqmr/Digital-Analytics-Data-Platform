/* =================================================================================================
FILE:         07_vw_sdi_adobe_bronze_byodFlowEntryPagesByAll_Weekly.sql
LAYER:        Bronze View
DATASET:      prj-dbi-prd-1.ds_dbi_digitalmedia_automation
VIEW:         vw_sdi_adobe_bronze_byodFlowEntryPagesByAll_Weekly

SOURCES (9 tables — ALL channel):
  prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_byod_flow_all_byod_uvnb_visitors_weekly_tmo
  prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_byod_flow_all_byod_entry_byodpage_visitors_weekly_tmo
  prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_byod_flow_all_byod_entry_homepage_visitors_weekly_tmo
  prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_byod_flow_all_byod_entry_devicepage_visitors_weekly_tmo
  prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_byod_flow_all_byod_entry_planspage_visitors_weekly_tmo
  prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_byod_flow_all_byod_entry_otherpage2_visitors_weekly_tmo  -- NEW Other (replaces otherpage)
  prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_byod_flow_all_byod_entry_storepage_visitors_weekly_tmo
  prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_byod_flow_all_byod_entry_byodlandingpage_visitors_weekly_tmo
  prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_byod_flow_all_byod_entry_offersswitch_visitors_weekly_tmo

  -- OLD OTHER (commented out — retained for reference only):
  -- prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_byod_flow_all_byod_entry_otherpage_visitors_weekly_tmo
  -- Old Other = ByodEntryStorePageVisitors + ByodEntryByodLandingPageVisitors + ByodEntryOffersSwitchVisitors + ByodEntryOtherPageVisitors (otherpage2)

DESTINATION:
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_adobe_bronze_byodFlowEntryPagesByAll_Weekly

PURPOSE:
  Bronze view for BYOD funnel entry page visitor metrics at ALL_CHANNELS granularity.
  Tracks how BYOD visitors entered the site — which page they landed on first.
  Each metric is a unique visitor count for that entry page type.

  Entry page metrics:
    ByodUvnbVisitors                 — total unique non-bounced BYOD visitors (all entry pages)
    ByodEntryByodPageVisitors        — entered directly on the BYOD page
    ByodEntryHomePageVisitors        — entered on the homepage
    ByodEntryDevicePageVisitors      — entered on a device page
    ByodEntryPlansPageVisitors       — entered on a plans page
    ByodEntryOtherPageVisitors       — entered on any other page (NEW: otherpage2 only — true residual)
    ByodEntryStorePageVisitors       — entered on a store page (NEW)
    ByodEntryByodLandingPageVisitors — entered on the BYOD landing page (NEW)
    ByodEntryOffersSwitchVisitors    — entered on offers/switch page (NEW)

BUSINESS GRAIN:
  One row per:
      WeekSunSat

BUSINESS RULES:
  - WeekSunSat is derived from date_yyyymmdd by adding 6 days (raw is week-starting Sunday)
  - ChannelGroup is fixed as 'ALL'
  - All metrics are visitor counts (SAFE_CAST visitors AS FLOAT64)
  - Missing metric values remain NULL — no fake zeroes
  - MetricName / MetricValue used internally only, not exposed in final output

KEY DEDUPE RULE:
  Deduplicate each source table at weekly grain using latest:
    File_Load_datetime DESC
    Filename DESC
    __insert_date DESC

CHANGES:
  2026-06-XX — Other segregation:
               ByodEntryOtherPageVisitors redefined from _otherpage_ to _otherpage2_ (new true residual)
               Added ByodEntryStorePageVisitors        <- _storepage_
               Added ByodEntryByodLandingPageVisitors  <- _byodlandingpage_
               Added ByodEntryOffersSwitchVisitors     <- _offersswitch_
               Old _otherpage_ source commented out for reference

DOWNSTREAM:
  Silver 03: vw_sdi_adobe_silver_byodFlowEntryPagesByChannelGroupsPlusAll_Weekly
================================================================================================= */

CREATE OR REPLACE VIEW
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_adobe_bronze_byodFlowEntryPagesByAll_Weekly`
AS

WITH RawUnion AS (

  -- BYOD UVNB Visitors (total unique non-bounced BYOD visitors)
  SELECT
    DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY) AS WeekSunSat,
    'ALL'                                                           AS ChannelGroup,
    'ByodUvnbVisitors'                                              AS MetricName,
    SAFE_CAST(visitors AS FLOAT64)                                  AS MetricValue,
    'sdi_raw_adobe_byod_flow_all_byod_uvnb_visitors_weekly_tmo'     AS SourceTable,
    __insert_date                                                   AS InsertDate,
    File_Load_datetime                                              AS FileLoadDatetime,
    Filename
  FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_byod_flow_all_byod_uvnb_visitors_weekly_tmo`

  UNION ALL

  -- Entry: BYOD Page
  SELECT
    DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY),
    'ALL',
    'ByodEntryByodPageVisitors',
    SAFE_CAST(visitors AS FLOAT64),
    'sdi_raw_adobe_byod_flow_all_byod_entry_byodpage_visitors_weekly_tmo',
    __insert_date, File_Load_datetime, Filename
  FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_byod_flow_all_byod_entry_byodpage_visitors_weekly_tmo`

  UNION ALL

  -- Entry: Homepage
  SELECT
    DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY),
    'ALL',
    'ByodEntryHomePageVisitors',
    SAFE_CAST(visitors AS FLOAT64),
    'sdi_raw_adobe_byod_flow_all_byod_entry_homepage_visitors_weekly_tmo',
    __insert_date, File_Load_datetime, Filename
  FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_byod_flow_all_byod_entry_homepage_visitors_weekly_tmo`

  UNION ALL

  -- Entry: Device Page
  SELECT
    DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY),
    'ALL',
    'ByodEntryDevicePageVisitors',
    SAFE_CAST(visitors AS FLOAT64),
    'sdi_raw_adobe_byod_flow_all_byod_entry_devicepage_visitors_weekly_tmo',
    __insert_date, File_Load_datetime, Filename
  FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_byod_flow_all_byod_entry_devicepage_visitors_weekly_tmo`

  UNION ALL

  -- Entry: Plans Page
  SELECT
    DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY),
    'ALL',
    'ByodEntryPlansPageVisitors',
    SAFE_CAST(visitors AS FLOAT64),
    'sdi_raw_adobe_byod_flow_all_byod_entry_planspage_visitors_weekly_tmo',
    __insert_date, File_Load_datetime, Filename
  FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_byod_flow_all_byod_entry_planspage_visitors_weekly_tmo`

  UNION ALL

  -- Entry: Other Page (NEW DEFINITION — otherpage2 = true residual other)
  -- OLD OTHER source commented out below for reference:
  --   sdi_raw_adobe_byod_flow_all_byod_entry_otherpage_visitors_weekly_tmo
  --   Old Other = ByodEntryStorePageVisitors + ByodEntryByodLandingPageVisitors
  --             + ByodEntryOffersSwitchVisitors + ByodEntryOtherPageVisitors (otherpage2)
  SELECT
    DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY),
    'ALL',
    'ByodEntryOtherPageVisitors',
    SAFE_CAST(visitors AS FLOAT64),
    'sdi_raw_adobe_byod_flow_all_byod_entry_otherpage2_visitors_weekly_tmo',
    __insert_date, File_Load_datetime, Filename
  FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_byod_flow_all_byod_entry_otherpage2_visitors_weekly_tmo`

  UNION ALL

  -- Entry: Store Page (NEW)
  SELECT
    DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY),
    'ALL',
    'ByodEntryStorePageVisitors',
    SAFE_CAST(visitors AS FLOAT64),
    'sdi_raw_adobe_byod_flow_all_byod_entry_storepage_visitors_weekly_tmo',
    __insert_date, File_Load_datetime, Filename
  FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_byod_flow_all_byod_entry_storepage_visitors_weekly_tmo`

  UNION ALL

  -- Entry: BYOD Landing Page (NEW)
  SELECT
    DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY),
    'ALL',
    'ByodEntryByodLandingPageVisitors',
    SAFE_CAST(visitors AS FLOAT64),
    'sdi_raw_adobe_byod_flow_all_byod_entry_byodlandingpage_visitors_weekly_tmo',
    __insert_date, File_Load_datetime, Filename
  FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_byod_flow_all_byod_entry_byodlandingpage_visitors_weekly_tmo`

  UNION ALL

  -- Entry: Offers / Switch Page (NEW)
  SELECT
    DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY),
    'ALL',
    'ByodEntryOffersSwitchVisitors',
    SAFE_CAST(visitors AS FLOAT64),
    'sdi_raw_adobe_byod_flow_all_byod_entry_offersswitch_visitors_weekly_tmo',
    __insert_date, File_Load_datetime, Filename
  FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_byod_flow_all_byod_entry_offersswitch_visitors_weekly_tmo`

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
  MAX(IF(MetricName = 'ByodUvnbVisitors',                MetricValue, NULL)) AS ByodUvnbVisitors,
  MAX(IF(MetricName = 'ByodEntryByodPageVisitors',       MetricValue, NULL)) AS ByodEntryByodPageVisitors,
  MAX(IF(MetricName = 'ByodEntryHomePageVisitors',       MetricValue, NULL)) AS ByodEntryHomePageVisitors,
  MAX(IF(MetricName = 'ByodEntryDevicePageVisitors',     MetricValue, NULL)) AS ByodEntryDevicePageVisitors,
  MAX(IF(MetricName = 'ByodEntryPlansPageVisitors',      MetricValue, NULL)) AS ByodEntryPlansPageVisitors,
  MAX(IF(MetricName = 'ByodEntryOtherPageVisitors',      MetricValue, NULL)) AS ByodEntryOtherPageVisitors,        -- NEW: otherpage2 only (true residual)
  MAX(IF(MetricName = 'ByodEntryStorePageVisitors',      MetricValue, NULL)) AS ByodEntryStorePageVisitors,        -- NEW
  MAX(IF(MetricName = 'ByodEntryByodLandingPageVisitors',MetricValue, NULL)) AS ByodEntryByodLandingPageVisitors,  -- NEW
  MAX(IF(MetricName = 'ByodEntryOffersSwitchVisitors',   MetricValue, NULL)) AS ByodEntryOffersSwitchVisitors,     -- NEW

  STRING_AGG(DISTINCT SourceTable, ', ' ORDER BY SourceTable) AS SourceTablesUsed,
  MAX(FileLoadDatetime)                                        AS MaxFileLoadDatetime,
  STRING_AGG(DISTINCT Filename, ', ' ORDER BY Filename)        AS FilenamesUsed

FROM Deduped
GROUP BY
  WeekSunSat,
  ChannelGroup;