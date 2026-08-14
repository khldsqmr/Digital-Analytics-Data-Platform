/* =================================================================================================
FILE:         06_sdi_tbl_adobeFunnel_bronze_byodFlowEntryPagesByLtcGroups_weekly.sql
LAYER:        Bronze Table (via Stored Procedure)
DATASET:      prdrzranalytics.lab42
TABLE:         sdi_tbl_adobeFunnel_bronze_byodFlowEntryPagesByLtcGroups_weekly
PROCEDURE:         sdi_sp_adobeFunnel_bronze_byodFlowEntryPagesByLtcGroups_weekly

SOURCES (54 tables — 6 channel groups x 9 entry metrics):
  Channel groups: PAID SEARCH, ORGANIC SEARCH, DIRECT, PROGRAMMATIC, SOCIAL, OTHER
  Metrics per group:
    byod_uvnb_visitors, entry_byodpage_visitors, entry_homepage_visitors,
    entry_devicepage_visitors, entry_planspage_visitors,
    entry_otherpage2_visitors (NEW — replaces entry_otherpage_visitors),
    entry_storepage_visitors (NEW),
    entry_byodlandingpage_visitors (NEW),
    entry_offersswitch_visitors (NEW)

  -- OLD OTHER (commented out — retained for reference only):
  -- entry_otherpage_visitors (per channel group)
  -- Old Other = ByodEntryStorePageVisitors + ByodEntryByodLandingPageVisitors
  --           + ByodEntryOffersSwitchVisitors + ByodEntryOtherPageVisitors (otherpage2)

DESTINATION:
  prdrzranalytics.lab42.sdi_tbl_adobeFunnel_bronze_byodFlowEntryPagesByLtcGroups_weekly

PURPOSE:
  Bronze view for BYOD funnel entry page visitor metrics at LTC_GROUPS granularity.
  Structurally mirrors Bronze 07 but broken out by channel group.
  Same 9 entry page metrics per channel group.

BUSINESS GRAIN:
  One row per:
      WeekSunSat
      ChannelGroup

BUSINESS RULES:
  - WeekSunSat is derived from date_yyyymmdd by adding 6 days (raw is week-starting Sunday)
  - ChannelGroup is hardcoded per source table family
  - All metrics are visitor counts (TRY_CAST(visitors AS DOUBLE))
  - Missing metric values remain NULL — no fake zeroes
  - MetricName / MetricValue used internally only, not exposed in final output

KEY DEDUPE RULE:
  Deduplicate each source table at weekly + ChannelGroup grain using latest:
    File_Load_datetime DESC
    Filename DESC
    __insert_date DESC

CHANGES:
  2026-06-XX — Other segregation:
               ByodEntryOtherPageVisitors redefined from _otherpage_ to _otherpage2_ (new true residual)
               Added ByodEntryStorePageVisitors        <- _storepage_
               Added ByodEntryByodLandingPageVisitors  <- _byodlandingpage_
               Added ByodEntryOffersSwitchVisitors     <- _offersswitch_
               Old _otherpage_ sources commented out for reference (all 6 channel groups)

DOWNSTREAM:
  Silver 03: vw_sdi_adobe_silver_byodFlowEntryPagesByChannelGroupsPlusAll_Weekly
================================================================================================= */

CREATE OR REPLACE PROCEDURE
`prdrzranalytics.lab42.sdi_sp_adobeFunnel_bronze_byodFlowEntryPagesByLtcGroups_weekly`()
LANGUAGE SQL
MODIFIES SQL DATA
AS
BEGIN

  CREATE OR REPLACE TABLE
  `prdrzranalytics.lab42.sdi_tbl_adobeFunnel_bronze_byodFlowEntryPagesByLtcGroups_weekly`
  AS

WITH RawUnion AS (

  /* ------------------------------------------------------------------ PAID SEARCH ------------------------------------------------------------------ */
  SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6) AS WeekSunSat, 'PAID SEARCH' AS ChannelGroup, 'ByodUpvVisitors' AS MetricName, TRY_CAST(visitors AS DOUBLE) AS MetricValue, 'sdi_raw_adobe_byod_flow_paid_search_byod_uvnb_visitors_weekly_tmo' AS SourceTable, __insert_date AS InsertDate, File_Load_datetime AS FileLoadDatetime, Filename FROM `prd_dbi_analytics.improvado.sdi_raw_adobe_byod_flow_paid_search_byod_uvnb_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'PAID SEARCH', 'ByodEntryByodPageVisitors',       TRY_CAST(visitors AS DOUBLE), 'sdi_raw_adobe_byod_flow_paid_search_byod_entry_byodpage_visitors_weekly_tmo',       __insert_date, File_Load_datetime, Filename FROM `prd_dbi_analytics.improvado.sdi_raw_adobe_byod_flow_paid_search_byod_entry_byodpage_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'PAID SEARCH', 'ByodEntryHomePageVisitors',       TRY_CAST(visitors AS DOUBLE), 'sdi_raw_adobe_byod_flow_paid_search_byod_entry_homepage_visitors_weekly_tmo',       __insert_date, File_Load_datetime, Filename FROM `prd_dbi_analytics.improvado.sdi_raw_adobe_byod_flow_paid_search_byod_entry_homepage_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'PAID SEARCH', 'ByodEntryDevicePageVisitors',     TRY_CAST(visitors AS DOUBLE), 'sdi_raw_adobe_byod_flow_paid_search_byod_entry_devicepage_visitors_weekly_tmo',     __insert_date, File_Load_datetime, Filename FROM `prd_dbi_analytics.improvado.sdi_raw_adobe_byod_flow_paid_search_byod_entry_devicepage_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'PAID SEARCH', 'ByodEntryPlansPageVisitors',      TRY_CAST(visitors AS DOUBLE), 'sdi_raw_adobe_byod_flow_paid_search_byod_entry_planspage_visitors_weekly_tmo',      __insert_date, File_Load_datetime, Filename FROM `prd_dbi_analytics.improvado.sdi_raw_adobe_byod_flow_paid_search_byod_entry_planspage_visitors_weekly_tmo`
  -- OLD OTHER: sdi_raw_adobe_byod_flow_paid_search_byod_entry_otherpage_visitors_weekly_tmo (Old Other = Store + ByodLanding + OffersSwitch + otherpage2)
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'PAID SEARCH', 'ByodEntryOtherPageVisitors',      TRY_CAST(visitors AS DOUBLE), 'sdi_raw_adobe_byod_flow_paid_search_byod_entry_otherpage2_visitors_weekly_tmo',      __insert_date, File_Load_datetime, Filename FROM `prd_dbi_analytics.improvado.sdi_raw_adobe_byod_flow_paid_search_byod_entry_otherpage2_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'PAID SEARCH', 'ByodEntryStorePageVisitors',      TRY_CAST(visitors AS DOUBLE), 'sdi_raw_adobe_byod_flow_paid_search_byod_entry_storepage_visitors_weekly_tmo',      __insert_date, File_Load_datetime, Filename FROM `prd_dbi_analytics.improvado.sdi_raw_adobe_byod_flow_paid_search_byod_entry_storepage_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'PAID SEARCH', 'ByodEntryByodLandingPageVisitors', TRY_CAST(visitors AS DOUBLE), 'sdi_raw_adobe_byod_flow_paid_search_byod_entry_byodlandingpage_visitors_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prd_dbi_analytics.improvado.sdi_raw_adobe_byod_flow_paid_search_byod_entry_byodlandingpage_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'PAID SEARCH', 'ByodEntryOffersSwitchVisitors',    TRY_CAST(visitors AS DOUBLE), 'sdi_raw_adobe_byod_flow_paid_search_byod_entry_offersswitch_visitors_weekly_tmo',    __insert_date, File_Load_datetime, Filename FROM `prd_dbi_analytics.improvado.sdi_raw_adobe_byod_flow_paid_search_byod_entry_offersswitch_visitors_weekly_tmo`

  /* ------------------------------------------------------------------ ORGANIC SEARCH ------------------------------------------------------------------ */
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'ORGANIC SEARCH', 'ByodUpvVisitors',               TRY_CAST(visitors AS DOUBLE), 'sdi_raw_adobe_byod_flow_organic_search_byod_uvnb_visitors_weekly_tmo',               __insert_date, File_Load_datetime, Filename FROM `prd_dbi_analytics.improvado.sdi_raw_adobe_byod_flow_organic_search_byod_uvnb_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'ORGANIC SEARCH', 'ByodEntryByodPageVisitors',       TRY_CAST(visitors AS DOUBLE), 'sdi_raw_adobe_byod_flow_organic_search_byod_entry_byodpage_visitors_weekly_tmo',    __insert_date, File_Load_datetime, Filename FROM `prd_dbi_analytics.improvado.sdi_raw_adobe_byod_flow_organic_search_byod_entry_byodpage_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'ORGANIC SEARCH', 'ByodEntryHomePageVisitors',       TRY_CAST(visitors AS DOUBLE), 'sdi_raw_adobe_byod_flow_organic_search_byod_entry_homepage_visitors_weekly_tmo',    __insert_date, File_Load_datetime, Filename FROM `prd_dbi_analytics.improvado.sdi_raw_adobe_byod_flow_organic_search_byod_entry_homepage_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'ORGANIC SEARCH', 'ByodEntryDevicePageVisitors',     TRY_CAST(visitors AS DOUBLE), 'sdi_raw_adobe_byod_flow_organic_search_byod_entry_devicepage_visitors_weekly_tmo',  __insert_date, File_Load_datetime, Filename FROM `prd_dbi_analytics.improvado.sdi_raw_adobe_byod_flow_organic_search_byod_entry_devicepage_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'ORGANIC SEARCH', 'ByodEntryPlansPageVisitors',      TRY_CAST(visitors AS DOUBLE), 'sdi_raw_adobe_byod_flow_organic_search_byod_entry_planspage_visitors_weekly_tmo',   __insert_date, File_Load_datetime, Filename FROM `prd_dbi_analytics.improvado.sdi_raw_adobe_byod_flow_organic_search_byod_entry_planspage_visitors_weekly_tmo`
  -- OLD OTHER: sdi_raw_adobe_byod_flow_organic_search_byod_entry_otherpage_visitors_weekly_tmo (Old Other = Store + ByodLanding + OffersSwitch + otherpage2)
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'ORGANIC SEARCH', 'ByodEntryOtherPageVisitors',      TRY_CAST(visitors AS DOUBLE), 'sdi_raw_adobe_byod_flow_organic_search_byod_entry_otherpage2_visitors_weekly_tmo',  __insert_date, File_Load_datetime, Filename FROM `prd_dbi_analytics.improvado.sdi_raw_adobe_byod_flow_organic_search_byod_entry_otherpage2_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'ORGANIC SEARCH', 'ByodEntryStorePageVisitors',      TRY_CAST(visitors AS DOUBLE), 'sdi_raw_adobe_byod_flow_organic_search_byod_entry_storepage_visitors_weekly_tmo',   __insert_date, File_Load_datetime, Filename FROM `prd_dbi_analytics.improvado.sdi_raw_adobe_byod_flow_organic_search_byod_entry_storepage_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'ORGANIC SEARCH', 'ByodEntryByodLandingPageVisitors', TRY_CAST(visitors AS DOUBLE), 'sdi_raw_adobe_byod_flow_organic_search_byod_entry_byodlandingpage_visitors_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prd_dbi_analytics.improvado.sdi_raw_adobe_byod_flow_organic_search_byod_entry_byodlandingpage_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'ORGANIC SEARCH', 'ByodEntryOffersSwitchVisitors',    TRY_CAST(visitors AS DOUBLE), 'sdi_raw_adobe_byod_flow_organic_search_byod_entry_offersswitch_visitors_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prd_dbi_analytics.improvado.sdi_raw_adobe_byod_flow_organic_search_byod_entry_offersswitch_visitors_weekly_tmo`

  /* ------------------------------------------------------------------ DIRECT ------------------------------------------------------------------ */
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'DIRECT', 'ByodUpvVisitors',               TRY_CAST(visitors AS DOUBLE), 'sdi_raw_adobe_byod_flow_direct_byod_uvnb_visitors_weekly_tmo',               __insert_date, File_Load_datetime, Filename FROM `prd_dbi_analytics.improvado.sdi_raw_adobe_byod_flow_direct_byod_uvnb_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'DIRECT', 'ByodEntryByodPageVisitors',       TRY_CAST(visitors AS DOUBLE), 'sdi_raw_adobe_byod_flow_direct_byod_entry_byodpage_visitors_weekly_tmo',   __insert_date, File_Load_datetime, Filename FROM `prd_dbi_analytics.improvado.sdi_raw_adobe_byod_flow_direct_byod_entry_byodpage_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'DIRECT', 'ByodEntryHomePageVisitors',       TRY_CAST(visitors AS DOUBLE), 'sdi_raw_adobe_byod_flow_direct_byod_entry_homepage_visitors_weekly_tmo',   __insert_date, File_Load_datetime, Filename FROM `prd_dbi_analytics.improvado.sdi_raw_adobe_byod_flow_direct_byod_entry_homepage_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'DIRECT', 'ByodEntryDevicePageVisitors',     TRY_CAST(visitors AS DOUBLE), 'sdi_raw_adobe_byod_flow_direct_byod_entry_devicepage_visitors_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prd_dbi_analytics.improvado.sdi_raw_adobe_byod_flow_direct_byod_entry_devicepage_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'DIRECT', 'ByodEntryPlansPageVisitors',      TRY_CAST(visitors AS DOUBLE), 'sdi_raw_adobe_byod_flow_direct_byod_entry_planspage_visitors_weekly_tmo',  __insert_date, File_Load_datetime, Filename FROM `prd_dbi_analytics.improvado.sdi_raw_adobe_byod_flow_direct_byod_entry_planspage_visitors_weekly_tmo`
  -- OLD OTHER: sdi_raw_adobe_byod_flow_direct_byod_entry_otherpage_visitors_weekly_tmo (Old Other = Store + ByodLanding + OffersSwitch + otherpage2)
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'DIRECT', 'ByodEntryOtherPageVisitors',      TRY_CAST(visitors AS DOUBLE), 'sdi_raw_adobe_byod_flow_direct_byod_entry_otherpage2_visitors_weekly_tmo',  __insert_date, File_Load_datetime, Filename FROM `prd_dbi_analytics.improvado.sdi_raw_adobe_byod_flow_direct_byod_entry_otherpage2_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'DIRECT', 'ByodEntryStorePageVisitors',      TRY_CAST(visitors AS DOUBLE), 'sdi_raw_adobe_byod_flow_direct_byod_entry_storepage_visitors_weekly_tmo',  __insert_date, File_Load_datetime, Filename FROM `prd_dbi_analytics.improvado.sdi_raw_adobe_byod_flow_direct_byod_entry_storepage_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'DIRECT', 'ByodEntryByodLandingPageVisitors', TRY_CAST(visitors AS DOUBLE), 'sdi_raw_adobe_byod_flow_direct_byod_entry_byodlandingpage_visitors_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prd_dbi_analytics.improvado.sdi_raw_adobe_byod_flow_direct_byod_entry_byodlandingpage_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'DIRECT', 'ByodEntryOffersSwitchVisitors',    TRY_CAST(visitors AS DOUBLE), 'sdi_raw_adobe_byod_flow_direct_byod_entry_offersswitch_visitors_weekly_tmo',  __insert_date, File_Load_datetime, Filename FROM `prd_dbi_analytics.improvado.sdi_raw_adobe_byod_flow_direct_byod_entry_offersswitch_visitors_weekly_tmo`

  /* ------------------------------------------------------------------ PROGRAMMATIC ------------------------------------------------------------------ */
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'PROGRAMMATIC', 'ByodUpvVisitors',               TRY_CAST(visitors AS DOUBLE), 'sdi_raw_adobe_byod_flow_programmatic_byod_uvnb_visitors_weekly_tmo',               __insert_date, File_Load_datetime, Filename FROM `prd_dbi_analytics.improvado.sdi_raw_adobe_byod_flow_programmatic_byod_uvnb_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'PROGRAMMATIC', 'ByodEntryByodPageVisitors',       TRY_CAST(visitors AS DOUBLE), 'sdi_raw_adobe_byod_flow_programmatic_byod_entry_byodpage_visitors_weekly_tmo',   __insert_date, File_Load_datetime, Filename FROM `prd_dbi_analytics.improvado.sdi_raw_adobe_byod_flow_programmatic_byod_entry_byodpage_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'PROGRAMMATIC', 'ByodEntryHomePageVisitors',       TRY_CAST(visitors AS DOUBLE), 'sdi_raw_adobe_byod_flow_programmatic_byod_entry_homepage_visitors_weekly_tmo',   __insert_date, File_Load_datetime, Filename FROM `prd_dbi_analytics.improvado.sdi_raw_adobe_byod_flow_programmatic_byod_entry_homepage_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'PROGRAMMATIC', 'ByodEntryDevicePageVisitors',     TRY_CAST(visitors AS DOUBLE), 'sdi_raw_adobe_byod_flow_programmatic_byod_entry_devicepage_visitors_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prd_dbi_analytics.improvado.sdi_raw_adobe_byod_flow_programmatic_byod_entry_devicepage_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'PROGRAMMATIC', 'ByodEntryPlansPageVisitors',      TRY_CAST(visitors AS DOUBLE), 'sdi_raw_adobe_byod_flow_programmatic_byod_entry_planspage_visitors_weekly_tmo',  __insert_date, File_Load_datetime, Filename FROM `prd_dbi_analytics.improvado.sdi_raw_adobe_byod_flow_programmatic_byod_entry_planspage_visitors_weekly_tmo`
  -- OLD OTHER: sdi_raw_adobe_byod_flow_programmatic_byod_entry_otherpage_visitors_weekly_tmo (Old Other = Store + ByodLanding + OffersSwitch + otherpage2)
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'PROGRAMMATIC', 'ByodEntryOtherPageVisitors',      TRY_CAST(visitors AS DOUBLE), 'sdi_raw_adobe_byod_flow_programmatic_byod_entry_otherpage2_visitors_weekly_tmo',  __insert_date, File_Load_datetime, Filename FROM `prd_dbi_analytics.improvado.sdi_raw_adobe_byod_flow_programmatic_byod_entry_otherpage2_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'PROGRAMMATIC', 'ByodEntryStorePageVisitors',      TRY_CAST(visitors AS DOUBLE), 'sdi_raw_adobe_byod_flow_programmatic_byod_entry_storepage_visitors_weekly_tmo',  __insert_date, File_Load_datetime, Filename FROM `prd_dbi_analytics.improvado.sdi_raw_adobe_byod_flow_programmatic_byod_entry_storepage_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'PROGRAMMATIC', 'ByodEntryByodLandingPageVisitors', TRY_CAST(visitors AS DOUBLE), 'sdi_raw_adobe_byod_flow_programmatic_byod_entry_byodlandingpage_visitors_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prd_dbi_analytics.improvado.sdi_raw_adobe_byod_flow_programmatic_byod_entry_byodlandingpage_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'PROGRAMMATIC', 'ByodEntryOffersSwitchVisitors',    TRY_CAST(visitors AS DOUBLE), 'sdi_raw_adobe_byod_flow_programmatic_byod_entry_offersswitch_visitors_weekly_tmo',  __insert_date, File_Load_datetime, Filename FROM `prd_dbi_analytics.improvado.sdi_raw_adobe_byod_flow_programmatic_byod_entry_offersswitch_visitors_weekly_tmo`

  /* ------------------------------------------------------------------ SOCIAL ------------------------------------------------------------------ */
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'SOCIAL', 'ByodUpvVisitors',               TRY_CAST(visitors AS DOUBLE), 'sdi_raw_adobe_byod_flow_social_byod_uvnb_visitors_weekly_tmo',               __insert_date, File_Load_datetime, Filename FROM `prd_dbi_analytics.improvado.sdi_raw_adobe_byod_flow_social_byod_uvnb_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'SOCIAL', 'ByodEntryByodPageVisitors',       TRY_CAST(visitors AS DOUBLE), 'sdi_raw_adobe_byod_flow_social_byod_entry_byodpage_visitors_weekly_tmo',   __insert_date, File_Load_datetime, Filename FROM `prd_dbi_analytics.improvado.sdi_raw_adobe_byod_flow_social_byod_entry_byodpage_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'SOCIAL', 'ByodEntryHomePageVisitors',       TRY_CAST(visitors AS DOUBLE), 'sdi_raw_adobe_byod_flow_social_byod_entry_homepage_visitors_weekly_tmo',   __insert_date, File_Load_datetime, Filename FROM `prd_dbi_analytics.improvado.sdi_raw_adobe_byod_flow_social_byod_entry_homepage_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'SOCIAL', 'ByodEntryDevicePageVisitors',     TRY_CAST(visitors AS DOUBLE), 'sdi_raw_adobe_byod_flow_social_byod_entry_devicepage_visitors_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prd_dbi_analytics.improvado.sdi_raw_adobe_byod_flow_social_byod_entry_devicepage_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'SOCIAL', 'ByodEntryPlansPageVisitors',      TRY_CAST(visitors AS DOUBLE), 'sdi_raw_adobe_byod_flow_social_byod_entry_planspage_visitors_weekly_tmo',  __insert_date, File_Load_datetime, Filename FROM `prd_dbi_analytics.improvado.sdi_raw_adobe_byod_flow_social_byod_entry_planspage_visitors_weekly_tmo`
  -- OLD OTHER: sdi_raw_adobe_byod_flow_social_byod_entry_otherpage_visitors_weekly_tmo (Old Other = Store + ByodLanding + OffersSwitch + otherpage2)
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'SOCIAL', 'ByodEntryOtherPageVisitors',      TRY_CAST(visitors AS DOUBLE), 'sdi_raw_adobe_byod_flow_social_byod_entry_otherpage2_visitors_weekly_tmo',  __insert_date, File_Load_datetime, Filename FROM `prd_dbi_analytics.improvado.sdi_raw_adobe_byod_flow_social_byod_entry_otherpage2_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'SOCIAL', 'ByodEntryStorePageVisitors',      TRY_CAST(visitors AS DOUBLE), 'sdi_raw_adobe_byod_flow_social_byod_entry_storepage_visitors_weekly_tmo',  __insert_date, File_Load_datetime, Filename FROM `prd_dbi_analytics.improvado.sdi_raw_adobe_byod_flow_social_byod_entry_storepage_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'SOCIAL', 'ByodEntryByodLandingPageVisitors', TRY_CAST(visitors AS DOUBLE), 'sdi_raw_adobe_byod_flow_social_byod_entry_byodlandingpage_visitors_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prd_dbi_analytics.improvado.sdi_raw_adobe_byod_flow_social_byod_entry_byodlandingpage_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'SOCIAL', 'ByodEntryOffersSwitchVisitors',    TRY_CAST(visitors AS DOUBLE), 'sdi_raw_adobe_byod_flow_social_byod_entry_offersswitch_visitors_weekly_tmo',  __insert_date, File_Load_datetime, Filename FROM `prd_dbi_analytics.improvado.sdi_raw_adobe_byod_flow_social_byod_entry_offersswitch_visitors_weekly_tmo`

  /* ------------------------------------------------------------------ OTHER ------------------------------------------------------------------ */
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'OTHER', 'ByodUpvVisitors',               TRY_CAST(visitors AS DOUBLE), 'sdi_raw_adobe_byod_flow_other_byod_uvnb_visitors_weekly_tmo',               __insert_date, File_Load_datetime, Filename FROM `prd_dbi_analytics.improvado.sdi_raw_adobe_byod_flow_other_byod_uvnb_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'OTHER', 'ByodEntryByodPageVisitors',       TRY_CAST(visitors AS DOUBLE), 'sdi_raw_adobe_byod_flow_other_byod_entry_byodpage_visitors_weekly_tmo',   __insert_date, File_Load_datetime, Filename FROM `prd_dbi_analytics.improvado.sdi_raw_adobe_byod_flow_other_byod_entry_byodpage_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'OTHER', 'ByodEntryHomePageVisitors',       TRY_CAST(visitors AS DOUBLE), 'sdi_raw_adobe_byod_flow_other_byod_entry_homepage_visitors_weekly_tmo',   __insert_date, File_Load_datetime, Filename FROM `prd_dbi_analytics.improvado.sdi_raw_adobe_byod_flow_other_byod_entry_homepage_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'OTHER', 'ByodEntryDevicePageVisitors',     TRY_CAST(visitors AS DOUBLE), 'sdi_raw_adobe_byod_flow_other_byod_entry_devicepage_visitors_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prd_dbi_analytics.improvado.sdi_raw_adobe_byod_flow_other_byod_entry_devicepage_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'OTHER', 'ByodEntryPlansPageVisitors',      TRY_CAST(visitors AS DOUBLE), 'sdi_raw_adobe_byod_flow_other_byod_entry_planspage_visitors_weekly_tmo',  __insert_date, File_Load_datetime, Filename FROM `prd_dbi_analytics.improvado.sdi_raw_adobe_byod_flow_other_byod_entry_planspage_visitors_weekly_tmo`
  -- OLD OTHER: sdi_raw_adobe_byod_flow_other_byod_entry_otherpage_visitors_weekly_tmo (Old Other = Store + ByodLanding + OffersSwitch + otherpage2)
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'OTHER', 'ByodEntryOtherPageVisitors',      TRY_CAST(visitors AS DOUBLE), 'sdi_raw_adobe_byod_flow_other_byod_entry_otherpage2_visitors_weekly_tmo',  __insert_date, File_Load_datetime, Filename FROM `prd_dbi_analytics.improvado.sdi_raw_adobe_byod_flow_other_byod_entry_otherpage2_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'OTHER', 'ByodEntryStorePageVisitors',      TRY_CAST(visitors AS DOUBLE), 'sdi_raw_adobe_byod_flow_other_byod_entry_storepage_visitors_weekly_tmo',  __insert_date, File_Load_datetime, Filename FROM `prd_dbi_analytics.improvado.sdi_raw_adobe_byod_flow_other_byod_entry_storepage_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'OTHER', 'ByodEntryByodLandingPageVisitors', TRY_CAST(visitors AS DOUBLE), 'sdi_raw_adobe_byod_flow_other_byod_entry_byodlandingpage_visitors_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM `prd_dbi_analytics.improvado.sdi_raw_adobe_byod_flow_other_byod_entry_byodlandingpage_visitors_weekly_tmo`
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'OTHER', 'ByodEntryOffersSwitchVisitors',    TRY_CAST(visitors AS DOUBLE), 'sdi_raw_adobe_byod_flow_other_byod_entry_offersswitch_visitors_weekly_tmo',  __insert_date, File_Load_datetime, Filename FROM `prd_dbi_analytics.improvado.sdi_raw_adobe_byod_flow_other_byod_entry_offersswitch_visitors_weekly_tmo`

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
  MAX(IF(MetricName = 'ByodUpvVisitors',                MetricValue, NULL)) AS ByodUpvVisitors,
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

END;