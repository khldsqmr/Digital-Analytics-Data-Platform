/* =================================================================================================
FILE: 01_vw_sdi_adobe_gold_flowPerformanceByChannelGroups_weekly.sql
LAYER: Gold View
DATASET: prj-dbi-prd-1.ds_dbi_digitalmedia_automation
VIEW: vw_sdi_adobe_gold_flowPerformanceByChannelGroups_weekly

SOURCES:
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_adobe_silver_flowPerformanceByChannelGroupsPlusAll_Weekly
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_adobe_silver_byodFlowEntryPagesByChannelGroupsPlusAll_Weekly
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_adobe_silver_byodFlowOutcomesByChannelGroupsPlusAll_Weekly

DESTINATION:
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_adobe_gold_flowPerformanceByChannelGroups_weekly

PURPOSE:
  Gold view consolidating all Adobe BYOD-related metrics at ChannelGroup granularity.
  Single source of truth for all Adobe BYOD metrics — replaces direct Silver reads
  in downstream pulse pipelines.

  Joins three Silver views on WeekSunSat + ChannelGroup:
    Silver flowPerformanceByChannelGroupsPlusAll        : existing flow performance (UVNB, Cartstart, EC, Orders) — EC newly added
    Silver byodFlowEntryPagesByChannelGroupsPlusAll      : BYOD funnel entry page metrics
    Silver byodFlowOutcomesByChannelGroupsPlusAll        : BYOD funnel outcome metrics

  When HSI, Metro, or other pulse pipelines are added, they read this Gold view
  and apply their own filtering/naming in their respective Silver layers.

BUSINESS GRAIN:
  One row per:
      WeekSunSat
      ChannelGroup

  ChannelGroup values: ALL, PAID SEARCH, ORGANIC SEARCH, DIRECT, PROGRAMMATIC, SOCIAL, OTHER
  (7 rows per week)

BUSINESS RULES:
  - All three Silver views share the same grain (WeekSunSat + ChannelGroup)
    so joins are clean with no fanout risk
  - LEFT JOIN used for the two BYOD Silver views — preserves flow-performance rows even if
    the BYOD funnel views don't yet have data for a given week
  - NULLs preserved throughout — no fake zeroes
  - No aggregation or derivation applied — metrics passed through as-is from Silvers
  - No ORDER BY — applied in downstream pulse Silver views

OUTPUT COLUMNS:
  WeekSunSat, ChannelGroup

  -- From Silver flowPerformanceByChannelGroupsPlusAll
  UvnbTotalAdobe
  UvnbPostpaid, UvnbHsi, UvnbByod, UvnbTrackedFlowSum, UvnbFlowTotal
  CartstartTotal, CartstartPostpaid, CartstartHsi, CartstartByod, CartstartTrackedFlowSum
  EcCompleted, EcSuccessful
  OrdersTotal
  OrdersUnassistedTotal, OrdersUnassistedPostpaid, OrdersUnassistedHsi, OrdersUnassistedByod
  OrdersAssistedTotal, OrdersAssistedPostpaid, OrdersAssistedHsi, OrdersAssistedByod

  -- From Silver byodFlowEntryPagesByChannelGroupsPlusAll
  ByodUvnbVisitors
  ByodEntryByodPageVisitors, ByodEntryHomePageVisitors, ByodEntryDevicePageVisitors
  ByodEntryPlansPageVisitors, ByodEntryOtherPageVisitors
  ByodEntryStorePageVisitors, ByodEntryByodLandingPageVisitors, ByodEntryOffersSwitchVisitors

  -- From Silver byodFlowOutcomesByChannelGroupsPlusAll
  ByodVrChatVisitors, ByodCallVisitors, ByodStoreLocatorVisitors
  ByodInternalTmoVisitors, ByodBouncersVisitors, ByodOrders

DOWNSTREAM:
  vw_sdi_pulseByod_silver_adobeFlow_weekly (BYOD medallion, separate pipeline)
  Future: vw_sdi_pulseHsi_silver_adobeFlow_weekly, vw_sdi_pulseMetro_silver_adobeFlow_weekly etc.
================================================================================================= */

CREATE OR REPLACE VIEW
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_adobe_gold_flowPerformanceByChannelGroups_weekly`
AS

  SELECT
    s2.WeekSunSat,
    s2.ChannelGroup,

    -- ================================================================ FLOW PERFORMANCE (Silver flowPerformanceByLtcGroupsPlusAllChannels — existing, unchanged)
    s2.UvnbTotalAdobe,

    -- UPV flows
    s2.UvnbPostpaid,
    s2.UvnbHsi,
    s2.UvnbByod,
    s2.UvnbTrackedFlowSum,
    s2.UvnbFlowTotal,

    -- Cartstart
    s2.CartstartTotal,
    s2.CartstartPostpaid,
    s2.CartstartHsi,
    s2.CartstartByod,
    s2.CartstartTrackedFlowSum,

    -- Adobe Experience Cloud (EC) — added after UPV and Cartstart metrics
    s2.EcCompleted,
    s2.EcSuccessful,

    -- Orders
    s2.OrdersTotal,
    s2.OrdersUnassistedTotal,
    s2.OrdersUnassistedPostpaid,
    s2.OrdersUnassistedHsi,
    s2.OrdersUnassistedByod,
    s2.OrdersAssistedTotal,
    s2.OrdersAssistedPostpaid,
    s2.OrdersAssistedHsi,
    s2.OrdersAssistedByod,

    -- ================================================================ BYOD ENTRY PAGES (Silver byodFlowEntryPagesByLtcGroupsPlusAllChannels)
    s3.ByodUvnbVisitors,
    s3.ByodEntryByodPageVisitors,
    s3.ByodEntryHomePageVisitors,
    s3.ByodEntryDevicePageVisitors,
    s3.ByodEntryPlansPageVisitors,
    s3.ByodEntryOtherPageVisitors,
    s3.ByodEntryStorePageVisitors,
    s3.ByodEntryByodLandingPageVisitors,
    s3.ByodEntryOffersSwitchVisitors,

    -- ================================================================ BYOD OUTCOMES (Silver byodFlowOutcomesByLtcGroupsPlusAllChannels)
    s4.ByodVrChatVisitors,
    s4.ByodCallVisitors,
    s4.ByodStoreLocatorVisitors,
    s4.ByodInternalTmoVisitors,
    s4.ByodBouncersVisitors,
    s4.ByodOrders

  FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_adobe_silver_flowPerformanceByChannelGroupsPlusAll_Weekly` s2
  LEFT JOIN `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_adobe_silver_byodFlowEntryPagesByChannelGroupsPlusAll_Weekly` s3
    ON s2.WeekSunSat   = s3.WeekSunSat
    AND s2.ChannelGroup = s3.ChannelGroup
  LEFT JOIN `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_adobe_silver_byodFlowOutcomesByChannelGroupsPlusAll_Weekly` s4
    ON s2.WeekSunSat   = s4.WeekSunSat
    AND s2.ChannelGroup = s4.ChannelGroup;