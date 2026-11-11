/* =================================================================================================
FILE:         sdi_sp_adobeFunnel_gold_flowPerformanceByChannelGroups_weekly.sql
LAYER:        Gold Table (via Stored Procedure)
CATALOG.SCHEMA: prdrzranalytics.lab42
TABLE:        sdi_tbl_adobeFunnel_gold_flowPerformanceByChannelGroups_weekly
PROCEDURE:    sdi_sp_adobeFunnel_gold_flowPerformanceByChannelGroups_weekly

SOURCES:
  prdrzranalytics.lab42.sdi_tbl_adobeFunnel_silver_flowPerformanceByLtcGroupsPlusAllChannels_weekly
  prdrzranalytics.lab42.sdi_tbl_adobeFunnel_silver_byodFlowEntryPagesByLtcGroupsPlusAllChannels_weekly
  prdrzranalytics.lab42.sdi_tbl_adobeFunnel_silver_byodFlowOutcomesByLtcGroupsPlusAllChannels_weekly

DESTINATION:
  prdrzranalytics.lab42.sdi_tbl_adobeFunnel_gold_flowPerformanceByChannelGroups_weekly

PURPOSE:
  Gold table consolidating Adobe metrics at ChannelGroup granularity: funnel performance, BYOD, TFB and Metro.
  Single source of truth for Adobe metrics — replaces direct Silver reads in downstream pulse pipelines.

  Joins three Silver tables on WeekSunSat + ChannelGroup:
    Silver flowPerformanceByLtcGroupsPlusAllChannels   : flow performance (UPV, Cartstart, C2C, Chat, EC, Orders) plus TFB and Metro metrics
    Silver byodFlowEntryPagesByLtcGroupsPlusAllChannels : BYOD funnel entry page metrics
    Silver byodFlowOutcomesByLtcGroupsPlusAllChannels   : BYOD funnel outcome metrics

  When HSI or other pulse pipelines are added, they read this Gold table
  and apply their own filtering/naming in their respective Silver layers.

BUSINESS GRAIN:
  One row per:
      WeekSunSat
      ChannelGroup

  ChannelGroup values: All Channels, Direct, Organic Search, Other, Paid Search, Paid Social, Programmatic
  (7 rows per week)

BUSINESS RULES:
  - All Silver tables share the same grain (WeekSunSat + ChannelGroup) so joins are clean with no fanout risk.
    The join relies on every Silver using the same ChannelGroup labels (the funnel Silver's labels).
  - LEFT JOIN used for all non-funnel Silvers — preserves flow-performance rows even if
    a downstream table doesn't yet have data for a given week
  - NULLs preserved throughout — no fake zeroes
  - No aggregation or derivation applied — metrics passed through as-is from Silvers
  - No ORDER BY — applied in downstream pulse Silver tables

OUTPUT COLUMNS:
  WeekSunSat, ChannelGroup

  -- From Silver flowPerformanceByLtcGroupsPlusAllChannels
  UpvTotalAdobe
  UpvPostpaid, UpvHsi, UpvByod, UpvTrackedFlowSum, UpvFlowTotal
  CartstartTotal, CartstartPostpaid, CartstartHsi, CartstartByod, CartstartTrackedFlowSum
  C2cVisitors, ChatEngagementVisitors
  BroadbandEcCompletedPageEvents, BroadbandEcSuccessfulVisits
  OrdersTotal
  OrdersUnassistedTotal, OrdersUnassistedPostpaid, OrdersUnassistedHsi, OrdersUnassistedByod
  OrdersAssistedTotal, OrdersAssistedPostpaid, OrdersAssistedHsi, OrdersAssistedByod
  TfbOrders, TfbCartstartVisits, TfbCartstartVisitors, TfbChatVisitors, TfbC2cVisitors, TfbCreditCheckEvents, TfbLeadgenFormCompleteEvents, TfbVisitorsExclSinglePage
  MetroOrders, MetroCartstartVisits, MetroCartstartVisitors, MetroC2cVisitors, MetroActivationIntentVisitors, MetroEntryHomePageVisitors, MetroEntryPlansPageVisitors, MetroEntryDealsPageVisitors, MetroEntryOtherPageVisitors, MetroProspectVisitors

  -- From Silver byodFlowEntryPagesByLtcGroupsPlusAllChannels
  ByodUpvVisitors
  ByodEntryByodPageVisitors, ByodEntryHomePageVisitors, ByodEntryDevicePageVisitors
  ByodEntryPlansPageVisitors, ByodEntryOtherPageVisitors
  ByodEntryStorePageVisitors, ByodEntryByodLandingPageVisitors, ByodEntryOffersSwitchVisitors

  -- From Silver byodFlowOutcomesByLtcGroupsPlusAllChannels
  ByodVrChatVisitors, ByodCallVisitors, ByodStoreLocatorVisitors
  ByodInternalTmoVisitors, ByodBouncersVisitors, ByodOrders


DOWNSTREAM:
  vw_sdi_pulseByod_silver_adobeFlow_weekly (BYOD medallion, separate pipeline, not yet ported)
  Future: vw_sdi_pulseHsi_silver_adobeFlow_weekly, vw_sdi_pulseMetro_silver_adobeFlow_weekly etc.
================================================================================================= */

CREATE OR REPLACE PROCEDURE
prdrzranalytics.lab42.sdi_sp_adobeFunnel_gold_flowPerformanceByChannelGroups_weekly()
LANGUAGE SQL
SQL SECURITY INVOKER
MODIFIES SQL DATA
AS
BEGIN

  CREATE OR REPLACE TABLE
  prdrzranalytics.lab42.sdi_tbl_adobeFunnel_gold_flowPerformanceByChannelGroups_weekly
  USING DELTA
  AS

  SELECT
    s2.WeekSunSat,
    s2.ChannelGroup,

    -- ================================================================ FLOW PERFORMANCE (Silver flowPerformanceByLtcGroupsPlusAllChannels)
    s2.UpvTotalAdobe,

    -- UPV flows
    s2.UpvPostpaid,
    s2.UpvHsi,
    s2.UpvByod,
    s2.UpvTrackedFlowSum,
    s2.UpvFlowTotal,

    -- Cartstart
    s2.CartstartTotal,
    s2.CartstartPostpaid,
    s2.CartstartHsi,
    s2.CartstartByod,
    s2.CartstartTrackedFlowSum,

    -- C2C / Chat engagement / Broadband Eligibility Check (EC)
    s2.C2cVisitors,
    s2.ChatEngagementVisitors,
    s2.BroadbandEcCompletedPageEvents,
    s2.BroadbandEcSuccessfulVisits,

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

    -- TFB (in Silver flowPerformance)
    s2.TfbOrders,
    s2.TfbCartstartVisits,
    s2.TfbCartstartVisitors,
    s2.TfbChatVisitors,
    s2.TfbC2cVisitors,
    s2.TfbCreditCheckEvents,
    s2.TfbLeadgenFormCompleteEvents,
    s2.TfbVisitorsExclSinglePage,

    -- Metro (in Silver flowPerformance)
    s2.MetroOrders,
    s2.MetroCartstartVisits,
    s2.MetroCartstartVisitors,
    s2.MetroC2cVisitors,
    s2.MetroActivationIntentVisitors,
    s2.MetroEntryHomePageVisitors,
    s2.MetroEntryPlansPageVisitors,
    s2.MetroEntryDealsPageVisitors,
    s2.MetroEntryOtherPageVisitors,
    s2.MetroProspectVisitors,

    -- ================================================================ BYOD ENTRY PAGES (Silver byodFlowEntryPagesByLtcGroupsPlusAllChannels)
    s3.ByodUpvVisitors,
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

  FROM prdrzranalytics.lab42.sdi_tbl_adobeFunnel_silver_flowPerformanceByLtcGroupsPlusAllChannels_weekly s2
  LEFT JOIN prdrzranalytics.lab42.sdi_tbl_adobeFunnel_silver_byodFlowEntryPagesByLtcGroupsPlusAllChannels_weekly s3
    ON s2.WeekSunSat   = s3.WeekSunSat
    AND s2.ChannelGroup = s3.ChannelGroup
  LEFT JOIN prdrzranalytics.lab42.sdi_tbl_adobeFunnel_silver_byodFlowOutcomesByLtcGroupsPlusAllChannels_weekly s4
    ON s2.WeekSunSat   = s4.WeekSunSat
    AND s2.ChannelGroup = s4.ChannelGroup;

END;