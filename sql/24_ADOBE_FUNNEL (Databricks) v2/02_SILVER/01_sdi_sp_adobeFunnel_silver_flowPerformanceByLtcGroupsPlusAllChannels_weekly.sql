/* =================================================================================================
FILE:         01_sdi_sp_adobeFunnel_silver_flowPerformanceByLtcGroupsPlusAllChannels_weekly.sql
LAYER:        Silver Table (via Stored Procedure)
CATALOG.SCHEMA: prdrzranalytics.lab42
TABLE:        sdi_tbl_adobeFunnel_silver_flowPerformanceByLtcGroupsPlusAllChannels_weekly
PROCEDURE:    sdi_sp_adobeFunnel_silver_flowPerformanceByLtcGroupsPlusAllChannels_weekly

SOURCES:
  prdrzranalytics.lab42.sdi_tbl_adobeFunnel_bronze_upvFunnelByAllChannels_weekly
  prdrzranalytics.lab42.sdi_tbl_adobeFunnel_bronze_upvFunnelByLtcGroups_weekly
  prdrzranalytics.lab42.sdi_tbl_adobeFunnel_bronze_upvTotalByAllChannels_weekly
  prdrzranalytics.lab42.sdi_tbl_adobeFunnel_bronze_upvTotalByLtcGroups_weekly
  prdrzranalytics.lab42.sdi_tbl_adobeFunnel_bronze_tfbMetroByAllChannels_weekly
  prdrzranalytics.lab42.sdi_tbl_adobeFunnel_bronze_tfbMetroByLtcGroups_weekly

DESTINATION:
  prdrzranalytics.lab42.sdi_tbl_adobeFunnel_silver_flowPerformanceByLtcGroupsPlusAllChannels_weekly

PURPOSE:
  Final weekly Silver table by ChannelGroup plus ALL with Adobe total UPV, tracked flow metrics, and the
  TFB and Metro metrics, so all Adobe metrics (except the BYOD entry-page and outcome Silvers) are in one table.

BUSINESS GRAIN:
  One row per:
      WeekSunSat
      ChannelGroup

BUSINESS RULES:
  - ALL row uses Bronze ALL flow metrics and Bronze ALL total UPV.
  - ChannelGroup rows use Bronze LTC Groups flow metrics and Bronze ChannelGroup total UPV.
  - UpvTotalAdobe comes from the total UPV stream (Bronze upvTotalByAllChannel / upvTotalByLtcGroups).
  - UpvPostpaid / UpvHsi / UpvByod come from flow-specific tables.
  - UpvFlowTotal comes from the Adobe flow total source table — not computed from LOB flows.
  - C2cVisitors (site-wide click to call) and ChatEngagementVisitors (catch-all chat: VR + Care) are
    Adobe visitor counts passed through directly. Not derived, not part of any total.
  - BroadbandEcCompletedPageEvents (page_events) and BroadbandEcSuccessfulVisits (visits) are broadband
    Eligibility Check (EC) metrics passed through directly. Different units — do not divide one by the other.
  - TFB (Tfb*) and Metro (Metro*) columns are passed through from the combined TFB + Metro Bronze tables and LEFT JOINed on
    WeekSunSat + ReportingGrain + ChannelGroup, the same way total UPV is. Each Bronze is unique per week + group,
    so the join cannot fan out. They are separate brands: never add TFB and Metro together, never add them to
    the Postpaid / HSI / BYOD columns, and do not mix units (Visitors / Visits / Orders / Events) in ratios.
    A week with no TFB or Metro data keeps its funnel row with NULLs.
  - CartstartTotal is Postpaid + HSI + BYOD. No COALESCE.
  - OrdersUnassistedTotal is Postpaid + HSI + BYOD unassisted. No COALESCE.
  - OrdersAssistedTotal is Postpaid + HSI + BYOD assisted. No COALESCE.
  - OrdersTotal is OrdersUnassistedTotal + OrdersAssistedTotal.
  - No COALESCE is used anywhere. If any component is NULL, the derived total remains NULL.

OUTPUT COLUMNS:
  - WeekSunSat
  - ReportingGrain
  - ChannelGroup
  - UpvTotalAdobe
  - UpvPostpaid
  - UpvHsi
  - UpvByod
  - UpvTrackedFlowSum
  - UpvFlowTotal
  - CartstartTotal
  - CartstartPostpaid
  - CartstartHsi
  - CartstartByod
  - CartstartTrackedFlowSum
  - C2cVisitors
  - ChatEngagementVisitors
  - BroadbandEcCompletedPageEvents
  - BroadbandEcSuccessfulVisits
  - OrdersTotal
  - OrdersUnassistedTotal
  - OrdersUnassistedPostpaid
  - OrdersUnassistedHsi
  - OrdersUnassistedByod
  - OrdersAssistedTotal
  - OrdersAssistedPostpaid
  - OrdersAssistedHsi
  - OrdersAssistedByod
  - TfbOrders
  - TfbCartstartVisits
  - TfbCartstartVisitors
  - TfbChatVisitors
  - TfbC2cVisitors
  - TfbCreditCheckEvents
  - TfbLeadgenFormCompleteEvents
  - TfbVisitorsExclSinglePage
  - MetroOrders
  - MetroCartstartVisits
  - MetroCartstartVisitors
  - MetroC2cVisitors
  - MetroActivationIntentVisitors
  - MetroEntryHomePageVisitors
  - MetroEntryPlansPageVisitors
  - MetroEntryDealsPageVisitors
  - MetroEntryOtherPageVisitors
  - MetroProspectVisitors

DOWNSTREAM:
  Gold: sdi_tbl_adobeFunnel_gold_flowPerformanceByChannelGroups_weekly
================================================================================================= */

CREATE OR REPLACE PROCEDURE
prdrzranalytics.lab42.sdi_sp_adobeFunnel_silver_flowPerformanceByLtcGroupsPlusAllChannels_weekly()
LANGUAGE SQL
SQL SECURITY INVOKER
MODIFIES SQL DATA
AS
BEGIN

  CREATE OR REPLACE TABLE
  prdrzranalytics.lab42.sdi_tbl_adobeFunnel_silver_flowPerformanceByLtcGroupsPlusAllChannels_weekly
  USING DELTA
  AS

  WITH FlowRows AS (

    -- ALL grain
    SELECT
      WeekSunSat,
      'CHANNEL_GROUP' AS ReportingGrain,
      'All Channels' AS ChannelGroup,
      UpvPostpaid,
      UpvHsi,
      UpvByod,
      UpvFlowTotal,
      CartstartPostpaid,
      CartstartHsi,
      CartstartByod,
      C2cVisitors,
      ChatEngagementVisitors,
      BroadbandEcCompletedPageEvents,
      BroadbandEcSuccessfulVisits,
      OrdersUnassistedPostpaid,
      OrdersUnassistedHsi,
      OrdersUnassistedByod,
      OrdersAssistedPostpaid,
      OrdersAssistedHsi,
      OrdersAssistedByod
    FROM prdrzranalytics.lab42.sdi_tbl_adobeFunnel_bronze_upvFunnelByAllChannels_weekly

    UNION ALL

    -- Channel Group grain
    SELECT
      WeekSunSat,
      'CHANNEL_GROUP' AS ReportingGrain,
      LtcGroup AS ChannelGroup,
      UpvPostpaid,
      UpvHsi,
      UpvByod,
      UpvFlowTotal,
      CartstartPostpaid,
      CartstartHsi,
      CartstartByod,
      C2cVisitors,
      ChatEngagementVisitors,
      BroadbandEcCompletedPageEvents,
      BroadbandEcSuccessfulVisits,
      OrdersUnassistedPostpaid,
      OrdersUnassistedHsi,
      OrdersUnassistedByod,
      OrdersAssistedPostpaid,
      OrdersAssistedHsi,
      OrdersAssistedByod
    FROM prdrzranalytics.lab42.sdi_tbl_adobeFunnel_bronze_upvFunnelByLtcGroups_weekly
  ),

  TotalUpvRows AS (

    SELECT
      WeekSunSat,
      'CHANNEL_GROUP' AS ReportingGrain,
      'All Channels' AS ChannelGroup,
      UpvTotalAdobe
    FROM prdrzranalytics.lab42.sdi_tbl_adobeFunnel_bronze_upvTotalByAllChannels_weekly

    UNION ALL

    SELECT
      WeekSunSat,
      'CHANNEL_GROUP' AS ReportingGrain,
      LtcGroup AS ChannelGroup,
      UpvTotalAdobe
    FROM prdrzranalytics.lab42.sdi_tbl_adobeFunnel_bronze_upvTotalByLtcGroups_weekly
  ),

  TfbMetroRows AS (
    SELECT
      WeekSunSat,
      'CHANNEL_GROUP' AS ReportingGrain,
      'All Channels' AS ChannelGroup,
      TfbOrders,
      TfbCartstartVisits,
      TfbCartstartVisitors,
      TfbChatVisitors,
      TfbC2cVisitors,
      TfbCreditCheckEvents,
      TfbLeadgenFormCompleteEvents,
      TfbVisitorsExclSinglePage,
      MetroOrders,
      MetroCartstartVisits,
      MetroCartstartVisitors,
      MetroC2cVisitors,
      MetroActivationIntentVisitors,
      MetroEntryHomePageVisitors,
      MetroEntryPlansPageVisitors,
      MetroEntryDealsPageVisitors,
      MetroEntryOtherPageVisitors,
      MetroProspectVisitors
    FROM prdrzranalytics.lab42.sdi_tbl_adobeFunnel_bronze_tfbMetroByAllChannels_weekly

    UNION ALL

    SELECT
      WeekSunSat,
      'CHANNEL_GROUP' AS ReportingGrain,
      LtcGroup AS ChannelGroup,
      TfbOrders,
      TfbCartstartVisits,
      TfbCartstartVisitors,
      TfbChatVisitors,
      TfbC2cVisitors,
      TfbCreditCheckEvents,
      TfbLeadgenFormCompleteEvents,
      TfbVisitorsExclSinglePage,
      MetroOrders,
      MetroCartstartVisits,
      MetroCartstartVisitors,
      MetroC2cVisitors,
      MetroActivationIntentVisitors,
      MetroEntryHomePageVisitors,
      MetroEntryPlansPageVisitors,
      MetroEntryDealsPageVisitors,
      MetroEntryOtherPageVisitors,
      MetroProspectVisitors
    FROM prdrzranalytics.lab42.sdi_tbl_adobeFunnel_bronze_tfbMetroByLtcGroups_weekly
  )

  SELECT
    f.WeekSunSat,
    f.ReportingGrain,
    f.ChannelGroup,

    -- Total UPV (from Bronze upvTotalByAllChannel / upvTotalByLtcGroups — unchanged)
    t.UpvTotalAdobe,

    -- UPV flows
    f.UpvPostpaid,
    f.UpvHsi,
    f.UpvByod,
    f.UpvPostpaid + f.UpvHsi + f.UpvByod                                                     AS UpvTrackedFlowSum,

    -- UPV Flow Total
    -- Sourced directly from Adobe flow total table — not computed from LOB flows.
    f.UpvFlowTotal,

    -- Cartstart
    f.CartstartPostpaid + f.CartstartHsi + f.CartstartByod                                     AS CartstartTotal,
    f.CartstartPostpaid,
    f.CartstartHsi,
    f.CartstartByod,
    f.CartstartPostpaid + f.CartstartHsi + f.CartstartByod                                     AS CartstartTrackedFlowSum,

    -- C2C / Chat engagement / Broadband Eligibility Check (EC) — straight pass-through, independent of totals
    f.C2cVisitors,
    f.ChatEngagementVisitors,
    f.BroadbandEcCompletedPageEvents,
    f.BroadbandEcSuccessfulVisits,

    -- Orders grand total (unassisted + assisted)
    (f.OrdersUnassistedPostpaid + f.OrdersUnassistedHsi + f.OrdersUnassistedByod)
    + (f.OrdersAssistedPostpaid + f.OrdersAssistedHsi + f.OrdersAssistedByod)                 AS OrdersTotal,

    -- Orders unassisted branch
    f.OrdersUnassistedPostpaid + f.OrdersUnassistedHsi + f.OrdersUnassistedByod               AS OrdersUnassistedTotal,
    f.OrdersUnassistedPostpaid,
    f.OrdersUnassistedHsi,
    f.OrdersUnassistedByod,

    -- Orders assisted branch
    f.OrdersAssistedPostpaid + f.OrdersAssistedHsi + f.OrdersAssistedByod                     AS OrdersAssistedTotal,
    f.OrdersAssistedPostpaid,
    f.OrdersAssistedHsi,
    f.OrdersAssistedByod,

    -- TFB (Bronze tfbMetro, pass-through)
    tm.TfbOrders,
    tm.TfbCartstartVisits,
    tm.TfbCartstartVisitors,
    tm.TfbChatVisitors,
    tm.TfbC2cVisitors,
    tm.TfbCreditCheckEvents,
    tm.TfbLeadgenFormCompleteEvents,
    tm.TfbVisitorsExclSinglePage,

    -- Metro (Bronze tfbMetro, pass-through)
    tm.MetroOrders,
    tm.MetroCartstartVisits,
    tm.MetroCartstartVisitors,
    tm.MetroC2cVisitors,
    tm.MetroActivationIntentVisitors,
    tm.MetroEntryHomePageVisitors,
    tm.MetroEntryPlansPageVisitors,
    tm.MetroEntryDealsPageVisitors,
    tm.MetroEntryOtherPageVisitors,
    tm.MetroProspectVisitors

  FROM FlowRows f
  LEFT JOIN TotalUpvRows t
    ON  f.WeekSunSat      = t.WeekSunSat
    AND f.ReportingGrain  = t.ReportingGrain
    AND f.ChannelGroup    = t.ChannelGroup
  LEFT JOIN TfbMetroRows tm
    ON  f.WeekSunSat      = tm.WeekSunSat
    AND f.ReportingGrain  = tm.ReportingGrain
    AND f.ChannelGroup    = tm.ChannelGroup;

END;