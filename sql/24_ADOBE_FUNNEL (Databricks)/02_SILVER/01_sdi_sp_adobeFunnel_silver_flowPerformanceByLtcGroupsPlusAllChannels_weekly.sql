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

DESTINATION:
  prdrzranalytics.lab42.sdi_tbl_adobeFunnel_silver_flowPerformanceByLtcGroupsPlusAllChannels_weekly

PURPOSE:
  Final weekly Silver table by ChannelGroup plus ALL with Adobe total UPV and tracked flow metrics.

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
  - EcCompleted / EcSuccessful are Adobe Experience Cloud metrics, passed through
    directly after the UPV and Cartstart metrics (before Orders) — no derivation, independent
    of the UPV/Cartstart/Orders funnel.
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
  - EcCompleted
  - EcSuccessful
  - OrdersTotal
  - OrdersUnassistedTotal
  - OrdersUnassistedPostpaid
  - OrdersUnassistedHsi
  - OrdersUnassistedByod
  - OrdersAssistedTotal
  - OrdersAssistedPostpaid
  - OrdersAssistedHsi
  - OrdersAssistedByod

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
      EcCompleted,
      EcSuccessful,
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
      EcCompleted,
      EcSuccessful,
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

    -- Adobe Experience Cloud (EC) — added after UPV and Cartstart metrics, passthrough only
    f.EcCompleted,
    f.EcSuccessful,

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
    f.OrdersAssistedByod

  FROM FlowRows f
  LEFT JOIN TotalUpvRows t
    ON  f.WeekSunSat      = t.WeekSunSat
    AND f.ReportingGrain  = t.ReportingGrain
    AND f.ChannelGroup    = t.ChannelGroup;

END;