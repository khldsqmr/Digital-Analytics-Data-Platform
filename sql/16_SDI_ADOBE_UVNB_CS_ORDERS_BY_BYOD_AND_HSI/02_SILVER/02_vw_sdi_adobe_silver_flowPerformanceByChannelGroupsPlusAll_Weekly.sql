/* =================================================================================================
FILE: 02_vw_sdi_adobe_silver_flowPerformanceByChannelGroupsPlusAll_Weekly.sql
LAYER: Silver View
DATASET: prj-dbi-prd-1.ds_dbi_digitalmedia_automation
VIEW: vw_sdi_adobe_silver_flowPerformanceByChannelGroupsPlusAll_Weekly

SOURCES:
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_adobe_bronze_uvnbCartstartOrdersByAll_Weekly
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_adobe_bronze_uvnbCartstartOrdersByLtcGroups_Weekly
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_adobe_bronze_uvnbTotalByAll_Weekly
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_adobe_bronze_uvnbTotalByChannelGroups_Weekly

DESTINATION:
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_adobe_silver_flowPerformanceByChannelGroupsPlusAll_Weekly

PURPOSE:
  Final weekly Silver view by ChannelGroup plus ALL with Adobe total UVNB and tracked flow metrics.

BUSINESS GRAIN:
  One row per:
      WeekSunSat
      ChannelGroup

BUSINESS RULES:
  - ALL row uses Bronze ALL flow metrics and Bronze ALL total UVNB.
  - ChannelGroup rows use Bronze LTC Groups flow metrics and Bronze ChannelGroup total UVNB.
  - UvnbTotalAdobe comes from the total UVNB stream.
  - UvnbPostpaid / UvnbHsi / UvnbByod come from flow-specific tables.
  - CartstartTotal is Postpaid + HSI + BYOD. No COALESCE — NULL if any component is NULL.
  - OrdersUnassistedTotal is Postpaid + HSI + BYOD unassisted. No COALESCE.
  - OrdersAssistedTotal is Postpaid + HSI + BYOD assisted. No COALESCE.
  - OrdersTotal is OrdersUnassistedTotal + OrdersAssistedTotal.
  - No COALESCE is used anywhere. If any component is NULL, the derived total remains NULL.

COLUMN CHANGES vs PREVIOUS VERSION:
  - OrdersPostpaid        renamed to  OrdersUnassistedPostpaid
  - OrdersHsi             renamed to  OrdersUnassistedHsi
  - OrdersByod            renamed to  OrdersUnassistedByod
  - OrdersTrackedFlowSum  renamed to  OrdersUnassistedTotal
  - OrdersTotal           was unassisted sum — now means grand total (unassisted + assisted)
  - OrdersAssistedPostpaid  ADDED
  - OrdersAssistedHsi       ADDED
  - OrdersAssistedByod      ADDED
  - OrdersAssistedTotal     ADDED
  - OrdersTotal             REDEFINED as OrdersUnassistedTotal + OrdersAssistedTotal

OUTPUT COLUMNS:
  - WeekSunSat
  - ReportingGrain
  - ChannelGroup
  - UvnbTotalAdobe
  - UvnbPostpaid
  - UvnbHsi
  - UvnbByod
  - UvnbTrackedFlowSum
  - CartstartTotal
  - CartstartPostpaid
  - CartstartHsi
  - CartstartByod
  - CartstartTrackedFlowSum
  - OrdersTotal
  - OrdersUnassistedTotal
  - OrdersUnassistedPostpaid
  - OrdersUnassistedHsi
  - OrdersUnassistedByod
  - OrdersAssistedTotal
  - OrdersAssistedPostpaid
  - OrdersAssistedHsi
  - OrdersAssistedByod
================================================================================================= */

CREATE OR REPLACE VIEW
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_adobe_silver_flowPerformanceByChannelGroupsPlusAll_Weekly`
AS

WITH FlowRows AS (

  -- ALL grain
  SELECT
    WeekSunSat,
    'CHANNEL_GROUP' AS ReportingGrain,
    'ALL' AS ChannelGroup,
    UvnbPostpaid,
    UvnbHsi,
    UvnbByod,
    CartstartPostpaid,
    CartstartHsi,
    CartstartByod,
    OrdersUnassistedPostpaid,
    OrdersUnassistedHsi,
    OrdersUnassistedByod,
    OrdersAssistedPostpaid,
    OrdersAssistedHsi,
    OrdersAssistedByod
  FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_adobe_bronze_uvnbCartstartOrdersByAll_Weekly`

  UNION ALL

  -- Channel Group grain
  SELECT
    WeekSunSat,
    'CHANNEL_GROUP' AS ReportingGrain,
    LtcGroup AS ChannelGroup,
    UvnbPostpaid,
    UvnbHsi,
    UvnbByod,
    CartstartPostpaid,
    CartstartHsi,
    CartstartByod,
    OrdersUnassistedPostpaid,
    OrdersUnassistedHsi,
    OrdersUnassistedByod,
    OrdersAssistedPostpaid,
    OrdersAssistedHsi,
    OrdersAssistedByod
  FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_adobe_bronze_uvnbCartstartOrdersByLtcGroups_Weekly`
),

TotalUvnbRows AS (

  SELECT
    WeekSunSat,
    'CHANNEL_GROUP' AS ReportingGrain,
    'ALL' AS ChannelGroup,
    UvnbTotalAdobe
  FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_adobe_bronze_uvnbTotalByAll_Weekly`

  UNION ALL

  SELECT
    WeekSunSat,
    'CHANNEL_GROUP' AS ReportingGrain,
    LtcGroup AS ChannelGroup,
    UvnbTotalAdobe
  FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_adobe_bronze_uvnbTotalByChannelGroups_Weekly`
)

SELECT
  f.WeekSunSat,
  f.ReportingGrain,
  f.ChannelGroup,

  -- Total UVNB (unchanged)
  t.UvnbTotalAdobe,

  -- UVNB flows (unchanged)
  f.UvnbPostpaid,
  f.UvnbHsi,
  f.UvnbByod,
  f.UvnbPostpaid + f.UvnbHsi + f.UvnbByod                                                     AS UvnbTrackedFlowSum,

  -- Cartstart (unchanged)
  f.CartstartPostpaid + f.CartstartHsi + f.CartstartByod                                       AS CartstartTotal,
  f.CartstartPostpaid,
  f.CartstartHsi,
  f.CartstartByod,
  f.CartstartPostpaid + f.CartstartHsi + f.CartstartByod                                       AS CartstartTrackedFlowSum,

  -- Orders grand total (unassisted + assisted)
  (f.OrdersUnassistedPostpaid + f.OrdersUnassistedHsi + f.OrdersUnassistedByod)
  + (f.OrdersAssistedPostpaid + f.OrdersAssistedHsi + f.OrdersAssistedByod)                   AS OrdersTotal,

  -- Orders unassisted branch
  f.OrdersUnassistedPostpaid + f.OrdersUnassistedHsi + f.OrdersUnassistedByod                 AS OrdersUnassistedTotal,
  f.OrdersUnassistedPostpaid,
  f.OrdersUnassistedHsi,
  f.OrdersUnassistedByod,

  -- Orders assisted branch
  f.OrdersAssistedPostpaid + f.OrdersAssistedHsi + f.OrdersAssistedByod                       AS OrdersAssistedTotal,
  f.OrdersAssistedPostpaid,
  f.OrdersAssistedHsi,
  f.OrdersAssistedByod

FROM FlowRows f
LEFT JOIN TotalUvnbRows t
  ON f.WeekSunSat      = t.WeekSunSat
  AND f.ReportingGrain  = t.ReportingGrain
  AND f.ChannelGroup    = t.ChannelGroup;