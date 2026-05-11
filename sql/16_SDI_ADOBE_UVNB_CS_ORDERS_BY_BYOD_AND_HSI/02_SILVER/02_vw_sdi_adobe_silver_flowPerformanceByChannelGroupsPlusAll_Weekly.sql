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
  - CartstartTotal is currently Postpaid + HSI + BYOD.
  - OrdersTotal is currently Postpaid + HSI + BYOD.
  - No COALESCE is used. If any component is NULL, the calculated total remains NULL.

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
  - OrdersPostpaid
  - OrdersHsi
  - OrdersByod
  - OrdersTrackedFlowSum

================================================================================================= */

CREATE OR REPLACE VIEW
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_adobe_silver_flowPerformanceByChannelGroupsPlusAll_Weekly`
AS

WITH FlowRows AS (

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

    OrdersPostpaid,
    OrdersHsi,
    OrdersByod

  FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_adobe_bronze_uvnbCartstartOrdersByAll_Weekly`

  UNION ALL

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

    OrdersPostpaid,
    OrdersHsi,
    OrdersByod

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

  t.UvnbTotalAdobe,

  f.UvnbPostpaid,
  f.UvnbHsi,
  f.UvnbByod,
  f.UvnbPostpaid + f.UvnbHsi + f.UvnbByod AS UvnbTrackedFlowSum,

  f.CartstartPostpaid + f.CartstartHsi + f.CartstartByod AS CartstartTotal,
  f.CartstartPostpaid,
  f.CartstartHsi,
  f.CartstartByod,
  f.CartstartPostpaid + f.CartstartHsi + f.CartstartByod AS CartstartTrackedFlowSum,

  f.OrdersPostpaid + f.OrdersHsi + f.OrdersByod AS OrdersTotal,
  f.OrdersPostpaid,
  f.OrdersHsi,
  f.OrdersByod,
  f.OrdersPostpaid + f.OrdersHsi + f.OrdersByod AS OrdersTrackedFlowSum

FROM FlowRows f

LEFT JOIN TotalUvnbRows t
ON f.WeekSunSat = t.WeekSunSat
AND f.ReportingGrain = t.ReportingGrain
AND f.ChannelGroup = t.ChannelGroup;