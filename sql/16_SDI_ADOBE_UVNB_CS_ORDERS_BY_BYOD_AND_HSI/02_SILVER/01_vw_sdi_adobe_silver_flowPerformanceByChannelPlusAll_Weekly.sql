/* =================================================================================================
FILE: 01_vw_sdi_adobe_silver_flowPerformanceByChannelPlusAll_Weekly.sql
LAYER: Silver View
DATASET: prj-dbi-prd-1.ds_dbi_digitalmedia_automation
VIEW: vw_sdi_adobe_silver_flowPerformanceByChannelPlusAll_Weekly

SOURCES:
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_adobe_bronze_uvnbCartstartOrdersByAll_Weekly
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_adobe_bronze_uvnbCartstartOrdersByLtc_Weekly
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_adobe_bronze_uvnbTotalByAll_Weekly
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_adobe_bronze_uvnbTotalByChannel_Weekly

DESTINATION:
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_adobe_silver_flowPerformanceByChannelPlusAll_Weekly

PURPOSE:
  Final weekly Silver view by Channel plus ALL with Adobe total UVNB and tracked flow metrics.

BUSINESS GRAIN:
  One row per:
      WeekSunSat
      Channel

BUSINESS RULES:
  - ALL row uses Bronze ALL flow metrics and Bronze ALL total UVNB.
  - Channel rows use Bronze LTC flow metrics and Bronze Channel total UVNB.
  - UvnbTotalAdobe comes from the total UVNB stream.
  - UvnbPostpaid / UvnbHsi / UvnbByod come from flow-specific tables.
  - CartstartTotal is currently Postpaid + HSI + BYOD.
  - OrdersTotal is currently Postpaid + HSI + BYOD.
  - No COALESCE is used. If any component is NULL, the calculated total remains NULL.

OUTPUT COLUMNS:
  - WeekSunSat
  - ReportingGrain
  - Channel
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
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_adobe_silver_flowPerformanceByChannelPlusAll_Weekly`
AS

WITH FlowRows AS (

  SELECT
    WeekSunSat,
    'CHANNEL' AS ReportingGrain,
    'ALL' AS Channel,

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
    'CHANNEL' AS ReportingGrain,
    LastTouchChannel AS Channel,

    UvnbPostpaid,
    UvnbHsi,
    UvnbByod,

    CartstartPostpaid,
    CartstartHsi,
    CartstartByod,

    OrdersPostpaid,
    OrdersHsi,
    OrdersByod

  FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_adobe_bronze_uvnbCartstartOrdersByLtc_Weekly`
),

TotalUvnbRows AS (

  SELECT
    WeekSunSat,
    'CHANNEL' AS ReportingGrain,
    'ALL' AS Channel,
    UvnbTotalAdobe

  FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_adobe_bronze_uvnbTotalByAll_Weekly`

  UNION ALL

  SELECT
    WeekSunSat,
    'CHANNEL' AS ReportingGrain,
    LastTouchChannel AS Channel,
    UvnbTotalAdobe

  FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_adobe_bronze_uvnbTotalByChannel_Weekly`
)

SELECT
  f.WeekSunSat,
  f.ReportingGrain,
  f.Channel,

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
AND f.Channel = t.Channel;