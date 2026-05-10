/* =================================================================================================
FILE: 01_vw_sdi_adobe_silver_uvnbCartstartOrdersByChannelPlusAll_Weekly.sql
LAYER: Silver View
DATASET: prj-dbi-prd-1.ds_dbi_digitalmedia_automation
VIEW: vw_sdi_adobe_silver_uvnbCartstartOrdersByChannelPlusAll_Weekly

SOURCES:
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_adobe_bronze_uvnbCartstartOrdersByAll_Weekly
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_adobe_bronze_uvnbCartstartOrdersByLtc_Weekly

DESTINATION:
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_adobe_silver_uvnbCartstartOrdersByChannelPlusAll_Weekly

PURPOSE:
  Tableau-ready weekly Adobe UVNB, Cartstart, and Orders view by Channel.
  This view combines ALL_CHANNELS and LAST_TOUCH_CHANNEL into one reporting object.

BUSINESS GRAIN:
  One row per:
      WeekSunSat
      Channel

BUSINESS RULES:
  - ALL_CHANNELS rows are exposed as Channel = ALL.
  - LAST_TOUCH_CHANNEL rows are exposed as Channel = LastTouchChannel.
  - ALL values come directly from Adobe ALL tables through Bronze ALL.
  - Postpaid / HSI / BYOD columns are separate metrics and are not summed together.
  - Missing metric values remain NULL.
  - Bronze lineage/load metadata is not exposed in Silver.

================================================================================================= */

CREATE OR REPLACE VIEW
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_adobe_silver_uvnbCartstartOrdersByChannelPlusAll_Weekly`
AS

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

FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_adobe_bronze_uvnbCartstartOrdersByLtc_Weekly`;