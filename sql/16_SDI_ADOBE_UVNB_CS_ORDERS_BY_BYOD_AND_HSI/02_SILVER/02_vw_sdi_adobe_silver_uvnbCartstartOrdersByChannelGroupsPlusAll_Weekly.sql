/* =================================================================================================
FILE: 02_vw_sdi_adobe_silver_uvnbCartstartOrdersByChannelGroupsPlusAll_Weekly.sql
LAYER: Silver View
DATASET: prj-dbi-prd-1.ds_dbi_digitalmedia_automation
VIEW: vw_sdi_adobe_silver_uvnbCartstartOrdersByChannelGroupsPlusAll_Weekly

SOURCES:
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_adobe_bronze_uvnbCartstartOrdersByAll_Weekly
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_adobe_bronze_uvnbCartstartOrdersByLtcGroups_Weekly

DESTINATION:
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_adobe_silver_uvnbCartstartOrdersByChannelGroupsPlusAll_Weekly

PURPOSE:
  Tableau-ready weekly Adobe UVNB, Cartstart, and Orders view by ChannelGroup.
  This view combines ALL_CHANNELS and LTC_GROUPS into one reporting object.

BUSINESS GRAIN:
  One row per:
      WeekSunSat
      ChannelGroup

BUSINESS RULES:
  - ALL_CHANNELS rows are exposed as ChannelGroup = ALL.
  - LTC_GROUPS rows are exposed as ChannelGroup = LtcGroup.
  - ALL values come directly from Adobe ALL tables through Bronze ALL.
  - ChannelGroup values come directly from Adobe group tables through Bronze LTC_GROUPS.
  - Postpaid / HSI / BYOD columns are separate metrics and are not summed together.
  - Missing metric values remain NULL.
  - Bronze lineage/load metadata is not exposed in Silver.

CHANNEL GROUPS:
  - ALL
  - DIRECT
  - ORGANIC SEARCH
  - OTHER
  - PAID SEARCH
  - PROGRAMMATIC
  - SOCIAL

================================================================================================= */

CREATE OR REPLACE VIEW
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_adobe_silver_uvnbCartstartOrdersByChannelGroupsPlusAll_Weekly`
AS

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

FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_adobe_bronze_uvnbCartstartOrdersByLtcGroups_Weekly`;