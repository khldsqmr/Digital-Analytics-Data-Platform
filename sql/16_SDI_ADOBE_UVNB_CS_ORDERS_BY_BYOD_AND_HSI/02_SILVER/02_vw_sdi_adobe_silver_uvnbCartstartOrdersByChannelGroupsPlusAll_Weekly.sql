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
  This view combines:
    - ALL_CHANNELS
    - LTC_GROUPS

  ALL_CHANNELS rows are exposed as ChannelGroup = ALL.
  LTC_GROUPS rows are exposed as ChannelGroup = LtcGroup.

BUSINESS GRAIN:
  One row per:
      WeekSunSat
      ChannelGroup

BUSINESS RULES:
  - Uses deduplicated Bronze ALL_CHANNELS and LTC_GROUPS views.
  - ALL_CHANNELS rows are labeled as ChannelGroup = ALL.
  - LTC_GROUPS rows use the standardized LtcGroup value from Bronze.
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

OUTPUT COLUMNS:
  - WeekSunSat
  - ReportingGrain
  - ChannelGroup
  - Uvnb
  - UvnbHsi
  - UvnbByod
  - Cartstart
  - CartstartHsi
  - CartstartByod
  - OrdersAll
  - OrdersHsi
  - OrdersByod

================================================================================================= */

CREATE OR REPLACE VIEW
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_adobe_silver_uvnbCartstartOrdersByChannelGroupsPlusAll_Weekly`
AS

/* -------------------------------------------------------------------------------------------------
   ALL CHANNELS
   - Converts the ALL_CHANNELS Bronze row into ChannelGroup = ALL.
------------------------------------------------------------------------------------------------- */
SELECT
  WeekSunSat,
  'CHANNEL_GROUP' AS ReportingGrain,
  'ALL' AS ChannelGroup,

  Uvnb,
  UvnbHsi,
  UvnbByod,

  Cartstart,
  CartstartHsi,
  CartstartByod,

  OrdersAll,
  OrdersHsi,
  OrdersByod

FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_adobe_bronze_uvnbCartstartOrdersByAll_Weekly`

UNION ALL

/* -------------------------------------------------------------------------------------------------
   LTC GROUPS
   - Converts LtcGroup into the Tableau-facing ChannelGroup column.
------------------------------------------------------------------------------------------------- */
SELECT
  WeekSunSat,
  'CHANNEL_GROUP' AS ReportingGrain,
  LtcGroup AS ChannelGroup,

  Uvnb,
  UvnbHsi,
  UvnbByod,

  Cartstart,
  CartstartHsi,
  CartstartByod,

  OrdersAll,
  OrdersHsi,
  OrdersByod

FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_adobe_bronze_uvnbCartstartOrdersByLtcGroups_Weekly`;