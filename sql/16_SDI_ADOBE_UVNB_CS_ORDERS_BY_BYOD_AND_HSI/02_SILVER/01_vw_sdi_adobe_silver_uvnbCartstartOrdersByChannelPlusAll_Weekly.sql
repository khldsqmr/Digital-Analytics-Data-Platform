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
  This view combines:
    - ALL_CHANNELS
    - LAST_TOUCH_CHANNEL

  ALL_CHANNELS rows are exposed as Channel = ALL.
  LAST_TOUCH_CHANNEL rows are exposed as Channel = LastTouchChannel.

BUSINESS GRAIN:
  One row per:
      WeekSunSat
      Channel

BUSINESS RULES:
  - Uses deduplicated Bronze ALL_CHANNELS and LAST_TOUCH_CHANNEL views.
  - ALL_CHANNELS rows are labeled as Channel = ALL.
  - LAST_TOUCH_CHANNEL rows use the standardized LastTouchChannel value from Bronze.
  - Missing metric values remain NULL.
  - Bronze lineage/load metadata is not exposed in Silver.

OUTPUT COLUMNS:
  - WeekSunSat
  - ReportingGrain
  - Channel
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
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_adobe_silver_uvnbCartstartOrdersByChannelPlusAll_Weekly`
AS

/* -------------------------------------------------------------------------------------------------
   ALL CHANNELS
   - Converts the ALL_CHANNELS Bronze row into Channel = ALL.
------------------------------------------------------------------------------------------------- */
SELECT
  WeekSunSat,
  'CHANNEL' AS ReportingGrain,
  'ALL' AS Channel,

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
   LAST TOUCH CHANNEL
   - Converts LastTouchChannel into the Tableau-facing Channel column.
------------------------------------------------------------------------------------------------- */
SELECT
  WeekSunSat,
  'CHANNEL' AS ReportingGrain,
  LastTouchChannel AS Channel,

  Uvnb,
  UvnbHsi,
  UvnbByod,

  Cartstart,
  CartstartHsi,
  CartstartByod,

  OrdersAll,
  OrdersHsi,
  OrdersByod

FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_adobe_bronze_uvnbCartstartOrdersByLtc_Weekly`;