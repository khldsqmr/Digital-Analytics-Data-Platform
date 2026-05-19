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
  - UvnbTotalAdobe comes from the total UVNB stream (Bronze 04/05).
  - UvnbPostpaid / UvnbHsi / UvnbByod come from flow-specific tables.
  - UvnbFlowTotal comes from the Adobe flow total source table — not computed from LOB flows.
    At LTC grain: NULL placeholder until LTC flow total table is available.
  - CartstartTotal is Postpaid + HSI + BYOD. No COALESCE.
  - OrdersUnassistedTotal is Postpaid + HSI + BYOD unassisted. No COALESCE.
  - OrdersAssistedTotal is Postpaid + HSI + BYOD assisted. No COALESCE.
    At LTC grain: NULL placeholder until LTC assisted tables are ingested.
  - OrdersTotal is OrdersUnassistedTotal + OrdersAssistedTotal.
  - No COALESCE is used anywhere. If any component is NULL, the derived total remains NULL.

COLUMN CHANGES vs PREVIOUS VERSION:
  - UvnbFlowTotal  ADDED (new)

OUTPUT COLUMNS:
  - WeekSunSat
  - ReportingGrain
  - Channel
  - UvnbTotalAdobe
  - UvnbPostpaid
  - UvnbHsi
  - UvnbByod
  - UvnbTrackedFlowSum
  - UvnbFlowTotal
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
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_adobe_silver_flowPerformanceByChannelPlusAll_Weekly`
AS

WITH FlowRows AS (

  -- ALL grain
  SELECT
    WeekSunSat,
    'CHANNEL' AS ReportingGrain,
    'ALL' AS Channel,
    UvnbPostpaid,
    UvnbHsi,
    UvnbByod,
    UvnbFlowTotal,
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

  -- Channel (LTC) grain
  -- NOTE: UvnbFlowTotal and OrdersAssisted* are NULL placeholders at this grain
  SELECT
    WeekSunSat,
    'CHANNEL' AS ReportingGrain,
    LastTouchChannel AS Channel,
    UvnbPostpaid,
    UvnbHsi,
    UvnbByod,
    UvnbFlowTotal,            -- NULL placeholder until LTC flow total table is available
    CartstartPostpaid,
    CartstartHsi,
    CartstartByod,
    OrdersUnassistedPostpaid,
    OrdersUnassistedHsi,
    OrdersUnassistedByod,
    OrdersAssistedPostpaid,   -- NULL placeholder until LTC assisted tables land
    OrdersAssistedHsi,        -- NULL placeholder until LTC assisted tables land
    OrdersAssistedByod        -- NULL placeholder until LTC assisted tables land
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

  -- Total UVNB (from Bronze 04/05 — unchanged)
  t.UvnbTotalAdobe,

  -- UVNB flows (unchanged)
  f.UvnbPostpaid,
  f.UvnbHsi,
  f.UvnbByod,
  f.UvnbPostpaid + f.UvnbHsi + f.UvnbByod                                                     AS UvnbTrackedFlowSum,

  -- UVNB Flow Total (new)
  -- Sourced directly from Adobe flow total table — not computed from LOB flows.
  -- NULL at LTC grain until source table is available.
  f.UvnbFlowTotal,

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

  -- Orders assisted branch (NULL at LTC grain)
  f.OrdersAssistedPostpaid + f.OrdersAssistedHsi + f.OrdersAssistedByod                       AS OrdersAssistedTotal,
  f.OrdersAssistedPostpaid,
  f.OrdersAssistedHsi,
  f.OrdersAssistedByod

FROM FlowRows f
LEFT JOIN TotalUvnbRows t
  ON f.WeekSunSat      = t.WeekSunSat
  AND f.ReportingGrain = t.ReportingGrain
  AND f.Channel        = t.Channel;