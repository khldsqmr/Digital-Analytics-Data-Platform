/* =================================================================================================
FILE: 02_vw_sdi_adobe_gold_uvnbCartstartOrdersIntentByChannelGroups_Weekly.sql
LAYER: Gold View
DATASET: prj-dbi-prd-1.ds_dbi_digitalmedia_automation
VIEW: vw_sdi_adobe_gold_uvnbCartstartOrdersIntentByChannelGroups_Weekly

SOURCE:
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_adobe_silver_uvnbCartstartOrdersByChannelGroupsPlusAll_Weekly

DESTINATION:
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_adobe_gold_uvnbCartstartOrdersIntentByChannelGroups_Weekly

PURPOSE:
  Weekly business-facing intent and conversion view by ChannelGroup.
  This view compares Postpaid vs BYOD lower-funnel efficiency using UVNB, Cartstart, and Orders.

BUSINESS GRAIN:
  One row per:
      WeekSunSat
      ChannelGroup

BUSINESS RULES:
  - ALL rows come directly from Adobe ALL tables through Bronze/Silver.
  - ChannelGroup rows come directly from Adobe LTC_GROUPS tables through Bronze/Silver.
  - Postpaid and BYOD are independent Adobe flow extracts.
  - BYOD is not treated as a subset of Postpaid.
  - Cartstart per UVNB is used as the main intent signal.
  - Orders per UVNB is used as the end-to-end conversion signal.
  - SAFE_DIVIDE returns NULL where denominator is 0 or NULL.

INTENT INTERPRETATION:
  - BYOD_HIGHER_INTENT when BYOD cartstart-per-UVNB is greater than Postpaid cartstart-per-UVNB.
  - POSTPAID_HIGHER_INTENT when Postpaid cartstart-per-UVNB is greater than BYOD cartstart-per-UVNB.
  - SAME_INTENT when both rates are equal.
  - UNKNOWN when one or both rates are NULL.

================================================================================================= */

CREATE OR REPLACE VIEW
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_adobe_gold_uvnbCartstartOrdersIntentByChannelGroups_Weekly`
AS

WITH Base AS (
  SELECT
    WeekSunSat,
    ReportingGrain,
    ChannelGroup,

    UvnbPostpaid,
    UvnbHsi,
    UvnbByod,

    CartstartPostpaid,
    CartstartHsi,
    CartstartByod,

    OrdersPostpaid,
    OrdersHsi,
    OrdersByod

  FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_adobe_silver_uvnbCartstartOrdersByChannelGroupsPlusAll_Weekly`
),

Calculated AS (
  SELECT
    WeekSunSat,
    ReportingGrain,
    ChannelGroup,

    UvnbPostpaid,
    UvnbHsi,
    UvnbByod,

    CartstartPostpaid,
    CartstartHsi,
    CartstartByod,

    OrdersPostpaid,
    OrdersHsi,
    OrdersByod,

    SAFE_DIVIDE(CartstartPostpaid, UvnbPostpaid) AS CartstartPerUvnbPostpaid,
    SAFE_DIVIDE(CartstartByod, UvnbByod) AS CartstartPerUvnbByod,

    SAFE_DIVIDE(OrdersPostpaid, CartstartPostpaid) AS OrdersPerCartstartPostpaid,
    SAFE_DIVIDE(OrdersByod, CartstartByod) AS OrdersPerCartstartByod,

    SAFE_DIVIDE(OrdersPostpaid, UvnbPostpaid) AS OrdersPerUvnbPostpaid,
    SAFE_DIVIDE(OrdersByod, UvnbByod) AS OrdersPerUvnbByod,

    CartstartByod - CartstartPostpaid AS CartstartByodMinusPostpaid,
    OrdersByod - OrdersPostpaid AS OrdersByodMinusPostpaid,

    SAFE_DIVIDE(
      SAFE_DIVIDE(CartstartByod, UvnbByod),
      SAFE_DIVIDE(CartstartPostpaid, UvnbPostpaid)
    ) AS ByodVsPostpaidCartstartIntentIndex,

    SAFE_DIVIDE(
      SAFE_DIVIDE(OrdersByod, UvnbByod),
      SAFE_DIVIDE(OrdersPostpaid, UvnbPostpaid)
    ) AS ByodVsPostpaidOrderEfficiencyIndex

  FROM Base
)

SELECT
  WeekSunSat,
  ReportingGrain,
  ChannelGroup,

  UvnbPostpaid,
  UvnbHsi,
  UvnbByod,

  CartstartPostpaid,
  CartstartHsi,
  CartstartByod,

  OrdersPostpaid,
  OrdersHsi,
  OrdersByod,

  CartstartPerUvnbPostpaid,
  CartstartPerUvnbByod,

  OrdersPerCartstartPostpaid,
  OrdersPerCartstartByod,

  OrdersPerUvnbPostpaid,
  OrdersPerUvnbByod,

  CartstartByodMinusPostpaid,
  OrdersByodMinusPostpaid,

  ByodVsPostpaidCartstartIntentIndex,
  ByodVsPostpaidOrderEfficiencyIndex,

  CASE
    WHEN CartstartPerUvnbByod IS NULL
      OR CartstartPerUvnbPostpaid IS NULL
      THEN 'UNKNOWN'
    WHEN CartstartPerUvnbByod > CartstartPerUvnbPostpaid
      THEN 'BYOD_HIGHER_INTENT'
    WHEN CartstartPerUvnbByod < CartstartPerUvnbPostpaid
      THEN 'POSTPAID_HIGHER_INTENT'
    ELSE 'SAME_INTENT'
  END AS CartstartIntentSignal,

  CASE
    WHEN OrdersPerUvnbByod IS NULL
      OR OrdersPerUvnbPostpaid IS NULL
      THEN 'UNKNOWN'
    WHEN OrdersPerUvnbByod > OrdersPerUvnbPostpaid
      THEN 'BYOD_HIGHER_ORDER_EFFICIENCY'
    WHEN OrdersPerUvnbByod < OrdersPerUvnbPostpaid
      THEN 'POSTPAID_HIGHER_ORDER_EFFICIENCY'
    ELSE 'SAME_ORDER_EFFICIENCY'
  END AS OrderEfficiencySignal,

  CASE
    WHEN ByodVsPostpaidCartstartIntentIndex IS NULL THEN 'UNKNOWN'
    WHEN ByodVsPostpaidCartstartIntentIndex >= 2 THEN 'BYOD_INTENT_2X_PLUS'
    WHEN ByodVsPostpaidCartstartIntentIndex > 1 THEN 'BYOD_INTENT_HIGHER'
    WHEN ByodVsPostpaidCartstartIntentIndex = 1 THEN 'SAME_INTENT'
    ELSE 'POSTPAID_INTENT_HIGHER'
  END AS CartstartIntentIndexBand,

  CASE
    WHEN ByodVsPostpaidOrderEfficiencyIndex IS NULL THEN 'UNKNOWN'
    WHEN ByodVsPostpaidOrderEfficiencyIndex >= 2 THEN 'BYOD_ORDER_EFFICIENCY_2X_PLUS'
    WHEN ByodVsPostpaidOrderEfficiencyIndex > 1 THEN 'BYOD_ORDER_EFFICIENCY_HIGHER'
    WHEN ByodVsPostpaidOrderEfficiencyIndex = 1 THEN 'SAME_ORDER_EFFICIENCY'
    ELSE 'POSTPAID_ORDER_EFFICIENCY_HIGHER'
  END AS OrderEfficiencyIndexBand

FROM Calculated;