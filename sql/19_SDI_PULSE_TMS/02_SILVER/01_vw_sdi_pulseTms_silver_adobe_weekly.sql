/* =================================================================================================
FILE:         01_vw_sdi_pulseTms_silver_adobe_weekly_long.sql
LAYER:        Silver View — Long
VIEW NAME:    vw_sdi_pulseTms_silver_adobe_weekly_long

PURPOSE:
  Normalized weekly Adobe TMS Silver. One row per week × channel × metric.
  Reporting variants are stored as columns:
    metric_value, metric_value_wow, metric_value_ly, wow_pct, yoy_pct.

WHY LONG SILVER:
  This avoids a very wide Adobe Silver and eliminates a massive Gold UNPIVOT.
  A new metric is normally added as one STRUCT entry in metric_rows.

BUSINESS DENOMINATOR MAP:
  UVNB flow share:
    UvnbFlowTotal / UvnbTotalAdobe

  Total-order site CVR:
    OrdersTotal / UvnbTotalAdobe

  Cart-start CVR:
    total       → CartstartTotal    / UvnbFlowTotal
    Postpaid    → CartstartPostpaid / UvnbPostpaid
    HSI         → CartstartHsi      / UvnbHsi
    BYOD        → CartstartByod     / UvnbByod

  Total-order product CVR:
    Postpaid    → OrdersTotalPostpaid / UvnbPostpaid
    HSI         → OrdersTotalHsi      / UvnbHsi
    BYOD        → OrdersTotalByod     / UvnbByod

  Unassisted-order CVR:
    total       → OrdersUnassistedTotal    / UvnbFlowTotal
    Postpaid    → OrdersUnassistedPostpaid / UvnbPostpaid
    HSI         → OrdersUnassistedHsi      / UvnbHsi
    BYOD        → OrdersUnassistedByod     / UvnbByod

  Assisted-order CVR:
    total       → OrdersAssistedTotal    / UvnbFlowTotal
    Postpaid    → OrdersAssistedPostpaid / UvnbPostpaid
    HSI         → OrdersAssistedHsi      / UvnbHsi
    BYOD        → OrdersAssistedByod     / UvnbByod

  BYOD order mix:
    OrdersUnassistedByod / OrdersUnassistedTotal
    OrdersAssistedByod   / OrdersAssistedTotal

APPLICABILITY:
  All Adobe metrics, including UvnbTotalAdobe, UvnbFlowTotal,
  PctUvnbFlowOfUvnbTotal, and CvrOrdersTotalVsUvnbTotal, are emitted for all seven Adobe channels.

UPSTREAM REQUIREMENT:
  The upstream Adobe source must populate UvnbTotalAdobe and UvnbFlowTotal for every ChannelGroup:
    ALL, PAID SEARCH, ORGANIC SEARCH, DIRECT, SOCIAL, PROGRAMMATIC, OTHER.
  If those fields are NULL outside ChannelGroup = 'ALL', the derived per-channel site metrics will
  correctly remain NULL and the upstream Adobe source must be enhanced before production cutover.
================================================================================================= */

CREATE OR REPLACE VIEW
  `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseTms_silver_adobe_weekly_long`
AS

WITH base AS (
  SELECT
    WeekSunSat AS week_sun_to_sat,
    ChannelGroup,

    CAST(UvnbTotalAdobe AS FLOAT64) AS UvnbTotalAdobe,
    CAST(UvnbFlowTotal AS FLOAT64) AS UvnbFlowTotal,
    CAST(UvnbPostpaid AS FLOAT64) AS UvnbPostpaid,
    CAST(UvnbHsi AS FLOAT64) AS UvnbHsi,
    CAST(UvnbByod AS FLOAT64) AS UvnbByod,
    CAST(UvnbTrackedFlowSum AS FLOAT64) AS UvnbTrackedFlowSum,

    CAST(CartstartTotal AS FLOAT64) AS CartstartTotal,
    CAST(CartstartPostpaid AS FLOAT64) AS CartstartPostpaid,
    CAST(CartstartHsi AS FLOAT64) AS CartstartHsi,
    CAST(CartstartByod AS FLOAT64) AS CartstartByod,

    CAST(OrdersTotal AS FLOAT64) AS OrdersTotal,
    CAST(OrdersUnassistedTotal AS FLOAT64) AS OrdersUnassistedTotal,
    CAST(OrdersUnassistedPostpaid AS FLOAT64) AS OrdersUnassistedPostpaid,
    CAST(OrdersUnassistedHsi AS FLOAT64) AS OrdersUnassistedHsi,
    CAST(OrdersUnassistedByod AS FLOAT64) AS OrdersUnassistedByod,
    CAST(OrdersAssistedTotal AS FLOAT64) AS OrdersAssistedTotal,
    CAST(OrdersAssistedPostpaid AS FLOAT64) AS OrdersAssistedPostpaid,
    CAST(OrdersAssistedHsi AS FLOAT64) AS OrdersAssistedHsi,
    CAST(OrdersAssistedByod AS FLOAT64) AS OrdersAssistedByod
  FROM
    `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_adobe_silver_flowPerformanceByChannelGroupsPlusAll_Weekly`
),

enriched AS (
  SELECT
    *,

    -- Product total orders. Preserve NULL propagation intentionally.
    OrdersUnassistedPostpaid + OrdersAssistedPostpaid AS OrdersTotalPostpaid,
    OrdersUnassistedHsi      + OrdersAssistedHsi      AS OrdersTotalHsi,
    OrdersUnassistedByod     + OrdersAssistedByod     AS OrdersTotalByod,

    -- Site / total-flow metrics.
    ROUND(SAFE_DIVIDE(UvnbFlowTotal, UvnbTotalAdobe), 6) AS PctUvnbFlowOfUvnbTotal,
    ROUND(SAFE_DIVIDE(OrdersTotal, UvnbTotalAdobe), 6) AS CvrOrdersTotalVsUvnbTotal,

    -- Cart-start funnel.
    ROUND(SAFE_DIVIDE(CartstartTotal,    UvnbFlowTotal), 6) AS CvrCartStartTotal,
    ROUND(SAFE_DIVIDE(CartstartPostpaid, UvnbPostpaid), 6)  AS CvrCartStartPostpaid,
    ROUND(SAFE_DIVIDE(CartstartHsi,      UvnbHsi), 6)       AS CvrCartStartHsi,
    ROUND(SAFE_DIVIDE(CartstartByod,     UvnbByod), 6)      AS CvrCartStartByod,

    -- Total-order product funnel.
    ROUND(SAFE_DIVIDE(OrdersUnassistedPostpaid + OrdersAssistedPostpaid, UvnbPostpaid), 6)
      AS CvrOrdersTotalPostpaid,
    ROUND(SAFE_DIVIDE(OrdersUnassistedHsi + OrdersAssistedHsi, UvnbHsi), 6)
      AS CvrOrdersTotalHsi,
    ROUND(SAFE_DIVIDE(OrdersUnassistedByod + OrdersAssistedByod, UvnbByod), 6)
      AS CvrOrdersTotalByod,

    -- Unassisted-order funnel.
    ROUND(SAFE_DIVIDE(OrdersUnassistedTotal,    UvnbFlowTotal), 6) AS CvrOrdersUnassistedTotal,
    ROUND(SAFE_DIVIDE(OrdersUnassistedPostpaid, UvnbPostpaid), 6)  AS CvrOrdersUnassistedPostpaid,
    ROUND(SAFE_DIVIDE(OrdersUnassistedHsi,      UvnbHsi), 6)       AS CvrOrdersUnassistedHsi,
    ROUND(SAFE_DIVIDE(OrdersUnassistedByod,     UvnbByod), 6)      AS CvrOrdersUnassistedByod,

    -- Assisted-order funnel.
    ROUND(SAFE_DIVIDE(OrdersAssistedTotal,    UvnbFlowTotal), 6) AS CvrOrdersAssistedTotal,
    ROUND(SAFE_DIVIDE(OrdersAssistedPostpaid, UvnbPostpaid), 6)  AS CvrOrdersAssistedPostpaid,
    ROUND(SAFE_DIVIDE(OrdersAssistedHsi,      UvnbHsi), 6)       AS CvrOrdersAssistedHsi,
    ROUND(SAFE_DIVIDE(OrdersAssistedByod,     UvnbByod), 6)      AS CvrOrdersAssistedByod,

    -- BYOD order mix.
    ROUND(SAFE_DIVIDE(OrdersUnassistedByod, OrdersUnassistedTotal), 6)
      AS PctOrdersUnassistedByodOfOrdersUnassistedTotal,
    ROUND(SAFE_DIVIDE(OrdersAssistedByod, OrdersAssistedTotal), 6)
      AS PctOrdersAssistedByodOfOrdersAssistedTotal,

    -- UVNB product mix versus total flow.
    ROUND(SAFE_DIVIDE(UvnbByod,     UvnbFlowTotal), 6) AS PctUvnbByodOfUvnbFlow,
    ROUND(SAFE_DIVIDE(UvnbHsi,      UvnbFlowTotal), 6) AS PctUvnbHsiOfUvnbFlow,
    ROUND(SAFE_DIVIDE(UvnbPostpaid, UvnbFlowTotal), 6) AS PctUvnbPostpaidOfUvnbFlow,

    -- UVNB product mix versus tracked product sum.
    ROUND(SAFE_DIVIDE(UvnbByod,     UvnbTrackedFlowSum), 6) AS PctUvnbByodOfTrackedSum,
    ROUND(SAFE_DIVIDE(UvnbHsi,      UvnbTrackedFlowSum), 6) AS PctUvnbHsiOfTrackedSum,
    ROUND(SAFE_DIVIDE(UvnbPostpaid, UvnbTrackedFlowSum), 6) AS PctUvnbPostpaidOfTrackedSum
  FROM base
),

metric_rows AS (
  SELECT
    e.week_sun_to_sat,
    CASE e.ChannelGroup
      WHEN 'ALL'            THEN 'ALL CHANNELS'
      WHEN 'PAID SEARCH'    THEN 'PAID SEARCH'
      WHEN 'ORGANIC SEARCH' THEN 'ORGANIC SEARCH'
      WHEN 'DIRECT'         THEN 'DIRECT'
      WHEN 'SOCIAL'         THEN 'SOCIAL'
      WHEN 'PROGRAMMATIC'   THEN 'PROGRAMMATIC'
      WHEN 'OTHER'          THEN 'OTHER'
      ELSE e.ChannelGroup
    END AS channel,
    m.metric_name,
    m.metric_value
  FROM enriched e
  CROSS JOIN UNNEST([
    -- Existing site-level raw metrics: all seven Adobe channels.
    -- Upstream must populate both values for each ChannelGroup.
    STRUCT('adobe_uvnbTotalAdobe' AS metric_name, e.UvnbTotalAdobe AS metric_value, TRUE AS is_applicable),
    STRUCT('adobe_uvnbFlowTotal', e.UvnbFlowTotal, TRUE),

    -- Existing raw metrics: all seven channels.
    STRUCT('adobe_uvnbPostpaid', e.UvnbPostpaid, TRUE),
    STRUCT('adobe_uvnbHsi', e.UvnbHsi, TRUE),
    STRUCT('adobe_uvnbByod', e.UvnbByod, TRUE),
    STRUCT('adobe_uvnbTrackedFlowSum', e.UvnbTrackedFlowSum, TRUE),

    STRUCT('adobe_cartStartTotal', e.CartstartTotal, TRUE),
    STRUCT('adobe_cartStartPostpaid', e.CartstartPostpaid, TRUE),
    STRUCT('adobe_cartStartHsi', e.CartstartHsi, TRUE),
    STRUCT('adobe_cartStartByod', e.CartstartByod, TRUE),

    STRUCT('adobe_ordersTotal', e.OrdersTotal, TRUE),
    STRUCT('adobe_ordersUnassistedTotal', e.OrdersUnassistedTotal, TRUE),
    STRUCT('adobe_ordersUnassistedPostpaid', e.OrdersUnassistedPostpaid, TRUE),
    STRUCT('adobe_ordersUnassistedHsi', e.OrdersUnassistedHsi, TRUE),
    STRUCT('adobe_ordersUnassistedByod', e.OrdersUnassistedByod, TRUE),
    STRUCT('adobe_ordersAssistedTotal', e.OrdersAssistedTotal, TRUE),
    STRUCT('adobe_ordersAssistedPostpaid', e.OrdersAssistedPostpaid, TRUE),
    STRUCT('adobe_ordersAssistedHsi', e.OrdersAssistedHsi, TRUE),
    STRUCT('adobe_ordersAssistedByod', e.OrdersAssistedByod, TRUE),

    -- Product total orders.
    STRUCT('adobe_ordersTotalPostpaid', e.OrdersTotalPostpaid, TRUE),
    STRUCT('adobe_ordersTotalHsi', e.OrdersTotalHsi, TRUE),
    STRUCT('adobe_ordersTotalByod', e.OrdersTotalByod, TRUE),

    -- Site-level metrics: all seven Adobe channels.
    STRUCT('adobe_pctUvnbFlowOfUvnbTotal', e.PctUvnbFlowOfUvnbTotal, TRUE),
    STRUCT('adobe_cvrOrdersTotalVsUvnbTotal', e.CvrOrdersTotalVsUvnbTotal, TRUE),

    -- Cart-start CVRs.
    STRUCT('adobe_cvrCartStartTotal', e.CvrCartStartTotal, TRUE),
    STRUCT('adobe_cvrCartStartPostpaid', e.CvrCartStartPostpaid, TRUE),
    STRUCT('adobe_cvrCartStartHsi', e.CvrCartStartHsi, TRUE),
    STRUCT('adobe_cvrCartStartByod', e.CvrCartStartByod, TRUE),

    -- Product total-order CVRs.
    STRUCT('adobe_cvrOrdersTotalPostpaid', e.CvrOrdersTotalPostpaid, TRUE),
    STRUCT('adobe_cvrOrdersTotalHsi', e.CvrOrdersTotalHsi, TRUE),
    STRUCT('adobe_cvrOrdersTotalByod', e.CvrOrdersTotalByod, TRUE),

    -- Unassisted-order CVRs.
    STRUCT('adobe_cvrOrdersUnassistedTotal', e.CvrOrdersUnassistedTotal, TRUE),
    STRUCT('adobe_cvrOrdersUnassistedPostpaid', e.CvrOrdersUnassistedPostpaid, TRUE),
    STRUCT('adobe_cvrOrdersUnassistedHsi', e.CvrOrdersUnassistedHsi, TRUE),
    STRUCT('adobe_cvrOrdersUnassistedByod', e.CvrOrdersUnassistedByod, TRUE),

    -- Assisted-order CVRs.
    STRUCT('adobe_cvrOrdersAssistedTotal', e.CvrOrdersAssistedTotal, TRUE),
    STRUCT('adobe_cvrOrdersAssistedPostpaid', e.CvrOrdersAssistedPostpaid, TRUE),
    STRUCT('adobe_cvrOrdersAssistedHsi', e.CvrOrdersAssistedHsi, TRUE),
    STRUCT('adobe_cvrOrdersAssistedByod', e.CvrOrdersAssistedByod, TRUE),

    -- BYOD order mix.
    STRUCT('adobe_pctOrdersUnassistedByodOfOrdersUnassistedTotal',
           e.PctOrdersUnassistedByodOfOrdersUnassistedTotal, TRUE),
    STRUCT('adobe_pctOrdersAssistedByodOfOrdersAssistedTotal',
           e.PctOrdersAssistedByodOfOrdersAssistedTotal, TRUE),

    -- UVNB product mix.
    STRUCT('adobe_pctUvnbByodOfUvnbFlow', e.PctUvnbByodOfUvnbFlow, TRUE),
    STRUCT('adobe_pctUvnbHsiOfUvnbFlow', e.PctUvnbHsiOfUvnbFlow, TRUE),
    STRUCT('adobe_pctUvnbPostpaidOfUvnbFlow', e.PctUvnbPostpaidOfUvnbFlow, TRUE),

    STRUCT('adobe_pctUvnbByodOfTrackedSum', e.PctUvnbByodOfTrackedSum, TRUE),
    STRUCT('adobe_pctUvnbHsiOfTrackedSum', e.PctUvnbHsiOfTrackedSum, TRUE),
    STRUCT('adobe_pctUvnbPostpaidOfTrackedSum', e.PctUvnbPostpaidOfTrackedSum, TRUE)
  ]) m
  WHERE m.is_applicable
),

with_week_num AS (
  SELECT
    *,
    DATE_DIFF(week_sun_to_sat, DATE '2023-01-01', WEEK) AS custom_week_num
  FROM metric_rows
),

max_date AS (
  SELECT MAX(IF(metric_value IS NOT NULL, week_sun_to_sat, NULL)) AS max_data_date
  FROM with_week_num
)

SELECT
  c.week_sun_to_sat,
  'WEEKLY' AS time_granularity,
  'ADOBE' AS data_source,
  c.channel,
  c.metric_name,
  c.metric_value,
  w.metric_value AS metric_value_wow,
  l.metric_value AS metric_value_ly,
  ROUND(SAFE_DIVIDE(c.metric_value - w.metric_value, w.metric_value), 6) AS wow_pct,
  ROUND(SAFE_DIVIDE(c.metric_value - l.metric_value, l.metric_value), 6) AS yoy_pct,
  md.max_data_date
FROM with_week_num c
LEFT JOIN with_week_num w
  ON w.week_sun_to_sat = DATE_SUB(c.week_sun_to_sat, INTERVAL 7 DAY)
 AND w.channel = c.channel
 AND w.metric_name = c.metric_name
LEFT JOIN with_week_num l
  ON l.custom_week_num = c.custom_week_num - 52
 AND l.channel = c.channel
 AND l.metric_name = c.metric_name
CROSS JOIN max_date md
;
