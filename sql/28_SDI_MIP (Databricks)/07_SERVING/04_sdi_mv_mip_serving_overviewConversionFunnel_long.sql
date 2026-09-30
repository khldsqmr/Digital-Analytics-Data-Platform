-- ============================================================================
-- FILE  : 04_sdi_mv_mip_serving_overviewConversionFunnel_long.sql
-- LAYER : SERVING / MATERIALIZED
-- TAB   : Overview
-- SECTION: Conversion funnel over time
-- PURPOSE:
--   Comparator-aware funnel stage and adjacent conversion metrics.
--   The same rows support Prior week, 4-wk trend and Same wk LY. A two-factor
--   traffic/conversion order-change decomposition is included and explicitly marked proposed.
--
-- REFRESH:
--   TRIGGER ON UPDATE keeps this object independent of browser/API refreshes.
--   AT MOST EVERY INTERVAL 1 MINUTE prevents refresh storms while remaining
--   event-driven. REFRESH POLICY AUTO lets Databricks choose incremental vs full.
--
-- NAMING:
--   sdi_mv_mip_serving_<tab><Section>_<shape>
--   Refresh cadence is intentionally not encoded in the object name.
-- ============================================================================

CREATE OR REPLACE MATERIALIZED VIEW prdrzranalytics.lab42.sdi_mv_mip_serving_overviewConversionFunnel_long
COMMENT 'MIP Overview conversion funnel over time. Comparator-aware stages, rates and order-change decomposition.'
CLUSTER BY (targetWeekStartDate, comparisonType, funnelRowType, funnelStepOrder)
REFRESH POLICY AUTO
TRIGGER ON UPDATE AT MOST EVERY INTERVAL 1 MINUTE
AS
WITH funnelMap AS (
    SELECT * FROM VALUES
      ('nbv',                     'stage',      'Total UPV',             10),
      ('nbvBuyFlow',              'stage',      'UPV Buy Flow',          20),
      ('nbvConfigure',            'stage',      'Configure',             30),
      ('nbvCheckoutStart',        'stage',      'Checkout Start',        40),
      ('orders',                  'stage',      'Orders',                50),
      ('nbvBuyFlowPerNbv',        'conversion', 'UPV → Buy Flow',        110),
      ('buyFlowToConfigureRate',  'conversion', 'Buy Flow → Configure', 120),
      ('configureToCheckoutRate', 'conversion', 'Configure → Checkout', 130),
      ('checkoutToOrderRate',     'conversion', 'Checkout → Order',     140)
    AS t(metricName,funnelRowType,funnelDisplayLabel,funnelStepOrder)
),
base AS (
    SELECT
        g.targetWeekStartDate,g.targetWeekEndDate,g.fiscalQuarterLabel,g.fiscalWeekCode,g.weekLabel,
        c.weekEndingLabel,c.priorWeekStartDate,c.fourWeekAvgStartDate,c.fourWeekAvgEndDate,c.sameWeekLastYearStartDate,
        g.filterLob,g.filterPlatform,
        g.metricName,g.metricLabel,f.funnelDisplayLabel,f.funnelRowType,f.funnelStepOrder,
        mc.metricDescription,g.metricKind,g.displayFormat,g.changeUnit,mc.definitionStatus,
        g.thisWeekNumerator,g.thisWeekDenominator,g.priorWeekNumerator,g.priorWeekDenominator,
        g.fourWeekTrendNumerator,g.fourWeekTrendDenominator,g.sameWeekLyNumerator,g.sameWeekLyDenominator,
        g.thisWeekDataAvailable,g.priorWeekDataAvailable,g.fourWeekTrendWeekCount,g.sameWeekLyDataAvailable,
        g.goldProcessedAt
    FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_overviewMetricIngredientsByWeek_long g
    JOIN funnelMap f ON f.metricName=g.metricName
    JOIN prdrzranalytics.lab42.sdi_vw_mip_control_metricCatalog_static mc ON mc.metricName=g.metricName AND mc.isActive
    LEFT JOIN prdrzranalytics.lab42.sdi_vw_mip_control_fiscalCalendar_static c ON c.weekStartDate=g.targetWeekStartDate
),

comparisonLong AS (
    SELECT
        base.*,
        'priorWeek' AS comparisonType,
        'Prior week' AS comparisonLabel,
        10 AS comparisonSortOrder,
        priorWeekStartDate AS comparisonStartDate,
        date_add(priorWeekStartDate, 6) AS comparisonEndDate,
        1 AS comparisonWeekCount,
        priorWeekDataAvailable AS comparisonDataAvailable,
        priorWeekDataAvailable AS comparisonWindowComplete,
        priorWeekNumerator AS comparisonNumerator,
        priorWeekDenominator AS comparisonDenominator
    FROM base

    UNION ALL

    SELECT
        base.*,
        'fourWeek' AS comparisonType,
        '4-wk trend' AS comparisonLabel,
        20 AS comparisonSortOrder,
        fourWeekAvgStartDate AS comparisonStartDate,
        fourWeekAvgEndDate AS comparisonEndDate,
        fourWeekTrendWeekCount AS comparisonWeekCount,
        fourWeekTrendWeekCount > 0 AS comparisonDataAvailable,
        fourWeekTrendWeekCount = 4 AS comparisonWindowComplete,
        CASE
            WHEN metricKind = 'count' AND fourWeekTrendWeekCount > 0
                THEN try_divide(fourWeekTrendNumerator, cast(fourWeekTrendWeekCount AS DOUBLE))
            ELSE fourWeekTrendNumerator
        END AS comparisonNumerator,
        CASE
            WHEN metricKind = 'count' THEN NULL
            ELSE fourWeekTrendDenominator
        END AS comparisonDenominator
    FROM base

    UNION ALL

    SELECT
        base.*,
        'lastYear' AS comparisonType,
        'Same wk LY' AS comparisonLabel,
        30 AS comparisonSortOrder,
        sameWeekLastYearStartDate AS comparisonStartDate,
        date_add(sameWeekLastYearStartDate, 6) AS comparisonEndDate,
        1 AS comparisonWeekCount,
        sameWeekLyDataAvailable AS comparisonDataAvailable,
        sameWeekLyDataAvailable AS comparisonWindowComplete,
        sameWeekLyNumerator AS comparisonNumerator,
        sameWeekLyDenominator AS comparisonDenominator
    FROM base
),
valuesCalculated AS (
    SELECT
        *,
        CASE
            WHEN metricKind = 'ratio' THEN try_divide(thisWeekNumerator, thisWeekDenominator)
            ELSE thisWeekNumerator
        END AS currentValue,
        CASE
            WHEN metricKind = 'ratio' THEN try_divide(comparisonNumerator, comparisonDenominator)
            ELSE comparisonNumerator
        END AS comparisonValue
    FROM comparisonLong
),
deltasCalculated AS (
    SELECT
        *,
        currentValue - comparisonValue AS absoluteDeltaValue,
        CASE
            WHEN NOT thisWeekDataAvailable OR NOT comparisonDataAvailable
              OR currentValue IS NULL OR comparisonValue IS NULL THEN NULL
            WHEN changeUnit = 'pp' THEN 100D * (currentValue - comparisonValue)
            WHEN changeUnit = 'pct' THEN 100D * (try_divide(currentValue, comparisonValue) - 1D)
            ELSE NULL
        END AS changeValue,
        CASE
            WHEN currentValue IS NULL OR comparisonValue IS NULL THEN 'unavailable'
            WHEN currentValue > comparisonValue THEN 'up'
            WHEN currentValue < comparisonValue THEN 'down'
            ELSE 'flat'
        END AS changeDirection
    FROM valuesCalculated
)
,
decompInputs AS (
    SELECT
        targetWeekStartDate,filterLob,filterPlatform,comparisonType,
        max(CASE WHEN metricName='nbv' THEN currentValue END) AS currentTraffic,
        max(CASE WHEN metricName='nbv' THEN comparisonValue END) AS comparisonTraffic,
        max(CASE WHEN metricName='orders' THEN currentValue END) AS currentOrders,
        max(CASE WHEN metricName='orders' THEN comparisonValue END) AS comparisonOrders
    FROM deltasCalculated
    GROUP BY targetWeekStartDate,filterLob,filterPlatform,comparisonType
),
decomposition AS (
    SELECT
        *,
        try_divide(currentOrders,currentTraffic) AS currentOrderConversion,
        try_divide(comparisonOrders,comparisonTraffic) AS comparisonOrderConversion
    FROM decompInputs
),
decompositionValues AS (
    SELECT
        *,
        currentOrders-comparisonOrders AS orderChangeValue,
        (currentTraffic-comparisonTraffic)
            * ((currentOrderConversion+comparisonOrderConversion)/2D) AS trafficEffectValue,
        (currentOrderConversion-comparisonOrderConversion)
            * ((currentTraffic+comparisonTraffic)/2D) AS conversionEffectValue
    FROM decomposition
)
SELECT
    d.targetWeekStartDate,d.targetWeekEndDate,d.fiscalQuarterLabel,d.fiscalWeekCode,d.weekLabel,d.weekEndingLabel,
    d.filterLob,d.filterPlatform,
    d.comparisonType,d.comparisonLabel,d.comparisonSortOrder,d.comparisonStartDate,d.comparisonEndDate,
    d.comparisonWeekCount,d.comparisonDataAvailable,d.comparisonWindowComplete,
    d.funnelRowType,d.funnelStepOrder,d.metricName,d.metricLabel,d.funnelDisplayLabel,d.metricDescription,
    d.metricKind,d.displayFormat,d.changeUnit,d.definitionStatus,
    d.thisWeekNumerator AS currentNumerator,d.thisWeekDenominator AS currentDenominator,
    d.comparisonNumerator,d.comparisonDenominator,d.currentValue,d.comparisonValue,
    d.absoluteDeltaValue,d.changeValue,d.changeDirection,
    x.currentTraffic,x.comparisonTraffic,x.currentOrders,x.comparisonOrders,
    x.currentOrderConversion,x.comparisonOrderConversion,
    x.orderChangeValue,x.trafficEffectValue,x.conversionEffectValue,
    x.orderChangeValue - (x.trafficEffectValue+x.conversionEffectValue) AS decompositionResidualValue,
    'symmetric_two_factor' AS decompositionMethod,
    'proposed' AS decompositionDefinitionStatus,
    d.thisWeekDataAvailable,d.goldProcessedAt
FROM deltasCalculated d
LEFT JOIN decompositionValues x
  ON x.targetWeekStartDate=d.targetWeekStartDate
 AND x.filterLob=d.filterLob
 AND x.filterPlatform=d.filterPlatform
 AND x.comparisonType=d.comparisonType;
