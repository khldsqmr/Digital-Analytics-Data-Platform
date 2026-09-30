-- ============================================================================
-- FILE  : 02_sdi_mv_mip_serving_overviewTrend_long.sql
-- LAYER : SERVING / MATERIALIZED
-- TAB   : Overview
-- SECTION: Trend
-- PURPOSE:
--   Comparator-aware trend contract. One row per reporting week × metric × comparator.
--   comparisonType is priorWeek | fourWeek | lastYear. Forecast remains available as an
--   overlay series independent of the selected comparator.
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

CREATE OR REPLACE MATERIALIZED VIEW prdrzranalytics.lab42.sdi_mv_mip_serving_overviewTrend_long
COMMENT 'MIP Overview trend. Comparator-aware long contract with forecast overlay fields.'
CLUSTER BY (metricName, targetWeekStartDate, comparisonType)
REFRESH POLICY AUTO
TRIGGER ON UPDATE AT MOST EVERY INTERVAL 1 MINUTE
AS
WITH forecastLatest AS (
    SELECT *
    FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_overviewMetricForecastByWeek_long
    QUALIFY row_number() OVER (
        PARTITION BY weekStartDate, filterLob, filterPlatform, metricName
        ORDER BY forecastCreatedAt DESC NULLS LAST, forecastRunId DESC NULLS LAST
    ) = 1
),
base AS (
    SELECT
        g.targetWeekStartDate,
        g.targetWeekEndDate,
        g.fiscalQuarterLabel,
        g.fiscalWeekCode,
        g.weekLabel,
        c.weekEndingLabel,
        c.priorWeekStartDate,
        c.fourWeekAvgStartDate,
        c.fourWeekAvgEndDate,
        c.sameWeekLastYearStartDate,
        g.filterLob,
        g.filterPlatform,
        g.metricName,
        g.metricLabel,
        CASE WHEN g.metricName='nbv' THEN 'Total UPV' ELSE g.metricLabel END AS uiMetricLabel,
        m.metricDescription,
        g.metricKind,
        g.displayFormat,
        g.changeUnit,
        m.definitionStatus,
        m.sortOrder AS metricSortOrder,
        g.thisWeekNumerator,
        g.thisWeekDenominator,
        g.priorWeekNumerator,
        g.priorWeekDenominator,
        g.fourWeekTrendNumerator,
        g.fourWeekTrendDenominator,
        g.sameWeekLyNumerator,
        g.sameWeekLyDenominator,
        g.thisWeekDataAvailable,
        g.priorWeekDataAvailable,
        g.fourWeekTrendWeekCount,
        g.sameWeekLyDataAvailable,
        g.goldProcessedAt,
        f.forecastValue,
        f.forecastLow,
        f.forecastHigh,
        f.modelName AS forecastModelName,
        f.modelVersion AS forecastModelVersion,
        f.forecastRunId,
        f.forecastCreatedAt
    FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_overviewMetricIngredientsByWeek_long g
    JOIN prdrzranalytics.lab42.sdi_vw_mip_control_metricCatalog_static m
      ON m.metricName=g.metricName AND m.isActive AND m.showOnOverview
    LEFT JOIN prdrzranalytics.lab42.sdi_vw_mip_control_fiscalCalendar_static c
      ON c.weekStartDate=g.targetWeekStartDate
    LEFT JOIN forecastLatest f
      ON f.weekStartDate=g.targetWeekStartDate
     AND f.filterLob=g.filterLob
     AND f.filterPlatform=g.filterPlatform
     AND f.metricName=g.metricName
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

SELECT
    targetWeekStartDate,
    targetWeekEndDate,
    fiscalQuarterLabel,
    fiscalWeekCode,
    weekLabel,
    weekEndingLabel,
    filterLob,
    filterPlatform,
    metricName,
    metricLabel,
    uiMetricLabel,
    metricDescription,
    metricKind,
    displayFormat,
    changeUnit,
    definitionStatus,
    metricSortOrder,
    comparisonType,
    comparisonLabel,
    comparisonSortOrder,
    comparisonStartDate,
    comparisonEndDate,
    comparisonWeekCount,
    comparisonDataAvailable,
    comparisonWindowComplete,
    thisWeekNumerator AS currentNumerator,
    thisWeekDenominator AS currentDenominator,
    comparisonNumerator,
    comparisonDenominator,
    currentValue AS actualValue,
    comparisonValue,
    absoluteDeltaValue,
    changeValue,
    changeDirection,
    forecastValue,
    forecastLow,
    forecastHigh,
    forecastModelName,
    forecastModelVersion,
    forecastRunId,
    forecastCreatedAt,
    thisWeekDataAvailable,
    goldProcessedAt
FROM deltasCalculated;
