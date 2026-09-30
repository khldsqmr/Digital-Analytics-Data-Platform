-- ============================================================================
-- FILE  : 01_sdi_mv_mip_serving_overviewCards_wide.sql
-- LAYER : SERVING / MATERIALIZED
-- TAB   : Overview
-- SECTION: Cards
-- PURPOSE:
--   One row per reporting week × report filter context × overview metric.
--   All card comparisons are intentionally WIDE because the card displays Prior week,
--   4-wk trend, Same wk LY and Forecast simultaneously.
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

CREATE OR REPLACE MATERIALIZED VIEW prdrzranalytics.lab42.sdi_mv_mip_serving_overviewCards_wide
COMMENT 'MIP Overview cards. Wide comparison contract for simultaneous card comparisons.'
CLUSTER BY (targetWeekStartDate, metricName)
REFRESH POLICY AUTO
TRIGGER ON UPDATE AT MOST EVERY INTERVAL 1 MINUTE
AS
WITH forecastLatest AS (
    SELECT
        weekStartDate,
        weekEndDate,
        filterLob,
        filterPlatform,
        metricName,
        forecastValue,
        forecastLow,
        forecastHigh,
        modelName,
        modelVersion,
        forecastRunId,
        forecastCreatedAt
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
        CASE WHEN g.metricName = 'nbv' THEN 'Total UPV' ELSE g.metricLabel END AS uiMetricLabel,
        m.metricDescription,
        g.metricKind,
        g.displayFormat,
        g.changeUnit,
        m.hasForecast,
        m.definitionStatus,
        m.sortOrder,
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
        CASE WHEN g.metricKind = 'ratio' THEN try_divide(g.thisWeekNumerator, g.thisWeekDenominator)
             ELSE g.thisWeekNumerator END AS currentValue,
        CASE WHEN g.metricKind = 'ratio' THEN try_divide(g.priorWeekNumerator, g.priorWeekDenominator)
             ELSE g.priorWeekNumerator END AS priorWeekValue,
        CASE WHEN g.fourWeekTrendWeekCount <= 0 THEN NULL
             WHEN g.metricKind = 'ratio' THEN try_divide(g.fourWeekTrendNumerator, g.fourWeekTrendDenominator)
             ELSE try_divide(g.fourWeekTrendNumerator, cast(g.fourWeekTrendWeekCount AS DOUBLE)) END AS fourWeekValue,
        CASE WHEN g.metricKind = 'ratio' THEN try_divide(g.sameWeekLyNumerator, g.sameWeekLyDenominator)
             ELSE g.sameWeekLyNumerator END AS lastYearValue,
        f.forecastValue,
        f.forecastLow,
        f.forecastHigh,
        f.modelName AS forecastModelName,
        f.modelVersion AS forecastModelVersion,
        f.forecastRunId,
        f.forecastCreatedAt,
        g.goldProcessedAt
    FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_overviewMetricIngredientsByWeek_long g
    JOIN prdrzranalytics.lab42.sdi_vw_mip_control_metricCatalog_static m
      ON m.metricName = g.metricName AND m.isActive AND m.showOnOverview
    LEFT JOIN prdrzranalytics.lab42.sdi_vw_mip_control_fiscalCalendar_static c
      ON c.weekStartDate = g.targetWeekStartDate
    LEFT JOIN forecastLatest f
      ON f.weekStartDate = g.targetWeekStartDate
     AND f.filterLob = g.filterLob
     AND f.filterPlatform = g.filterPlatform
     AND f.metricName = g.metricName
)
SELECT
    *,
    currentValue - priorWeekValue AS priorWeekAbsoluteDeltaValue,
    CASE WHEN changeUnit='pp' THEN 100D*(currentValue-priorWeekValue)
         WHEN changeUnit='pct' THEN 100D*(try_divide(currentValue,priorWeekValue)-1D) END AS priorWeekChangeValue,
    currentValue - fourWeekValue AS fourWeekAbsoluteDeltaValue,
    CASE WHEN changeUnit='pp' THEN 100D*(currentValue-fourWeekValue)
         WHEN changeUnit='pct' THEN 100D*(try_divide(currentValue,fourWeekValue)-1D) END AS fourWeekChangeValue,
    currentValue - lastYearValue AS lastYearAbsoluteDeltaValue,
    CASE WHEN changeUnit='pp' THEN 100D*(currentValue-lastYearValue)
         WHEN changeUnit='pct' THEN 100D*(try_divide(currentValue,lastYearValue)-1D) END AS lastYearChangeValue,
    currentValue - forecastValue AS forecastAbsoluteDeltaValue,
    CASE WHEN forecastValue IS NULL THEN NULL
         WHEN changeUnit='pp' THEN 100D*(currentValue-forecastValue)
         WHEN changeUnit='pct' THEN 100D*(try_divide(currentValue,forecastValue)-1D) END AS forecastChangeValue,
    forecastValue IS NOT NULL AS forecastDataAvailable,
    fourWeekTrendWeekCount = 4 AS fourWeekWindowComplete
FROM base;
