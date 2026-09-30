-- ============================================================================
-- FILE  : 01_sdi_vw_mip_serving_overviewCards_long.sql
-- LAYER : SERVING
-- TAB   : Overview
-- SECTION: Cards
-- PURPOSE:
--   UI/API-ready Overview card metrics with current, WoW, prior-4-week average,
--   and same-week-last-year values and changes.
--
-- CHANGE VALUE CONVENTION:
--   changeUnit = 'pct' -> change fields are percentage values (e.g. 4.3 = +4.3%).
--   changeUnit = 'pp'  -> change fields are percentage-point values (e.g. 2.1 = +2.1 pp).
--
-- NAMING:
--   Object name expresses app tab/section + physical shape only.
--   Current reporting grain is week-based, but cadence/grain is intentionally
--   not encoded in the serving-view object name.
-- ============================================================================

CREATE OR REPLACE VIEW prdrzranalytics.lab42.sdi_vw_mip_serving_overviewCards_long AS
WITH base AS (
    SELECT
        g.targetWeekStartDate,
        g.targetWeekEndDate,
        g.fiscalQuarterLabel,
        g.fiscalWeekCode,
        g.weekLabel,
        g.filterLob,
        g.filterPlatform,

        g.metricName,
        g.metricLabel,
        m.metricDescription,
        g.metricKind,
        g.displayFormat,
        g.changeUnit,
        m.hasForecast,
        m.definitionStatus,
        m.sortOrder,

        CASE
            WHEN g.metricKind = 'ratio'
                THEN try_divide(g.thisWeekNumerator, g.thisWeekDenominator)
            ELSE g.thisWeekNumerator
        END AS currentValue,

        CASE
            WHEN g.metricKind = 'ratio'
                THEN try_divide(g.priorWeekNumerator, g.priorWeekDenominator)
            ELSE g.priorWeekNumerator
        END AS priorWeekValue,

        CASE
            WHEN g.fourWeekTrendWeekCount <= 0 THEN NULL
            WHEN g.metricKind = 'ratio'
                THEN try_divide(g.fourWeekTrendNumerator, g.fourWeekTrendDenominator)
            ELSE try_divide(
                g.fourWeekTrendNumerator,
                cast(g.fourWeekTrendWeekCount AS DOUBLE)
            )
        END AS fourWeekAvgValue,

        CASE
            WHEN g.metricKind = 'ratio'
                THEN try_divide(g.sameWeekLyNumerator, g.sameWeekLyDenominator)
            ELSE g.sameWeekLyNumerator
        END AS sameWeekLastYearValue,

        g.thisWeekDataAvailable,
        g.priorWeekDataAvailable,
        g.fourWeekTrendWeekCount,
        g.sameWeekLyDataAvailable,
        g.goldProcessedAt

    FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_overviewMetricIngredientsByWeek_long g

    JOIN prdrzranalytics.lab42.sdi_vw_mip_control_metricCatalog_static m
      ON  m.metricName = g.metricName
      AND m.isActive
      AND m.showOnOverview
),
calculated AS (
    SELECT
        *,

        CASE
            WHEN NOT thisWeekDataAvailable
              OR NOT priorWeekDataAvailable
              OR currentValue IS NULL
              OR priorWeekValue IS NULL
                THEN NULL
            WHEN changeUnit = 'pp'
                THEN 100D * (currentValue - priorWeekValue)
            WHEN changeUnit = 'pct'
                THEN 100D * (try_divide(currentValue, priorWeekValue) - 1D)
            ELSE NULL
        END AS wowChange,

        CASE
            WHEN NOT thisWeekDataAvailable
              OR fourWeekTrendWeekCount <= 0
              OR currentValue IS NULL
              OR fourWeekAvgValue IS NULL
                THEN NULL
            WHEN changeUnit = 'pp'
                THEN 100D * (currentValue - fourWeekAvgValue)
            WHEN changeUnit = 'pct'
                THEN 100D * (try_divide(currentValue, fourWeekAvgValue) - 1D)
            ELSE NULL
        END AS vsFourWeekAvgChange,

        CASE
            WHEN NOT thisWeekDataAvailable
              OR NOT sameWeekLyDataAvailable
              OR currentValue IS NULL
              OR sameWeekLastYearValue IS NULL
                THEN NULL
            WHEN changeUnit = 'pp'
                THEN 100D * (currentValue - sameWeekLastYearValue)
            WHEN changeUnit = 'pct'
                THEN 100D * (try_divide(currentValue, sameWeekLastYearValue) - 1D)
            ELSE NULL
        END AS yoyChange

    FROM base
)
SELECT
    targetWeekStartDate,
    targetWeekEndDate,
    fiscalQuarterLabel,
    fiscalWeekCode,
    weekLabel,
    filterLob,
    filterPlatform,

    metricName,
    metricLabel,
    metricDescription,
    metricKind,
    displayFormat,
    changeUnit,
    hasForecast,
    definitionStatus,
    sortOrder,

    currentValue,
    priorWeekValue,
    wowChange,
    fourWeekAvgValue,
    vsFourWeekAvgChange,
    sameWeekLastYearValue,
    yoyChange,

    thisWeekDataAvailable,
    priorWeekDataAvailable,
    fourWeekTrendWeekCount,
    sameWeekLyDataAvailable,

    goldProcessedAt
FROM calculated;
