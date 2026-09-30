-- ============================================================================
-- FILE  : 05_sdi_vw_mip_serving_breakoutsTable_long.sql
-- LAYER : SERVING
-- TAB   : Breakouts
-- SECTION: Table
-- PURPOSE:
--   UI/API-ready breakout table.
--   Gold non-Top-N values are bucketed into '(Other)' here, then metric values
--   are calculated from the aggregated comparison ingredients.
--
-- NAMING:
--   Object name expresses app tab/section + physical shape only.
--   Current reporting grain is week-based, but cadence/grain is intentionally
--   not encoded in the serving-view object name.
-- ============================================================================

CREATE OR REPLACE VIEW prdrzranalytics.lab42.sdi_vw_mip_serving_breakoutsTable_long AS
WITH bucketedIngredients AS (
    SELECT
        g.targetWeekStartDate,
        max(g.targetWeekEndDate) AS targetWeekEndDate,
        max(g.fiscalQuarterLabel) AS fiscalQuarterLabel,
        max(g.fiscalWeekCode) AS fiscalWeekCode,
        max(g.weekLabel) AS weekLabel,
        g.filterLob,
        g.filterPlatform,

        g.breakoutType,
        max(g.breakoutLabel) AS breakoutLabel,

        CASE
            WHEN g.isTopN THEN g.breakoutValue
            ELSE '(Other)'
        END AS breakoutValue,

        CASE
            WHEN max(CASE WHEN g.isTopN THEN 0 ELSE 1 END) = 1 THEN NULL
            ELSE min(g.valueRankByNbv)
        END AS valueRankByNbv,

        CASE
            WHEN max(CASE WHEN g.isTopN THEN 0 ELSE 1 END) = 1 THEN FALSE
            ELSE TRUE
        END AS isTopN,

        g.metricName,
        max(g.metricLabel) AS metricLabel,
        max(g.metricKind) AS metricKind,
        max(g.displayFormat) AS displayFormat,
        max(g.changeUnit) AS changeUnit,

        sum(g.thisWeekNumerator) AS thisWeekNumerator,
        sum(g.thisWeekDenominator) AS thisWeekDenominator,
        sum(g.priorWeekNumerator) AS priorWeekNumerator,
        sum(g.priorWeekDenominator) AS priorWeekDenominator,
        sum(g.fourWeekTrendNumerator) AS fourWeekTrendNumerator,
        sum(g.fourWeekTrendDenominator) AS fourWeekTrendDenominator,
        sum(g.sameWeekLyNumerator) AS sameWeekLyNumerator,
        sum(g.sameWeekLyDenominator) AS sameWeekLyDenominator,

        max(CASE WHEN g.thisWeekDataAvailable THEN 1 ELSE 0 END) = 1
            AS thisWeekDataAvailable,
        max(CASE WHEN g.priorWeekDataAvailable THEN 1 ELSE 0 END) = 1
            AS priorWeekDataAvailable,
        max(g.fourWeekTrendWeekCount) AS fourWeekTrendWeekCount,
        max(CASE WHEN g.sameWeekLyDataAvailable THEN 1 ELSE 0 END) = 1
            AS sameWeekLyDataAvailable,

        max(g.goldProcessedAt) AS goldProcessedAt

    FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_breakoutMetricIngredientsByWeek_long g

    GROUP BY
        g.targetWeekStartDate,
        g.filterLob,
        g.filterPlatform,
        g.breakoutType,
        CASE WHEN g.isTopN THEN g.breakoutValue ELSE '(Other)' END,
        g.metricName
),
base AS (
    SELECT
        b.*,
        bc.sortOrder AS breakoutSortOrder,
        bc.definitionStatus AS breakoutDefinitionStatus,
        mc.metricDescription,
        mc.definitionStatus AS metricDefinitionStatus,
        mc.sortOrder AS metricSortOrder,

        CASE
            WHEN b.metricKind = 'ratio'
                THEN try_divide(b.thisWeekNumerator, b.thisWeekDenominator)
            ELSE b.thisWeekNumerator
        END AS currentValue,

        CASE
            WHEN b.metricKind = 'ratio'
                THEN try_divide(b.priorWeekNumerator, b.priorWeekDenominator)
            ELSE b.priorWeekNumerator
        END AS priorWeekValue,

        CASE
            WHEN b.fourWeekTrendWeekCount <= 0 THEN NULL
            WHEN b.metricKind = 'ratio'
                THEN try_divide(b.fourWeekTrendNumerator, b.fourWeekTrendDenominator)
            ELSE try_divide(
                b.fourWeekTrendNumerator,
                cast(b.fourWeekTrendWeekCount AS DOUBLE)
            )
        END AS fourWeekAvgValue,

        CASE
            WHEN b.metricKind = 'ratio'
                THEN try_divide(b.sameWeekLyNumerator, b.sameWeekLyDenominator)
            ELSE b.sameWeekLyNumerator
        END AS sameWeekLastYearValue

    FROM bucketedIngredients b

    JOIN prdrzranalytics.lab42.sdi_vw_mip_control_breakoutCatalog_static bc
      ON  bc.breakoutType = b.breakoutType
      AND bc.isActive
      AND bc.isPrebuiltBreakout

    JOIN prdrzranalytics.lab42.sdi_vw_mip_control_metricCatalog_static mc
      ON  mc.metricName = b.metricName
      AND mc.isActive
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

    breakoutType,
    breakoutLabel,
    breakoutValue,
    valueRankByNbv,
    isTopN,
    breakoutSortOrder,
    breakoutDefinitionStatus,

    metricName,
    metricLabel,
    metricDescription,
    metricKind,
    displayFormat,
    changeUnit,
    metricSortOrder,
    metricDefinitionStatus,

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
