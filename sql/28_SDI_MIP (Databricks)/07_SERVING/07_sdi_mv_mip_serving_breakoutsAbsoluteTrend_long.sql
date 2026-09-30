-- ============================================================================
-- FILE  : 07_sdi_mv_mip_serving_breakoutsAbsoluteTrend_long.sql
-- LAYER : SERVING / MATERIALIZED
-- TAB   : Breakouts
-- SECTION: Absolute trend
-- PURPOSE:
--   Comparator-aware time-series contract by breakout bucket.
--   displaySize is top5 | top10 | all, where All = Top100 + (Other).
--   The API chooses the selected target week/comparator and then requests the
--   corresponding breakout buckets across the desired historical window.
-- ============================================================================

CREATE OR REPLACE MATERIALIZED VIEW prdrzranalytics.lab42.sdi_mv_mip_serving_breakoutsAbsoluteTrend_long
COMMENT 'MIP Breakouts absolute trend. Actual and benchmark series for Top5/Top10/All; All = Top100 + Other.'
CLUSTER BY (metricName, breakoutType, targetWeekStartDate, comparisonType, displaySize)
REFRESH POLICY AUTO
TRIGGER ON UPDATE AT MOST EVERY INTERVAL 1 MINUTE
AS
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
    metricDescription,
    metricKind,
    displayFormat,
    changeUnit,
    metricSortOrder,
    metricDefinitionStatus,

    breakoutType,
    breakoutLabel,
    breakoutValue,
    breakoutSortOrder,
    breakoutDefinitionStatus,

    comparisonType,
    comparisonLabel,
    comparisonSortOrder,
    comparisonStartDate,
    comparisonEndDate,
    comparisonWeekCount,
    comparisonWindowComplete,

    displaySize,
    displaySizeLabel,
    displayLimit,
    displaySizeSortOrder,
    displayRank,
    isOtherBucket,
    rawMemberCount,

    sliceEndValue AS actualValue,
    sliceStartValue AS benchmarkValue,
    absoluteDeltaValue,
    changeValue,
    changeDirection,
    impactOnToplineValue,
    impactOnToplineUnit,

    goldProcessedAt
FROM prdrzranalytics.lab42.sdi_mv_mip_serving_breakoutsWaterfall_long;
