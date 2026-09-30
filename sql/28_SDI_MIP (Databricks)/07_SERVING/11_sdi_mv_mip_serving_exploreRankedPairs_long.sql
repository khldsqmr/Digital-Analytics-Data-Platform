-- ============================================================================
-- FILE  : 11_sdi_mv_mip_serving_exploreRankedPairs_long.sql
-- LAYER : SERVING / MATERIALIZED
-- TAB   : Explore
-- SECTION: Every pair ranked
-- PURPOSE:
--   Fast-path comparator-aware ranked-pair contract for the default unfiltered Explore state.
--   It reuses the Crosstabs All=Top100+Other pair calculations.
--   When users apply arbitrary Explore-only filters, the backend must recompute
--   pair rankings from exploreBase_wide so those filters are honored.
-- ============================================================================

CREATE OR REPLACE MATERIALIZED VIEW prdrzranalytics.lab42.sdi_mv_mip_serving_exploreRankedPairs_long
COMMENT 'MIP Explore ranked-pairs default fast path. Dynamic filtered Explore must query exploreBase_wide.'
CLUSTER BY (weekStartDate, metricName, comparisonType)
REFRESH POLICY AUTO
TRIGGER ON UPDATE AT MOST EVERY INTERVAL 1 MINUTE
AS
SELECT
    targetWeekStartDate AS weekStartDate,
    targetWeekEndDate AS weekEndDate,
    fiscalQuarterLabel,
    fiscalWeekCode,
    weekLabel,
    weekEndingLabel,
    filterLob,
    filterPlatform,

    pairKey,
    pairLabel,
    pairSortOrder,
    rowBreakoutType,
    rowBreakoutValue,
    rowDisplayRank,
    isRowOtherBucket,
    columnBreakoutType,
    columnBreakoutValue,
    columnDisplayRank,
    isColumnOtherBucket,
    intersectionLabel,

    metricName,
    metricLabel,
    metricDescription,
    metricKind,
    displayFormat,
    changeUnit,
    metricSortOrder,

    comparisonType,
    comparisonLabel,
    comparisonSortOrder,
    comparisonStartDate,
    comparisonEndDate,
    comparisonWeekCount,
    comparisonDataAvailable,
    comparisonWindowComplete,

    currentValue,
    comparisonValue,
    absoluteDeltaValue,
    changeValue,
    changeDirection,

    toplineCurrentValue,
    toplineComparisonValue,
    impactOnToplineValue,
    impactOnToplineUnit,

    cellImpactRankWithinPair,
    cellImpactRankAcrossPairs,
    isTop5,
    isTop10,
    isTop18,
    isTop100,

    candidateIntersectionCount,
    allSuppressedIntersectionCount,
    allSelectionLimit,
    allSelectionRule,

    peerSetValue,
    peerSetChangeValue,
    peerSetDataAvailable,

    'defaultAll' AS exploreFilterMode,
    FALSE AS supportsArbitraryExploreFilters,
    'prdrzranalytics.lab42.sdi_mv_mip_serving_exploreBase_wide' AS dynamicFilterSourceObject,

    goldProcessedAt
FROM prdrzranalytics.lab42.sdi_mv_mip_serving_crosstabsRankedPairs_long;
