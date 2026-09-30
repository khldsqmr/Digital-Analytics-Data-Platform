-- ============================================================================
-- FILE  : 09_sdi_mv_mip_serving_crosstabsRankedPairs_long.sql
-- LAYER : SERVING / MATERIALIZED
-- TAB   : Crosstabs
-- SECTION: Every pair ranked
-- PURPOSE:
--   Comparator-aware global ranking of intersections across every supported pair.
--   Source cells use displaySize='all', meaning each pair axis is already Top100 + (Other).
--   Top 5 / Top 10 / Top 18 / Top 100 are rank filters.
--
--   A single global numeric (Other) across different crosstab pairs is intentionally
--   not created because pair populations overlap; summing them would double-count.
-- ============================================================================

CREATE OR REPLACE MATERIALIZED VIEW prdrzranalytics.lab42.sdi_mv_mip_serving_crosstabsRankedPairs_long
COMMENT 'MIP Crosstabs Every pair ranked. Global comparator-aware ranking over All=Top100+Other pair cells.'
CLUSTER BY (targetWeekStartDate, metricName, comparisonType)
REFRESH POLICY AUTO
TRIGGER ON UPDATE AT MOST EVERY INTERVAL 1 MINUTE
AS
WITH ranked AS (
    SELECT
        m.*,
        row_number() OVER (
            PARTITION BY targetWeekStartDate,filterLob,filterPlatform,metricName,comparisonType
            ORDER BY
                abs(impactOnToplineValue) DESC NULLS LAST,
                abs(absoluteDeltaValue) DESC NULLS LAST,
                pairKey,rowBreakoutValue,columnBreakoutValue
        ) AS globalIntersectionImpactRank,
        count(*) OVER (
            PARTITION BY targetWeekStartDate,filterLob,filterPlatform,metricName,comparisonType
        ) AS candidateIntersectionCount
    FROM prdrzranalytics.lab42.sdi_mv_mip_serving_crosstabsMatrix_long m
    WHERE displaySize='all'
      AND comparisonDataAvailable
      AND impactOnToplineValue IS NOT NULL
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
    concat(rowBreakoutValue,' × ',columnBreakoutValue) AS intersectionLabel,

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
    globalIntersectionImpactRank AS cellImpactRankAcrossPairs,

    coalesce(globalIntersectionImpactRank<=5,FALSE) AS isTop5,
    coalesce(globalIntersectionImpactRank<=10,FALSE) AS isTop10,
    coalesce(globalIntersectionImpactRank<=18,FALSE) AS isTop18,
    coalesce(globalIntersectionImpactRank<=100,FALSE) AS isTop100,

    candidateIntersectionCount,
    greatest(candidateIntersectionCount-100,0) AS allSuppressedIntersectionCount,
    100 AS allSelectionLimit,
    'All = Top 100 globally ranked intersections. Row/column (Other) buckets are already scoped within each pair; no cross-pair numeric Other is created.' AS allSelectionRule,

    peerSetValue,
    peerSetChangeValue,
    peerSetDataAvailable,

    goldProcessedAt
FROM ranked;
