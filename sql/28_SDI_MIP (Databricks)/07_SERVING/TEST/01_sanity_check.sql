-- ============================================================================
-- MIP serving materialized views v3 - validation queries
-- ============================================================================

-- 1) Breakout comparison table: All must be <= 101 rows (Top100 + optional Other)
SELECT
    targetWeekStartDate, metricName, breakoutType, comparisonType,
    count(*) AS displayRows,
    sum(CASE WHEN isOtherBucket THEN 1 ELSE 0 END) AS otherRows,
    max(displayRankWithinBreakout) AS maxDisplayRank
FROM prdrzranalytics.lab42.sdi_mv_mip_serving_breakoutsComparisonTable_long
GROUP BY 1,2,3,4
HAVING count(*) > 101
    OR sum(CASE WHEN isOtherBucket THEN 1 ELSE 0 END) > 1
    OR max(displayRankWithinBreakout) > 101;

-- Expected: 0 rows.


-- 2) Waterfall: each selection is TopN + optional Other.
SELECT
    targetWeekStartDate, metricName, breakoutType, comparisonType, displaySize, displayLimit,
    count(*) AS displayRows,
    sum(CASE WHEN isOtherBucket THEN 1 ELSE 0 END) AS otherRows,
    max(displayRank) AS maxDisplayRank
FROM prdrzranalytics.lab42.sdi_mv_mip_serving_breakoutsWaterfall_long
GROUP BY 1,2,3,4,5,6
HAVING count(*) > displayLimit + 1
    OR sum(CASE WHEN isOtherBucket THEN 1 ELSE 0 END) > 1
    OR max(displayRank) > displayLimit + 1;

-- Expected: 0 rows.


-- 3) Waterfall must reconcile to topline change.
SELECT
    targetWeekStartDate, metricName, breakoutType, comparisonType, displaySize,
    max(toplineWaterfallDeltaValue) AS toplineDelta,
    max(displayedWaterfallDeltaSum) AS displayedDelta,
    max(abs(waterfallReconciliationResidual)) AS absResidual
FROM prdrzranalytics.lab42.sdi_mv_mip_serving_breakoutsWaterfall_long
GROUP BY 1,2,3,4,5
HAVING max(abs(waterfallReconciliationResidual)) > 0.000001D;

-- Expected: 0 rows or only tiny floating-point noise if tolerance is loosened.


-- 4) Crosstab matrix: row/column display ranks must obey selected size.
SELECT
    targetWeekStartDate, metricName, pairKey, comparisonType, displaySize, displayLimit,
    max(rowDisplayRank) AS maxRowRank,
    max(columnDisplayRank) AS maxColumnRank
FROM prdrzranalytics.lab42.sdi_mv_mip_serving_crosstabsMatrix_long
GROUP BY 1,2,3,4,5,6
HAVING max(rowDisplayRank) > displayLimit + 1
    OR max(columnDisplayRank) > displayLimit + 1;

-- Expected: 0 rows.


-- 5) Overview movers All cap.
SELECT
    targetWeekStartDate, metricName, comparisonType,
    max(candidateRowCount) AS candidateRows,
    sum(CASE WHEN isTop100 THEN 1 ELSE 0 END) AS top100Rows
FROM prdrzranalytics.lab42.sdi_mv_mip_serving_overviewToplineMovers_long
GROUP BY 1,2,3
HAVING sum(CASE WHEN isTop100 THEN 1 ELSE 0 END) > 100;

-- Expected: 0 rows.


-- 6) Crosstab ranked pairs All cap.
SELECT
    targetWeekStartDate, metricName, comparisonType,
    max(candidateIntersectionCount) AS candidateIntersections,
    sum(CASE WHEN isTop100 THEN 1 ELSE 0 END) AS top100Rows
FROM prdrzranalytics.lab42.sdi_mv_mip_serving_crosstabsRankedPairs_long
GROUP BY 1,2,3
HAVING sum(CASE WHEN isTop100 THEN 1 ELSE 0 END) > 100;

-- Expected: 0 rows.


-- 7) Quick row counts.
SELECT 'overviewCards' AS objectName, count(*) AS rowCount
FROM prdrzranalytics.lab42.sdi_mv_mip_serving_overviewCards_wide
UNION ALL
SELECT 'overviewTrend', count(*) FROM prdrzranalytics.lab42.sdi_mv_mip_serving_overviewTrend_long
UNION ALL
SELECT 'overviewToplineMovers', count(*) FROM prdrzranalytics.lab42.sdi_mv_mip_serving_overviewToplineMovers_long
UNION ALL
SELECT 'overviewConversionFunnel', count(*) FROM prdrzranalytics.lab42.sdi_mv_mip_serving_overviewConversionFunnel_long
UNION ALL
SELECT 'breakoutsComparisonTable', count(*) FROM prdrzranalytics.lab42.sdi_mv_mip_serving_breakoutsComparisonTable_long
UNION ALL
SELECT 'breakoutsWaterfall', count(*) FROM prdrzranalytics.lab42.sdi_mv_mip_serving_breakoutsWaterfall_long
UNION ALL
SELECT 'breakoutsAbsoluteTrend', count(*) FROM prdrzranalytics.lab42.sdi_mv_mip_serving_breakoutsAbsoluteTrend_long
UNION ALL
SELECT 'crosstabsMatrix', count(*) FROM prdrzranalytics.lab42.sdi_mv_mip_serving_crosstabsMatrix_long
UNION ALL
SELECT 'crosstabsRankedPairs', count(*) FROM prdrzranalytics.lab42.sdi_mv_mip_serving_crosstabsRankedPairs_long
UNION ALL
SELECT 'exploreBase', count(*) FROM prdrzranalytics.lab42.sdi_mv_mip_serving_exploreBase_wide
UNION ALL
SELECT 'exploreRankedPairs', count(*) FROM prdrzranalytics.lab42.sdi_mv_mip_serving_exploreRankedPairs_long;
