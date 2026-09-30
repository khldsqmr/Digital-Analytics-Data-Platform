-- =================================================================================================
-- MIP GOLD APP TABLES
-- EXECUTION + VALIDATION
--
-- Example:
--   p_asOfDate       = 2026-09-28
--   p_weeksToRebuild = 1
--
-- With the Sunday-Saturday reporting calendar, DATE '2026-09-28'
-- resolves to targetWeekStartDate = DATE '2026-09-27'.
--
-- IMPORTANT:
--   1. Analytical Gold procedures should complete successfully first.
--   2. Gold App procedures do NOT depend on one another.
--   3. Therefore, the Gold App procedures below can be executed independently.
--   4. For historical corrections, use the SAME p_asOfDate / p_weeksToRebuild
--      that was used to rebuild analytical Gold.
-- =================================================================================================


-- =================================================================================================
-- 0. OPTIONAL PREFLIGHT
--    Run this first when validating deployment / source readiness.
--    p_validateOnly = TRUE does not modify the Gold App tables.
-- =================================================================================================

CALL prdrzranalytics.lab42.sdi_sp_mip_gold_appOverviewCards_wide(
    p_asOfDate       => DATE '2026-09-28',
    p_weeksToRebuild => 1,
    p_validateOnly   => TRUE
);

CALL prdrzranalytics.lab42.sdi_sp_mip_gold_appOverviewTrend_long(
    p_asOfDate       => DATE '2026-09-28',
    p_weeksToRebuild => 1,
    p_validateOnly   => TRUE
);

CALL prdrzranalytics.lab42.sdi_sp_mip_gold_appOverviewToplineMovers_long(
    p_asOfDate       => DATE '2026-09-28',
    p_weeksToRebuild => 1,
    p_validateOnly   => TRUE
);

CALL prdrzranalytics.lab42.sdi_sp_mip_gold_appOverviewConversionFunnel_long(
    p_asOfDate       => DATE '2026-09-28',
    p_weeksToRebuild => 1,
    p_validateOnly   => TRUE
);

CALL prdrzranalytics.lab42.sdi_sp_mip_gold_appBreakoutsComparisonTable_long(
    p_asOfDate       => DATE '2026-09-28',
    p_weeksToRebuild => 1,
    p_validateOnly   => TRUE
);

CALL prdrzranalytics.lab42.sdi_sp_mip_gold_appBreakoutsWaterfall_long(
    p_asOfDate       => DATE '2026-09-28',
    p_weeksToRebuild => 1,
    p_validateOnly   => TRUE
);

CALL prdrzranalytics.lab42.sdi_sp_mip_gold_appBreakoutsAbsoluteTrend_long(
    p_asOfDate       => DATE '2026-09-28',
    p_weeksToRebuild => 1,
    p_validateOnly   => TRUE
);

CALL prdrzranalytics.lab42.sdi_sp_mip_gold_appCrosstabsMatrix_long(
    p_asOfDate       => DATE '2026-09-28',
    p_weeksToRebuild => 1,
    p_validateOnly   => TRUE
);

CALL prdrzranalytics.lab42.sdi_sp_mip_gold_appCrosstabsRankedPairs_long(
    p_asOfDate       => DATE '2026-09-28',
    p_weeksToRebuild => 1,
    p_validateOnly   => TRUE
);

CALL prdrzranalytics.lab42.sdi_sp_mip_gold_appExploreBase_wide(
    p_asOfDate       => DATE '2026-09-28',
    p_weeksToRebuild => 1,
    p_validateOnly   => TRUE
);

CALL prdrzranalytics.lab42.sdi_sp_mip_gold_appExploreRankedPairs_long(
    p_asOfDate       => DATE '2026-09-28',
    p_weeksToRebuild => 1,
    p_validateOnly   => TRUE
);


-- =================================================================================================
-- 1. ACTUAL GOLD APP EXECUTION
--    Each procedure independently reads analytical Gold + control views.
--
--    Normal weekly run:
--       p_weeksToRebuild = 1
--
--    Historical rebuild example:
--       p_weeksToRebuild = 4
--
--    Each procedure REPLACE WHEREs only the requested reporting-week range.
-- =================================================================================================


-- -------------------------------------------------------------------------------------------------
-- OVERVIEW
-- -------------------------------------------------------------------------------------------------

CALL prdrzranalytics.lab42.sdi_sp_mip_gold_appOverviewCards_wide(
    p_asOfDate       => DATE '2026-09-28',
    p_weeksToRebuild => 1,
    p_validateOnly   => FALSE
);

CALL prdrzranalytics.lab42.sdi_sp_mip_gold_appOverviewTrend_long(
    p_asOfDate       => DATE '2026-09-28',
    p_weeksToRebuild => 1,
    p_validateOnly   => FALSE
);

CALL prdrzranalytics.lab42.sdi_sp_mip_gold_appOverviewToplineMovers_long(
    p_asOfDate       => DATE '2026-09-28',
    p_weeksToRebuild => 1,
    p_validateOnly   => FALSE
);

CALL prdrzranalytics.lab42.sdi_sp_mip_gold_appOverviewConversionFunnel_long(
    p_asOfDate       => DATE '2026-09-28',
    p_weeksToRebuild => 1,
    p_validateOnly   => FALSE
);


-- -------------------------------------------------------------------------------------------------
-- BREAKOUTS
-- -------------------------------------------------------------------------------------------------

CALL prdrzranalytics.lab42.sdi_sp_mip_gold_appBreakoutsComparisonTable_long(
    p_asOfDate       => DATE '2026-09-28',
    p_weeksToRebuild => 1,
    p_validateOnly   => FALSE
);

CALL prdrzranalytics.lab42.sdi_sp_mip_gold_appBreakoutsWaterfall_long(
    p_asOfDate       => DATE '2026-09-28',
    p_weeksToRebuild => 1,
    p_validateOnly   => FALSE
);

CALL prdrzranalytics.lab42.sdi_sp_mip_gold_appBreakoutsAbsoluteTrend_long(
    p_asOfDate       => DATE '2026-09-28',
    p_weeksToRebuild => 1,
    p_validateOnly   => FALSE
);


-- -------------------------------------------------------------------------------------------------
-- CROSSTABS
-- -------------------------------------------------------------------------------------------------

CALL prdrzranalytics.lab42.sdi_sp_mip_gold_appCrosstabsMatrix_long(
    p_asOfDate       => DATE '2026-09-28',
    p_weeksToRebuild => 1,
    p_validateOnly   => FALSE
);

CALL prdrzranalytics.lab42.sdi_sp_mip_gold_appCrosstabsRankedPairs_long(
    p_asOfDate       => DATE '2026-09-28',
    p_weeksToRebuild => 1,
    p_validateOnly   => FALSE
);


-- -------------------------------------------------------------------------------------------------
-- EXPLORE
-- -------------------------------------------------------------------------------------------------

CALL prdrzranalytics.lab42.sdi_sp_mip_gold_appExploreBase_wide(
    p_asOfDate       => DATE '2026-09-28',
    p_weeksToRebuild => 1,
    p_validateOnly   => FALSE
);

CALL prdrzranalytics.lab42.sdi_sp_mip_gold_appExploreRankedPairs_long(
    p_asOfDate       => DATE '2026-09-28',
    p_weeksToRebuild => 1,
    p_validateOnly   => FALSE
);



-- =================================================================================================
-- VALIDATION 1
-- TABLE POPULATION + APP FRESHNESS
--
-- Purpose:
--   - confirms every Gold App table received rows for the target week
--   - compares latest analytical Gold processing timestamp vs App processing timestamp
--
-- Expected:
--   rowCount > 0 for every table
--   freshnessStatus = OK for every table
-- =================================================================================================

WITH params AS (
    SELECT DATE '2026-09-28' AS asOfDate
),

target AS (
    SELECT
        date_add(asOfDate, 1 - dayofweek(asOfDate)) AS targetWeekStartDate
    FROM params
),

health AS (

    SELECT
        'appOverviewCards_wide' AS objectName,
        count(*) AS rowCount,
        max(goldProcessedAt) AS latestGoldProcessedAt,
        max(appProcessedAt) AS latestAppProcessedAt
    FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_appOverviewCards_wide
    CROSS JOIN target
    WHERE targetWeekStartDate = target.targetWeekStartDate

    UNION ALL

    SELECT
        'appOverviewTrend_long',
        count(*),
        max(goldProcessedAt),
        max(appProcessedAt)
    FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_appOverviewTrend_long
    CROSS JOIN target
    WHERE targetWeekStartDate = target.targetWeekStartDate

    UNION ALL

    SELECT
        'appOverviewToplineMovers_long',
        count(*),
        max(goldProcessedAt),
        max(appProcessedAt)
    FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_appOverviewToplineMovers_long
    CROSS JOIN target
    WHERE targetWeekStartDate = target.targetWeekStartDate

    UNION ALL

    SELECT
        'appOverviewConversionFunnel_long',
        count(*),
        max(goldProcessedAt),
        max(appProcessedAt)
    FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_appOverviewConversionFunnel_long
    CROSS JOIN target
    WHERE targetWeekStartDate = target.targetWeekStartDate

    UNION ALL

    SELECT
        'appBreakoutsComparisonTable_long',
        count(*),
        max(goldProcessedAt),
        max(appProcessedAt)
    FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_appBreakoutsComparisonTable_long
    CROSS JOIN target
    WHERE targetWeekStartDate = target.targetWeekStartDate

    UNION ALL

    SELECT
        'appBreakoutsWaterfall_long',
        count(*),
        max(goldProcessedAt),
        max(appProcessedAt)
    FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_appBreakoutsWaterfall_long
    CROSS JOIN target
    WHERE targetWeekStartDate = target.targetWeekStartDate

    UNION ALL

    SELECT
        'appBreakoutsAbsoluteTrend_long',
        count(*),
        max(goldProcessedAt),
        max(appProcessedAt)
    FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_appBreakoutsAbsoluteTrend_long
    CROSS JOIN target
    WHERE targetWeekStartDate = target.targetWeekStartDate

    UNION ALL

    SELECT
        'appCrosstabsMatrix_long',
        count(*),
        max(goldProcessedAt),
        max(appProcessedAt)
    FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_appCrosstabsMatrix_long
    CROSS JOIN target
    WHERE targetWeekStartDate = target.targetWeekStartDate

    UNION ALL

    SELECT
        'appCrosstabsRankedPairs_long',
        count(*),
        max(goldProcessedAt),
        max(appProcessedAt)
    FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_appCrosstabsRankedPairs_long
    CROSS JOIN target
    WHERE targetWeekStartDate = target.targetWeekStartDate

    UNION ALL

    SELECT
        'appExploreBase_wide',
        count(*),
        max(goldProcessedAt),
        max(appProcessedAt)
    FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_appExploreBase_wide
    CROSS JOIN target
    WHERE weekStartDate = target.targetWeekStartDate

    UNION ALL

    SELECT
        'appExploreRankedPairs_long',
        count(*),
        max(goldProcessedAt),
        max(appProcessedAt)
    FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_appExploreRankedPairs_long
    CROSS JOIN target
    WHERE weekStartDate = target.targetWeekStartDate
)

SELECT
    objectName,
    rowCount,
    latestGoldProcessedAt,
    latestAppProcessedAt,

    CASE
        WHEN rowCount = 0
            THEN 'FAIL - NO ROWS'

        WHEN latestAppProcessedAt IS NULL
            THEN 'FAIL - NO APP TIMESTAMP'

        WHEN latestGoldProcessedAt IS NOT NULL
         AND latestAppProcessedAt < latestGoldProcessedAt
            THEN 'CHECK - APP OLDER THAN GOLD'

        ELSE 'OK'
    END AS freshnessStatus

FROM health
ORDER BY objectName;



-- =================================================================================================
-- VALIDATION 2
-- COMPARATOR COVERAGE
--
-- Comparator-aware sections should contain exactly:
--   priorWeek
--   fourWeek
--   lastYear
--
-- Overview Cards and Explore Base are intentionally excluded:
--   - Overview Cards displays comparisons simultaneously in wide format.
--   - Explore Base is a dynamic aggregation base rather than a comparator-long UI contract.
--
-- Expected:
--   comparatorCount = 3
--   comparatorStatus = OK
-- =================================================================================================

WITH params AS (
    SELECT DATE '2026-09-28' AS asOfDate
),

target AS (
    SELECT
        date_add(asOfDate, 1 - dayofweek(asOfDate)) AS targetWeekStartDate
    FROM params
),

expectedObjects AS (
    SELECT * FROM VALUES
        ('appOverviewTrend_long'),
        ('appOverviewToplineMovers_long'),
        ('appOverviewConversionFunnel_long'),
        ('appBreakoutsComparisonTable_long'),
        ('appBreakoutsWaterfall_long'),
        ('appBreakoutsAbsoluteTrend_long'),
        ('appCrosstabsMatrix_long'),
        ('appCrosstabsRankedPairs_long'),
        ('appExploreRankedPairs_long')
    AS e(objectName)
),

comparatorRows AS (

    SELECT
        'appOverviewTrend_long' AS objectName,
        comparisonType
    FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_appOverviewTrend_long
    CROSS JOIN target
    WHERE targetWeekStartDate = target.targetWeekStartDate

    UNION ALL

    SELECT
        'appOverviewToplineMovers_long',
        comparisonType
    FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_appOverviewToplineMovers_long
    CROSS JOIN target
    WHERE targetWeekStartDate = target.targetWeekStartDate

    UNION ALL

    SELECT
        'appOverviewConversionFunnel_long',
        comparisonType
    FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_appOverviewConversionFunnel_long
    CROSS JOIN target
    WHERE targetWeekStartDate = target.targetWeekStartDate

    UNION ALL

    SELECT
        'appBreakoutsComparisonTable_long',
        comparisonType
    FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_appBreakoutsComparisonTable_long
    CROSS JOIN target
    WHERE targetWeekStartDate = target.targetWeekStartDate

    UNION ALL

    SELECT
        'appBreakoutsWaterfall_long',
        comparisonType
    FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_appBreakoutsWaterfall_long
    CROSS JOIN target
    WHERE targetWeekStartDate = target.targetWeekStartDate

    UNION ALL

    SELECT
        'appBreakoutsAbsoluteTrend_long',
        comparisonType
    FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_appBreakoutsAbsoluteTrend_long
    CROSS JOIN target
    WHERE targetWeekStartDate = target.targetWeekStartDate

    UNION ALL

    SELECT
        'appCrosstabsMatrix_long',
        comparisonType
    FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_appCrosstabsMatrix_long
    CROSS JOIN target
    WHERE targetWeekStartDate = target.targetWeekStartDate

    UNION ALL

    SELECT
        'appCrosstabsRankedPairs_long',
        comparisonType
    FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_appCrosstabsRankedPairs_long
    CROSS JOIN target
    WHERE targetWeekStartDate = target.targetWeekStartDate

    UNION ALL

    SELECT
        'appExploreRankedPairs_long',
        comparisonType
    FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_appExploreRankedPairs_long
    CROSS JOIN target
    WHERE weekStartDate = target.targetWeekStartDate
),

summary AS (
    SELECT
        objectName,
        sort_array(collect_set(comparisonType)) AS comparatorsFound,
        count(DISTINCT comparisonType) AS comparatorCount,

        sum(
            CASE
                WHEN comparisonType NOT IN ('priorWeek', 'fourWeek', 'lastYear')
                THEN 1
                ELSE 0
            END
        ) AS unexpectedComparatorRows

    FROM comparatorRows
    GROUP BY objectName
)

SELECT
    e.objectName,
    coalesce(s.comparatorCount, 0) AS comparatorCount,
    s.comparatorsFound,
    coalesce(s.unexpectedComparatorRows, 0) AS unexpectedComparatorRows,

    CASE
        WHEN coalesce(s.comparatorCount, 0) = 3
         AND coalesce(s.unexpectedComparatorRows, 0) = 0
            THEN 'OK'
        ELSE 'CHECK'
    END AS comparatorStatus

FROM expectedObjects e
LEFT JOIN summary s
    ON s.objectName = e.objectName
ORDER BY e.objectName;



-- =================================================================================================
-- VALIDATION 3
-- TOP-N / ALL = TOP 100 + OTHER
--
-- Validates:
--   Breakout Comparison Table:
--       All <= 100 individual values + at most one (Other)
--
--   Waterfall / Absolute Trend:
--       Top 5  <= 5 individual values + at most one (Other)
--       Top 10 <= 10 individual values + at most one (Other)
--       All    <= 100 individual values + at most one (Other)
--
--   Crosstab Matrix:
--       Each ROW axis and COLUMN axis respects the configured displayLimit.
--
--   Global ranked sections:
--       Topline Movers / Crosstab Ranked Pairs / Explore Ranked Pairs
--       never expose a global rank > 100.
--
-- Expected:
--   ZERO ROWS.
--   Any returned row is something to investigate.
-- =================================================================================================

WITH params AS (
    SELECT DATE '2026-09-28' AS asOfDate
),

target AS (
    SELECT
        date_add(asOfDate, 1 - dayofweek(asOfDate)) AS targetWeekStartDate
    FROM params
),


-- -------------------------------------------------------------------------------------------------
-- A. Breakout Top-N + Other rules
-- -------------------------------------------------------------------------------------------------

breakoutRows AS (

    SELECT
        'appBreakoutsComparisonTable_long' AS objectName,
        targetWeekStartDate,
        metricName,
        breakoutType,
        comparisonType,
        displaySize,
        displayLimit,
        isOtherBucket
    FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_appBreakoutsComparisonTable_long
    CROSS JOIN target
    WHERE targetWeekStartDate = target.targetWeekStartDate

    UNION ALL

    SELECT
        'appBreakoutsWaterfall_long',
        targetWeekStartDate,
        metricName,
        breakoutType,
        comparisonType,
        displaySize,
        displayLimit,
        isOtherBucket
    FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_appBreakoutsWaterfall_long
    CROSS JOIN target
    WHERE targetWeekStartDate = target.targetWeekStartDate

    UNION ALL

    SELECT
        'appBreakoutsAbsoluteTrend_long',
        targetWeekStartDate,
        metricName,
        breakoutType,
        comparisonType,
        displaySize,
        displayLimit,
        isOtherBucket
    FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_appBreakoutsAbsoluteTrend_long
    CROSS JOIN target
    WHERE targetWeekStartDate = target.targetWeekStartDate
),

breakoutViolations AS (
    SELECT
        objectName,
        targetWeekStartDate,
        metricName,
        breakoutType AS scopeName,
        comparisonType,
        displaySize,
        displayLimit,

        sum(CASE WHEN NOT isOtherBucket THEN 1 ELSE 0 END)
            AS individualValueCount,

        sum(CASE WHEN isOtherBucket THEN 1 ELSE 0 END)
            AS otherBucketCount,

        'BREAKOUT_TOP_N' AS validationRule

    FROM breakoutRows

    GROUP BY
        objectName,
        targetWeekStartDate,
        metricName,
        breakoutType,
        comparisonType,
        displaySize,
        displayLimit

    HAVING
           sum(CASE WHEN NOT isOtherBucket THEN 1 ELSE 0 END) > displayLimit
        OR sum(CASE WHEN isOtherBucket THEN 1 ELSE 0 END) > 1
),


-- -------------------------------------------------------------------------------------------------
-- B. Crosstab axis Top-N + Other rules
--    Rows repeat across matrix cells, so DISTINCT bucket values are validated.
-- -------------------------------------------------------------------------------------------------

crosstabAxes AS (

    SELECT
        'appCrosstabsMatrix_long' AS objectName,
        targetWeekStartDate,
        metricName,
        pairKey,
        comparisonType,
        displaySize,
        displayLimit,
        'ROW' AS axisType,
        rowBreakoutValue AS bucketValue,
        isRowOtherBucket AS isOtherBucket

    FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_appCrosstabsMatrix_long
    CROSS JOIN target
    WHERE targetWeekStartDate = target.targetWeekStartDate

    UNION ALL

    SELECT
        'appCrosstabsMatrix_long',
        targetWeekStartDate,
        metricName,
        pairKey,
        comparisonType,
        displaySize,
        displayLimit,
        'COLUMN',
        columnBreakoutValue,
        isColumnOtherBucket

    FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_appCrosstabsMatrix_long
    CROSS JOIN target
    WHERE targetWeekStartDate = target.targetWeekStartDate
),

crosstabViolations AS (
    SELECT
        objectName,
        targetWeekStartDate,
        metricName,
        concat(pairKey, ' / ', axisType) AS scopeName,
        comparisonType,
        displaySize,
        displayLimit,

        count(
            DISTINCT CASE
                WHEN NOT isOtherBucket THEN bucketValue
            END
        ) AS individualValueCount,

        count(
            DISTINCT CASE
                WHEN isOtherBucket THEN bucketValue
            END
        ) AS otherBucketCount,

        'CROSSTAB_AXIS_TOP_N' AS validationRule

    FROM crosstabAxes

    GROUP BY
        objectName,
        targetWeekStartDate,
        metricName,
        pairKey,
        comparisonType,
        displaySize,
        displayLimit,
        axisType

    HAVING
           count(
               DISTINCT CASE
                   WHEN NOT isOtherBucket THEN bucketValue
               END
           ) > displayLimit

        OR count(
               DISTINCT CASE
                   WHEN isOtherBucket THEN bucketValue
               END
           ) > 1
),


-- -------------------------------------------------------------------------------------------------
-- C. Global "All" ranked-list hard cap
-- -------------------------------------------------------------------------------------------------

globalRankViolations AS (

    SELECT
        'appOverviewToplineMovers_long' AS objectName,
        targetWeekStartDate,
        metricName,
        'All breakouts' AS scopeName,
        comparisonType,
        'all' AS displaySize,
        100 AS displayLimit,
        max(impactRankAcrossBreakouts) AS individualValueCount,
        cast(NULL AS BIGINT) AS otherBucketCount,
        'GLOBAL_RANK_MAX_100' AS validationRule

    FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_appOverviewToplineMovers_long
    CROSS JOIN target
    WHERE targetWeekStartDate = target.targetWeekStartDate

    GROUP BY
        targetWeekStartDate,
        metricName,
        comparisonType

    HAVING max(impactRankAcrossBreakouts) > 100


    UNION ALL


    SELECT
        'appCrosstabsRankedPairs_long',
        targetWeekStartDate,
        metricName,
        'All crosstab pairs',
        comparisonType,
        'all',
        100,
        max(cellImpactRankAcrossPairs),
        cast(NULL AS BIGINT),
        'GLOBAL_RANK_MAX_100'

    FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_appCrosstabsRankedPairs_long
    CROSS JOIN target
    WHERE targetWeekStartDate = target.targetWeekStartDate

    GROUP BY
        targetWeekStartDate,
        metricName,
        comparisonType

    HAVING max(cellImpactRankAcrossPairs) > 100


    UNION ALL


    SELECT
        'appExploreRankedPairs_long',
        weekStartDate AS targetWeekStartDate,
        metricName,
        'All Explore pairs',
        comparisonType,
        'all',
        100,
        max(cellImpactRankAcrossPairs),
        cast(NULL AS BIGINT),
        'GLOBAL_RANK_MAX_100'

    FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_appExploreRankedPairs_long
    CROSS JOIN target
    WHERE weekStartDate = target.targetWeekStartDate

    GROUP BY
        weekStartDate,
        metricName,
        comparisonType

    HAVING max(cellImpactRankAcrossPairs) > 100
)


-- -------------------------------------------------------------------------------------------------
-- FINAL:
-- ZERO ROWS = PASS
-- -------------------------------------------------------------------------------------------------

SELECT *
FROM breakoutViolations

UNION ALL

SELECT *
FROM crosstabViolations

UNION ALL

SELECT *
FROM globalRankViolations

ORDER BY
    objectName,
    metricName,
    scopeName,
    comparisonType,
    displaySize;