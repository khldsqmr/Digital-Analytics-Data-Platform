-- ============================================================================
-- FILE  : 06_sdi_mv_mip_serving_breakoutsWaterfall_long.sql
-- LAYER : SERVING / MATERIALIZED
-- TAB   : Breakouts
-- SECTION: Waterfall
-- PURPOSE:
--   Comparator-aware waterfall contract with an explicit displaySize dimension:
--     top5  = Top 5 + (Other)
--     top10 = Top 10 + (Other)
--     all   = Top 100 + (Other)
--
--   Ranking is comparator-aware. The canonical comparison-table source already
--   applies Top100+Other, and this MV safely re-buckets it for Top5/Top10.
--   Ratios are recomputed from summed numerators/denominators; percentages are never averaged.
-- ============================================================================

CREATE OR REPLACE MATERIALIZED VIEW prdrzranalytics.lab42.sdi_mv_mip_serving_breakoutsWaterfall_long
COMMENT 'MIP Breakouts waterfall. Comparator-aware Top5/Top10/All presentation buckets; All = Top100 + Other.'
CLUSTER BY (targetWeekStartDate, metricName, breakoutType, comparisonType, displaySize)
REFRESH POLICY AUTO
TRIGGER ON UPDATE AT MOST EVERY INTERVAL 1 MINUTE
AS
WITH sizeConfig AS (
    SELECT * FROM VALUES
        ('top5',  'Top 5',  5,   10),
        ('top10', 'Top 10', 10,  20),
        ('all',   'All',    100, 30)
    AS s(displaySize, displaySizeLabel, displayLimit, displaySizeSortOrder)
),
expanded AS (
    SELECT
        b.*,
        s.displaySize,
        s.displaySizeLabel,
        s.displayLimit,
        s.displaySizeSortOrder,

        CASE
            WHEN NOT b.isOtherBucket AND b.displayRankWithinBreakout <= s.displayLimit
                THEN concat('VALUE::', coalesce(b.breakoutValue, '(null)'))
            ELSE 'OTHER::REMAINDER'
        END AS sizeBucketKey,

        CASE
            WHEN NOT b.isOtherBucket AND b.displayRankWithinBreakout <= s.displayLimit
                THEN b.breakoutValue
            ELSE '(Other)'
        END AS displayBreakoutValue,

        CASE
            WHEN NOT b.isOtherBucket AND b.displayRankWithinBreakout <= s.displayLimit
                THEN FALSE
            ELSE TRUE
        END AS sizeOtherMember
    FROM prdrzranalytics.lab42.sdi_mv_mip_serving_breakoutsComparisonTable_long b
    CROSS JOIN sizeConfig s
    WHERE b.comparisonDataAvailable
),
bucketAgg AS (
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
        sizeBucketKey,
        displayBreakoutValue AS breakoutValue,
        max(CASE WHEN sizeOtherMember THEN 1 ELSE 0 END) = 1 AS isOtherBucket,

        CASE
            WHEN max(CASE WHEN sizeOtherMember THEN 1 ELSE 0 END) = 1 THEN displayLimit + 1
            ELSE min(displayRankWithinBreakout)
        END AS displayRank,

        sum(rawMemberCount) AS rawMemberCount,
        min(rawMinImpactRankWithinBreakout) AS rawMinImpactRankWithinBreakout,
        max(rawMaxImpactRankWithinBreakout) AS rawMaxImpactRankWithinBreakout,

        sum(currentNumerator) AS currentNumerator,
        sum(currentDenominator) AS currentDenominator,
        sum(comparisonNumerator) AS comparisonNumerator,
        sum(comparisonDenominator) AS comparisonDenominator,

        max(toplineCurrentNumerator) AS toplineCurrentNumerator,
        max(toplineCurrentDenominator) AS toplineCurrentDenominator,
        max(toplineComparisonNumerator) AS toplineComparisonNumerator,
        max(toplineComparisonDenominator) AS toplineComparisonDenominator,
        max(toplineCurrentValue) AS toplineCurrentValue,
        max(toplineComparisonValue) AS toplineComparisonValue,

        max(goldProcessedAt) AS goldProcessedAt
    FROM expanded
    GROUP BY
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
        sizeBucketKey,
        displayBreakoutValue
),
bucketValues AS (
    SELECT
        a.*,
        CASE
            WHEN metricKind = 'ratio' THEN try_divide(currentNumerator, currentDenominator)
            ELSE currentNumerator
        END AS currentValue,
        CASE
            WHEN metricKind = 'ratio' THEN try_divide(comparisonNumerator, comparisonDenominator)
            ELSE comparisonNumerator
        END AS comparisonValue
    FROM bucketAgg a
),
calculated AS (
    SELECT
        v.*,
        currentValue - comparisonValue AS absoluteDeltaValue,
        CASE
            WHEN changeUnit = 'pp' THEN 100D * (currentValue - comparisonValue)
            WHEN changeUnit = 'pct' THEN 100D * (try_divide(currentValue, comparisonValue) - 1D)
            ELSE NULL
        END AS changeValue,
        CASE
            WHEN currentValue IS NULL OR comparisonValue IS NULL THEN 'unavailable'
            WHEN currentValue > comparisonValue THEN 'up'
            WHEN currentValue < comparisonValue THEN 'down'
            ELSE 'flat'
        END AS changeDirection,
        CASE
            WHEN metricKind = 'count' THEN
                100D * try_divide(currentValue - comparisonValue, toplineComparisonValue)
            WHEN metricKind = 'ratio' THEN
                100D * (
                    try_divide(currentNumerator, toplineCurrentDenominator)
                    - try_divide(comparisonNumerator, toplineComparisonDenominator)
                )
            ELSE NULL
        END AS impactOnToplineValue,
        CASE WHEN metricKind = 'count' THEN 'pct'
             WHEN metricKind = 'ratio' THEN 'pp'
             ELSE NULL END AS impactOnToplineUnit,
        CASE WHEN metricKind = 'count' THEN currentValue - comparisonValue
             WHEN metricKind = 'ratio' THEN
                100D * (
                    try_divide(currentNumerator, toplineCurrentDenominator)
                    - try_divide(comparisonNumerator, toplineComparisonDenominator)
                )
             ELSE NULL END AS waterfallDeltaValue,
        CASE WHEN metricKind = 'count' THEN 'number'
             WHEN metricKind = 'ratio' THEN 'pp'
             ELSE NULL END AS waterfallDeltaUnit,
        CASE WHEN metricKind = 'count' THEN toplineCurrentValue - toplineComparisonValue
             WHEN metricKind = 'ratio' THEN 100D * (toplineCurrentValue - toplineComparisonValue)
             ELSE NULL END AS toplineWaterfallDeltaValue
    FROM bucketValues v
),
recon AS (
    SELECT
        c.*,
        sum(waterfallDeltaValue) OVER (
            PARTITION BY targetWeekStartDate, filterLob, filterPlatform, metricName,
                         breakoutType, comparisonType, displaySize
        ) AS displayedWaterfallDeltaSum
    FROM calculated c
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
    rawMinImpactRankWithinBreakout,
    rawMaxImpactRankWithinBreakout,

    comparisonValue AS sliceStartValue,
    currentValue AS sliceEndValue,
    currentNumerator,
    currentDenominator,
    comparisonNumerator,
    comparisonDenominator,
    absoluteDeltaValue,
    changeValue,
    changeDirection,

    toplineComparisonValue AS waterfallStartValue,
    toplineCurrentValue AS waterfallEndValue,
    toplineWaterfallDeltaValue,
    waterfallDeltaValue,
    waterfallDeltaUnit,
    impactOnToplineValue,
    impactOnToplineUnit,

    displayedWaterfallDeltaSum,
    toplineWaterfallDeltaValue - displayedWaterfallDeltaSum AS waterfallReconciliationResidual,

    goldProcessedAt
FROM recon;
