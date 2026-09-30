-- ============================================================================
-- FILE  : 05_sdi_mv_mip_serving_breakoutsComparisonTable_long.sql
-- LAYER : SERVING / MATERIALIZED
-- TAB   : Breakouts
-- SECTION: Comparison table
-- PURPOSE:
--   Canonical comparator-aware breakout serving contract for the table's All state.
--   All is intentionally capped at Top 100 comparator-ranked values + one scoped (Other)
--   bucket per breakout. This prevents high-cardinality dimensions from flooding the UI.
--   The synthetic (Other) row is recomputed from summed numerators/denominators; ratios
--   are never averaged.
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

CREATE OR REPLACE MATERIALIZED VIEW prdrzranalytics.lab42.sdi_mv_mip_serving_breakoutsComparisonTable_long
COMMENT 'MIP Breakouts comparison table. All state = Top 100 comparator-ranked values + scoped (Other), with ratio-safe recomputation.'
CLUSTER BY (targetWeekStartDate, metricName, breakoutType, comparisonType)
REFRESH POLICY AUTO
TRIGGER ON UPDATE AT MOST EVERY INTERVAL 1 MINUTE
AS

WITH base AS (
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
        g.breakoutType,
        g.breakoutLabel,
        g.breakoutValue,
        g.valueRankByNbv AS goldValueRankByNbv,
        g.isTopN AS goldIsConfiguredTopN,
        bc.topN AS configuredTopN,
        bc.pairTopN AS configuredPairTopN,
        bc.definitionStatus AS breakoutDefinitionStatus,
        bc.sortOrder AS breakoutSortOrder,
        g.metricName,
        g.metricLabel,
        mc.metricDescription,
        g.metricKind,
        g.displayFormat,
        g.changeUnit,
        mc.definitionStatus AS metricDefinitionStatus,
        mc.sortOrder AS metricSortOrder,
        g.thisWeekNumerator,
        g.thisWeekDenominator,
        g.priorWeekNumerator,
        g.priorWeekDenominator,
        g.fourWeekTrendNumerator,
        g.fourWeekTrendDenominator,
        g.sameWeekLyNumerator,
        g.sameWeekLyDenominator,
        g.peerSetNumerator,
        g.peerSetDenominator,
        g.thisWeekDataAvailable,
        g.priorWeekDataAvailable,
        g.fourWeekTrendWeekCount,
        g.sameWeekLyDataAvailable,
        g.goldProcessedAt
    FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_breakoutMetricIngredientsByWeek_long g
    JOIN prdrzranalytics.lab42.sdi_vw_mip_control_breakoutCatalog_static bc
      ON bc.breakoutType=g.breakoutType AND bc.isActive AND bc.isPrebuiltBreakout
    JOIN prdrzranalytics.lab42.sdi_vw_mip_control_metricCatalog_static mc
      ON mc.metricName=g.metricName AND mc.isActive
    LEFT JOIN prdrzranalytics.lab42.sdi_vw_mip_control_fiscalCalendar_static c
      ON c.weekStartDate=g.targetWeekStartDate
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
,
peerCalculated AS (
    SELECT
        d.*,
        CASE WHEN metricKind='ratio' THEN try_divide(peerSetNumerator,peerSetDenominator)
             ELSE peerSetNumerator END AS peerSetValue,
        CASE WHEN metricKind='ratio' THEN peerSetNumerator IS NOT NULL AND nullif(peerSetDenominator,0D) IS NOT NULL
             ELSE peerSetNumerator IS NOT NULL END AS peerSetDataAvailable
    FROM deltasCalculated d
),
toplineBase AS (
    SELECT
        g.targetWeekStartDate,
        g.filterLob,
        g.filterPlatform,
        g.metricName,
        g.metricKind,
        g.thisWeekNumerator,
        g.thisWeekDenominator,
        g.priorWeekNumerator,
        g.priorWeekDenominator,
        g.fourWeekTrendNumerator,
        g.fourWeekTrendDenominator,
        g.sameWeekLyNumerator,
        g.sameWeekLyDenominator,
        g.fourWeekTrendWeekCount
    FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_overviewMetricIngredientsByWeek_long g
),
toplineLong AS (
    SELECT *, 'priorWeek' AS comparisonType,
           priorWeekNumerator AS comparisonNumerator,
           priorWeekDenominator AS comparisonDenominator
    FROM toplineBase
    UNION ALL
    SELECT *, 'fourWeek' AS comparisonType,
           CASE WHEN metricKind='count' AND fourWeekTrendWeekCount>0
                THEN try_divide(fourWeekTrendNumerator,cast(fourWeekTrendWeekCount AS DOUBLE))
                ELSE fourWeekTrendNumerator END AS comparisonNumerator,
           CASE WHEN metricKind='count' THEN NULL ELSE fourWeekTrendDenominator END AS comparisonDenominator
    FROM toplineBase
    UNION ALL
    SELECT *, 'lastYear' AS comparisonType,
           sameWeekLyNumerator AS comparisonNumerator,
           sameWeekLyDenominator AS comparisonDenominator
    FROM toplineBase
),
toplineValues AS (
    SELECT
        *,
        CASE WHEN metricKind='ratio' THEN try_divide(thisWeekNumerator,thisWeekDenominator)
             ELSE thisWeekNumerator END AS toplineCurrentValue,
        CASE WHEN metricKind='ratio' THEN try_divide(comparisonNumerator,comparisonDenominator)
             ELSE comparisonNumerator END AS toplineComparisonValue
    FROM toplineLong
),
withTopline AS (
    SELECT
        p.*,
        t.toplineCurrentValue,
        t.toplineComparisonValue,
        t.thisWeekNumerator AS toplineCurrentNumerator,
        t.thisWeekDenominator AS toplineCurrentDenominator,
        t.comparisonNumerator AS toplineComparisonNumerator,
        t.comparisonDenominator AS toplineComparisonDenominator,
        CASE
            WHEN p.metricKind='count' THEN
                100D * try_divide(p.absoluteDeltaValue,t.toplineComparisonValue)
            WHEN p.metricKind='ratio' THEN
                100D * (
                    try_divide(p.thisWeekNumerator,t.thisWeekDenominator)
                    - try_divide(p.comparisonNumerator,t.comparisonDenominator)
                )
            ELSE NULL
        END AS impactOnToplineValue,
        CASE WHEN p.metricKind='count' THEN 'pct'
             WHEN p.metricKind='ratio' THEN 'pp'
             ELSE NULL END AS impactOnToplineUnit,
        p.currentValue - p.peerSetValue AS peerSetAbsoluteDeltaValue,
        CASE
            WHEN NOT p.peerSetDataAvailable OR p.currentValue IS NULL THEN NULL
            WHEN p.changeUnit='pp' THEN 100D*(p.currentValue-p.peerSetValue)
            WHEN p.changeUnit='pct' THEN 100D*(try_divide(p.currentValue,p.peerSetValue)-1D)
            ELSE NULL
        END AS peerSetChangeValue
    FROM peerCalculated p
    LEFT JOIN toplineValues t
      ON t.targetWeekStartDate=p.targetWeekStartDate
     AND t.filterLob=p.filterLob
     AND t.filterPlatform=p.filterPlatform
     AND t.metricName=p.metricName
     AND t.comparisonType=p.comparisonType
),
ranked AS (
    SELECT
        *,
        CASE WHEN comparisonDataAvailable AND impactOnToplineValue IS NOT NULL THEN
            row_number() OVER (
                PARTITION BY targetWeekStartDate,filterLob,filterPlatform,metricName,breakoutType,comparisonType
                ORDER BY abs(impactOnToplineValue) DESC NULLS LAST,
                         abs(absoluteDeltaValue) DESC NULLS LAST,
                         breakoutValue
            )
        END AS impactRankWithinBreakout,
        CASE WHEN comparisonDataAvailable AND impactOnToplineValue IS NOT NULL THEN
            row_number() OVER (
                PARTITION BY targetWeekStartDate,filterLob,filterPlatform,metricName,comparisonType
                ORDER BY abs(impactOnToplineValue) DESC NULLS LAST,
                         abs(absoluteDeltaValue) DESC NULLS LAST,
                         breakoutType,breakoutValue
            )
        END AS impactRankAcrossBreakouts
    FROM withTopline
)
,
allBucketMembers AS (
    SELECT
        r.*,
        CASE
            WHEN impactRankWithinBreakout <= 100
                THEN concat('VALUE::', coalesce(breakoutValue, '(null)'))
            ELSE 'OTHER::REMAINDER'
        END AS displayBucketKey,
        CASE
            WHEN impactRankWithinBreakout <= 100 THEN breakoutValue
            ELSE '(Other)'
        END AS displayBreakoutValue,
        CASE
            WHEN impactRankWithinBreakout <= 100 THEN FALSE
            ELSE TRUE
        END AS isSyntheticOtherMember
    FROM ranked r
),
allBucketAgg AS (
    SELECT
        targetWeekStartDate,
        targetWeekEndDate,
        fiscalQuarterLabel,
        fiscalWeekCode,
        weekLabel,
        weekEndingLabel,
        filterLob,
        filterPlatform,

        breakoutType,
        breakoutLabel,
        breakoutSortOrder,
        breakoutDefinitionStatus,
        configuredTopN,
        configuredPairTopN,

        metricName,
        metricLabel,
        metricDescription,
        metricKind,
        displayFormat,
        changeUnit,
        metricSortOrder,
        metricDefinitionStatus,

        comparisonType,
        comparisonLabel,
        comparisonSortOrder,
        comparisonStartDate,
        comparisonEndDate,
        comparisonWeekCount,
        comparisonDataAvailable,
        comparisonWindowComplete,

        displayBucketKey,
        displayBreakoutValue AS breakoutValue,
        max(CASE WHEN isSyntheticOtherMember THEN 1 ELSE 0 END) = 1 AS isOtherBucket,

        CASE
            WHEN max(CASE WHEN isSyntheticOtherMember THEN 1 ELSE 0 END) = 1 THEN 101
            ELSE min(impactRankWithinBreakout)
        END AS displayRankWithinBreakout,

        count(*) AS rawMemberCount,
        min(impactRankWithinBreakout) AS rawMinImpactRankWithinBreakout,
        max(impactRankWithinBreakout) AS rawMaxImpactRankWithinBreakout,

        sum(thisWeekNumerator) AS currentNumerator,
        sum(thisWeekDenominator) AS currentDenominator,
        sum(comparisonNumerator) AS comparisonNumerator,
        sum(comparisonDenominator) AS comparisonDenominator,

        sum(peerSetNumerator) AS peerSetNumerator,
        sum(peerSetDenominator) AS peerSetDenominator,

        max(toplineCurrentNumerator) AS toplineCurrentNumerator,
        max(toplineCurrentDenominator) AS toplineCurrentDenominator,
        max(toplineComparisonNumerator) AS toplineComparisonNumerator,
        max(toplineComparisonDenominator) AS toplineComparisonDenominator,
        max(toplineCurrentValue) AS toplineCurrentValue,
        max(toplineComparisonValue) AS toplineComparisonValue,

        thisWeekDataAvailable,
        max(goldProcessedAt) AS goldProcessedAt
    FROM allBucketMembers
    GROUP BY
        targetWeekStartDate,
        targetWeekEndDate,
        fiscalQuarterLabel,
        fiscalWeekCode,
        weekLabel,
        weekEndingLabel,
        filterLob,
        filterPlatform,
        breakoutType,
        breakoutLabel,
        breakoutSortOrder,
        breakoutDefinitionStatus,
        configuredTopN,
        configuredPairTopN,
        metricName,
        metricLabel,
        metricDescription,
        metricKind,
        displayFormat,
        changeUnit,
        metricSortOrder,
        metricDefinitionStatus,
        comparisonType,
        comparisonLabel,
        comparisonSortOrder,
        comparisonStartDate,
        comparisonEndDate,
        comparisonWeekCount,
        comparisonDataAvailable,
        comparisonWindowComplete,
        displayBucketKey,
        displayBreakoutValue,
        thisWeekDataAvailable
),
allBucketValues AS (
    SELECT
        a.*,
        CASE
            WHEN metricKind = 'ratio' THEN try_divide(currentNumerator, currentDenominator)
            ELSE currentNumerator
        END AS currentValue,
        CASE
            WHEN metricKind = 'ratio' THEN try_divide(comparisonNumerator, comparisonDenominator)
            ELSE comparisonNumerator
        END AS comparisonValue,
        CASE
            WHEN metricKind = 'ratio' THEN try_divide(peerSetNumerator, peerSetDenominator)
            ELSE peerSetNumerator
        END AS peerSetValue,
        CASE
            WHEN metricKind = 'ratio'
                THEN peerSetNumerator IS NOT NULL AND nullif(peerSetDenominator, 0D) IS NOT NULL
            ELSE peerSetNumerator IS NOT NULL
        END AS peerSetDataAvailable
    FROM allBucketAgg a
),
allBucketCalculated AS (
    SELECT
        v.*,
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
        CASE
            WHEN metricKind = 'count' THEN 'pct'
            WHEN metricKind = 'ratio' THEN 'pp'
            ELSE NULL
        END AS impactOnToplineUnit,
        currentValue - peerSetValue AS peerSetAbsoluteDeltaValue,
        CASE
            WHEN NOT peerSetDataAvailable OR currentValue IS NULL THEN NULL
            WHEN changeUnit = 'pp' THEN 100D * (currentValue - peerSetValue)
            WHEN changeUnit = 'pct' THEN 100D * (try_divide(currentValue, peerSetValue) - 1D)
            ELSE NULL
        END AS peerSetChangeValue
    FROM allBucketValues v
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

    breakoutType,
    breakoutLabel,
    breakoutValue,
    breakoutSortOrder,
    breakoutDefinitionStatus,
    configuredTopN,
    configuredPairTopN,

    'all' AS displaySize,
    'All' AS displaySizeLabel,
    100 AS displayLimit,
    displayRankWithinBreakout,
    isOtherBucket,
    rawMemberCount,
    rawMinImpactRankWithinBreakout,
    rawMaxImpactRankWithinBreakout,

    metricName,
    metricLabel,
    metricDescription,
    metricKind,
    displayFormat,
    changeUnit,
    metricSortOrder,
    metricDefinitionStatus,

    comparisonType,
    comparisonLabel,
    comparisonSortOrder,
    comparisonStartDate,
    comparisonEndDate,
    comparisonWeekCount,
    comparisonDataAvailable,
    comparisonWindowComplete,

    currentNumerator,
    currentDenominator,
    comparisonNumerator,
    comparisonDenominator,
    currentValue,
    comparisonValue,
    absoluteDeltaValue,
    changeValue,
    changeDirection,

    toplineCurrentNumerator,
    toplineCurrentDenominator,
    toplineComparisonNumerator,
    toplineComparisonDenominator,
    toplineCurrentValue,
    toplineComparisonValue,
    impactOnToplineValue,
    impactOnToplineUnit,

    peerSetNumerator,
    peerSetDenominator,
    peerSetValue,
    peerSetAbsoluteDeltaValue,
    peerSetChangeValue,
    peerSetDataAvailable,

    thisWeekDataAvailable,
    goldProcessedAt
FROM allBucketCalculated;
