-- ============================================================================
-- FILE  : 08_sdi_mv_mip_serving_crosstabsMatrix_long.sql
-- LAYER : SERVING / MATERIALIZED
-- TAB   : Crosstabs
-- SECTION: Matrix
-- PURPOSE:
--   Comparator-aware crosstab-cell contract with explicit displaySize variants.
--   top5/top8/top10 bucket each axis to the selected Top-N + (Other).
--   all is capped at Top 100 + (Other) independently on rows and columns.
--   Cells are then re-aggregated from numerators/denominators so ratio metrics remain correct.
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

CREATE OR REPLACE MATERIALIZED VIEW prdrzranalytics.lab42.sdi_mv_mip_serving_crosstabsMatrix_long
COMMENT 'MIP Crosstabs matrix. Comparator-aware Top5/Top8/Top10/All buckets; All = Top100 + Other on each axis.'
CLUSTER BY (targetWeekStartDate, metricName, pairKey, comparisonType)
REFRESH POLICY AUTO
TRIGGER ON UPDATE AT MOST EVERY INTERVAL 1 MINUTE
AS
WITH base AS (
    SELECT
        g.targetWeekStartDate,g.targetWeekEndDate,g.fiscalQuarterLabel,g.fiscalWeekCode,g.weekLabel,
        c.weekEndingLabel,c.priorWeekStartDate,c.fourWeekAvgStartDate,c.fourWeekAvgEndDate,c.sameWeekLastYearStartDate,
        g.filterLob,g.filterPlatform,
        g.pairKey,g.pairLabel,pc.sortOrder AS pairSortOrder,
        g.rowBreakoutType,g.rowBreakoutValue,rb.pairTopN AS configuredRowPairTopN,
        g.columnBreakoutType,g.columnBreakoutValue,cb.pairTopN AS configuredColumnPairTopN,
        g.metricName,g.metricLabel,mc.metricDescription,g.metricKind,g.displayFormat,g.changeUnit,
        mc.definitionStatus AS metricDefinitionStatus,mc.sortOrder AS metricSortOrder,
        g.thisWeekNumerator,g.thisWeekDenominator,g.priorWeekNumerator,g.priorWeekDenominator,
        g.fourWeekTrendNumerator,g.fourWeekTrendDenominator,g.sameWeekLyNumerator,g.sameWeekLyDenominator,
        g.peerSetNumerator,g.peerSetDenominator,
        g.thisWeekDataAvailable,g.priorWeekDataAvailable,g.fourWeekTrendWeekCount,g.sameWeekLyDataAvailable,
        g.goldProcessedAt
    FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_crosstabMetricIngredientsByWeek_long g
    JOIN prdrzranalytics.lab42.sdi_vw_mip_control_crosstabCatalog_static pc ON pc.pairKey=g.pairKey AND pc.isActive
    JOIN prdrzranalytics.lab42.sdi_vw_mip_control_breakoutCatalog_static rb ON rb.breakoutType=g.rowBreakoutType AND rb.isActive
    JOIN prdrzranalytics.lab42.sdi_vw_mip_control_breakoutCatalog_static cb ON cb.breakoutType=g.columnBreakoutType AND cb.isActive
    JOIN prdrzranalytics.lab42.sdi_vw_mip_control_metricCatalog_static mc ON mc.metricName=g.metricName AND mc.isActive
    LEFT JOIN prdrzranalytics.lab42.sdi_vw_mip_control_fiscalCalendar_static c ON c.weekStartDate=g.targetWeekStartDate
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
    SELECT targetWeekStartDate,filterLob,filterPlatform,metricName,metricKind,
           thisWeekNumerator,thisWeekDenominator,priorWeekNumerator,priorWeekDenominator,
           fourWeekTrendNumerator,fourWeekTrendDenominator,sameWeekLyNumerator,sameWeekLyDenominator,fourWeekTrendWeekCount
    FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_overviewMetricIngredientsByWeek_long
),
toplineLong AS (
    SELECT *, 'priorWeek' AS comparisonType, priorWeekNumerator AS comparisonNumerator, priorWeekDenominator AS comparisonDenominator FROM toplineBase
    UNION ALL
    SELECT *, 'fourWeek' AS comparisonType,
           CASE WHEN metricKind='count' AND fourWeekTrendWeekCount>0 THEN try_divide(fourWeekTrendNumerator,cast(fourWeekTrendWeekCount AS DOUBLE)) ELSE fourWeekTrendNumerator END,
           CASE WHEN metricKind='count' THEN NULL ELSE fourWeekTrendDenominator END
    FROM toplineBase
    UNION ALL
    SELECT *, 'lastYear' AS comparisonType, sameWeekLyNumerator, sameWeekLyDenominator FROM toplineBase
),
toplineValues AS (
    SELECT *,
           CASE WHEN metricKind='ratio' THEN try_divide(thisWeekNumerator,thisWeekDenominator) ELSE thisWeekNumerator END AS toplineCurrentValue,
           CASE WHEN metricKind='ratio' THEN try_divide(comparisonNumerator,comparisonDenominator) ELSE comparisonNumerator END AS toplineComparisonValue
    FROM toplineLong
),
cellValues AS (
    SELECT
        p.*,
        t.toplineCurrentValue,t.toplineComparisonValue,
        t.thisWeekDenominator AS toplineCurrentDenominator,t.comparisonDenominator AS toplineComparisonDenominator,
        CASE WHEN p.metricKind='count' THEN 100D*try_divide(p.absoluteDeltaValue,t.toplineComparisonValue)
             WHEN p.metricKind='ratio' THEN 100D*(try_divide(p.thisWeekNumerator,t.thisWeekDenominator)-try_divide(p.comparisonNumerator,t.comparisonDenominator)) END AS impactOnToplineValue,
        CASE WHEN p.metricKind='count' THEN 'pct' WHEN p.metricKind='ratio' THEN 'pp' END AS impactOnToplineUnit,
        p.currentValue-p.peerSetValue AS peerSetAbsoluteDeltaValue,
        CASE WHEN NOT p.peerSetDataAvailable THEN NULL
             WHEN p.changeUnit='pp' THEN 100D*(p.currentValue-p.peerSetValue)
             WHEN p.changeUnit='pct' THEN 100D*(try_divide(p.currentValue,p.peerSetValue)-1D) END AS peerSetChangeValue
    FROM peerCalculated p
    LEFT JOIN toplineValues t
      ON t.targetWeekStartDate=p.targetWeekStartDate AND t.filterLob=p.filterLob AND t.filterPlatform=p.filterPlatform
     AND t.metricName=p.metricName AND t.comparisonType=p.comparisonType
),
rowAgg AS (
    SELECT
        targetWeekStartDate,filterLob,filterPlatform,pairKey,metricName,comparisonType,rowBreakoutValue,metricKind,
        sum(thisWeekNumerator) AS currentNumerator,sum(thisWeekDenominator) AS currentDenominator,
        sum(comparisonNumerator) AS comparisonNumerator,sum(comparisonDenominator) AS comparisonDenominator
    FROM cellValues
    GROUP BY targetWeekStartDate,filterLob,filterPlatform,pairKey,metricName,comparisonType,rowBreakoutValue,metricKind
),
rowValues AS (
    SELECT *,
        CASE WHEN metricKind='ratio' THEN try_divide(currentNumerator,currentDenominator) ELSE currentNumerator END AS rowCurrentValue,
        CASE WHEN metricKind='ratio' THEN try_divide(comparisonNumerator,comparisonDenominator) ELSE comparisonNumerator END AS rowComparisonValue
    FROM rowAgg
),
rowRanks AS (
    SELECT *, row_number() OVER (
        PARTITION BY targetWeekStartDate,filterLob,filterPlatform,pairKey,metricName,comparisonType
        ORDER BY rowCurrentValue DESC NULLS LAST,rowBreakoutValue
    ) AS rowRankByMetric
    FROM rowValues
),
columnAgg AS (
    SELECT
        targetWeekStartDate,filterLob,filterPlatform,pairKey,metricName,comparisonType,columnBreakoutValue,metricKind,
        sum(thisWeekNumerator) AS currentNumerator,sum(thisWeekDenominator) AS currentDenominator,
        sum(comparisonNumerator) AS comparisonNumerator,sum(comparisonDenominator) AS comparisonDenominator
    FROM cellValues
    GROUP BY targetWeekStartDate,filterLob,filterPlatform,pairKey,metricName,comparisonType,columnBreakoutValue,metricKind
),
columnValues AS (
    SELECT *,
        CASE WHEN metricKind='ratio' THEN try_divide(currentNumerator,currentDenominator) ELSE currentNumerator END AS columnCurrentValue,
        CASE WHEN metricKind='ratio' THEN try_divide(comparisonNumerator,comparisonDenominator) ELSE comparisonNumerator END AS columnComparisonValue
    FROM columnAgg
),
columnRanks AS (
    SELECT *, row_number() OVER (
        PARTITION BY targetWeekStartDate,filterLob,filterPlatform,pairKey,metricName,comparisonType
        ORDER BY columnCurrentValue DESC NULLS LAST,columnBreakoutValue
    ) AS columnRankByMetric
    FROM columnValues
),
ranked AS (
    SELECT
        c.*,
        r.rowCurrentValue,r.rowComparisonValue,r.rowRankByMetric,
        k.columnCurrentValue,k.columnComparisonValue,k.columnRankByMetric,
        CASE WHEN c.comparisonDataAvailable AND c.impactOnToplineValue IS NOT NULL THEN
            row_number() OVER (
                PARTITION BY c.targetWeekStartDate,c.filterLob,c.filterPlatform,c.pairKey,c.metricName,c.comparisonType
                ORDER BY abs(c.impactOnToplineValue) DESC NULLS LAST,abs(c.absoluteDeltaValue) DESC NULLS LAST,
                         c.rowBreakoutValue,c.columnBreakoutValue
            )
        END AS cellImpactRankWithinPair,
        CASE WHEN c.comparisonDataAvailable AND c.impactOnToplineValue IS NOT NULL THEN
            row_number() OVER (
                PARTITION BY c.targetWeekStartDate,c.filterLob,c.filterPlatform,c.metricName,c.comparisonType
                ORDER BY abs(c.impactOnToplineValue) DESC NULLS LAST,abs(c.absoluteDeltaValue) DESC NULLS LAST,
                         c.pairKey,c.rowBreakoutValue,c.columnBreakoutValue
            )
        END AS cellImpactRankAcrossPairs
    FROM cellValues c
    LEFT JOIN rowRanks r
      ON r.targetWeekStartDate=c.targetWeekStartDate AND r.filterLob=c.filterLob AND r.filterPlatform=c.filterPlatform
     AND r.pairKey=c.pairKey AND r.metricName=c.metricName AND r.comparisonType=c.comparisonType
     AND r.rowBreakoutValue=c.rowBreakoutValue
    LEFT JOIN columnRanks k
      ON k.targetWeekStartDate=c.targetWeekStartDate AND k.filterLob=c.filterLob AND k.filterPlatform=c.filterPlatform
     AND k.pairKey=c.pairKey AND k.metricName=c.metricName AND k.comparisonType=c.comparisonType
     AND k.columnBreakoutValue=c.columnBreakoutValue
)
,
sizeConfig AS (
    SELECT * FROM VALUES
        ('top5',  'Top 5',  5,   10),
        ('top8',  'Top 8',  8,   20),
        ('top10', 'Top 10', 10,  30),
        ('all',   'All',    100, 40)
    AS s(displaySize, displaySizeLabel, displayLimit, displaySizeSortOrder)
),
expanded AS (
    SELECT
        r.*,
        s.displaySize,
        s.displaySizeLabel,
        s.displayLimit,
        s.displaySizeSortOrder,

        CASE
            WHEN rowRankByMetric <= s.displayLimit
                THEN concat('ROW::VALUE::', coalesce(rowBreakoutValue, '(null)'))
            ELSE 'ROW::OTHER::REMAINDER'
        END AS rowBucketKey,
        CASE
            WHEN rowRankByMetric <= s.displayLimit THEN rowBreakoutValue
            ELSE '(Other)'
        END AS displayRowBreakoutValue,
        rowRankByMetric > s.displayLimit AS rowOtherMember,

        CASE
            WHEN columnRankByMetric <= s.displayLimit
                THEN concat('COL::VALUE::', coalesce(columnBreakoutValue, '(null)'))
            ELSE 'COL::OTHER::REMAINDER'
        END AS columnBucketKey,
        CASE
            WHEN columnRankByMetric <= s.displayLimit THEN columnBreakoutValue
            ELSE '(Other)'
        END AS displayColumnBreakoutValue,
        columnRankByMetric > s.displayLimit AS columnOtherMember
    FROM ranked r
    CROSS JOIN sizeConfig s
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

        pairKey,
        pairLabel,
        pairSortOrder,
        rowBreakoutType,
        columnBreakoutType,
        configuredRowPairTopN,
        configuredColumnPairTopN,

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

        displaySize,
        displaySizeLabel,
        displayLimit,
        displaySizeSortOrder,

        rowBucketKey,
        displayRowBreakoutValue AS rowBreakoutValue,
        max(CASE WHEN rowOtherMember THEN 1 ELSE 0 END) = 1 AS isRowOtherBucket,
        CASE
            WHEN max(CASE WHEN rowOtherMember THEN 1 ELSE 0 END) = 1 THEN displayLimit + 1
            ELSE min(rowRankByMetric)
        END AS rowDisplayRank,

        columnBucketKey,
        displayColumnBreakoutValue AS columnBreakoutValue,
        max(CASE WHEN columnOtherMember THEN 1 ELSE 0 END) = 1 AS isColumnOtherBucket,
        CASE
            WHEN max(CASE WHEN columnOtherMember THEN 1 ELSE 0 END) = 1 THEN displayLimit + 1
            ELSE min(columnRankByMetric)
        END AS columnDisplayRank,

        count(*) AS rawCellMemberCount,

        sum(thisWeekNumerator) AS currentNumerator,
        sum(thisWeekDenominator) AS currentDenominator,
        sum(comparisonNumerator) AS comparisonNumerator,
        sum(comparisonDenominator) AS comparisonDenominator,

        sum(peerSetNumerator) AS peerSetNumerator,
        sum(peerSetDenominator) AS peerSetDenominator,

        max(toplineCurrentValue) AS toplineCurrentValue,
        max(toplineComparisonValue) AS toplineComparisonValue,
        max(toplineCurrentDenominator) AS toplineCurrentDenominator,
        max(toplineComparisonDenominator) AS toplineComparisonDenominator,

        thisWeekDataAvailable,
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
        pairKey,
        pairLabel,
        pairSortOrder,
        rowBreakoutType,
        columnBreakoutType,
        configuredRowPairTopN,
        configuredColumnPairTopN,
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
        displaySize,
        displaySizeLabel,
        displayLimit,
        displaySizeSortOrder,
        rowBucketKey,
        displayRowBreakoutValue,
        columnBucketKey,
        displayColumnBreakoutValue,
        thisWeekDataAvailable
),
bucketValues AS (
    SELECT
        a.*,
        CASE WHEN metricKind='ratio' THEN try_divide(currentNumerator,currentDenominator)
             ELSE currentNumerator END AS currentValue,
        CASE WHEN metricKind='ratio' THEN try_divide(comparisonNumerator,comparisonDenominator)
             ELSE comparisonNumerator END AS comparisonValue,
        CASE WHEN metricKind='ratio' THEN try_divide(peerSetNumerator,peerSetDenominator)
             ELSE peerSetNumerator END AS peerSetValue,
        CASE WHEN metricKind='ratio'
                  THEN peerSetNumerator IS NOT NULL AND nullif(peerSetDenominator,0D) IS NOT NULL
             ELSE peerSetNumerator IS NOT NULL END AS peerSetDataAvailable
    FROM bucketAgg a
),
bucketCalculated AS (
    SELECT
        v.*,
        currentValue-comparisonValue AS absoluteDeltaValue,
        CASE
            WHEN NOT thisWeekDataAvailable OR NOT comparisonDataAvailable
              OR currentValue IS NULL OR comparisonValue IS NULL THEN NULL
            WHEN changeUnit='pp' THEN 100D*(currentValue-comparisonValue)
            WHEN changeUnit='pct' THEN 100D*(try_divide(currentValue,comparisonValue)-1D)
            ELSE NULL
        END AS changeValue,
        CASE
            WHEN currentValue IS NULL OR comparisonValue IS NULL THEN 'unavailable'
            WHEN currentValue > comparisonValue THEN 'up'
            WHEN currentValue < comparisonValue THEN 'down'
            ELSE 'flat'
        END AS changeDirection,
        CASE
            WHEN metricKind='count' THEN 100D*try_divide(currentValue-comparisonValue,toplineComparisonValue)
            WHEN metricKind='ratio' THEN 100D*(
                try_divide(currentNumerator,toplineCurrentDenominator)
                - try_divide(comparisonNumerator,toplineComparisonDenominator)
            )
            ELSE NULL
        END AS impactOnToplineValue,
        CASE WHEN metricKind='count' THEN 'pct'
             WHEN metricKind='ratio' THEN 'pp'
             ELSE NULL END AS impactOnToplineUnit,
        currentValue-peerSetValue AS peerSetAbsoluteDeltaValue,
        CASE
            WHEN NOT peerSetDataAvailable OR currentValue IS NULL THEN NULL
            WHEN changeUnit='pp' THEN 100D*(currentValue-peerSetValue)
            WHEN changeUnit='pct' THEN 100D*(try_divide(currentValue,peerSetValue)-1D)
            ELSE NULL
        END AS peerSetChangeValue
    FROM bucketValues v
),
rowAggDisplay AS (
    SELECT
        targetWeekStartDate,filterLob,filterPlatform,pairKey,metricName,comparisonType,displaySize,
        rowBreakoutValue,rowDisplayRank,isRowOtherBucket,metricKind,
        sum(currentNumerator) AS rowCurrentNumerator,
        sum(currentDenominator) AS rowCurrentDenominator,
        sum(comparisonNumerator) AS rowComparisonNumerator,
        sum(comparisonDenominator) AS rowComparisonDenominator
    FROM bucketCalculated
    GROUP BY
        targetWeekStartDate,filterLob,filterPlatform,pairKey,metricName,comparisonType,displaySize,
        rowBreakoutValue,rowDisplayRank,isRowOtherBucket,metricKind
),
rowValuesDisplay AS (
    SELECT
        *,
        CASE WHEN metricKind='ratio' THEN try_divide(rowCurrentNumerator,rowCurrentDenominator)
             ELSE rowCurrentNumerator END AS rowCurrentValue,
        CASE WHEN metricKind='ratio' THEN try_divide(rowComparisonNumerator,rowComparisonDenominator)
             ELSE rowComparisonNumerator END AS rowComparisonValue
    FROM rowAggDisplay
),
columnAggDisplay AS (
    SELECT
        targetWeekStartDate,filterLob,filterPlatform,pairKey,metricName,comparisonType,displaySize,
        columnBreakoutValue,columnDisplayRank,isColumnOtherBucket,metricKind,
        sum(currentNumerator) AS columnCurrentNumerator,
        sum(currentDenominator) AS columnCurrentDenominator,
        sum(comparisonNumerator) AS columnComparisonNumerator,
        sum(comparisonDenominator) AS columnComparisonDenominator
    FROM bucketCalculated
    GROUP BY
        targetWeekStartDate,filterLob,filterPlatform,pairKey,metricName,comparisonType,displaySize,
        columnBreakoutValue,columnDisplayRank,isColumnOtherBucket,metricKind
),
columnValuesDisplay AS (
    SELECT
        *,
        CASE WHEN metricKind='ratio' THEN try_divide(columnCurrentNumerator,columnCurrentDenominator)
             ELSE columnCurrentNumerator END AS columnCurrentValue,
        CASE WHEN metricKind='ratio' THEN try_divide(columnComparisonNumerator,columnComparisonDenominator)
             ELSE columnComparisonNumerator END AS columnComparisonValue
    FROM columnAggDisplay
),
withTotals AS (
    SELECT
        c.*,
        r.rowCurrentValue,
        r.rowComparisonValue,
        k.columnCurrentValue,
        k.columnComparisonValue
    FROM bucketCalculated c
    LEFT JOIN rowValuesDisplay r
      ON r.targetWeekStartDate=c.targetWeekStartDate
     AND r.filterLob=c.filterLob
     AND r.filterPlatform=c.filterPlatform
     AND r.pairKey=c.pairKey
     AND r.metricName=c.metricName
     AND r.comparisonType=c.comparisonType
     AND r.displaySize=c.displaySize
     AND r.rowBreakoutValue=c.rowBreakoutValue
     AND r.rowDisplayRank=c.rowDisplayRank
    LEFT JOIN columnValuesDisplay k
      ON k.targetWeekStartDate=c.targetWeekStartDate
     AND k.filterLob=c.filterLob
     AND k.filterPlatform=c.filterPlatform
     AND k.pairKey=c.pairKey
     AND k.metricName=c.metricName
     AND k.comparisonType=c.comparisonType
     AND k.displaySize=c.displaySize
     AND k.columnBreakoutValue=c.columnBreakoutValue
     AND k.columnDisplayRank=c.columnDisplayRank
),
finalRanked AS (
    SELECT
        w.*,
        row_number() OVER (
            PARTITION BY targetWeekStartDate,filterLob,filterPlatform,pairKey,metricName,comparisonType,displaySize
            ORDER BY abs(impactOnToplineValue) DESC NULLS LAST,
                     abs(absoluteDeltaValue) DESC NULLS LAST,
                     rowBreakoutValue,columnBreakoutValue
        ) AS cellImpactRankWithinPair,
        row_number() OVER (
            PARTITION BY targetWeekStartDate,filterLob,filterPlatform,metricName,comparisonType,displaySize
            ORDER BY abs(impactOnToplineValue) DESC NULLS LAST,
                     abs(absoluteDeltaValue) DESC NULLS LAST,
                     pairKey,rowBreakoutValue,columnBreakoutValue
        ) AS cellImpactRankAcrossPairs
    FROM withTotals w
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

    displaySize,
    displaySizeLabel,
    displayLimit,
    displaySizeSortOrder,

    rowBreakoutType,
    rowBreakoutValue,
    rowDisplayRank,
    isRowOtherBucket,
    rowCurrentValue,
    rowComparisonValue,
    configuredRowPairTopN,

    columnBreakoutType,
    columnBreakoutValue,
    columnDisplayRank,
    isColumnOtherBucket,
    columnCurrentValue,
    columnComparisonValue,
    configuredColumnPairTopN,

    rawCellMemberCount,

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

    toplineCurrentValue,
    toplineComparisonValue,
    impactOnToplineValue,
    impactOnToplineUnit,

    cellImpactRankWithinPair,
    cellImpactRankAcrossPairs,

    peerSetValue,
    peerSetAbsoluteDeltaValue,
    peerSetChangeValue,
    peerSetDataAvailable,

    thisWeekDataAvailable,
    goldProcessedAt
FROM finalRanked;
