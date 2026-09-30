-- ============================================================================
-- FILE  : 09_sdi_vw_mip_serving_crosstabsMatrix_long.sql
-- LAYER : SERVING
-- TAB   : Crosstabs
-- SECTION: Matrix
-- PURPOSE:
--   UI/API-ready supported crosstab matrix.
--   Row/column values beyond pairTopN are bucketed into '(Other)' here,
--   then comparison values are recalculated from aggregated ingredients.
--
-- NAMING:
--   Object name expresses app tab/section + physical shape only.
--   Current reporting grain is week-based, but cadence/grain is intentionally
--   not encoded in the serving-view object name.
-- ============================================================================

CREATE OR REPLACE VIEW prdrzranalytics.lab42.sdi_vw_mip_serving_crosstabsMatrix_long AS
WITH rowTotals AS (
    SELECT
        targetWeekStartDate,
        filterLob,
        filterPlatform,
        pairKey,
        rowBreakoutValue,
        sum(coalesce(thisWeekNumerator, 0D)) AS nbv
    FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_crosstabMetricIngredientsByWeek_long
    WHERE metricName = 'nbv'
    GROUP BY
        targetWeekStartDate,
        filterLob,
        filterPlatform,
        pairKey,
        rowBreakoutValue
),
rowRanks AS (
    SELECT
        *,
        dense_rank() OVER (
            PARTITION BY targetWeekStartDate, filterLob, filterPlatform, pairKey
            ORDER BY nbv DESC, rowBreakoutValue
        ) AS rowRankByNbv
    FROM rowTotals
),
columnTotals AS (
    SELECT
        targetWeekStartDate,
        filterLob,
        filterPlatform,
        pairKey,
        columnBreakoutValue,
        sum(coalesce(thisWeekNumerator, 0D)) AS nbv
    FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_crosstabMetricIngredientsByWeek_long
    WHERE metricName = 'nbv'
    GROUP BY
        targetWeekStartDate,
        filterLob,
        filterPlatform,
        pairKey,
        columnBreakoutValue
),
columnRanks AS (
    SELECT
        *,
        dense_rank() OVER (
            PARTITION BY targetWeekStartDate, filterLob, filterPlatform, pairKey
            ORDER BY nbv DESC, columnBreakoutValue
        ) AS columnRankByNbv
    FROM columnTotals
),
bucketedRaw AS (
    SELECT
        g.*,
        p.sortOrder AS pairSortOrder,
        rb.pairTopN AS rowPairTopN,
        cb.pairTopN AS columnPairTopN,
        rr.rowRankByNbv,
        cr.columnRankByNbv,

        CASE
            WHEN rb.pairTopN IS NULL OR rr.rowRankByNbv <= rb.pairTopN
                THEN g.rowBreakoutValue
            ELSE '(Other)'
        END AS rowBreakoutValueBucket,

        CASE
            WHEN cb.pairTopN IS NULL OR cr.columnRankByNbv <= cb.pairTopN
                THEN g.columnBreakoutValue
            ELSE '(Other)'
        END AS columnBreakoutValueBucket

    FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_crosstabMetricIngredientsByWeek_long g

    JOIN prdrzranalytics.lab42.sdi_vw_mip_control_crosstabCatalog_static p
      ON  p.pairKey = g.pairKey
      AND p.isActive

    JOIN prdrzranalytics.lab42.sdi_vw_mip_control_breakoutCatalog_static rb
      ON  rb.breakoutType = g.rowBreakoutType
      AND rb.isActive

    JOIN prdrzranalytics.lab42.sdi_vw_mip_control_breakoutCatalog_static cb
      ON  cb.breakoutType = g.columnBreakoutType
      AND cb.isActive

    LEFT JOIN rowRanks rr
      ON  rr.targetWeekStartDate = g.targetWeekStartDate
      AND rr.filterLob = g.filterLob
      AND rr.filterPlatform = g.filterPlatform
      AND rr.pairKey = g.pairKey
      AND rr.rowBreakoutValue = g.rowBreakoutValue

    LEFT JOIN columnRanks cr
      ON  cr.targetWeekStartDate = g.targetWeekStartDate
      AND cr.filterLob = g.filterLob
      AND cr.filterPlatform = g.filterPlatform
      AND cr.pairKey = g.pairKey
      AND cr.columnBreakoutValue = g.columnBreakoutValue
),
bucketedIngredients AS (
    SELECT
        targetWeekStartDate,
        max(targetWeekEndDate) AS targetWeekEndDate,
        max(fiscalQuarterLabel) AS fiscalQuarterLabel,
        max(fiscalWeekCode) AS fiscalWeekCode,
        max(weekLabel) AS weekLabel,
        filterLob,
        filterPlatform,

        pairKey,
        max(pairLabel) AS pairLabel,
        max(pairSortOrder) AS pairSortOrder,

        max(rowBreakoutType) AS rowBreakoutType,
        rowBreakoutValueBucket AS rowBreakoutValue,
        CASE
            WHEN rowBreakoutValueBucket = '(Other)' THEN NULL
            ELSE min(rowRankByNbv)
        END AS rowRankByNbv,

        max(columnBreakoutType) AS columnBreakoutType,
        columnBreakoutValueBucket AS columnBreakoutValue,
        CASE
            WHEN columnBreakoutValueBucket = '(Other)' THEN NULL
            ELSE min(columnRankByNbv)
        END AS columnRankByNbv,

        metricName,
        max(metricLabel) AS metricLabel,
        max(metricKind) AS metricKind,
        max(displayFormat) AS displayFormat,
        max(changeUnit) AS changeUnit,

        sum(thisWeekNumerator) AS thisWeekNumerator,
        sum(thisWeekDenominator) AS thisWeekDenominator,
        sum(priorWeekNumerator) AS priorWeekNumerator,
        sum(priorWeekDenominator) AS priorWeekDenominator,
        sum(fourWeekTrendNumerator) AS fourWeekTrendNumerator,
        sum(fourWeekTrendDenominator) AS fourWeekTrendDenominator,
        sum(sameWeekLyNumerator) AS sameWeekLyNumerator,
        sum(sameWeekLyDenominator) AS sameWeekLyDenominator,

        max(CASE WHEN thisWeekDataAvailable THEN 1 ELSE 0 END) = 1
            AS thisWeekDataAvailable,
        max(CASE WHEN priorWeekDataAvailable THEN 1 ELSE 0 END) = 1
            AS priorWeekDataAvailable,
        max(fourWeekTrendWeekCount) AS fourWeekTrendWeekCount,
        max(CASE WHEN sameWeekLyDataAvailable THEN 1 ELSE 0 END) = 1
            AS sameWeekLyDataAvailable,

        max(goldProcessedAt) AS goldProcessedAt

    FROM bucketedRaw

    GROUP BY
        targetWeekStartDate,
        filterLob,
        filterPlatform,
        pairKey,
        rowBreakoutValueBucket,
        columnBreakoutValueBucket,
        metricName
),
base AS (
    SELECT
        b.*,
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

    pairKey,
    pairLabel,
    pairSortOrder,

    rowBreakoutType,
    rowBreakoutValue,
    rowRankByNbv,

    columnBreakoutType,
    columnBreakoutValue,
    columnRankByNbv,

    metricName,
    metricLabel,
    metricDescription,
    metricKind,
    displayFormat,
    changeUnit,
    metricDefinitionStatus,
    metricSortOrder,

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
