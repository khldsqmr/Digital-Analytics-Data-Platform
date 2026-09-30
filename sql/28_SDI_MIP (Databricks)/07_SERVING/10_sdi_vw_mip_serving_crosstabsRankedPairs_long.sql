-- ============================================================================
-- FILE  : 10_sdi_vw_mip_serving_crosstabsRankedPairs_long.sql
-- LAYER : SERVING
-- TAB   : Crosstabs
-- SECTION: Ranked Pairs
-- PURPOSE:
--   Flattens matrix cells into rankable breakout-value pairs for each metric.
--
-- NAMING:
--   Object name expresses app tab/section + physical shape only.
--   Current reporting grain is week-based, but cadence/grain is intentionally
--   not encoded in the serving-view object name.
-- ============================================================================

CREATE OR REPLACE VIEW prdrzranalytics.lab42.sdi_vw_mip_serving_crosstabsRankedPairs_long AS
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
    columnBreakoutType,
    columnBreakoutValue,

    concat(rowBreakoutValue, ' × ', columnBreakoutValue) AS cellLabel,

    metricName,
    metricLabel,
    metricKind,
    displayFormat,
    changeUnit,
    metricSortOrder,

    currentValue,
    priorWeekValue,
    wowChange,
    fourWeekAvgValue,
    vsFourWeekAvgChange,
    sameWeekLastYearValue,
    yoyChange,

    dense_rank() OVER (
        PARTITION BY
            targetWeekStartDate,
            filterLob,
            filterPlatform,
            pairKey,
            metricName
        ORDER BY currentValue DESC NULLS LAST,
                 rowBreakoutValue,
                 columnBreakoutValue
    ) AS cellRankByMetric,

    thisWeekDataAvailable,
    goldProcessedAt

FROM prdrzranalytics.lab42.sdi_vw_mip_serving_crosstabsMatrix_long;
