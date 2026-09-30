-- ============================================================================
-- FILE  : 08_sdi_vw_mip_serving_breakoutsTrend_long.sql
-- LAYER : SERVING
-- TAB   : Breakouts
-- SECTION: Trend
-- PURPOSE:
--   Weekly breakout-value trend by metric.
--
-- NAMING:
--   Object name expresses app tab/section + physical shape only.
--   Current reporting grain is week-based, but cadence/grain is intentionally
--   not encoded in the serving-view object name.
-- ============================================================================

CREATE OR REPLACE VIEW prdrzranalytics.lab42.sdi_vw_mip_serving_breakoutsTrend_long AS
SELECT
    targetWeekStartDate AS weekStartDate,
    targetWeekEndDate AS weekEndDate,
    fiscalQuarterLabel,
    fiscalWeekCode,
    weekLabel,
    filterLob,
    filterPlatform,

    breakoutType,
    breakoutLabel,
    breakoutValue,
    valueRankByNbv,
    isTopN,
    breakoutSortOrder,

    metricName,
    metricLabel,
    metricKind,
    displayFormat,
    changeUnit,
    metricSortOrder,

    currentValue AS metricValue,
    wowChange,
    fourWeekAvgValue,
    vsFourWeekAvgChange,
    sameWeekLastYearValue,
    yoyChange,

    thisWeekDataAvailable,
    goldProcessedAt

FROM prdrzranalytics.lab42.sdi_vw_mip_serving_breakoutsTable_long;
