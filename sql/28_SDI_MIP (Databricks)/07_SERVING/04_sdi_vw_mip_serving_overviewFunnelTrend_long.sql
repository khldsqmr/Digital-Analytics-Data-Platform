-- ============================================================================
-- FILE  : 04_sdi_vw_mip_serving_overviewFunnelTrend_long.sql
-- LAYER : SERVING
-- TAB   : Overview
-- SECTION: Funnel Trend
-- PURPOSE:
--   Weekly funnel-stage/conversion trend. API filters metricName as needed.
--
-- NAMING:
--   Object name expresses app tab/section + physical shape only.
--   Current reporting grain is week-based, but cadence/grain is intentionally
--   not encoded in the serving-view object name.
-- ============================================================================

CREATE OR REPLACE VIEW prdrzranalytics.lab42.sdi_vw_mip_serving_overviewFunnelTrend_long AS
SELECT
    targetWeekStartDate AS weekStartDate,
    targetWeekEndDate AS weekEndDate,
    fiscalQuarterLabel,
    fiscalWeekCode,
    weekLabel,
    filterLob,
    filterPlatform,

    metricName,
    metricLabel,
    metricDescription,
    metricKind,
    funnelMetricType,
    displayFormat,
    changeUnit,
    definitionStatus,
    sortOrder,

    currentValue AS metricValue,
    wowChange,
    fourWeekAvgValue,
    vsFourWeekAvgChange,
    sameWeekLastYearValue,
    yoyChange,

    thisWeekDataAvailable,
    goldProcessedAt
FROM prdrzranalytics.lab42.sdi_vw_mip_serving_overviewFunnel_long;
