-- ============================================================================
-- FILE  : 02_sdi_vw_mip_serving_overviewTrend_long.sql
-- LAYER : SERVING
-- TAB   : Overview
-- SECTION: Trend
-- PURPOSE:
--   Weekly Overview actual trend plus latest available forecast record.
--   Forecast-only future weeks are supported once Gold forecast data exists.
--
-- NAMING:
--   Object name expresses app tab/section + physical shape only.
--   Current reporting grain is week-based, but cadence/grain is intentionally
--   not encoded in the serving-view object name.
-- ============================================================================

CREATE OR REPLACE VIEW prdrzranalytics.lab42.sdi_vw_mip_serving_overviewTrend_long AS
WITH actual AS (
    SELECT *
    FROM prdrzranalytics.lab42.sdi_vw_mip_serving_overviewCards_long
),
forecastLatest AS (
    SELECT
        weekStartDate,
        weekEndDate,
        filterLob,
        filterPlatform,
        metricName,
        forecastValue,
        forecastLow,
        forecastHigh,
        modelName,
        modelVersion,
        forecastRunId,
        forecastCreatedAt
    FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_overviewMetricForecastByWeek_long
    QUALIFY row_number() OVER (
        PARTITION BY weekStartDate, filterLob, filterPlatform, metricName
        ORDER BY forecastCreatedAt DESC NULLS LAST, forecastRunId DESC NULLS LAST
    ) = 1
),
keys AS (
    SELECT
        targetWeekStartDate AS weekStartDate,
        filterLob,
        filterPlatform,
        metricName
    FROM actual

    UNION

    SELECT
        weekStartDate,
        filterLob,
        filterPlatform,
        metricName
    FROM forecastLatest
)
SELECT
    k.weekStartDate,
    coalesce(a.targetWeekEndDate, f.weekEndDate, c.weekEndDate) AS weekEndDate,
    coalesce(a.fiscalQuarterLabel, c.fiscalQuarterLabel) AS fiscalQuarterLabel,
    coalesce(a.fiscalWeekCode, c.fiscalWeekCode) AS fiscalWeekCode,
    coalesce(a.weekLabel, c.weekLabel) AS weekLabel,

    k.filterLob,
    k.filterPlatform,

    k.metricName,
    coalesce(a.metricLabel, m.metricLabel) AS metricLabel,
    coalesce(a.metricDescription, m.metricDescription) AS metricDescription,
    coalesce(a.metricKind, m.metricKind) AS metricKind,
    coalesce(a.displayFormat, m.displayFormat) AS displayFormat,
    coalesce(a.changeUnit, m.changeUnit) AS changeUnit,
    m.hasForecast,
    m.definitionStatus,
    m.sortOrder,

    a.currentValue AS actualValue,
    a.wowChange,
    a.fourWeekAvgValue,
    a.vsFourWeekAvgChange,
    a.sameWeekLastYearValue,
    a.yoyChange,

    f.forecastValue,
    f.forecastLow,
    f.forecastHigh,
    f.modelName,
    f.modelVersion,
    f.forecastRunId,
    f.forecastCreatedAt,

    CASE
        WHEN a.currentValue IS NOT NULL AND f.forecastValue IS NOT NULL THEN 'actual+forecast'
        WHEN a.currentValue IS NOT NULL THEN 'actual'
        WHEN f.forecastValue IS NOT NULL THEN 'forecast'
        ELSE 'none'
    END AS pointType,

    a.thisWeekDataAvailable,
    a.goldProcessedAt

FROM keys k

JOIN prdrzranalytics.lab42.sdi_vw_mip_control_metricCatalog_static m
  ON  m.metricName = k.metricName
  AND m.isActive
  AND m.showOnOverview

LEFT JOIN actual a
  ON  a.targetWeekStartDate = k.weekStartDate
  AND a.filterLob = k.filterLob
  AND a.filterPlatform = k.filterPlatform
  AND a.metricName = k.metricName

LEFT JOIN forecastLatest f
  ON  f.weekStartDate = k.weekStartDate
  AND f.filterLob = k.filterLob
  AND f.filterPlatform = k.filterPlatform
  AND f.metricName = k.metricName

LEFT JOIN prdrzranalytics.lab42.sdi_vw_mip_control_fiscalCalendar_static c
  ON c.weekStartDate = k.weekStartDate;
