-- ============================================================================
-- FILE  : 05_sdi_tbl_mip_gold_overviewMetricForecastByWeek_long.sql
-- LAYER : GOLD
-- PURPOSE:
--   Weekly metric forecast target populated by a separate forecasting workflow.
--
-- NAMING:
--   ByWeek = temporal grain.
--   long   = one row per week x filter context x metric.
--
-- DESIGN:
--   - DDL-only; the actuals pipeline does NOT populate this table.
--   - forecastRunId is forecasting-model lineage, not MIP orchestration lineage.
--   - metricName must use the canonical control-catalog key; NBV = 'nbv'.
--   - Total NBV is a display label from the metric catalog, not a metricName.
-- ============================================================================
CREATE TABLE IF NOT EXISTS prdrzranalytics.lab42.sdi_tbl_mip_gold_overviewMetricForecastByWeek_long (
    weekStartDate      DATE,
    weekEndDate        DATE,

    filterLob          STRING COMMENT 'All or future whole-report LOB filter context',
    filterPlatform     STRING COMMENT 'All or future whole-report Platform filter context',

    metricName         STRING COMMENT 'Canonical metric key from sdi_vw_mip_control_metricCatalog_static; NBV key is nbv',

    forecastValue      DOUBLE,
    forecastLow        DOUBLE,
    forecastHigh       DOUBLE,

    modelName          STRING,
    modelVersion       STRING,

    forecastRunId      STRING COMMENT 'Forecasting-workflow/model run identifier; not MIP orchestration run ID',
    forecastCreatedAt  TIMESTAMP
)
USING DELTA
CLUSTER BY (weekStartDate,metricName)
COMMENT 'Gold Overview forecast inputs by week and metric. Populated by the separate forecasting workflow, not the actuals pipeline.';

-- ============================================================================
-- DEVELOPMENT / TEST EXAMPLES
-- ============================================================================

-- A. PREFLIGHT
-- No source-data preflight is required because this file is DDL-only.
-- Confirm the control metric catalog is available before the forecasting
-- workflow writes rows:
-- SELECT metricName,metricLabel,hasForecast,isActive
-- FROM prdrzranalytics.lab42.sdi_vw_mip_control_metricCatalog_static
-- WHERE hasForecast AND isActive
-- ORDER BY sortOrder;

-- B. EXECUTION
-- Execute the CREATE TABLE statement above once/declaratively.
-- The separate forecasting workflow subsequently INSERTs/MERGEs forecast rows.

-- C. VALIDATION 1: FORECAST GRAIN
-- Expected: no duplicate rows for the same model run/grain.
-- SELECT
--     weekStartDate,filterLob,filterPlatform,metricName,modelName,modelVersion,forecastRunId,
--     COUNT(*) AS rowCount
-- FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_overviewMetricForecastByWeek_long
-- GROUP BY weekStartDate,filterLob,filterPlatform,metricName,modelName,modelVersion,forecastRunId
-- HAVING COUNT(*) > 1
-- ORDER BY rowCount DESC;

-- D. VALIDATION 2: METRIC CATALOG MATCH
-- Expected: invalidMetricRows = 0.
-- SELECT COUNT(*) AS invalidMetricRows
-- FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_overviewMetricForecastByWeek_long f
-- LEFT JOIN prdrzranalytics.lab42.sdi_vw_mip_control_metricCatalog_static m
--   ON m.metricName=f.metricName
-- WHERE m.metricName IS NULL;

-- E. VALIDATION 3: INTERVAL SANITY
-- Expected: invalidIntervalRows = 0.
-- SELECT COUNT(*) AS invalidIntervalRows
-- FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_overviewMetricForecastByWeek_long
-- WHERE forecastLow IS NOT NULL
--   AND forecastHigh IS NOT NULL
--   AND (
--       forecastLow > forecastValue
--       OR forecastValue > forecastHigh
--       OR weekEndDate <> date_add(weekStartDate,6)
--   );
