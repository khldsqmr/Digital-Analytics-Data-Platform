-- ============================================================================
-- FILE  : 05_sdi_sp_mip_gold_overviewMetricForecastByWeek_long.sql
-- LAYER : GOLD
--
-- PURPOSE:
--   Callable DDL-only owner for the weekly metric forecast target.
--   The actuals pipeline does NOT populate forecast rows.
--
-- RUNTIME CONTRACT:
--   - This procedure only validates control metadata and ensures the target exists.
--   - It does NOT INSERT/MERGE forecast data.
--   - Forecast rows remain owned by the separate forecasting workflow.
--   - forecastRunId is forecasting-model lineage, not MIP orchestration lineage.
--
-- CALL COMPATIBILITY:
--   - Manual SQL: CALL with p_validateOnly TRUE/FALSE.
--   - Notebook: CALL can render the BOOLEAN literal directly.
-- ============================================================================
CREATE OR REPLACE PROCEDURE prdrzranalytics.lab42.sdi_sp_mip_gold_overviewMetricForecastByWeek_long(
    IN p_validateOnly BOOLEAN DEFAULT FALSE
)
LANGUAGE SQL
SQL SECURITY INVOKER
MODIFIES SQL DATA
COMMENT 'Gold Overview forecast target DDL owner. Forecast rows are populated only by the separate forecasting workflow.'
AS
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM prdrzranalytics.lab42.sdi_vw_mip_control_metricCatalog_static
        WHERE hasForecast
          AND isActive
        LIMIT 1
    ) THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'Metric catalog has no active forecast-enabled metrics.';
    END IF;

    IF p_validateOnly THEN
        SELECT
            'VALIDATION_ONLY' AS status,
            'prdrzranalytics.lab42.sdi_vw_mip_control_metricCatalog_static' AS controlObject,
            'No Gold forecast table was created or modified.' AS message;
    ELSE
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

        SELECT
            'SUCCESS' AS status,
            'prdrzranalytics.lab42.sdi_tbl_mip_gold_overviewMetricForecastByWeek_long' AS targetObject,
            'DDL ensured. Forecast rows were not populated by MIP.' AS message;
    END IF;
END;

-- ============================================================================
-- MANUAL CALL EXAMPLES
-- ============================================================================
-- Preflight:
-- CALL prdrzranalytics.lab42.sdi_sp_mip_gold_overviewMetricForecastByWeek_long(
--     p_validateOnly => TRUE
-- );

-- Ensure target exists:
-- CALL prdrzranalytics.lab42.sdi_sp_mip_gold_overviewMetricForecastByWeek_long(
--     p_validateOnly => FALSE
-- );

-- ============================================================================
-- NOTEBOOK CALL EXAMPLE
-- ============================================================================
-- call_sql = """
--     CALL prdrzranalytics.lab42.sdi_sp_mip_gold_overviewMetricForecastByWeek_long(
--         p_validateOnly => FALSE
--     )
-- """
-- result = spark.sql(call_sql).collect()

-- ============================================================================
-- VALIDATION EXAMPLES
-- ============================================================================
-- A. FORECAST GRAIN
-- Expected: no duplicate rows for the same model run/grain.
-- SELECT
--     weekStartDate,filterLob,filterPlatform,metricName,modelName,modelVersion,forecastRunId,
--     COUNT(*) AS rowCount
-- FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_overviewMetricForecastByWeek_long
-- GROUP BY weekStartDate,filterLob,filterPlatform,metricName,modelName,modelVersion,forecastRunId
-- HAVING COUNT(*) > 1
-- ORDER BY rowCount DESC;

-- B. METRIC CATALOG MATCH
-- Expected: invalidMetricRows = 0.
-- SELECT COUNT(*) AS invalidMetricRows
-- FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_overviewMetricForecastByWeek_long f
-- LEFT JOIN prdrzranalytics.lab42.sdi_vw_mip_control_metricCatalog_static m
--   ON m.metricName=f.metricName
-- WHERE m.metricName IS NULL;

-- C. INTERVAL SANITY
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
