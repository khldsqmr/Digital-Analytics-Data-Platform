-- ============================================================================
-- FILE  : 05_sdi_tbl_mip_gold_overviewMetricForecastByWeek_long.sql
-- LAYER : GOLD
-- PURPOSE:
--   Weekly metric forecast target populated by a separate forecasting workflow.
--
-- NAMING:
--   ByWeek = temporal grain.
--   long   = one row per week × filter context × metric.
--
-- DESIGN:
--   - This file is intentionally DDL-only for now.
--   - It contains one top-level SQL statement.
--   - The actuals pipeline does NOT populate this table.
--   - forecastRunId is forecasting-model lineage and is intentionally retained;
--     it is separate from the orchestration/job run ID we will add later.
-- ============================================================================

CREATE TABLE IF NOT EXISTS prdrzranalytics.lab42.sdi_tbl_mip_gold_overviewMetricForecastByWeek_long (
    weekStartDate      DATE,
    weekEndDate        DATE,

    filterLob          STRING COMMENT 'All or future whole-report LOB filter context',
    filterPlatform     STRING COMMENT 'All or future whole-report Platform filter context',

    metricName         STRING,

    forecastValue      DOUBLE,
    forecastLow        DOUBLE,
    forecastHigh       DOUBLE,

    modelName          STRING,
    modelVersion       STRING,

    forecastRunId      STRING COMMENT 'Forecasting-workflow/model run identifier; not MIP orchestration run ID',
    forecastCreatedAt  TIMESTAMP
)
USING DELTA
CLUSTER BY (weekStartDate, metricName)
COMMENT 'Gold Overview forecast inputs by week and metric. Populated by the separate forecasting workflow, not the actuals pipeline.';
