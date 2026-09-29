
-- ###########################################################################
-- BEGIN gold/05_sdi_tbl_mip_gold_overviewMetricForecastByWeek_long.sql
-- ###########################################################################

-- ============================================================================
-- FILE  : 05_sdi_tbl_mip_gold_overviewMetricForecastByWeek_long.sql
-- LAYER : GOLD
-- PURPOSE:
--   Forecast table definition; populated by a separate forecasting workflow.
-- ============================================================================

USE CATALOG prdrzranalytics;
USE SCHEMA lab42;
-- GOLD PRINCIPLE
-- Persist comparison INGREDIENTS, not precomputed percentages.
-- WoW / 4-week / LY comparisons join the appropriate weekly aggregates at the same
-- dimensional grain; do not cross-join raw visitors across weeks.
--
-- ----------------------------------------------------------------------------
-- FORECAST
-- Written by a separate forecasting process later.
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS sdi_tbl_mip_gold_overviewMetricForecastByWeek_long (
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
  forecastRunId      STRING,
  forecastCreatedAt  TIMESTAMP
)
USING DELTA
CLUSTER BY (weekStartDate, metricName)
COMMENT 'Gold Overview forecast inputs. Populated by the forecasting workflow, not the actuals pipeline.';


-- ###########################################################################
-- END gold/05_sdi_tbl_mip_gold_overviewMetricForecastByWeek_long.sql
-- ###########################################################################
