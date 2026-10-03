-- ============================================================================
-- FILE  : 02_sdi_tbl_mip_validation_objectRuns_perRun.sql
-- LAYER : VALIDATION
-- PURPOSE:
--   Creates prdrzranalytics.lab42.sdi_tbl_mip_validation_objectRuns_perRun.
-- ============================================================================

CREATE TABLE IF NOT EXISTS prdrzranalytics.lab42.sdi_tbl_mip_validation_objectRuns_perRun (
    objectRunId              STRING,
    runId                    STRING,
    layerName                STRING COMMENT 'CONTROL | BRONZE | SILVER | GOLD | GOLD_APP',
    procedureName            STRING,
    targetObject             STRING,
    databricksTaskRunId      STRING COMMENT 'Nullable until Databricks Jobs/task metadata is wired',
    scopeType                STRING COMMENT 'snapshot | eventDate | sessionStartDatePst | weekStartDate',
    scopeStart               STRING,
    scopeEnd                 STRING,
    objectRunStartedAt       TIMESTAMP,
    objectRunFinishedAt      TIMESTAMP,
    objectRunStatus          STRING COMMENT 'RUNNING | SUCCEEDED | FAILED | ERROR',
    rowsInScope              BIGINT COMMENT 'Optional execution-level row metric; detailed row counts live in checkHistory',
    sourceWatermark          STRING,
    errorSqlState            STRING,
    errorCondition           STRING,
    errorLine                BIGINT,
    errorMessage             STRING,
    notes                    STRING
)
USING DELTA
CLUSTER BY (objectRunStartedAt)
COMMENT 'MIP validation: one row per transformation/control stored-procedure execution inside an orchestration run.';
