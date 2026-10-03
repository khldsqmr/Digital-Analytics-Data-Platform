-- ============================================================================
-- FILE  : 01_sdi_tbl_mip_validation_runDetails_perRun.sql
-- LAYER : VALIDATION
-- PURPOSE:
--   Creates prdrzranalytics.lab42.sdi_tbl_mip_validation_runDetails_perRun.
-- ============================================================================

CREATE TABLE IF NOT EXISTS prdrzranalytics.lab42.sdi_tbl_mip_validation_runDetails_perRun (
    runId                    STRING,
    executionType            STRING COMMENT 'MAN | JOB | BCK | RPR | RTY | TST',
    triggerType              STRING COMMENT 'MANUAL | SCHEDULED | API | UPSTREAM | RETRY | BACKFILL',
    databricksJobId          STRING,
    databricksJobRunId       STRING,
    databricksTaskRunId      STRING,
    databricksJobName        STRING,
    notebookPath             STRING,
    orchestrationProcedure   STRING,
    asOfDate                 DATE,
    eventWindowStart         DATE,
    eventWindowEnd           DATE,
    weekWindowStart          DATE,
    weekWindowEnd            DATE,
    runStartedAt             TIMESTAMP,
    runFinishedAt            TIMESTAMP,
    runStatus                STRING COMMENT 'RUNNING | HEALTHY | WARNING | FAILED | ERROR',
    warningCount             BIGINT,
    failureCount             BIGINT,
    errorSqlState            STRING,
    errorCondition           STRING,
    errorLine                BIGINT,
    errorMessage             STRING,
    createdAt                TIMESTAMP,
    updatedAt                TIMESTAMP
)
USING DELTA
CLUSTER BY (runStartedAt)
COMMENT 'MIP validation: one row per end-to-end orchestration run; Job/Run/Task metadata is nullable until Databricks Jobs is wired.';
