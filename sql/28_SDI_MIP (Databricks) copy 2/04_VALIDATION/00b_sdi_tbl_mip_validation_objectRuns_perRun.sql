-- ============================================================================
-- FILE  : 00b_sdi_tbl_mip_validation_objectRuns_perRun.sql
-- LAYER : VALIDATION
-- PURPOSE:
--   Creates object-level execution history for MIP.
--
-- GRAIN:
--   One row per transformation/control execution attempt.
--
-- OBJECT-RUN ID CONVENTION:
--   <objectCode>_<UTC timestamp>_<8-char token>
--
--   Examples:
--     B01_20261007T061945Z_7F3A91C2
--     B02_20261007T062015Z_18D621A0
--     S01_20261007T071201Z_93BE772F
--
--   Stable object codes:
--     B01, B02, ... = Bronze
--     S01, S02, ... = Silver
--     G01, G02, ... = Gold
--
-- RETRY CONTRACT:
--   Every retry receives a NEW objectRunId while preserving the same runId.
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

    rowsInScope              BIGINT COMMENT 'Optional execution-level row metric; detailed validation values live in checkHistory',
    sourceWatermark          STRING,

    errorSqlState            STRING,
    errorCondition           STRING,
    errorLine                BIGINT,
    errorMessage             STRING,
    notes                    STRING
)
USING DELTA
CLUSTER BY (objectRunStartedAt)
COMMENT 'MIP validation: one row per transformation/control execution attempt inside an orchestration run.';
