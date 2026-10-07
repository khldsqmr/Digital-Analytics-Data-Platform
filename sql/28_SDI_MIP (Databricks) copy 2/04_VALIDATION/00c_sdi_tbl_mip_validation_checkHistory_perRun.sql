-- ============================================================================
-- FILE  : 00c_sdi_tbl_mip_validation_checkHistory_perRun.sql
-- LAYER : VALIDATION
-- PURPOSE:
--   Creates canonical validation check history across MIP.
--
-- GRAIN:
--   One row per validation check per object execution attempt.
--
-- OBJECT LINK:
--   objectRunId links a check to the exact execution attempt in
--   sdi_tbl_mip_validation_objectRuns_perRun.
--
-- PERFORMANCE POLICY:
--   The table can store both lightweight daily checks and deeper diagnostics.
--   Bronze01 currently writes LIGHT checks only; expensive source rescans and
--   duplicate-grain scans are intentionally excluded from normal validation.
-- ============================================================================

CREATE TABLE IF NOT EXISTS prdrzranalytics.lab42.sdi_tbl_mip_validation_checkHistory_perRun (
    validationId             STRING,
    runId                    STRING,
    objectRunId              STRING COMMENT 'Exact execution attempt from sdi_tbl_mip_validation_objectRuns_perRun',

    checkedAt                TIMESTAMP,
    asOfDate                 DATE,
    validationPhase          STRING COMMENT 'PRE | POST | EXECUTION',

    stageName                STRING COMMENT 'BRONZE | SILVER_HIT | SILVER_SESSION | SILVER_WEEK | GOLD_OVERVIEW | GOLD_BREAKOUT | GOLD_CROSSTAB | GOLD_EXPLORE | GOLD_APP',
    layerName                STRING COMMENT 'CONTROL | BRONZE | SILVER | GOLD | GOLD_APP',
    objectName               STRING,

    scopeType                STRING,
    scopeStart               DATE,
    scopeEnd                 DATE,
    targetWeekStartDate      DATE,

    metricName               STRING,
    comparisonType           STRING,
    breakoutType             STRING,
    breakoutValue            STRING,
    pairKey                  STRING,
    displaySize              STRING,

    checkName                STRING,
    checkType                STRING COMMENT 'AVAILABILITY | VOLUME | FRESHNESS | COVERAGE | UNIQUENESS | RECONCILIATION | DATA_QUALITY | EXECUTION',
    issueType                STRING COMMENT 'DATA_AVAILABILITY | COMPARATOR_AVAILABILITY | FRESHNESS | POPULATION | DUPLICATE_KEY | RECONCILIATION | DATA_QUALITY | CONFIGURATION | EXECUTION_ERROR',

    expectedValue            DOUBLE,
    actualValue              DOUBLE,
    varianceValue            DOUBLE,
    variancePct              DOUBLE,

    checkStatus              STRING COMMENT 'HEALTHY | INFO | WARNING | FAILED | ERROR',
    severity                 STRING COMMENT 'INFO | LOW | MEDIUM | HIGH | CRITICAL',
    gateAction               STRING COMMENT 'PROCEED | STOP',
    isBlocking               BOOLEAN,

    sourceLatestProcessedAt  TIMESTAMP,
    targetLatestProcessedAt  TIMESTAMP,

    issueShortDescription    STRING,
    likelyCause              STRING,
    nextSteps                STRING,
    ownerTeam                STRING,

    errorSqlState            STRING,
    errorCondition           STRING,
    errorLine                BIGINT,
    errorMessage             STRING
)
USING DELTA
CLUSTER BY (checkedAt, runId, stageName)
COMMENT 'MIP validation: canonical check history linked to exact object execution attempts.';
