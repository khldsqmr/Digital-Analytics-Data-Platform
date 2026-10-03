-- ============================================================================
-- FILE  : 03_sdi_tbl_mip_validation_checkHistory_perRun.sql
-- LAYER : VALIDATION
-- PURPOSE:
--   Creates prdrzranalytics.lab42.sdi_tbl_mip_validation_checkHistory_perRun.
-- ============================================================================

CREATE TABLE IF NOT EXISTS prdrzranalytics.lab42.sdi_tbl_mip_validation_checkHistory_perRun (
    validationId             STRING,
    runId                    STRING,
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
COMMENT 'MIP validation: canonical PRE/POST/execution check history across Bronze, Silver, analytical Gold and Gold App.';
