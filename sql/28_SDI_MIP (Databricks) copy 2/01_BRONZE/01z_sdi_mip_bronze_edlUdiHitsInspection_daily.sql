-- ============================================================================
-- FILE  : 01z_sdi_mip_bronze_edlUdiHitsInspection_daily.sql
-- PURPOSE:
--   Inspect B01 execution attempts and their lightweight validation results.
-- ============================================================================

-- ----------------------------------------------------------------------------
-- 1. Latest B01 execution attempts.
-- ----------------------------------------------------------------------------
SELECT
    objectRunId,
    runId,
    layerName,
    procedureName,
    targetObject,
    scopeStart,
    scopeEnd,
    objectRunStartedAt,
    objectRunFinishedAt,
    objectRunStatus,
    rowsInScope,
    sourceWatermark,
    errorMessage,
    notes
FROM prdrzranalytics.lab42.sdi_tbl_mip_validation_objectRuns_perRun
WHERE objectRunId LIKE 'B01_%'
ORDER BY objectRunStartedAt DESC
LIMIT 100;

-- ----------------------------------------------------------------------------
-- 2. Validation checks for one exact B01 execution attempt.
-- Replace the objectRunId below.
-- ----------------------------------------------------------------------------
SELECT
    validationId,
    runId,
    objectRunId,
    checkedAt,
    asOfDate,
    validationPhase,
    stageName,
    objectName,
    scopeStart,
    scopeEnd,
    checkName,
    checkType,
    expectedValue,
    actualValue,
    varianceValue,
    checkStatus,
    severity,
    gateAction,
    isBlocking,
    targetLatestProcessedAt,
    issueShortDescription,
    likelyCause,
    nextSteps,
    errorMessage
FROM prdrzranalytics.lab42.sdi_tbl_mip_validation_checkHistory_perRun
WHERE objectRunId = 'B01_YYYYMMDDTHHMMSSZ_TOKEN'
ORDER BY checkedAt, checkName;

-- ----------------------------------------------------------------------------
-- 3. Recent B01 warnings/failures.
-- ----------------------------------------------------------------------------
SELECT
    runId,
    objectRunId,
    checkedAt,
    checkName,
    checkStatus,
    severity,
    gateAction,
    issueShortDescription
FROM prdrzranalytics.lab42.sdi_tbl_mip_validation_checkHistory_perRun
WHERE objectRunId LIKE 'B01_%'
  AND checkStatus IN ('WARNING','FAILED','ERROR')
ORDER BY checkedAt DESC;

-- ----------------------------------------------------------------------------
-- 4. Quick validation-table fill check.
-- ----------------------------------------------------------------------------
SELECT
    o.objectRunId,
    o.runId,
    o.objectRunStatus,
    o.rowsInScope,
    COUNT(h.validationId) AS validationCheckCount,
    SUM(CASE WHEN h.checkStatus='WARNING' THEN 1 ELSE 0 END) AS warningCount,
    SUM(CASE WHEN h.checkStatus IN ('FAILED','ERROR') THEN 1 ELSE 0 END) AS failureCount
FROM prdrzranalytics.lab42.sdi_tbl_mip_validation_objectRuns_perRun o
LEFT JOIN prdrzranalytics.lab42.sdi_tbl_mip_validation_checkHistory_perRun h
  ON h.objectRunId = o.objectRunId
WHERE o.objectRunId LIKE 'B01_%'
GROUP BY
    o.objectRunId,
    o.runId,
    o.objectRunStatus,
    o.rowsInScope
ORDER BY MAX(o.objectRunStartedAt) DESC
LIMIT 100;
