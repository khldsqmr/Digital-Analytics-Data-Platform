# Databricks notebook source
# ============================================================================
# FILE   : 01c_sdi_nb_mip_silver_detailsPerHitValidator_daily.py
# NAME   : sdi_nb_mip_silver_detailsPerHitValidator_daily
# OBJECT : S01
# LAYER  : SILVER
# PURPOSE:
#   Lightweight target-only validation for one exact S01 objectRunId.
#
# VISIBLE INPUT:
#   objectRunId
#
# PERFORMANCE:
#   One scoped target scan only. No upstream source rescan or expensive
#   reconciliation is performed in the normal validator.
# ============================================================================

# COMMAND ----------
dbutils.widgets.text("objectRunId", "")
object_run_id = dbutils.widgets.get("objectRunId").strip()

if not object_run_id:
    try:
        object_run_id = dbutils.jobs.taskValues.get(
            taskKey="silver_s01_runner",
            key="objectRunId",
            debugValue="",
        )
    except Exception:
        object_run_id = ""

if not object_run_id:
    raise ValueError(
        "objectRunId is required for an interactive run. "
        "For Lakeflow, pass the upstream Runner task value into this parameter."
    )

OBJECT_RUNS = "prdrzranalytics.lab42.sdi_tbl_mip_validation_objectRuns_perRun"
RUN_DETAILS = "prdrzranalytics.lab42.sdi_tbl_mip_validation_runDetails_perRun"
CHECK_HISTORY = "prdrzranalytics.lab42.sdi_tbl_mip_validation_checkHistory_perRun"
TARGET = "prdrzranalytics.lab42.sdi_tbl_mip_silver_detailsPerHit_daily"
SCOPE_FIELD = "eventDate"
SCOPE_TYPE = "eventDate"
STAGE_NAME = "SILVER_HIT"

# COMMAND ----------
object_rows = spark.sql(
    f"""
    SELECT
        o.runId,
        CAST(o.scopeStart AS DATE) AS scopeStart,
        CAST(o.scopeEnd AS DATE) AS scopeEnd,
        o.objectRunStatus,
        r.asOfDate
    FROM {OBJECT_RUNS} o
    LEFT JOIN {RUN_DETAILS} r
      ON r.runId = o.runId
    WHERE o.objectRunId = :objectRunId
    """,
    args={"objectRunId": object_run_id},
).collect()

if len(object_rows) != 1:
    raise RuntimeError(
        f"Expected exactly one objectRuns_perRun row for {object_run_id}; "
        f"found {len(object_rows)}."
    )

object_row = object_rows[0]
run_id = object_row["runId"]
scope_start = object_row["scopeStart"]
scope_end = object_row["scopeEnd"]
as_of_date = object_row["asOfDate"] or scope_end
object_status = object_row["objectRunStatus"]

if object_status != "SUCCEEDED":
    raise RuntimeError(
        f"S01 Validator will not run because objectRunStatus={object_status}."
    )

expected_scope_count = (scope_end - scope_start).days + 1

# Replace only this object's validation records if the notebook is rerun.
spark.sql(
    f"""
    DELETE FROM {CHECK_HISTORY}
    WHERE objectRunId = :objectRunId
      AND validationPhase = 'POST'
      AND stageName = :stageName
      AND objectName = :objectName
    """,
    args={
        "objectRunId": object_run_id,
        "stageName": STAGE_NAME,
        "objectName": TARGET,
    },
)

# COMMAND ----------
# One scoped target scan produces all normal validation ingredients.
stats = spark.sql(
    f"""
    SELECT
        COUNT(*) AS rowCount,
        COUNT(DISTINCT {SCOPE_FIELD}) AS loadedScopeCount,
        MAX(silverProcessedAt) AS latestProcessedAt,
        COUNT_IF(sessionId IS NULL) AS invalidContractRows
    FROM {TARGET}
    WHERE {SCOPE_FIELD} BETWEEN :scopeStart AND :scopeEnd
    """,
    args={
        "scopeStart": scope_start,
        "scopeEnd": scope_end,
    },
).collect()[0]

row_count = int(stats["rowCount"] or 0)
loaded_scope_count = int(stats["loadedScopeCount"] or 0)
latest_processed_at = stats["latestProcessedAt"]
invalid_contract_rows = int(stats["invalidContractRows"] or 0)

checks = [
    {
        "checkName": "dateCoverage",
        "checkType": "COVERAGE",
        "issueType": "DATA_AVAILABILITY",
        "expected": float(expected_scope_count),
        "actual": float(loaded_scope_count),
        "status": "HEALTHY" if loaded_scope_count == expected_scope_count else "FAILED",
        "severity": "INFO" if loaded_scope_count == expected_scope_count else "CRITICAL",
        "gateAction": "PROCEED" if loaded_scope_count == expected_scope_count else "STOP",
        "isBlocking": loaded_scope_count != expected_scope_count,
        "description": (
            "Every requested Silver scope value is present."
            if loaded_scope_count == expected_scope_count
            else "One or more requested Silver scope values are missing."
        ),
        "likelyCause": None if loaded_scope_count == expected_scope_count else "The scoped Silver rebuild is incomplete.",
        "nextSteps": None if loaded_scope_count == expected_scope_count else "Review the object execution and rerun the missing scope.",
    },
    {
        "checkName": "targetPopulation",
        "checkType": "VOLUME",
        "issueType": "POPULATION",
        "expected": 1.0,
        "actual": 1.0 if row_count > 0 else 0.0,
        "status": "HEALTHY" if row_count > 0 else "FAILED",
        "severity": "INFO" if row_count > 0 else "CRITICAL",
        "gateAction": "PROCEED" if row_count > 0 else "STOP",
        "isBlocking": row_count == 0,
        "description": f"Target contains {row_count:,} rows in the requested scope.",
        "likelyCause": None if row_count > 0 else "The Silver write produced no rows.",
        "nextSteps": None if row_count > 0 else "Review the transformation inputs and scope.",
    },
    {
        "checkName": "processedMarker",
        "checkType": "FRESHNESS",
        "issueType": "FRESHNESS",
        "expected": 1.0,
        "actual": 1.0 if latest_processed_at is not None else 0.0,
        "status": "HEALTHY" if latest_processed_at is not None else "WARNING",
        "severity": "INFO" if latest_processed_at is not None else "LOW",
        "gateAction": "PROCEED",
        "isBlocking": False,
        "description": (
            f"Latest silverProcessedAt is {latest_processed_at}."
            if latest_processed_at is not None
            else "No silverProcessedAt value was found."
        ),
        "likelyCause": None if latest_processed_at is not None else "The processing marker is unexpectedly null.",
        "nextSteps": None if latest_processed_at is not None else "Inspect the target if the warning persists.",
    },
    {
        "checkName": "sessionizationInvariant",
        "checkType": "DATA_QUALITY",
        "issueType": "DATA_QUALITY",
        "expected": 0.0,
        "actual": float(invalid_contract_rows),
        "status": "HEALTHY" if invalid_contract_rows == 0 else "FAILED",
        "severity": "INFO" if invalid_contract_rows == 0 else "HIGH",
        "gateAction": "PROCEED" if invalid_contract_rows == 0 else "STOP",
        "isBlocking": invalid_contract_rows > 0,
        "description": "Rows with NULL sessionId.",
        "likelyCause": None if invalid_contract_rows == 0 else "The target violates a core object-level contract.",
        "nextSteps": None if invalid_contract_rows == 0 else "Inspect the affected Silver scope before continuing downstream.",
    },
]

# COMMAND ----------
insert_sql = f"""
INSERT INTO {CHECK_HISTORY} (
    validationId, runId, objectRunId, checkedAt, asOfDate,
    validationPhase, stageName, layerName, objectName,
    scopeType, scopeStart, scopeEnd, targetWeekStartDate,
    metricName, comparisonType, breakoutType, breakoutValue, pairKey, displaySize,
    checkName, checkType, issueType,
    expectedValue, actualValue, varianceValue, variancePct,
    checkStatus, severity, gateAction, isBlocking,
    sourceLatestProcessedAt, targetLatestProcessedAt,
    issueShortDescription, likelyCause, nextSteps, ownerTeam,
    errorSqlState, errorCondition, errorLine, errorMessage
)
VALUES (
    :validationId, :runId, :objectRunId, current_timestamp(), :asOfDate,
    'POST', :stageName, 'SILVER', :objectName,
    :scopeType, :scopeStart, :scopeEnd, :targetWeekStartDate,
    NULL, NULL, NULL, NULL, NULL, NULL,
    :checkName, :checkType, :issueType,
    :expectedValue, :actualValue, :varianceValue, NULL,
    :checkStatus, :severity, :gateAction, :isBlocking,
    NULL, :targetLatestProcessedAt,
    :issueShortDescription, :likelyCause, :nextSteps, 'MIP Data Engineering',
    NULL, NULL, NULL, NULL
)
"""

for check in checks:
    spark.sql(
        insert_sql,
        args={
            "validationId": f"VAL_{object_run_id}_{check['checkName']}",
            "runId": run_id,
            "objectRunId": object_run_id,
            "asOfDate": as_of_date,
            "stageName": STAGE_NAME,
            "objectName": TARGET,
            "scopeType": SCOPE_TYPE,
            "scopeStart": scope_start,
            "scopeEnd": scope_end,
            "targetWeekStartDate": scope_end if SCOPE_TYPE == "weekStartDate" else None,
            "checkName": check["checkName"],
            "checkType": check["checkType"],
            "issueType": check["issueType"],
            "expectedValue": check["expected"],
            "actualValue": check["actual"],
            "varianceValue": check["actual"] - check["expected"],
            "checkStatus": check["status"],
            "severity": check["severity"],
            "gateAction": check["gateAction"],
            "isBlocking": check["isBlocking"],
            "targetLatestProcessedAt": latest_processed_at,
            "issueShortDescription": check["description"],
            "likelyCause": check["likelyCause"],
            "nextSteps": check["nextSteps"],
        },
    )

spark.sql(
    f"""
    UPDATE {OBJECT_RUNS}
    SET
        rowsInScope = :rowCount,
        notes = concat(
            coalesce(notes,''),
            :validatorNote
        )
    WHERE objectRunId = :objectRunId
    """,
    args={
        "rowCount": row_count,
        "validatorNote": (
            f"; validator=LIGHT; loadedScopeCount={loaded_scope_count}; "
            f"rowCount={row_count}; invalidContractRows={invalid_contract_rows}"
        ),
        "objectRunId": object_run_id,
    },
)

blocking_failures = [
    c for c in checks
    if c["isBlocking"] and c["status"] in ("FAILED", "ERROR")
]
warnings = [c for c in checks if c["status"] == "WARNING"]

validation_status = (
    "FAILED" if blocking_failures
    else "WARNING" if warnings
    else "HEALTHY"
)

dbutils.jobs.taskValues.set(key="validationStatus", value=validation_status)
dbutils.jobs.taskValues.set(key="blockingFailureCount", value=len(blocking_failures))
dbutils.jobs.taskValues.set(key="warningCount", value=len(warnings))

print("=" * 96)
print("MIP S01 | LIGHTWEIGHT SILVER VALIDATION")
print(f"runId       : {run_id}")
print(f"objectRunId : {object_run_id}")
print(f"scope       : {scope_start} -> {scope_end}")
print(f"rowCount    : {row_count:,}")
print("=" * 96)

for check in checks:
    print(
        f"{check['status']:8} | {check['checkName']} | "
        f"expected={check['expected']} | actual={check['actual']}"
    )

if blocking_failures:
    raise RuntimeError(
        "S01_VALIDATION_STOP: "
        + ", ".join(c["checkName"] for c in blocking_failures)
    )

print(f"S01 lightweight validation completed with status={validation_status}.")
