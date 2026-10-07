# Databricks notebook source
# ============================================================================
# FILE   : 01c_sdi_nb_mip_bronze_edlUdiHitsValidator_daily.py new
# NAME   : sdi_nb_mip_bronze_edlUdiHitsValidator_daily
# OBJECT : B01
# LAYER  : BRONZE
#
# PURPOSE:
#   Lightweight target-side validation for one exact B01 objectRunId.
#
# VISIBLE INPUT:
#   objectRunId : optional when executed as the downstream Lakeflow task;
#                 required for a manual interactive run.
#
# LAKEFLOW:
#   Preferred: pass {{tasks.bronze_udi_runner.values.objectRunId}} into this
#   notebook's objectRunId task parameter.
#   Fallback: if the widget is blank, this notebook also attempts to read the
#   task value directly from task key "bronze_udi_runner".
#
# PERFORMANCE:
#   ONE date-filtered scan of the Bronze target only.
#   No UDI source rescan, duplicate-key grouping, or metric reconciliation.
#
# CHECKS:
#   dateCoverage
#   targetPopulation
#   ingestionMarker
# ============================================================================

# COMMAND ----------
dbutils.widgets.text("objectRunId", "")
object_run_id = dbutils.widgets.get("objectRunId").strip()

# Fallback for a simple Runner -> Validator Lakeflow chain.
if not object_run_id:
    try:
        object_run_id = dbutils.jobs.taskValues.get(
            taskKey="bronze_udi_runner",
            key="objectRunId",
            debugValue="",
        )
    except Exception:
        object_run_id = ""

if not object_run_id:
    raise ValueError(
        "objectRunId is required for an interactive run. "
        "For Lakeflow, pass the Runner task value into the objectRunId parameter."
    )

OBJECT_RUNS = "prdrzranalytics.lab42.sdi_tbl_mip_validation_objectRuns_perRun"
CHECK_HISTORY = "prdrzranalytics.lab42.sdi_tbl_mip_validation_checkHistory_perRun"
TARGET = "prdrzranalytics.lab42.sdi_tbl_mip_bronze_edlUdiHits_daily"

# COMMAND ----------
# Resolve the authoritative run/scope from the execution record.
object_rows = spark.sql(
    f"""
    SELECT
        runId,
        CAST(scopeStart AS DATE) AS scopeStart,
        CAST(scopeEnd AS DATE) AS scopeEnd,
        objectRunStatus
    FROM {OBJECT_RUNS}
    WHERE objectRunId = :objectRunId
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
window_start = object_row["scopeStart"]
window_end = object_row["scopeEnd"]
object_status = object_row["objectRunStatus"]

if object_status != "SUCCEEDED":
    raise RuntimeError(
        f"B01 Validator will not run because objectRunStatus={object_status}."
    )

expected_date_count = (window_end - window_start).days + 1
as_of_date = window_end

# Idempotency: replace only this object's post-load checks.
spark.sql(
    f"""
    DELETE FROM {CHECK_HISTORY}
    WHERE objectRunId = :objectRunId
      AND validationPhase = 'POST'
      AND stageName = 'BRONZE'
      AND objectName = :objectName
    """,
    args={
        "objectRunId": object_run_id,
        "objectName": TARGET,
    },
)

# COMMAND ----------
# ONE narrow target-only scan.
daily_stats = spark.sql(
    f"""
    SELECT
        event_date,
        COUNT(*) AS rowCount,
        MAX(_ingestedAt) AS latestIngestedAt
    FROM {TARGET}
    WHERE event_date BETWEEN :windowStart AND :windowEnd
    GROUP BY event_date
    ORDER BY event_date
    """,
    args={
        "windowStart": window_start,
        "windowEnd": window_end,
    },
).collect()

row_count = sum(int(r["rowCount"] or 0) for r in daily_stats)
loaded_date_count = len(daily_stats)
latest_ingested_at = max(
    (
        r["latestIngestedAt"]
        for r in daily_stats
        if r["latestIngestedAt"] is not None
    ),
    default=None,
)

checks = [
    {
        "checkName": "dateCoverage",
        "checkType": "COVERAGE",
        "issueType": "DATA_AVAILABILITY",
        "expected": float(expected_date_count),
        "actual": float(loaded_date_count),
        "status": "HEALTHY" if loaded_date_count == expected_date_count else "FAILED",
        "severity": "INFO" if loaded_date_count == expected_date_count else "CRITICAL",
        "gateAction": "PROCEED" if loaded_date_count == expected_date_count else "STOP",
        "isBlocking": loaded_date_count != expected_date_count,
        "description": (
            "Every requested Bronze event date is present."
            if loaded_date_count == expected_date_count
            else "One or more requested Bronze event dates are missing."
        ),
        "likelyCause": (
            None
            if loaded_date_count == expected_date_count
            else "The requested date scope was not fully written."
        ),
        "nextSteps": (
            None
            if loaded_date_count == expected_date_count
            else "Review the B01 execution attempt and rerun the missing scope."
        ),
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
        "description": (
            f"Bronze target contains {row_count:,} rows in the requested scope."
            if row_count > 0
            else "Bronze target is empty in the requested scope."
        ),
        "likelyCause": (
            None if row_count > 0 else "The Bronze write did not produce rows."
        ),
        "nextSteps": (
            None
            if row_count > 0
            else "Review the B01 procedure output and requested source dates."
        ),
    },
    {
        "checkName": "ingestionMarker",
        "checkType": "FRESHNESS",
        "issueType": "FRESHNESS",
        "expected": 1.0,
        "actual": 1.0 if latest_ingested_at is not None else 0.0,
        "status": "HEALTHY" if latest_ingested_at is not None else "WARNING",
        "severity": "INFO" if latest_ingested_at is not None else "LOW",
        "gateAction": "PROCEED",
        "isBlocking": False,
        "description": (
            f"Latest Bronze _ingestedAt is {latest_ingested_at}."
            if latest_ingested_at is not None
            else "No _ingestedAt value was found in the requested Bronze scope."
        ),
        "likelyCause": (
            None
            if latest_ingested_at is not None
            else "Target ingestion marker is unexpectedly null."
        ),
        "nextSteps": (
            None
            if latest_ingested_at is not None
            else "Inspect the Bronze target if this warning persists."
        ),
    },
]

# COMMAND ----------
# Persist the three lightweight checks.
insert_sql = f"""
INSERT INTO {CHECK_HISTORY} (
    validationId,
    runId,
    objectRunId,
    checkedAt,
    asOfDate,
    validationPhase,
    stageName,
    layerName,
    objectName,
    scopeType,
    scopeStart,
    scopeEnd,
    targetWeekStartDate,
    metricName,
    comparisonType,
    breakoutType,
    breakoutValue,
    pairKey,
    displaySize,
    checkName,
    checkType,
    issueType,
    expectedValue,
    actualValue,
    varianceValue,
    variancePct,
    checkStatus,
    severity,
    gateAction,
    isBlocking,
    sourceLatestProcessedAt,
    targetLatestProcessedAt,
    issueShortDescription,
    likelyCause,
    nextSteps,
    ownerTeam,
    errorSqlState,
    errorCondition,
    errorLine,
    errorMessage
)
VALUES (
    :validationId,
    :runId,
    :objectRunId,
    current_timestamp(),
    :asOfDate,
    'POST',
    'BRONZE',
    'BRONZE',
    :objectName,
    'eventDate',
    :scopeStart,
    :scopeEnd,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    :checkName,
    :checkType,
    :issueType,
    :expectedValue,
    :actualValue,
    :varianceValue,
    NULL,
    :checkStatus,
    :severity,
    :gateAction,
    :isBlocking,
    NULL,
    :targetLatestProcessedAt,
    :issueShortDescription,
    :likelyCause,
    :nextSteps,
    'MIP Data Engineering',
    NULL,
    NULL,
    NULL,
    NULL
)
"""

for check in checks:
    variance = check["actual"] - check["expected"]

    spark.sql(
        insert_sql,
        args={
            "validationId": f"VAL_{object_run_id}_{check['checkName']}",
            "runId": run_id,
            "objectRunId": object_run_id,
            "asOfDate": as_of_date,
            "objectName": TARGET,
            "scopeStart": window_start,
            "scopeEnd": window_end,
            "checkName": check["checkName"],
            "checkType": check["checkType"],
            "issueType": check["issueType"],
            "expectedValue": check["expected"],
            "actualValue": check["actual"],
            "varianceValue": variance,
            "checkStatus": check["status"],
            "severity": check["severity"],
            "gateAction": check["gateAction"],
            "isBlocking": check["isBlocking"],
            "targetLatestProcessedAt": latest_ingested_at,
            "issueShortDescription": check["description"],
            "likelyCause": check["likelyCause"],
            "nextSteps": check["nextSteps"],
        },
    )

# Reuse the already-computed count; no extra target scan.
spark.sql(
    f"""
    UPDATE {OBJECT_RUNS}
    SET
        rowsInScope = :rowCount,
        notes = concat(
            coalesce(notes, ''),
            :validatorNote
        )
    WHERE objectRunId = :objectRunId
    """,
    args={
        "rowCount": row_count,
        "validatorNote": (
            f"; validator=LIGHT; loadedDateCount={loaded_date_count}; "
            f"rowCount={row_count}"
        ),
        "objectRunId": object_run_id,
    },
)

blocking_failures = [
    check
    for check in checks
    if check["isBlocking"] and check["status"] in ("FAILED", "ERROR")
]
warnings = [check for check in checks if check["status"] == "WARNING"]

validation_status = (
    "FAILED"
    if blocking_failures
    else "WARNING"
    if warnings
    else "HEALTHY"
)

dbutils.jobs.taskValues.set(key="validationStatus", value=validation_status)
dbutils.jobs.taskValues.set(
    key="blockingFailureCount",
    value=len(blocking_failures),
)
dbutils.jobs.taskValues.set(
    key="warningCount",
    value=len(warnings),
)

print("=" * 96)
print("MIP B01 | LIGHTWEIGHT VALIDATION")
print(f"runId       : {run_id}")
print(f"objectRunId : {object_run_id}")
print(f"scope       : {window_start} -> {window_end}")
print(f"rowCount    : {row_count:,}")
print("=" * 96)

for check in checks:
    print(
        f"{check['status']:8} | {check['checkName']} | "
        f"expected={check['expected']} | actual={check['actual']}"
    )

if blocking_failures:
    raise RuntimeError(
        "B01_VALIDATION_STOP: "
        + ", ".join(check["checkName"] for check in blocking_failures)
    )

print(f"B01 lightweight validation completed with status={validation_status}.")
