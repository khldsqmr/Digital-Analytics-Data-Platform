# Databricks notebook source
# ============================================================================
# FILE   : 01c_sdi_nb_mip_bronze_edlUdiHitsValidator_daily.py
# NAME   : sdi_nb_mip_bronze_edlUdiHitsValidator_daily
# OBJECT : B01
# LAYER  : BRONZE
# PURPOSE:
#   Lightweight post-load validation for the exact B01 execution attempt.
#
# PERFORMANCE POLICY:
#   Normal B01 validation must remain cheap because the UDI source is very large.
#
#   This Validator therefore:
#     - DOES NOT rescan the source UDI table.
#     - DOES NOT group by row_identity_hash.
#     - DOES NOT perform metric-by-metric reconciliation.
#     - DOES NOT run duplicate-key diagnostics.
#     - Performs ONE narrow, date-filtered scan of the Bronze target only.
#
# WHY THIS IS ENOUGH FOR NOW:
#   The B01 stored procedure already performs a hard source-window preflight
#   before REPLACE WHERE. The Validator's job is only to confirm that the target
#   scope exists, is populated, and has an ingestion marker after the write.
#
# CHECKS WRITTEN TO checkHistory_perRun:
#   1. dateCoverage
#   2. targetPopulation
#   3. ingestionMarker
#
# DEEP DIAGNOSTICS:
#   Keep expensive checks as manual investigation queries until there is a
#   demonstrated operational need to schedule them.
# ============================================================================

# COMMAND ----------
from datetime import datetime

# COMMAND ----------
dbutils.widgets.text("runId", "")
dbutils.widgets.text("objectRunId", "")
dbutils.widgets.text("upstreamTaskKey", "bronze_udi_runner")
dbutils.widgets.text("asOfDate", "")
dbutils.widgets.text("eventWindowDays", "1")

run_id = dbutils.widgets.get("runId").strip()
object_run_id = dbutils.widgets.get("objectRunId").strip()
upstream_task_key = dbutils.widgets.get("upstreamTaskKey").strip()
as_of_date_raw = dbutils.widgets.get("asOfDate").strip()
event_window_days_raw = dbutils.widgets.get("eventWindowDays").strip()

# When running in Lakeflow, retrieve exact execution identifiers from the Runner
# if they were not passed explicitly.
if not object_run_id:
    object_run_id = dbutils.jobs.taskValues.get(
        taskKey=upstream_task_key,
        key="objectRunId",
        debugValue=""
    )

if not run_id:
    run_id = dbutils.jobs.taskValues.get(
        taskKey=upstream_task_key,
        key="runId",
        debugValue=""
    )

if not as_of_date_raw:
    as_of_date_raw = dbutils.jobs.taskValues.get(
        taskKey=upstream_task_key,
        key="asOfDate",
        debugValue=""
    )

if not event_window_days_raw:
    event_window_days_raw = str(
        dbutils.jobs.taskValues.get(
            taskKey=upstream_task_key,
            key="eventWindowDays",
            debugValue=1
        )
    )

if not run_id:
    raise ValueError("runId is required.")
if not object_run_id:
    raise ValueError("objectRunId is required.")
if not as_of_date_raw:
    raise ValueError("asOfDate is required.")

event_window_days = int(event_window_days_raw)
if event_window_days < 1:
    raise ValueError("eventWindowDays must be >= 1.")

as_of_date = datetime.strptime(as_of_date_raw, "%Y-%m-%d").date()

OBJECT_RUNS = "prdrzranalytics.lab42.sdi_tbl_mip_validation_objectRuns_perRun"
CHECK_HISTORY = "prdrzranalytics.lab42.sdi_tbl_mip_validation_checkHistory_perRun"
TARGET = "prdrzranalytics.lab42.sdi_tbl_mip_bronze_edlUdiHits_daily"

def qs(value):
    """SQL-quote a nullable scalar string."""
    if value is None:
        return "NULL"
    return "'" + str(value).replace("'", "''") + "'"

# COMMAND ----------
# Resolve scope from the exact execution attempt. This avoids trusting a
# separately supplied scope when a retry is being validated.
object_rows = spark.sql(f"""
SELECT
    runId,
    CAST(scopeStart AS DATE) AS scopeStart,
    CAST(scopeEnd AS DATE) AS scopeEnd,
    objectRunStatus
FROM {OBJECT_RUNS}
WHERE objectRunId = {qs(object_run_id)}
""").collect()

if len(object_rows) != 1:
    raise RuntimeError(
        f"Expected one objectRuns_perRun row for {object_run_id}; "
        f"found {len(object_rows)}."
    )

object_row = object_rows[0]

if object_row["runId"] != run_id:
    raise RuntimeError(
        f"objectRunId {object_run_id} belongs to runId "
        f"{object_row['runId']}, not {run_id}."
    )

if object_row["objectRunStatus"] != "SUCCEEDED":
    raise RuntimeError(
        f"B01 Validator will not run because objectRunStatus="
        f"{object_row['objectRunStatus']}."
    )

window_start = object_row["scopeStart"]
window_end = object_row["scopeEnd"]

# Idempotency: rerunning the Validator for the same execution attempt replaces
# only that attempt's lightweight B01 checks.
spark.sql(f"""
DELETE FROM {CHECK_HISTORY}
WHERE objectRunId = {qs(object_run_id)}
  AND validationPhase = 'POST'
  AND stageName = 'BRONZE'
  AND objectName = {qs(TARGET)}
""")

# COMMAND ----------
# ONE narrow TARGET-only scan.
#
# Grouping only by event_date lets us derive:
#   - total rows
#   - number of loaded dates
#   - latest ingestion marker
#
# No source scan and no high-cardinality grouping is performed.
daily_stats = spark.sql(f"""
SELECT
    event_date,
    COUNT(*) AS rowCount,
    MAX(_ingestedAt) AS latestIngestedAt
FROM {TARGET}
WHERE event_date BETWEEN DATE '{window_start}' AND DATE '{window_end}'
GROUP BY event_date
ORDER BY event_date
""").collect()

row_count = sum(int(r["rowCount"] or 0) for r in daily_stats)
loaded_date_count = len(daily_stats)
latest_ingested_at = max(
    (r["latestIngestedAt"] for r in daily_stats if r["latestIngestedAt"] is not None),
    default=None
)

checks = [
    {
        "checkName": "dateCoverage",
        "checkType": "COVERAGE",
        "issueType": "DATA_AVAILABILITY",
        "expected": float(event_window_days),
        "actual": float(loaded_date_count),
        "status": "HEALTHY" if loaded_date_count == event_window_days else "FAILED",
        "severity": "INFO" if loaded_date_count == event_window_days else "CRITICAL",
        "gateAction": "PROCEED" if loaded_date_count == event_window_days else "STOP",
        "isBlocking": loaded_date_count != event_window_days,
        "description": (
            "Every requested Bronze event date is present."
            if loaded_date_count == event_window_days
            else "One or more requested Bronze event dates are missing."
        ),
        "likelyCause": (
            None
            if loaded_date_count == event_window_days
            else "The requested date scope was not fully written."
        ),
        "nextSteps": (
            None
            if loaded_date_count == event_window_days
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
        "likelyCause": None if row_count > 0 else "The Bronze write did not produce rows.",
        "nextSteps": None if row_count > 0 else "Review B01 procedure output and requested source dates.",
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
        "likelyCause": None if latest_ingested_at is not None else "Target ingestion marker is unexpectedly null.",
        "nextSteps": None if latest_ingested_at is not None else "Inspect the Bronze target if this warning persists.",
    },
]

# COMMAND ----------
# Persist one canonical checkHistory row per lightweight check.
for c in checks:
    variance = c["actual"] - c["expected"]

    spark.sql(f"""
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
        {qs("VAL_" + object_run_id + "_" + c["checkName"])},
        {qs(run_id)},
        {qs(object_run_id)},
        current_timestamp(),
        DATE '{as_of_date}',
        'POST',
        'BRONZE',
        'BRONZE',
        {qs(TARGET)},
        'eventDate',
        DATE '{window_start}',
        DATE '{window_end}',
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        {qs(c["checkName"])},
        {qs(c["checkType"])},
        {qs(c["issueType"])},
        {c["expected"]},
        {c["actual"]},
        {variance},
        NULL,
        {qs(c["status"])},
        {qs(c["severity"])},
        {qs(c["gateAction"])},
        {str(c["isBlocking"]).upper()},
        NULL,
        {("TIMESTAMP " + qs(str(latest_ingested_at))) if latest_ingested_at is not None else "NULL"},
        {qs(c["description"])},
        {qs(c["likelyCause"])},
        {qs(c["nextSteps"])},
        'MIP Data Engineering',
        NULL,
        NULL,
        NULL,
        NULL
    )
    """)

# Object-level execution metric. No additional data scan is required.
spark.sql(f"""
UPDATE {OBJECT_RUNS}
SET
    rowsInScope = {row_count},
    notes = concat(
        coalesce(notes,''),
        {qs(
            f"; validator=LIGHT; "
            f"loadedDateCount={loaded_date_count}; "
            f"rowCount={row_count}"
        )}
    )
WHERE objectRunId = {qs(object_run_id)}
""")

blocking_failures = [
    c for c in checks
    if c["isBlocking"] and c["status"] in ("FAILED", "ERROR")
]
warnings = [c for c in checks if c["status"] == "WARNING"]

for c in checks:
    print(
        f"{c['status']:8} | {c['checkName']} | "
        f"expected={c['expected']} | actual={c['actual']}"
    )

try:
    dbutils.jobs.taskValues.set(
        key="validationStatus",
        value="FAILED" if blocking_failures else "WARNING" if warnings else "HEALTHY"
    )
    dbutils.jobs.taskValues.set(
        key="blockingFailureCount",
        value=len(blocking_failures)
    )
    dbutils.jobs.taskValues.set(
        key="warningCount",
        value=len(warnings)
    )
except Exception:
    pass

if blocking_failures:
    raise RuntimeError(
        "B01_VALIDATION_STOP: "
        + ", ".join(c["checkName"] for c in blocking_failures)
    )

print("B01 lightweight validation completed successfully.")
