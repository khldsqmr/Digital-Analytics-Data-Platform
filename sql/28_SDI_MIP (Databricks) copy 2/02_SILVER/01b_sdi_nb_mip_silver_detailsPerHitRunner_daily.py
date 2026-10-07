# Databricks notebook source
# ============================================================================
# FILE   : 01b_sdi_nb_mip_silver_detailsPerHitRunner_daily.py
# NAME   : sdi_nb_mip_silver_detailsPerHitRunner_daily
# OBJECT : S01
# LAYER  : SILVER
# PURPOSE:
#   Thin Runner for prdrzranalytics.lab42.sdi_sp_mip_silver_detailsPerHit_daily.
#
# VISIBLE INPUTS:
#   asOfDate
#   eventWindowDays
#   runId (optional)
#
# CALL NOTE:
#   The stored-procedure CALL renders only an already-validated Python date and
#   integer as SQL literals. This is intentional because the current Databricks
#   runtime requires stored-procedure CALL arguments to be foldable.
# ============================================================================

# COMMAND ----------
import json
import time
import uuid
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

# COMMAND ----------
dbutils.widgets.text("asOfDate", "")
dbutils.widgets.text("eventWindowDays", "1")
dbutils.widgets.text("runId", "")

as_of_raw = dbutils.widgets.get("asOfDate").strip()
provided_run_id = dbutils.widgets.get("runId").strip()
eventWindowDays_raw = dbutils.widgets.get("eventWindowDays").strip()
try:
    eventWindowDays_value = int(eventWindowDays_raw)
except Exception as exc:
    raise ValueError("eventWindowDays must be an integer >= 1.") from exc
if eventWindowDays_value < 1:
    raise ValueError("eventWindowDays must be >= 1.")

if as_of_raw:
    as_of_date = datetime.strptime(as_of_raw, "%Y-%m-%d").date()
else:
    as_of_date = (
        datetime.now(ZoneInfo("America/Los_Angeles")).date()
        - timedelta(days=1)
    )

window_end = as_of_date
window_start = as_of_date - timedelta(days=eventWindowDays_value - 1)

scope_start = window_start
scope_end = window_end
scope_type = "eventDate"
event_window_start = window_start
event_window_end = window_end
week_window_start = None
week_window_end = None

if provided_run_id:
    run_id = provided_run_id
    execution_type = "JOB"
    trigger_type = "UPSTREAM"
else:
    run_id = (
        "MAN_"
        + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        + "_"
        + uuid.uuid4().hex[:8].upper()
    )
    execution_type = "MAN"
    trigger_type = "MANUAL"

object_run_id = (
    "S01_"
    + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    + "_"
    + uuid.uuid4().hex[:8].upper()
)

PROC = "prdrzranalytics.lab42.sdi_sp_mip_silver_detailsPerHit_daily"
TARGET = "prdrzranalytics.lab42.sdi_tbl_mip_silver_detailsPerHit_daily"
RUN_DETAILS = "prdrzranalytics.lab42.sdi_tbl_mip_validation_runDetails_perRun"
OBJECT_RUNS = "prdrzranalytics.lab42.sdi_tbl_mip_validation_objectRuns_perRun"

# COMMAND ----------
# Ensure a parent run exists. In a production end-to-end job a dedicated
# run-start task can own this row; this keeps each Runner independently testable.
spark.sql(
    f"""
    MERGE INTO {RUN_DETAILS} t
    USING (
        SELECT
            :runId AS runId,
            :executionType AS executionType,
            :triggerType AS triggerType
    ) s
    ON t.runId = s.runId
    WHEN NOT MATCHED THEN INSERT (
        runId, executionType, triggerType,
        databricksJobId, databricksJobRunId, databricksTaskRunId,
        databricksJobName, notebookPath, orchestrationProcedure,
        asOfDate, eventWindowStart, eventWindowEnd,
        weekWindowStart, weekWindowEnd,
        runStartedAt, runFinishedAt, runStatus,
        warningCount, failureCount,
        errorSqlState, errorCondition, errorLine, errorMessage,
        createdAt, updatedAt
    )
    VALUES (
        s.runId, s.executionType, s.triggerType,
        NULL, NULL, NULL,
        NULL, NULL, NULL,
        :asOfDate, :eventWindowStart, :eventWindowEnd,
        :weekWindowStart, :weekWindowEnd,
        current_timestamp(), NULL, 'RUNNING',
        0, 0,
        NULL, NULL, NULL, NULL,
        current_timestamp(), current_timestamp()
    )
    """,
    args={
        "runId": run_id,
        "executionType": execution_type,
        "triggerType": trigger_type,
        "asOfDate": as_of_date,
        "eventWindowStart": event_window_start,
        "eventWindowEnd": event_window_end,
        "weekWindowStart": week_window_start,
        "weekWindowEnd": week_window_end,
    },
)

spark.sql(
    f"""
    INSERT INTO {OBJECT_RUNS} (
        objectRunId, runId, layerName, procedureName, targetObject,
        databricksTaskRunId, scopeType, scopeStart, scopeEnd,
        objectRunStartedAt, objectRunFinishedAt, objectRunStatus,
        rowsInScope, sourceWatermark,
        errorSqlState, errorCondition, errorLine, errorMessage, notes
    )
    VALUES (
        :objectRunId, :runId, 'SILVER', :procedureName, :targetObject,
        NULL, :scopeType, :scopeStart, :scopeEnd,
        current_timestamp(), NULL, 'RUNNING',
        NULL, NULL,
        NULL, NULL, NULL, NULL, :notes
    )
    """,
    args={
        "objectRunId": object_run_id,
        "runId": run_id,
        "procedureName": PROC,
        "targetObject": TARGET,
        "scopeType": scope_type,
        "scopeStart": str(scope_start),
        "scopeEnd": str(scope_end),
        "notes": "S01 Silver execution.",
    },
)

for key, value in {
    "runId": run_id,
    "objectRunId": object_run_id,
    "asOfDate": str(as_of_date),
    "eventWindowDays": eventWindowDays_value,
    "scopeStart": str(scope_start),
    "scopeEnd": str(scope_end),
}.items():
    dbutils.jobs.taskValues.set(key=key, value=value)

# COMMAND ----------
print("=" * 96)
print("MIP S01 | SILVER detailsPerHit")
print(f"runId             : {run_id}")
print(f"objectRunId       : {object_run_id}")
print(f"asOfDate          : {as_of_date}")
print(f"eventWindowDays  : {eventWindowDays_value}")
print(f"scopeStart       : {scope_start}")
print(f"scopeEnd         : {scope_end}")
print("=" * 96)

started = time.perf_counter()

try:
    call_sql = f"""
        CALL {PROC}(
            p_asOfDate        => DATE '{as_of_date.isoformat()}',
            p_eventWindowDays => {int(eventWindowDays_value)},
            p_validateOnly    => FALSE
        )
    """
    result = spark.sql(call_sql).collect()
    elapsed_seconds = time.perf_counter() - started

    spark.sql(
        f"""
        UPDATE {OBJECT_RUNS}
        SET
            objectRunFinishedAt = current_timestamp(),
            objectRunStatus = 'SUCCEEDED',
            errorSqlState = NULL,
            errorCondition = NULL,
            errorLine = NULL,
            errorMessage = NULL,
            notes = concat(coalesce(notes,''), :durationNote)
        WHERE objectRunId = :objectRunId
        """,
        args={
            "durationNote": f"; durationSeconds={elapsed_seconds:.3f}",
            "objectRunId": object_run_id,
        },
    )

    dbutils.jobs.taskValues.set(key="status", value="SUCCEEDED")
    dbutils.jobs.taskValues.set(
        key="durationSeconds",
        value=round(elapsed_seconds, 3),
    )

    for row in result:
        print(row)

    payload = {
        "runId": run_id,
        "objectRunId": object_run_id,
        "status": "SUCCEEDED",
        "asOfDate": str(as_of_date),
        "eventWindowDays": eventWindowDays_value,
        "scopeStart": str(scope_start),
        "scopeEnd": str(scope_end),
        "durationSeconds": round(elapsed_seconds, 3),
    }
    print(json.dumps(payload, indent=2))

except Exception as exc:
    elapsed_seconds = time.perf_counter() - started
    error_message = str(exc)

    spark.sql(
        f"""
        UPDATE {OBJECT_RUNS}
        SET
            objectRunFinishedAt = current_timestamp(),
            objectRunStatus = 'FAILED',
            errorMessage = :errorMessage,
            notes = concat(coalesce(notes,''), :durationNote)
        WHERE objectRunId = :objectRunId
        """,
        args={
            "errorMessage": error_message,
            "durationNote": f"; durationSeconds={elapsed_seconds:.3f}",
            "objectRunId": object_run_id,
        },
    )

    dbutils.jobs.taskValues.set(key="status", value="FAILED")
    dbutils.jobs.taskValues.set(
        key="durationSeconds",
        value=round(elapsed_seconds, 3),
    )

    print(
        f"FAILED | objectRunId={object_run_id} | "
        f"duration={elapsed_seconds / 60:.2f} minutes"
    )
    print(error_message)
    raise
