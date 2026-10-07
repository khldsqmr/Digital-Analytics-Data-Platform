# Databricks notebook source
# ============================================================================
# FILE   : 01b_sdi_nb_mip_bronze_edlUdiHitsRunner_daily.py new
# NAME   : sdi_nb_mip_bronze_edlUdiHitsRunner_daily
# OBJECT : B01
# LAYER  : BRONZE
#
# PURPOSE:
#   Thin production/development Runner for the B01 stored procedure.
#
# VISIBLE INPUTS:
#   asOfDate        : YYYY-MM-DD; blank = previous Pacific day
#   eventWindowDays : >= 1
#   runId           : optional; blank = generate a MAN_* run ID
#
# IMPORTANT:
#   Normal DML uses Spark named parameter binding. The stored-procedure CALL
#   uses validated DATE/INT literals because this Databricks runtime requires
#   CALL arguments to be foldable at analysis time.
#
# MANUAL:
#   Fill the widgets and Run All.
#
# LAKEFLOW:
#   Pass asOfDate/eventWindowDays/runId as notebook task parameters. The Runner
#   publishes objectRunId and scope values as task values for the Validator.
# ============================================================================

# COMMAND ----------
import json
import time
import uuid
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

# COMMAND ----------
# Keep the interactive UI intentionally small.
dbutils.widgets.text("asOfDate", "")
dbutils.widgets.text("eventWindowDays", "1")
dbutils.widgets.text("runId", "")

as_of_date_raw = dbutils.widgets.get("asOfDate").strip()
event_window_days_raw = dbutils.widgets.get("eventWindowDays").strip()
provided_run_id = dbutils.widgets.get("runId").strip()

try:
    event_window_days = int(event_window_days_raw)
except Exception as exc:
    raise ValueError("eventWindowDays must be an integer >= 1.") from exc

if event_window_days < 1:
    raise ValueError("eventWindowDays must be >= 1.")

if as_of_date_raw:
    as_of_date = datetime.strptime(as_of_date_raw, "%Y-%m-%d").date()
else:
    as_of_date = (
        datetime.now(ZoneInfo("America/Los_Angeles")).date()
        - timedelta(days=1)
    )

window_end = as_of_date
window_start = as_of_date - timedelta(days=event_window_days - 1)

if provided_run_id:
    run_id = provided_run_id
    fallback_execution_type = "JOB"
    fallback_trigger_type = "UPSTREAM"
else:
    run_id = (
        "MAN_"
        + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        + "_"
        + uuid.uuid4().hex[:8].upper()
    )
    fallback_execution_type = "MAN"
    fallback_trigger_type = "MANUAL"

object_run_id = (
    "B01_"
    + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    + "_"
    + uuid.uuid4().hex[:8].upper()
)

PROC = "prdrzranalytics.lab42.sdi_sp_mip_bronze_edlUdiHits_daily"
TARGET = "prdrzranalytics.lab42.sdi_tbl_mip_bronze_edlUdiHits_daily"
RUN_DETAILS = "prdrzranalytics.lab42.sdi_tbl_mip_validation_runDetails_perRun"
OBJECT_RUNS = "prdrzranalytics.lab42.sdi_tbl_mip_validation_objectRuns_perRun"

# COMMAND ----------
# Ensure a run-level header exists.
#
# In a future full Lakeflow orchestration, a dedicated run-start task can own
# this row. WHEN NOT MATCHED keeps this Runner independently runnable now.
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
        runId,
        executionType,
        triggerType,
        databricksJobId,
        databricksJobRunId,
        databricksTaskRunId,
        databricksJobName,
        notebookPath,
        orchestrationProcedure,
        asOfDate,
        eventWindowStart,
        eventWindowEnd,
        weekWindowStart,
        weekWindowEnd,
        runStartedAt,
        runFinishedAt,
        runStatus,
        warningCount,
        failureCount,
        errorSqlState,
        errorCondition,
        errorLine,
        errorMessage,
        createdAt,
        updatedAt
    )
    VALUES (
        s.runId,
        s.executionType,
        s.triggerType,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        :asOfDate,
        :windowStart,
        :windowEnd,
        NULL,
        NULL,
        current_timestamp(),
        NULL,
        'RUNNING',
        0,
        0,
        NULL,
        NULL,
        NULL,
        NULL,
        current_timestamp(),
        current_timestamp()
    )
    """,
    args={
        "runId": run_id,
        "executionType": fallback_execution_type,
        "triggerType": fallback_trigger_type,
        "asOfDate": as_of_date,
        "windowStart": window_start,
        "windowEnd": window_end,
    },
)

# One row per B01 execution attempt.
spark.sql(
    f"""
    INSERT INTO {OBJECT_RUNS} (
        objectRunId,
        runId,
        layerName,
        procedureName,
        targetObject,
        databricksTaskRunId,
        scopeType,
        scopeStart,
        scopeEnd,
        objectRunStartedAt,
        objectRunFinishedAt,
        objectRunStatus,
        rowsInScope,
        sourceWatermark,
        errorSqlState,
        errorCondition,
        errorLine,
        errorMessage,
        notes
    )
    VALUES (
        :objectRunId,
        :runId,
        'BRONZE',
        :procedureName,
        :targetObject,
        NULL,
        'eventDate',
        :scopeStart,
        :scopeEnd,
        current_timestamp(),
        NULL,
        'RUNNING',
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        :notes
    )
    """,
    args={
        "objectRunId": object_run_id,
        "runId": run_id,
        "procedureName": PROC,
        "targetObject": TARGET,
        "scopeStart": str(window_start),
        "scopeEnd": str(window_end),
        "notes": "B01 Bronze UDI execution.",
    },
)

# Publish values for a downstream Validator task.
# Outside a Databricks Job, taskValues.set() is a no-op.
for key, value in {
    "runId": run_id,
    "objectRunId": object_run_id,
    "asOfDate": str(as_of_date),
    "eventWindowDays": event_window_days,
    "windowStart": str(window_start),
    "windowEnd": str(window_end),
}.items():
    dbutils.jobs.taskValues.set(key=key, value=value)

# COMMAND ----------
print("=" * 96)
print("MIP B01 | BRONZE UDI HITS")
print(f"runId            : {run_id}")
print(f"objectRunId      : {object_run_id}")
print(f"asOfDate         : {as_of_date}")
print(f"eventWindowDays  : {event_window_days}")
print(f"windowStart      : {window_start}")
print(f"windowEnd        : {window_end}")
print("=" * 96)

started = time.perf_counter()

try:
    # IMPORTANT:
    # Databricks stored-procedure CALL arguments must be foldable in this
    # runtime. Spark parameter markers supplied through args= are not accepted
    # by CALL here ("requirement failed: args must be foldable").
    #
    # These two values are safe to render as SQL literals because:
    #   - as_of_date was parsed strictly as YYYY-MM-DD into a Python date
    #   - event_window_days was parsed/validated as an integer >= 1
    #
    # Keep parameter binding for the surrounding DML statements; only CALL
    # requires literal rendering in this notebook/runtime.
    call_sql = f"""
        CALL {PROC}(
            p_asOfDate        => DATE '{as_of_date.isoformat()}',
            p_eventWindowDays => {int(event_window_days)},
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
            notes = concat(
                coalesce(notes, ''),
                :durationNote
            )
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

    payload = {
        "runId": run_id,
        "objectRunId": object_run_id,
        "status": "SUCCEEDED",
        "asOfDate": str(as_of_date),
        "eventWindowDays": event_window_days,
        "windowStart": str(window_start),
        "windowEnd": str(window_end),
        "durationSeconds": round(elapsed_seconds, 3),
    }

    for row in result:
        print(row)

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
            notes = concat(
                coalesce(notes, ''),
                :durationNote
            )
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
