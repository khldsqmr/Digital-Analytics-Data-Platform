# Databricks notebook source
# ============================================================================
# FILE   : 01b_sdi_nb_mip_silver_detailsPerHitRunner_daily.py
# NAME   : sdi_nb_mip_silver_detailsPerHitRunner_daily
# OBJECT : S01
# LAYER  : SILVER
#
# PURPOSE:
#   Thin production/development Runner for:
#   prdrzranalytics.lab42.sdi_sp_mip_silver_detailsPerHit_daily
#
# VISIBLE INPUTS:
#   asOfDate        : YYYY-MM-DD; blank = previous Pacific day
#   eventWindowDays : >= 1
#   runId           : optional; blank = generate a MAN_* run ID
#
# RUNTIME MODEL:
#   - S01 remains eventDate-scoped.
#   - The stored procedure owns preflight + atomic selective overwrite.
#   - This Runner owns execution lineage only.
#   - A new objectRunId is created for every execution attempt.
#
# CALL NOTE:
#   Normal DML uses Spark named parameter binding.
#
#   The stored-procedure CALL renders only the already-validated Python DATE
#   and INT values as SQL literals because this Databricks runtime requires
#   CALL arguments to be foldable at analysis time.
#
# VALIDATION:
#   The downstream 01c Validator should receive this Runner's objectRunId.
#
# LAKEFLOW:
#   Pass asOfDate/eventWindowDays/runId as notebook task parameters.
#   This Runner publishes objectRunId and scope values as task values.
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

as_of_raw = dbutils.widgets.get("asOfDate").strip()
event_window_days_raw = dbutils.widgets.get("eventWindowDays").strip()
provided_run_id = dbutils.widgets.get("runId").strip()

# COMMAND ----------
# ---------------------------------------------------------------------------
# 1. Validate and resolve inputs
# ---------------------------------------------------------------------------
try:
    event_window_days = int(event_window_days_raw)
except Exception as exc:
    raise ValueError("eventWindowDays must be an integer >= 1.") from exc

if event_window_days < 1:
    raise ValueError("eventWindowDays must be >= 1.")

if as_of_raw:
    try:
        as_of_date = datetime.strptime(as_of_raw, "%Y-%m-%d").date()
    except Exception as exc:
        raise ValueError("asOfDate must be YYYY-MM-DD.") from exc
else:
    as_of_date = (
        datetime.now(ZoneInfo("America/Los_Angeles")).date()
        - timedelta(days=1)
    )

window_end = as_of_date
window_start = as_of_date - timedelta(days=event_window_days - 1)

# S01 is intentionally eventDate-scoped.
scope_type = "eventDate"
scope_start = window_start
scope_end = window_end

event_window_start = window_start
event_window_end = window_end

# S01 is daily/event-date grain, so no weekly scope belongs to this Runner.
week_window_start = None
week_window_end = None

# COMMAND ----------
# ---------------------------------------------------------------------------
# 2. Resolve parent runId + unique execution attempt objectRunId
# ---------------------------------------------------------------------------
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

# COMMAND ----------
PROC = "prdrzranalytics.lab42.sdi_sp_mip_silver_detailsPerHit_daily"
TARGET = "prdrzranalytics.lab42.sdi_tbl_mip_silver_detailsPerHit_daily"

RUN_DETAILS = (
    "prdrzranalytics.lab42."
    "sdi_tbl_mip_validation_runDetails_perRun"
)

OBJECT_RUNS = (
    "prdrzranalytics.lab42."
    "sdi_tbl_mip_validation_objectRuns_perRun"
)

# COMMAND ----------
# ---------------------------------------------------------------------------
# 3. Ensure parent run exists
#
# In the final Lakeflow orchestration a dedicated run-start task can own this
# record. WHEN NOT MATCHED keeps S01 independently runnable during development.
# ---------------------------------------------------------------------------
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
        :eventWindowStart,
        :eventWindowEnd,
        :weekWindowStart,
        :weekWindowEnd,
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
        "executionType": execution_type,
        "triggerType": trigger_type,
        "asOfDate": as_of_date,
        "eventWindowStart": event_window_start,
        "eventWindowEnd": event_window_end,
        "weekWindowStart": week_window_start,
        "weekWindowEnd": week_window_end,
    },
)

# COMMAND ----------
# ---------------------------------------------------------------------------
# 4. Record this exact S01 execution attempt
# ---------------------------------------------------------------------------
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
        'SILVER',
        :procedureName,
        :targetObject,
        NULL,
        :scopeType,
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
        "scopeType": scope_type,
        "scopeStart": str(scope_start),
        "scopeEnd": str(scope_end),
        "notes": "S01 Silver detailsPerHit execution.",
    },
)

# COMMAND ----------
# ---------------------------------------------------------------------------
# 5. Publish execution context for the downstream Validator
# ---------------------------------------------------------------------------
for key, value in {
    "runId": run_id,
    "objectRunId": object_run_id,
    "asOfDate": str(as_of_date),
    "eventWindowDays": event_window_days,
    "scopeType": scope_type,
    "scopeStart": str(scope_start),
    "scopeEnd": str(scope_end),
}.items():
    dbutils.jobs.taskValues.set(
        key=key,
        value=value,
    )

# COMMAND ----------
print("=" * 96)
print("MIP S01 | SILVER detailsPerHit")
print(f"runId            : {run_id}")
print(f"objectRunId      : {object_run_id}")
print(f"asOfDate         : {as_of_date}")
print(f"eventWindowDays  : {event_window_days}")
print(f"scopeType        : {scope_type}")
print(f"scopeStart       : {scope_start}")
print(f"scopeEnd         : {scope_end}")
print("=" * 96)

# COMMAND ----------
# ---------------------------------------------------------------------------
# 6. Execute S01
#
# IMPORTANT:
# Databricks stored-procedure CALL arguments must be foldable in this runtime.
#
# Therefore:
#   - as_of_date was strictly parsed into a Python date
#   - event_window_days was strictly parsed into an integer
#   - those already-validated values are rendered as SQL literals
#
# Do NOT replace this CALL with:
#
#     spark.sql("CALL ... :asOfDate ...", args={...})
#
# because that path produced:
#     requirement failed: args must be foldable
# ---------------------------------------------------------------------------
started = time.perf_counter()

try:
    call_sql = f"""
        CALL {PROC}(
            p_asOfDate        => DATE '{as_of_date.isoformat()}',
            p_eventWindowDays => {int(event_window_days)},
            p_validateOnly    => FALSE
        )
    """

    result = spark.sql(call_sql).collect()

    elapsed_seconds = time.perf_counter() - started

    # -----------------------------------------------------------------------
    # 7. Mark this execution attempt successful
    # -----------------------------------------------------------------------
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
            "durationNote": (
                f"; durationSeconds={elapsed_seconds:.3f}"
            ),
            "objectRunId": object_run_id,
        },
    )

    dbutils.jobs.taskValues.set(
        key="status",
        value="SUCCEEDED",
    )

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
        "eventWindowDays": event_window_days,
        "scopeType": scope_type,
        "scopeStart": str(scope_start),
        "scopeEnd": str(scope_end),
        "durationSeconds": round(elapsed_seconds, 3),
    }

    print(json.dumps(payload, indent=2))

except Exception as exc:
    elapsed_seconds = time.perf_counter() - started
    error_message = str(exc)

    # -----------------------------------------------------------------------
    # 8. Mark this execution attempt failed
    #
    # Keep this lightweight like the working Bronze Runner. Detailed Spark /
    # Databricks diagnostics remain available in the task logs.
    # -----------------------------------------------------------------------
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
            "durationNote": (
                f"; durationSeconds={elapsed_seconds:.3f}"
            ),
            "objectRunId": object_run_id,
        },
    )

    dbutils.jobs.taskValues.set(
        key="status",
        value="FAILED",
    )

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