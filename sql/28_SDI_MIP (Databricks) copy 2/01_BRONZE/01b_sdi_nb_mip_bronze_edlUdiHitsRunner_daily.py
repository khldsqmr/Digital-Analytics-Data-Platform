# Databricks notebook source
# ============================================================================
# FILE   : 01b_sdi_nb_mip_bronze_edlUdiHitsRunner_daily.py
# NAME   : sdi_nb_mip_bronze_edlUdiHitsRunner_daily
# OBJECT : B01
# LAYER  : BRONZE
# PURPOSE:
#   Production Runner for Bronze01 UDI hits.
#
# RESPONSIBILITIES:
#   1. Resolve runtime parameters.
#   2. Create/ensure the run-level validation header exists.
#   3. Generate one human-readable objectRunId for this execution attempt.
#   4. Register the object execution as RUNNING.
#   5. Call the Bronze01 stored procedure.
#   6. Mark the object execution SUCCEEDED/FAILED.
#   7. Publish task values for the downstream Validator.
#
# OBJECT-RUN ID:
#   B01_<UTC timestamp>_<8-char token>
#   Example: B01_20261007T061945Z_7F3A91C2
#
# PERFORMANCE:
#   This notebook does not perform source/target reconciliation. The stored
#   procedure performs the required hard source-window preflight itself.
#
# CONCURRENCY:
#   Production concurrency is intentionally NOT implemented here. Keep one
#   Runner invocation = one transformation invocation. Use the temporary B01
#   Benchmark notebook to test chunking/concurrency before configuring Lakeflow.
# ============================================================================

# COMMAND ----------
import json
import time
import uuid
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

# COMMAND ----------
# Runtime parameters.
dbutils.widgets.text("asOfDate", "")
dbutils.widgets.text("eventWindowDays", "1")
dbutils.widgets.text("runId", "")
dbutils.widgets.dropdown("executionType", "MAN", ["MAN", "JOB", "BCK", "RPR", "RTY", "TST"])
dbutils.widgets.dropdown("triggerType", "MANUAL", ["MANUAL", "SCHEDULED", "API", "UPSTREAM", "RETRY", "BACKFILL"])
dbutils.widgets.text("databricksJobId", "")
dbutils.widgets.text("databricksJobRunId", "")
dbutils.widgets.text("databricksTaskRunId", "")
dbutils.widgets.text("databricksJobName", "")
dbutils.widgets.text("databricksTaskName", "")
dbutils.widgets.text("notebookPath", "")

as_of_date_raw = dbutils.widgets.get("asOfDate").strip()
event_window_days = int(dbutils.widgets.get("eventWindowDays"))
run_id = dbutils.widgets.get("runId").strip()
execution_type = dbutils.widgets.get("executionType").strip().upper()
trigger_type = dbutils.widgets.get("triggerType").strip().upper()

job_id = dbutils.widgets.get("databricksJobId").strip()
job_run_id = dbutils.widgets.get("databricksJobRunId").strip()
task_run_id = dbutils.widgets.get("databricksTaskRunId").strip()
job_name = dbutils.widgets.get("databricksJobName").strip()
task_name = dbutils.widgets.get("databricksTaskName").strip()
notebook_path = dbutils.widgets.get("notebookPath").strip()

if event_window_days < 1:
    raise ValueError("eventWindowDays must be >= 1")

# Resolve the effective date ONCE so Runner, SP, object-run record and Validator
# all operate on exactly the same date scope.
if as_of_date_raw:
    as_of_date = datetime.strptime(as_of_date_raw, "%Y-%m-%d").date()
else:
    as_of_date = (
        datetime.now(ZoneInfo("America/Los_Angeles")).date()
        - timedelta(days=1)
    )

window_end = as_of_date
window_start = as_of_date - timedelta(days=event_window_days - 1)

if not run_id:
    run_id = (
        "MAN_"
        + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        + "_"
        + uuid.uuid4().hex[:8].upper()
    )

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

def qs(value):
    """SQL-quote a nullable scalar string."""
    if value is None or value == "":
        return "NULL"
    return "'" + str(value).replace("'", "''") + "'"

# COMMAND ----------
# Ensure a run-level header exists.
#
# In the future, an end-to-end Lakeflow run-start task can create this row.
# Keeping this MERGE makes B01 independently runnable during development.
spark.sql(f"""
MERGE INTO {RUN_DETAILS} t
USING (
    SELECT
        {qs(run_id)} AS runId,
        {qs(execution_type)} AS executionType,
        {qs(trigger_type)} AS triggerType
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
    {qs(job_id)},
    {qs(job_run_id)},
    {qs(task_run_id)},
    {qs(job_name)},
    {qs(notebook_path)},
    NULL,
    DATE '{as_of_date}',
    DATE '{window_start}',
    DATE '{window_end}',
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
""")

# One row per B01 execution attempt.
spark.sql(f"""
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
    {qs(object_run_id)},
    {qs(run_id)},
    'BRONZE',
    {qs(PROC)},
    {qs(TARGET)},
    {qs(task_run_id)},
    'eventDate',
    {qs(str(window_start))},
    {qs(str(window_end))},
    current_timestamp(),
    NULL,
    'RUNNING',
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    {qs(
        "B01 Bronze UDI execution; "
        f"taskName={task_name or 'N/A'}; "
        f"notebookPath={notebook_path or 'N/A'}"
    )}
)
""")

# Publish identifiers immediately so a downstream task can retrieve the exact
# execution attempt after the Runner succeeds.
try:
    dbutils.jobs.taskValues.set(key="runId", value=run_id)
    dbutils.jobs.taskValues.set(key="objectRunId", value=object_run_id)
    dbutils.jobs.taskValues.set(key="asOfDate", value=str(as_of_date))
    dbutils.jobs.taskValues.set(key="eventWindowDays", value=event_window_days)
    dbutils.jobs.taskValues.set(key="windowStart", value=str(window_start))
    dbutils.jobs.taskValues.set(key="windowEnd", value=str(window_end))
except Exception:
    # Direct interactive notebook execution does not require task values.
    pass

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
    result = spark.sql(f"""
        CALL {PROC}(
            p_asOfDate        => DATE '{as_of_date}',
            p_eventWindowDays => {event_window_days},
            p_validateOnly    => FALSE
        )
    """).collect()

    elapsed_seconds = time.perf_counter() - started

    spark.sql(f"""
        UPDATE {OBJECT_RUNS}
        SET
            objectRunFinishedAt = current_timestamp(),
            objectRunStatus = 'SUCCEEDED',
            notes = concat(
                coalesce(notes,''),
                {qs(f'; durationSeconds={elapsed_seconds:.3f}')}
            )
        WHERE objectRunId = {qs(object_run_id)}
    """)

    try:
        dbutils.jobs.taskValues.set(key="status", value="SUCCEEDED")
        dbutils.jobs.taskValues.set(
            key="durationSeconds",
            value=round(elapsed_seconds, 3)
        )
    except Exception:
        pass

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

    spark.sql(f"""
        UPDATE {OBJECT_RUNS}
        SET
            objectRunFinishedAt = current_timestamp(),
            objectRunStatus = 'FAILED',
            errorMessage = {qs(error_message)},
            notes = concat(
                coalesce(notes,''),
                {qs(f'; durationSeconds={elapsed_seconds:.3f}')}
            )
        WHERE objectRunId = {qs(object_run_id)}
    """)

    try:
        dbutils.jobs.taskValues.set(key="status", value="FAILED")
        dbutils.jobs.taskValues.set(
            key="durationSeconds",
            value=round(elapsed_seconds, 3)
        )
    except Exception:
        pass

    print(
        f"FAILED | objectRunId={object_run_id} | "
        f"duration={elapsed_seconds / 60:.2f} minutes"
    )
    print(error_message)
    raise
