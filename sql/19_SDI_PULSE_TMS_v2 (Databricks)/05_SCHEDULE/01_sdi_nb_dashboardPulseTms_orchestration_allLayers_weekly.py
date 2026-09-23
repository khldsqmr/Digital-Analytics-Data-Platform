# Databricks notebook source

# ==================================================================================================
# Dashboard Pulse TMS - Weekly Orchestration
#
# JOB RUN:
#   Receives Databricks Job/Task metadata through notebook task parameters.
#
# MANUAL NOTEBOOK RUN:
#   Generates a PULSETMS_MAN_* execution ID.
#
# The orchestration SP runs the full SQL pipeline and executes validation as its final step.
# ==================================================================================================

from datetime import datetime, timezone


procedure_name = (
    "prdrzranalytics.lab42."
    "sdi_sp_dashboardPulseTms_orchestration_allLayers_weekly"
)


# --------------------------------------------------------------------------------------------------
# Read a notebook task parameter.
#
# Parameters exist when this notebook runs as a Databricks Job task.
# During a direct interactive notebook run they may not exist.
# --------------------------------------------------------------------------------------------------

def get_param(name):
    try:
        value = dbutils.widgets.get(name).strip()

        # Also protect against an unresolved Databricks dynamic-value reference.
        if not value or value.startswith("{{"):
            return None

        return value

    except Exception:
        return None


# --------------------------------------------------------------------------------------------------
# Databricks Job metadata
# --------------------------------------------------------------------------------------------------

job_id = get_param("orchestration_job_id")
job_run_id = get_param("orchestration_job_run_id")
task_run_id = get_param("orchestration_task_run_id")
execution_count = get_param("orchestration_execution_count")


# --------------------------------------------------------------------------------------------------
# Determine execution mode.
#
# If a Job Run ID exists, this is a Databricks Job execution.
# Otherwise this notebook was executed interactively/manually.
# --------------------------------------------------------------------------------------------------

if job_run_id:

    run_type = "JOB"

    execution_count = int(
        execution_count or 1
    )

else:

    run_type = "MANUAL"

    now = datetime.now(timezone.utc)

    manual_run_id = (
        "PULSETMS_MAN_"
        + now.strftime("%Y%m%d_%H%M%S")
        + f"_{now.microsecond // 1000:03d}"
    )

    job_id = "PULSETMS_MAN"
    job_run_id = manual_run_id
    task_run_id = f"{manual_run_id}_T01"
    execution_count = 1


# --------------------------------------------------------------------------------------------------
# Execution information
# --------------------------------------------------------------------------------------------------

start_time = datetime.now(timezone.utc)

print("Starting Dashboard Pulse TMS orchestration")
print(f"Run Type:       {run_type}")
print(f"Job ID:         {job_id}")
print(f"Job Run ID:     {job_run_id}")
print(f"Task Run ID:    {task_run_id}")
print(f"Execution #:    {execution_count}")
print(f"Start UTC:      {start_time.isoformat()}")


# --------------------------------------------------------------------------------------------------
# Execute the orchestration procedure.
#
# Parameter markers keep runtime values separate from the SQL statement.
# --------------------------------------------------------------------------------------------------

try:

    result = spark.sql(
        f"""
        CALL {procedure_name}
        (
          :run_type,
          :job_id,
          :job_run_id,
          :task_run_id,
          :execution_count
        )
        """,
        args={
            "run_type": run_type,
            "job_id": job_id,
            "job_run_id": job_run_id,
            "task_run_id": task_run_id,
            "execution_count": execution_count,
        },
    )

    # Ensure the CALL fully completes before the notebook reports success.
    result.collect()

    end_time = datetime.now(timezone.utc)

    print("")
    print("Dashboard Pulse TMS orchestration completed successfully.")
    print(f"Job Run ID: {job_run_id}")
    print(f"End UTC:    {end_time.isoformat()}")
    print(f"Duration:   {end_time - start_time}")


except Exception as error:

    end_time = datetime.now(timezone.utc)

    print("")
    print("Dashboard Pulse TMS orchestration FAILED.")
    print(f"Job Run ID: {job_run_id}")
    print(f"End UTC:    {end_time.isoformat()}")
    print(f"Duration:   {end_time - start_time}")
    print(f"Error:      {error}")

    # Important: re-raise so Databricks marks the Job task as Failed.
    raise
