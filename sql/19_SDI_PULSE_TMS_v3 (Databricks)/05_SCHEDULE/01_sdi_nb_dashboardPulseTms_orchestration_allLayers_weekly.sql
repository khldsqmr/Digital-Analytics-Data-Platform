# Databricks notebook source

# ==================================================================================================
# Dashboard Pulse TMS - Weekly Orchestration Scheduler
#
# JOB RUN:
#   Receives Databricks Job/Task metadata through notebook task parameters.
#
# MANUAL NOTEBOOK RUN:
#   Generates a PULSETMS_MAN_* execution ID.
#
# The orchestration SP runs the pipeline and validation as its final step.
# ==================================================================================================

from datetime import datetime, timezone


procedure_name = (
    "prdrzranalytics.lab42."
    "sdi_sp_dashboardPulseTms_orchestration_allLayers_weekly"
)


# --------------------------------------------------------------------------------------------------
# Read notebook task parameter.
#
# Job task parameters become notebook widgets in Databricks.
# If the notebook is run manually, those parameters may not exist.
# --------------------------------------------------------------------------------------------------

def get_param(name):
    try:
        value = dbutils.widgets.get(name).strip()

        # Protect against blank or unresolved dynamic references.
        if not value or value.startswith("{{"):
            return None

        return value

    except Exception:
        return None


# --------------------------------------------------------------------------------------------------
# Read Databricks Job metadata.
# --------------------------------------------------------------------------------------------------

job_id = get_param("orchestration_job_id")
job_run_id = get_param("orchestration_job_run_id")
task_run_id = get_param("orchestration_task_run_id")
execution_count = get_param("orchestration_execution_count")


# --------------------------------------------------------------------------------------------------
# Determine execution mode.
# --------------------------------------------------------------------------------------------------

if job_run_id:

    run_type = "JOB"

    # If this is a Job run, the Job ID and Task Run ID should also have been supplied.
    # Fail rather than writing incomplete lineage.
    if not job_id:
        raise ValueError(
            "orchestration_job_id was not supplied to the scheduler notebook."
        )

    if not task_run_id:
        raise ValueError(
            "orchestration_task_run_id was not supplied to the scheduler notebook."
        )

    execution_count = int(execution_count or 1)


else:

    # Direct interactive notebook execution.
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
# Execution information.
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
# Execute orchestration SP.
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

    # Force the CALL to complete before the notebook reports success.
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

    # Re-raise so Databricks marks the task as Failed.
    raise