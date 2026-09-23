%python

from datetime import datetime, timezone

# ================================================================================================
# NOTEBOOK: sdi_nb_dashboardPulseTms_orchestration_allLayers_weekly
#
# PURPOSE:
#   Runs the full Dashboard Pulse TMS orchestration stored procedure.
#
# EXECUTION TYPES:
#   1. Databricks Job / "Run now"
#      -> uses actual Databricks Job / Task IDs passed through task parameters.
#
#   2. Manual notebook execution
#      -> generates a lightweight manual run ID:
#         PULSETMS_MAN_yyyyMMdd_HHmmss_SSS
#
# IMPORTANT:
#   This notebook calls ONLY the orchestration stored procedure.
#   The orchestration stored procedure calls validation as its final step.
#
#   Do NOT call the validation procedure separately from this notebook.
# ================================================================================================
  

# Full orchestration stored procedure name
procedure_name = (
    "prdrzranalytics.lab42."
    "sdi_sp_dashboardPulseTms_orchestration_allLayers_weekly"
)


# ================================================================================================
# HELPER: READ JOB PARAMETERS
#
# Scheduled Job / "Run now":
#   Values are supplied through Databricks task parameters.
#
# Manual notebook execution:
#   Widgets may not exist, so return None.
# ================================================================================================

def get_param(name):
    try:
        value = dbutils.widgets.get(name)

        # Treat blank or unresolved dynamic values as unavailable
        if not value or value.startswith("{{"):
            return None

        return value.strip()

    except Exception:
        return None


# ================================================================================================
# READ DATABRICKS JOB / TASK METADATA
# ================================================================================================

job_id = get_param("orchestration_job_id")
job_run_id = get_param("orchestration_job_run_id")
task_run_id = get_param("orchestration_task_run_id")
execution_count = get_param("orchestration_execution_count")


# ================================================================================================
# DETERMINE EXECUTION TYPE
#
# If a real Databricks Job Run ID exists:
#   -> JOB
#
# Otherwise:
#   -> manual notebook execution
# ================================================================================================

if job_run_id:

    # --------------------------------------------------------------------------------------------
    # DATABRICKS JOB EXECUTION
    # --------------------------------------------------------------------------------------------

    run_type = "JOB"

    # Defensive fallbacks in case optional metadata is unavailable
    job_id = job_id or "PULSETMS_JOB"
    task_run_id = task_run_id or f"{job_run_id}_T01"
    execution_count = int(execution_count or 1)

else:

    # --------------------------------------------------------------------------------------------
    # MANUAL NOTEBOOK EXECUTION
    #
    # Generate one manual run ID and use it consistently through:
    #
    #   Notebook
    #      -> Orchestration SP
    #      -> Validation SP
    #      -> Validation History
    # --------------------------------------------------------------------------------------------

    now = datetime.now(timezone.utc)

    manual_run_id = (
        "PULSETMS_MAN_"
        + now.strftime("%Y%m%d_%H%M%S")
        + f"_{now.microsecond // 1000:03d}"
    )

    run_type = "MANUAL"

    job_id = "PULSETMS_MAN"
    job_run_id = manual_run_id
    task_run_id = f"{manual_run_id}_T01"
    execution_count = 1


# ================================================================================================
# START EXECUTION
# ================================================================================================

start_time = datetime.now(timezone.utc)

print("=" * 100)
print(f"Starting procedure: {procedure_name}")
print(f"Run Type: {run_type}")
print(f"Job ID: {job_id}")
print(f"Run ID: {job_run_id}")
print(f"Task Run ID: {task_run_id}")
print(f"Execution Count: {execution_count}")
print(f"Start Time UTC: {start_time.isoformat()}")
print("=" * 100)


# ================================================================================================
# RUN ORCHESTRATION
#
# The orchestration SP performs:
#   - upstream MFC refreshes
#   - QGP Archive refresh
#   - PulseTMS Bronze
#   - PulseTMS Silver
#   - dependent UPV Forecast Silver
#   - post-run validation
#
# Gold objects are live views, so no Gold CALL is required.
# ================================================================================================

try:

    spark.sql(
        f"""
        CALL {procedure_name}(
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


    # --------------------------------------------------------------------------------------------
    # SUCCESS
    # --------------------------------------------------------------------------------------------

    end_time = datetime.now(timezone.utc)

    print("=" * 100)
    print(f"Procedure completed successfully: {procedure_name}")
    print(f"Run ID: {job_run_id}")
    print(f"End Time UTC: {end_time.isoformat()}")
    print(f"Duration: {end_time - start_time}")
    print("=" * 100)


except Exception as error:

    # --------------------------------------------------------------------------------------------
    # FAILURE
    #
    # Re-raise the exception so Databricks:
    #   - marks the task as failed
    #   - applies configured retries
    #   - sends configured failure notifications
    # --------------------------------------------------------------------------------------------

    end_time = datetime.now(timezone.utc)

    print("=" * 100)
    print(f"Procedure failed: {procedure_name}")
    print(f"Run ID: {job_run_id}")
    print(f"Failure Time UTC: {end_time.isoformat()}")
    print(f"Duration Before Failure: {end_time - start_time}")
    print(f"Error Type: {type(error).__name__}")
    print(f"Error Details: {error}")
    print("=" * 100)

    raise