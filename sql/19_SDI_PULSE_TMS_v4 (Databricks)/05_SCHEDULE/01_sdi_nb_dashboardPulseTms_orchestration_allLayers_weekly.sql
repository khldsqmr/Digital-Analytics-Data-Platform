# Databricks notebook source

# ==================================================================================================
# Dashboard Pulse TMS - Weekly Orchestration Scheduler
#
# FLOW:
#   Job metadata -> Orchestration SP -> Bronze/Silver/Gold -> Validation -> Status check
#
# FAILURE:
#   - SQL/SP/runtime error        -> FAIL
#   - No validation result       -> FAIL
#   - Validation count != 64     -> FAIL
#   - Validation status FAILED   -> FAIL only when OPTIONAL FAILURE GATE is enabled
#   - Validation status WARNING  -> SUCCESS
# ==================================================================================================

from datetime import datetime, timezone


# --------------------------------------------------------------------------------------------------
# Configuration
# --------------------------------------------------------------------------------------------------

procedure_name = (
    "prdrzranalytics.lab42."
    "sdi_sp_dashboardPulseTms_orchestration_allLayers_weekly"
)

validation_runs_view = (
    "prdrzranalytics.lab42."
    "sdi_vw_dashboardPulseTms_validation_runs_perRun"
)

EXPECTED_VALIDATION_METRICS = 64


# --------------------------------------------------------------------------------------------------
# Read Databricks notebook task parameter.
# Returns None for manual runs, blank values, or unresolved {{...}} references.
# --------------------------------------------------------------------------------------------------

def get_param(name):
    try:
        value = dbutils.widgets.get(name).strip()
        return None if not value or value.startswith("{{") else value
    except Exception:
        return None


# --------------------------------------------------------------------------------------------------
# Runtime metadata
# --------------------------------------------------------------------------------------------------

job_id = get_param("orchestration_job_id")
job_run_id = get_param("orchestration_job_run_id")
task_run_id = get_param("orchestration_task_run_id")
execution_count = get_param("orchestration_execution_count")


# --------------------------------------------------------------------------------------------------
# Determine JOB vs MANUAL execution.
# --------------------------------------------------------------------------------------------------

if job_run_id:

    run_type = "JOB"

    # Job executions must have complete lineage.
    if not job_id:
        raise ValueError("orchestration_job_id was not supplied.")

    if not task_run_id:
        raise ValueError("orchestration_task_run_id was not supplied.")

    execution_count = int(execution_count or 1)

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
# Start log
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
# Execute pipeline.
# --------------------------------------------------------------------------------------------------

try:

    # ==============================================================================================
    # 1. RUN ORCHESTRATION
    #
    # The SP runs sequentially:
    # MFC -> QGP -> Bronze -> Silver -> Gold views -> Validation
    #
    # Any unhandled SQL/SP error stops here and moves to the except block.
    # ==============================================================================================

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

    # Force the stored procedure to fully finish before continuing.
    result.collect()


    # ==============================================================================================
    # 2. GET VALIDATION RESULT FOR THIS EXACT EXECUTION
    #
    # Match using Job Run ID + Task Run ID instead of "latest run".
    # ==============================================================================================

    validation_rows = spark.sql(
        f"""
        SELECT
          validation_run_id,
          validation_run_ts,
          overall_status,
          metric_count,
          healthy_count,
          warning_count,
          failed_count,
          issue_summary,
          next_step_summary

        FROM {validation_runs_view}

        WHERE orchestration_job_run_id = :job_run_id
          AND orchestration_task_run_id = :task_run_id

        ORDER BY validation_run_ts DESC
        LIMIT 1
        """,
        args={
            "job_run_id": job_run_id,
            "task_run_id": task_run_id,
        },
    ).collect()


    # ==============================================================================================
    # 3. VALIDATION RESULT MUST EXIST
    # ==============================================================================================

    if not validation_rows:
        raise RuntimeError(
            "Pipeline completed but no validation result was found for "
            f"Job Run ID {job_run_id} / Task Run ID {task_run_id}."
        )


    validation = validation_rows[0]

    validation_run_id = validation["validation_run_id"]
    validation_status = (validation["overall_status"] or "").strip().upper()

    metric_count = validation["metric_count"]
    healthy_count = validation["healthy_count"]
    warning_count = validation["warning_count"]
    failed_count = validation["failed_count"]


    print("")
    print("Validation completed")
    print(f"Validation Run ID: {validation_run_id}")
    print(f"Validation Status: {validation_status}")
    print(f"Metric Count:      {metric_count}")
    print(f"Healthy:           {healthy_count}")
    print(f"Warnings:          {warning_count}")
    print(f"Failed:            {failed_count}")


    # ==============================================================================================
    # 4. STRUCTURAL CHECK - ALWAYS KEEP ENABLED
    #
    # A complete Pulse TMS validation execution must contain exactly 64 metrics.
    # ==============================================================================================

    if metric_count != EXPECTED_VALIDATION_METRICS:
        raise RuntimeError(
            "Pulse TMS validation is incomplete. "
            f"Expected {EXPECTED_VALIDATION_METRICS} metrics but found {metric_count}. "
            f"Validation Run ID: {validation_run_id}."
        )


    # ==============================================================================================
    # 5. VALID STATUS CHECK - ALWAYS KEEP ENABLED
    # ==============================================================================================

    if validation_status not in ("HEALTHY", "WARNING", "FAILED"):
        raise RuntimeError(
            "Pulse TMS validation returned an unexpected status. "
            f"Status: {validation_status}. "
            f"Validation Run ID: {validation_run_id}."
        )


    # ==============================================================================================
    # 6. OPTIONAL VALIDATION FAILURE GATE
    #
    # NORMAL AUTOMATED RUN:
    #   Keep this block UNCOMMENTED.
    #   FAILED validation -> Databricks task fails.
    #
    # MANUAL OVERRIDE / TROUBLESHOOTING:
    #   Comment out ONLY this block.
    #   The pipeline can complete even when validation status = FAILED.
    #
    # SQL/SP/runtime failures will STILL fail the notebook regardless.
    # ==============================================================================================

    if validation_status == "FAILED":
        raise RuntimeError(
            "Pulse TMS validation FAILED. "
            f"Validation Run ID: {validation_run_id}. "
            f"Failed metrics: {failed_count}. "
            f"Issues: {validation['issue_summary']}. "
            f"Next steps: {validation['next_step_summary']}"
        )


    # ==============================================================================================
    # 7. SUCCESS
    # ==============================================================================================

    end_time = datetime.now(timezone.utc)

    print("")
    print("Dashboard Pulse TMS orchestration completed successfully.")
    print(f"Job Run ID:        {job_run_id}")
    print(f"Validation Run ID: {validation_run_id}")
    print(f"Validation Status: {validation_status}")
    print(f"End UTC:           {end_time.isoformat()}")
    print(f"Duration:          {end_time - start_time}")


# --------------------------------------------------------------------------------------------------
# Any technical, orchestration, structural, or enabled validation-gate failure lands here.
# Re-raise so Databricks marks the task as FAILED.
# --------------------------------------------------------------------------------------------------

except Exception as error:

    end_time = datetime.now(timezone.utc)

    print("")
    print("Dashboard Pulse TMS orchestration FAILED.")
    print(f"Job Run ID: {job_run_id}")
    print(f"End UTC:    {end_time.isoformat()}")
    print(f"Duration:   {end_time - start_time}")
    print(f"Error:      {error}")

    raise