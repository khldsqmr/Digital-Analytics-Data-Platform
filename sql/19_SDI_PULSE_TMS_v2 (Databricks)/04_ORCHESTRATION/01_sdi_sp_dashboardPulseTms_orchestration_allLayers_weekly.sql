/* =================================================================================================
FILE:           sdi_sp_dashboardPulseTms_orchestration_allLayers_weekly.sql
LAYER:          Orchestration
CATALOG.SCHEMA: prdrzranalytics.lab42
PROCEDURE:      sdi_sp_dashboardPulseTms_orchestration_allLayers_weekly

PURPOSE:
  Runs the full Dashboard Pulse TMS pipeline end to end in dependency order and then performs
  one post-run validation snapshot.

EXECUTION MODES:

  DATABRICKS JOB:
    The scheduler notebook passes:
      - orchestration_run_type
      - Databricks Job ID
      - Databricks Job Run ID
      - Databricks Task Run ID
      - Task execution count

  INTERACTIVE NOTEBOOK:
    The scheduler notebook generates a PULSETMS_MAN_* execution ID and passes it into this SP.

  DIRECT SQL CALL:
    All parameters have DEFAULT NULL, so this remains valid:

      CALL prdrzranalytics.lab42.sdi_sp_dashboardPulseTms_orchestration_allLayers_weekly();

    In that case the validation SP automatically creates a PULSETMS_MAN_* lineage ID.

PIPELINE EXECUTION ORDER:

  0. External UPV Forecast Bronze notebook upload
       - Must complete successfully before this orchestration procedure runs.
       - There is no CALL for that notebook inside this procedure.

  1. Upstream MFC Bronze
       - sdi_sp_mfc_bronze_spendActuals_weekly
       - sdi_sp_mfc_bronze_spendActualsGranular_weekly
       - sdi_sp_mfc_bronze_spendForecast_weekly
       - sdi_sp_mfc_bronze_spendForecastGranular_weekly

  2. Upstream MFC Silver
       - sdi_sp_mfc_silver_spend_weekly
       - sdi_sp_mfc_silver_spendGranular_weekly

  3. QGP Archive
       - sdi_sp_qgpArchive_orchestration_allLayers_weekly

  4. Dashboard Pulse TMS Bronze
       - adobeFunnel
       - mfcSpend
       - platformSpend
       - qgp
       - biddableSpend

  5. Dashboard Pulse TMS Silver
       - adobeFunnel
       - mfcSpend
       - platformSpend
       - qgp
       - biddableSpend

  6. Dependent Silver
       - upvForecast
       - Must run after Silver Adobe Funnel.

  7. Gold
       - No Gold stored procedure is called.
       - Gold objects are live views:
           sdi_vw_dashboardPulseTms_dim_qgp_calendar
           sdi_vw_dashboardPulseTms_gold_unified_long
           sdi_vw_dashboardPulseTms_gold_unified_wide

  8. Post-run Validation
       - sdi_sp_dashboardPulseTms_validation_history_perRun
       - This MUST remain the final callable step.
       - One validation snapshot is appended for each completed orchestration execution.

USAGE:

  From scheduler notebook:
    Parameters are supplied automatically.

  Direct manual SQL:
    CALL prdrzranalytics.lab42.sdi_sp_dashboardPulseTms_orchestration_allLayers_weekly();

================================================================================================= */

CREATE OR REPLACE PROCEDURE
  prdrzranalytics.lab42.sdi_sp_dashboardPulseTms_orchestration_allLayers_weekly
(
  IN p_orchestration_run_type STRING DEFAULT NULL,
  IN p_orchestration_job_id STRING DEFAULT NULL,
  IN p_orchestration_job_run_id STRING DEFAULT NULL,
  IN p_orchestration_task_run_id STRING DEFAULT NULL,
  IN p_orchestration_execution_count INT DEFAULT NULL
)

LANGUAGE SQL
SQL SECURITY INVOKER
MODIFIES SQL DATA

AS
BEGIN

  /* ===============================================================================================
     EXTERNAL DEPENDENCY: UPV FORECAST BRONZE DATA

     The external notebook that populates the UPV Forecast Bronze table must run successfully
     before this orchestration procedure starts.

     There is no CALL for that notebook here.

     The dependency must remain configured in the Databricks Job by making the orchestration
     notebook task depend on the UPV Forecast Bronze upload task.
     =============================================================================================== */


  /* ===============================================================================================
     1. UPSTREAM MFC BRONZE
     =============================================================================================== */

  CALL prdrzranalytics.lab42.sdi_sp_mfc_bronze_spendActuals_weekly();

  CALL prdrzranalytics.lab42.sdi_sp_mfc_bronze_spendActualsGranular_weekly();

  CALL prdrzranalytics.lab42.sdi_sp_mfc_bronze_spendForecast_weekly();

  CALL prdrzranalytics.lab42.sdi_sp_mfc_bronze_spendForecastGranular_weekly();


  /* ===============================================================================================
     2. UPSTREAM MFC SILVER
     =============================================================================================== */

  CALL prdrzranalytics.lab42.sdi_sp_mfc_silver_spend_weekly();

  CALL prdrzranalytics.lab42.sdi_sp_mfc_silver_spendGranular_weekly();


  /* ===============================================================================================
     3. QGP ARCHIVE
     =============================================================================================== */

  CALL prdrzranalytics.lab42.sdi_sp_qgpArchive_orchestration_allLayers_weekly();


  /* ===============================================================================================
     4. DASHBOARD PULSE TMS BRONZE LAYER
     =============================================================================================== */

  CALL prdrzranalytics.lab42.sdi_sp_dashboardPulseTms_bronze_adobeFunnel_weekly();

  CALL prdrzranalytics.lab42.sdi_sp_dashboardPulseTms_bronze_mfcSpend_weekly();

  CALL prdrzranalytics.lab42.sdi_sp_dashboardPulseTms_bronze_platformSpend_weekly();

  CALL prdrzranalytics.lab42.sdi_sp_dashboardPulseTms_bronze_qgp_weekly();

  CALL prdrzranalytics.lab42.sdi_sp_dashboardPulseTms_bronze_biddableSpend_weekly();


  /* ===============================================================================================
     5. DASHBOARD PULSE TMS SILVER LAYER
     =============================================================================================== */

  CALL prdrzranalytics.lab42.sdi_sp_dashboardPulseTms_silver_adobeFunnel_weekly();

  CALL prdrzranalytics.lab42.sdi_sp_dashboardPulseTms_silver_mfcSpend_weekly();

  CALL prdrzranalytics.lab42.sdi_sp_dashboardPulseTms_silver_platformSpend_weekly();

  CALL prdrzranalytics.lab42.sdi_sp_dashboardPulseTms_silver_qgp_weekly();

  CALL prdrzranalytics.lab42.sdi_sp_dashboardPulseTms_silver_biddableSpend_weekly();


  /* ===============================================================================================
     6. DASHBOARD PULSE TMS DEPENDENT SILVER: UPV FORECAST

     Must run after Silver Adobe Funnel because it uses prior-year channel allocation ratios
     from the Silver Adobe Funnel table.

     It also requires the external UPV Forecast Bronze upload.
     =============================================================================================== */

  CALL prdrzranalytics.lab42.sdi_sp_dashboardPulseTms_silver_upvForecast_weekly();


  /* ===============================================================================================
     7. DASHBOARD PULSE TMS GOLD LAYER: LIVE VIEWS

     No Gold procedure is required.

     At this point all underlying Silver tables have been refreshed, so the following views resolve
     automatically against the latest Silver state:

       - sdi_vw_dashboardPulseTms_dim_qgp_calendar
       - sdi_vw_dashboardPulseTms_gold_unified_long
       - sdi_vw_dashboardPulseTms_gold_unified_wide
     =============================================================================================== */


  /* ===============================================================================================
     8. POST-RUN VALIDATION

     THIS MUST REMAIN THE FINAL CALLABLE STEP.

     The orchestration execution metadata received from the scheduler notebook is forwarded to the
     validation procedure.

     IMPORTANT:
       Procedure input parameters are explicitly qualified with this orchestration procedure name.
       This prevents Databricks from attempting to resolve them as columns.

     For a direct SQL call with no parameters:
       - the orchestration parameters are NULL
       - the validation procedure receives NULL
       - the validation procedure automatically generates PULSETMS_MAN_* lineage

     The validation SP:
       - creates the history table if it does not already exist
       - determines the latest completed reporting period
       - performs Source -> Bronze -> Silver -> Gold reconciliation
       - records Healthy / Warning / Failed
       - appends one validation snapshot
       - attaches orchestration execution lineage
     =============================================================================================== */

  CALL prdrzranalytics.lab42.sdi_sp_dashboardPulseTms_validation_history_perRun
  (
    sdi_sp_dashboardPulseTms_orchestration_allLayers_weekly.p_orchestration_run_type,
    sdi_sp_dashboardPulseTms_orchestration_allLayers_weekly.p_orchestration_job_id,
    sdi_sp_dashboardPulseTms_orchestration_allLayers_weekly.p_orchestration_job_run_id,
    sdi_sp_dashboardPulseTms_orchestration_allLayers_weekly.p_orchestration_task_run_id,
    sdi_sp_dashboardPulseTms_orchestration_allLayers_weekly.p_orchestration_execution_count
  );


END;



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