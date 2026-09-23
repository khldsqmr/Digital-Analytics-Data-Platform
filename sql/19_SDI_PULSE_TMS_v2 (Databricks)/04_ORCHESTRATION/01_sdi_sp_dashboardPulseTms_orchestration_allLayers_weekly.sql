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
     UPSTREAM MFC BRONZE
     =============================================================================================== */

  CALL prdrzranalytics.lab42.sdi_sp_mfc_bronze_spendActuals_weekly();

  CALL prdrzranalytics.lab42.sdi_sp_mfc_bronze_spendActualsGranular_weekly();

  CALL prdrzranalytics.lab42.sdi_sp_mfc_bronze_spendForecast_weekly();

  CALL prdrzranalytics.lab42.sdi_sp_mfc_bronze_spendForecastGranular_weekly();


  /* ===============================================================================================
     UPSTREAM MFC SILVER
     =============================================================================================== */

  CALL prdrzranalytics.lab42.sdi_sp_mfc_silver_spend_weekly();

  CALL prdrzranalytics.lab42.sdi_sp_mfc_silver_spendGranular_weekly();


  /* ===============================================================================================
     QGP ARCHIVE
     =============================================================================================== */

  CALL prdrzranalytics.lab42.sdi_sp_qgpArchive_orchestration_allLayers_weekly();


  /* ===============================================================================================
     DASHBOARD PULSE TMS BRONZE LAYER: 5 PROCEDURES
     =============================================================================================== */

  CALL prdrzranalytics.lab42.sdi_sp_dashboardPulseTms_bronze_adobeFunnel_weekly();

  CALL prdrzranalytics.lab42.sdi_sp_dashboardPulseTms_bronze_mfcSpend_weekly();

  CALL prdrzranalytics.lab42.sdi_sp_dashboardPulseTms_bronze_platformSpend_weekly();

  CALL prdrzranalytics.lab42.sdi_sp_dashboardPulseTms_bronze_qgp_weekly();

  CALL prdrzranalytics.lab42.sdi_sp_dashboardPulseTms_bronze_biddableSpend_weekly();


  /* ===============================================================================================
     DASHBOARD PULSE TMS SILVER LAYER: 5 PROCEDURES
     =============================================================================================== */

  CALL prdrzranalytics.lab42.sdi_sp_dashboardPulseTms_silver_adobeFunnel_weekly();

  CALL prdrzranalytics.lab42.sdi_sp_dashboardPulseTms_silver_mfcSpend_weekly();

  CALL prdrzranalytics.lab42.sdi_sp_dashboardPulseTms_silver_platformSpend_weekly();

  CALL prdrzranalytics.lab42.sdi_sp_dashboardPulseTms_silver_qgp_weekly();

  CALL prdrzranalytics.lab42.sdi_sp_dashboardPulseTms_silver_biddableSpend_weekly();


  /* ===============================================================================================
     DASHBOARD PULSE TMS DEPENDENT SILVER: UPV FORECAST

     This procedure must run after Silver Adobe Funnel because it uses prior-year channel allocation
     ratios from the Silver Adobe Funnel table.

     It also requires the external UPV Forecast Bronze upload.
     =============================================================================================== */

  CALL prdrzranalytics.lab42.sdi_sp_dashboardPulseTms_silver_upvForecast_weekly();


  /* ===============================================================================================
     DASHBOARD PULSE TMS GOLD LAYER: LIVE VIEWS

     No Gold procedure is required.

     At this point all underlying Silver tables have been refreshed, so the following views resolve
     automatically against the current Silver state:

       - sdi_vw_dashboardPulseTms_dim_qgp_calendar
       - sdi_vw_dashboardPulseTms_gold_unified_long
       - sdi_vw_dashboardPulseTms_gold_unified_wide
     =============================================================================================== */


  /* ===============================================================================================
     POST-RUN VALIDATION

     THIS MUST REMAIN THE FINAL CALLABLE STEP.

     The orchestration execution metadata received from the scheduler notebook is forwarded to the
     validation procedure.

     For a direct SQL call with no parameters, these values are NULL here and the validation
     procedure automatically generates its own PULSETMS_MAN_* execution identity.

     The validation SP:
       - creates the history table if it does not already exist
       - determines the reporting period
       - performs Source -> Bronze -> Silver -> Gold reconciliation
       - records Healthy / Warning / Failed
       - appends one validation snapshot
       - attaches this orchestration execution lineage
     =============================================================================================== */

  CALL prdrzranalytics.lab42.sdi_sp_dashboardPulseTms_validation_history_perRun
  (
    p_orchestration_run_type,
    p_orchestration_job_id,
    p_orchestration_job_run_id,
    p_orchestration_task_run_id,
    p_orchestration_execution_count
  );


END;



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