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


