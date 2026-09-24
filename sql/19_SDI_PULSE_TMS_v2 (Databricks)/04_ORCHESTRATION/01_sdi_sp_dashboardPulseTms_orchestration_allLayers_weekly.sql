/* =================================================================================================
FILE:           sdi_sp_dashboardPulseTms_orchestration_allLayers_weekly.sql
LAYER:          Orchestration
CATALOG.SCHEMA: prdrzranalytics.lab42
PROCEDURE:      sdi_sp_dashboardPulseTms_orchestration_allLayers_weekly

PURPOSE:
  Runs the Dashboard Pulse TMS pipeline in dependency order and performs post-run validation
  as the final callable step.

EXECUTION FLOW:

  External UPV Forecast Bronze Job Task
      ↓
  Upstream MFC Bronze
      ↓
  Upstream MFC Silver
      ↓
  QGP Archive
      ↓
  Pulse TMS Bronze
      ↓
  Pulse TMS Silver
      ↓
  UPV Forecast Silver
      ↓
  Gold live views
      ↓
  Post-run validation

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
     0. EXTERNAL DEPENDENCY

     UPV Forecast Bronze is populated by an external notebook.

     That notebook must run successfully before the scheduler notebook calls this procedure.
     The Databricks Job dependency controls this.
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
     4. DASHBOARD PULSE TMS BRONZE
     =============================================================================================== */

  CALL prdrzranalytics.lab42.sdi_sp_dashboardPulseTms_bronze_adobeFunnel_weekly();

  CALL prdrzranalytics.lab42.sdi_sp_dashboardPulseTms_bronze_mfcSpend_weekly();

  CALL prdrzranalytics.lab42.sdi_sp_dashboardPulseTms_bronze_platformSpend_weekly();

  CALL prdrzranalytics.lab42.sdi_sp_dashboardPulseTms_bronze_qgp_weekly();

  CALL prdrzranalytics.lab42.sdi_sp_dashboardPulseTms_bronze_biddableSpend_weekly();


  /* ===============================================================================================
     5. DASHBOARD PULSE TMS SILVER
     =============================================================================================== */

  CALL prdrzranalytics.lab42.sdi_sp_dashboardPulseTms_silver_adobeFunnel_weekly();

  CALL prdrzranalytics.lab42.sdi_sp_dashboardPulseTms_silver_mfcSpend_weekly();

  CALL prdrzranalytics.lab42.sdi_sp_dashboardPulseTms_silver_platformSpend_weekly();

  CALL prdrzranalytics.lab42.sdi_sp_dashboardPulseTms_silver_qgp_weekly();

  CALL prdrzranalytics.lab42.sdi_sp_dashboardPulseTms_silver_biddableSpend_weekly();


  /* ===============================================================================================
     6. DEPENDENT SILVER: UPV FORECAST

     Runs after Silver Adobe Funnel because UPV Forecast uses Adobe allocation information.
     =============================================================================================== */

  CALL prdrzranalytics.lab42.sdi_sp_dashboardPulseTms_silver_upvForecast_weekly();


  /* ===============================================================================================
     7. GOLD

     Gold is implemented as live views, so no Gold procedure is required.

       sdi_vw_dashboardPulseTms_dim_qgp_calendar
       sdi_vw_dashboardPulseTms_gold_unified_long
       sdi_vw_dashboardPulseTms_gold_unified_wide

     At this point those views resolve against the newly refreshed Silver state.
     =============================================================================================== */


  /* ===============================================================================================
     8. POST-RUN VALIDATION

     IMPORTANT:
       This must remain the final callable step.

     Pass the orchestration execution metadata received from the scheduler notebook into the
     validation procedure.

     Procedure parameters are qualified with the current procedure name so Databricks resolves
     them as routine parameters rather than columns.
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