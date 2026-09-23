/* =================================================================================================
FILE:           sdi_sp_dashboardPulseTms_orchestration_allLayers_weekly.sql
LAYER:          Orchestration
CATALOG.SCHEMA: prdrzranalytics.lab42
PROCEDURE:      sdi_sp_dashboardPulseTms_orchestration_allLayers_weekly

PURPOSE:
  Runs the full weekly Dashboard Pulse TMS pipeline end to end in dependency order:

    0. External dependency: UPV Forecast Bronze data
       - The UPV Forecast Bronze table is populated by an external notebook upload.
       - That notebook must complete successfully before this orchestration procedure runs.
       - The Silver UPV Forecast procedure also depends on the refreshed Silver Adobe Funnel
         table for prior-year channel allocation ratios.

    1. Bronze
       - Runs 5 procedures:
           - adobeFunnel
           - mfcSpend
           - platformSpend
           - qgp
           - biddableSpend
       - These Bronze procedures do not depend on one another.
       - They are executed sequentially by this orchestration procedure.

    2. Silver
       - Runs 5 independent Silver procedures:
           - adobeFunnel
           - mfcSpend
           - platformSpend
           - qgp
           - biddableSpend
       - Each Silver procedure depends on its corresponding Bronze table and any required
         live reference views.

    3. Dependent Silver
       - Runs 1 additional Silver procedure:
           - upvForecast
       - This procedure must run after Silver adobeFunnel because it uses prior-year
         channel allocation ratios from the Silver Adobe Funnel table.
       - It also requires the UPV Forecast Bronze data populated by the external notebook.

    4. Gold
       - No Gold stored procedures are called.
       - The following Gold objects are views:
           - sdi_vw_dashboardPulseTms_dim_qgp_calendar
           - sdi_vw_dashboardPulseTms_gold_unified_long
           - sdi_vw_dashboardPulseTms_gold_unified_wide
       - These views resolve dynamically from the current Silver tables and therefore
         do not require materialization or an explicit CALL.

  Each Bronze and Silver procedure performs its configured table-refresh operation.
  The complete callable portion of the pipeline can be rerun through this orchestration
  procedure.

EXECUTION ORDER:
  External UPV Forecast Bronze upload
    -> Dashboard Pulse TMS Bronze
    -> Independent Dashboard Pulse TMS Silver
    -> Silver UPV Forecast
    -> Live Gold views

USAGE:
  CALL prdrzranalytics.lab42.sdi_sp_dashboardPulseTms_orchestration_allLayers_weekly();

================================================================================================= */

CREATE OR REPLACE PROCEDURE
  prdrzranalytics.lab42.sdi_sp_dashboardPulseTms_orchestration_allLayers_weekly()

LANGUAGE SQL
SQL SECURITY INVOKER
MODIFIES SQL DATA

AS
BEGIN

  /* ===============================================================================================
     EXTERNAL DEPENDENCY: UPV FORECAST BRONZE DATA

     The external notebook that populates the UPV Forecast Bronze table must run successfully
     before this orchestration procedure starts. There is no CALL for that notebook here.
     =============================================================================================== */

  -- ============================================================
  -- BRONZE — must run first (Silver reads from these tables)
  -- ============================================================
  CALL prdrzranalytics.lab42.sdi_sp_mfc_bronze_spendActuals_weekly();
  CALL prdrzranalytics.lab42.sdi_sp_mfc_bronze_spendActualsGranular_weekly();
  CALL prdrzranalytics.lab42.sdi_sp_mfc_bronze_spendForecast_weekly();
  CALL prdrzranalytics.lab42.sdi_sp_mfc_bronze_spendForecastGranular_weekly();

  -- ============================================================
  -- SILVER — must run after Bronze (Gold reads from these tables)
  -- ============================================================
  CALL prdrzranalytics.lab42.sdi_sp_mfc_silver_spend_weekly();
  CALL prdrzranalytics.lab42.sdi_sp_mfc_silver_spendGranular_weekly();

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
     DASHBOARD PULSE TMS SILVER LAYER: 5 INDEPENDENT PROCEDURES
     =============================================================================================== */

  CALL prdrzranalytics.lab42.sdi_sp_dashboardPulseTms_silver_adobeFunnel_weekly();

  CALL prdrzranalytics.lab42.sdi_sp_dashboardPulseTms_silver_mfcSpend_weekly();

  CALL prdrzranalytics.lab42.sdi_sp_dashboardPulseTms_silver_platformSpend_weekly();

  CALL prdrzranalytics.lab42.sdi_sp_dashboardPulseTms_silver_qgp_weekly();

  CALL prdrzranalytics.lab42.sdi_sp_dashboardPulseTms_silver_biddableSpend_weekly();


  /* ===============================================================================================
     DASHBOARD PULSE TMS DEPENDENT SILVER LAYER: 1 PROCEDURE

     This procedure must run after Silver adobeFunnel.
     It also requires the UPV Forecast Bronze data populated by the external notebook.
     =============================================================================================== */

  CALL prdrzranalytics.lab42.sdi_sp_dashboardPulseTms_silver_upvForecast_weekly();

  /* ===============================================================================================
     POST-RUN VALIDATION

     IMPORTANT:
       This must remain AFTER every Bronze/Silver refresh.

       Gold is live, so calling validation here immediately reads the final current Gold state.

       One row set is appended to:
         sdi_tbl_dashboardPulseTms_validation_history_perRun

       for every orchestration execution.
     =============================================================================================== */

  -- CALL prdrzranalytics.lab42.sdi_sp_dashboardPulseTms_validation_history_perRun();

  /* ===============================================================================================
     DASHBOARD PULSE TMS GOLD LAYER: LIVE VIEWS

     No Gold procedures are called because the Gold objects are live views that resolve
     automatically from the refreshed Silver tables:
       - sdi_vw_dashboardPulseTms_gold_unified_long
       - sdi_vw_dashboardPulseTms_gold_unified_wide
     =============================================================================================== */

END;
