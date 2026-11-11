/* =================================================================================================
FILE:           01_sdi_sp_adobeFunnel_orchestration_allLayers_weekly.sql
LAYER:          Orchestration
CATALOG.SCHEMA: prdrzranalytics.lab42
PROCEDURE:      sdi_sp_adobeFunnel_orchestration_allLayers_weekly

PURPOSE:
  Runs the full weekly Adobe/BYOD funnel pipeline end to end in dependency order:

    1. Bronze
       - Runs 8 procedures.
       - The Bronze procedures have no interdependencies.
       - The execution order within the Bronze layer does not affect downstream processing.

    2. Silver
       - Runs 3 procedures.
       - Each Silver procedure depends on the required Bronze tables.
       - The supported grains are All Channels and LTC Groups.
       - The individual Last Touch Channel grain is outside the current scope.

    3. Gold
       - Runs 1 procedure.
       - The Gold procedure depends on all three Silver tables.

  Each procedure performs a full CREATE OR REPLACE TABLE rebuild.
  No incremental or MERGE logic is used, so the full pipeline can be rerun
  through this orchestration procedure.

EXECUTION ORDER:
  Bronze -> Silver -> Gold

USAGE:
  CALL prdrzranalytics.lab42.sdi_sp_adobeFunnel_orchestration_allLayers_weekly();

================================================================================================= */

CREATE OR REPLACE PROCEDURE
  prdrzranalytics.lab42.sdi_sp_adobeFunnel_orchestration_allLayers_weekly()
LANGUAGE SQL
SQL SECURITY INVOKER
MODIFIES SQL DATA
AS
BEGIN

  /* ===============================================================================================
     BRONZE LAYER: 8 PROCEDURES
     =============================================================================================== */

  CALL prdrzranalytics.lab42.sdi_sp_adobeFunnel_bronze_upvFunnelByAllChannels_weekly();
  CALL prdrzranalytics.lab42.sdi_sp_adobeFunnel_bronze_upvFunnelByLtcGroups_weekly();
  CALL prdrzranalytics.lab42.sdi_sp_adobeFunnel_bronze_upvTotalByAllChannels_weekly();
  CALL prdrzranalytics.lab42.sdi_sp_adobeFunnel_bronze_upvTotalByLtcGroups_weekly();
  CALL prdrzranalytics.lab42.sdi_sp_adobeFunnel_bronze_byodFlowEntryPagesByAllChannels_weekly();
  CALL prdrzranalytics.lab42.sdi_sp_adobeFunnel_bronze_byodFlowEntryPagesByLtcGroups_weekly();
  CALL prdrzranalytics.lab42.sdi_sp_adobeFunnel_bronze_byodFlowOutcomesByAllChannels_weekly();
  CALL prdrzranalytics.lab42.sdi_sp_adobeFunnel_bronze_byodFlowOutcomesByLtcGroups_weekly();

  /* ===============================================================================================
     SILVER LAYER: 3 PROCEDURES
     =============================================================================================== */

  CALL prdrzranalytics.lab42.sdi_sp_adobeFunnel_silver_flowPerformanceByLtcGroupsPlusAllChannels_weekly();
  CALL prdrzranalytics.lab42.sdi_sp_adobeFunnel_silver_byodFlowEntryPagesByLtcGroupsPlusAllChannels_weekly();
  CALL prdrzranalytics.lab42.sdi_sp_adobeFunnel_silver_byodFlowOutcomesByLtcGroupsPlusAllChannels_weekly();

  /* ===============================================================================================
     GOLD LAYER: 1 PROCEDURE
     =============================================================================================== */

  CALL prdrzranalytics.lab42.sdi_sp_adobeFunnel_gold_flowPerformanceByChannelGroups_weekly();

END;