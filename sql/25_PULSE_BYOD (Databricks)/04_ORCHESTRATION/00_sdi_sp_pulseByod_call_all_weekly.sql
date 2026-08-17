/* =================================================================================================
FILE:         00_sdi_sp_pulseByod_call_all_weekly.sql
LAYER:        Orchestrator (via Stored Procedure)
CATALOG.SCHEMA: prdrzranalytics.lab42
PROCEDURE:    sdi_sp_pulseByod_call_all_weekly

PURPOSE:
  Runs the full pulseByod pipeline end to end, in dependency order:
    0. adobeFunnel pipeline (external dependency — sdi_sp_adobeFunnel_call_all_weekly)
       Three of the five active pulseByod Silver procedures (adobe, adobeByodEntryPages,
       adobeByodOutcomes) read from adobeFunnel's Silver tables, so that pipeline must be
       fresh before this one runs. Called first for that reason.
    1. Bronze  (2 active — profound, profoundGofish. sa360Adgroup/gscQuery/googleTrends
                are commented out in their own files pending source-table confirmation and
                are NOT called here.)
    2. Silver  (5 active — profound, profoundGofish, adobe, adobeByodEntryPages,
                adobeByodOutcomes. sa360/gsc/googleTrends Silver were never provided and do
                not exist yet.)
    3. Gold    (2 — unified_wide, unified_long. Both read only the 5 active Silver tables;
                sa360/gsc/googleTrends sections are commented out inside each.)

  Every procedure called here does a full CREATE OR REPLACE TABLE rebuild on each run
  (no incremental/MERGE logic), so this orchestrator can simply be re-run in full each time.

USAGE:
  CALL prdrzranalytics.lab42.sdi_sp_pulseByod_call_all_weekly();
================================================================================================= */

CREATE OR REPLACE PROCEDURE
prdrzranalytics.lab42.sdi_sp_pulseByod_call_all_weekly()
LANGUAGE SQL
SQL SECURITY INVOKER
MODIFIES SQL DATA
AS
BEGIN

  -- ================================================================ EXTERNAL DEPENDENCY
  CALL prdrzranalytics.lab42.sdi_sp_adobeFunnel_call_all_weekly();

  -- ================================================================ BRONZE (2 active)
  CALL prdrzranalytics.lab42.sdi_sp_pulseByod_bronze_profound_weekly();
  CALL prdrzranalytics.lab42.sdi_sp_pulseByod_bronze_profoundGofish_weekly();
  -- CALL prdrzranalytics.lab42.sdi_sp_pulseByod_bronze_sa360Adgroup_daily();     -- commented out in its own file
  -- CALL prdrzranalytics.lab42.sdi_sp_pulseByod_bronze_gscQuery_daily();        -- commented out in its own file
  -- CALL prdrzranalytics.lab42.sdi_sp_pulseByod_bronze_googleTrends_weekly();   -- commented out in its own file

  -- ================================================================ SILVER (5 active)
  CALL prdrzranalytics.lab42.sdi_sp_pulseByod_silver_profound_weekly();
  CALL prdrzranalytics.lab42.sdi_sp_pulseByod_silver_profoundGofish_weekly();
  CALL prdrzranalytics.lab42.sdi_sp_pulseByod_silver_adobe_weekly();
  CALL prdrzranalytics.lab42.sdi_sp_pulseByod_silver_adobeByodEntryPages_weekly();
  CALL prdrzranalytics.lab42.sdi_sp_pulseByod_silver_adobeByodOutcomes_weekly();

  -- ================================================================ GOLD (2)
  CALL prdrzranalytics.lab42.sdi_sp_pulseByod_gold_unified_wide();
  CALL prdrzranalytics.lab42.sdi_sp_pulseByod_gold_unified_long();

END;