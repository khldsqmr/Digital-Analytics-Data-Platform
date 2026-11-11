/* =================================================================================================
FILE:         sdi_sp_adobeFunnel_call_all_weekly.sql
LAYER:        Orchestrator (via Stored Procedure)
CATALOG.SCHEMA: prdrzranalytics.lab42
PROCEDURE:    sdi_sp_adobeFunnel_call_all_weekly

PURPOSE:
  Runs the full Adobe/BYOD funnel pipeline end to end, in dependency order:
    1. Bronze  (10 procedures — no interdependencies, order among them doesn't matter)
                 8 funnel + BYOD procedures, plus 2 TFB/Metro procedures
    2. Silver  (3 procedures — each depends only on Bronze; grain is ByAllChannel + ByLtcGroups/
                ByChannelGroups only — the singular last-touch-Channel grain is out of scope)
                Silver 01 (flowPerformance) now also reads the 2 TFB/Metro Bronze tables.
    3. Gold    (1 procedure — depends on all 3 Silver tables above)

  Every procedure called here does a full CREATE OR REPLACE TABLE rebuild on each run
  (no incremental/MERGE logic), so this orchestrator can simply be re-run in full each time.

  FAILURE BEHAVIOUR: the calls run in sequence and there is no error handler, so the first procedure
  that fails stops the run and nothing after it executes. Because Silver 01 and Gold come last, a failure in
  any Bronze procedure (including the 2 TFB/Metro ones) leaves Silver and Gold at their previous refresh.
  Run the pre-flight in 00_deploy_and_validate.sql before the first scheduled run.

USAGE:
  CALL prdrzranalytics.lab42.sdi_sp_adobeFunnel_call_all_weekly();
================================================================================================= */

CREATE OR REPLACE PROCEDURE
prdrzranalytics.lab42.sdi_sp_adobeFunnel_call_all_weekly()
LANGUAGE SQL
SQL SECURITY INVOKER
MODIFIES SQL DATA
AS
BEGIN

  -- ================================================================ BRONZE (10)
  CALL prdrzranalytics.lab42.sdi_sp_adobeFunnel_bronze_upvFunnelByAllChannels_weekly();
  CALL prdrzranalytics.lab42.sdi_sp_adobeFunnel_bronze_upvFunnelByLtcGroups_weekly();
  CALL prdrzranalytics.lab42.sdi_sp_adobeFunnel_bronze_upvTotalByAllChannels_weekly();
  CALL prdrzranalytics.lab42.sdi_sp_adobeFunnel_bronze_upvTotalByLtcGroups_weekly();
  CALL prdrzranalytics.lab42.sdi_sp_adobeFunnel_bronze_byodFlowEntryPagesByAllChannels_weekly();
  CALL prdrzranalytics.lab42.sdi_sp_adobeFunnel_bronze_byodFlowEntryPagesByLtcGroups_weekly();
  CALL prdrzranalytics.lab42.sdi_sp_adobeFunnel_bronze_byodFlowOutcomesByAllChannels_weekly();
  CALL prdrzranalytics.lab42.sdi_sp_adobeFunnel_bronze_byodFlowOutcomesByLtcGroups_weekly();
  -- NEW: TFB + Metro (read by Silver 01)
  CALL prdrzranalytics.lab42.sdi_sp_adobeFunnel_bronze_tfbMetroByAllChannels_weekly();
  CALL prdrzranalytics.lab42.sdi_sp_adobeFunnel_bronze_tfbMetroByLtcGroups_weekly();

  -- ================================================================ SILVER (3)
  CALL prdrzranalytics.lab42.sdi_sp_adobeFunnel_silver_flowPerformanceByLtcGroupsPlusAllChannels_weekly();
  CALL prdrzranalytics.lab42.sdi_sp_adobeFunnel_silver_byodFlowEntryPagesByLtcGroupsPlusAllChannels_weekly();
  CALL prdrzranalytics.lab42.sdi_sp_adobeFunnel_silver_byodFlowOutcomesByLtcGroupsPlusAllChannels_weekly();

  -- ================================================================ GOLD (1)
  CALL prdrzranalytics.lab42.sdi_sp_adobeFunnel_gold_flowPerformanceByChannelGroups_weekly();

END;