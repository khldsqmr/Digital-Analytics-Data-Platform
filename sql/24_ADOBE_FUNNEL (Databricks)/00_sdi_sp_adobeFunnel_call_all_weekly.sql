/* =================================================================================================
FILE:         00_sdi_sp_adobeFunnel_call_all_weekly.sql
LAYER:        Orchestrator (via Stored Procedure)
CATALOG.SCHEMA: prdrzranalytics.lab42
PROCEDURE:    sdi_sp_adobeFunnel_call_all_weekly

PURPOSE:
  Runs the full Adobe/BYOD funnel pipeline end to end, in dependency order:
    1. Bronze  (8 procedures — no interdependencies, order among them doesn't matter)
    2. Silver  (3 procedures — each depends only on Bronze; grain is ByAllChannel + ByLtcGroups/
                ByChannelGroups only — the singular last-touch-Channel grain is out of scope)
    3. Gold    (1 procedure — depends on all 3 Silver tables above)

  Every procedure called here does a full CREATE OR REPLACE TABLE rebuild on each run
  (no incremental/MERGE logic), so this orchestrator can simply be re-run in full each time.

USAGE:
  CALL `prdrzranalytics.lab42.sdi_sp_adobeFunnel_call_all_weekly`();
================================================================================================= */

CREATE OR REPLACE PROCEDURE
`prdrzranalytics.lab42.sdi_sp_adobeFunnel_call_all_weekly`()
LANGUAGE SQL
MODIFIES SQL DATA
AS
BEGIN

  -- ================================================================ BRONZE (8)
  CALL `prdrzranalytics.lab42.sdi_sp_adobeFunnel_bronze_upvFunnelByAllChannel_weekly`();
  CALL `prdrzranalytics.lab42.sdi_sp_adobeFunnel_bronze_upvFunnelByLtcGroups_weekly`();
  CALL `prdrzranalytics.lab42.sdi_sp_adobeFunnel_bronze_upvTotalByAllChannel_weekly`();
  CALL `prdrzranalytics.lab42.sdi_sp_adobeFunnel_bronze_upvTotalByChannelGroups_weekly`();
  CALL `prdrzranalytics.lab42.sdi_sp_adobeFunnel_bronze_byodFlowEntryPagesByAllChannel_weekly`();
  CALL `prdrzranalytics.lab42.sdi_sp_adobeFunnel_bronze_byodFlowEntryPagesByLtcGroups_weekly`();
  CALL `prdrzranalytics.lab42.sdi_sp_adobeFunnel_bronze_byodFlowOutcomesByAllChannel_weekly`();
  CALL `prdrzranalytics.lab42.sdi_sp_adobeFunnel_bronze_byodFlowOutcomesByLtcGroups_weekly`();

  -- ================================================================ SILVER (3 of 4 — Silver 01 not yet built, see KNOWN GAP / PURPOSE above)
  CALL `prdrzranalytics.lab42.sdi_sp_adobeFunnel_silver_flowPerformanceByChannelGroupsPlusAll_weekly`();
  CALL `prdrzranalytics.lab42.sdi_sp_adobeFunnel_silver_byodFlowEntryPagesByChannelGroupsPlusAll_weekly`();
  CALL `prdrzranalytics.lab42.sdi_sp_adobeFunnel_silver_byodFlowOutcomesByChannelGroupsPlusAll_weekly`();

  -- ================================================================ GOLD (1)
  CALL `prdrzranalytics.lab42.sdi_sp_adobeFunnel_gold_flowPerformanceByChannelGroups_weekly`();

END;