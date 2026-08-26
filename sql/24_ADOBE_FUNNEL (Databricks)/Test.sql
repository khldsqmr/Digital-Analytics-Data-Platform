/* =================================================================================================
FILE:         sdi_cleanup_adobeFunnel_dashboardPulseByod_tables.sql
PURPOSE:
  One-time cleanup script. Drops every table these two pipelines could have created,
  across all naming iterations, so nothing orphaned/garbage is left in the catalog
  before a fresh CALL of the orchestrators.

  All statements are DROP TABLE IF EXISTS — safe to run even if a given table was
  never created (e.g. the 3 pulseByod sources that stayed commented out).

SECTIONS:
  1. adobeFunnel   — current names (sdi_tbl_adobeFunnel_...)
  2. dashboardPulseByod — current names (sdi_tbl_dashboardPulseByod_...)
  3. pulseByod (OLD, pre-rename) — orphaned tables from before the dashboardPulseByod
     rename. Only relevant if you ran the pipeline before that rename; harmless no-ops
     otherwise.

NOTE: This only drops TABLES. The PROCEDURES (sdi_sp_...) are not touched — re-running
      CREATE OR REPLACE PROCEDURE for each is what rebuilds these tables from scratch.
================================================================================================= */

-- ================================================================ 1. adobeFunnel — Bronze
DROP TABLE IF EXISTS prdrzranalytics.lab42.sdi_tbl_adobeFunnel_bronze_upvFunnelByAllChannel_weekly;
DROP TABLE IF EXISTS prdrzranalytics.lab42.sdi_tbl_adobeFunnel_bronze_upvFunnelByLtcGroups_weekly;
DROP TABLE IF EXISTS prdrzranalytics.lab42.sdi_tbl_adobeFunnel_bronze_upvTotalByAllChannel_weekly;
DROP TABLE IF EXISTS prdrzranalytics.lab42.sdi_tbl_adobeFunnel_bronze_upvTotalByLtcGroups_weekly;
DROP TABLE IF EXISTS prdrzranalytics.lab42.sdi_tbl_adobeFunnel_bronze_upvTotalByChannelGroups_weekly;  -- old pre-rename name, orphaned if it was ever created
DROP TABLE IF EXISTS prdrzranalytics.lab42.sdi_tbl_adobeFunnel_bronze_byodFlowEntryPagesByAllChannel_weekly;
DROP TABLE IF EXISTS prdrzranalytics.lab42.sdi_tbl_adobeFunnel_bronze_byodFlowEntryPagesByLtcGroups_weekly;
DROP TABLE IF EXISTS prdrzranalytics.lab42.sdi_tbl_adobeFunnel_bronze_byodFlowOutcomesByAllChannel_weekly;
DROP TABLE IF EXISTS prdrzranalytics.lab42.sdi_tbl_adobeFunnel_bronze_byodFlowOutcomesByLtcGroups_weekly;

-- ================================================================ 1. adobeFunnel — Silver
DROP TABLE IF EXISTS prdrzranalytics.lab42.sdi_tbl_adobeFunnel_silver_flowPerformanceByLtcGroupsPlusAllChannels_weekly;
DROP TABLE IF EXISTS prdrzranalytics.lab42.sdi_tbl_adobeFunnel_silver_flowPerformanceByChannelGroupsPlusAll_weekly;  -- old pre-rename name, orphaned if it was ever created
DROP TABLE IF EXISTS prdrzranalytics.lab42.sdi_tbl_adobeFunnel_silver_byodFlowEntryPagesByLtcGroupsPlusAllChannels_weekly;
DROP TABLE IF EXISTS prdrzranalytics.lab42.sdi_tbl_adobeFunnel_silver_byodFlowEntryPagesByChannelGroupsPlusAll_weekly;  -- old pre-rename name, orphaned if it was ever created
DROP TABLE IF EXISTS prdrzranalytics.lab42.sdi_tbl_adobeFunnel_silver_byodFlowOutcomesByLtcGroupsPlusAllChannels_weekly;
DROP TABLE IF EXISTS prdrzranalytics.lab42.sdi_tbl_adobeFunnel_silver_byodFlowOutcomesByChannelGroupsPlusAll_weekly;  -- old pre-rename name, orphaned if it was ever created

-- ================================================================ 1. adobeFunnel — Gold
DROP TABLE IF EXISTS prdrzranalytics.lab42.sdi_tbl_adobeFunnel_gold_flowPerformanceByChannelGroups_weekly;
DROP TABLE IF EXISTS prdrzranalytics.lab42.sdi_tbl_adobeFunnel_gold_flowPerformanceByLtcGroups_weekly;  -- briefly-live intermediate name, orphaned if it was ever created


-- ================================================================ 2. dashboardPulseByod — Bronze
DROP TABLE IF EXISTS prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_bronze_profound_weekly;
DROP TABLE IF EXISTS prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_bronze_profoundGofish_weekly;
DROP TABLE IF EXISTS prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_bronze_sa360Adgroup_daily;       -- source commented out, likely never created
DROP TABLE IF EXISTS prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_bronze_gscQuery_daily;           -- source commented out, likely never created
DROP TABLE IF EXISTS prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_bronze_googleTrends_weekly;      -- source commented out, likely never created

-- ================================================================ 2. dashboardPulseByod — Silver
DROP TABLE IF EXISTS prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_profound_weekly;
DROP TABLE IF EXISTS prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_profoundGofish_weekly;
DROP TABLE IF EXISTS prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_adobe_weekly;
DROP TABLE IF EXISTS prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_adobeByodEntryPages_weekly;
DROP TABLE IF EXISTS prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_adobeByodOutcomes_weekly;

-- ================================================================ 2. dashboardPulseByod — Gold
DROP TABLE IF EXISTS prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_gold_unified_wide;
DROP TABLE IF EXISTS prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_gold_unified_long;


-- ================================================================ 3. pulseByod (OLD, pre-rename) — orphaned if you ran this before the rename
DROP TABLE IF EXISTS prdrzranalytics.lab42.sdi_tbl_pulseByod_bronze_profound_weekly;
DROP TABLE IF EXISTS prdrzranalytics.lab42.sdi_tbl_pulseByod_bronze_profoundGofish_weekly;
DROP TABLE IF EXISTS prdrzranalytics.lab42.sdi_tbl_pulseByod_bronze_sa360Adgroup_daily;
DROP TABLE IF EXISTS prdrzranalytics.lab42.sdi_tbl_pulseByod_bronze_gscQuery_daily;
DROP TABLE IF EXISTS prdrzranalytics.lab42.sdi_tbl_pulseByod_bronze_googleTrends_weekly;
DROP TABLE IF EXISTS prdrzranalytics.lab42.sdi_tbl_pulseByod_silver_profound_weekly;
DROP TABLE IF EXISTS prdrzranalytics.lab42.sdi_tbl_pulseByod_silver_profoundGofish_weekly;
DROP TABLE IF EXISTS prdrzranalytics.lab42.sdi_tbl_pulseByod_silver_adobe_weekly;
DROP TABLE IF EXISTS prdrzranalytics.lab42.sdi_tbl_pulseByod_silver_adobeByodEntryPages_weekly;
DROP TABLE IF EXISTS prdrzranalytics.lab42.sdi_tbl_pulseByod_silver_adobeByodOutcomes_weekly;
DROP TABLE IF EXISTS prdrzranalytics.lab42.sdi_tbl_pulseByod_gold_unified_wide;
DROP TABLE IF EXISTS prdrzranalytics.lab42.sdi_tbl_pulseByod_gold_unified_long;


/* =================================================================================================
FILE:         sdi_cleanup_adobeFunnel_dashboardPulseByod_tables.sql
PURPOSE:
  One-time cleanup script. Drops every table these two pipelines could have created,
  across all naming iterations, so nothing orphaned/garbage is left in the catalog
  before a fresh CALL of the orchestrators.

  All statements are DROP TABLE IF EXISTS — safe to run even if a given table was
  never created (e.g. the 3 pulseByod sources that stayed commented out).

SECTIONS:
  1. adobeFunnel   — current names (sdi_tbl_adobeFunnel_...)
  2. dashboardPulseByod — current names (sdi_tbl_dashboardPulseByod_...)
  3. pulseByod (OLD, pre-rename) — orphaned tables from before the dashboardPulseByod
     rename. Only relevant if you ran the pipeline before that rename; harmless no-ops
     otherwise.

NOTE: This only drops TABLES. The PROCEDURES (sdi_sp_...) are not touched — re-running
      CREATE OR REPLACE PROCEDURE for each is what rebuilds these tables from scratch.
================================================================================================= */

-- ================================================================ 1. adobeFunnel — Bronze
DROP TABLE IF EXISTS prdrzranalytics.lab42.sdi_tbl_adobeFunnel_bronze_upvFunnelByAllChannels_weekly;
DROP TABLE IF EXISTS prdrzranalytics.lab42.sdi_tbl_adobeFunnel_bronze_upvFunnelByAllChannel_weekly;  -- old pre-rename (singular) name, orphaned if it was ever created
DROP TABLE IF EXISTS prdrzranalytics.lab42.sdi_tbl_adobeFunnel_bronze_upvFunnelByLtcGroups_weekly;
DROP TABLE IF EXISTS prdrzranalytics.lab42.sdi_tbl_adobeFunnel_bronze_upvTotalByAllChannels_weekly;
DROP TABLE IF EXISTS prdrzranalytics.lab42.sdi_tbl_adobeFunnel_bronze_upvTotalByAllChannel_weekly;  -- old pre-rename (singular) name, orphaned if it was ever created
DROP TABLE IF EXISTS prdrzranalytics.lab42.sdi_tbl_adobeFunnel_bronze_upvTotalByLtcGroups_weekly;
DROP TABLE IF EXISTS prdrzranalytics.lab42.sdi_tbl_adobeFunnel_bronze_upvTotalByChannelGroups_weekly;  -- old pre-rename name, orphaned if it was ever created
DROP TABLE IF EXISTS prdrzranalytics.lab42.sdi_tbl_adobeFunnel_bronze_byodFlowEntryPagesByAllChannels_weekly;
DROP TABLE IF EXISTS prdrzranalytics.lab42.sdi_tbl_adobeFunnel_bronze_byodFlowEntryPagesByAllChannel_weekly;  -- old pre-rename (singular) name, orphaned if it was ever created
DROP TABLE IF EXISTS prdrzranalytics.lab42.sdi_tbl_adobeFunnel_bronze_byodFlowEntryPagesByLtcGroups_weekly;
DROP TABLE IF EXISTS prdrzranalytics.lab42.sdi_tbl_adobeFunnel_bronze_byodFlowOutcomesByAllChannels_weekly;
DROP TABLE IF EXISTS prdrzranalytics.lab42.sdi_tbl_adobeFunnel_bronze_byodFlowOutcomesByAllChannel_weekly;  -- old pre-rename (singular) name, orphaned if it was ever created
DROP TABLE IF EXISTS prdrzranalytics.lab42.sdi_tbl_adobeFunnel_bronze_byodFlowOutcomesByLtcGroups_weekly;

-- ================================================================ 1. adobeFunnel — Silver
DROP TABLE IF EXISTS prdrzranalytics.lab42.sdi_tbl_adobeFunnel_silver_flowPerformanceByLtcGroupsPlusAllChannels_weekly;
DROP TABLE IF EXISTS prdrzranalytics.lab42.sdi_tbl_adobeFunnel_silver_flowPerformanceByChannelGroupsPlusAll_weekly;  -- old pre-rename name, orphaned if it was ever created
DROP TABLE IF EXISTS prdrzranalytics.lab42.sdi_tbl_adobeFunnel_silver_byodFlowEntryPagesByLtcGroupsPlusAllChannels_weekly;
DROP TABLE IF EXISTS prdrzranalytics.lab42.sdi_tbl_adobeFunnel_silver_byodFlowEntryPagesByChannelGroupsPlusAll_weekly;  -- old pre-rename name, orphaned if it was ever created
DROP TABLE IF EXISTS prdrzranalytics.lab42.sdi_tbl_adobeFunnel_silver_byodFlowOutcomesByLtcGroupsPlusAllChannels_weekly;
DROP TABLE IF EXISTS prdrzranalytics.lab42.sdi_tbl_adobeFunnel_silver_byodFlowOutcomesByChannelGroupsPlusAll_weekly;  -- old pre-rename name, orphaned if it was ever created

-- ================================================================ 1. adobeFunnel — Gold
DROP TABLE IF EXISTS prdrzranalytics.lab42.sdi_tbl_adobeFunnel_gold_flowPerformanceByChannelGroups_weekly;
DROP TABLE IF EXISTS prdrzranalytics.lab42.sdi_tbl_adobeFunnel_gold_flowPerformanceByLtcGroups_weekly;  -- briefly-live intermediate name, orphaned if it was ever created


-- ================================================================ 2. dashboardPulseByod — Bronze
DROP TABLE IF EXISTS prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_bronze_profound_weekly;
DROP TABLE IF EXISTS prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_bronze_profoundGofish_weekly;
DROP TABLE IF EXISTS prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_bronze_sa360Adgroup_daily;       -- source commented out, likely never created
DROP TABLE IF EXISTS prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_bronze_gscQuery_daily;           -- source commented out, likely never created
DROP TABLE IF EXISTS prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_bronze_googleTrends_weekly;      -- source commented out, likely never created

-- ================================================================ 2. dashboardPulseByod — Silver
DROP TABLE IF EXISTS prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_profound_weekly;
DROP TABLE IF EXISTS prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_profoundGofish_weekly;
DROP TABLE IF EXISTS prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_adobe_weekly;
DROP TABLE IF EXISTS prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_adobeByodEntryPages_weekly;
DROP TABLE IF EXISTS prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_adobeByodOutcomes_weekly;

-- ================================================================ 2. dashboardPulseByod — Gold
DROP TABLE IF EXISTS prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_gold_unified_wide;
DROP TABLE IF EXISTS prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_gold_unified_long;


-- ================================================================ 3. pulseByod (OLD, pre-rename) — orphaned if you ran this before the rename
DROP TABLE IF EXISTS prdrzranalytics.lab42.sdi_tbl_pulseByod_bronze_profound_weekly;
DROP TABLE IF EXISTS prdrzranalytics.lab42.sdi_tbl_pulseByod_bronze_profoundGofish_weekly;
DROP TABLE IF EXISTS prdrzranalytics.lab42.sdi_tbl_pulseByod_bronze_sa360Adgroup_daily;
DROP TABLE IF EXISTS prdrzranalytics.lab42.sdi_tbl_pulseByod_bronze_gscQuery_daily;
DROP TABLE IF EXISTS prdrzranalytics.lab42.sdi_tbl_pulseByod_bronze_googleTrends_weekly;
DROP TABLE IF EXISTS prdrzranalytics.lab42.sdi_tbl_pulseByod_silver_profound_weekly;
DROP TABLE IF EXISTS prdrzranalytics.lab42.sdi_tbl_pulseByod_silver_profoundGofish_weekly;
DROP TABLE IF EXISTS prdrzranalytics.lab42.sdi_tbl_pulseByod_silver_adobe_weekly;
DROP TABLE IF EXISTS prdrzranalytics.lab42.sdi_tbl_pulseByod_silver_adobeByodEntryPages_weekly;
DROP TABLE IF EXISTS prdrzranalytics.lab42.sdi_tbl_pulseByod_silver_adobeByodOutcomes_weekly;
DROP TABLE IF EXISTS prdrzranalytics.lab42.sdi_tbl_pulseByod_gold_unified_wide;
DROP TABLE IF EXISTS prdrzranalytics.lab42.sdi_tbl_pulseByod_gold_unified_long;