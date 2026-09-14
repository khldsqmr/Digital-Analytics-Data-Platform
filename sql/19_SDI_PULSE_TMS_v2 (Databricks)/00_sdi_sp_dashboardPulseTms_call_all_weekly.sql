/* =================================================================================================
FILE:         00_sdi_sp_dashboardPulseTms_call_all_weekly.sql   (Databricks port of 00_call_all_sp_pulseTms)
PURPOSE:
  Weekly orchestration entry point. Calls every PulseTMS Bronze and Silver procedure in
  dependency order so the whole pipeline refreshes with one job run.

  NOT included here: sdi_vw_dashboardPulseTms_dim_qgp_calendar, sdi_vw_dashboardPulseTms_gold_unified_long,
  sdi_vw_dashboardPulseTms_gold_unified_wide -- these are VIEWS, not materialized by a procedure.
  They resolve live off whatever is currently in the Silver tables, so they never need a CALL here.

HOW TO SCHEDULE:
  Databricks Jobs > Tasks > Type = SQL > SQL task = File > Source = Workspace, point at this file,
  select a serverless or pro SQL warehouse. One task, one file, statements run top-to-bottom in
  the order below -- no separate task dependency graph needed at this data volume.

  If you'd rather parallelize the independent Bronze calls (or get per-step retry/monitoring),
  split this into one Job task per CALL instead and wire up "depends on" edges in the Jobs UI
  matching the ordering below. Not necessary yet at this scale, but the option's there later.

DEPENDENCY ORDER:
  1. Bronze: adobeFunnel, mfcSpend, platformSpend, qgp -- no dependency on each other, any order
     is fine. platformSpend stays commented out until its raw source catalog/schema is confirmed.
  2. Silver: adobeFunnel, mfcSpend, platformSpend, qgp -- each depends only on its own Bronze
     table plus the calendar view (a view, always live, no CALL needed). qgp has no cross-Silver
     dependency (unlike upvForecast below), so it can run right alongside the others in this step.
  3. Silver: upvForecast -- depends on Silver adobeFunnel specifically (prior-year channel
     allocation ratios), so it must run strictly after step 2, not alongside it. Also depends on
     its own Bronze table being populated by the external notebook upload mentioned in that
     procedure's header -- make sure that's run before this job, or upvForecast will compute
     against stale/missing Bronze data.
================================================================================================= */

-- ===========================================================================
-- STEP 1: Bronze
-- ===========================================================================
CALL prdrzranalytics.lab42.sdi_sp_dashboardPulseTms_bronze_adobeFunnel_weekly();
CALL prdrzranalytics.lab42.sdi_sp_dashboardPulseTms_bronze_mfcSpend_weekly();
--CALL prdrzranalytics.lab42.sdi_sp_dashboardPulseTms_bronze_platformSpend_weekly();
CALL prdrzranalytics.lab42.sdi_sp_dashboardPulseTms_bronze_qgp_weekly();

-- ===========================================================================
-- STEP 2: Silver -- adobeFunnel, mfcSpend, and qgp are already translated;
--         platformSpend Silver hasn't been ported yet, left commented until it is.
-- ===========================================================================
CALL prdrzranalytics.lab42.sdi_sp_dashboardPulseTms_silver_adobeFunnel_weekly();
CALL prdrzranalytics.lab42.sdi_sp_dashboardPulseTms_silver_mfcSpend_weekly();
-- CALL prdrzranalytics.lab42.sdi_sp_dashboardPulseTms_silver_platformSpend_weekly();
CALL prdrzranalytics.lab42.sdi_sp_dashboardPulseTms_silver_qgp_weekly();

-- ===========================================================================
-- STEP 3: Silver upvForecast -- must come after Silver adobeFunnel above.
--         Not yet translated/deployed -- left commented until it is.
-- ===========================================================================
-- CALL prdrzranalytics.lab42.sdi_sp_dashboardPulseTms_silver_upvForecast_weekly();