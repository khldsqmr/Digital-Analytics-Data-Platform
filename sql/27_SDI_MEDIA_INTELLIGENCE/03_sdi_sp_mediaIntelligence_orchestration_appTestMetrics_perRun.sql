/* =================================================================================================
FILE:           sdi_sp_mediaIntelligence_orchestration_appTestMetrics_perRun.sql
LAYER:          Orchestration
CATALOG.SCHEMA: prdrzranalytics.lab42
PROCEDURE:      sdi_sp_mediaIntelligence_orchestration_appTestMetrics_perRun

PURPOSE:
  Runs the Media Intelligence application test Gold refresh in dependency order.

CURRENT SCOPE:

  1. Gold Long
       - sdi_sp_mediaIntelligence_gold_appTestMetrics_long

  2. Gold Wide
       - sdi_sp_mediaIntelligence_gold_appTestMetrics_wide

UPSTREAM DEPENDENCIES:

  This procedure does NOT refresh the existing PulseTMS pipeline.

  The following existing PulseTMS Silver tables must already be refreshed:

    - prdrzranalytics.lab42.sdi_tbl_dashboardPulseTms_silver_adobeFunnel_weekly
    - prdrzranalytics.lab42.sdi_tbl_dashboardPulseTms_silver_mfcSpend_weekly

EXECUTION ORDER:

  1. Long
  2. Wide

NOTES:

  - Long is the primary application / API-facing dataset.
  - Wide is primarily for validation and sense-checking.
  - Wide does not technically depend on Long, but sequential execution keeps
    the initial application-test orchestration simple and deterministic.
  - If Long fails, execution stops and Wide does not run.
  - If Wide fails, the orchestration execution fails.

USAGE:

  CALL prdrzranalytics.lab42
    .sdi_sp_mediaIntelligence_orchestration_appTestMetrics_perRun();

================================================================================================= */


CREATE OR REPLACE PROCEDURE
  prdrzranalytics.lab42.sdi_sp_mediaIntelligence_orchestration_appTestMetrics_perRun()

LANGUAGE SQL

SQL SECURITY INVOKER

MODIFIES SQL DATA

COMMENT 'Runs the Media Intelligence application test Gold Long and Wide refresh procedures in dependency order.'

AS

BEGIN


  /* ===============================================================================================
     1. MEDIA INTELLIGENCE GOLD LONG

     Primary application / API-facing dataset.
     =============================================================================================== */

  CALL prdrzranalytics.lab42
    .sdi_sp_mediaIntelligence_gold_appTestMetrics_long();



  /* ===============================================================================================
     2. MEDIA INTELLIGENCE GOLD WIDE

     Validation / sense-check dataset.
     =============================================================================================== */

  CALL prdrzranalytics.lab42
    .sdi_sp_mediaIntelligence_gold_appTestMetrics_wide();


END;