/* =================================================================================================
FILE:           04_sdi_sp_biddProgrammaticArchive_orchestration_allLayers_weekly.sql
PLATFORM:       Databricks
PROJECT:        biddProgrammaticArchive (Biddable Programmatic spend archive)
LAYER:          Orchestration
CATALOG.SCHEMA: prdrzranalytics.lab42
PROCEDURE:      sdi_sp_biddProgrammaticArchive_orchestration_allLayers_weekly

PURPOSE:
  Runs the Biddable Programmatic archive end to end, in order:

    1. sdi_sp_biddProgrammaticArchive_bronze_snapshot_weekly   -> ..._bronze_retained_weekly
    2. sdi_sp_biddProgrammaticArchive_silver_conformed_weekly  -> ..._silver_conformed_weekly
    3. sdi_sp_biddProgrammaticArchive_gold_curated_weekly      -> ..._gold_curated_weekly

  Called by sdi_sp_dashboardPulseTms_orchestration_allLayers_weekly right after the QGP archive
  and BEFORE PulseTMS Bronze, so PulseTMS Biddable Bronze always reads a freshly refreshed,
  complete Programmatic history (same cross-pipeline pattern as the QGP archive).

FAILURE BEHAVIOR:
  Any step that fails stops the run (and therefore the PulseTMS run that called it), so PulseTMS
  never builds Biddable from a half-refreshed archive. Bronze never deletes anything before its
  insert succeeds, so a failed run can simply be re-run.

================================================================================================= */

CREATE OR REPLACE PROCEDURE
  prdrzranalytics.lab42.sdi_sp_biddProgrammaticArchive_orchestration_allLayers_weekly()

LANGUAGE SQL
SQL SECURITY INVOKER
MODIFIES SQL DATA
COMMENT 'Biddable Programmatic archive orchestration: Bronze (retained) -> Silver (conformed) -> Gold (curated). Called by PulseTMS orchestration before PulseTMS Bronze.'

AS
BEGIN

  CALL prdrzranalytics.lab42.sdi_sp_biddProgrammaticArchive_bronze_snapshot_weekly();

  CALL prdrzranalytics.lab42.sdi_sp_biddProgrammaticArchive_silver_conformed_weekly();

  CALL prdrzranalytics.lab42.sdi_sp_biddProgrammaticArchive_gold_curated_weekly();

END;