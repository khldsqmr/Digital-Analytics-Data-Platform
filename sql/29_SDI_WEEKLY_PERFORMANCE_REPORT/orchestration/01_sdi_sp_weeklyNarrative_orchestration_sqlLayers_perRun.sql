/* ============================================================================
 PROJECT      : Weekly Narrative | Databricks Unity Catalog | v7
 FILE         : x
 LAYER        : ORCHESTRATION
 PURPOSE      : SQL-only deterministic tasks Track A + Track B incl. physical Gold; AI notebooks are Workflow tasks
 DEPENDENCIES : Deployed SQL layer SPs
 EXECUTION    : CALL (mode,week_end,path)
 OWNERSHIP    : Production writes only by governed job/service principal.
 GRAIN        : As defined by target object. All reporting weeks Sun-Sat.
 SAFETY       : Review/approval not inferred from LLM output. Retry-safe writer.
 ============================================================================ */

CREATE OR REPLACE PROCEDURE prdrzranalytics.lab42.sdi_sp_weeklyNarrative_orchestration_sqlLayers_perRun(
 IN p_mode STRING, IN p_week_end DATE DEFAULT NULL, IN p_historical_path STRING DEFAULT NULL
)
LANGUAGE SQL SQL SECURITY INVOKER MODIFIES SQL DATA
AS BEGIN
  IF p_mode='HISTORICAL_IMPORT' THEN
    CALL prdrzranalytics.lab42.sdi_sp_weeklyNarrative_bronze_narrativeBank_perRun('HISTORICAL_IMPORT',p_historical_path);
    CALL prdrzranalytics.lab42.sdi_sp_weeklyNarrative_silver_narrativeChunks_perRun(NULL,FALSE);
    CALL prdrzranalytics.lab42.sdi_sp_weeklyNarrative_gold_approvedEvidence_perRun('REFRESH',NULL,NULL);
  ELSEIF p_mode='WEEKLY_REFRESH' THEN
    CALL prdrzranalytics.lab42.sdi_sp_weeklyNarrative_gold_factsHistory_long(p_week_end,'JOB',NULL);
    CALL prdrzranalytics.lab42.sdi_sp_weeklyNarrative_bronze_narrativeBank_perRun('APP_SYNC',NULL);
    CALL prdrzranalytics.lab42.sdi_sp_weeklyNarrative_silver_narrativeChunks_perRun(NULL,TRUE);
    CALL prdrzranalytics.lab42.sdi_sp_weeklyNarrative_gold_approvedEvidence_perRun('REFRESH',NULL,NULL);
  ELSEIF p_mode='APP_SYNC' THEN
    CALL prdrzranalytics.lab42.sdi_sp_weeklyNarrative_bronze_narrativeBank_perRun('APP_SYNC',NULL);
    CALL prdrzranalytics.lab42.sdi_sp_weeklyNarrative_silver_narrativeChunks_perRun(NULL,TRUE);
    CALL prdrzranalytics.lab42.sdi_sp_weeklyNarrative_gold_approvedEvidence_perRun('REFRESH',NULL,NULL);
  ELSEIF p_mode='REPROCESS' THEN
    CALL prdrzranalytics.lab42.sdi_sp_weeklyNarrative_silver_narrativeChunks_perRun(NULL,TRUE);
    CALL prdrzranalytics.lab42.sdi_sp_weeklyNarrative_gold_approvedEvidence_perRun('REFRESH',NULL,NULL);
  ELSE
    SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='Unsupported orchestration mode';
  END IF;
END;
