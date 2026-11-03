/* ============================================================================
 PROJECT      : Weekly Narrative | Databricks Unity Catalog | v7
 FILE         : 01_sdi_weeklyNarrative_validation_narrativeChecks_adhoc.sql
 LAYER        : trackB/validation
 PURPOSE      : See object-level comments and README for schema/processing contract
 DEPENDENCIES : Deploy in documented layer order
 EXECUTION    : Use Databricks SQL / UC runtime
 OWNERSHIP    : Production writes only by governed job/service principal.
 GRAIN        : As defined by target object. All reporting weeks Sun-Sat.
 SAFETY       : Review/approval not inferred from LLM output. Retry-safe writer.
 ============================================================================ */
/* TRACK B smoke checks. Execute after HISTORICAL_IMPORT and after APP_SYNC. */
SELECT source_type,COUNT(*) AS raw_reports
FROM prdrzranalytics.lab42.sdi_tbl_weeklyNarrative_bronze_narrativeBank_weekly
GROUP BY source_type ORDER BY source_type;
SELECT source_type,knowledge_role,COUNT(*) AS passages
FROM prdrzranalytics.lab42.sdi_tbl_weeklyNarrative_silver_narrativeChunks_weekly
GROUP BY 1,2 ORDER BY 1,2;
/* Each query below should return zero rows. */
SELECT source_id,COUNT(*) AS duplicate_count
FROM prdrzranalytics.lab42.sdi_tbl_weeklyNarrative_bronze_narrativeBank_weekly
GROUP BY 1 HAVING COUNT(*)>1;
SELECT chunk_id,COUNT(*) AS duplicate_count
FROM prdrzranalytics.lab42.sdi_tbl_weeklyNarrative_silver_narrativeChunks_weekly
GROUP BY 1 HAVING COUNT(*)>1;
SELECT chunk_id,report_week_end FROM prdrzranalytics.lab42.sdi_tbl_weeklyNarrative_silver_narrativeChunks_weekly
WHERE DAYOFWEEK(report_week_end)<>7;
SELECT chunk_id,knowledge_role FROM prdrzranalytics.lab42.sdi_tbl_weeklyNarrative_silver_narrativeChunks_weekly
WHERE is_evidence_eligible AND knowledge_role<>'APPROVED_EVIDENCE';
SELECT s.chunk_id FROM prdrzranalytics.lab42.sdi_tbl_weeklyNarrative_silver_narrativeChunks_weekly s
LEFT JOIN prdrzranalytics.lab42.sdi_tbl_weeklyNarrative_bronze_narrativeBank_weekly b ON s.source_id=b.source_id
WHERE b.source_id IS NULL;
/* Historical source expectation for the supplied initial JSONL is 11 / 87. */
SELECT COUNT(DISTINCT source_id) AS imported_reports,COUNT(*) AS imported_passages
FROM prdrzranalytics.lab42.sdi_tbl_weeklyNarrative_silver_narrativeChunks_weekly
WHERE source_type='HISTORICAL_IMPORT';
