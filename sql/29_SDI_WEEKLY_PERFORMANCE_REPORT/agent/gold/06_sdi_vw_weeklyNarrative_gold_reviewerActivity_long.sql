/* ============================================================================
 PROJECT      : Weekly Narrative | Databricks Unity Catalog | v7
 FILE         : 06_sdi_vw_weeklyNarrative_gold_reviewerActivity_long.sql
 LAYER        : agent/gold
 PURPOSE      : See object-level comments and README for schema/processing contract
 DEPENDENCIES : Deploy in documented layer order
 EXECUTION    : Use Databricks SQL / UC runtime
 OWNERSHIP    : Production writes only by governed job/service principal.
 GRAIN        : As defined by target object. All reporting weeks Sun-Sat.
 SAFETY       : Review/approval not inferred from LLM output. Retry-safe writer.
 ============================================================================ */
/* Manager-friendly read-only audit of all reviewer actions; nobody's vote is discarded. */
CREATE OR REPLACE VIEW prdrzranalytics.lab42.sdi_vw_weeklyNarrative_gold_reviewerActivity_long AS
SELECT e.report_id,e.week_end,e.version_id,s.version_number,
       e.section_key,e.review_action,e.reviewer_id,e.reviewer_name,
       e.review_comment,e.feedback_category,e.related_metric_keys,e.reviewed_at,
       s.report_status,e.review_event_id
FROM prdrzranalytics.lab42.sdi_tbl_weeklyNarrative_gold_reviewEvents_long e
JOIN prdrzranalytics.lab42.sdi_vw_weeklyNarrative_gold_reportStatus_wide s
 ON e.version_id=s.version_id;
