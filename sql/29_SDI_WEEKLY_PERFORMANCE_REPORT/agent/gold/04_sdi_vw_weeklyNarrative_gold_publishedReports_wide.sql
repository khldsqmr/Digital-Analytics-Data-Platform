/* ============================================================================
 PROJECT      : Weekly Narrative | Databricks Unity Catalog | v7
 FILE         : 04_sdi_vw_weeklyNarrative_gold_publishedReports_wide.sql
 LAYER        : agent/gold
 PURPOSE      : See object-level comments and README for schema/processing contract
 DEPENDENCIES : Deploy in documented layer order
 EXECUTION    : Use Databricks SQL / UC runtime
 OWNERSHIP    : Production writes only by governed job/service principal.
 GRAIN        : As defined by target object. All reporting weeks Sun-Sat.
 SAFETY       : Review/approval not inferred from LLM output. Retry-safe writer.
 ============================================================================ */
/* Only last explicitly published version for each logical report_id; publication
   worker verifies approvals BEFORE appending PUBLISH. */
CREATE OR REPLACE VIEW prdrzranalytics.lab42.sdi_vw_weeklyNarrative_gold_publishedReports_wide AS
WITH Pub AS (
 SELECT v.*,e.reviewed_at AS published_at,e.reviewer_id AS published_by_user_id,
  e.reviewer_name AS published_by_display_name,
  ROW_NUMBER() OVER(PARTITION BY v.report_id ORDER BY e.reviewed_at DESC,e.review_event_id DESC) AS rn
 FROM prdrzranalytics.lab42.sdi_tbl_weeklyNarrative_gold_reportVersions_wide v
 JOIN prdrzranalytics.lab42.sdi_tbl_weeklyNarrative_gold_reviewEvents_long e
  ON e.version_id=v.version_id AND e.report_id=v.report_id
 WHERE e.review_action='PUBLISH' AND e.section_key='REPORT'
)
SELECT version_id,report_id,week_end,report_scope,investigation_id,sections_json,
 published_at,published_by_user_id,published_by_display_name
FROM Pub WHERE rn=1;
