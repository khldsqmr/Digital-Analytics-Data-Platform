/* ============================================================================
 PROJECT      : Weekly Narrative | Databricks Unity Catalog | v7
 FILE         : 05_sdi_vw_weeklyNarrative_gold_reportStatus_wide.sql
 LAYER        : agent/gold
 PURPOSE      : See object-level comments and README for schema/processing contract
 DEPENDENCIES : Deploy in documented layer order
 EXECUTION    : Use Databricks SQL / UC runtime
 OWNERSHIP    : Production writes only by governed job/service principal.
 GRAIN        : As defined by target object. All reporting weeks Sun-Sat.
 SAFETY       : Review/approval not inferred from LLM output. Retry-safe writer.
 ============================================================================ */
/* ==================================================================================================
 FILE : 35_sdi_vw_weeklyNarrative_gold_reportStatus_wide.sql
 PURPOSE:
   - Preserve the latest vote PER PERSON PER SECTION. Never let John ACCEPT overwrite Sara REJECT.
   - A section with both ACCEPT and REJECT/REQUEST_CHANGES is REVIEW_CONFLICT.
   - Publish only when all four sections have >=1 ACCEPT and zero unresolved negative votes.
   - A reviewer can change their own vote; a new event supersedes only THEIR old vote.
   - New draft/version starts from zero votes. Parent approvals do not carry forward.
================================================================================================== */
CREATE OR REPLACE VIEW prdrzranalytics.lab42.sdi_vw_weeklyNarrative_gold_reportStatus_wide AS
WITH reviewer_latest AS (
 SELECT e.*,
   ROW_NUMBER() OVER (
     PARTITION BY e.version_id,e.section_key,e.reviewer_id
     ORDER BY e.reviewed_at DESC,e.review_event_id DESC
   ) AS rn
 FROM prdrzranalytics.lab42.sdi_tbl_weeklyNarrative_gold_reviewEvents_long e
 WHERE e.section_key IN ('WHAT_HAPPENED','WHY_IT_HAPPENED','IS_IT_MEANINGFUL','RECOMMENDATIONS_AND_RISKS')
   AND e.review_action IN ('ACCEPT','REJECT','REQUEST_CHANGES')
), section_votes AS (
 SELECT version_id,section_key,
   SUM(IF(review_action='ACCEPT',1,0)) AS accept_votes,
   SUM(IF(review_action='REJECT',1,0)) AS reject_votes,
   SUM(IF(review_action='REQUEST_CHANGES',1,0)) AS changes_votes,
   COUNT(*) AS reviewer_votes
 FROM reviewer_latest WHERE rn=1 GROUP BY version_id,section_key
), section_rollup AS (
 SELECT version_id,
   COUNT(*) AS reviewed_sections,
   SUM(IF(accept_votes>0 AND reject_votes=0 AND changes_votes=0,1,0)) AS accepted_sections,
   SUM(IF(reject_votes>0,1,0)) AS rejected_sections,
   SUM(IF(changes_votes>0,1,0)) AS flagged_sections,
   SUM(IF(accept_votes>0 AND (reject_votes>0 OR changes_votes>0),1,0)) AS conflicted_sections,
   SUM(IF(accept_votes=0 AND (reject_votes>0 OR changes_votes>0),1,0)) AS blocked_sections
 FROM section_votes GROUP BY version_id
), published AS (
 SELECT e.report_id,e.version_id,e.reviewed_at AS published_at,e.review_event_id,
   ROW_NUMBER() OVER (PARTITION BY e.report_id ORDER BY e.reviewed_at DESC,e.review_event_id DESC) AS publication_rank
 FROM prdrzranalytics.lab42.sdi_tbl_weeklyNarrative_gold_reviewEvents_long e
 WHERE e.section_key='REPORT' AND e.review_action='PUBLISH'
), ordered AS (
 SELECT version_id,ROW_NUMBER() OVER (PARTITION BY report_id ORDER BY created_at,version_id) AS version_number
 FROM prdrzranalytics.lab42.sdi_tbl_weeklyNarrative_gold_reportVersions_wide
)
SELECT v.version_id,v.report_id,v.week_end,v.report_scope,o.version_number,
       v.parent_version_id,v.investigation_id,v.author_type,
       v.created_by_user_id,v.created_by_display_name,
       COALESCE(s.reviewed_sections,0) AS reviewed_sections,
       COALESCE(s.accepted_sections,0) AS accepted_sections,
       COALESCE(s.rejected_sections,0) AS rejected_sections,
       COALESCE(s.flagged_sections,0) AS flagged_sections,
       COALESCE(s.conflicted_sections,0) AS conflicted_sections,
       p.published_at,
       CASE
         WHEN p.version_id IS NOT NULL AND p.publication_rank>1 THEN 'SUPERSEDED'
         WHEN p.version_id IS NOT NULL THEN 'PUBLISHED'
         WHEN COALESCE(s.conflicted_sections,0)>0 THEN 'REVIEW_CONFLICT'
         WHEN COALESCE(s.rejected_sections,0)>0 THEN 'REJECTED'
         WHEN COALESCE(s.flagged_sections,0)>0 THEN 'CHANGES_REQUESTED'
         WHEN COALESCE(s.accepted_sections,0)=4 THEN 'APPROVED'
         WHEN COALESCE(s.reviewed_sections,0)>0 THEN 'IN_REVIEW'
         ELSE 'DRAFT' END AS report_status,
       v.created_at
FROM prdrzranalytics.lab42.sdi_tbl_weeklyNarrative_gold_reportVersions_wide v
JOIN ordered o ON v.version_id=o.version_id
LEFT JOIN section_rollup s ON v.version_id=s.version_id
LEFT JOIN published p ON v.version_id=p.version_id AND v.report_id=p.report_id;
