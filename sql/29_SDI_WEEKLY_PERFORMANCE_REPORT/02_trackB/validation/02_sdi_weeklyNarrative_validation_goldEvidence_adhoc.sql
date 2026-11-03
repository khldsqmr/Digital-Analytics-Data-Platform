/* =================================================================================
 PROJECT : Weekly Narrative v8 | TRACK B / VALIDATION
 FILE    : 02_sdi_weeklyNarrative_validation_goldEvidence_adhoc.sql
 PURPOSE : Verify physical Gold evidence exactly tracks Silver published eligibility.
 EXPECT  : First three queries return 0 rows; fourth is a distribution for inspection.
 ================================================================================= */
-- Gold must not contain currently ineligible Silver chunks.
SELECT g.chunk_id,g.report_week_end,g.version_id
FROM prdrzranalytics.lab42.sdi_tbl_weeklyNarrative_gold_approvedEvidence_long g
LEFT JOIN prdrzranalytics.lab42.sdi_tbl_weeklyNarrative_silver_narrativeChunks_weekly s
  ON s.chunk_id=g.chunk_id AND s.is_evidence_eligible=TRUE AND s.knowledge_role='APPROVED_EVIDENCE'
WHERE s.chunk_id IS NULL;

-- Every eligible Silver chunk should have a materialized Gold row.
SELECT s.chunk_id,s.report_week_end
FROM prdrzranalytics.lab42.sdi_tbl_weeklyNarrative_silver_narrativeChunks_weekly s
LEFT JOIN prdrzranalytics.lab42.sdi_tbl_weeklyNarrative_gold_approvedEvidence_long g ON s.chunk_id=g.chunk_id
WHERE s.is_evidence_eligible=TRUE AND s.knowledge_role='APPROVED_EVIDENCE' AND g.chunk_id IS NULL;

-- No duplicate physical Gold evidence keys.
SELECT chunk_id,count(*) n FROM prdrzranalytics.lab42.sdi_tbl_weeklyNarrative_gold_approvedEvidence_long
GROUP BY chunk_id HAVING count(*)>1;

-- Enrichment coverage, by week.
SELECT report_week_end,count(*) evidence_passages,
       count_if(last_enriched_at IS NOT NULL) ai_enriched_passages,
       count_if(last_enriched_at IS NULL) pending_enrichment
FROM prdrzranalytics.lab42.sdi_tbl_weeklyNarrative_gold_approvedEvidence_long
GROUP BY report_week_end ORDER BY report_week_end DESC;
