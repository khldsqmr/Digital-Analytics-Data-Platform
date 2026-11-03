/* Only safe positive retrieval results. The read-path MUST filter this view.
   Publication changes trigger a subsequent APP_SYNC refresh to update eligibility.
   For strict no-stale-evidence enforcement, also validate the publication event
   at query time if app-sync jobs can lag. */
CREATE OR REPLACE VIEW prdrzranalytics.lab42.sdi_vw_weeklyNarrative_silver_eligibleEvidence_long AS
SELECT chunk_id,source_id,report_id,version_id,report_week_end,section_key,
       chunk_text,search_text,metric_keys,source_type,source_uri,created_at
FROM prdrzranalytics.lab42.sdi_tbl_weeklyNarrative_silver_narrativeChunks_weekly
WHERE is_evidence_eligible = TRUE AND knowledge_role = 'APPROVED_EVIDENCE';
