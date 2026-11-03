/* Compatibility adapter for the Agent generation notebook. This is a VIEW,
   not an additional Silver table. Keeps one Silver data store.
   source_available_at for imported historical chunks uses ingestion time;
   backtests must not assume 2026 historical publication availability. */
CREATE OR REPLACE VIEW prdrzranalytics.lab42.sdi_vw_weeklyNarrative_silver_retrievalEligible_long AS
SELECT s.chunk_id,s.source_id,s.report_id,s.version_id,s.report_week_end,
       s.section_key,s.chunk_text,s.search_text AS embedding_text,
       s.metric_keys,s.source_type,s.source_uri,
       b.original_available_at, b.ingested_at AS source_available_at,
       s.created_at
FROM prdrzranalytics.lab42.sdi_tbl_weeklyNarrative_silver_narrativeChunks_weekly s
JOIN prdrzranalytics.lab42.sdi_tbl_weeklyNarrative_bronze_narrativeBank_weekly b ON s.source_id=b.source_id
WHERE s.is_evidence_eligible=TRUE AND s.knowledge_role='APPROVED_EVIDENCE';
