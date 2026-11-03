/* =====================================================================================
 PROJECT : Weekly Narrative v8 | TRACK B / GOLD
 FILE    : 03_sdi_vw_weeklyNarrative_gold_retrievalEligible_long.sql
 PURPOSE : Narrow read contract over the physical Gold table for Agent fallback.
 NOTE    : Imported historical source availability is UNKNOWN when original_available_at
           is NULL. Use report_week_end only for historical ordering, not as-of backtest proof.
 ===================================================================================== */
CREATE OR REPLACE VIEW prdrzranalytics.lab42.sdi_vw_weeklyNarrative_gold_retrievalEligible_long AS
SELECT chunk_id,source_id,report_id,version_id,report_week_end,section_key,
       chunk_text,embedding_text,metric_keys,source_type,source_uri,
       original_available_at,ingested_at AS source_available_at,ingested_at AS created_at
FROM prdrzranalytics.lab42.sdi_tbl_weeklyNarrative_gold_approvedEvidence_long;
