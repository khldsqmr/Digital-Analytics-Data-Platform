/* =============================================================================
SILVER: One passage per report/version/section/item, including negative feedback.
Only is_evidence_eligible=true is retrievable as positive historical evidence.
Explicit reviewer events remain in Gold Review Events; Silver holds a convenient
materialized classification refreshed by the single processing notebook.
============================================================================= */
CREATE TABLE IF NOT EXISTS prdrzranalytics.lab42.sdi_tbl_weeklyNarrative_silver_narrativeChunks_weekly (
 chunk_id               STRING NOT NULL COMMENT 'CHK_<sha256>: deterministic source_id, section ordinal, item ordinal; never reused.',
 source_id              STRING NOT NULL COMMENT 'FK-like reference to immutable Bronze row.',
 report_id              STRING NOT NULL COMMENT 'WN_YYYYMMDD_ALL from Bronze; logical reporting-week report.',
 version_id             STRING COMMENT 'App draft/revision ID; NULL for historical imports.',
 report_week_end        DATE NOT NULL COMMENT 'Saturday report period end.',
 source_type            STRING NOT NULL COMMENT 'HISTORICAL_IMPORT or APP_VERSION; copied from Bronze.',
 section_key            STRING NOT NULL COMMENT 'WHAT_HAPPENED / WHY_IT_HAPPENED / IS_IT_MEANINGFUL / RECOMMENDATIONS_AND_RISKS.',
 section_index          INT NOT NULL COMMENT 'Zero-based section ordinal in original source, used for stable chunk identity.',
 item_index             INT NOT NULL COMMENT 'Zero-based item ordinal within section.',
 chunk_text             STRING NOT NULL COMMENT 'Verbatim original item, never regenerated or rewritten by AI.',
 search_text            STRING NOT NULL COMMENT 'Search input: section label + original passage; derived deterministically.',
 metric_keys            ARRAY<STRING> COMMENT 'Candidate registry metric keys detected by controlled alias matching; never assumed verified.',
 tag_status             STRING NOT NULL COMMENT 'RULE_CANDIDATE or NOT_TAGGED; AI may later propose improved tags via reviewed job.',
 review_status          STRING NOT NULL COMMENT 'HISTORICAL_PUBLISHED / DRAFT / ACCEPTED / REJECTED / CHANGES_REQUESTED / PUBLISHED / SUPERSEDED, derived from events.',
 knowledge_role         STRING NOT NULL COMMENT 'APPROVED_EVIDENCE / NEGATIVE_EXAMPLE / PENDING_REVIEW / SUPERSEDED_EVIDENCE.',
 is_evidence_eligible   BOOLEAN NOT NULL COMMENT 'True ONLY for currently published source passages, not drafts or rejected content.',
 is_feedback_eligible   BOOLEAN NOT NULL COMMENT 'True for rejected/flagged section passages; may inform future feedback evaluation, not facts.',
 last_review_event_id   STRING COMMENT 'Most recent applicable reviewer event for this section, if any.',
 source_uri             STRING NOT NULL COMMENT 'Provenance pointer back to Bronze source origin.',
 created_at             TIMESTAMP NOT NULL COMMENT 'First insertion timestamp of this passage.',
 classified_at          TIMESTAMP NOT NULL COMMENT 'Most recent eligibility refresh timestamp.'
)
USING DELTA
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
COMMENT 'Track B single normalized knowledge store: all historical and app narratives with controlled evidence/feedback eligibility.';
