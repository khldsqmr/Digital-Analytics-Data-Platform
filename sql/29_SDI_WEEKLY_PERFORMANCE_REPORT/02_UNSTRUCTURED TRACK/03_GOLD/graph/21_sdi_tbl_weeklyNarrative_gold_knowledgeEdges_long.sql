/* PHASE 2 | GRAPH ASSERTIONS. These are time-scoped claims with preserved source evidence.
 Source-reported relationships must never be represented as proven causality. */
CREATE TABLE IF NOT EXISTS prdrzranalytics.lab42.sdi_tbl_weeklyNarrative_gold_knowledgeEdges_long (
  edge_id STRING NOT NULL COMMENT 'Stable hash of claim type, entities, week and evidence source.',
  source_node_id STRING NOT NULL COMMENT 'Canonical subject entity.',
  relationship_type STRING NOT NULL COMMENT 'Governed vocabulary e.g. REPORTED_DRIVER_OF.',
  target_node_id STRING NOT NULL COMMENT 'Canonical related entity.',
  report_week_end DATE COMMENT 'Saturday of historical report where assertion was made.',
  evidence_chunk_id STRING NOT NULL COMMENT 'Exact supporting Silver narrative passage ID.',
  claim_text STRING NOT NULL COMMENT 'Source-faithful wording that can be audited.',
  confidence_score DOUBLE COMMENT 'Extractor confidence, not independent verification of cause.',
  verification_status STRING NOT NULL COMMENT 'PENDING_REVIEW, APPROVED, REJECTED.',
  extraction_model_id STRING COMMENT 'Extraction model/rule version used to propose claim.',
  reviewer_id STRING COMMENT 'Reviewer stable ID, only when explicitly reviewed.',
  reviewer_name STRING COMMENT 'Reviewer display name at graph review time.',
  reviewed_at TIMESTAMP COMMENT 'Time graph assertion review occurred.',
  created_at TIMESTAMP NOT NULL COMMENT 'Original assertion insertion timestamp.'
) USING DELTA COMMENT 'Time-scoped evidence-backed business relationship assertions.';
