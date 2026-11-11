/* ============================================================================
 PROJECT      : Weekly Narrative | Databricks Unity Catalog | v7.1
 FILE         : 02_sdi_sp_weeklyNarrative_gold_knowledgeEdges_perRun.sql
 LAYER        : KNOWLEDGE GRAPH / GOLD
 PURPOSE      : Create and populate graph Edges from validated JSON proposals
 DEPENDENCIES : Knowledge Nodes; Track B Silver chunks (is_evidence_eligible = TRUE)
 EXECUTION    : CALL (JSON array of proposals from governed AI notebook)
 OWNERSHIP    : Production writes only by governed job/service principal.
 GRAIN        : One row per edge_id (claim type, entities, week, evidence chunk).
 SAFETY       : Review/approval not inferred from LLM output. Retry-safe writer.
 CHANGE (7.1) : Evidence is now validated against Silver (is_evidence_eligible = TRUE);
                the Track B Gold approvedEvidence table no longer exists.
                Temp view removed (it referenced the procedure parameter).
 ============================================================================ */
CREATE OR REPLACE PROCEDURE prdrzranalytics.lab42.sdi_sp_weeklyNarrative_gold_knowledgeEdges_perRun(IN p_proposals_json STRING)
LANGUAGE SQL SQL SECURITY INVOKER MODIFIES SQL DATA
COMMENT 'Validates AI edge proposals against existing nodes and eligible Silver evidence chunks; inserts new edges as PENDING_REVIEW. Insert-only.'
AS BEGIN
  DECLARE v_bad BIGINT DEFAULT 0;

  CREATE TABLE IF NOT EXISTS prdrzranalytics.lab42.sdi_tbl_weeklyNarrative_gold_knowledgeEdges_long (
    edge_id STRING NOT NULL COMMENT 'Stable hash of claim type, entities, week and evidence source.',
    source_node_id STRING NOT NULL COMMENT 'Canonical subject entity.',
    relationship_type STRING NOT NULL COMMENT 'Governed vocabulary e.g. REPORTED_DRIVER_OF.',
    target_node_id STRING NOT NULL COMMENT 'Canonical related entity.',
    report_week_end DATE COMMENT 'Saturday of historical report where assertion was made.',
    evidence_chunk_id STRING NOT NULL COMMENT 'Exact supporting Silver narrative passage ID (sdi_tbl_weeklyNarrative_silver_narrativeChunks_weekly.chunk_id).',
    claim_text STRING NOT NULL COMMENT 'Source-faithful wording that can be audited.',
    confidence_score DOUBLE COMMENT 'Extractor confidence, not independent verification of cause.',
    verification_status STRING NOT NULL COMMENT 'PENDING_REVIEW, APPROVED, REJECTED.',
    extraction_model_id STRING COMMENT 'Extraction model/rule version used to propose claim.',
    reviewer_id STRING COMMENT 'Reviewer stable ID, only when explicitly reviewed.',
    reviewer_name STRING COMMENT 'Reviewer display name at graph review time.',
    reviewed_at TIMESTAMP COMMENT 'Time graph assertion review occurred.',
    created_at TIMESTAMP NOT NULL COMMENT 'Original assertion insertion timestamp.'
  ) USING DELTA COMMENT 'Time-scoped evidence-backed business relationship assertions. Trust is decided at read time by joining to Silver eligibility (see knowledgeEdgesActive view).';

  IF p_proposals_json IS NULL OR trim(p_proposals_json)='' THEN
    SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='JSON proposal batch required';
  END IF;

  SET v_bad = COALESCE(size(from_json(p_proposals_json,
    'ARRAY<STRUCT<edge_id:STRING,source_node_id:STRING,relationship_type:STRING,target_node_id:STRING,report_week_end:STRING,evidence_chunk_id:STRING,claim_text:STRING,confidence_score:DOUBLE,extraction_model_id:STRING>>')), 0);
  IF v_bad <= 0 THEN
    SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='Edge proposal JSON is empty or could not be parsed';
  END IF;

  -- Evidence must be a currently eligible Silver passage (published source text only).
  SET v_bad = (
    SELECT count(*)
    FROM (SELECT inline(from_json(p_proposals_json,
      'ARRAY<STRUCT<edge_id:STRING,source_node_id:STRING,relationship_type:STRING,target_node_id:STRING,report_week_end:STRING,evidence_chunk_id:STRING,claim_text:STRING,confidence_score:DOUBLE,extraction_model_id:STRING>>'))) e
    LEFT JOIN prdrzranalytics.lab42.sdi_tbl_weeklyNarrative_gold_knowledgeNodes_long s ON e.source_node_id = s.node_id
    LEFT JOIN prdrzranalytics.lab42.sdi_tbl_weeklyNarrative_gold_knowledgeNodes_long t ON e.target_node_id = t.node_id
    LEFT JOIN prdrzranalytics.lab42.sdi_tbl_weeklyNarrative_silver_narrativeChunks_weekly c
           ON e.evidence_chunk_id = c.chunk_id AND c.is_evidence_eligible = TRUE
    WHERE e.edge_id IS NULL OR s.node_id IS NULL OR t.node_id IS NULL OR c.chunk_id IS NULL
       OR e.claim_text IS NULL OR trim(e.claim_text) = ''
       OR e.relationship_type NOT IN ('ASSOCIATED_WITH_CHANNEL','REPORTED_DRIVER_OF','MENTIONED_WITH','AFFECTS_LOB','RELATED_TO_METRIC','REPORTED_ISSUE_FOR')
  );
  IF v_bad>0 THEN
    SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='Invalid edges: missing nodes, ineligible Silver evidence or unsupported predicate';
  END IF;

  SET v_bad = (
    SELECT count(*) FROM (
      SELECT edge_id FROM (SELECT inline(from_json(p_proposals_json,
        'ARRAY<STRUCT<edge_id:STRING,source_node_id:STRING,relationship_type:STRING,target_node_id:STRING,report_week_end:STRING,evidence_chunk_id:STRING,claim_text:STRING,confidence_score:DOUBLE,extraction_model_id:STRING>>'))) e
      GROUP BY edge_id HAVING count(*)>1)
  );
  IF v_bad>0 THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='Duplicate edge proposals'; END IF;

  MERGE INTO prdrzranalytics.lab42.sdi_tbl_weeklyNarrative_gold_knowledgeEdges_long t
  USING (
    SELECT edge_id, source_node_id, relationship_type, target_node_id,
           try_cast(report_week_end AS DATE) report_week_end,
           evidence_chunk_id, claim_text, confidence_score,
           'PENDING_REVIEW' verification_status, extraction_model_id,
           CAST(NULL AS STRING) reviewer_id, CAST(NULL AS STRING) reviewer_name,
           CAST(NULL AS TIMESTAMP) reviewed_at, current_timestamp() created_at
    FROM (SELECT inline(from_json(p_proposals_json,
      'ARRAY<STRUCT<edge_id:STRING,source_node_id:STRING,relationship_type:STRING,target_node_id:STRING,report_week_end:STRING,evidence_chunk_id:STRING,claim_text:STRING,confidence_score:DOUBLE,extraction_model_id:STRING>>'))) e
  ) s ON t.edge_id = s.edge_id
  WHEN NOT MATCHED THEN INSERT *;
END;