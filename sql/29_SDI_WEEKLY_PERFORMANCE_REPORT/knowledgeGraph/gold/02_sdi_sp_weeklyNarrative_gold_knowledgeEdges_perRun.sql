/* ============================================================================
 PROJECT      : Weekly Narrative | Databricks Unity Catalog | v7
 FILE         : x
 LAYER        : KNOWLEDGE GRAPH / GOLD
 PURPOSE      : Create and populate graph Edges from validated in-memory JSON proposals
 DEPENDENCIES : Track B Gold approved evidence, previous Nodes for edges
 EXECUTION    : CALL (JSON array of proposals from governed AI notebook)
 OWNERSHIP    : Production writes only by governed job/service principal.
 GRAIN        : As defined by target object. All reporting weeks Sun-Sat.
 SAFETY       : Review/approval not inferred from LLM output. Retry-safe writer.
 ============================================================================ */
CREATE OR REPLACE PROCEDURE prdrzranalytics.lab42.sdi_sp_weeklyNarrative_gold_knowledgeEdges_perRun(IN p_proposals_json STRING)
LANGUAGE SQL SQL SECURITY INVOKER MODIFIES SQL DATA
AS BEGIN
  DECLARE v_bad BIGINT DEFAULT 0;
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
  IF p_proposals_json IS NULL OR trim(p_proposals_json)='' THEN
    SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='JSON proposal batch required'; END IF;
  CREATE OR REPLACE TEMP VIEW wn_graph_edges AS
    SELECT get_json_object(value,'$.edge_id') edge_id,
      get_json_object(value,'$.source_node_id') source_node_id,
      get_json_object(value,'$.relationship_type') relationship_type,
      get_json_object(value,'$.target_node_id') target_node_id,
      try_cast(get_json_object(value,'$.report_week_end') AS DATE) report_week_end,
      get_json_object(value,'$.evidence_chunk_id') evidence_chunk_id,
      get_json_object(value,'$.claim_text') claim_text,
      try_cast(get_json_object(value,'$.confidence_score') AS DOUBLE) confidence_score,
      'PENDING_REVIEW' verification_status,
      get_json_object(value,'$.extraction_model_id') extraction_model_id,
      cast(null AS STRING) reviewer_id,cast(null AS STRING) reviewer_name,
      cast(null AS TIMESTAMP) reviewed_at,current_timestamp() created_at
    FROM (SELECT to_json(p) AS value FROM (SELECT explode(from_json(p_proposals_json, 'ARRAY<STRUCT<edge_id:STRING,source_node_id:STRING,relationship_type:STRING,target_node_id:STRING,report_week_end:STRING,evidence_chunk_id:STRING,claim_text:STRING,confidence_score:DOUBLE,extraction_model_id:STRING>>')) p));
  SET v_bad=(SELECT count(*) FROM wn_graph_edges e
    LEFT JOIN prdrzranalytics.lab42.sdi_tbl_weeklyNarrative_gold_knowledgeNodes_long s ON e.source_node_id=s.node_id
    LEFT JOIN prdrzranalytics.lab42.sdi_tbl_weeklyNarrative_gold_knowledgeNodes_long t ON e.target_node_id=t.node_id
    LEFT JOIN prdrzranalytics.lab42.sdi_tbl_weeklyNarrative_gold_approvedEvidence_long c ON e.evidence_chunk_id=c.chunk_id
    WHERE e.edge_id IS NULL OR s.node_id IS NULL OR t.node_id IS NULL OR c.chunk_id IS NULL
      OR e.relationship_type NOT IN ('ASSOCIATED_WITH_CHANNEL','REPORTED_DRIVER_OF','MENTIONED_WITH','AFFECTS_LOB','RELATED_TO_METRIC','REPORTED_ISSUE_FOR'));
  IF v_bad>0 THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='Invalid edges: missing nodes, ineligible evidence or unsupported predicate'; END IF;
  SET v_bad=(SELECT count(*) FROM (SELECT edge_id FROM wn_graph_edges GROUP BY edge_id HAVING count(*)>1));
  IF v_bad>0 THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='Duplicate edge proposals'; END IF;
  MERGE INTO prdrzranalytics.lab42.sdi_tbl_weeklyNarrative_gold_knowledgeEdges_long t
  USING wn_graph_edges s ON t.edge_id=s.edge_id
  WHEN NOT MATCHED THEN INSERT *;
END;
