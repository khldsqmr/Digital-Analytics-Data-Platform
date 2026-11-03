/* ============================================================================
 PROJECT      : Weekly Narrative | Databricks Unity Catalog | v7
 FILE         : x
 LAYER        : KNOWLEDGE GRAPH / GOLD
 PURPOSE      : Create and populate graph Nodes from validated in-memory JSON proposals
 DEPENDENCIES : Eligible Silver, previous Nodes for edges
 EXECUTION    : CALL (JSON array of proposals from governed AI notebook)
 OWNERSHIP    : Production writes only by governed job/service principal.
 GRAIN        : As defined by target object. All reporting weeks Sun-Sat.
 SAFETY       : Review/approval not inferred from LLM output. Retry-safe writer.
 ============================================================================ */
CREATE OR REPLACE PROCEDURE prdrzranalytics.lab42.sdi_sp_weeklyNarrative_gold_knowledgeNodes_perRun(IN p_proposals_json STRING)
LANGUAGE SQL SQL SECURITY INVOKER MODIFIES SQL DATA
AS BEGIN
  DECLARE v_bad BIGINT DEFAULT 0;
  CREATE TABLE IF NOT EXISTS prdrzranalytics.lab42.sdi_tbl_weeklyNarrative_gold_knowledgeNodes_long (
    node_id STRING NOT NULL COMMENT 'Canonical stable entity key, e.g. campaign:freedomExplorer.',
    node_type STRING NOT NULL COMMENT 'METRIC, CAMPAIGN, CHANNEL, EVENT, LOB, or other governed entity category.',
    canonical_name STRING NOT NULL COMMENT 'Readable preferred display name of the entity.',
    metric_key STRING COMMENT 'Optional registered source metric key for METRIC nodes.',
    aliases ARRAY<STRING> COMMENT 'Alternative labels associated with the same canonical entity.',
    first_seen_date DATE COMMENT 'Earliest date linked to the entity in imported/published evidence.',
    last_seen_date DATE COMMENT 'Most recent date linked to the entity in imported/published evidence.',
    verification_status STRING NOT NULL COMMENT 'PENDING_REVIEW, APPROVED or REJECTED; only approved nodes enter trusted retrieval.',
    attributes_json STRING COMMENT 'Additional metadata that does not determine canonical identity.',
    created_at TIMESTAMP NOT NULL COMMENT 'First insertion timestamp.',
    updated_at TIMESTAMP NOT NULL COMMENT 'Last approved metadata refresh timestamp.'
  ) USING DELTA COMMENT 'Reusable canonical business entities; AI proposals separated from approval.';
  IF p_proposals_json IS NULL OR trim(p_proposals_json)='' THEN
    SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='JSON proposal batch required'; END IF;
  CREATE OR REPLACE TEMP VIEW wn_graph_nodes AS
    SELECT get_json_object(value,'$.node_id') node_id,
      get_json_object(value,'$.node_type') node_type,
      get_json_object(value,'$.canonical_name') canonical_name,
      get_json_object(value,'$.metric_key') metric_key,
      from_json(get_json_object(value,'$.aliases'),'ARRAY<STRING>') aliases,
      cast(null AS DATE) first_seen_date, cast(null AS DATE) last_seen_date,
      'PENDING_REVIEW' verification_status,
      get_json_object(value,'$.attributes_json') attributes_json,
      current_timestamp() created_at,current_timestamp() updated_at
    FROM (SELECT to_json(p) AS value FROM (SELECT explode(from_json(p_proposals_json, 'ARRAY<STRUCT<node_id:STRING,node_type:STRING,canonical_name:STRING,metric_key:STRING,aliases:ARRAY<STRING>,attributes_json:STRING>>')) p));
  SET v_bad=(SELECT count(*) FROM wn_graph_nodes
    WHERE node_id IS NULL OR canonical_name IS NULL
      OR node_type NOT IN ('METRIC','CAMPAIGN','CHANNEL','EVENT','LOB','ISSUE','PAGE','PRODUCT'));
  IF v_bad>0 THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='Invalid node proposal type/key'; END IF;
  SET v_bad=(SELECT count(*) FROM (SELECT node_id FROM wn_graph_nodes GROUP BY node_id HAVING count(*)>1));
  IF v_bad>0 THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='Duplicate node proposals'; END IF;
  SET v_bad=(SELECT count(*) FROM wn_graph_nodes n
     LEFT JOIN prdrzranalytics.lab42.sdi_vw_weeklyNarrative_control_metricRegistry_static r
       ON n.metric_key=r.metric_key
     WHERE n.node_type='METRIC' AND (n.metric_key IS NULL OR r.metric_key IS NULL));
  IF v_bad>0 THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='METRIC node must resolve to registered metric'; END IF;
  MERGE INTO prdrzranalytics.lab42.sdi_tbl_weeklyNarrative_gold_knowledgeNodes_long t
  USING wn_graph_nodes s ON t.node_id=s.node_id
  WHEN NOT MATCHED THEN INSERT *;
END;
