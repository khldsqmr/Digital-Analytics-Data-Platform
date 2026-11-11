/* ============================================================================
 PROJECT      : Weekly Narrative | Databricks Unity Catalog | v7.1
 FILE         : 03_sdi_vw_weeklyNarrative_gold_approvedGraphEdges_long.sql
 LAYER        : KNOWLEDGE GRAPH / GOLD
 PURPOSE      : Secure graph retrieval; evidence eligibility checked live
 DEPENDENCIES : Nodes, Edges, Track B Silver narrative chunks
 EXECUTION    : SELECT only
 ============================================================================ */

CREATE OR REPLACE VIEW prdrzranalytics.lab42.sdi_vw_weeklyNarrative_gold_approvedGraphEdges_long AS
SELECT e.* FROM prdrzranalytics.lab42.sdi_tbl_weeklyNarrative_gold_knowledgeEdges_long e
JOIN prdrzranalytics.lab42.sdi_tbl_weeklyNarrative_gold_knowledgeNodes_long s
 ON e.source_node_id=s.node_id AND s.verification_status='APPROVED'
JOIN prdrzranalytics.lab42.sdi_tbl_weeklyNarrative_gold_knowledgeNodes_long t
 ON e.target_node_id=t.node_id AND t.verification_status='APPROVED'
JOIN prdrzranalytics.lab42.sdi_tbl_weeklyNarrative_silver_narrativeChunks_weekly c
 ON e.evidence_chunk_id=c.chunk_id AND c.is_evidence_eligible=TRUE
WHERE e.verification_status='APPROVED';