/* ============================================================================
 PROJECT      : Weekly Narrative | Databricks Unity Catalog | v7
 FILE         : x
 LAYER        : KG / VALIDATION
 PURPOSE      : Graph sanity and trusted retrieval
 DEPENDENCIES : Graph Gold + Silver
 EXECUTION    : Ad hoc
 OWNERSHIP    : Production writes only by governed job/service principal.
 GRAIN        : As defined by target object. All reporting weeks Sun-Sat.
 SAFETY       : Review/approval not inferred from LLM output. Retry-safe writer.
 ============================================================================ */

SELECT 'duplicate_nodes' test_name,count(*) failures FROM (SELECT node_id FROM prdrzranalytics.lab42.sdi_tbl_weeklyNarrative_gold_knowledgeNodes_long GROUP BY node_id HAVING count(*)>1);
SELECT 'duplicate_edges' test_name,count(*) failures FROM (SELECT edge_id FROM prdrzranalytics.lab42.sdi_tbl_weeklyNarrative_gold_knowledgeEdges_long GROUP BY edge_id HAVING count(*)>1);
SELECT e.edge_id FROM prdrzranalytics.lab42.sdi_tbl_weeklyNarrative_gold_knowledgeEdges_long e
 LEFT JOIN prdrzranalytics.lab42.sdi_tbl_weeklyNarrative_gold_knowledgeNodes_long n ON e.source_node_id=n.node_id
 WHERE n.node_id IS NULL;
SELECT e.edge_id FROM prdrzranalytics.lab42.sdi_tbl_weeklyNarrative_gold_knowledgeEdges_long e
 LEFT JOIN prdrzranalytics.lab42.sdi_tbl_weeklyNarrative_gold_knowledgeNodes_long n ON e.target_node_id=n.node_id
 WHERE n.node_id IS NULL;
