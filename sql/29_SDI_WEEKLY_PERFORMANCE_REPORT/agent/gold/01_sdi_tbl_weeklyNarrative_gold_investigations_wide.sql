/* ============================================================================
 PROJECT      : Weekly Narrative | Databricks Unity Catalog | v7
 FILE         : 01_sdi_tbl_weeklyNarrative_gold_investigations_wide.sql
 LAYER        : agent/gold
 PURPOSE      : See object-level comments and README for schema/processing contract
 DEPENDENCIES : Deploy in documented layer order
 EXECUTION    : Use Databricks SQL / UC runtime
 OWNERSHIP    : Production writes only by governed job/service principal.
 GRAIN        : As defined by target object. All reporting weeks Sun-Sat.
 SAFETY       : Review/approval not inferred from LLM output. Retry-safe writer.
 ============================================================================ */
/* PHASE 3 | IMMUTABLE INVESTIGATION. One record per fact/evidence exploration.
 The LLM can request governed tools, but application checks source lineage and freezes results. */
CREATE TABLE IF NOT EXISTS prdrzranalytics.lab42.sdi_tbl_weeklyNarrative_gold_investigations_wide (
  investigation_id STRING NOT NULL COMMENT 'INV_<UUID>: unique stored AI investigation; never use ambiguous context_id.',
  report_id STRING NOT NULL COMMENT 'WN_yyyyMMdd_SCOPE: week+scope report under investigation.',
  week_end DATE NOT NULL COMMENT 'Completed Saturday this investigation analyzes.',
  source_snapshots_json STRING NOT NULL COMMENT 'Array of source_project/snapshot_id pairs; all metric facts pinned.',
  facts_json STRING NOT NULL COMMENT 'Exact numerical rows returned by authorized SQL adapters.',
  evidence_json STRING NOT NULL COMMENT 'Historical narrative passages selected for this investigation.',
  graph_json STRING COMMENT 'Approved graph relationships selected as supporting history.',
  agent_findings_json STRING COMMENT 'Agent interpretation, uncertainties and questions; not facts.',
  validation_json STRING NOT NULL COMMENT 'Checks for source IDs, registered metrics and frozen snapshots.',
  investigation_status STRING NOT NULL COMMENT 'VALIDATED, NEEDS_REVIEW or REJECTED.',
  mode STRING NOT NULL COMMENT 'HUMAN_FIRST or AGENT_ASSISTED.',
  model_id STRING COMMENT 'LLM/model endpoint used for investigation, if any.',
  requested_by_user_id STRING NOT NULL COMMENT 'Authenticated user that requested the investigation.',
  requested_by_display_name STRING COMMENT 'Display name at time of request.',
  created_at TIMESTAMP NOT NULL COMMENT 'UTC time when evidence package was frozen.'
) USING DELTA COMMENT 'Immutable numerical and narrative evidence package for one AI draft.';
