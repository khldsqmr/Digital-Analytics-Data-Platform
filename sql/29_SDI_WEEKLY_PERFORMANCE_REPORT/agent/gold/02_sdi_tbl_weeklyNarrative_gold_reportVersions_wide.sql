/* ============================================================================
 PROJECT      : Weekly Narrative | Databricks Unity Catalog | v7
 FILE         : 02_sdi_tbl_weeklyNarrative_gold_reportVersions_wide.sql
 LAYER        : agent/gold
 PURPOSE      : See object-level comments and README for schema/processing contract
 DEPENDENCIES : Deploy in documented layer order
 EXECUTION    : Use Databricks SQL / UC runtime
 OWNERSHIP    : Production writes only by governed job/service principal.
 GRAIN        : As defined by target object. All reporting weeks Sun-Sat.
 SAFETY       : Review/approval not inferred from LLM output. Retry-safe writer.
 ============================================================================ */
/* PHASE 3 | IMMUTABLE DRAFTS AND REVISIONS.
 Two people can generate distinct versions of the same report_id. Never overwrite earlier drafts.
 A new draft after publication DOES NOT replace the published version.
 Display status is DERIVED from reviewer votes in reportStatus view, not maintained in this table. */
CREATE TABLE IF NOT EXISTS prdrzranalytics.lab42.sdi_tbl_weeklyNarrative_gold_reportVersions_wide (
  version_id STRING NOT NULL COMMENT 'VER_<UUID>: exact immutable draft, regeneration or human edit.',
  report_id STRING NOT NULL COMMENT 'WN_yyyyMMdd_SCOPE: shared weekly report across all generated versions.',
  week_end DATE NOT NULL COMMENT 'Saturday of reporting week, starting previous Sunday.',
  report_scope STRING NOT NULL COMMENT 'ALL, POSTPAID or BROADBAND; part of logical report identity.',
  parent_version_id STRING COMMENT 'Exact source version on which this saved revision was based. NULL for an independent first draft. Supports stale-editor detection.',
  investigation_id STRING NOT NULL COMMENT 'INV_<UUID> pointing at frozen numerical facts and retrieved evidence.',
  sections_json STRING NOT NULL COMMENT 'Four narrative sections and the evidence IDs used for each.',
  author_type STRING NOT NULL COMMENT 'AI or HUMAN_EDIT, identifying who produced the text.',
  generation_mode STRING NOT NULL COMMENT 'HUMAN_FIRST, AGENT_ASSISTED, or MANUAL_EDIT.',
  model_id STRING COMMENT 'LLM endpoint/model that generated the version, null for human edits.',
  prompt_version STRING COMMENT 'Prompt template revision, null for human edits.',
  created_by_user_id STRING NOT NULL COMMENT 'Authenticated user initiating this version.',
  created_by_display_name STRING COMMENT 'User name recorded when the version was created.',
  generation_request_id STRING COMMENT 'Client idempotency key; same retries must not duplicate a version.',
  created_at TIMESTAMP NOT NULL COMMENT 'UTC timestamp for this immutable version.'
) USING DELTA COMMENT 'All AI and human edited narrative versions, including later rejected drafts.';
