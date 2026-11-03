/* ============================================================================
 PROJECT      : Weekly Narrative | Databricks Unity Catalog | v7
 FILE         : 07_sdi_vw_weeklyNarrative_gold_latestWorking_wide.sql
 LAYER        : agent/gold
 PURPOSE      : See object-level comments and README for schema/processing contract
 DEPENDENCIES : Deploy in documented layer order
 EXECUTION    : Use Databricks SQL / UC runtime
 OWNERSHIP    : Production writes only by governed job/service principal.
 GRAIN        : As defined by target object. All reporting weeks Sun-Sat.
 SAFETY       : Review/approval not inferred from LLM output. Retry-safe writer.
 ============================================================================ */
/* =================================================================================================
 FILE   : 38_sdi_vw_weeklyNarrative_gold_latestWorking_wide.sql
 LAYER  : AGENT / GOLD (derived read-only UI view)
 GRAIN  : ONE row per logical report_id (reporting Saturday + scope)
 PURPOSE:
   Return most recently SAVED immutable narrative version, whether approved or still DRAFT.
   This is the default for editing. It is NOT necessarily the official published version.
   ORDER BY uses created_at and version_id as a deterministic tie-breaker.
   Do not infer latest using MAX(version_id): version IDs are UUIDs, not counters.
================================================================================================= */
CREATE OR REPLACE VIEW prdrzranalytics.lab42.sdi_vw_weeklyNarrative_gold_latestWorking_wide AS
SELECT
  v.report_id,
  v.week_end,
  v.report_scope,
  v.version_id AS latest_working_version_id,
  v.parent_version_id,
  v.investigation_id,
  v.sections_json,
  v.author_type,
  v.created_by_user_id AS last_editor_id,
  v.created_by_display_name AS last_editor_name,
  v.created_at AS last_saved_at,
  s.report_status AS latest_working_status,
  s.accepted_sections,
  s.rejected_sections,
  s.flagged_sections,
  s.conflicted_sections
FROM prdrzranalytics.lab42.sdi_tbl_weeklyNarrative_gold_reportVersions_wide v
JOIN prdrzranalytics.lab42.sdi_vw_weeklyNarrative_gold_reportStatus_wide s
  ON v.version_id = s.version_id
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY v.report_id
  ORDER BY v.created_at DESC, v.version_id DESC
) = 1;
