/* ============================================================================
 PROJECT      : Weekly Narrative | Databricks Unity Catalog | v7
 FILE         : 08_sdi_vw_weeklyNarrative_gold_reportHeads_wide.sql
 LAYER        : agent/gold
 PURPOSE      : See object-level comments and README for schema/processing contract
 DEPENDENCIES : Deploy in documented layer order
 EXECUTION    : Use Databricks SQL / UC runtime
 OWNERSHIP    : Production writes only by governed job/service principal.
 GRAIN        : As defined by target object. All reporting weeks Sun-Sat.
 SAFETY       : Review/approval not inferred from LLM output. Retry-safe writer.
 ============================================================================ */
/* =================================================================================================
 FILE    : 39_sdi_vw_weeklyNarrative_gold_reportHeads_wide.sql
 PURPOSE : Single read model for frontend. Distinguishes the newest saved working version
           from the most recently EXPLICITLY published official report.
 GRAIN   : One row per report_id.
 EXAMPLE : After Sarah publishes V3 and John saves V4, latest_working=V4 and
           published=V3; the official report does not change automatically.
================================================================================================= */
CREATE OR REPLACE VIEW prdrzranalytics.lab42.sdi_vw_weeklyNarrative_gold_reportHeads_wide AS
SELECT
  w.report_id,
  w.week_end,
  w.report_scope,
  w.latest_working_version_id,
  w.latest_working_status,
  w.last_editor_id,
  w.last_editor_name,
  w.last_saved_at,
  p.version_id AS published_version_id,
  p.published_at,
  p.published_by_user_id,
  p.published_by_display_name,
  CASE
    WHEN p.version_id IS NULL THEN 'NOT_YET_PUBLISHED'
    WHEN w.latest_working_version_id <> p.version_id THEN 'UNPUBLISHED_CHANGES'
    ELSE 'UP_TO_DATE'
  END AS publication_sync_status
FROM prdrzranalytics.lab42.sdi_vw_weeklyNarrative_gold_latestWorking_wide w
LEFT JOIN prdrzranalytics.lab42.sdi_vw_weeklyNarrative_gold_publishedReports_wide p
  ON p.report_id = w.report_id;
