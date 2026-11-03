/* ============================================================================
 PROJECT      : Weekly Narrative | Databricks Unity Catalog | v7
 FILE         : 00_sdi_vw_weeklyNarrative_control_narrativeSources_static.sql
 LAYER        : trackB/control
 PURPOSE      : See object-level comments and README for schema/processing contract
 DEPENDENCIES : Deploy in documented layer order
 EXECUTION    : Use Databricks SQL / UC runtime
 OWNERSHIP    : Production writes only by governed job/service principal.
 GRAIN        : As defined by target object. All reporting weeks Sun-Sat.
 SAFETY       : Review/approval not inferred from LLM output. Retry-safe writer.
 ============================================================================ */
/* ================================================================================
OBJECT: sdi_vw_weeklyNarrative_control_narrativeSources_static
WHY:   One active source registry for all Track B ingestion paths. This is a CODE
       whitelist, not an access-control boundary. Enforce permissions separately.
GRAIN: One row per source_system. Run BEFORE the ingestion notebook.
       Historical import and app versions both land in ONE Bronze table.
       New Word/SharePoint sources are disabled until a tested parser exists.
================================================================================ */
CREATE OR REPLACE VIEW prdrzranalytics.lab42.sdi_vw_weeklyNarrative_control_narrativeSources_static AS
SELECT * FROM VALUES
 ('HISTORICAL_JSONL','HISTORICAL_IMPORT',TRUE,'ONE_TIME_BACKFILL', 'JSONL historical weekly reports; preserve original payload'),
 ('WEEKLY_NARRATIVE_APP','APP_VERSION',TRUE,'APP_SYNC', 'All saved app versions, including versions later rejected'),
 ('EXTERNAL_DOCUMENT','EXTERNAL_DOCUMENT',FALSE,'EXTERNAL_INGEST','Future source; no parser currently enabled')
AS t(source_system,source_type,is_enabled,ingestion_mode,description);
