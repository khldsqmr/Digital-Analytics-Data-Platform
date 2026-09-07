/* =================================================================================================
FILE:         05_sdi_vw_dashboardPulseTms_gold_metricAnnotated_long.sql
LAYER:        Gold View - AI-Ready Annotated Fact Table
VIEW NAME:    sdi_vw_dashboardPulseTms_gold_metricAnnotated_long

PURPOSE:
  Pre-built three-way join: gold_unified_long + appendixMetricBridge + the appendix's wide
  content, so an agentic Genie/SQL-querying LLM queries ONE object with a plain
  WHERE apx_id = 'x' instead of reconstructing the wildcard-lob join logic itself every query.
  That wildcard pattern (b.gul_lob = g.lob OR b.gul_lob IS NULL) is exactly the kind of thing a
  text-to-SQL model is likely to get wrong on its own - either a naive equality join that
  silently drops every wildcarded row, or the wildcard forgotten entirely - and that failure
  mode is worse than an obviously-wrong answer, since the query still runs and returns
  something plausible-looking.

  Joins against the WIDE appendix content (Bronze, one row per apx_id), NOT the long/unpivoted
  Gold appendix view. This is deliberate: gold_appendix_long is exploded to one row per
  header/bullet for Tableau's expandable detail cards, and joining a fact table against that
  grain would multiply every metric_value row by however many bullets that metric's card
  happens to have. One annotation per fact row needs the wide grain.

GRAIN:
  Same as gold_unified_long: one row per data_source x qgp_date x lob x channel_group x
  metric_name x metric_type, LEFT JOINed to at most one apx_id's annotation. Rows with no
  bridge match (a fact row whose metric has no appendix card yet - Platform Spend, Biddable
  Spend, and MFC's TFB lob all currently fall in this bucket) carry NULL for every apx_* column
  rather than being dropped, so this view is always a superset of gold_unified_long's row count
  for filtering/aggregation purposes.

  NOTE - MFC rows fan out 1:many through the bridge (Channel + Granular grain both map to the
  same apx_id) but the bridge's OWN grain already matches gold_unified_long's data_source
  column, so this LEFT JOIN does not multiply MFC rows - a MFC_SPEND_CHANNEL fact row only
  matches the two MFC_SPEND_CHANNEL bridge rows (Actual, Forecast), never the Granular ones.

KEY COLUMNS (added on top of gold_unified_long's full schema):
  apx_id                   - the matched appendix definition, NULL if none exists yet
  apx_metric_name          - display name for the metric (may differ in casing/spacing from
                             gold_unified_long's camelCase metric_name)
  apx_funnel_stage         - 'Top Funnel' | 'Mid Funnel' | 'Bottom Funnel' | NULL
  apx_vp_one_liner         - plain-language definition, the first thing to surface for
                             "what does this metric mean"
  apx_data_source_label    - human-readable source description (e.g. 'Media Flow Chart (MFC)')
  apx_source_table         - the actual upstream table backing this metric
  apx_is_subflow           - whether this metric rolls up into a parent metric
  apx_parent_id            - the apx_id it rolls up into, if any
  apx_summable_to_parent   - whether it ACTUALLY sums to that parent (false for UPV flows,
                             despite being marked as subflows - this is the single most
                             important flag for a model reasoning about whether summing several
                             metric rows produces a valid total)
  apx_build_detail_raw     - full segment-stacking logic, unparsed (use gold_appendix_long for
                             the bullet-by-bullet unpivoted version)
  apx_key_exclusions_raw   - full exclusion logic, unparsed
  apx_referenced_glossary_ids - array of glossary apx_ids this metric's definition depends on

DOWNSTREAM:
  Intended as the source for a Unity Catalog metric view (proposed name:
  sdi_mv_dashboardPulseTms_gold_unified_weekly), which would carry the synonym/display-name
  metadata Databricks' semantic layer supports specifically for LLM accuracy - sourcing the
  metric view off this annotated view rather than off gold_unified_long directly, so the
  semantic layer itself carries appendix context instead of Genie needing a separate lookup
  step. Also directly queryable by Genie today, ahead of that metric view existing.

CHANGE LOG:
  - Built this session, resolving the wildcard-lob join question worked through with Khalid
    across the appendix design conversation.
================================================================================================= */

CREATE OR REPLACE VIEW
  prdrzranalytics.lab42.sdi_vw_dashboardPulseTms_gold_metricAnnotated_long
AS

SELECT
  g.*,

  a.apx_id,
  a.apx_metric_name,
  a.apx_funnel_stage,
  a.apx_category,
  a.apx_flow_scope,
  a.apx_is_subflow,
  a.apx_parent_id,
  a.apx_summable_to_parent,
  a.apx_data_source_label,
  a.apx_source_table,
  a.apx_source_owner,
  a.apx_vp_one_liner,
  a.apx_build_detail_raw,
  a.apx_key_exclusions_raw,
  a.apx_refresh_cadence,
  a.apx_referenced_glossary_ids

FROM prdrzranalytics.lab42.sdi_vw_dashboardPulseTms_gold_unified_long g

LEFT JOIN prdrzranalytics.lab42.sdi_tbl_dashboardPulseTms_gold_appendixMetricBridge_weekly b
  ON  b.gul_data_source = g.data_source
  AND b.gul_metric_name = g.metric_name
  AND (b.gul_lob = g.lob OR b.gul_lob IS NULL)   -- the wildcard: NULL on the bridge side means
                                                   -- "match every lob", not "match only NULL"

LEFT JOIN prdrzranalytics.lab42.sdi_vw_dashboardPulseTms_bronze_appendix_wide a
  ON  a.apx_id = b.apx_id
  AND a.apx_record_type = 'Metric'
;