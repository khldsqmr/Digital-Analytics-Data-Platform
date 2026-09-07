/* =================================================================================================
FILE:         03_sdi_vw_dashboardPulseTms_gold_appendix_long.sql
LAYER:        Dimension View - Gold
VIEW NAME:    sdi_vw_dashboardPulseTms_gold_appendix_long

PURPOSE:
  Terminal Gold output for the appendix stack. Pass-through of Silver's unpivoted rows, plus
  Tableau-binding convenience columns: a short funnel-stage key for parameter/filter values, and
  a broadcast list of every Metric display name for a dashboard-wide metric-picker parameter.

  Still deliberately NOT stacked into sdi_vw_dashboardPulseTms_gold_unified_long - different
  grain (one row per apx_id x section, a dimension/reference shape, not one row per
  qgp_date x lob x channel_group x metric_name, a fact shape). Connects to the fact table via
  sdi_tbl_dashboardPulseTms_gold_appendixMetricBridge_weekly and
  sdi_vw_dashboardPulseTms_gold_metricAnnotated_long instead of a UNION ALL.

GRAIN:
  Same as Silver: one row per apx_id x apx_section_type x apx_section_seq.

KEY COLUMNS (added on top of Silver):
  apx_funnel_stage_key   - 'TOP' | 'MID' | 'BOTTOM' | NULL (Glossary), short key for a Tableau
                           parameter/filter value distinct from the display label
  apx_funnel_stage_label - same as Bronze's apx_funnel_stage, carried through explicitly as its
                           own column name for binding clarity
  apx_display_name       - COALESCE(apx_metric_name, apx_glossary_term). Metric rows carry a
                           name here, Glossary rows carry a term, never both - this collapses
                           them to one field so a single dropdown/parameter can bind against it
                           regardless of which Content Type toggle position the dashboard is on.
  apx_label_options      - array of every distinct apx_display_name across the whole table
                           (Metric AND Glossary), identical on every row (a broadcast constant)

SINGLE-TABLE DASHBOARD USAGE:
  This view is denormalized on purpose so ONE Tableau data source can drive every section of the
  appendix panel - no second wide view needed. Every worksheet points at this same table, only
  the row filter differs:
    - Item selector dropdown: filter on apx_record_type = [p_content_type], show apx_display_name
    - Section 1 (Details), 2a (Definition), 2b (Warning), 2c (Source info): filter to
      apx_section_seq = 0 AND apx_section_type IN ('BUILD', 'GLOSSARY_DEFINITION') - this
      predicate lands on exactly one row per apx_id (a Metric's first build paragraph for
      Metric rows, the only row for Glossary rows), which is where every Section 1/2b/2c field
      already lives. Section 2a's definition text is IFNULL(apx_vp_one_liner, apx_section_detail)
      on that same row - Metric rows carry it in apx_vp_one_liner, Glossary rows carry it in
      apx_section_detail (their glossary definition, already placed there in Silver).
    - Section 3 (How it's built): filter to apx_section_type = 'BUILD', no seq restriction,
      ordered by apx_section_seq - naturally empty for Glossary rows (they have none), correct
      behavior, not a bug to hide separately.
    - Section 4 (Key Exclusions): same, apx_section_type = 'EXCLUSION'.

CHANGE LOG:
  - Added apx_display_name (COALESCE across Metric/Glossary name fields) to support a single
    denormalized table driving the whole dashboard panel, no separate wide view.
  - apx_warning_message now carried through from Bronze (Section 2b source field).
  - Ported from the team's "Pulse 1.2 - Metric Appendix" spreadsheet, this session, alongside
    Bronze/Silver.
================================================================================================= */

CREATE OR REPLACE VIEW
  prdrzranalytics.lab42.sdi_vw_dashboardPulseTms_gold_appendix_long
AS

WITH

-- NOTE: apx_display_name and apx_warning_message need to exist on Silver's carried-through
-- columns for this to compile as written. Silver currently passes through everything from
-- Bronze except apx_warning_message and the raw apx_metric_name/apx_glossary_term pairing
-- needed for apx_display_name - both are one-line additions to Silver's three SELECT branches
-- (add b.apx_warning_message to the carried-through column list in all three CTEs; Silver
-- already exposes apx_metric_name, and glossary term already lands in apx_section_label on the
-- GLOSSARY_DEFINITION row, so COALESCE happens here in Gold using that plus apx_metric_name).

LabelOptions AS (
  SELECT sort_array(collect_set(
    COALESCE(apx_metric_name, apx_section_label)
  ))                                                                         AS apx_label_options
  FROM prdrzranalytics.lab42.sdi_vw_dashboardPulseTms_silver_appendix_long
  WHERE apx_section_seq = 0
    AND apx_section_type IN ('BUILD', 'GLOSSARY_DEFINITION')
)

SELECT
  s.apx_id,
  s.apx_record_type,
  s.apx_funnel_stage                                                        AS apx_funnel_stage_label,
  CASE s.apx_funnel_stage
    WHEN 'Top Funnel'    THEN 'TOP'
    WHEN 'Mid Funnel'    THEN 'MID'
    WHEN 'Bottom Funnel' THEN 'BOTTOM'
    ELSE NULL
  END                                                                        AS apx_funnel_stage_key,
  s.apx_category,
  s.apx_metric_name,
  -- Metric rows carry a name in apx_metric_name; Glossary rows carry their term in
  -- apx_section_label on the GLOSSARY_DEFINITION row (set in Silver from apx_glossary_term).
  -- COALESCE collapses both into one field so a single dropdown can bind against it.
  COALESCE(
    s.apx_metric_name,
    CASE WHEN s.apx_section_type = 'GLOSSARY_DEFINITION' THEN s.apx_section_label END
  )                                                                          AS apx_display_name,
  s.apx_flow_scope,
  s.apx_is_subflow,
  s.apx_parent_id,
  s.apx_summable_to_parent,
  s.apx_data_source_label,
  s.apx_source_table,
  s.apx_source_owner,
  s.apx_vp_one_liner,
  s.apx_warning_message,
  s.apx_refresh_cadence,
  s.apx_referenced_glossary_ids,
  s.apx_section_type,
  s.apx_section_seq,
  s.apx_section_label,
  s.apx_section_detail,
  -- Section 2a definition, unified across both record types on the seq=0 row: Metric rows
  -- carry it in apx_vp_one_liner, Glossary rows carry it in apx_section_detail already.
  IFNULL(s.apx_vp_one_liner, s.apx_section_detail)                          AS apx_definition_text,
  s.apx_sort_order,
  lo.apx_label_options
FROM prdrzranalytics.lab42.sdi_vw_dashboardPulseTms_silver_appendix_long s
CROSS JOIN LabelOptions lo
ORDER BY s.apx_sort_order, s.apx_section_type, s.apx_section_seq
;