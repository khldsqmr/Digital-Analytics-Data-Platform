/* =================================================================================================
FILE:         02_sdi_vw_dashboardPulseTms_silver_appendix_long.sql
LAYER:        Dimension View - Silver
VIEW NAME:    sdi_vw_dashboardPulseTms_silver_appendix_long

PURPOSE:
  Unpivots Bronze's wide apx_build_detail_raw and apx_key_exclusions_raw text blocks into one
  row per header/bullet, so the dashboard's expandable detail cards (and any downstream reader)
  can render segment-stacking logic as a real list instead of a single text blob.

  Splits on the "[Layer N - description]" bracket markers already present in Bronze's text
  (see Bronze header CHANGE LOG) - every paragraph in apx_build_detail_raw is separated by a
  blank line, and paragraphs that open with a bracketed header get that header split out as
  apx_section_label; paragraphs with no bracket header (freeform lead-in text like "Source: ..."
  or trailing formula notes like "CVR%: ...") get apx_section_label = NULL and the whole
  paragraph as apx_section_detail.

  apx_key_exclusions_raw is almost always a single flat paragraph (no bracket structure) in this
  content, so it typically unpivots to exactly one EXCLUSION row per Metric apx_id - this is
  expected, not a parsing failure.

  Glossary rows have no build/exclusion text at all - they pass through as a single
  GLOSSARY_DEFINITION row each, term as apx_section_label, definition as apx_section_detail.

GRAIN:
  One row per apx_id x apx_section_type x apx_section_seq.

KEY COLUMNS:
  apx_section_type   - 'BUILD' | 'EXCLUSION' | 'GLOSSARY_DEFINITION'
  apx_section_seq    - 0-based position within this apx_id's section_type (paragraph order
                       preserved from Bronze's text)
  apx_section_label  - the "[Layer N - ...]" header text with brackets stripped, or the
                       glossary term for GLOSSARY_DEFINITION rows, NULL for unheadered paragraphs
  apx_section_detail - the paragraph body with its header line removed (BUILD/EXCLUSION), or the
                       glossary definition (GLOSSARY_DEFINITION)

  All Bronze dimension columns (apx_record_type, apx_funnel_stage, apx_category,
  apx_metric_name, apx_is_subflow, apx_parent_id, apx_summable_to_parent, apx_vp_one_liner,
  apx_warning_message, apx_referenced_glossary_ids, apx_sort_order) are carried through
  unchanged on every row. apx_warning_message rides along here purely as a passthrough - Gold
  is where it actually gets consumed (Section 2b), unpivoting doesn't touch its content.

PORTING NOTE:
  Uses split() + posexplode() rather than a Python/Scala UDF, kept to plain SQL so this stays a
  view with no external dependency. regexp_extract pulls the bracket header off the front of
  each paragraph if one exists; regexp_replace strips it from the detail text.
================================================================================================= */

CREATE OR REPLACE VIEW
  prdrzranalytics.lab42.sdi_vw_dashboardPulseTms_silver_appendix_long
AS

WITH

BuildUnpivoted AS (
  SELECT
    b.apx_id, b.apx_record_type, b.apx_funnel_stage, b.apx_category, b.apx_metric_name,
    b.apx_flow_scope, b.apx_is_subflow, b.apx_parent_id, b.apx_summable_to_parent,
    b.apx_data_source_label, b.apx_source_table, b.apx_source_owner, b.apx_vp_one_liner, b.apx_warning_message,
    b.apx_refresh_cadence, b.apx_referenced_glossary_ids, b.apx_sort_order,
    'BUILD'                                                                AS apx_section_type,
    build_seq                                                              AS apx_section_seq,
    NULLIF(regexp_extract(build_chunk, '^\\[(.*?)\\]', 1), '')             AS apx_section_label,
    TRIM(regexp_replace(build_chunk, '^\\[.*?\\]\\s*\\n?', ''))            AS apx_section_detail
  FROM prdrzranalytics.lab42.sdi_vw_dashboardPulseTms_bronze_appendix_wide b
  LATERAL VIEW posexplode(split(b.apx_build_detail_raw, '\n\n')) exploded_build AS build_seq, build_chunk
  WHERE b.apx_record_type = 'Metric'
    AND b.apx_build_detail_raw IS NOT NULL
    AND TRIM(build_chunk) != ''
),

ExclusionUnpivoted AS (
  SELECT
    b.apx_id, b.apx_record_type, b.apx_funnel_stage, b.apx_category, b.apx_metric_name,
    b.apx_flow_scope, b.apx_is_subflow, b.apx_parent_id, b.apx_summable_to_parent,
    b.apx_data_source_label, b.apx_source_table, b.apx_source_owner, b.apx_vp_one_liner, b.apx_warning_message,
    b.apx_refresh_cadence, b.apx_referenced_glossary_ids, b.apx_sort_order,
    'EXCLUSION'                                                            AS apx_section_type,
    excl_seq                                                               AS apx_section_seq,
    NULLIF(regexp_extract(excl_chunk, '^\\[(.*?)\\]', 1), '')              AS apx_section_label,
    TRIM(regexp_replace(excl_chunk, '^\\[.*?\\]\\s*\\n?', ''))             AS apx_section_detail
  FROM prdrzranalytics.lab42.sdi_vw_dashboardPulseTms_bronze_appendix_wide b
  LATERAL VIEW posexplode(split(b.apx_key_exclusions_raw, '\n\n')) exploded_excl AS excl_seq, excl_chunk
  WHERE b.apx_record_type = 'Metric'
    AND b.apx_key_exclusions_raw IS NOT NULL
    AND TRIM(excl_chunk) != ''
),

GlossaryUnpivoted AS (
  SELECT
    b.apx_id, b.apx_record_type, b.apx_funnel_stage, b.apx_category, b.apx_metric_name,
    b.apx_flow_scope, b.apx_is_subflow, b.apx_parent_id, b.apx_summable_to_parent,
    b.apx_data_source_label, b.apx_source_table, b.apx_source_owner, b.apx_vp_one_liner, b.apx_warning_message,
    b.apx_refresh_cadence, b.apx_referenced_glossary_ids, b.apx_sort_order,
    'GLOSSARY_DEFINITION'                                                  AS apx_section_type,
    0                                                                      AS apx_section_seq,
    b.apx_glossary_term                                                   AS apx_section_label,
    b.apx_glossary_definition                                             AS apx_section_detail
  FROM prdrzranalytics.lab42.sdi_vw_dashboardPulseTms_bronze_appendix_wide b
  WHERE b.apx_record_type = 'Glossary'
)

SELECT * FROM BuildUnpivoted
UNION ALL SELECT * FROM ExclusionUnpivoted
UNION ALL SELECT * FROM GlossaryUnpivoted
;