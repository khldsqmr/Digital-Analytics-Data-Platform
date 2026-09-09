/* =================================================================================================
FILE:         02_sdi_vw_dashboardPulseTms_silver_appendix_long.sql
LAYER:        Dimension View - Silver
VIEW NAME:    sdi_vw_dashboardPulseTms_silver_appendix_long

PURPOSE:
  Unpivots Bronze's wide apx_build_detail_raw and apx_key_exclusions_raw text blocks into one
  row per header/bullet, so the dashboard's expandable detail cards (and any downstream reader)
  can render segment-stacking logic as a real list instead of a single text blob. Also unpivots
  the four fixed source fields (data source, table, owner, refresh cadence) into their own
  section rows, so Tableau's Source panel uses the identical Rows-shelf mechanism as the Build
  and Exclusions panels instead of a special-cased single concatenated Text mark.

  Splits on the "[Layer N - description]" bracket markers already present in Bronze's text
  (see Bronze header CHANGE LOG) - every paragraph in apx_build_detail_raw is separated by a
  blank line, and paragraphs that open with a bracketed header get that header split out as
  apx_section_label; paragraphs with no bracket header (freeform lead-in text like "Source: ..."
  or trailing formula notes like "CVR%: ...") get apx_section_label = NULL and the whole
  paragraph as apx_section_detail.

  apx_key_exclusions_raw is almost always a single flat paragraph (no bracket structure) in this
  content, so it typically unpivots to exactly one EXCLUSION row per Metric apx_id - this is
  expected, not a parsing failure.

  Glossary rows have no build/exclusion/source text at all - they pass through as a single
  GLOSSARY_DEFINITION row each, term as apx_section_label, definition as apx_section_detail.

  The SOURCE rows are structurally different from BUILD/EXCLUSION/GLOSSARY_DEFINITION: those
  three all come from splitting a free-text blob on blank lines via split()/regexp_extract().
  SOURCE instead unpivots four already-separate, already-clean Bronze columns
  (apx_data_source_label, apx_source_table, apx_source_owner, apx_refresh_cadence) via a plain
  4-way UNION ALL with hardcoded labels, the same unpivot shape this pipeline already uses
  elsewhere (e.g. silver_qgp_weekly's own metric unpivot) - there's no text to parse, so there's
  nothing for a bracket regex to do here.

GRAIN:
  One row per apx_id x apx_section_type x apx_section_seq.

KEY COLUMNS:
  apx_section_type   - 'BUILD' | 'EXCLUSION' | 'GLOSSARY_DEFINITION' | 'SOURCE'
  apx_section_seq    - 0-based position within this apx_id's section_type (paragraph order
                       preserved from Bronze's text for BUILD/EXCLUSION; a fixed
                       Source=0/Table=1/Owner=2/Refresh=3 order for SOURCE)
  apx_section_label  - the "[Layer N - ...]" header text with brackets stripped (BUILD/
                       EXCLUSION), the glossary term (GLOSSARY_DEFINITION), or one of
                       'Source'/'Table'/'Owner'/'Refresh' (SOURCE) - NULL only on unheadered
                       BUILD/EXCLUSION paragraphs, never NULL for SOURCE rows
  apx_section_detail - the paragraph body with its header line removed (BUILD/EXCLUSION), the
                       glossary definition (GLOSSARY_DEFINITION), or the corresponding source
                       field's value (SOURCE) - apx_source_table falls back to the literal
                       'Not yet live' when NULL (currently only eligibilityChecksCompleted),
                       matching the fallback the old apx.Source Block calculated field used to
                       apply in Tableau, now handled here instead

  All Bronze dimension columns (apx_record_type, apx_funnel_stage, apx_category,
  apx_metric_name, apx_is_subflow, apx_parent_id, apx_summable_to_parent, apx_vp_one_liner,
  apx_warning_message, apx_referenced_glossary_ids, apx_sort_order) are carried through
  unchanged on every row. apx_warning_message rides along here purely as a passthrough - Gold
  is where it actually gets consumed (Section 2b), unpivoting doesn't touch its content.

PORTING NOTE:
  BUILD/EXCLUSION use split() + posexplode() rather than a Python/Scala UDF, kept to plain SQL
  so this stays a view with no external dependency. regexp_extract pulls the bracket header off
  the front of each paragraph if one exists; regexp_replace strips it from the detail text.
  SOURCE needs none of that, it's a literal column unpivot, no regex involved.

CHANGE LOG:
  - Added SourceUnpivoted (4 rows per Metric apx_id: Source, Table, Owner, Refresh), replacing
    the single-calculated-field Source Block approach in Tableau with the same row-based
    pattern Build and Exclusions already use. WS - Source in the Tableau build guide updates to
    match: filter on apx_section_type = 'SOURCE', Rows shelf apx_section_seq (hidden) +
    apx_section_label, Text mark = apx_section_detail - no Seq0 filter needed here, all four
    SOURCE rows are wanted, not just row 0.
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
),

-- SourceUnpivoted: unlike BUILD/EXCLUSION, these four fields are already separate, clean
-- database columns in Bronze, not a text blob needing split()/regexp_extract() -- so this is a
-- plain 4-way UNION ALL of named columns, the same unpivot shape used elsewhere in this
-- pipeline (e.g. silver_qgp_weekly's own metric unpivot), not a text-parsing operation. One row
-- per source fact, in a fixed, always-4-row order (Source, Table, Owner, Refresh), so Section
-- 2c renders through the exact same Rows-shelf mechanism as Sections 3 and 4 in Tableau instead
-- of a single concatenated Text mark.
SourceUnpivoted AS (
  SELECT
    b.apx_id, b.apx_record_type, b.apx_funnel_stage, b.apx_category, b.apx_metric_name,
    b.apx_flow_scope, b.apx_is_subflow, b.apx_parent_id, b.apx_summable_to_parent,
    b.apx_data_source_label, b.apx_source_table, b.apx_source_owner, b.apx_vp_one_liner, b.apx_warning_message,
    b.apx_refresh_cadence, b.apx_referenced_glossary_ids, b.apx_sort_order,
    'SOURCE'                                                               AS apx_section_type,
    0                                                                      AS apx_section_seq,
    'Source'                                                               AS apx_section_label,
    b.apx_data_source_label                                                AS apx_section_detail
  FROM prdrzranalytics.lab42.sdi_vw_dashboardPulseTms_bronze_appendix_wide b
  WHERE b.apx_record_type = 'Metric'

  UNION ALL

  SELECT
    b.apx_id, b.apx_record_type, b.apx_funnel_stage, b.apx_category, b.apx_metric_name,
    b.apx_flow_scope, b.apx_is_subflow, b.apx_parent_id, b.apx_summable_to_parent,
    b.apx_data_source_label, b.apx_source_table, b.apx_source_owner, b.apx_vp_one_liner, b.apx_warning_message,
    b.apx_refresh_cadence, b.apx_referenced_glossary_ids, b.apx_sort_order,
    'SOURCE'                                                               AS apx_section_type,
    1                                                                      AS apx_section_seq,
    'Table'                                                                AS apx_section_label,
    IFNULL(b.apx_source_table, 'Not yet live')                             AS apx_section_detail
  FROM prdrzranalytics.lab42.sdi_vw_dashboardPulseTms_bronze_appendix_wide b
  WHERE b.apx_record_type = 'Metric'

  UNION ALL

  SELECT
    b.apx_id, b.apx_record_type, b.apx_funnel_stage, b.apx_category, b.apx_metric_name,
    b.apx_flow_scope, b.apx_is_subflow, b.apx_parent_id, b.apx_summable_to_parent,
    b.apx_data_source_label, b.apx_source_table, b.apx_source_owner, b.apx_vp_one_liner, b.apx_warning_message,
    b.apx_refresh_cadence, b.apx_referenced_glossary_ids, b.apx_sort_order,
    'SOURCE'                                                               AS apx_section_type,
    2                                                                      AS apx_section_seq,
    'Owner'                                                                AS apx_section_label,
    b.apx_source_owner                                                    AS apx_section_detail
  FROM prdrzranalytics.lab42.sdi_vw_dashboardPulseTms_bronze_appendix_wide b
  WHERE b.apx_record_type = 'Metric'

  UNION ALL

  SELECT
    b.apx_id, b.apx_record_type, b.apx_funnel_stage, b.apx_category, b.apx_metric_name,
    b.apx_flow_scope, b.apx_is_subflow, b.apx_parent_id, b.apx_summable_to_parent,
    b.apx_data_source_label, b.apx_source_table, b.apx_source_owner, b.apx_vp_one_liner, b.apx_warning_message,
    b.apx_refresh_cadence, b.apx_referenced_glossary_ids, b.apx_sort_order,
    'SOURCE'                                                               AS apx_section_type,
    3                                                                      AS apx_section_seq,
    'Refresh'                                                              AS apx_section_label,
    b.apx_refresh_cadence                                                 AS apx_section_detail
  FROM prdrzranalytics.lab42.sdi_vw_dashboardPulseTms_bronze_appendix_wide b
  WHERE b.apx_record_type = 'Metric'
)

SELECT * FROM BuildUnpivoted
UNION ALL SELECT * FROM ExclusionUnpivoted
UNION ALL SELECT * FROM GlossaryUnpivoted
UNION ALL SELECT * FROM SourceUnpivoted
;