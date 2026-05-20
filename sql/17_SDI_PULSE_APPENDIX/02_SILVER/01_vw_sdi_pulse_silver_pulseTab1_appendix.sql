CREATE OR REPLACE VIEW `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulse_silver_pulseTab1_appendix`
OPTIONS(description="Silver: structural transformation of bronze. Unpivots build/excl/source bullets into one row per bullet using GENERATE_ARRAY + SAFE_OFFSET zip. apx_row_type: metric_header | glossary_header | bullet_build | bullet_excl | bullet_source. Join all bullet types to header rows on apx_id.")
AS
WITH
b AS (
  SELECT * FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulse_bronze_pulseTab1_appendix`
),
headers AS (
  SELECT
    apx_sort_order,
    CASE apx_record_type
      WHEN 'Metric'   THEN 'metric_header'
      ELSE                 'glossary_header'
    END                        AS apx_row_type,
    apx_id, apx_label, apx_record_type,
    apx_funnel_stage, apx_funnel_sort,
    apx_category, apx_category_sort,
    apx_scope, apx_lob,
    apx_is_subflow, apx_parent_id, apx_subflows_summable,
    apx_source_system, apx_source_platform,
    apx_source_table, apx_data_owner, apx_refresh_cadence,
    apx_definition, apx_sum_warning,
    apx_cvr_numerator_id, apx_cvr_denominator_id,
    apx_glossary_category,
    CAST(NULL AS INT64)  AS apx_bullet_order,
    CAST(NULL AS STRING) AS apx_bullet_label,
    CAST(NULL AS STRING) AS apx_bullet_detail
  FROM b
),
build_bullets AS (
  SELECT
    b.apx_sort_order,
    'bullet_build'               AS apx_row_type,
    b.apx_id,
    CAST(NULL AS STRING)         AS apx_label,
    CAST(NULL AS STRING)         AS apx_record_type,
    CAST(NULL AS STRING)         AS apx_funnel_stage,
    CAST(NULL AS INT64)          AS apx_funnel_sort,
    CAST(NULL AS STRING)         AS apx_category,
    CAST(NULL AS INT64)          AS apx_category_sort,
    CAST(NULL AS STRING)         AS apx_scope,
    CAST(NULL AS STRING)         AS apx_lob,
    CAST(NULL AS BOOL)           AS apx_is_subflow,
    CAST(NULL AS STRING)         AS apx_parent_id,
    CAST(NULL AS BOOL)           AS apx_subflows_summable,
    CAST(NULL AS STRING)         AS apx_source_system,
    CAST(NULL AS STRING)         AS apx_source_platform,
    CAST(NULL AS STRING)         AS apx_source_table,
    CAST(NULL AS STRING)         AS apx_data_owner,
    CAST(NULL AS STRING)         AS apx_refresh_cadence,
    CAST(NULL AS STRING)         AS apx_definition,
    CAST(NULL AS STRING)         AS apx_sum_warning,
    CAST(NULL AS STRING)         AS apx_cvr_numerator_id,
    CAST(NULL AS STRING)         AS apx_cvr_denominator_id,
    CAST(NULL AS STRING)         AS apx_glossary_category,
    pos + 1                      AS apx_bullet_order,
    SAFE_OFFSET(SPLIT(b.apx_build_labels,'\n'), pos) AS apx_bullet_label,
    SAFE_OFFSET(SPLIT(b.apx_build_details,'\n'), pos) AS apx_bullet_detail
  FROM b
  CROSS JOIN UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(SPLIT(b.apx_build_labels,'\n')) - 1)) AS pos
  WHERE b.apx_record_type = 'Metric'
    AND b.apx_build_labels IS NOT NULL
),
excl_bullets AS (
  SELECT
    b.apx_sort_order,
    'bullet_excl'                AS apx_row_type,
    b.apx_id,
    CAST(NULL AS STRING)         AS apx_label,
    CAST(NULL AS STRING)         AS apx_record_type,
    CAST(NULL AS STRING)         AS apx_funnel_stage,
    CAST(NULL AS INT64)          AS apx_funnel_sort,
    CAST(NULL AS STRING)         AS apx_category,
    CAST(NULL AS INT64)          AS apx_category_sort,
    CAST(NULL AS STRING)         AS apx_scope,
    CAST(NULL AS STRING)         AS apx_lob,
    CAST(NULL AS BOOL)           AS apx_is_subflow,
    CAST(NULL AS STRING)         AS apx_parent_id,
    CAST(NULL AS BOOL)           AS apx_subflows_summable,
    CAST(NULL AS STRING)         AS apx_source_system,
    CAST(NULL AS STRING)         AS apx_source_platform,
    CAST(NULL AS STRING)         AS apx_source_table,
    CAST(NULL AS STRING)         AS apx_data_owner,
    CAST(NULL AS STRING)         AS apx_refresh_cadence,
    CAST(NULL AS STRING)         AS apx_definition,
    CAST(NULL AS STRING)         AS apx_sum_warning,
    CAST(NULL AS STRING)         AS apx_cvr_numerator_id,
    CAST(NULL AS STRING)         AS apx_cvr_denominator_id,
    CAST(NULL AS STRING)         AS apx_glossary_category,
    pos + 1                      AS apx_bullet_order,
    SAFE_OFFSET(SPLIT(b.apx_excl_labels,'\n'), pos) AS apx_bullet_label,
    SAFE_OFFSET(SPLIT(b.apx_excl_details,'\n'), pos) AS apx_bullet_detail
  FROM b
  CROSS JOIN UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(SPLIT(b.apx_excl_labels,'\n')) - 1)) AS pos
  WHERE b.apx_record_type = 'Metric'
    AND b.apx_excl_labels IS NOT NULL
),
source_bullets AS (
  SELECT
    b.apx_sort_order,
    'bullet_source'              AS apx_row_type,
    b.apx_id,
    CAST(NULL AS STRING)         AS apx_label,
    CAST(NULL AS STRING)         AS apx_record_type,
    CAST(NULL AS STRING)         AS apx_funnel_stage,
    CAST(NULL AS INT64)          AS apx_funnel_sort,
    CAST(NULL AS STRING)         AS apx_category,
    CAST(NULL AS INT64)          AS apx_category_sort,
    CAST(NULL AS STRING)         AS apx_scope,
    CAST(NULL AS STRING)         AS apx_lob,
    CAST(NULL AS BOOL)           AS apx_is_subflow,
    CAST(NULL AS STRING)         AS apx_parent_id,
    CAST(NULL AS BOOL)           AS apx_subflows_summable,
    CAST(NULL AS STRING)         AS apx_source_system,
    CAST(NULL AS STRING)         AS apx_source_platform,
    CAST(NULL AS STRING)         AS apx_source_table,
    CAST(NULL AS STRING)         AS apx_data_owner,
    CAST(NULL AS STRING)         AS apx_refresh_cadence,
    CAST(NULL AS STRING)         AS apx_definition,
    CAST(NULL AS STRING)         AS apx_sum_warning,
    CAST(NULL AS STRING)         AS apx_cvr_numerator_id,
    CAST(NULL AS STRING)         AS apx_cvr_denominator_id,
    CAST(NULL AS STRING)         AS apx_glossary_category,
    src.bullet_order             AS apx_bullet_order,
    src.bullet_label             AS apx_bullet_label,
    src.bullet_detail            AS apx_bullet_detail
  FROM b
  CROSS JOIN UNNEST([
    STRUCT(1 AS bullet_order, 'SOURCE'   AS bullet_label, IFNULL(b.apx_source_system,'—')   AS bullet_detail),
    STRUCT(2,                 'TABLE',                    IFNULL(b.apx_source_table,'—')),
    STRUCT(3,                 'OWNER',                    IFNULL(b.apx_data_owner,'—')),
    STRUCT(4,                 'REFRESH',                  IFNULL(b.apx_refresh_cadence,'—'))
  ]) AS src
  WHERE b.apx_record_type = 'Metric'
)
SELECT * FROM headers
UNION ALL SELECT * FROM build_bullets
UNION ALL SELECT * FROM excl_bullets
UNION ALL SELECT * FROM source_bullets
ORDER BY apx_sort_order, apx_row_type, apx_bullet_order;