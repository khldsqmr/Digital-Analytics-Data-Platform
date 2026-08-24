/*
  01_sdi_vw_dashboardPulseTms_silver_appendix_long.sql
  ------------------------------------------------------------------------
  PulseTMS dashboard appendix (Silver): unpivots Bronze's wide metric rows
  into a long/tall structure -- one row per (metric/glossary item, bullet)
  -- for the dashboard's expandable metric-detail cards. Ported from
  BigQuery's vw_sdi_pulse_silver_pulseTab1_appendix.

  Four row shapes, stacked via UNION ALL and distinguished by apx_row_type:
    - metric_header / glossary_header : one row per apx_id (all 49),
      carries every Bronze column as-is.
    - bullet_build   : one row per \n-delimited entry in apx_build_labels /
      apx_build_details, Metric rows only.
    - bullet_excl    : same pattern, apx_excl_labels / apx_excl_details.
    - bullet_source  : exactly 4 fixed rows per Metric (SOURCE / TABLE /
      OWNER / REFRESH), built from apx_source_system / apx_source_table /
      apx_data_owner / apx_refresh_cadence.

  Port notes (BQ -> Databricks) -- every change below, and why:

  1. SAFE_OFFSET(pos) -> try_element_at(array, pos)
     BQ's SAFE_OFFSET is 0-indexed and null-safe (out-of-range -> NULL,
     never an error). Databricks' array indexing is ANSI-mode-dependent
     (can throw on out-of-range with `arr[i]`), so this uses
     try_element_at() instead, which is guaranteed null-safe regardless
     of the workspace's ANSI setting -- the direct, safer equivalent.
     try_element_at is 1-indexed, so `pos` itself is generated 1-indexed
     below (see #2) rather than carrying BQ's 0-indexed pos + 1 offset
     arithmetic through every reference.

  2. GENERATE_ARRAY(0, ARRAY_LENGTH(x) - 1) -> sequence(1, size(x))
     ARRAY_LENGTH -> size() is a straight rename. The start bound changes
     from 0 to 1 to match try_element_at's 1-indexing from #1, so
     apx_bullet_order = pos directly (no +1 needed, unlike the BQ source).
     Wrapped in coalesce(..., cast(array() as array<int>)) so a future row
     with a NULL apx_build_labels/apx_excl_labels can never make
     sequence()/explode() see a NULL array -- it degrades to zero bullet
     rows for that item instead of erroring. The explicit array<int> cast
     on the empty-array fallback matters: an untyped array() can fail to
     unify against sequence()'s array<int> output inside coalesce(). Not
     reachable today (validated: every Metric row's labels/details are
     populated with matching bullet counts) but cheap, permanent
     insurance for the next person who adds a row.

  3. CROSS JOIN UNNEST(<array>) AS pos -> LATERAL VIEW explode(<array>) AS pos
     Databricks' generator-function syntax. Same fan-out semantics.

  4. CROSS JOIN UNNEST([STRUCT(...), STRUCT(...), ...]) -> LATERAL VIEW
     INLINE(array(named_struct(...), named_struct(...), ...))
     INLINE is Databricks' direct equivalent for exploding an
     array<struct> into named columns in one step -- same job BQ's
     UNNEST(ARRAY<STRUCT>) was doing for the 4 fixed SOURCE/TABLE/OWNER/
     REFRESH rows.

  5. INT64 -> BIGINT, BOOL -> BOOLEAN
     Straight type-name renames -- INT64 and BOOL are not valid Databricks
     SQL type names (BOOL in particular will fail to parse, not just
     behave differently).

  6. apx_funnel_sort / apx_category_sort explicitly cast to BIGINT when
     read from Bronze in the `headers` branch below. Bronze's own integer
     literals default to INT (32-bit) in Databricks, but the other three
     branches (build_bullets/excl_bullets/source_bullets) NULL these
     columns out as BIGINT to match apx_bullet_order's type -- so without
     this cast, `headers` would hand INT down one UNION ALL branch while
     the other three hand BIGINT, which Spark will silently widen for you,
     but pinning it explicitly removes any ambiguity rather than relying
     on implicit promotion.

  7. `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulse_bronze_
     pulseTab1_appendix` -> the fully-qualified Unity Catalog name of the
     ported Bronze view above (prdrzranalytics.lab42.sdi_vw_dashboard
     PulseTms_bronze_appendix_wide -- adjust the catalog/schema below if
     yours differs). IFNULL and SPLIT (with '\n' as the delimiter) behave
     identically in both engines and needed no change.
*/

CREATE OR REPLACE VIEW prdrzranalytics.lab42.sdi_vw_dashboardPulseTms_silver_appendix_long
COMMENT 'PulseTMS dashboard appendix (Silver): unpivoted long/tall structure, one row per header or bullet, for the expandable metric-detail cards.'
AS
with b as (

    select * from prdrzranalytics.lab42.sdi_vw_dashboardPulseTms_bronze_appendix_wide

),

headers as (

    select
        b.apx_sort_order,
        case b.apx_record_type
            when 'Metric' then 'metric_header'
            else               'glossary_header'
        end                              as apx_row_type,
        b.apx_id,
        b.apx_label,
        b.apx_record_type,
        b.apx_funnel_stage,
        cast(b.apx_funnel_sort as bigint)   as apx_funnel_sort,
        b.apx_category,
        cast(b.apx_category_sort as bigint) as apx_category_sort,
        b.apx_scope,
        b.apx_lob,
        b.apx_is_subflow,
        b.apx_parent_id,
        b.apx_subflows_summable,
        b.apx_source_system,
        b.apx_source_platform,
        b.apx_source_table,
        b.apx_data_owner,
        b.apx_refresh_cadence,
        b.apx_definition,
        b.apx_sum_warning,
        b.apx_cvr_numerator_id,
        b.apx_cvr_denominator_id,
        b.apx_glossary_category,
        cast(null as bigint) as apx_bullet_order,
        cast(null as string) as apx_bullet_label,
        cast(null as string) as apx_bullet_detail
    from b

),

build_bullets as (

    select
        b.apx_sort_order,
        'bullet_build'          as apx_row_type,
        b.apx_id,
        cast(null as string)    as apx_label,
        cast(null as string)    as apx_record_type,
        cast(null as string)    as apx_funnel_stage,
        cast(null as bigint)    as apx_funnel_sort,
        cast(null as string)    as apx_category,
        cast(null as bigint)    as apx_category_sort,
        cast(null as string)    as apx_scope,
        cast(null as string)    as apx_lob,
        cast(null as boolean)   as apx_is_subflow,
        cast(null as string)    as apx_parent_id,
        cast(null as boolean)   as apx_subflows_summable,
        cast(null as string)    as apx_source_system,
        cast(null as string)    as apx_source_platform,
        cast(null as string)    as apx_source_table,
        cast(null as string)    as apx_data_owner,
        cast(null as string)    as apx_refresh_cadence,
        cast(null as string)    as apx_definition,
        cast(null as string)    as apx_sum_warning,
        cast(null as string)    as apx_cvr_numerator_id,
        cast(null as string)    as apx_cvr_denominator_id,
        cast(null as string)    as apx_glossary_category,
        cast(pos as bigint)                                     as apx_bullet_order,
        try_element_at(split(b.apx_build_labels,  '\n'), pos)    as apx_bullet_label,
        try_element_at(split(b.apx_build_details, '\n'), pos)    as apx_bullet_detail
    from b
    lateral view explode(
        coalesce(sequence(1, size(split(b.apx_build_labels, '\n'))), cast(array() as array<int>))
    ) exploded_build_pos as pos
    where b.apx_record_type = 'Metric'
      and b.apx_build_labels is not null

),

excl_bullets as (

    select
        b.apx_sort_order,
        'bullet_excl'            as apx_row_type,
        b.apx_id,
        cast(null as string)     as apx_label,
        cast(null as string)     as apx_record_type,
        cast(null as string)     as apx_funnel_stage,
        cast(null as bigint)     as apx_funnel_sort,
        cast(null as string)     as apx_category,
        cast(null as bigint)     as apx_category_sort,
        cast(null as string)     as apx_scope,
        cast(null as string)     as apx_lob,
        cast(null as boolean)    as apx_is_subflow,
        cast(null as string)     as apx_parent_id,
        cast(null as boolean)    as apx_subflows_summable,
        cast(null as string)     as apx_source_system,
        cast(null as string)     as apx_source_platform,
        cast(null as string)     as apx_source_table,
        cast(null as string)     as apx_data_owner,
        cast(null as string)     as apx_refresh_cadence,
        cast(null as string)     as apx_definition,
        cast(null as string)     as apx_sum_warning,
        cast(null as string)     as apx_cvr_numerator_id,
        cast(null as string)     as apx_cvr_denominator_id,
        cast(null as string)     as apx_glossary_category,
        cast(pos as bigint)                                    as apx_bullet_order,
        try_element_at(split(b.apx_excl_labels,  '\n'), pos)   as apx_bullet_label,
        try_element_at(split(b.apx_excl_details, '\n'), pos)   as apx_bullet_detail
    from b
    lateral view explode(
        coalesce(sequence(1, size(split(b.apx_excl_labels, '\n'))), cast(array() as array<int>))
    ) exploded_excl_pos as pos
    where b.apx_record_type = 'Metric'
      and b.apx_excl_labels is not null

),

source_bullets as (

    select
        b.apx_sort_order,
        'bullet_source'          as apx_row_type,
        b.apx_id,
        cast(null as string)     as apx_label,
        cast(null as string)     as apx_record_type,
        cast(null as string)     as apx_funnel_stage,
        cast(null as bigint)     as apx_funnel_sort,
        cast(null as string)     as apx_category,
        cast(null as bigint)     as apx_category_sort,
        cast(null as string)     as apx_scope,
        cast(null as string)     as apx_lob,
        cast(null as boolean)    as apx_is_subflow,
        cast(null as string)     as apx_parent_id,
        cast(null as boolean)    as apx_subflows_summable,
        cast(null as string)     as apx_source_system,
        cast(null as string)     as apx_source_platform,
        cast(null as string)     as apx_source_table,
        cast(null as string)     as apx_data_owner,
        cast(null as string)     as apx_refresh_cadence,
        cast(null as string)     as apx_definition,
        cast(null as string)     as apx_sum_warning,
        cast(null as string)     as apx_cvr_numerator_id,
        cast(null as string)     as apx_cvr_denominator_id,
        cast(null as string)     as apx_glossary_category,
        cast(src.bullet_order as bigint) as apx_bullet_order,
        src.bullet_label                 as apx_bullet_label,
        src.bullet_detail                as apx_bullet_detail
    from b
    lateral view inline(
        array(
            named_struct('bullet_order', 1, 'bullet_label', 'SOURCE',  'bullet_detail', ifnull(b.apx_source_system,   '—')),
            named_struct('bullet_order', 2, 'bullet_label', 'TABLE',   'bullet_detail', ifnull(b.apx_source_table,    '—')),
            named_struct('bullet_order', 3, 'bullet_label', 'OWNER',   'bullet_detail', ifnull(b.apx_data_owner,      '—')),
            named_struct('bullet_order', 4, 'bullet_label', 'REFRESH', 'bullet_detail', ifnull(b.apx_refresh_cadence, '—'))
        )
    ) src as bullet_order, bullet_label, bullet_detail
    where b.apx_record_type = 'Metric'

)

select * from headers
union all select * from build_bullets
union all select * from excl_bullets
union all select * from source_bullets
order by apx_sort_order, apx_row_type, apx_bullet_order;