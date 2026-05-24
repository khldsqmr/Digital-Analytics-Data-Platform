/* =================================================================================================
FILE:         05_vw_sdi_pulseByod_silver_googleTrends_weekly.sql
LAYER:        Silver View
DATASET:      prj-dbi-prd-1.ds_dbi_digitalmedia_automation
VIEW NAME:    vw_sdi_pulseByod_silver_googleTrends_weekly

SOURCE:
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_bronze_googleTrends_weekly

DESTINATION:
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_silver_googleTrends_weekly

PURPOSE:
  Silver view for Google Trends BYOD weekly search interest data.
  Applies week-end Saturday conversion, preserves byod_index as-is,
  and unpivots the wide keyword columns (kw1-kw5) into long format
  so each keyword becomes its own row. This powers the Top N Keywords
  visualization in the dashboard.
  Output has two row types per week:
    1. One row for byod_index (no keyword dimension)
    2. Up to 5 rows for keywords (one per keyword with interest and wow_change)

BUSINESS GRAIN:
  One row per:
    week_sun_to_sat + metric_name + dimension_value
  where dimension_value is NULL for byod_index rows
  and the keyword text for keyword rows

FILTERS APPLIED:
  - Keyword rows only included where keyword text is not null and not empty
    (filters out pre-2026-05-09 rows where keyword data is not yet populated)

BUSINESS LOGIC APPLIED:
  - week_sun_to_sat = DATE_ADD(event_date_sun, INTERVAL 6 DAY)
  - Keyword unpivot: wide columns (top_kw_1 through top_kw_5) → long rows
  - keyword_rank assigned 1-5 to preserve original ordering from source

KEY MODELING NOTES:
  - byod_index rows have dimension_name = NULL, dimension_value = NULL
  - keyword rows have dimension_name = 'KEYWORD', dimension_value = keyword text
  - kw_interest and kw_wow_change are keyword-level metrics, NULL for byod_index rows
  - Keywords preserve natural language casing (e.g. 'byod', 'bring your device')
    as they are human-readable labels, not programmatic identifiers
  - Pre-2026-05-09 keyword data excluded via NULLIF check — byod_index still
    included for those weeks since it is reliable across full history

DOWNSTREAM:
  Gold : vw_sdi_pulseByod_gold_unified_weekly
================================================================================================= */

CREATE OR REPLACE VIEW `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_silver_googleTrends_weekly`
AS

WITH base AS (
    SELECT
        -- Week-end Saturday conversion
        DATE_ADD(event_date_sun, INTERVAL 6 DAY)    AS week_sun_to_sat,
        byod_index,
        top_kw_1, kw1_interest, kw1_change,
        top_kw_2, kw2_interest, kw2_change,
        top_kw_3, kw3_interest, kw3_change,
        top_kw_4, kw4_interest, kw4_change,
        top_kw_5, kw5_interest, kw5_change
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_bronze_googleTrends_weekly`
),

-- byod_index rows: one per week, no keyword dimension
byod_index_rows AS (
    SELECT
        week_sun_to_sat,
        'byod_index'        AS metric_name,
        byod_index          AS metric_value,
        NULL                AS dimension_name,
        NULL                AS dimension_value,
        NULL                AS kw_interest,
        NULL                AS kw_wow_change,
        NULL                AS keyword_rank
    FROM base
),

-- Keyword rows: unpivot wide keyword columns into long format
-- Each keyword rank becomes its own row with interest and wow_change
-- Only included where keyword text is not null/empty (post-2026-05-09)
keyword_rows AS (

    -- Keyword rank 1
    SELECT
        week_sun_to_sat,
        'kw_interest'       AS metric_name,
        kw1_interest        AS metric_value,
        'KEYWORD'           AS dimension_name,
        top_kw_1            AS dimension_value,
        kw1_interest        AS kw_interest,
        kw1_change          AS kw_wow_change,
        1                   AS keyword_rank
    FROM base
    WHERE NULLIF(TRIM(top_kw_1), '') IS NOT NULL

    UNION ALL

    -- Keyword rank 2
    SELECT
        week_sun_to_sat,
        'kw_interest'       AS metric_name,
        kw2_interest        AS metric_value,
        'KEYWORD'           AS dimension_name,
        top_kw_2            AS dimension_value,
        kw2_interest        AS kw_interest,
        kw2_change          AS kw_wow_change,
        2                   AS keyword_rank
    FROM base
    WHERE NULLIF(TRIM(top_kw_2), '') IS NOT NULL

    UNION ALL

    -- Keyword rank 3
    SELECT
        week_sun_to_sat,
        'kw_interest'       AS metric_name,
        kw3_interest        AS metric_value,
        'KEYWORD'           AS dimension_name,
        top_kw_3            AS dimension_value,
        kw3_interest        AS kw_interest,
        kw3_change          AS kw_wow_change,
        3                   AS keyword_rank
    FROM base
    WHERE NULLIF(TRIM(top_kw_3), '') IS NOT NULL

    UNION ALL

    -- Keyword rank 4
    SELECT
        week_sun_to_sat,
        'kw_interest'       AS metric_name,
        kw4_interest        AS metric_value,
        'KEYWORD'           AS dimension_name,
        top_kw_4            AS dimension_value,
        kw4_interest        AS kw_interest,
        kw4_change          AS kw_wow_change,
        4                   AS keyword_rank
    FROM base
    WHERE NULLIF(TRIM(top_kw_4), '') IS NOT NULL

    UNION ALL

    -- Keyword rank 5
    SELECT
        week_sun_to_sat,
        'kw_interest'       AS metric_name,
        kw5_interest        AS metric_value,
        'KEYWORD'           AS dimension_name,
        top_kw_5            AS dimension_value,
        kw5_interest        AS kw_interest,
        kw5_change          AS kw_wow_change,
        5                   AS keyword_rank
    FROM base
    WHERE NULLIF(TRIM(top_kw_5), '') IS NOT NULL
),

-- Keyword wow_change rows: separate rows for wow_change metric per keyword
kw_change_rows AS (

    -- Keyword rank 1 wow_change
    SELECT
        week_sun_to_sat,
        'kw_wow_change'     AS metric_name,
        kw1_change          AS metric_value,
        'KEYWORD'           AS dimension_name,
        top_kw_1            AS dimension_value,
        kw1_interest        AS kw_interest,
        kw1_change          AS kw_wow_change,
        1                   AS keyword_rank
    FROM base
    WHERE NULLIF(TRIM(top_kw_1), '') IS NOT NULL

    UNION ALL

    -- Keyword rank 2 wow_change
    SELECT
        week_sun_to_sat,
        'kw_wow_change'     AS metric_name,
        kw2_change          AS metric_value,
        'KEYWORD'           AS dimension_name,
        top_kw_2            AS dimension_value,
        kw2_interest        AS kw_interest,
        kw2_change          AS kw_wow_change,
        2                   AS keyword_rank
    FROM base
    WHERE NULLIF(TRIM(top_kw_2), '') IS NOT NULL

    UNION ALL

    -- Keyword rank 3 wow_change
    SELECT
        week_sun_to_sat,
        'kw_wow_change'     AS metric_name,
        kw3_change          AS metric_value,
        'KEYWORD'           AS dimension_name,
        top_kw_3            AS dimension_value,
        kw3_interest        AS kw_interest,
        kw3_change          AS kw_wow_change,
        3                   AS keyword_rank
    FROM base
    WHERE NULLIF(TRIM(top_kw_3), '') IS NOT NULL

    UNION ALL

    -- Keyword rank 4 wow_change
    SELECT
        week_sun_to_sat,
        'kw_wow_change'     AS metric_name,
        kw4_change          AS metric_value,
        'KEYWORD'           AS dimension_name,
        top_kw_4            AS dimension_value,
        kw4_interest        AS kw_interest,
        kw4_change          AS kw_wow_change,
        4                   AS keyword_rank
    FROM base
    WHERE NULLIF(TRIM(top_kw_4), '') IS NOT NULL

    UNION ALL

    -- Keyword rank 5 wow_change
    SELECT
        week_sun_to_sat,
        'kw_wow_change'     AS metric_name,
        kw5_change          AS metric_value,
        'KEYWORD'           AS dimension_name,
        top_kw_5            AS dimension_value,
        kw5_interest        AS kw_interest,
        kw5_change          AS kw_wow_change,
        5                   AS keyword_rank
    FROM base
    WHERE NULLIF(TRIM(top_kw_5), '') IS NOT NULL
)

-- Combine byod_index rows + keyword interest rows + keyword wow_change rows
SELECT
    week_sun_to_sat,
    metric_name,
    metric_value,
    dimension_name,
    dimension_value,
    kw_interest,
    kw_wow_change,
    keyword_rank
FROM byod_index_rows

UNION ALL

SELECT
    week_sun_to_sat,
    metric_name,
    metric_value,
    dimension_name,
    dimension_value,
    kw_interest,
    kw_wow_change,
    keyword_rank
FROM keyword_rows

UNION ALL

SELECT
    week_sun_to_sat,
    metric_name,
    metric_value,
    dimension_name,
    dimension_value,
    kw_interest,
    kw_wow_change,
    keyword_rank
FROM kw_change_rows

ORDER BY week_sun_to_sat ASC, keyword_rank ASC, metric_name ASC
;