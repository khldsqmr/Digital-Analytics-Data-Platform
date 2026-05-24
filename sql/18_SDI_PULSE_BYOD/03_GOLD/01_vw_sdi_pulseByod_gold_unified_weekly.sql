/* =================================================================================================
FILE:         01_vw_sdi_pulseByod_gold_unified_weekly.sql
LAYER:        Gold View
DATASET:      prj-dbi-prd-1.ds_dbi_digitalmedia_automation
VIEW NAME:    vw_sdi_pulseByod_gold_unified_weekly

SOURCES:
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_silver_profound_weekly
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_silver_profoundGofish_weekly
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_silver_sa360_weekly
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_silver_gsc_weekly
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_silver_googleTrends_weekly

DESTINATION:
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_gold_unified_weekly

PURPOSE:
  Unified Gold long table for the Pulse BYOD dashboard.
  Combines all 5 Silver sources into a single long table with a consistent schema.
  Each metric from each source becomes its own row with a descriptive metric_name.
  Brand and asset context is baked into metric_name for SA360, GSC, and Profound.
  Keywords from Google Trends are represented with dimension_name and dimension_value
  to support dynamic Top N Keywords visualization in the dashboard.
  max_data_date per data_source enables freshness indicators in the dashboard.

OUTPUT SCHEMA:
  week_sun_to_sat  : DATE    — Week ending Saturday (Sun-to-Sat week)
  data_source      : STRING  — Source short code: PROFOUND, GOFISH, SA360, GSC, TRENDS
  metric_name      : STRING  — Descriptive metric identifier (see METRIC NAMES below)
  metric_value     : FLOAT64 — Numeric metric value
  max_data_date    : DATE    — Latest week_sun_to_sat with non-null metric_value per source
  dimension_name   : STRING  — Dimension type: KEYWORD for Trends keywords, NULL otherwise
  dimension_value  : STRING  — Dimension value: keyword text for Trends, NULL otherwise

METRIC NAMES:
  PROFOUND source:
    tmo_nonbrand_visibility_score
    verizon_nonbrand_visibility_score
    att_nonbrand_visibility_score
    tmo_nonbrand_executions
    verizon_nonbrand_executions
    att_nonbrand_executions
    tmo_nonbrand_mentions_count
    verizon_nonbrand_mentions_count
    att_nonbrand_mentions_count
    tmo_nonbrand_share_of_voice
    verizon_nonbrand_share_of_voice
    att_nonbrand_share_of_voice

  GOFISH source:
    tmo_brand_visibility_score
    verizon_brand_visibility_score
    att_brand_visibility_score
    tmo_brand_executions
    verizon_brand_executions
    att_brand_executions
    tmo_brand_mentions_count
    verizon_brand_mentions_count
    att_brand_mentions_count
    tmo_brand_share_of_voice
    verizon_brand_share_of_voice
    att_brand_share_of_voice

  SA360 source:
    tmo_brand_impressions
    tmo_brand_clicks
    tmo_brand_cost
    tmo_brand_orders
    tmo_brand_cart_start
    tmo_brand_postpaid_pspv
    tmo_nonbrand_impressions
    tmo_nonbrand_clicks
    tmo_nonbrand_cost
    tmo_nonbrand_orders
    tmo_nonbrand_cart_start
    tmo_nonbrand_postpaid_pspv

  GSC source:
    tmo_brand_impressions
    tmo_brand_clicks
    tmo_nonbrand_impressions
    tmo_nonbrand_clicks

  TRENDS source:
    byod_index
    kw_interest     (dimension_name = KEYWORD, dimension_value = keyword text)
    kw_wow_change   (dimension_name = KEYWORD, dimension_value = keyword text)

KEY MODELING NOTES:
  - asset_name mapping for Profound/GoFish metric names:
      'T-Mobile' → 'tmo'
      'Verizon'  → 'verizon'
      'AT&T'     → 'att'
  - brand_type mapping for SA360/GSC metric names:
      'BRAND'    → 'brand'
      'NONBRAND' → 'nonbrand'
  - max_data_date computed as MAX(week_sun_to_sat) per data_source
    using a window function — updates dynamically as new data arrives
  - NULLs preserved — no COALESCE to zero applied
  - Trends Silver already outputs in long format — Gold just adds
    data_source and max_data_date columns
  - SA360 and GSC share metric names (e.g. tmo_brand_impressions) —
    data_source column distinguishes them in the unified table

AUTHOR:       Pulse BYOD Team
CREATED:      2026-05-24
================================================================================================= */

CREATE OR REPLACE VIEW `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_gold_unified_weekly`
AS

-- -----------------------------------------------------------------------
-- PROFOUND: NON-BRAND AI Visibility
-- Unpivot visibility_score, executions, mentions_count, share_of_voice
-- per asset_name (T-Mobile, Verizon, AT&T)
-- metric_name = {asset_prefix}_nonbrand_{metric}
-- -----------------------------------------------------------------------
WITH profound AS (
    SELECT
        week_sun_to_sat,
        'PROFOUND'                                                          AS data_source,
        CONCAT(
            CASE asset_name
                WHEN 'T-Mobile' THEN 'tmo'
                WHEN 'Verizon'  THEN 'verizon'
                WHEN 'AT&T'     THEN 'att'
            END,
            '_nonbrand_visibility_score'
        )                                                                   AS metric_name,
        visibility_score                                                    AS metric_value,
        NULL                                                                AS dimension_name,
        NULL                                                                AS dimension_value
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_silver_profound_weekly`

    UNION ALL

    SELECT
        week_sun_to_sat,
        'PROFOUND'                                                          AS data_source,
        CONCAT(
            CASE asset_name
                WHEN 'T-Mobile' THEN 'tmo'
                WHEN 'Verizon'  THEN 'verizon'
                WHEN 'AT&T'     THEN 'att'
            END,
            '_nonbrand_executions'
        )                                                                   AS metric_name,
        executions                                                          AS metric_value,
        NULL                                                                AS dimension_name,
        NULL                                                                AS dimension_value
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_silver_profound_weekly`

    UNION ALL

    SELECT
        week_sun_to_sat,
        'PROFOUND'                                                          AS data_source,
        CONCAT(
            CASE asset_name
                WHEN 'T-Mobile' THEN 'tmo'
                WHEN 'Verizon'  THEN 'verizon'
                WHEN 'AT&T'     THEN 'att'
            END,
            '_nonbrand_mentions_count'
        )                                                                   AS metric_name,
        mentions_count                                                      AS metric_value,
        NULL                                                                AS dimension_name,
        NULL                                                                AS dimension_value
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_silver_profound_weekly`

    UNION ALL

    SELECT
        week_sun_to_sat,
        'PROFOUND'                                                          AS data_source,
        CONCAT(
            CASE asset_name
                WHEN 'T-Mobile' THEN 'tmo'
                WHEN 'Verizon'  THEN 'verizon'
                WHEN 'AT&T'     THEN 'att'
            END,
            '_nonbrand_share_of_voice'
        )                                                                   AS metric_name,
        share_of_voice                                                      AS metric_value,
        NULL                                                                AS dimension_name,
        NULL                                                                AS dimension_value
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_silver_profound_weekly`
),

-- -----------------------------------------------------------------------
-- GOFISH: BRAND AI Visibility
-- Unpivot visibility_score, executions, mentions_count, share_of_voice
-- per asset_name (T-Mobile, Verizon, AT&T)
-- metric_name = {asset_prefix}_brand_{metric}
-- -----------------------------------------------------------------------
gofish AS (
    SELECT
        week_sun_to_sat,
        'GOFISH'                                                            AS data_source,
        CONCAT(
            CASE asset_name
                WHEN 'T-Mobile' THEN 'tmo'
                WHEN 'Verizon'  THEN 'verizon'
                WHEN 'AT&T'     THEN 'att'
            END,
            '_brand_visibility_score'
        )                                                                   AS metric_name,
        visibility_score                                                    AS metric_value,
        NULL                                                                AS dimension_name,
        NULL                                                                AS dimension_value
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_silver_profoundGofish_weekly`

    UNION ALL

    SELECT
        week_sun_to_sat,
        'GOFISH'                                                            AS data_source,
        CONCAT(
            CASE asset_name
                WHEN 'T-Mobile' THEN 'tmo'
                WHEN 'Verizon'  THEN 'verizon'
                WHEN 'AT&T'     THEN 'att'
            END,
            '_brand_executions'
        )                                                                   AS metric_name,
        executions                                                          AS metric_value,
        NULL                                                                AS dimension_name,
        NULL                                                                AS dimension_value
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_silver_profoundGofish_weekly`

    UNION ALL

    SELECT
        week_sun_to_sat,
        'GOFISH'                                                            AS data_source,
        CONCAT(
            CASE asset_name
                WHEN 'T-Mobile' THEN 'tmo'
                WHEN 'Verizon'  THEN 'verizon'
                WHEN 'AT&T'     THEN 'att'
            END,
            '_brand_mentions_count'
        )                                                                   AS metric_name,
        mentions_count                                                      AS metric_value,
        NULL                                                                AS dimension_name,
        NULL                                                                AS dimension_value
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_silver_profoundGofish_weekly`

    UNION ALL

    SELECT
        week_sun_to_sat,
        'GOFISH'                                                            AS data_source,
        CONCAT(
            CASE asset_name
                WHEN 'T-Mobile' THEN 'tmo'
                WHEN 'Verizon'  THEN 'verizon'
                WHEN 'AT&T'     THEN 'att'
            END,
            '_brand_share_of_voice'
        )                                                                   AS metric_name,
        share_of_voice                                                      AS metric_value,
        NULL                                                                AS dimension_name,
        NULL                                                                AS dimension_value
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_silver_profoundGofish_weekly`
),

-- -----------------------------------------------------------------------
-- SA360: Paid Search Performance
-- Unpivot impressions, clicks, cost, orders, cart_start, postpaid_pspv
-- per brand_type (BRAND, NONBRAND)
-- metric_name = tmo_{brand_type_lower}_{metric}
-- -----------------------------------------------------------------------
sa360 AS (
    SELECT
        week_sun_to_sat,
        'SA360'                                                             AS data_source,
        CONCAT('tmo_', LOWER(brand_type), '_impressions')                  AS metric_name,
        impressions                                                         AS metric_value,
        NULL                                                                AS dimension_name,
        NULL                                                                AS dimension_value
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_silver_sa360_weekly`

    UNION ALL

    SELECT
        week_sun_to_sat,
        'SA360'                                                             AS data_source,
        CONCAT('tmo_', LOWER(brand_type), '_clicks')                       AS metric_name,
        clicks                                                              AS metric_value,
        NULL                                                                AS dimension_name,
        NULL                                                                AS dimension_value
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_silver_sa360_weekly`

    UNION ALL

    SELECT
        week_sun_to_sat,
        'SA360'                                                             AS data_source,
        CONCAT('tmo_', LOWER(brand_type), '_cost')                         AS metric_name,
        cost                                                                AS metric_value,
        NULL                                                                AS dimension_name,
        NULL                                                                AS dimension_value
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_silver_sa360_weekly`

    UNION ALL

    SELECT
        week_sun_to_sat,
        'SA360'                                                             AS data_source,
        CONCAT('tmo_', LOWER(brand_type), '_orders')                       AS metric_name,
        postpaid_prospect_web_order                                         AS metric_value,
        NULL                                                                AS dimension_name,
        NULL                                                                AS dimension_value
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_silver_sa360_weekly`

    UNION ALL

    SELECT
        week_sun_to_sat,
        'SA360'                                                             AS data_source,
        CONCAT('tmo_', LOWER(brand_type), '_cart_start')                   AS metric_name,
        postpaid_cart_start                                                 AS metric_value,
        NULL                                                                AS dimension_name,
        NULL                                                                AS dimension_value
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_silver_sa360_weekly`

    UNION ALL

    SELECT
        week_sun_to_sat,
        'SA360'                                                             AS data_source,
        CONCAT('tmo_', LOWER(brand_type), '_postpaid_pspv')                AS metric_name,
        postpaid_pspv                                                       AS metric_value,
        NULL                                                                AS dimension_name,
        NULL                                                                AS dimension_value
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_silver_sa360_weekly`
),

-- -----------------------------------------------------------------------
-- GSC: Organic Search Performance
-- Unpivot impressions, clicks
-- per brand_type (BRAND, NONBRAND)
-- metric_name = tmo_{brand_type_lower}_{metric}
-- -----------------------------------------------------------------------
gsc AS (
    SELECT
        week_sun_to_sat,
        'GSC'                                                               AS data_source,
        CONCAT('tmo_', LOWER(brand_type), '_impressions')                  AS metric_name,
        impressions                                                         AS metric_value,
        NULL                                                                AS dimension_name,
        NULL                                                                AS dimension_value
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_silver_gsc_weekly`

    UNION ALL

    SELECT
        week_sun_to_sat,
        'GSC'                                                               AS data_source,
        CONCAT('tmo_', LOWER(brand_type), '_clicks')                       AS metric_name,
        clicks                                                              AS metric_value,
        NULL                                                                AS dimension_name,
        NULL                                                                AS dimension_value
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_silver_gsc_weekly`
),

-- -----------------------------------------------------------------------
-- TRENDS: Market-Level BYOD Interest + Top N Keywords
-- Silver already outputs in long format — just add data_source
-- byod_index rows: dimension_name = NULL, dimension_value = NULL
-- keyword rows   : dimension_name = KEYWORD, dimension_value = keyword text
-- -----------------------------------------------------------------------
trends AS (
    SELECT
        week_sun_to_sat,
        'TRENDS'                                                            AS data_source,
        metric_name,
        metric_value,
        dimension_name,
        dimension_value
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_silver_googleTrends_weekly`
),

-- -----------------------------------------------------------------------
-- UNION ALL: Combine all sources into single long table
-- -----------------------------------------------------------------------
unioned AS (
    SELECT * FROM profound
    UNION ALL
    SELECT * FROM gofish
    UNION ALL
    SELECT * FROM sa360
    UNION ALL
    SELECT * FROM gsc
    UNION ALL
    SELECT * FROM trends
),

-- -----------------------------------------------------------------------
-- MAX DATA DATE: Latest week_sun_to_sat with non-null metric_value per source
-- Computed as a window function — updates dynamically as new data arrives
-- -----------------------------------------------------------------------
with_max_date AS (
    SELECT
        week_sun_to_sat,
        data_source,
        metric_name,
        metric_value,
        dimension_name,
        dimension_value,
        MAX(CASE WHEN metric_value IS NOT NULL THEN week_sun_to_sat END)
            OVER (PARTITION BY data_source)                                 AS max_data_date
    FROM unioned
)

-- -----------------------------------------------------------------------
-- FINAL OUTPUT
-- -----------------------------------------------------------------------
SELECT
    week_sun_to_sat,
    data_source,
    metric_name,
    metric_value,
    max_data_date,
    dimension_name,
    dimension_value
FROM with_max_date
ORDER BY
    week_sun_to_sat  ASC,
    data_source      ASC,
    metric_name      ASC,
    dimension_value  ASC
;