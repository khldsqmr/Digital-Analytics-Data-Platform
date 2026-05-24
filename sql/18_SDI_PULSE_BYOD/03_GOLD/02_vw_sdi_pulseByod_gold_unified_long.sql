/* =================================================================================================
FILE:         02_vw_sdi_pulseByod_gold_unified_long.sql
LAYER:        Gold View — Long
DATASET:      prj-dbi-prd-1.ds_dbi_digitalmedia_automation
VIEW NAME:    vw_sdi_pulseByod_gold_unified_long

SOURCES:
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_silver_profound_weekly
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_silver_profoundGofish_weekly
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_silver_sa360_weekly
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_silver_gsc_weekly
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_silver_googleTrends_weekly

DESTINATION:
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_gold_unified_long

PURPOSE:
  Gold Long view for the Pulse BYOD dashboard.
  Reads directly from Silver views — NOT from Gold Wide.
  Each Silver view is unpivoted independently and combined via UNION ALL.
  This ensures each source is scanned exactly once — fast and cheap.
  One row per metric per week.
  time_granularity = 'WEEKLY' for self-describing schema.
  Keywords unpivoted from Trends Silver for Top N Keywords visualization.

OUTPUT SCHEMA:
  week_sun_to_sat  : DATE    — Week ending Saturday (Sun-to-Sat)
  time_granularity : STRING  — Always 'WEEKLY'
  data_source      : STRING  — PROFOUND, GOFISH, SA360, GSC, TRENDS
  channel          : STRING  — AI SEARCH, PAID SEARCH, ORGANIC SEARCH
  dimension_name   : STRING  — KEYWORD for Trends keywords, NULL otherwise
  dimension_value  : STRING  — Keyword text for Trends, NULL otherwise
  metric_name      : STRING  — Full prefixed metric name
  metric_value     : FLOAT64 — Current week metric value
  metric_value_wow : FLOAT64 — Prior week value
  metric_value_ly  : FLOAT64 — Same Sun-to-Sat week last year
  wow_pct          : FLOAT64 — WoW% as decimal
  yoy_pct          : FLOAT64 — YoY% as decimal
  max_data_date    : DATE    — Latest week with non-null data per data_source

PERFORMANCE NOTES:
  - Each Silver view scanned exactly once — no re-scanning
  - No Gold Wide dependency — independent of spine join
  - Each source unpivoted in its own CTE
  - UNION ALL at the end combines all sources
  - max_data_date per data_source via window function on final output

ADDING NEW SOURCES:
  1. Create Bronze + Silver views
  2. Add new unpivot CTE below
  3. Add to final UNION ALL
  No structural changes needed

AUTHOR:       Pulse BYOD Team
CREATED:      2026-05-24
================================================================================================= */

CREATE OR REPLACE VIEW `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_gold_unified_long`
AS

-- -----------------------------------------------------------------------
-- Each source is unpivoted in its own CTE
-- Silver views are small (1 row per week after aggregation)
-- so unpivoting here is cheap
-- -----------------------------------------------------------------------
WITH

-- -----------------------------------------------------------------------
-- PROFOUND: NON-BRAND AI Visibility
-- 12 metric rows per week (4 metrics × 3 assets)
-- -----------------------------------------------------------------------
profound_long AS (
    SELECT
        week_sun_to_sat,
        data_source,
        channel,
        max_data_date,
        metric_name,
        metric_value,
        metric_value_wow,
        metric_value_ly,
        wow_pct,
        yoy_pct
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_silver_profound_weekly`
    UNPIVOT (
        (metric_value, metric_value_wow, metric_value_ly, wow_pct, yoy_pct)
        FOR metric_name IN (
            (profound_tmo_nonbrand_visibility_score,    profound_tmo_nonbrand_visibility_score_wow,    profound_tmo_nonbrand_visibility_score_ly,    profound_tmo_nonbrand_visibility_score_wow_pct,    profound_tmo_nonbrand_visibility_score_yoy_pct)    AS 'profound_tmo_nonbrand_visibility_score',
            (profound_tmo_nonbrand_executions,          profound_tmo_nonbrand_executions_wow,          profound_tmo_nonbrand_executions_ly,          profound_tmo_nonbrand_executions_wow_pct,          profound_tmo_nonbrand_executions_yoy_pct)          AS 'profound_tmo_nonbrand_executions',
            (profound_tmo_nonbrand_mentions_count,      profound_tmo_nonbrand_mentions_count_wow,      profound_tmo_nonbrand_mentions_count_ly,      profound_tmo_nonbrand_mentions_count_wow_pct,      profound_tmo_nonbrand_mentions_count_yoy_pct)      AS 'profound_tmo_nonbrand_mentions_count',
            (profound_tmo_nonbrand_share_of_voice,      profound_tmo_nonbrand_share_of_voice_wow,      profound_tmo_nonbrand_share_of_voice_ly,      profound_tmo_nonbrand_share_of_voice_wow_pct,      profound_tmo_nonbrand_share_of_voice_yoy_pct)      AS 'profound_tmo_nonbrand_share_of_voice',
            (profound_verizon_nonbrand_visibility_score,profound_verizon_nonbrand_visibility_score_wow,profound_verizon_nonbrand_visibility_score_ly,profound_verizon_nonbrand_visibility_score_wow_pct,profound_verizon_nonbrand_visibility_score_yoy_pct) AS 'profound_verizon_nonbrand_visibility_score',
            (profound_verizon_nonbrand_executions,      profound_verizon_nonbrand_executions_wow,      profound_verizon_nonbrand_executions_ly,      profound_verizon_nonbrand_executions_wow_pct,      profound_verizon_nonbrand_executions_yoy_pct)      AS 'profound_verizon_nonbrand_executions',
            (profound_verizon_nonbrand_mentions_count,  profound_verizon_nonbrand_mentions_count_wow,  profound_verizon_nonbrand_mentions_count_ly,  profound_verizon_nonbrand_mentions_count_wow_pct,  profound_verizon_nonbrand_mentions_count_yoy_pct)  AS 'profound_verizon_nonbrand_mentions_count',
            (profound_verizon_nonbrand_share_of_voice,  profound_verizon_nonbrand_share_of_voice_wow,  profound_verizon_nonbrand_share_of_voice_ly,  profound_verizon_nonbrand_share_of_voice_wow_pct,  profound_verizon_nonbrand_share_of_voice_yoy_pct)  AS 'profound_verizon_nonbrand_share_of_voice',
            (profound_att_nonbrand_visibility_score,    profound_att_nonbrand_visibility_score_wow,    profound_att_nonbrand_visibility_score_ly,    profound_att_nonbrand_visibility_score_wow_pct,    profound_att_nonbrand_visibility_score_yoy_pct)    AS 'profound_att_nonbrand_visibility_score',
            (profound_att_nonbrand_executions,          profound_att_nonbrand_executions_wow,          profound_att_nonbrand_executions_ly,          profound_att_nonbrand_executions_wow_pct,          profound_att_nonbrand_executions_yoy_pct)          AS 'profound_att_nonbrand_executions',
            (profound_att_nonbrand_mentions_count,      profound_att_nonbrand_mentions_count_wow,      profound_att_nonbrand_mentions_count_ly,      profound_att_nonbrand_mentions_count_wow_pct,      profound_att_nonbrand_mentions_count_yoy_pct)      AS 'profound_att_nonbrand_mentions_count',
            (profound_att_nonbrand_share_of_voice,      profound_att_nonbrand_share_of_voice_wow,      profound_att_nonbrand_share_of_voice_ly,      profound_att_nonbrand_share_of_voice_wow_pct,      profound_att_nonbrand_share_of_voice_yoy_pct)      AS 'profound_att_nonbrand_share_of_voice'
        )
    )
),

-- -----------------------------------------------------------------------
-- GOFISH: BRAND AI Visibility
-- 12 metric rows per week (4 metrics × 3 assets)
-- -----------------------------------------------------------------------
gofish_long AS (
    SELECT
        week_sun_to_sat,
        data_source,
        channel,
        max_data_date,
        metric_name,
        metric_value,
        metric_value_wow,
        metric_value_ly,
        wow_pct,
        yoy_pct
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_silver_profoundGofish_weekly`
    UNPIVOT (
        (metric_value, metric_value_wow, metric_value_ly, wow_pct, yoy_pct)
        FOR metric_name IN (
            (gofish_tmo_brand_visibility_score,    gofish_tmo_brand_visibility_score_wow,    gofish_tmo_brand_visibility_score_ly,    gofish_tmo_brand_visibility_score_wow_pct,    gofish_tmo_brand_visibility_score_yoy_pct)    AS 'gofish_tmo_brand_visibility_score',
            (gofish_tmo_brand_executions,          gofish_tmo_brand_executions_wow,          gofish_tmo_brand_executions_ly,          gofish_tmo_brand_executions_wow_pct,          gofish_tmo_brand_executions_yoy_pct)          AS 'gofish_tmo_brand_executions',
            (gofish_tmo_brand_mentions_count,      gofish_tmo_brand_mentions_count_wow,      gofish_tmo_brand_mentions_count_ly,      gofish_tmo_brand_mentions_count_wow_pct,      gofish_tmo_brand_mentions_count_yoy_pct)      AS 'gofish_tmo_brand_mentions_count',
            (gofish_tmo_brand_share_of_voice,      gofish_tmo_brand_share_of_voice_wow,      gofish_tmo_brand_share_of_voice_ly,      gofish_tmo_brand_share_of_voice_wow_pct,      gofish_tmo_brand_share_of_voice_yoy_pct)      AS 'gofish_tmo_brand_share_of_voice',
            (gofish_verizon_brand_visibility_score,gofish_verizon_brand_visibility_score_wow,gofish_verizon_brand_visibility_score_ly,gofish_verizon_brand_visibility_score_wow_pct,gofish_verizon_brand_visibility_score_yoy_pct) AS 'gofish_verizon_brand_visibility_score',
            (gofish_verizon_brand_executions,      gofish_verizon_brand_executions_wow,      gofish_verizon_brand_executions_ly,      gofish_verizon_brand_executions_wow_pct,      gofish_verizon_brand_executions_yoy_pct)      AS 'gofish_verizon_brand_executions',
            (gofish_verizon_brand_mentions_count,  gofish_verizon_brand_mentions_count_wow,  gofish_verizon_brand_mentions_count_ly,  gofish_verizon_brand_mentions_count_wow_pct,  gofish_verizon_brand_mentions_count_yoy_pct)  AS 'gofish_verizon_brand_mentions_count',
            (gofish_verizon_brand_share_of_voice,  gofish_verizon_brand_share_of_voice_wow,  gofish_verizon_brand_share_of_voice_ly,  gofish_verizon_brand_share_of_voice_wow_pct,  gofish_verizon_brand_share_of_voice_yoy_pct)  AS 'gofish_verizon_brand_share_of_voice',
            (gofish_att_brand_visibility_score,    gofish_att_brand_visibility_score_wow,    gofish_att_brand_visibility_score_ly,    gofish_att_brand_visibility_score_wow_pct,    gofish_att_brand_visibility_score_yoy_pct)    AS 'gofish_att_brand_visibility_score',
            (gofish_att_brand_executions,          gofish_att_brand_executions_wow,          gofish_att_brand_executions_ly,          gofish_att_brand_executions_wow_pct,          gofish_att_brand_executions_yoy_pct)          AS 'gofish_att_brand_executions',
            (gofish_att_brand_mentions_count,      gofish_att_brand_mentions_count_wow,      gofish_att_brand_mentions_count_ly,      gofish_att_brand_mentions_count_wow_pct,      gofish_att_brand_mentions_count_yoy_pct)      AS 'gofish_att_brand_mentions_count',
            (gofish_att_brand_share_of_voice,      gofish_att_brand_share_of_voice_wow,      gofish_att_brand_share_of_voice_ly,      gofish_att_brand_share_of_voice_wow_pct,      gofish_att_brand_share_of_voice_yoy_pct)      AS 'gofish_att_brand_share_of_voice'
        )
    )
),

-- -----------------------------------------------------------------------
-- SA360: Paid Search Performance
-- 12 metric rows per week (6 metrics × 2 brand types)
-- -----------------------------------------------------------------------
sa360_long AS (
    SELECT
        week_sun_to_sat,
        data_source,
        channel,
        max_data_date,
        metric_name,
        metric_value,
        metric_value_wow,
        metric_value_ly,
        wow_pct,
        yoy_pct
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_silver_sa360_weekly`
    UNPIVOT (
        (metric_value, metric_value_wow, metric_value_ly, wow_pct, yoy_pct)
        FOR metric_name IN (
            (sa360_tmo_brand_impressions,    sa360_tmo_brand_impressions_wow,    sa360_tmo_brand_impressions_ly,    sa360_tmo_brand_impressions_wow_pct,    sa360_tmo_brand_impressions_yoy_pct)    AS 'sa360_tmo_brand_impressions',
            (sa360_tmo_brand_clicks,         sa360_tmo_brand_clicks_wow,         sa360_tmo_brand_clicks_ly,         sa360_tmo_brand_clicks_wow_pct,         sa360_tmo_brand_clicks_yoy_pct)         AS 'sa360_tmo_brand_clicks',
            (sa360_tmo_brand_cost,           sa360_tmo_brand_cost_wow,           sa360_tmo_brand_cost_ly,           sa360_tmo_brand_cost_wow_pct,           sa360_tmo_brand_cost_yoy_pct)           AS 'sa360_tmo_brand_cost',
            (sa360_tmo_brand_orders,         sa360_tmo_brand_orders_wow,         sa360_tmo_brand_orders_ly,         sa360_tmo_brand_orders_wow_pct,         sa360_tmo_brand_orders_yoy_pct)         AS 'sa360_tmo_brand_orders',
            (sa360_tmo_brand_cart_start,     sa360_tmo_brand_cart_start_wow,     sa360_tmo_brand_cart_start_ly,     sa360_tmo_brand_cart_start_wow_pct,     sa360_tmo_brand_cart_start_yoy_pct)     AS 'sa360_tmo_brand_cart_start',
            (sa360_tmo_brand_postpaid_pspv,  sa360_tmo_brand_postpaid_pspv_wow,  sa360_tmo_brand_postpaid_pspv_ly,  sa360_tmo_brand_postpaid_pspv_wow_pct,  sa360_tmo_brand_postpaid_pspv_yoy_pct)  AS 'sa360_tmo_brand_postpaid_pspv',
            (sa360_tmo_nonbrand_impressions, sa360_tmo_nonbrand_impressions_wow, sa360_tmo_nonbrand_impressions_ly, sa360_tmo_nonbrand_impressions_wow_pct, sa360_tmo_nonbrand_impressions_yoy_pct) AS 'sa360_tmo_nonbrand_impressions',
            (sa360_tmo_nonbrand_clicks,      sa360_tmo_nonbrand_clicks_wow,      sa360_tmo_nonbrand_clicks_ly,      sa360_tmo_nonbrand_clicks_wow_pct,      sa360_tmo_nonbrand_clicks_yoy_pct)      AS 'sa360_tmo_nonbrand_clicks',
            (sa360_tmo_nonbrand_cost,        sa360_tmo_nonbrand_cost_wow,        sa360_tmo_nonbrand_cost_ly,        sa360_tmo_nonbrand_cost_wow_pct,        sa360_tmo_nonbrand_cost_yoy_pct)        AS 'sa360_tmo_nonbrand_cost',
            (sa360_tmo_nonbrand_orders,      sa360_tmo_nonbrand_orders_wow,      sa360_tmo_nonbrand_orders_ly,      sa360_tmo_nonbrand_orders_wow_pct,      sa360_tmo_nonbrand_orders_yoy_pct)      AS 'sa360_tmo_nonbrand_orders',
            (sa360_tmo_nonbrand_cart_start,  sa360_tmo_nonbrand_cart_start_wow,  sa360_tmo_nonbrand_cart_start_ly,  sa360_tmo_nonbrand_cart_start_wow_pct,  sa360_tmo_nonbrand_cart_start_yoy_pct)  AS 'sa360_tmo_nonbrand_cart_start',
            (sa360_tmo_nonbrand_postpaid_pspv,sa360_tmo_nonbrand_postpaid_pspv_wow,sa360_tmo_nonbrand_postpaid_pspv_ly,sa360_tmo_nonbrand_postpaid_pspv_wow_pct,sa360_tmo_nonbrand_postpaid_pspv_yoy_pct) AS 'sa360_tmo_nonbrand_postpaid_pspv'
        )
    )
),

-- -----------------------------------------------------------------------
-- GSC: Organic Search Performance
-- 4 metric rows per week (2 metrics × 2 brand types)
-- -----------------------------------------------------------------------
gsc_long AS (
    SELECT
        week_sun_to_sat,
        data_source,
        channel,
        max_data_date,
        metric_name,
        metric_value,
        metric_value_wow,
        metric_value_ly,
        wow_pct,
        yoy_pct
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_silver_gsc_weekly`
    UNPIVOT (
        (metric_value, metric_value_wow, metric_value_ly, wow_pct, yoy_pct)
        FOR metric_name IN (
            (gsc_tmo_brand_impressions,    gsc_tmo_brand_impressions_wow,    gsc_tmo_brand_impressions_ly,    gsc_tmo_brand_impressions_wow_pct,    gsc_tmo_brand_impressions_yoy_pct)    AS 'gsc_tmo_brand_impressions',
            (gsc_tmo_brand_clicks,         gsc_tmo_brand_clicks_wow,         gsc_tmo_brand_clicks_ly,         gsc_tmo_brand_clicks_wow_pct,         gsc_tmo_brand_clicks_yoy_pct)         AS 'gsc_tmo_brand_clicks',
            (gsc_tmo_nonbrand_impressions, gsc_tmo_nonbrand_impressions_wow, gsc_tmo_nonbrand_impressions_ly, gsc_tmo_nonbrand_impressions_wow_pct, gsc_tmo_nonbrand_impressions_yoy_pct) AS 'gsc_tmo_nonbrand_impressions',
            (gsc_tmo_nonbrand_clicks,      gsc_tmo_nonbrand_clicks_wow,      gsc_tmo_nonbrand_clicks_ly,      gsc_tmo_nonbrand_clicks_wow_pct,      gsc_tmo_nonbrand_clicks_yoy_pct)      AS 'gsc_tmo_nonbrand_clicks'
        )
    )
),

-- -----------------------------------------------------------------------
-- TRENDS: byod_index
-- 1 metric row per week
-- -----------------------------------------------------------------------
trends_index_long AS (
    SELECT
        week_sun_to_sat,
        data_source,
        channel,
        max_data_date,
        metric_name,
        metric_value,
        metric_value_wow,
        metric_value_ly,
        wow_pct,
        yoy_pct
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_silver_googleTrends_weekly`
    UNPIVOT (
        (metric_value, metric_value_wow, metric_value_ly, wow_pct, yoy_pct)
        FOR metric_name IN (
            (trends_byod_index, trends_byod_index_wow, trends_byod_index_ly,
             trends_byod_index_wow_pct, trends_byod_index_yoy_pct)
            AS 'trends_byod_index'
        )
    )
),

-- -----------------------------------------------------------------------
-- TRENDS: Keywords unpivoted
-- Up to 10 rows per week (5 keywords × 2 metrics: interest + wow_change)
-- No WoW/LY — keywords change week to week
-- dimension_name = 'KEYWORD', dimension_value = keyword text
-- -----------------------------------------------------------------------
trends_keywords_long AS (

    -- Keyword rank 1 — interest
    SELECT week_sun_to_sat, data_source, channel, max_data_date,
        'KEYWORD' AS dimension_name, trends_top_kw_1 AS dimension_value,
        'trends_kw_interest' AS metric_name, trends_kw1_interest AS metric_value,
        CAST(NULL AS FLOAT64) AS metric_value_wow, CAST(NULL AS FLOAT64) AS metric_value_ly,
        CAST(NULL AS FLOAT64) AS wow_pct, CAST(NULL AS FLOAT64) AS yoy_pct
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_silver_googleTrends_weekly`
    WHERE NULLIF(TRIM(trends_top_kw_1), '') IS NOT NULL

    UNION ALL SELECT week_sun_to_sat, data_source, channel, max_data_date,
        'KEYWORD', trends_top_kw_1,
        'trends_kw_wow_change', trends_kw1_change,
        CAST(NULL AS FLOAT64), CAST(NULL AS FLOAT64),
        CAST(NULL AS FLOAT64), CAST(NULL AS FLOAT64)
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_silver_googleTrends_weekly`
    WHERE NULLIF(TRIM(trends_top_kw_1), '') IS NOT NULL

    UNION ALL SELECT week_sun_to_sat, data_source, channel, max_data_date,
        'KEYWORD', trends_top_kw_2,
        'trends_kw_interest', trends_kw2_interest,
        CAST(NULL AS FLOAT64), CAST(NULL AS FLOAT64),
        CAST(NULL AS FLOAT64), CAST(NULL AS FLOAT64)
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_silver_googleTrends_weekly`
    WHERE NULLIF(TRIM(trends_top_kw_2), '') IS NOT NULL

    UNION ALL SELECT week_sun_to_sat, data_source, channel, max_data_date,
        'KEYWORD', trends_top_kw_2,
        'trends_kw_wow_change', trends_kw2_change,
        CAST(NULL AS FLOAT64), CAST(NULL AS FLOAT64),
        CAST(NULL AS FLOAT64), CAST(NULL AS FLOAT64)
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_silver_googleTrends_weekly`
    WHERE NULLIF(TRIM(trends_top_kw_2), '') IS NOT NULL

    UNION ALL SELECT week_sun_to_sat, data_source, channel, max_data_date,
        'KEYWORD', trends_top_kw_3,
        'trends_kw_interest', trends_kw3_interest,
        CAST(NULL AS FLOAT64), CAST(NULL AS FLOAT64),
        CAST(NULL AS FLOAT64), CAST(NULL AS FLOAT64)
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_silver_googleTrends_weekly`
    WHERE NULLIF(TRIM(trends_top_kw_3), '') IS NOT NULL

    UNION ALL SELECT week_sun_to_sat, data_source, channel, max_data_date,
        'KEYWORD', trends_top_kw_3,
        'trends_kw_wow_change', trends_kw3_change,
        CAST(NULL AS FLOAT64), CAST(NULL AS FLOAT64),
        CAST(NULL AS FLOAT64), CAST(NULL AS FLOAT64)
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_silver_googleTrends_weekly`
    WHERE NULLIF(TRIM(trends_top_kw_3), '') IS NOT NULL

    UNION ALL SELECT week_sun_to_sat, data_source, channel, max_data_date,
        'KEYWORD', trends_top_kw_4,
        'trends_kw_interest', trends_kw4_interest,
        CAST(NULL AS FLOAT64), CAST(NULL AS FLOAT64),
        CAST(NULL AS FLOAT64), CAST(NULL AS FLOAT64)
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_silver_googleTrends_weekly`
    WHERE NULLIF(TRIM(trends_top_kw_4), '') IS NOT NULL

    UNION ALL SELECT week_sun_to_sat, data_source, channel, max_data_date,
        'KEYWORD', trends_top_kw_4,
        'trends_kw_wow_change', trends_kw4_change,
        CAST(NULL AS FLOAT64), CAST(NULL AS FLOAT64),
        CAST(NULL AS FLOAT64), CAST(NULL AS FLOAT64)
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_silver_googleTrends_weekly`
    WHERE NULLIF(TRIM(trends_top_kw_4), '') IS NOT NULL

    UNION ALL SELECT week_sun_to_sat, data_source, channel, max_data_date,
        'KEYWORD', trends_top_kw_5,
        'trends_kw_interest', trends_kw5_interest,
        CAST(NULL AS FLOAT64), CAST(NULL AS FLOAT64),
        CAST(NULL AS FLOAT64), CAST(NULL AS FLOAT64)
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_silver_googleTrends_weekly`
    WHERE NULLIF(TRIM(trends_top_kw_5), '') IS NOT NULL

    UNION ALL SELECT week_sun_to_sat, data_source, channel, max_data_date,
        'KEYWORD', trends_top_kw_5,
        'trends_kw_wow_change', trends_kw5_change,
        CAST(NULL AS FLOAT64), CAST(NULL AS FLOAT64),
        CAST(NULL AS FLOAT64), CAST(NULL AS FLOAT64)
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_silver_googleTrends_weekly`
    WHERE NULLIF(TRIM(trends_top_kw_5), '') IS NOT NULL
),

-- -----------------------------------------------------------------------
-- COMBINE: All sources into single long table
-- Each source scanned exactly once via its own CTE
-- -----------------------------------------------------------------------
combined AS (

    SELECT
        week_sun_to_sat, data_source, channel, max_data_date,
        CAST(NULL AS STRING)    AS dimension_name,
        CAST(NULL AS STRING)    AS dimension_value,
        metric_name, metric_value,
        metric_value_wow, metric_value_ly,
        wow_pct, yoy_pct
    FROM profound_long

    UNION ALL

    SELECT
        week_sun_to_sat, data_source, channel, max_data_date,
        CAST(NULL AS STRING)    AS dimension_name,
        CAST(NULL AS STRING)    AS dimension_value,
        metric_name, metric_value,
        metric_value_wow, metric_value_ly,
        wow_pct, yoy_pct
    FROM gofish_long

    UNION ALL

    SELECT
        week_sun_to_sat, data_source, channel, max_data_date,
        CAST(NULL AS STRING)    AS dimension_name,
        CAST(NULL AS STRING)    AS dimension_value,
        metric_name, metric_value,
        metric_value_wow, metric_value_ly,
        wow_pct, yoy_pct
    FROM sa360_long

    UNION ALL

    SELECT
        week_sun_to_sat, data_source, channel, max_data_date,
        CAST(NULL AS STRING)    AS dimension_name,
        CAST(NULL AS STRING)    AS dimension_value,
        metric_name, metric_value,
        metric_value_wow, metric_value_ly,
        wow_pct, yoy_pct
    FROM gsc_long

    UNION ALL

    SELECT
        week_sun_to_sat, data_source, channel, max_data_date,
        CAST(NULL AS STRING)    AS dimension_name,
        CAST(NULL AS STRING)    AS dimension_value,
        metric_name, metric_value,
        metric_value_wow, metric_value_ly,
        wow_pct, yoy_pct
    FROM trends_index_long

    UNION ALL

    SELECT
        week_sun_to_sat, data_source, channel, max_data_date,
        dimension_name,
        dimension_value,
        metric_name, metric_value,
        metric_value_wow, metric_value_ly,
        wow_pct, yoy_pct
    FROM trends_keywords_long
)

-- -----------------------------------------------------------------------
-- FINAL OUTPUT
-- Column order: time → granularity → source → channel → dimensions → metric
-- max_data_date per data_source via window function
-- -----------------------------------------------------------------------
SELECT
    week_sun_to_sat,
    'WEEKLY'                                                            AS time_granularity,
    data_source,
    channel,
    dimension_name,
    dimension_value,
    metric_name,
    metric_value,
    metric_value_wow,
    metric_value_ly,
    wow_pct,
    yoy_pct,
    MAX(max_data_date) OVER (
        PARTITION BY data_source
    )                                                                   AS max_data_date
FROM combined
ORDER BY
    week_sun_to_sat  ASC,
    data_source      ASC,
    channel          ASC,
    metric_name      ASC,
    dimension_value  ASC
;