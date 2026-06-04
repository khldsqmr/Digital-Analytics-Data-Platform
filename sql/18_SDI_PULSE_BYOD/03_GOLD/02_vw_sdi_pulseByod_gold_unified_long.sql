/* =================================================================================================
FILE:         02_vw_sdi_pulseByod_gold_unified_long.sql
LAYER:        Gold View — Long
DATASET:      prj-dbi-prd-1.ds_dbi_digitalmedia_automation
VIEW NAME:    vw_sdi_pulseByod_gold_unified_long

PURPOSE:
  Gold Long view for the Pulse BYOD dashboard.
  Reads directly from Silver views — NOT from Gold Wide.
  Each Silver view unpivoted independently via UNION ALL.
  One row per metric per week.

CHANGES:
  2026-05-28 — trends_kw_change rows added (source-supplied, not BQ-computed)
  2026-05-31 — Adobe: new metrics added, pct/cvr naming convention applied:
               uvnbByodPctOfUvnbFlow → pctUvnbByodOfUvnbFlow
               new: uvnbTotal, ordersTotal, pctOrdersByodOfOrdersTotal,
                    cvrByod, cvrSite (allChannels)
               new: pctUvnbByodOfTotal (per channel)
             — Sort fixed: dimension_name ASC (was dimension_value ASC)
  2026-06-04 — Profound CIT & VIS rename:
               VIS columns renamed: profound_{asset}_nonbrand_{metric} → profoundVis_{asset}_nonbrand_{metric}
               CIT columns added: profoundCit_{asset}_nonbrand_shareOfVoice and
               wow/ly/wow_pct/yoy_pct variants to profound_long UNPIVOT (+3 rows/week in Profound section)
================================================================================================= */

CREATE OR REPLACE VIEW `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_gold_unified_long`
AS

WITH

-- -----------------------------------------------------------------------
-- PROFOUND: 15 rows/week (4 profound_vis metrics × 3 assets + 1 profound_cit metric × 3 assets)
-- -----------------------------------------------------------------------
profound_long AS (
    SELECT week_sun_to_sat, data_source, channel, max_data_date, metric_name, metric_value, metric_value_wow, metric_value_ly, wow_pct, yoy_pct
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_silver_profound_weekly`
    UNPIVOT (
        (metric_value, metric_value_wow, metric_value_ly, wow_pct, yoy_pct)
        FOR metric_name IN (
            -- ---- VIS metrics ----
            (profoundVis_tmo_nonbrand_visibilityScore,    profoundVis_tmo_nonbrand_visibilityScore_wow,    profoundVis_tmo_nonbrand_visibilityScore_ly,    profoundVis_tmo_nonbrand_visibilityScore_wow_pct,    profoundVis_tmo_nonbrand_visibilityScore_yoy_pct)    AS 'profoundVis_tmo_nonbrand_visibilityScore',
            (profoundVis_tmo_nonbrand_executions,          profoundVis_tmo_nonbrand_executions_wow,          profoundVis_tmo_nonbrand_executions_ly,          profoundVis_tmo_nonbrand_executions_wow_pct,          profoundVis_tmo_nonbrand_executions_yoy_pct)          AS 'profoundVis_tmo_nonbrand_executions',
            (profoundVis_tmo_nonbrand_mentionsCount,      profoundVis_tmo_nonbrand_mentionsCount_wow,      profoundVis_tmo_nonbrand_mentionsCount_ly,      profoundVis_tmo_nonbrand_mentionsCount_wow_pct,      profoundVis_tmo_nonbrand_mentionsCount_yoy_pct)      AS 'profoundVis_tmo_nonbrand_mentionsCount',
            (profoundVis_tmo_nonbrand_shareOfVoice,      profoundVis_tmo_nonbrand_shareOfVoice_wow,      profoundVis_tmo_nonbrand_shareOfVoice_ly,      profoundVis_tmo_nonbrand_shareOfVoice_wow_pct,      profoundVis_tmo_nonbrand_shareOfVoice_yoy_pct)      AS 'profoundVis_tmo_nonbrand_shareOfVoice',
            (profoundVis_verizon_nonbrand_visibilityScore,profoundVis_verizon_nonbrand_visibilityScore_wow,profoundVis_verizon_nonbrand_visibilityScore_ly,profoundVis_verizon_nonbrand_visibilityScore_wow_pct,profoundVis_verizon_nonbrand_visibilityScore_yoy_pct) AS 'profoundVis_verizon_nonbrand_visibilityScore',
            (profoundVis_verizon_nonbrand_executions,      profoundVis_verizon_nonbrand_executions_wow,      profoundVis_verizon_nonbrand_executions_ly,      profoundVis_verizon_nonbrand_executions_wow_pct,      profoundVis_verizon_nonbrand_executions_yoy_pct)      AS 'profoundVis_verizon_nonbrand_executions',
            (profoundVis_verizon_nonbrand_mentionsCount,  profoundVis_verizon_nonbrand_mentionsCount_wow,  profoundVis_verizon_nonbrand_mentionsCount_ly,  profoundVis_verizon_nonbrand_mentionsCount_wow_pct,  profoundVis_verizon_nonbrand_mentionsCount_yoy_pct)  AS 'profoundVis_verizon_nonbrand_mentionsCount',
            (profoundVis_verizon_nonbrand_shareOfVoice,  profoundVis_verizon_nonbrand_shareOfVoice_wow,  profoundVis_verizon_nonbrand_shareOfVoice_ly,  profoundVis_verizon_nonbrand_shareOfVoice_wow_pct,  profoundVis_verizon_nonbrand_shareOfVoice_yoy_pct)  AS 'profoundVis_verizon_nonbrand_shareOfVoice',
            (profoundVis_att_nonbrand_visibilityScore,    profoundVis_att_nonbrand_visibilityScore_wow,    profoundVis_att_nonbrand_visibilityScore_ly,    profoundVis_att_nonbrand_visibilityScore_wow_pct,    profoundVis_att_nonbrand_visibilityScore_yoy_pct)    AS 'profoundVis_att_nonbrand_visibilityScore',
            (profoundVis_att_nonbrand_executions,          profoundVis_att_nonbrand_executions_wow,          profoundVis_att_nonbrand_executions_ly,          profoundVis_att_nonbrand_executions_wow_pct,          profoundVis_att_nonbrand_executions_yoy_pct)          AS 'profoundVis_att_nonbrand_executions',
            (profoundVis_att_nonbrand_mentionsCount,      profoundVis_att_nonbrand_mentionsCount_wow,      profoundVis_att_nonbrand_mentionsCount_ly,      profoundVis_att_nonbrand_mentionsCount_wow_pct,      profoundVis_att_nonbrand_mentionsCount_yoy_pct)      AS 'profoundVis_att_nonbrand_mentionsCount',
            (profoundVis_att_nonbrand_shareOfVoice,      profoundVis_att_nonbrand_shareOfVoice_wow,      profoundVis_att_nonbrand_shareOfVoice_ly,      profoundVis_att_nonbrand_shareOfVoice_wow_pct,      profoundVis_att_nonbrand_shareOfVoice_yoy_pct)      AS 'profoundVis_att_nonbrand_shareOfVoice',
            -- ---- CIT metrics ----
            (profoundCit_tmo_nonbrand_shareOfVoice,     profoundCit_tmo_nonbrand_shareOfVoice_wow,     profoundCit_tmo_nonbrand_shareOfVoice_ly,     profoundCit_tmo_nonbrand_shareOfVoice_wow_pct,     profoundCit_tmo_nonbrand_shareOfVoice_yoy_pct)     AS 'profoundCit_tmo_nonbrand_shareOfVoice',
            (profoundCit_verizon_nonbrand_shareOfVoice, profoundCit_verizon_nonbrand_shareOfVoice_wow, profoundCit_verizon_nonbrand_shareOfVoice_ly, profoundCit_verizon_nonbrand_shareOfVoice_wow_pct, profoundCit_verizon_nonbrand_shareOfVoice_yoy_pct) AS 'profoundCit_verizon_nonbrand_shareOfVoice',
            (profoundCit_att_nonbrand_shareOfVoice,     profoundCit_att_nonbrand_shareOfVoice_wow,     profoundCit_att_nonbrand_shareOfVoice_ly,     profoundCit_att_nonbrand_shareOfVoice_wow_pct,     profoundCit_att_nonbrand_shareOfVoice_yoy_pct)     AS 'profoundCit_att_nonbrand_shareOfVoice'
        )
    )
),

-- -----------------------------------------------------------------------
-- GOFISH: 12 rows/week (4 metrics × 3 assets)
-- -----------------------------------------------------------------------
gofish_long AS (
    SELECT week_sun_to_sat, data_source, channel, max_data_date, metric_name, metric_value, metric_value_wow, metric_value_ly, wow_pct, yoy_pct
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_silver_profoundGofish_weekly`
    UNPIVOT (
        (metric_value, metric_value_wow, metric_value_ly, wow_pct, yoy_pct)
        FOR metric_name IN (
            (gofish_tmo_brand_visibilityScore,    gofish_tmo_brand_visibilityScore_wow,    gofish_tmo_brand_visibilityScore_ly,    gofish_tmo_brand_visibilityScore_wow_pct,    gofish_tmo_brand_visibilityScore_yoy_pct)    AS 'gofish_tmo_brand_visibilityScore',
            (gofish_tmo_brand_executions,          gofish_tmo_brand_executions_wow,          gofish_tmo_brand_executions_ly,          gofish_tmo_brand_executions_wow_pct,          gofish_tmo_brand_executions_yoy_pct)          AS 'gofish_tmo_brand_executions',
            (gofish_tmo_brand_mentionsCount,      gofish_tmo_brand_mentionsCount_wow,      gofish_tmo_brand_mentionsCount_ly,      gofish_tmo_brand_mentionsCount_wow_pct,      gofish_tmo_brand_mentionsCount_yoy_pct)      AS 'gofish_tmo_brand_mentionsCount',
            (gofish_tmo_brand_shareOfVoice,      gofish_tmo_brand_shareOfVoice_wow,      gofish_tmo_brand_shareOfVoice_ly,      gofish_tmo_brand_shareOfVoice_wow_pct,      gofish_tmo_brand_shareOfVoice_yoy_pct)      AS 'gofish_tmo_brand_shareOfVoice',
            (gofish_verizon_brand_visibilityScore,gofish_verizon_brand_visibilityScore_wow,gofish_verizon_brand_visibilityScore_ly,gofish_verizon_brand_visibilityScore_wow_pct,gofish_verizon_brand_visibilityScore_yoy_pct) AS 'gofish_verizon_brand_visibilityScore',
            (gofish_verizon_brand_executions,      gofish_verizon_brand_executions_wow,      gofish_verizon_brand_executions_ly,      gofish_verizon_brand_executions_wow_pct,      gofish_verizon_brand_executions_yoy_pct)      AS 'gofish_verizon_brand_executions',
            (gofish_verizon_brand_mentionsCount,  gofish_verizon_brand_mentionsCount_wow,  gofish_verizon_brand_mentionsCount_ly,  gofish_verizon_brand_mentionsCount_wow_pct,  gofish_verizon_brand_mentionsCount_yoy_pct)  AS 'gofish_verizon_brand_mentionsCount',
            (gofish_verizon_brand_shareOfVoice,  gofish_verizon_brand_shareOfVoice_wow,  gofish_verizon_brand_shareOfVoice_ly,  gofish_verizon_brand_shareOfVoice_wow_pct,  gofish_verizon_brand_shareOfVoice_yoy_pct)  AS 'gofish_verizon_brand_shareOfVoice',
            (gofish_att_brand_visibilityScore,    gofish_att_brand_visibilityScore_wow,    gofish_att_brand_visibilityScore_ly,    gofish_att_brand_visibilityScore_wow_pct,    gofish_att_brand_visibilityScore_yoy_pct)    AS 'gofish_att_brand_visibilityScore',
            (gofish_att_brand_executions,          gofish_att_brand_executions_wow,          gofish_att_brand_executions_ly,          gofish_att_brand_executions_wow_pct,          gofish_att_brand_executions_yoy_pct)          AS 'gofish_att_brand_executions',
            (gofish_att_brand_mentionsCount,      gofish_att_brand_mentionsCount_wow,      gofish_att_brand_mentionsCount_ly,      gofish_att_brand_mentionsCount_wow_pct,      gofish_att_brand_mentionsCount_yoy_pct)      AS 'gofish_att_brand_mentionsCount',
            (gofish_att_brand_shareOfVoice,      gofish_att_brand_shareOfVoice_wow,      gofish_att_brand_shareOfVoice_ly,      gofish_att_brand_shareOfVoice_wow_pct,      gofish_att_brand_shareOfVoice_yoy_pct)      AS 'gofish_att_brand_shareOfVoice'
        )
    )
),

-- -----------------------------------------------------------------------
-- SA360: 12 rows/week (6 metrics × 2 brand types)
-- -----------------------------------------------------------------------
sa360_long AS (
    SELECT week_sun_to_sat, data_source, channel, max_data_date, metric_name, metric_value, metric_value_wow, metric_value_ly, wow_pct, yoy_pct
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_silver_sa360_weekly`
    UNPIVOT (
        (metric_value, metric_value_wow, metric_value_ly, wow_pct, yoy_pct)
        FOR metric_name IN (
            (sa360_tmo_brand_impressions,     sa360_tmo_brand_impressions_wow,     sa360_tmo_brand_impressions_ly,     sa360_tmo_brand_impressions_wow_pct,     sa360_tmo_brand_impressions_yoy_pct)     AS 'sa360_tmo_brand_impressions',
            (sa360_tmo_brand_clicks,          sa360_tmo_brand_clicks_wow,          sa360_tmo_brand_clicks_ly,          sa360_tmo_brand_clicks_wow_pct,          sa360_tmo_brand_clicks_yoy_pct)          AS 'sa360_tmo_brand_clicks',
            (sa360_tmo_brand_cost,            sa360_tmo_brand_cost_wow,            sa360_tmo_brand_cost_ly,            sa360_tmo_brand_cost_wow_pct,            sa360_tmo_brand_cost_yoy_pct)            AS 'sa360_tmo_brand_cost',
            (sa360_tmo_brand_orders,          sa360_tmo_brand_orders_wow,          sa360_tmo_brand_orders_ly,          sa360_tmo_brand_orders_wow_pct,          sa360_tmo_brand_orders_yoy_pct)          AS 'sa360_tmo_brand_orders',
            (sa360_tmo_brand_cart_start,      sa360_tmo_brand_cart_start_wow,      sa360_tmo_brand_cart_start_ly,      sa360_tmo_brand_cart_start_wow_pct,      sa360_tmo_brand_cart_start_yoy_pct)      AS 'sa360_tmo_brand_cart_start',
            (sa360_tmo_brand_postpaid_pspv,   sa360_tmo_brand_postpaid_pspv_wow,   sa360_tmo_brand_postpaid_pspv_ly,   sa360_tmo_brand_postpaid_pspv_wow_pct,   sa360_tmo_brand_postpaid_pspv_yoy_pct)   AS 'sa360_tmo_brand_postpaid_pspv',
            (sa360_tmo_nonbrand_impressions,  sa360_tmo_nonbrand_impressions_wow,  sa360_tmo_nonbrand_impressions_ly,  sa360_tmo_nonbrand_impressions_wow_pct,  sa360_tmo_nonbrand_impressions_yoy_pct)  AS 'sa360_tmo_nonbrand_impressions',
            (sa360_tmo_nonbrand_clicks,       sa360_tmo_nonbrand_clicks_wow,       sa360_tmo_nonbrand_clicks_ly,       sa360_tmo_nonbrand_clicks_wow_pct,       sa360_tmo_nonbrand_clicks_yoy_pct)       AS 'sa360_tmo_nonbrand_clicks',
            (sa360_tmo_nonbrand_cost,         sa360_tmo_nonbrand_cost_wow,         sa360_tmo_nonbrand_cost_ly,         sa360_tmo_nonbrand_cost_wow_pct,         sa360_tmo_nonbrand_cost_yoy_pct)         AS 'sa360_tmo_nonbrand_cost',
            (sa360_tmo_nonbrand_orders,       sa360_tmo_nonbrand_orders_wow,       sa360_tmo_nonbrand_orders_ly,       sa360_tmo_nonbrand_orders_wow_pct,       sa360_tmo_nonbrand_orders_yoy_pct)       AS 'sa360_tmo_nonbrand_orders',
            (sa360_tmo_nonbrand_cart_start,   sa360_tmo_nonbrand_cart_start_wow,   sa360_tmo_nonbrand_cart_start_ly,   sa360_tmo_nonbrand_cart_start_wow_pct,   sa360_tmo_nonbrand_cart_start_yoy_pct)   AS 'sa360_tmo_nonbrand_cart_start',
            (sa360_tmo_nonbrand_postpaid_pspv,sa360_tmo_nonbrand_postpaid_pspv_wow,sa360_tmo_nonbrand_postpaid_pspv_ly,sa360_tmo_nonbrand_postpaid_pspv_wow_pct,sa360_tmo_nonbrand_postpaid_pspv_yoy_pct) AS 'sa360_tmo_nonbrand_postpaid_pspv'
        )
    )
),

-- -----------------------------------------------------------------------
-- GSC: 4 rows/week
-- -----------------------------------------------------------------------
gsc_long AS (
    SELECT week_sun_to_sat, data_source, channel, max_data_date, metric_name, metric_value, metric_value_wow, metric_value_ly, wow_pct, yoy_pct
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
-- TRENDS: byod_index — 1 row/week
-- -----------------------------------------------------------------------
trends_index_long AS (
    SELECT week_sun_to_sat, data_source, channel, max_data_date, metric_name, metric_value, metric_value_wow, metric_value_ly, wow_pct, yoy_pct
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_silver_googleTrends_weekly`
    UNPIVOT (
        (metric_value, metric_value_wow, metric_value_ly, wow_pct, yoy_pct)
        FOR metric_name IN (
            (trends_byod_index, trends_byod_index_wow, trends_byod_index_ly, trends_byod_index_wow_pct, trends_byod_index_yoy_pct) AS 'trends_byod_index'
        )
    )
),

-- -----------------------------------------------------------------------
-- TRENDS: Keywords — up to 10 rows/week (5 ranks × 2 metrics)
-- trends_kw_change is source-supplied, not BQ-computed
-- wow/ly all NULL — keywords change position week to week
-- -----------------------------------------------------------------------
trends_keywords_long AS (
    SELECT week_sun_to_sat, data_source, channel, max_data_date,
        'KEYWORD_RANK_1' AS dimension_name, trends_top_kw_1 AS dimension_value,
        'trends_kw_interest' AS metric_name, trends_kw1_interest AS metric_value,
        CAST(NULL AS FLOAT64) AS metric_value_wow, CAST(NULL AS FLOAT64) AS metric_value_ly,
        CAST(NULL AS FLOAT64) AS wow_pct, CAST(NULL AS FLOAT64) AS yoy_pct
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_silver_googleTrends_weekly`
    WHERE NULLIF(TRIM(trends_top_kw_1), '') IS NOT NULL
    UNION ALL SELECT week_sun_to_sat, data_source, channel, max_data_date, 'KEYWORD_RANK_1', trends_top_kw_1, 'trends_kw_change', trends_kw1_change, CAST(NULL AS FLOAT64), CAST(NULL AS FLOAT64), CAST(NULL AS FLOAT64), CAST(NULL AS FLOAT64)
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_silver_googleTrends_weekly` WHERE NULLIF(TRIM(trends_top_kw_1), '') IS NOT NULL
    UNION ALL SELECT week_sun_to_sat, data_source, channel, max_data_date, 'KEYWORD_RANK_2', trends_top_kw_2, 'trends_kw_interest', trends_kw2_interest, CAST(NULL AS FLOAT64), CAST(NULL AS FLOAT64), CAST(NULL AS FLOAT64), CAST(NULL AS FLOAT64)
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_silver_googleTrends_weekly` WHERE NULLIF(TRIM(trends_top_kw_2), '') IS NOT NULL
    UNION ALL SELECT week_sun_to_sat, data_source, channel, max_data_date, 'KEYWORD_RANK_2', trends_top_kw_2, 'trends_kw_change', trends_kw2_change, CAST(NULL AS FLOAT64), CAST(NULL AS FLOAT64), CAST(NULL AS FLOAT64), CAST(NULL AS FLOAT64)
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_silver_googleTrends_weekly` WHERE NULLIF(TRIM(trends_top_kw_2), '') IS NOT NULL
    UNION ALL SELECT week_sun_to_sat, data_source, channel, max_data_date, 'KEYWORD_RANK_3', trends_top_kw_3, 'trends_kw_interest', trends_kw3_interest, CAST(NULL AS FLOAT64), CAST(NULL AS FLOAT64), CAST(NULL AS FLOAT64), CAST(NULL AS FLOAT64)
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_silver_googleTrends_weekly` WHERE NULLIF(TRIM(trends_top_kw_3), '') IS NOT NULL
    UNION ALL SELECT week_sun_to_sat, data_source, channel, max_data_date, 'KEYWORD_RANK_3', trends_top_kw_3, 'trends_kw_change', trends_kw3_change, CAST(NULL AS FLOAT64), CAST(NULL AS FLOAT64), CAST(NULL AS FLOAT64), CAST(NULL AS FLOAT64)
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_silver_googleTrends_weekly` WHERE NULLIF(TRIM(trends_top_kw_3), '') IS NOT NULL
    UNION ALL SELECT week_sun_to_sat, data_source, channel, max_data_date, 'KEYWORD_RANK_4', trends_top_kw_4, 'trends_kw_interest', trends_kw4_interest, CAST(NULL AS FLOAT64), CAST(NULL AS FLOAT64), CAST(NULL AS FLOAT64), CAST(NULL AS FLOAT64)
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_silver_googleTrends_weekly` WHERE NULLIF(TRIM(trends_top_kw_4), '') IS NOT NULL
    UNION ALL SELECT week_sun_to_sat, data_source, channel, max_data_date, 'KEYWORD_RANK_4', trends_top_kw_4, 'trends_kw_change', trends_kw4_change, CAST(NULL AS FLOAT64), CAST(NULL AS FLOAT64), CAST(NULL AS FLOAT64), CAST(NULL AS FLOAT64)
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_silver_googleTrends_weekly` WHERE NULLIF(TRIM(trends_top_kw_4), '') IS NOT NULL
    UNION ALL SELECT week_sun_to_sat, data_source, channel, max_data_date, 'KEYWORD_RANK_5', trends_top_kw_5, 'trends_kw_interest', trends_kw5_interest, CAST(NULL AS FLOAT64), CAST(NULL AS FLOAT64), CAST(NULL AS FLOAT64), CAST(NULL AS FLOAT64)
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_silver_googleTrends_weekly` WHERE NULLIF(TRIM(trends_top_kw_5), '') IS NOT NULL
    UNION ALL SELECT week_sun_to_sat, data_source, channel, max_data_date, 'KEYWORD_RANK_5', trends_top_kw_5, 'trends_kw_change', trends_kw5_change, CAST(NULL AS FLOAT64), CAST(NULL AS FLOAT64), CAST(NULL AS FLOAT64), CAST(NULL AS FLOAT64)
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_silver_googleTrends_weekly` WHERE NULLIF(TRIM(trends_top_kw_5), '') IS NOT NULL
),

-- -----------------------------------------------------------------------
-- ADOBE: channel derived from metric_name suffix in combined CTE
-- pct/cvr naming convention applied throughout
-- -----------------------------------------------------------------------
adobe_long AS (
    SELECT week_sun_to_sat, data_source, max_data_date, metric_name, metric_value, metric_value_wow, metric_value_ly, wow_pct, yoy_pct
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_silver_adobe_weekly`
    UNPIVOT (
        (metric_value, metric_value_wow, metric_value_ly, wow_pct, yoy_pct)
        FOR metric_name IN (
            -- ---- ALL CHANNELS ----
            (adobe_uvnbByod_allChannels,                      adobe_uvnbByod_allChannels_wow,                      adobe_uvnbByod_allChannels_ly,                      adobe_uvnbByod_allChannels_wow_pct,                      adobe_uvnbByod_allChannels_yoy_pct)                      AS 'adobe_uvnbByod_allChannels',
            (adobe_uvnbTotal_allChannels,                     adobe_uvnbTotal_allChannels_wow,                     adobe_uvnbTotal_allChannels_ly,                     adobe_uvnbTotal_allChannels_wow_pct,                     adobe_uvnbTotal_allChannels_yoy_pct)                     AS 'adobe_uvnbTotal_allChannels',
            (adobe_uvnbFlowTotal_allChannels,                 adobe_uvnbFlowTotal_allChannels_wow,                 adobe_uvnbFlowTotal_allChannels_ly,                 adobe_uvnbFlowTotal_allChannels_wow_pct,                 adobe_uvnbFlowTotal_allChannels_yoy_pct)                 AS 'adobe_uvnbFlowTotal_allChannels',
            (adobe_pctUvnbByodOfUvnbFlow_allChannels,         adobe_pctUvnbByodOfUvnbFlow_allChannels_wow,         adobe_pctUvnbByodOfUvnbFlow_allChannels_ly,         adobe_pctUvnbByodOfUvnbFlow_allChannels_wow_pct,         adobe_pctUvnbByodOfUvnbFlow_allChannels_yoy_pct)         AS 'adobe_pctUvnbByodOfUvnbFlow_allChannels',
            (adobe_cartStartByod_allChannels,                 adobe_cartStartByod_allChannels_wow,                 adobe_cartStartByod_allChannels_ly,                 adobe_cartStartByod_allChannels_wow_pct,                 adobe_cartStartByod_allChannels_yoy_pct)                 AS 'adobe_cartStartByod_allChannels',
            (adobe_ordersUnassistedByod_allChannels,          adobe_ordersUnassistedByod_allChannels_wow,          adobe_ordersUnassistedByod_allChannels_ly,          adobe_ordersUnassistedByod_allChannels_wow_pct,          adobe_ordersUnassistedByod_allChannels_yoy_pct)          AS 'adobe_ordersUnassistedByod_allChannels',
            (adobe_ordersAssistedByod_allChannels,            adobe_ordersAssistedByod_allChannels_wow,            adobe_ordersAssistedByod_allChannels_ly,            adobe_ordersAssistedByod_allChannels_wow_pct,            adobe_ordersAssistedByod_allChannels_yoy_pct)            AS 'adobe_ordersAssistedByod_allChannels',
            (adobe_ordersTotalByod_allChannels,               adobe_ordersTotalByod_allChannels_wow,               adobe_ordersTotalByod_allChannels_ly,               adobe_ordersTotalByod_allChannels_wow_pct,               adobe_ordersTotalByod_allChannels_yoy_pct)               AS 'adobe_ordersTotalByod_allChannels',
            (adobe_ordersTotal_allChannels,                   adobe_ordersTotal_allChannels_wow,                   adobe_ordersTotal_allChannels_ly,                   adobe_ordersTotal_allChannels_wow_pct,                   adobe_ordersTotal_allChannels_yoy_pct)                   AS 'adobe_ordersTotal_allChannels',
            (adobe_pctOrdersByodOfOrdersTotal_allChannels,    adobe_pctOrdersByodOfOrdersTotal_allChannels_wow,    adobe_pctOrdersByodOfOrdersTotal_allChannels_ly,    adobe_pctOrdersByodOfOrdersTotal_allChannels_wow_pct,    adobe_pctOrdersByodOfOrdersTotal_allChannels_yoy_pct)    AS 'adobe_pctOrdersByodOfOrdersTotal_allChannels',
            (adobe_cvrByod_allChannels,                       adobe_cvrByod_allChannels_wow,                       adobe_cvrByod_allChannels_ly,                       adobe_cvrByod_allChannels_wow_pct,                       adobe_cvrByod_allChannels_yoy_pct)                       AS 'adobe_cvrByod_allChannels',
            (adobe_cvrSite_allChannels,                       adobe_cvrSite_allChannels_wow,                       adobe_cvrSite_allChannels_ly,                       adobe_cvrSite_allChannels_wow_pct,                       adobe_cvrSite_allChannels_yoy_pct)                       AS 'adobe_cvrSite_allChannels',
            -- ---- PAID SEARCH ----
            (adobe_uvnbByod_paidSearch,                       adobe_uvnbByod_paidSearch_wow,                       adobe_uvnbByod_paidSearch_ly,                       adobe_uvnbByod_paidSearch_wow_pct,                       adobe_uvnbByod_paidSearch_yoy_pct)                       AS 'adobe_uvnbByod_paidSearch',
            (adobe_pctUvnbByodOfTotal_paidSearch,             adobe_pctUvnbByodOfTotal_paidSearch_wow,             adobe_pctUvnbByodOfTotal_paidSearch_ly,             adobe_pctUvnbByodOfTotal_paidSearch_wow_pct,             adobe_pctUvnbByodOfTotal_paidSearch_yoy_pct)             AS 'adobe_pctUvnbByodOfTotal_paidSearch',
            (adobe_cartStartByod_paidSearch,                  adobe_cartStartByod_paidSearch_wow,                  adobe_cartStartByod_paidSearch_ly,                  adobe_cartStartByod_paidSearch_wow_pct,                  adobe_cartStartByod_paidSearch_yoy_pct)                  AS 'adobe_cartStartByod_paidSearch',
            (adobe_ordersUnassistedByod_paidSearch,           adobe_ordersUnassistedByod_paidSearch_wow,           adobe_ordersUnassistedByod_paidSearch_ly,           adobe_ordersUnassistedByod_paidSearch_wow_pct,           adobe_ordersUnassistedByod_paidSearch_yoy_pct)           AS 'adobe_ordersUnassistedByod_paidSearch',
            (adobe_ordersAssistedByod_paidSearch,             adobe_ordersAssistedByod_paidSearch_wow,             adobe_ordersAssistedByod_paidSearch_ly,             adobe_ordersAssistedByod_paidSearch_wow_pct,             adobe_ordersAssistedByod_paidSearch_yoy_pct)             AS 'adobe_ordersAssistedByod_paidSearch',
            (adobe_ordersTotalByod_paidSearch,                adobe_ordersTotalByod_paidSearch_wow,                adobe_ordersTotalByod_paidSearch_ly,                adobe_ordersTotalByod_paidSearch_wow_pct,                adobe_ordersTotalByod_paidSearch_yoy_pct)                AS 'adobe_ordersTotalByod_paidSearch',
            -- ---- ORGANIC SEARCH ----
            (adobe_uvnbByod_organicSearch,                    adobe_uvnbByod_organicSearch_wow,                    adobe_uvnbByod_organicSearch_ly,                    adobe_uvnbByod_organicSearch_wow_pct,                    adobe_uvnbByod_organicSearch_yoy_pct)                    AS 'adobe_uvnbByod_organicSearch',
            (adobe_pctUvnbByodOfTotal_organicSearch,          adobe_pctUvnbByodOfTotal_organicSearch_wow,          adobe_pctUvnbByodOfTotal_organicSearch_ly,          adobe_pctUvnbByodOfTotal_organicSearch_wow_pct,          adobe_pctUvnbByodOfTotal_organicSearch_yoy_pct)          AS 'adobe_pctUvnbByodOfTotal_organicSearch',
            (adobe_cartStartByod_organicSearch,               adobe_cartStartByod_organicSearch_wow,               adobe_cartStartByod_organicSearch_ly,               adobe_cartStartByod_organicSearch_wow_pct,               adobe_cartStartByod_organicSearch_yoy_pct)               AS 'adobe_cartStartByod_organicSearch',
            (adobe_ordersUnassistedByod_organicSearch,        adobe_ordersUnassistedByod_organicSearch_wow,        adobe_ordersUnassistedByod_organicSearch_ly,        adobe_ordersUnassistedByod_organicSearch_wow_pct,        adobe_ordersUnassistedByod_organicSearch_yoy_pct)        AS 'adobe_ordersUnassistedByod_organicSearch',
            (adobe_ordersAssistedByod_organicSearch,          adobe_ordersAssistedByod_organicSearch_wow,          adobe_ordersAssistedByod_organicSearch_ly,          adobe_ordersAssistedByod_organicSearch_wow_pct,          adobe_ordersAssistedByod_organicSearch_yoy_pct)          AS 'adobe_ordersAssistedByod_organicSearch',
            (adobe_ordersTotalByod_organicSearch,             adobe_ordersTotalByod_organicSearch_wow,             adobe_ordersTotalByod_organicSearch_ly,             adobe_ordersTotalByod_organicSearch_wow_pct,             adobe_ordersTotalByod_organicSearch_yoy_pct)             AS 'adobe_ordersTotalByod_organicSearch',
            -- ---- DIRECT ----
            (adobe_uvnbByod_direct,                           adobe_uvnbByod_direct_wow,                           adobe_uvnbByod_direct_ly,                           adobe_uvnbByod_direct_wow_pct,                           adobe_uvnbByod_direct_yoy_pct)                           AS 'adobe_uvnbByod_direct',
            (adobe_pctUvnbByodOfTotal_direct,                 adobe_pctUvnbByodOfTotal_direct_wow,                 adobe_pctUvnbByodOfTotal_direct_ly,                 adobe_pctUvnbByodOfTotal_direct_wow_pct,                 adobe_pctUvnbByodOfTotal_direct_yoy_pct)                 AS 'adobe_pctUvnbByodOfTotal_direct',
            (adobe_cartStartByod_direct,                      adobe_cartStartByod_direct_wow,                      adobe_cartStartByod_direct_ly,                      adobe_cartStartByod_direct_wow_pct,                      adobe_cartStartByod_direct_yoy_pct)                      AS 'adobe_cartStartByod_direct',
            (adobe_ordersUnassistedByod_direct,               adobe_ordersUnassistedByod_direct_wow,               adobe_ordersUnassistedByod_direct_ly,               adobe_ordersUnassistedByod_direct_wow_pct,               adobe_ordersUnassistedByod_direct_yoy_pct)               AS 'adobe_ordersUnassistedByod_direct',
            (adobe_ordersAssistedByod_direct,                 adobe_ordersAssistedByod_direct_wow,                 adobe_ordersAssistedByod_direct_ly,                 adobe_ordersAssistedByod_direct_wow_pct,                 adobe_ordersAssistedByod_direct_yoy_pct)                 AS 'adobe_ordersAssistedByod_direct',
            (adobe_ordersTotalByod_direct,                    adobe_ordersTotalByod_direct_wow,                    adobe_ordersTotalByod_direct_ly,                    adobe_ordersTotalByod_direct_wow_pct,                    adobe_ordersTotalByod_direct_yoy_pct)                    AS 'adobe_ordersTotalByod_direct',
            -- ---- SOCIAL ----
            (adobe_uvnbByod_social,                           adobe_uvnbByod_social_wow,                           adobe_uvnbByod_social_ly,                           adobe_uvnbByod_social_wow_pct,                           adobe_uvnbByod_social_yoy_pct)                           AS 'adobe_uvnbByod_social',
            (adobe_pctUvnbByodOfTotal_social,                 adobe_pctUvnbByodOfTotal_social_wow,                 adobe_pctUvnbByodOfTotal_social_ly,                 adobe_pctUvnbByodOfTotal_social_wow_pct,                 adobe_pctUvnbByodOfTotal_social_yoy_pct)                 AS 'adobe_pctUvnbByodOfTotal_social',
            (adobe_cartStartByod_social,                      adobe_cartStartByod_social_wow,                      adobe_cartStartByod_social_ly,                      adobe_cartStartByod_social_wow_pct,                      adobe_cartStartByod_social_yoy_pct)                      AS 'adobe_cartStartByod_social',
            (adobe_ordersUnassistedByod_social,               adobe_ordersUnassistedByod_social_wow,               adobe_ordersUnassistedByod_social_ly,               adobe_ordersUnassistedByod_social_wow_pct,               adobe_ordersUnassistedByod_social_yoy_pct)               AS 'adobe_ordersUnassistedByod_social',
            (adobe_ordersAssistedByod_social,                 adobe_ordersAssistedByod_social_wow,                 adobe_ordersAssistedByod_social_ly,                 adobe_ordersAssistedByod_social_wow_pct,                 adobe_ordersAssistedByod_social_yoy_pct)                 AS 'adobe_ordersAssistedByod_social',
            (adobe_ordersTotalByod_social,                    adobe_ordersTotalByod_social_wow,                    adobe_ordersTotalByod_social_ly,                    adobe_ordersTotalByod_social_wow_pct,                    adobe_ordersTotalByod_social_yoy_pct)                    AS 'adobe_ordersTotalByod_social',
            -- ---- PROGRAMMATIC ----
            (adobe_uvnbByod_programmatic,                     adobe_uvnbByod_programmatic_wow,                     adobe_uvnbByod_programmatic_ly,                     adobe_uvnbByod_programmatic_wow_pct,                     adobe_uvnbByod_programmatic_yoy_pct)                     AS 'adobe_uvnbByod_programmatic',
            (adobe_pctUvnbByodOfTotal_programmatic,           adobe_pctUvnbByodOfTotal_programmatic_wow,           adobe_pctUvnbByodOfTotal_programmatic_ly,           adobe_pctUvnbByodOfTotal_programmatic_wow_pct,           adobe_pctUvnbByodOfTotal_programmatic_yoy_pct)           AS 'adobe_pctUvnbByodOfTotal_programmatic',
            (adobe_cartStartByod_programmatic,                adobe_cartStartByod_programmatic_wow,                adobe_cartStartByod_programmatic_ly,                adobe_cartStartByod_programmatic_wow_pct,                adobe_cartStartByod_programmatic_yoy_pct)                AS 'adobe_cartStartByod_programmatic',
            (adobe_ordersUnassistedByod_programmatic,         adobe_ordersUnassistedByod_programmatic_wow,         adobe_ordersUnassistedByod_programmatic_ly,         adobe_ordersUnassistedByod_programmatic_wow_pct,         adobe_ordersUnassistedByod_programmatic_yoy_pct)         AS 'adobe_ordersUnassistedByod_programmatic',
            (adobe_ordersAssistedByod_programmatic,           adobe_ordersAssistedByod_programmatic_wow,           adobe_ordersAssistedByod_programmatic_ly,           adobe_ordersAssistedByod_programmatic_wow_pct,           adobe_ordersAssistedByod_programmatic_yoy_pct)           AS 'adobe_ordersAssistedByod_programmatic',
            (adobe_ordersTotalByod_programmatic,              adobe_ordersTotalByod_programmatic_wow,              adobe_ordersTotalByod_programmatic_ly,              adobe_ordersTotalByod_programmatic_wow_pct,              adobe_ordersTotalByod_programmatic_yoy_pct)              AS 'adobe_ordersTotalByod_programmatic',
            -- ---- OTHER ----
            (adobe_uvnbByod_other,                            adobe_uvnbByod_other_wow,                            adobe_uvnbByod_other_ly,                            adobe_uvnbByod_other_wow_pct,                            adobe_uvnbByod_other_yoy_pct)                            AS 'adobe_uvnbByod_other',
            (adobe_pctUvnbByodOfTotal_other,                  adobe_pctUvnbByodOfTotal_other_wow,                  adobe_pctUvnbByodOfTotal_other_ly,                  adobe_pctUvnbByodOfTotal_other_wow_pct,                  adobe_pctUvnbByodOfTotal_other_yoy_pct)                  AS 'adobe_pctUvnbByodOfTotal_other',
            (adobe_cartStartByod_other,                       adobe_cartStartByod_other_wow,                       adobe_cartStartByod_other_ly,                       adobe_cartStartByod_other_wow_pct,                       adobe_cartStartByod_other_yoy_pct)                       AS 'adobe_cartStartByod_other',
            (adobe_ordersUnassistedByod_other,                adobe_ordersUnassistedByod_other_wow,                adobe_ordersUnassistedByod_other_ly,                adobe_ordersUnassistedByod_other_wow_pct,                adobe_ordersUnassistedByod_other_yoy_pct)                AS 'adobe_ordersUnassistedByod_other',
            (adobe_ordersAssistedByod_other,                  adobe_ordersAssistedByod_other_wow,                  adobe_ordersAssistedByod_other_ly,                  adobe_ordersAssistedByod_other_wow_pct,                  adobe_ordersAssistedByod_other_yoy_pct)                  AS 'adobe_ordersAssistedByod_other',
            (adobe_ordersTotalByod_other,                     adobe_ordersTotalByod_other_wow,                     adobe_ordersTotalByod_other_ly,                     adobe_ordersTotalByod_other_wow_pct,                     adobe_ordersTotalByod_other_yoy_pct)                     AS 'adobe_ordersTotalByod_other'
        )
    )
),

-- -----------------------------------------------------------------------
-- COMBINE
-- -----------------------------------------------------------------------
combined AS (
    SELECT week_sun_to_sat, data_source, channel, max_data_date,
        CAST(NULL AS STRING) AS dimension_name, CAST(NULL AS STRING) AS dimension_value,
        metric_name, metric_value, metric_value_wow, metric_value_ly, wow_pct, yoy_pct
    FROM profound_long
    UNION ALL
    SELECT week_sun_to_sat, data_source, channel, max_data_date,
        CAST(NULL AS STRING), CAST(NULL AS STRING),
        metric_name, metric_value, metric_value_wow, metric_value_ly, wow_pct, yoy_pct
    FROM gofish_long
    UNION ALL
    SELECT week_sun_to_sat, data_source, channel, max_data_date,
        CAST(NULL AS STRING), CAST(NULL AS STRING),
        metric_name, metric_value, metric_value_wow, metric_value_ly, wow_pct, yoy_pct
    FROM sa360_long
    UNION ALL
    SELECT week_sun_to_sat, data_source, channel, max_data_date,
        CAST(NULL AS STRING), CAST(NULL AS STRING),
        metric_name, metric_value, metric_value_wow, metric_value_ly, wow_pct, yoy_pct
    FROM gsc_long
    UNION ALL
    SELECT week_sun_to_sat, data_source, channel, max_data_date,
        CAST(NULL AS STRING), CAST(NULL AS STRING),
        metric_name, metric_value, metric_value_wow, metric_value_ly, wow_pct, yoy_pct
    FROM trends_index_long
    UNION ALL
    SELECT week_sun_to_sat, data_source, channel, max_data_date,
        dimension_name, dimension_value,
        metric_name, metric_value, metric_value_wow, metric_value_ly, wow_pct, yoy_pct
    FROM trends_keywords_long
    UNION ALL
    SELECT
        week_sun_to_sat,
        data_source,
        CASE
            WHEN metric_name LIKE '%_allChannels'   THEN 'ALL CHANNELS'
            WHEN metric_name LIKE '%_paidSearch'    THEN 'PAID SEARCH'
            WHEN metric_name LIKE '%_organicSearch' THEN 'ORGANIC SEARCH'
            WHEN metric_name LIKE '%_direct'        THEN 'DIRECT'
            WHEN metric_name LIKE '%_social'        THEN 'SOCIAL'
            WHEN metric_name LIKE '%_programmatic'  THEN 'PROGRAMMATIC'
            WHEN metric_name LIKE '%_other'         THEN 'OTHER'
        END                                         AS channel,
        max_data_date,
        CAST(NULL AS STRING)                        AS dimension_name,
        CAST(NULL AS STRING)                        AS dimension_value,
        metric_name, metric_value, metric_value_wow, metric_value_ly, wow_pct, yoy_pct
    FROM adobe_long
)

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
    MAX(max_data_date) OVER (PARTITION BY data_source)                  AS max_data_date
FROM combined
ORDER BY
    week_sun_to_sat  ASC,
    data_source      ASC,
    channel          ASC,
    metric_name      ASC,
    dimension_name   ASC
;