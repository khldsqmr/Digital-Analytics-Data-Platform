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
  2026-05-31 — Adobe: new metrics added, pct/cvr naming convention applied
  2026-06-04 — Profound CIT & VIS rename
  2026-06-05 — Adobe BYOD funnel metrics added:
               New entryPages_long CTE — reads Silver 07 (42 metrics × 7 channels)
               New outcomes_long CTE  — reads Silver 08 (42 metrics × 7 channels)
               Both added to combined UNION ALL
               channel derived from metric_name suffix (same pattern as adobe_long)
  2026-06-XX — Adobe cvrPostpaid_allChannels and cvrHsi_allChannels added to adobe_long UNPIVOT
  2026-06-XX — Other segregation:
               byodEntryOtherPageVisitors redefined to otherpage2 (no column rename)
               entryPages_long: added 3 new metrics x 7 channels = 21 new UNPIVOT entries
               byodEntryStorePageVisitors, byodEntryByodLandingPageVisitors, byodEntryOffersSwitchVisitors
               channel derivation LIKE patterns in combined CTE unchanged (handles new metric names)
================================================================================================= */

CREATE OR REPLACE VIEW `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_gold_unified_long`
AS

WITH

-- -----------------------------------------------------------------------
-- PROFOUND: 15 rows/week
-- -----------------------------------------------------------------------
profound_long AS (
    SELECT week_sun_to_sat, data_source, channel, max_data_date, metric_name, metric_value, metric_value_wow, metric_value_ly, wow_pct, yoy_pct
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_silver_profound_weekly`
    UNPIVOT (
        (metric_value, metric_value_wow, metric_value_ly, wow_pct, yoy_pct)
        FOR metric_name IN (
            (profoundVis_tmo_nonbrand_visibilityScore,    profoundVis_tmo_nonbrand_visibilityScore_wow,    profoundVis_tmo_nonbrand_visibilityScore_ly,    profoundVis_tmo_nonbrand_visibilityScore_wow_pct,    profoundVis_tmo_nonbrand_visibilityScore_yoy_pct)    AS 'profoundVis_tmo_nonbrand_visibilityScore',
            (profoundVis_tmo_nonbrand_executions,         profoundVis_tmo_nonbrand_executions_wow,         profoundVis_tmo_nonbrand_executions_ly,         profoundVis_tmo_nonbrand_executions_wow_pct,         profoundVis_tmo_nonbrand_executions_yoy_pct)         AS 'profoundVis_tmo_nonbrand_executions',
            (profoundVis_tmo_nonbrand_mentionsCount,      profoundVis_tmo_nonbrand_mentionsCount_wow,      profoundVis_tmo_nonbrand_mentionsCount_ly,      profoundVis_tmo_nonbrand_mentionsCount_wow_pct,      profoundVis_tmo_nonbrand_mentionsCount_yoy_pct)      AS 'profoundVis_tmo_nonbrand_mentionsCount',
            (profoundVis_tmo_nonbrand_shareOfVoice,       profoundVis_tmo_nonbrand_shareOfVoice_wow,       profoundVis_tmo_nonbrand_shareOfVoice_ly,       profoundVis_tmo_nonbrand_shareOfVoice_wow_pct,       profoundVis_tmo_nonbrand_shareOfVoice_yoy_pct)       AS 'profoundVis_tmo_nonbrand_shareOfVoice',
            (profoundVis_verizon_nonbrand_visibilityScore,profoundVis_verizon_nonbrand_visibilityScore_wow,profoundVis_verizon_nonbrand_visibilityScore_ly,profoundVis_verizon_nonbrand_visibilityScore_wow_pct,profoundVis_verizon_nonbrand_visibilityScore_yoy_pct) AS 'profoundVis_verizon_nonbrand_visibilityScore',
            (profoundVis_verizon_nonbrand_executions,     profoundVis_verizon_nonbrand_executions_wow,     profoundVis_verizon_nonbrand_executions_ly,     profoundVis_verizon_nonbrand_executions_wow_pct,     profoundVis_verizon_nonbrand_executions_yoy_pct)     AS 'profoundVis_verizon_nonbrand_executions',
            (profoundVis_verizon_nonbrand_mentionsCount,  profoundVis_verizon_nonbrand_mentionsCount_wow,  profoundVis_verizon_nonbrand_mentionsCount_ly,  profoundVis_verizon_nonbrand_mentionsCount_wow_pct,  profoundVis_verizon_nonbrand_mentionsCount_yoy_pct)  AS 'profoundVis_verizon_nonbrand_mentionsCount',
            (profoundVis_verizon_nonbrand_shareOfVoice,   profoundVis_verizon_nonbrand_shareOfVoice_wow,   profoundVis_verizon_nonbrand_shareOfVoice_ly,   profoundVis_verizon_nonbrand_shareOfVoice_wow_pct,   profoundVis_verizon_nonbrand_shareOfVoice_yoy_pct)   AS 'profoundVis_verizon_nonbrand_shareOfVoice',
            (profoundVis_att_nonbrand_visibilityScore,    profoundVis_att_nonbrand_visibilityScore_wow,    profoundVis_att_nonbrand_visibilityScore_ly,    profoundVis_att_nonbrand_visibilityScore_wow_pct,    profoundVis_att_nonbrand_visibilityScore_yoy_pct)    AS 'profoundVis_att_nonbrand_visibilityScore',
            (profoundVis_att_nonbrand_executions,         profoundVis_att_nonbrand_executions_wow,         profoundVis_att_nonbrand_executions_ly,         profoundVis_att_nonbrand_executions_wow_pct,         profoundVis_att_nonbrand_executions_yoy_pct)         AS 'profoundVis_att_nonbrand_executions',
            (profoundVis_att_nonbrand_mentionsCount,      profoundVis_att_nonbrand_mentionsCount_wow,      profoundVis_att_nonbrand_mentionsCount_ly,      profoundVis_att_nonbrand_mentionsCount_wow_pct,      profoundVis_att_nonbrand_mentionsCount_yoy_pct)      AS 'profoundVis_att_nonbrand_mentionsCount',
            (profoundVis_att_nonbrand_shareOfVoice,       profoundVis_att_nonbrand_shareOfVoice_wow,       profoundVis_att_nonbrand_shareOfVoice_ly,       profoundVis_att_nonbrand_shareOfVoice_wow_pct,       profoundVis_att_nonbrand_shareOfVoice_yoy_pct)       AS 'profoundVis_att_nonbrand_shareOfVoice',
            (profoundCit_tmo_nonbrand_shareOfVoice,       profoundCit_tmo_nonbrand_shareOfVoice_wow,       profoundCit_tmo_nonbrand_shareOfVoice_ly,       profoundCit_tmo_nonbrand_shareOfVoice_wow_pct,       profoundCit_tmo_nonbrand_shareOfVoice_yoy_pct)       AS 'profoundCit_tmo_nonbrand_shareOfVoice',
            (profoundCit_verizon_nonbrand_shareOfVoice,   profoundCit_verizon_nonbrand_shareOfVoice_wow,   profoundCit_verizon_nonbrand_shareOfVoice_ly,   profoundCit_verizon_nonbrand_shareOfVoice_wow_pct,   profoundCit_verizon_nonbrand_shareOfVoice_yoy_pct)   AS 'profoundCit_verizon_nonbrand_shareOfVoice',
            (profoundCit_att_nonbrand_shareOfVoice,       profoundCit_att_nonbrand_shareOfVoice_wow,       profoundCit_att_nonbrand_shareOfVoice_ly,       profoundCit_att_nonbrand_shareOfVoice_wow_pct,       profoundCit_att_nonbrand_shareOfVoice_yoy_pct)       AS 'profoundCit_att_nonbrand_shareOfVoice'
        )
    )
),

-- -----------------------------------------------------------------------
-- GOFISH: 12 rows/week
-- -----------------------------------------------------------------------
gofish_long AS (
    SELECT week_sun_to_sat, data_source, channel, max_data_date, metric_name, metric_value, metric_value_wow, metric_value_ly, wow_pct, yoy_pct
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_silver_profoundGofish_weekly`
    UNPIVOT (
        (metric_value, metric_value_wow, metric_value_ly, wow_pct, yoy_pct)
        FOR metric_name IN (
            (gofish_tmo_brand_visibilityScore,    gofish_tmo_brand_visibilityScore_wow,    gofish_tmo_brand_visibilityScore_ly,    gofish_tmo_brand_visibilityScore_wow_pct,    gofish_tmo_brand_visibilityScore_yoy_pct)    AS 'gofish_tmo_brand_visibilityScore',
            (gofish_tmo_brand_executions,         gofish_tmo_brand_executions_wow,         gofish_tmo_brand_executions_ly,         gofish_tmo_brand_executions_wow_pct,         gofish_tmo_brand_executions_yoy_pct)         AS 'gofish_tmo_brand_executions',
            (gofish_tmo_brand_mentionsCount,      gofish_tmo_brand_mentionsCount_wow,      gofish_tmo_brand_mentionsCount_ly,      gofish_tmo_brand_mentionsCount_wow_pct,      gofish_tmo_brand_mentionsCount_yoy_pct)      AS 'gofish_tmo_brand_mentionsCount',
            (gofish_tmo_brand_shareOfVoice,       gofish_tmo_brand_shareOfVoice_wow,       gofish_tmo_brand_shareOfVoice_ly,       gofish_tmo_brand_shareOfVoice_wow_pct,       gofish_tmo_brand_shareOfVoice_yoy_pct)       AS 'gofish_tmo_brand_shareOfVoice',
            (gofish_verizon_brand_visibilityScore,gofish_verizon_brand_visibilityScore_wow,gofish_verizon_brand_visibilityScore_ly,gofish_verizon_brand_visibilityScore_wow_pct,gofish_verizon_brand_visibilityScore_yoy_pct) AS 'gofish_verizon_brand_visibilityScore',
            (gofish_verizon_brand_executions,     gofish_verizon_brand_executions_wow,     gofish_verizon_brand_executions_ly,     gofish_verizon_brand_executions_wow_pct,     gofish_verizon_brand_executions_yoy_pct)     AS 'gofish_verizon_brand_executions',
            (gofish_verizon_brand_mentionsCount,  gofish_verizon_brand_mentionsCount_wow,  gofish_verizon_brand_mentionsCount_ly,  gofish_verizon_brand_mentionsCount_wow_pct,  gofish_verizon_brand_mentionsCount_yoy_pct)  AS 'gofish_verizon_brand_mentionsCount',
            (gofish_verizon_brand_shareOfVoice,   gofish_verizon_brand_shareOfVoice_wow,   gofish_verizon_brand_shareOfVoice_ly,   gofish_verizon_brand_shareOfVoice_wow_pct,   gofish_verizon_brand_shareOfVoice_yoy_pct)   AS 'gofish_verizon_brand_shareOfVoice',
            (gofish_att_brand_visibilityScore,    gofish_att_brand_visibilityScore_wow,    gofish_att_brand_visibilityScore_ly,    gofish_att_brand_visibilityScore_wow_pct,    gofish_att_brand_visibilityScore_yoy_pct)    AS 'gofish_att_brand_visibilityScore',
            (gofish_att_brand_executions,         gofish_att_brand_executions_wow,         gofish_att_brand_executions_ly,         gofish_att_brand_executions_wow_pct,         gofish_att_brand_executions_yoy_pct)         AS 'gofish_att_brand_executions',
            (gofish_att_brand_mentionsCount,      gofish_att_brand_mentionsCount_wow,      gofish_att_brand_mentionsCount_ly,      gofish_att_brand_mentionsCount_wow_pct,      gofish_att_brand_mentionsCount_yoy_pct)      AS 'gofish_att_brand_mentionsCount',
            (gofish_att_brand_shareOfVoice,       gofish_att_brand_shareOfVoice_wow,       gofish_att_brand_shareOfVoice_ly,       gofish_att_brand_shareOfVoice_wow_pct,       gofish_att_brand_shareOfVoice_yoy_pct)       AS 'gofish_att_brand_shareOfVoice'
        )
    )
),

-- -----------------------------------------------------------------------
-- SA360: 12 rows/week
-- -----------------------------------------------------------------------
sa360_long AS (
    SELECT week_sun_to_sat, data_source, channel, max_data_date, metric_name, metric_value, metric_value_wow, metric_value_ly, wow_pct, yoy_pct
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_silver_sa360_weekly`
    UNPIVOT (
        (metric_value, metric_value_wow, metric_value_ly, wow_pct, yoy_pct)
        FOR metric_name IN (
            (sa360_tmo_brand_impressions,      sa360_tmo_brand_impressions_wow,      sa360_tmo_brand_impressions_ly,      sa360_tmo_brand_impressions_wow_pct,      sa360_tmo_brand_impressions_yoy_pct)      AS 'sa360_tmo_brand_impressions',
            (sa360_tmo_brand_clicks,           sa360_tmo_brand_clicks_wow,           sa360_tmo_brand_clicks_ly,           sa360_tmo_brand_clicks_wow_pct,           sa360_tmo_brand_clicks_yoy_pct)           AS 'sa360_tmo_brand_clicks',
            (sa360_tmo_brand_cost,             sa360_tmo_brand_cost_wow,             sa360_tmo_brand_cost_ly,             sa360_tmo_brand_cost_wow_pct,             sa360_tmo_brand_cost_yoy_pct)             AS 'sa360_tmo_brand_cost',
            (sa360_tmo_brand_orders,           sa360_tmo_brand_orders_wow,           sa360_tmo_brand_orders_ly,           sa360_tmo_brand_orders_wow_pct,           sa360_tmo_brand_orders_yoy_pct)           AS 'sa360_tmo_brand_orders',
            (sa360_tmo_brand_cart_start,       sa360_tmo_brand_cart_start_wow,       sa360_tmo_brand_cart_start_ly,       sa360_tmo_brand_cart_start_wow_pct,       sa360_tmo_brand_cart_start_yoy_pct)       AS 'sa360_tmo_brand_cart_start',
            (sa360_tmo_brand_postpaid_pspv,    sa360_tmo_brand_postpaid_pspv_wow,    sa360_tmo_brand_postpaid_pspv_ly,    sa360_tmo_brand_postpaid_pspv_wow_pct,    sa360_tmo_brand_postpaid_pspv_yoy_pct)    AS 'sa360_tmo_brand_postpaid_pspv',
            (sa360_tmo_nonbrand_impressions,   sa360_tmo_nonbrand_impressions_wow,   sa360_tmo_nonbrand_impressions_ly,   sa360_tmo_nonbrand_impressions_wow_pct,   sa360_tmo_nonbrand_impressions_yoy_pct)   AS 'sa360_tmo_nonbrand_impressions',
            (sa360_tmo_nonbrand_clicks,        sa360_tmo_nonbrand_clicks_wow,        sa360_tmo_nonbrand_clicks_ly,        sa360_tmo_nonbrand_clicks_wow_pct,        sa360_tmo_nonbrand_clicks_yoy_pct)        AS 'sa360_tmo_nonbrand_clicks',
            (sa360_tmo_nonbrand_cost,          sa360_tmo_nonbrand_cost_wow,          sa360_tmo_nonbrand_cost_ly,          sa360_tmo_nonbrand_cost_wow_pct,          sa360_tmo_nonbrand_cost_yoy_pct)          AS 'sa360_tmo_nonbrand_cost',
            (sa360_tmo_nonbrand_orders,        sa360_tmo_nonbrand_orders_wow,        sa360_tmo_nonbrand_orders_ly,        sa360_tmo_nonbrand_orders_wow_pct,        sa360_tmo_nonbrand_orders_yoy_pct)        AS 'sa360_tmo_nonbrand_orders',
            (sa360_tmo_nonbrand_cart_start,    sa360_tmo_nonbrand_cart_start_wow,    sa360_tmo_nonbrand_cart_start_ly,    sa360_tmo_nonbrand_cart_start_wow_pct,    sa360_tmo_nonbrand_cart_start_yoy_pct)    AS 'sa360_tmo_nonbrand_cart_start',
            (sa360_tmo_nonbrand_postpaid_pspv, sa360_tmo_nonbrand_postpaid_pspv_wow, sa360_tmo_nonbrand_postpaid_pspv_ly, sa360_tmo_nonbrand_postpaid_pspv_wow_pct, sa360_tmo_nonbrand_postpaid_pspv_yoy_pct) AS 'sa360_tmo_nonbrand_postpaid_pspv'
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
-- TRENDS: Keywords — up to 10 rows/week
-- -----------------------------------------------------------------------
trends_keywords_long AS (
    SELECT week_sun_to_sat, data_source, channel, max_data_date, 'KEYWORD_RANK_1' AS dimension_name, trends_top_kw_1 AS dimension_value, 'trends_kw_interest' AS metric_name, trends_kw1_interest AS metric_value, CAST(NULL AS FLOAT64) AS metric_value_wow, CAST(NULL AS FLOAT64) AS metric_value_ly, CAST(NULL AS FLOAT64) AS wow_pct, CAST(NULL AS FLOAT64) AS yoy_pct FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_silver_googleTrends_weekly` WHERE NULLIF(TRIM(trends_top_kw_1), '') IS NOT NULL
    UNION ALL SELECT week_sun_to_sat, data_source, channel, max_data_date, 'KEYWORD_RANK_1', trends_top_kw_1, 'trends_kw_change', trends_kw1_change, CAST(NULL AS FLOAT64), CAST(NULL AS FLOAT64), CAST(NULL AS FLOAT64), CAST(NULL AS FLOAT64) FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_silver_googleTrends_weekly` WHERE NULLIF(TRIM(trends_top_kw_1), '') IS NOT NULL
    UNION ALL SELECT week_sun_to_sat, data_source, channel, max_data_date, 'KEYWORD_RANK_2', trends_top_kw_2, 'trends_kw_interest', trends_kw2_interest, CAST(NULL AS FLOAT64), CAST(NULL AS FLOAT64), CAST(NULL AS FLOAT64), CAST(NULL AS FLOAT64) FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_silver_googleTrends_weekly` WHERE NULLIF(TRIM(trends_top_kw_2), '') IS NOT NULL
    UNION ALL SELECT week_sun_to_sat, data_source, channel, max_data_date, 'KEYWORD_RANK_2', trends_top_kw_2, 'trends_kw_change', trends_kw2_change, CAST(NULL AS FLOAT64), CAST(NULL AS FLOAT64), CAST(NULL AS FLOAT64), CAST(NULL AS FLOAT64) FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_silver_googleTrends_weekly` WHERE NULLIF(TRIM(trends_top_kw_2), '') IS NOT NULL
    UNION ALL SELECT week_sun_to_sat, data_source, channel, max_data_date, 'KEYWORD_RANK_3', trends_top_kw_3, 'trends_kw_interest', trends_kw3_interest, CAST(NULL AS FLOAT64), CAST(NULL AS FLOAT64), CAST(NULL AS FLOAT64), CAST(NULL AS FLOAT64) FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_silver_googleTrends_weekly` WHERE NULLIF(TRIM(trends_top_kw_3), '') IS NOT NULL
    UNION ALL SELECT week_sun_to_sat, data_source, channel, max_data_date, 'KEYWORD_RANK_3', trends_top_kw_3, 'trends_kw_change', trends_kw3_change, CAST(NULL AS FLOAT64), CAST(NULL AS FLOAT64), CAST(NULL AS FLOAT64), CAST(NULL AS FLOAT64) FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_silver_googleTrends_weekly` WHERE NULLIF(TRIM(trends_top_kw_3), '') IS NOT NULL
    UNION ALL SELECT week_sun_to_sat, data_source, channel, max_data_date, 'KEYWORD_RANK_4', trends_top_kw_4, 'trends_kw_interest', trends_kw4_interest, CAST(NULL AS FLOAT64), CAST(NULL AS FLOAT64), CAST(NULL AS FLOAT64), CAST(NULL AS FLOAT64) FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_silver_googleTrends_weekly` WHERE NULLIF(TRIM(trends_top_kw_4), '') IS NOT NULL
    UNION ALL SELECT week_sun_to_sat, data_source, channel, max_data_date, 'KEYWORD_RANK_4', trends_top_kw_4, 'trends_kw_change', trends_kw4_change, CAST(NULL AS FLOAT64), CAST(NULL AS FLOAT64), CAST(NULL AS FLOAT64), CAST(NULL AS FLOAT64) FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_silver_googleTrends_weekly` WHERE NULLIF(TRIM(trends_top_kw_4), '') IS NOT NULL
    UNION ALL SELECT week_sun_to_sat, data_source, channel, max_data_date, 'KEYWORD_RANK_5', trends_top_kw_5, 'trends_kw_interest', trends_kw5_interest, CAST(NULL AS FLOAT64), CAST(NULL AS FLOAT64), CAST(NULL AS FLOAT64), CAST(NULL AS FLOAT64) FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_silver_googleTrends_weekly` WHERE NULLIF(TRIM(trends_top_kw_5), '') IS NOT NULL
    UNION ALL SELECT week_sun_to_sat, data_source, channel, max_data_date, 'KEYWORD_RANK_5', trends_top_kw_5, 'trends_kw_change', trends_kw5_change, CAST(NULL AS FLOAT64), CAST(NULL AS FLOAT64), CAST(NULL AS FLOAT64), CAST(NULL AS FLOAT64) FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_silver_googleTrends_weekly` WHERE NULLIF(TRIM(trends_top_kw_5), '') IS NOT NULL
),

-- -----------------------------------------------------------------------
-- ADOBE (Silver 06): conversion metrics
-- cvrPostpaid_allChannels and cvrHsi_allChannels added 2026-06-XX
-- -----------------------------------------------------------------------
adobe_long AS (
    SELECT week_sun_to_sat, data_source, max_data_date, metric_name, metric_value, metric_value_wow, metric_value_ly, wow_pct, yoy_pct
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_silver_adobe_weekly`
    UNPIVOT (
        (metric_value, metric_value_wow, metric_value_ly, wow_pct, yoy_pct)
        FOR metric_name IN (
            (adobe_uvnbByod_allChannels,                   adobe_uvnbByod_allChannels_wow,                   adobe_uvnbByod_allChannels_ly,                   adobe_uvnbByod_allChannels_wow_pct,                   adobe_uvnbByod_allChannels_yoy_pct)                   AS 'adobe_uvnbByod_allChannels',
            (adobe_uvnbTotal_allChannels,                  adobe_uvnbTotal_allChannels_wow,                  adobe_uvnbTotal_allChannels_ly,                  adobe_uvnbTotal_allChannels_wow_pct,                  adobe_uvnbTotal_allChannels_yoy_pct)                  AS 'adobe_uvnbTotal_allChannels',
            (adobe_uvnbFlowTotal_allChannels,              adobe_uvnbFlowTotal_allChannels_wow,              adobe_uvnbFlowTotal_allChannels_ly,              adobe_uvnbFlowTotal_allChannels_wow_pct,              adobe_uvnbFlowTotal_allChannels_yoy_pct)              AS 'adobe_uvnbFlowTotal_allChannels',
            (adobe_pctUvnbByodOfUvnbFlow_allChannels,      adobe_pctUvnbByodOfUvnbFlow_allChannels_wow,      adobe_pctUvnbByodOfUvnbFlow_allChannels_ly,      adobe_pctUvnbByodOfUvnbFlow_allChannels_wow_pct,      adobe_pctUvnbByodOfUvnbFlow_allChannels_yoy_pct)      AS 'adobe_pctUvnbByodOfUvnbFlow_allChannels',
            (adobe_cartStartByod_allChannels,              adobe_cartStartByod_allChannels_wow,              adobe_cartStartByod_allChannels_ly,              adobe_cartStartByod_allChannels_wow_pct,              adobe_cartStartByod_allChannels_yoy_pct)              AS 'adobe_cartStartByod_allChannels',
            (adobe_ordersUnassistedByod_allChannels,       adobe_ordersUnassistedByod_allChannels_wow,       adobe_ordersUnassistedByod_allChannels_ly,       adobe_ordersUnassistedByod_allChannels_wow_pct,       adobe_ordersUnassistedByod_allChannels_yoy_pct)       AS 'adobe_ordersUnassistedByod_allChannels',
            (adobe_ordersAssistedByod_allChannels,         adobe_ordersAssistedByod_allChannels_wow,         adobe_ordersAssistedByod_allChannels_ly,         adobe_ordersAssistedByod_allChannels_wow_pct,         adobe_ordersAssistedByod_allChannels_yoy_pct)         AS 'adobe_ordersAssistedByod_allChannels',
            (adobe_ordersTotalByod_allChannels,            adobe_ordersTotalByod_allChannels_wow,            adobe_ordersTotalByod_allChannels_ly,            adobe_ordersTotalByod_allChannels_wow_pct,            adobe_ordersTotalByod_allChannels_yoy_pct)            AS 'adobe_ordersTotalByod_allChannels',
            (adobe_ordersTotal_allChannels,                adobe_ordersTotal_allChannels_wow,                adobe_ordersTotal_allChannels_ly,                adobe_ordersTotal_allChannels_wow_pct,                adobe_ordersTotal_allChannels_yoy_pct)                AS 'adobe_ordersTotal_allChannels',
            (adobe_pctOrdersByodOfOrdersTotal_allChannels, adobe_pctOrdersByodOfOrdersTotal_allChannels_wow, adobe_pctOrdersByodOfOrdersTotal_allChannels_ly, adobe_pctOrdersByodOfOrdersTotal_allChannels_wow_pct, adobe_pctOrdersByodOfOrdersTotal_allChannels_yoy_pct) AS 'adobe_pctOrdersByodOfOrdersTotal_allChannels',
            (adobe_cvrByod_allChannels,                    adobe_cvrByod_allChannels_wow,                    adobe_cvrByod_allChannels_ly,                    adobe_cvrByod_allChannels_wow_pct,                    adobe_cvrByod_allChannels_yoy_pct)                    AS 'adobe_cvrByod_allChannels',
            (adobe_cvrSite_allChannels,                    adobe_cvrSite_allChannels_wow,                    adobe_cvrSite_allChannels_ly,                    adobe_cvrSite_allChannels_wow_pct,                    adobe_cvrSite_allChannels_yoy_pct)                    AS 'adobe_cvrSite_allChannels',
            -- NEW: Postpaid and HSI CVR
            (adobe_cvrPostpaid_allChannels,                adobe_cvrPostpaid_allChannels_wow,                adobe_cvrPostpaid_allChannels_ly,                adobe_cvrPostpaid_allChannels_wow_pct,                adobe_cvrPostpaid_allChannels_yoy_pct)                AS 'adobe_cvrPostpaid_allChannels',
            (adobe_cvrHsi_allChannels,                     adobe_cvrHsi_allChannels_wow,                     adobe_cvrHsi_allChannels_ly,                     adobe_cvrHsi_allChannels_wow_pct,                     adobe_cvrHsi_allChannels_yoy_pct)                     AS 'adobe_cvrHsi_allChannels',
            (adobe_uvnbByod_paidSearch,                    adobe_uvnbByod_paidSearch_wow,                    adobe_uvnbByod_paidSearch_ly,                    adobe_uvnbByod_paidSearch_wow_pct,                    adobe_uvnbByod_paidSearch_yoy_pct)                    AS 'adobe_uvnbByod_paidSearch',
            (adobe_pctUvnbByodOfTotal_paidSearch,          adobe_pctUvnbByodOfTotal_paidSearch_wow,          adobe_pctUvnbByodOfTotal_paidSearch_ly,          adobe_pctUvnbByodOfTotal_paidSearch_wow_pct,          adobe_pctUvnbByodOfTotal_paidSearch_yoy_pct)          AS 'adobe_pctUvnbByodOfTotal_paidSearch',
            (adobe_cartStartByod_paidSearch,               adobe_cartStartByod_paidSearch_wow,               adobe_cartStartByod_paidSearch_ly,               adobe_cartStartByod_paidSearch_wow_pct,               adobe_cartStartByod_paidSearch_yoy_pct)               AS 'adobe_cartStartByod_paidSearch',
            (adobe_ordersUnassistedByod_paidSearch,        adobe_ordersUnassistedByod_paidSearch_wow,        adobe_ordersUnassistedByod_paidSearch_ly,        adobe_ordersUnassistedByod_paidSearch_wow_pct,        adobe_ordersUnassistedByod_paidSearch_yoy_pct)        AS 'adobe_ordersUnassistedByod_paidSearch',
            (adobe_ordersAssistedByod_paidSearch,          adobe_ordersAssistedByod_paidSearch_wow,          adobe_ordersAssistedByod_paidSearch_ly,          adobe_ordersAssistedByod_paidSearch_wow_pct,          adobe_ordersAssistedByod_paidSearch_yoy_pct)          AS 'adobe_ordersAssistedByod_paidSearch',
            (adobe_ordersTotalByod_paidSearch,             adobe_ordersTotalByod_paidSearch_wow,             adobe_ordersTotalByod_paidSearch_ly,             adobe_ordersTotalByod_paidSearch_wow_pct,             adobe_ordersTotalByod_paidSearch_yoy_pct)             AS 'adobe_ordersTotalByod_paidSearch',
            (adobe_uvnbByod_organicSearch,                 adobe_uvnbByod_organicSearch_wow,                 adobe_uvnbByod_organicSearch_ly,                 adobe_uvnbByod_organicSearch_wow_pct,                 adobe_uvnbByod_organicSearch_yoy_pct)                 AS 'adobe_uvnbByod_organicSearch',
            (adobe_pctUvnbByodOfTotal_organicSearch,       adobe_pctUvnbByodOfTotal_organicSearch_wow,       adobe_pctUvnbByodOfTotal_organicSearch_ly,       adobe_pctUvnbByodOfTotal_organicSearch_wow_pct,       adobe_pctUvnbByodOfTotal_organicSearch_yoy_pct)       AS 'adobe_pctUvnbByodOfTotal_organicSearch',
            (adobe_cartStartByod_organicSearch,            adobe_cartStartByod_organicSearch_wow,            adobe_cartStartByod_organicSearch_ly,            adobe_cartStartByod_organicSearch_wow_pct,            adobe_cartStartByod_organicSearch_yoy_pct)            AS 'adobe_cartStartByod_organicSearch',
            (adobe_ordersUnassistedByod_organicSearch,     adobe_ordersUnassistedByod_organicSearch_wow,     adobe_ordersUnassistedByod_organicSearch_ly,     adobe_ordersUnassistedByod_organicSearch_wow_pct,     adobe_ordersUnassistedByod_organicSearch_yoy_pct)     AS 'adobe_ordersUnassistedByod_organicSearch',
            (adobe_ordersAssistedByod_organicSearch,       adobe_ordersAssistedByod_organicSearch_wow,       adobe_ordersAssistedByod_organicSearch_ly,       adobe_ordersAssistedByod_organicSearch_wow_pct,       adobe_ordersAssistedByod_organicSearch_yoy_pct)       AS 'adobe_ordersAssistedByod_organicSearch',
            (adobe_ordersTotalByod_organicSearch,          adobe_ordersTotalByod_organicSearch_wow,          adobe_ordersTotalByod_organicSearch_ly,          adobe_ordersTotalByod_organicSearch_wow_pct,          adobe_ordersTotalByod_organicSearch_yoy_pct)          AS 'adobe_ordersTotalByod_organicSearch',
            (adobe_uvnbByod_direct,                        adobe_uvnbByod_direct_wow,                        adobe_uvnbByod_direct_ly,                        adobe_uvnbByod_direct_wow_pct,                        adobe_uvnbByod_direct_yoy_pct)                        AS 'adobe_uvnbByod_direct',
            (adobe_pctUvnbByodOfTotal_direct,              adobe_pctUvnbByodOfTotal_direct_wow,              adobe_pctUvnbByodOfTotal_direct_ly,              adobe_pctUvnbByodOfTotal_direct_wow_pct,              adobe_pctUvnbByodOfTotal_direct_yoy_pct)              AS 'adobe_pctUvnbByodOfTotal_direct',
            (adobe_cartStartByod_direct,                   adobe_cartStartByod_direct_wow,                   adobe_cartStartByod_direct_ly,                   adobe_cartStartByod_direct_wow_pct,                   adobe_cartStartByod_direct_yoy_pct)                   AS 'adobe_cartStartByod_direct',
            (adobe_ordersUnassistedByod_direct,            adobe_ordersUnassistedByod_direct_wow,            adobe_ordersUnassistedByod_direct_ly,            adobe_ordersUnassistedByod_direct_wow_pct,            adobe_ordersUnassistedByod_direct_yoy_pct)            AS 'adobe_ordersUnassistedByod_direct',
            (adobe_ordersAssistedByod_direct,              adobe_ordersAssistedByod_direct_wow,              adobe_ordersAssistedByod_direct_ly,              adobe_ordersAssistedByod_direct_wow_pct,              adobe_ordersAssistedByod_direct_yoy_pct)              AS 'adobe_ordersAssistedByod_direct',
            (adobe_ordersTotalByod_direct,                 adobe_ordersTotalByod_direct_wow,                 adobe_ordersTotalByod_direct_ly,                 adobe_ordersTotalByod_direct_wow_pct,                 adobe_ordersTotalByod_direct_yoy_pct)                 AS 'adobe_ordersTotalByod_direct',
            (adobe_uvnbByod_social,                        adobe_uvnbByod_social_wow,                        adobe_uvnbByod_social_ly,                        adobe_uvnbByod_social_wow_pct,                        adobe_uvnbByod_social_yoy_pct)                        AS 'adobe_uvnbByod_social',
            (adobe_pctUvnbByodOfTotal_social,              adobe_pctUvnbByodOfTotal_social_wow,              adobe_pctUvnbByodOfTotal_social_ly,              adobe_pctUvnbByodOfTotal_social_wow_pct,              adobe_pctUvnbByodOfTotal_social_yoy_pct)              AS 'adobe_pctUvnbByodOfTotal_social',
            (adobe_cartStartByod_social,                   adobe_cartStartByod_social_wow,                   adobe_cartStartByod_social_ly,                   adobe_cartStartByod_social_wow_pct,                   adobe_cartStartByod_social_yoy_pct)                   AS 'adobe_cartStartByod_social',
            (adobe_ordersUnassistedByod_social,            adobe_ordersUnassistedByod_social_wow,            adobe_ordersUnassistedByod_social_ly,            adobe_ordersUnassistedByod_social_wow_pct,            adobe_ordersUnassistedByod_social_yoy_pct)            AS 'adobe_ordersUnassistedByod_social',
            (adobe_ordersAssistedByod_social,              adobe_ordersAssistedByod_social_wow,              adobe_ordersAssistedByod_social_ly,              adobe_ordersAssistedByod_social_wow_pct,              adobe_ordersAssistedByod_social_yoy_pct)              AS 'adobe_ordersAssistedByod_social',
            (adobe_ordersTotalByod_social,                 adobe_ordersTotalByod_social_wow,                 adobe_ordersTotalByod_social_ly,                 adobe_ordersTotalByod_social_wow_pct,                 adobe_ordersTotalByod_social_yoy_pct)                 AS 'adobe_ordersTotalByod_social',
            (adobe_uvnbByod_programmatic,                  adobe_uvnbByod_programmatic_wow,                  adobe_uvnbByod_programmatic_ly,                  adobe_uvnbByod_programmatic_wow_pct,                  adobe_uvnbByod_programmatic_yoy_pct)                  AS 'adobe_uvnbByod_programmatic',
            (adobe_pctUvnbByodOfTotal_programmatic,        adobe_pctUvnbByodOfTotal_programmatic_wow,        adobe_pctUvnbByodOfTotal_programmatic_ly,        adobe_pctUvnbByodOfTotal_programmatic_wow_pct,        adobe_pctUvnbByodOfTotal_programmatic_yoy_pct)        AS 'adobe_pctUvnbByodOfTotal_programmatic',
            (adobe_cartStartByod_programmatic,             adobe_cartStartByod_programmatic_wow,             adobe_cartStartByod_programmatic_ly,             adobe_cartStartByod_programmatic_wow_pct,             adobe_cartStartByod_programmatic_yoy_pct)             AS 'adobe_cartStartByod_programmatic',
            (adobe_ordersUnassistedByod_programmatic,      adobe_ordersUnassistedByod_programmatic_wow,      adobe_ordersUnassistedByod_programmatic_ly,      adobe_ordersUnassistedByod_programmatic_wow_pct,      adobe_ordersUnassistedByod_programmatic_yoy_pct)      AS 'adobe_ordersUnassistedByod_programmatic',
            (adobe_ordersAssistedByod_programmatic,        adobe_ordersAssistedByod_programmatic_wow,        adobe_ordersAssistedByod_programmatic_ly,        adobe_ordersAssistedByod_programmatic_wow_pct,        adobe_ordersAssistedByod_programmatic_yoy_pct)        AS 'adobe_ordersAssistedByod_programmatic',
            (adobe_ordersTotalByod_programmatic,           adobe_ordersTotalByod_programmatic_wow,           adobe_ordersTotalByod_programmatic_ly,           adobe_ordersTotalByod_programmatic_wow_pct,           adobe_ordersTotalByod_programmatic_yoy_pct)           AS 'adobe_ordersTotalByod_programmatic',
            (adobe_uvnbByod_other,                         adobe_uvnbByod_other_wow,                         adobe_uvnbByod_other_ly,                         adobe_uvnbByod_other_wow_pct,                         adobe_uvnbByod_other_yoy_pct)                         AS 'adobe_uvnbByod_other',
            (adobe_pctUvnbByodOfTotal_other,               adobe_pctUvnbByodOfTotal_other_wow,               adobe_pctUvnbByodOfTotal_other_ly,               adobe_pctUvnbByodOfTotal_other_wow_pct,               adobe_pctUvnbByodOfTotal_other_yoy_pct)               AS 'adobe_pctUvnbByodOfTotal_other',
            (adobe_cartStartByod_other,                    adobe_cartStartByod_other_wow,                    adobe_cartStartByod_other_ly,                    adobe_cartStartByod_other_wow_pct,                    adobe_cartStartByod_other_yoy_pct)                    AS 'adobe_cartStartByod_other',
            (adobe_ordersUnassistedByod_other,             adobe_ordersUnassistedByod_other_wow,             adobe_ordersUnassistedByod_other_ly,             adobe_ordersUnassistedByod_other_wow_pct,             adobe_ordersUnassistedByod_other_yoy_pct)             AS 'adobe_ordersUnassistedByod_other',
            (adobe_ordersAssistedByod_other,               adobe_ordersAssistedByod_other_wow,               adobe_ordersAssistedByod_other_ly,               adobe_ordersAssistedByod_other_wow_pct,               adobe_ordersAssistedByod_other_yoy_pct)               AS 'adobe_ordersAssistedByod_other',
            (adobe_ordersTotalByod_other,                  adobe_ordersTotalByod_other_wow,                  adobe_ordersTotalByod_other_ly,                  adobe_ordersTotalByod_other_wow_pct,                  adobe_ordersTotalByod_other_yoy_pct)                  AS 'adobe_ordersTotalByod_other'
        )
    )
),

-- -----------------------------------------------------------------------
-- ADOBE BYOD ENTRY PAGES (Silver 07): 9 metrics × 7 channels — 63 rows/week
-- -----------------------------------------------------------------------
entryPages_long AS (
    SELECT week_sun_to_sat, data_source, max_data_date, metric_name, metric_value, metric_value_wow, metric_value_ly, wow_pct, yoy_pct
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_silver_adobeByodEntryPages_weekly`
    UNPIVOT (
        (metric_value, metric_value_wow, metric_value_ly, wow_pct, yoy_pct)
        FOR metric_name IN (
            (adobe_byodUvnbVisitors_allChannels, adobe_byodUvnbVisitors_allChannels_wow, adobe_byodUvnbVisitors_allChannels_ly, adobe_byodUvnbVisitors_allChannels_wow_pct, adobe_byodUvnbVisitors_allChannels_yoy_pct) AS 'adobe_byodUvnbVisitors_allChannels',
            (adobe_byodEntryByodPageVisitors_allChannels, adobe_byodEntryByodPageVisitors_allChannels_wow, adobe_byodEntryByodPageVisitors_allChannels_ly, adobe_byodEntryByodPageVisitors_allChannels_wow_pct, adobe_byodEntryByodPageVisitors_allChannels_yoy_pct) AS 'adobe_byodEntryByodPageVisitors_allChannels',
            (adobe_byodEntryHomePageVisitors_allChannels, adobe_byodEntryHomePageVisitors_allChannels_wow, adobe_byodEntryHomePageVisitors_allChannels_ly, adobe_byodEntryHomePageVisitors_allChannels_wow_pct, adobe_byodEntryHomePageVisitors_allChannels_yoy_pct) AS 'adobe_byodEntryHomePageVisitors_allChannels',
            (adobe_byodEntryDevicePageVisitors_allChannels, adobe_byodEntryDevicePageVisitors_allChannels_wow, adobe_byodEntryDevicePageVisitors_allChannels_ly, adobe_byodEntryDevicePageVisitors_allChannels_wow_pct, adobe_byodEntryDevicePageVisitors_allChannels_yoy_pct) AS 'adobe_byodEntryDevicePageVisitors_allChannels',
            (adobe_byodEntryPlansPageVisitors_allChannels, adobe_byodEntryPlansPageVisitors_allChannels_wow, adobe_byodEntryPlansPageVisitors_allChannels_ly, adobe_byodEntryPlansPageVisitors_allChannels_wow_pct, adobe_byodEntryPlansPageVisitors_allChannels_yoy_pct) AS 'adobe_byodEntryPlansPageVisitors_allChannels',
            (adobe_byodEntryOtherPageVisitors_allChannels, adobe_byodEntryOtherPageVisitors_allChannels_wow, adobe_byodEntryOtherPageVisitors_allChannels_ly, adobe_byodEntryOtherPageVisitors_allChannels_wow_pct, adobe_byodEntryOtherPageVisitors_allChannels_yoy_pct) AS 'adobe_byodEntryOtherPageVisitors_allChannels',
            (adobe_byodEntryStorePageVisitors_allChannels, adobe_byodEntryStorePageVisitors_allChannels_wow, adobe_byodEntryStorePageVisitors_allChannels_ly, adobe_byodEntryStorePageVisitors_allChannels_wow_pct, adobe_byodEntryStorePageVisitors_allChannels_yoy_pct) AS 'adobe_byodEntryStorePageVisitors_allChannels'  -- NEW,
            (adobe_byodEntryByodLandingPageVisitors_allChannels, adobe_byodEntryByodLandingPageVisitors_allChannels_wow, adobe_byodEntryByodLandingPageVisitors_allChannels_ly, adobe_byodEntryByodLandingPageVisitors_allChannels_wow_pct, adobe_byodEntryByodLandingPageVisitors_allChannels_yoy_pct) AS 'adobe_byodEntryByodLandingPageVisitors_allChannels'  -- NEW,
            (adobe_byodEntryOffersSwitchVisitors_allChannels, adobe_byodEntryOffersSwitchVisitors_allChannels_wow, adobe_byodEntryOffersSwitchVisitors_allChannels_ly, adobe_byodEntryOffersSwitchVisitors_allChannels_wow_pct, adobe_byodEntryOffersSwitchVisitors_allChannels_yoy_pct) AS 'adobe_byodEntryOffersSwitchVisitors_allChannels'  -- NEW,
            (adobe_byodUvnbVisitors_paidSearch, adobe_byodUvnbVisitors_paidSearch_wow, adobe_byodUvnbVisitors_paidSearch_ly, adobe_byodUvnbVisitors_paidSearch_wow_pct, adobe_byodUvnbVisitors_paidSearch_yoy_pct) AS 'adobe_byodUvnbVisitors_paidSearch',
            (adobe_byodEntryByodPageVisitors_paidSearch, adobe_byodEntryByodPageVisitors_paidSearch_wow, adobe_byodEntryByodPageVisitors_paidSearch_ly, adobe_byodEntryByodPageVisitors_paidSearch_wow_pct, adobe_byodEntryByodPageVisitors_paidSearch_yoy_pct) AS 'adobe_byodEntryByodPageVisitors_paidSearch',
            (adobe_byodEntryHomePageVisitors_paidSearch, adobe_byodEntryHomePageVisitors_paidSearch_wow, adobe_byodEntryHomePageVisitors_paidSearch_ly, adobe_byodEntryHomePageVisitors_paidSearch_wow_pct, adobe_byodEntryHomePageVisitors_paidSearch_yoy_pct) AS 'adobe_byodEntryHomePageVisitors_paidSearch',
            (adobe_byodEntryDevicePageVisitors_paidSearch, adobe_byodEntryDevicePageVisitors_paidSearch_wow, adobe_byodEntryDevicePageVisitors_paidSearch_ly, adobe_byodEntryDevicePageVisitors_paidSearch_wow_pct, adobe_byodEntryDevicePageVisitors_paidSearch_yoy_pct) AS 'adobe_byodEntryDevicePageVisitors_paidSearch',
            (adobe_byodEntryPlansPageVisitors_paidSearch, adobe_byodEntryPlansPageVisitors_paidSearch_wow, adobe_byodEntryPlansPageVisitors_paidSearch_ly, adobe_byodEntryPlansPageVisitors_paidSearch_wow_pct, adobe_byodEntryPlansPageVisitors_paidSearch_yoy_pct) AS 'adobe_byodEntryPlansPageVisitors_paidSearch',
            (adobe_byodEntryOtherPageVisitors_paidSearch, adobe_byodEntryOtherPageVisitors_paidSearch_wow, adobe_byodEntryOtherPageVisitors_paidSearch_ly, adobe_byodEntryOtherPageVisitors_paidSearch_wow_pct, adobe_byodEntryOtherPageVisitors_paidSearch_yoy_pct) AS 'adobe_byodEntryOtherPageVisitors_paidSearch',
            (adobe_byodEntryStorePageVisitors_paidSearch, adobe_byodEntryStorePageVisitors_paidSearch_wow, adobe_byodEntryStorePageVisitors_paidSearch_ly, adobe_byodEntryStorePageVisitors_paidSearch_wow_pct, adobe_byodEntryStorePageVisitors_paidSearch_yoy_pct) AS 'adobe_byodEntryStorePageVisitors_paidSearch'  -- NEW,
            (adobe_byodEntryByodLandingPageVisitors_paidSearch, adobe_byodEntryByodLandingPageVisitors_paidSearch_wow, adobe_byodEntryByodLandingPageVisitors_paidSearch_ly, adobe_byodEntryByodLandingPageVisitors_paidSearch_wow_pct, adobe_byodEntryByodLandingPageVisitors_paidSearch_yoy_pct) AS 'adobe_byodEntryByodLandingPageVisitors_paidSearch'  -- NEW,
            (adobe_byodEntryOffersSwitchVisitors_paidSearch, adobe_byodEntryOffersSwitchVisitors_paidSearch_wow, adobe_byodEntryOffersSwitchVisitors_paidSearch_ly, adobe_byodEntryOffersSwitchVisitors_paidSearch_wow_pct, adobe_byodEntryOffersSwitchVisitors_paidSearch_yoy_pct) AS 'adobe_byodEntryOffersSwitchVisitors_paidSearch'  -- NEW,
            (adobe_byodUvnbVisitors_organicSearch, adobe_byodUvnbVisitors_organicSearch_wow, adobe_byodUvnbVisitors_organicSearch_ly, adobe_byodUvnbVisitors_organicSearch_wow_pct, adobe_byodUvnbVisitors_organicSearch_yoy_pct) AS 'adobe_byodUvnbVisitors_organicSearch',
            (adobe_byodEntryByodPageVisitors_organicSearch, adobe_byodEntryByodPageVisitors_organicSearch_wow, adobe_byodEntryByodPageVisitors_organicSearch_ly, adobe_byodEntryByodPageVisitors_organicSearch_wow_pct, adobe_byodEntryByodPageVisitors_organicSearch_yoy_pct) AS 'adobe_byodEntryByodPageVisitors_organicSearch',
            (adobe_byodEntryHomePageVisitors_organicSearch, adobe_byodEntryHomePageVisitors_organicSearch_wow, adobe_byodEntryHomePageVisitors_organicSearch_ly, adobe_byodEntryHomePageVisitors_organicSearch_wow_pct, adobe_byodEntryHomePageVisitors_organicSearch_yoy_pct) AS 'adobe_byodEntryHomePageVisitors_organicSearch',
            (adobe_byodEntryDevicePageVisitors_organicSearch, adobe_byodEntryDevicePageVisitors_organicSearch_wow, adobe_byodEntryDevicePageVisitors_organicSearch_ly, adobe_byodEntryDevicePageVisitors_organicSearch_wow_pct, adobe_byodEntryDevicePageVisitors_organicSearch_yoy_pct) AS 'adobe_byodEntryDevicePageVisitors_organicSearch',
            (adobe_byodEntryPlansPageVisitors_organicSearch, adobe_byodEntryPlansPageVisitors_organicSearch_wow, adobe_byodEntryPlansPageVisitors_organicSearch_ly, adobe_byodEntryPlansPageVisitors_organicSearch_wow_pct, adobe_byodEntryPlansPageVisitors_organicSearch_yoy_pct) AS 'adobe_byodEntryPlansPageVisitors_organicSearch',
            (adobe_byodEntryOtherPageVisitors_organicSearch, adobe_byodEntryOtherPageVisitors_organicSearch_wow, adobe_byodEntryOtherPageVisitors_organicSearch_ly, adobe_byodEntryOtherPageVisitors_organicSearch_wow_pct, adobe_byodEntryOtherPageVisitors_organicSearch_yoy_pct) AS 'adobe_byodEntryOtherPageVisitors_organicSearch',
            (adobe_byodEntryStorePageVisitors_organicSearch, adobe_byodEntryStorePageVisitors_organicSearch_wow, adobe_byodEntryStorePageVisitors_organicSearch_ly, adobe_byodEntryStorePageVisitors_organicSearch_wow_pct, adobe_byodEntryStorePageVisitors_organicSearch_yoy_pct) AS 'adobe_byodEntryStorePageVisitors_organicSearch'  -- NEW,
            (adobe_byodEntryByodLandingPageVisitors_organicSearch, adobe_byodEntryByodLandingPageVisitors_organicSearch_wow, adobe_byodEntryByodLandingPageVisitors_organicSearch_ly, adobe_byodEntryByodLandingPageVisitors_organicSearch_wow_pct, adobe_byodEntryByodLandingPageVisitors_organicSearch_yoy_pct) AS 'adobe_byodEntryByodLandingPageVisitors_organicSearch'  -- NEW,
            (adobe_byodEntryOffersSwitchVisitors_organicSearch, adobe_byodEntryOffersSwitchVisitors_organicSearch_wow, adobe_byodEntryOffersSwitchVisitors_organicSearch_ly, adobe_byodEntryOffersSwitchVisitors_organicSearch_wow_pct, adobe_byodEntryOffersSwitchVisitors_organicSearch_yoy_pct) AS 'adobe_byodEntryOffersSwitchVisitors_organicSearch'  -- NEW,
            (adobe_byodUvnbVisitors_direct, adobe_byodUvnbVisitors_direct_wow, adobe_byodUvnbVisitors_direct_ly, adobe_byodUvnbVisitors_direct_wow_pct, adobe_byodUvnbVisitors_direct_yoy_pct) AS 'adobe_byodUvnbVisitors_direct',
            (adobe_byodEntryByodPageVisitors_direct, adobe_byodEntryByodPageVisitors_direct_wow, adobe_byodEntryByodPageVisitors_direct_ly, adobe_byodEntryByodPageVisitors_direct_wow_pct, adobe_byodEntryByodPageVisitors_direct_yoy_pct) AS 'adobe_byodEntryByodPageVisitors_direct',
            (adobe_byodEntryHomePageVisitors_direct, adobe_byodEntryHomePageVisitors_direct_wow, adobe_byodEntryHomePageVisitors_direct_ly, adobe_byodEntryHomePageVisitors_direct_wow_pct, adobe_byodEntryHomePageVisitors_direct_yoy_pct) AS 'adobe_byodEntryHomePageVisitors_direct',
            (adobe_byodEntryDevicePageVisitors_direct, adobe_byodEntryDevicePageVisitors_direct_wow, adobe_byodEntryDevicePageVisitors_direct_ly, adobe_byodEntryDevicePageVisitors_direct_wow_pct, adobe_byodEntryDevicePageVisitors_direct_yoy_pct) AS 'adobe_byodEntryDevicePageVisitors_direct',
            (adobe_byodEntryPlansPageVisitors_direct, adobe_byodEntryPlansPageVisitors_direct_wow, adobe_byodEntryPlansPageVisitors_direct_ly, adobe_byodEntryPlansPageVisitors_direct_wow_pct, adobe_byodEntryPlansPageVisitors_direct_yoy_pct) AS 'adobe_byodEntryPlansPageVisitors_direct',
            (adobe_byodEntryOtherPageVisitors_direct, adobe_byodEntryOtherPageVisitors_direct_wow, adobe_byodEntryOtherPageVisitors_direct_ly, adobe_byodEntryOtherPageVisitors_direct_wow_pct, adobe_byodEntryOtherPageVisitors_direct_yoy_pct) AS 'adobe_byodEntryOtherPageVisitors_direct',
            (adobe_byodEntryStorePageVisitors_direct, adobe_byodEntryStorePageVisitors_direct_wow, adobe_byodEntryStorePageVisitors_direct_ly, adobe_byodEntryStorePageVisitors_direct_wow_pct, adobe_byodEntryStorePageVisitors_direct_yoy_pct) AS 'adobe_byodEntryStorePageVisitors_direct'  -- NEW,
            (adobe_byodEntryByodLandingPageVisitors_direct, adobe_byodEntryByodLandingPageVisitors_direct_wow, adobe_byodEntryByodLandingPageVisitors_direct_ly, adobe_byodEntryByodLandingPageVisitors_direct_wow_pct, adobe_byodEntryByodLandingPageVisitors_direct_yoy_pct) AS 'adobe_byodEntryByodLandingPageVisitors_direct'  -- NEW,
            (adobe_byodEntryOffersSwitchVisitors_direct, adobe_byodEntryOffersSwitchVisitors_direct_wow, adobe_byodEntryOffersSwitchVisitors_direct_ly, adobe_byodEntryOffersSwitchVisitors_direct_wow_pct, adobe_byodEntryOffersSwitchVisitors_direct_yoy_pct) AS 'adobe_byodEntryOffersSwitchVisitors_direct'  -- NEW,
            (adobe_byodUvnbVisitors_social, adobe_byodUvnbVisitors_social_wow, adobe_byodUvnbVisitors_social_ly, adobe_byodUvnbVisitors_social_wow_pct, adobe_byodUvnbVisitors_social_yoy_pct) AS 'adobe_byodUvnbVisitors_social',
            (adobe_byodEntryByodPageVisitors_social, adobe_byodEntryByodPageVisitors_social_wow, adobe_byodEntryByodPageVisitors_social_ly, adobe_byodEntryByodPageVisitors_social_wow_pct, adobe_byodEntryByodPageVisitors_social_yoy_pct) AS 'adobe_byodEntryByodPageVisitors_social',
            (adobe_byodEntryHomePageVisitors_social, adobe_byodEntryHomePageVisitors_social_wow, adobe_byodEntryHomePageVisitors_social_ly, adobe_byodEntryHomePageVisitors_social_wow_pct, adobe_byodEntryHomePageVisitors_social_yoy_pct) AS 'adobe_byodEntryHomePageVisitors_social',
            (adobe_byodEntryDevicePageVisitors_social, adobe_byodEntryDevicePageVisitors_social_wow, adobe_byodEntryDevicePageVisitors_social_ly, adobe_byodEntryDevicePageVisitors_social_wow_pct, adobe_byodEntryDevicePageVisitors_social_yoy_pct) AS 'adobe_byodEntryDevicePageVisitors_social',
            (adobe_byodEntryPlansPageVisitors_social, adobe_byodEntryPlansPageVisitors_social_wow, adobe_byodEntryPlansPageVisitors_social_ly, adobe_byodEntryPlansPageVisitors_social_wow_pct, adobe_byodEntryPlansPageVisitors_social_yoy_pct) AS 'adobe_byodEntryPlansPageVisitors_social',
            (adobe_byodEntryOtherPageVisitors_social, adobe_byodEntryOtherPageVisitors_social_wow, adobe_byodEntryOtherPageVisitors_social_ly, adobe_byodEntryOtherPageVisitors_social_wow_pct, adobe_byodEntryOtherPageVisitors_social_yoy_pct) AS 'adobe_byodEntryOtherPageVisitors_social',
            (adobe_byodEntryStorePageVisitors_social, adobe_byodEntryStorePageVisitors_social_wow, adobe_byodEntryStorePageVisitors_social_ly, adobe_byodEntryStorePageVisitors_social_wow_pct, adobe_byodEntryStorePageVisitors_social_yoy_pct) AS 'adobe_byodEntryStorePageVisitors_social'  -- NEW,
            (adobe_byodEntryByodLandingPageVisitors_social, adobe_byodEntryByodLandingPageVisitors_social_wow, adobe_byodEntryByodLandingPageVisitors_social_ly, adobe_byodEntryByodLandingPageVisitors_social_wow_pct, adobe_byodEntryByodLandingPageVisitors_social_yoy_pct) AS 'adobe_byodEntryByodLandingPageVisitors_social'  -- NEW,
            (adobe_byodEntryOffersSwitchVisitors_social, adobe_byodEntryOffersSwitchVisitors_social_wow, adobe_byodEntryOffersSwitchVisitors_social_ly, adobe_byodEntryOffersSwitchVisitors_social_wow_pct, adobe_byodEntryOffersSwitchVisitors_social_yoy_pct) AS 'adobe_byodEntryOffersSwitchVisitors_social'  -- NEW,
            (adobe_byodUvnbVisitors_programmatic, adobe_byodUvnbVisitors_programmatic_wow, adobe_byodUvnbVisitors_programmatic_ly, adobe_byodUvnbVisitors_programmatic_wow_pct, adobe_byodUvnbVisitors_programmatic_yoy_pct) AS 'adobe_byodUvnbVisitors_programmatic',
            (adobe_byodEntryByodPageVisitors_programmatic, adobe_byodEntryByodPageVisitors_programmatic_wow, adobe_byodEntryByodPageVisitors_programmatic_ly, adobe_byodEntryByodPageVisitors_programmatic_wow_pct, adobe_byodEntryByodPageVisitors_programmatic_yoy_pct) AS 'adobe_byodEntryByodPageVisitors_programmatic',
            (adobe_byodEntryHomePageVisitors_programmatic, adobe_byodEntryHomePageVisitors_programmatic_wow, adobe_byodEntryHomePageVisitors_programmatic_ly, adobe_byodEntryHomePageVisitors_programmatic_wow_pct, adobe_byodEntryHomePageVisitors_programmatic_yoy_pct) AS 'adobe_byodEntryHomePageVisitors_programmatic',
            (adobe_byodEntryDevicePageVisitors_programmatic, adobe_byodEntryDevicePageVisitors_programmatic_wow, adobe_byodEntryDevicePageVisitors_programmatic_ly, adobe_byodEntryDevicePageVisitors_programmatic_wow_pct, adobe_byodEntryDevicePageVisitors_programmatic_yoy_pct) AS 'adobe_byodEntryDevicePageVisitors_programmatic',
            (adobe_byodEntryPlansPageVisitors_programmatic, adobe_byodEntryPlansPageVisitors_programmatic_wow, adobe_byodEntryPlansPageVisitors_programmatic_ly, adobe_byodEntryPlansPageVisitors_programmatic_wow_pct, adobe_byodEntryPlansPageVisitors_programmatic_yoy_pct) AS 'adobe_byodEntryPlansPageVisitors_programmatic',
            (adobe_byodEntryOtherPageVisitors_programmatic, adobe_byodEntryOtherPageVisitors_programmatic_wow, adobe_byodEntryOtherPageVisitors_programmatic_ly, adobe_byodEntryOtherPageVisitors_programmatic_wow_pct, adobe_byodEntryOtherPageVisitors_programmatic_yoy_pct) AS 'adobe_byodEntryOtherPageVisitors_programmatic',
            (adobe_byodEntryStorePageVisitors_programmatic, adobe_byodEntryStorePageVisitors_programmatic_wow, adobe_byodEntryStorePageVisitors_programmatic_ly, adobe_byodEntryStorePageVisitors_programmatic_wow_pct, adobe_byodEntryStorePageVisitors_programmatic_yoy_pct) AS 'adobe_byodEntryStorePageVisitors_programmatic'  -- NEW,
            (adobe_byodEntryByodLandingPageVisitors_programmatic, adobe_byodEntryByodLandingPageVisitors_programmatic_wow, adobe_byodEntryByodLandingPageVisitors_programmatic_ly, adobe_byodEntryByodLandingPageVisitors_programmatic_wow_pct, adobe_byodEntryByodLandingPageVisitors_programmatic_yoy_pct) AS 'adobe_byodEntryByodLandingPageVisitors_programmatic'  -- NEW,
            (adobe_byodEntryOffersSwitchVisitors_programmatic, adobe_byodEntryOffersSwitchVisitors_programmatic_wow, adobe_byodEntryOffersSwitchVisitors_programmatic_ly, adobe_byodEntryOffersSwitchVisitors_programmatic_wow_pct, adobe_byodEntryOffersSwitchVisitors_programmatic_yoy_pct) AS 'adobe_byodEntryOffersSwitchVisitors_programmatic'  -- NEW,
            (adobe_byodUvnbVisitors_other, adobe_byodUvnbVisitors_other_wow, adobe_byodUvnbVisitors_other_ly, adobe_byodUvnbVisitors_other_wow_pct, adobe_byodUvnbVisitors_other_yoy_pct) AS 'adobe_byodUvnbVisitors_other',
            (adobe_byodEntryByodPageVisitors_other, adobe_byodEntryByodPageVisitors_other_wow, adobe_byodEntryByodPageVisitors_other_ly, adobe_byodEntryByodPageVisitors_other_wow_pct, adobe_byodEntryByodPageVisitors_other_yoy_pct) AS 'adobe_byodEntryByodPageVisitors_other',
            (adobe_byodEntryHomePageVisitors_other, adobe_byodEntryHomePageVisitors_other_wow, adobe_byodEntryHomePageVisitors_other_ly, adobe_byodEntryHomePageVisitors_other_wow_pct, adobe_byodEntryHomePageVisitors_other_yoy_pct) AS 'adobe_byodEntryHomePageVisitors_other',
            (adobe_byodEntryDevicePageVisitors_other, adobe_byodEntryDevicePageVisitors_other_wow, adobe_byodEntryDevicePageVisitors_other_ly, adobe_byodEntryDevicePageVisitors_other_wow_pct, adobe_byodEntryDevicePageVisitors_other_yoy_pct) AS 'adobe_byodEntryDevicePageVisitors_other',
            (adobe_byodEntryPlansPageVisitors_other, adobe_byodEntryPlansPageVisitors_other_wow, adobe_byodEntryPlansPageVisitors_other_ly, adobe_byodEntryPlansPageVisitors_other_wow_pct, adobe_byodEntryPlansPageVisitors_other_yoy_pct) AS 'adobe_byodEntryPlansPageVisitors_other',
            (adobe_byodEntryOtherPageVisitors_other, adobe_byodEntryOtherPageVisitors_other_wow, adobe_byodEntryOtherPageVisitors_other_ly, adobe_byodEntryOtherPageVisitors_other_wow_pct, adobe_byodEntryOtherPageVisitors_other_yoy_pct) AS 'adobe_byodEntryOtherPageVisitors_other',
            (adobe_byodEntryStorePageVisitors_other, adobe_byodEntryStorePageVisitors_other_wow, adobe_byodEntryStorePageVisitors_other_ly, adobe_byodEntryStorePageVisitors_other_wow_pct, adobe_byodEntryStorePageVisitors_other_yoy_pct) AS 'adobe_byodEntryStorePageVisitors_other'  -- NEW,
            (adobe_byodEntryByodLandingPageVisitors_other, adobe_byodEntryByodLandingPageVisitors_other_wow, adobe_byodEntryByodLandingPageVisitors_other_ly, adobe_byodEntryByodLandingPageVisitors_other_wow_pct, adobe_byodEntryByodLandingPageVisitors_other_yoy_pct) AS 'adobe_byodEntryByodLandingPageVisitors_other'  -- NEW,
            (adobe_byodEntryOffersSwitchVisitors_other, adobe_byodEntryOffersSwitchVisitors_other_wow, adobe_byodEntryOffersSwitchVisitors_other_ly, adobe_byodEntryOffersSwitchVisitors_other_wow_pct, adobe_byodEntryOffersSwitchVisitors_other_yoy_pct) AS 'adobe_byodEntryOffersSwitchVisitors_other'  -- NEW
        )
    )
),

-- -----------------------------------------------------------------------
-- ADOBE BYOD OUTCOMES (Silver 08): 6 metrics × 7 channels — 42 rows/week
-- -----------------------------------------------------------------------
outcomes_long AS (
    SELECT week_sun_to_sat, data_source, max_data_date, metric_name, metric_value, metric_value_wow, metric_value_ly, wow_pct, yoy_pct
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_silver_adobeByodOutcomes_weekly`
    UNPIVOT (
        (metric_value, metric_value_wow, metric_value_ly, wow_pct, yoy_pct)
        FOR metric_name IN (
            (adobe_byodVrChatVisitors_allChannels,       adobe_byodVrChatVisitors_allChannels_wow,       adobe_byodVrChatVisitors_allChannels_ly,       adobe_byodVrChatVisitors_allChannels_wow_pct,       adobe_byodVrChatVisitors_allChannels_yoy_pct)       AS 'adobe_byodVrChatVisitors_allChannels',
            (adobe_byodCallVisitors_allChannels,          adobe_byodCallVisitors_allChannels_wow,          adobe_byodCallVisitors_allChannels_ly,          adobe_byodCallVisitors_allChannels_wow_pct,          adobe_byodCallVisitors_allChannels_yoy_pct)          AS 'adobe_byodCallVisitors_allChannels',
            (adobe_byodStoreLocatorVisitors_allChannels,  adobe_byodStoreLocatorVisitors_allChannels_wow,  adobe_byodStoreLocatorVisitors_allChannels_ly,  adobe_byodStoreLocatorVisitors_allChannels_wow_pct,  adobe_byodStoreLocatorVisitors_allChannels_yoy_pct)  AS 'adobe_byodStoreLocatorVisitors_allChannels',
            (adobe_byodInternalTmoVisitors_allChannels,   adobe_byodInternalTmoVisitors_allChannels_wow,   adobe_byodInternalTmoVisitors_allChannels_ly,   adobe_byodInternalTmoVisitors_allChannels_wow_pct,   adobe_byodInternalTmoVisitors_allChannels_yoy_pct)   AS 'adobe_byodInternalTmoVisitors_allChannels',
            (adobe_byodBouncersVisitors_allChannels,      adobe_byodBouncersVisitors_allChannels_wow,      adobe_byodBouncersVisitors_allChannels_ly,      adobe_byodBouncersVisitors_allChannels_wow_pct,      adobe_byodBouncersVisitors_allChannels_yoy_pct)      AS 'adobe_byodBouncersVisitors_allChannels',
            (adobe_byodOrders_allChannels,                adobe_byodOrders_allChannels_wow,                adobe_byodOrders_allChannels_ly,                adobe_byodOrders_allChannels_wow_pct,                adobe_byodOrders_allChannels_yoy_pct)                AS 'adobe_byodOrders_allChannels',
            (adobe_byodVrChatVisitors_paidSearch,         adobe_byodVrChatVisitors_paidSearch_wow,         adobe_byodVrChatVisitors_paidSearch_ly,         adobe_byodVrChatVisitors_paidSearch_wow_pct,         adobe_byodVrChatVisitors_paidSearch_yoy_pct)         AS 'adobe_byodVrChatVisitors_paidSearch',
            (adobe_byodCallVisitors_paidSearch,           adobe_byodCallVisitors_paidSearch_wow,           adobe_byodCallVisitors_paidSearch_ly,           adobe_byodCallVisitors_paidSearch_wow_pct,           adobe_byodCallVisitors_paidSearch_yoy_pct)           AS 'adobe_byodCallVisitors_paidSearch',
            (adobe_byodStoreLocatorVisitors_paidSearch,   adobe_byodStoreLocatorVisitors_paidSearch_wow,   adobe_byodStoreLocatorVisitors_paidSearch_ly,   adobe_byodStoreLocatorVisitors_paidSearch_wow_pct,   adobe_byodStoreLocatorVisitors_paidSearch_yoy_pct)   AS 'adobe_byodStoreLocatorVisitors_paidSearch',
            (adobe_byodInternalTmoVisitors_paidSearch,    adobe_byodInternalTmoVisitors_paidSearch_wow,    adobe_byodInternalTmoVisitors_paidSearch_ly,    adobe_byodInternalTmoVisitors_paidSearch_wow_pct,    adobe_byodInternalTmoVisitors_paidSearch_yoy_pct)    AS 'adobe_byodInternalTmoVisitors_paidSearch',
            (adobe_byodBouncersVisitors_paidSearch,       adobe_byodBouncersVisitors_paidSearch_wow,       adobe_byodBouncersVisitors_paidSearch_ly,       adobe_byodBouncersVisitors_paidSearch_wow_pct,       adobe_byodBouncersVisitors_paidSearch_yoy_pct)       AS 'adobe_byodBouncersVisitors_paidSearch',
            (adobe_byodOrders_paidSearch,                 adobe_byodOrders_paidSearch_wow,                 adobe_byodOrders_paidSearch_ly,                 adobe_byodOrders_paidSearch_wow_pct,                 adobe_byodOrders_paidSearch_yoy_pct)                 AS 'adobe_byodOrders_paidSearch',
            (adobe_byodVrChatVisitors_organicSearch,      adobe_byodVrChatVisitors_organicSearch_wow,      adobe_byodVrChatVisitors_organicSearch_ly,      adobe_byodVrChatVisitors_organicSearch_wow_pct,      adobe_byodVrChatVisitors_organicSearch_yoy_pct)      AS 'adobe_byodVrChatVisitors_organicSearch',
            (adobe_byodCallVisitors_organicSearch,        adobe_byodCallVisitors_organicSearch_wow,        adobe_byodCallVisitors_organicSearch_ly,        adobe_byodCallVisitors_organicSearch_wow_pct,        adobe_byodCallVisitors_organicSearch_yoy_pct)        AS 'adobe_byodCallVisitors_organicSearch',
            (adobe_byodStoreLocatorVisitors_organicSearch,adobe_byodStoreLocatorVisitors_organicSearch_wow,adobe_byodStoreLocatorVisitors_organicSearch_ly,adobe_byodStoreLocatorVisitors_organicSearch_wow_pct,adobe_byodStoreLocatorVisitors_organicSearch_yoy_pct) AS 'adobe_byodStoreLocatorVisitors_organicSearch',
            (adobe_byodInternalTmoVisitors_organicSearch, adobe_byodInternalTmoVisitors_organicSearch_wow, adobe_byodInternalTmoVisitors_organicSearch_ly, adobe_byodInternalTmoVisitors_organicSearch_wow_pct, adobe_byodInternalTmoVisitors_organicSearch_yoy_pct) AS 'adobe_byodInternalTmoVisitors_organicSearch',
            (adobe_byodBouncersVisitors_organicSearch,    adobe_byodBouncersVisitors_organicSearch_wow,    adobe_byodBouncersVisitors_organicSearch_ly,    adobe_byodBouncersVisitors_organicSearch_wow_pct,    adobe_byodBouncersVisitors_organicSearch_yoy_pct)    AS 'adobe_byodBouncersVisitors_organicSearch',
            (adobe_byodOrders_organicSearch,              adobe_byodOrders_organicSearch_wow,              adobe_byodOrders_organicSearch_ly,              adobe_byodOrders_organicSearch_wow_pct,              adobe_byodOrders_organicSearch_yoy_pct)              AS 'adobe_byodOrders_organicSearch',
            (adobe_byodVrChatVisitors_direct,             adobe_byodVrChatVisitors_direct_wow,             adobe_byodVrChatVisitors_direct_ly,             adobe_byodVrChatVisitors_direct_wow_pct,             adobe_byodVrChatVisitors_direct_yoy_pct)             AS 'adobe_byodVrChatVisitors_direct',
            (adobe_byodCallVisitors_direct,               adobe_byodCallVisitors_direct_wow,               adobe_byodCallVisitors_direct_ly,               adobe_byodCallVisitors_direct_wow_pct,               adobe_byodCallVisitors_direct_yoy_pct)               AS 'adobe_byodCallVisitors_direct',
            (adobe_byodStoreLocatorVisitors_direct,       adobe_byodStoreLocatorVisitors_direct_wow,       adobe_byodStoreLocatorVisitors_direct_ly,       adobe_byodStoreLocatorVisitors_direct_wow_pct,       adobe_byodStoreLocatorVisitors_direct_yoy_pct)       AS 'adobe_byodStoreLocatorVisitors_direct',
            (adobe_byodInternalTmoVisitors_direct,        adobe_byodInternalTmoVisitors_direct_wow,        adobe_byodInternalTmoVisitors_direct_ly,        adobe_byodInternalTmoVisitors_direct_wow_pct,        adobe_byodInternalTmoVisitors_direct_yoy_pct)        AS 'adobe_byodInternalTmoVisitors_direct',
            (adobe_byodBouncersVisitors_direct,           adobe_byodBouncersVisitors_direct_wow,           adobe_byodBouncersVisitors_direct_ly,           adobe_byodBouncersVisitors_direct_wow_pct,           adobe_byodBouncersVisitors_direct_yoy_pct)           AS 'adobe_byodBouncersVisitors_direct',
            (adobe_byodOrders_direct,                     adobe_byodOrders_direct_wow,                     adobe_byodOrders_direct_ly,                     adobe_byodOrders_direct_wow_pct,                     adobe_byodOrders_direct_yoy_pct)                     AS 'adobe_byodOrders_direct',
            (adobe_byodVrChatVisitors_social,             adobe_byodVrChatVisitors_social_wow,             adobe_byodVrChatVisitors_social_ly,             adobe_byodVrChatVisitors_social_wow_pct,             adobe_byodVrChatVisitors_social_yoy_pct)             AS 'adobe_byodVrChatVisitors_social',
            (adobe_byodCallVisitors_social,               adobe_byodCallVisitors_social_wow,               adobe_byodCallVisitors_social_ly,               adobe_byodCallVisitors_social_wow_pct,               adobe_byodCallVisitors_social_yoy_pct)               AS 'adobe_byodCallVisitors_social',
            (adobe_byodStoreLocatorVisitors_social,       adobe_byodStoreLocatorVisitors_social_wow,       adobe_byodStoreLocatorVisitors_social_ly,       adobe_byodStoreLocatorVisitors_social_wow_pct,       adobe_byodStoreLocatorVisitors_social_yoy_pct)       AS 'adobe_byodStoreLocatorVisitors_social',
            (adobe_byodInternalTmoVisitors_social,        adobe_byodInternalTmoVisitors_social_wow,        adobe_byodInternalTmoVisitors_social_ly,        adobe_byodInternalTmoVisitors_social_wow_pct,        adobe_byodInternalTmoVisitors_social_yoy_pct)        AS 'adobe_byodInternalTmoVisitors_social',
            (adobe_byodBouncersVisitors_social,           adobe_byodBouncersVisitors_social_wow,           adobe_byodBouncersVisitors_social_ly,           adobe_byodBouncersVisitors_social_wow_pct,           adobe_byodBouncersVisitors_social_yoy_pct)           AS 'adobe_byodBouncersVisitors_social',
            (adobe_byodOrders_social,                     adobe_byodOrders_social_wow,                     adobe_byodOrders_social_ly,                     adobe_byodOrders_social_wow_pct,                     adobe_byodOrders_social_yoy_pct)                     AS 'adobe_byodOrders_social',
            (adobe_byodVrChatVisitors_programmatic,       adobe_byodVrChatVisitors_programmatic_wow,       adobe_byodVrChatVisitors_programmatic_ly,       adobe_byodVrChatVisitors_programmatic_wow_pct,       adobe_byodVrChatVisitors_programmatic_yoy_pct)       AS 'adobe_byodVrChatVisitors_programmatic',
            (adobe_byodCallVisitors_programmatic,         adobe_byodCallVisitors_programmatic_wow,         adobe_byodCallVisitors_programmatic_ly,         adobe_byodCallVisitors_programmatic_wow_pct,         adobe_byodCallVisitors_programmatic_yoy_pct)         AS 'adobe_byodCallVisitors_programmatic',
            (adobe_byodStoreLocatorVisitors_programmatic, adobe_byodStoreLocatorVisitors_programmatic_wow, adobe_byodStoreLocatorVisitors_programmatic_ly, adobe_byodStoreLocatorVisitors_programmatic_wow_pct, adobe_byodStoreLocatorVisitors_programmatic_yoy_pct) AS 'adobe_byodStoreLocatorVisitors_programmatic',
            (adobe_byodInternalTmoVisitors_programmatic,  adobe_byodInternalTmoVisitors_programmatic_wow,  adobe_byodInternalTmoVisitors_programmatic_ly,  adobe_byodInternalTmoVisitors_programmatic_wow_pct,  adobe_byodInternalTmoVisitors_programmatic_yoy_pct)  AS 'adobe_byodInternalTmoVisitors_programmatic',
            (adobe_byodBouncersVisitors_programmatic,     adobe_byodBouncersVisitors_programmatic_wow,     adobe_byodBouncersVisitors_programmatic_ly,     adobe_byodBouncersVisitors_programmatic_wow_pct,     adobe_byodBouncersVisitors_programmatic_yoy_pct)     AS 'adobe_byodBouncersVisitors_programmatic',
            (adobe_byodOrders_programmatic,               adobe_byodOrders_programmatic_wow,               adobe_byodOrders_programmatic_ly,               adobe_byodOrders_programmatic_wow_pct,               adobe_byodOrders_programmatic_yoy_pct)               AS 'adobe_byodOrders_programmatic',
            (adobe_byodVrChatVisitors_other,              adobe_byodVrChatVisitors_other_wow,              adobe_byodVrChatVisitors_other_ly,              adobe_byodVrChatVisitors_other_wow_pct,              adobe_byodVrChatVisitors_other_yoy_pct)              AS 'adobe_byodVrChatVisitors_other',
            (adobe_byodCallVisitors_other,                adobe_byodCallVisitors_other_wow,                adobe_byodCallVisitors_other_ly,                adobe_byodCallVisitors_other_wow_pct,                adobe_byodCallVisitors_other_yoy_pct)                AS 'adobe_byodCallVisitors_other',
            (adobe_byodStoreLocatorVisitors_other,        adobe_byodStoreLocatorVisitors_other_wow,        adobe_byodStoreLocatorVisitors_other_ly,        adobe_byodStoreLocatorVisitors_other_wow_pct,        adobe_byodStoreLocatorVisitors_other_yoy_pct)        AS 'adobe_byodStoreLocatorVisitors_other',
            (adobe_byodInternalTmoVisitors_other,         adobe_byodInternalTmoVisitors_other_wow,         adobe_byodInternalTmoVisitors_other_ly,         adobe_byodInternalTmoVisitors_other_wow_pct,         adobe_byodInternalTmoVisitors_other_yoy_pct)         AS 'adobe_byodInternalTmoVisitors_other',
            (adobe_byodBouncersVisitors_other,            adobe_byodBouncersVisitors_other_wow,            adobe_byodBouncersVisitors_other_ly,            adobe_byodBouncersVisitors_other_wow_pct,            adobe_byodBouncersVisitors_other_yoy_pct)            AS 'adobe_byodBouncersVisitors_other',
            (adobe_byodOrders_other,                      adobe_byodOrders_other_wow,                      adobe_byodOrders_other_ly,                      adobe_byodOrders_other_wow_pct,                      adobe_byodOrders_other_yoy_pct)                      AS 'adobe_byodOrders_other'
        )
    )
),

-- -----------------------------------------------------------------------
-- COMBINE all sources
-- -----------------------------------------------------------------------
combined AS (
    -- Profound
    SELECT week_sun_to_sat, data_source, channel, max_data_date, CAST(NULL AS STRING) AS dimension_name, CAST(NULL AS STRING) AS dimension_value, metric_name, metric_value, metric_value_wow, metric_value_ly, wow_pct, yoy_pct FROM profound_long
    UNION ALL
    -- GoFish
    SELECT week_sun_to_sat, data_source, channel, max_data_date, CAST(NULL AS STRING), CAST(NULL AS STRING), metric_name, metric_value, metric_value_wow, metric_value_ly, wow_pct, yoy_pct FROM gofish_long
    UNION ALL
    -- SA360
    SELECT week_sun_to_sat, data_source, channel, max_data_date, CAST(NULL AS STRING), CAST(NULL AS STRING), metric_name, metric_value, metric_value_wow, metric_value_ly, wow_pct, yoy_pct FROM sa360_long
    UNION ALL
    -- GSC
    SELECT week_sun_to_sat, data_source, channel, max_data_date, CAST(NULL AS STRING), CAST(NULL AS STRING), metric_name, metric_value, metric_value_wow, metric_value_ly, wow_pct, yoy_pct FROM gsc_long
    UNION ALL
    -- Trends index
    SELECT week_sun_to_sat, data_source, channel, max_data_date, CAST(NULL AS STRING), CAST(NULL AS STRING), metric_name, metric_value, metric_value_wow, metric_value_ly, wow_pct, yoy_pct FROM trends_index_long
    UNION ALL
    -- Trends keywords
    SELECT week_sun_to_sat, data_source, channel, max_data_date, dimension_name, dimension_value, metric_name, metric_value, metric_value_wow, metric_value_ly, wow_pct, yoy_pct FROM trends_keywords_long
    UNION ALL
    -- Adobe Silver 06 — conversion metrics (inc. cvrPostpaid, cvrHsi)
    SELECT
        week_sun_to_sat, data_source,
        CASE
            WHEN metric_name LIKE '%_allChannels'   THEN 'ALL CHANNELS'
            WHEN metric_name LIKE '%_paidSearch'    THEN 'PAID SEARCH'
            WHEN metric_name LIKE '%_organicSearch' THEN 'ORGANIC SEARCH'
            WHEN metric_name LIKE '%_direct'        THEN 'DIRECT'
            WHEN metric_name LIKE '%_social'        THEN 'SOCIAL'
            WHEN metric_name LIKE '%_programmatic'  THEN 'PROGRAMMATIC'
            WHEN metric_name LIKE '%_other'         THEN 'OTHER'
        END AS channel,
        max_data_date, CAST(NULL AS STRING), CAST(NULL AS STRING),
        metric_name, metric_value, metric_value_wow, metric_value_ly, wow_pct, yoy_pct
    FROM adobe_long
    UNION ALL
    -- Adobe Silver 07 — BYOD entry page metrics (inc. 3 new metrics from Other segregation)
    SELECT
        week_sun_to_sat, data_source,
        CASE
            WHEN metric_name LIKE '%_allChannels'   THEN 'ALL CHANNELS'
            WHEN metric_name LIKE '%_paidSearch'    THEN 'PAID SEARCH'
            WHEN metric_name LIKE '%_organicSearch' THEN 'ORGANIC SEARCH'
            WHEN metric_name LIKE '%_direct'        THEN 'DIRECT'
            WHEN metric_name LIKE '%_social'        THEN 'SOCIAL'
            WHEN metric_name LIKE '%_programmatic'  THEN 'PROGRAMMATIC'
            WHEN metric_name LIKE '%_other'         THEN 'OTHER'
        END AS channel,
        max_data_date, CAST(NULL AS STRING), CAST(NULL AS STRING),
        metric_name, metric_value, metric_value_wow, metric_value_ly, wow_pct, yoy_pct
    FROM entryPages_long
    UNION ALL
    -- Adobe Silver 08 — BYOD outcome metrics
    SELECT
        week_sun_to_sat, data_source,
        CASE
            WHEN metric_name LIKE '%_allChannels'   THEN 'ALL CHANNELS'
            WHEN metric_name LIKE '%_paidSearch'    THEN 'PAID SEARCH'
            WHEN metric_name LIKE '%_organicSearch' THEN 'ORGANIC SEARCH'
            WHEN metric_name LIKE '%_direct'        THEN 'DIRECT'
            WHEN metric_name LIKE '%_social'        THEN 'SOCIAL'
            WHEN metric_name LIKE '%_programmatic'  THEN 'PROGRAMMATIC'
            WHEN metric_name LIKE '%_other'         THEN 'OTHER'
        END AS channel,
        max_data_date, CAST(NULL AS STRING), CAST(NULL AS STRING),
        metric_name, metric_value, metric_value_wow, metric_value_ly, wow_pct, yoy_pct
    FROM outcomes_long
)

SELECT
    week_sun_to_sat,
    'WEEKLY'                                                           AS time_granularity,
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
    MAX(max_data_date) OVER (PARTITION BY data_source)                 AS max_data_date
FROM combined
ORDER BY
    week_sun_to_sat  ASC,
    data_source      ASC,
    channel          ASC,
    metric_name      ASC,
    dimension_name   ASC
;