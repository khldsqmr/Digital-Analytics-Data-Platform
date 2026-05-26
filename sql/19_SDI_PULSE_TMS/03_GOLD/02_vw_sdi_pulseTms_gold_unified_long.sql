/* =================================================================================================
FILE:         02_vw_sdi_pulseTms_gold_unified_long.sql
LAYER:        Gold View — Long
DATASET:      prj-dbi-prd-1.ds_dbi_digitalmedia_automation
VIEW NAME:    vw_sdi_pulseTms_gold_unified_long

SOURCES:
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseTms_silver_profound_weekly
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseTms_silver_profoundGofish_weekly
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseTms_silver_sa360_weekly
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseTms_silver_gsc_weekly
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseTms_silver_googleTrends_weekly
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseTms_silver_adobe_weekly
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseTms_silver_mfc_weekly

DESTINATION:
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseTms_gold_unified_long

PURPOSE:
  Gold Long view for the pulseTms dashboard.
  Reads directly from Silver views — NOT from Gold Wide.
  Each Silver view is unpivoted independently and combined via UNION ALL.
  One row per metric per week for all sources including MFC.
  All product lines: BYOD, Postpaid, HSI, Totals.

  MFC rows have additional dimension columns populated:
    channel          = MFC channel (PAID SEARCH, OLV, OTT, DISPLAY, etc.)
    dimension_name   = 'TACTIC',  dimension_value   = tactic value
    dimension_name_2 = 'AGENCY',  dimension_value_2 = agency value
    lob_supported    = 'BROADBAND' or 'CONSUMER POSTPAID'
  All other sources have dimension_name_2, dimension_value_2,
  lob_supported, act_vs_fcst_pct, act_vs_fcst_delta, is_mfc_pre_apportioned = NULL

  MFC BOUNDARY_WEEK rows (already apportioned at source):
    is_mfc_pre_apportioned = TRUE
    → reporting view must NOT multiply by partial_weight for these rows
    BOUNDARY_WEEK where week_sun_to_sat = quarter_end_date:
    → wow_pct = NULL (partial tail days, no WoW)
    BOUNDARY_WEEK where week_sun_to_sat = actual Saturday:
    → wow_pct populated (full week comparison)

  Trends keywords:
    dimension_name = 'KEYWORD_RANK_1..5', dimension_value = keyword text
    wow_pct = NULL (keywords change week to week)

OUTPUT SCHEMA:
  week_sun_to_sat       : DATE
  time_granularity      : STRING   — Always 'WEEKLY'
  data_source           : STRING   — PROFOUND, GOFISH, SA360, GSC, TRENDS, ADOBE, MFC
  channel               : STRING
  dimension_name        : STRING   — 'KEYWORD_RANK_1..5' for Trends keywords
                                     'TACTIC' for MFC; NULL otherwise
  dimension_value       : STRING   — keyword text for Trends; tactic value for MFC
  dimension_name_2      : STRING   — 'AGENCY' for MFC; NULL for all other sources
  dimension_value_2     : STRING   — agency value for MFC; NULL for all other sources
  lob_supported         : STRING   — 'BROADBAND'/'CONSUMER POSTPAID' for MFC; NULL otherwise
  metric_name           : STRING   — prefixed metric name
  metric_value          : FLOAT64
  metric_value_wow      : FLOAT64
  metric_value_ly       : FLOAT64
  wow_pct               : FLOAT64
  yoy_pct               : FLOAT64
  act_vs_fcst_pct       : FLOAT64  — MFC only; NULL for all other sources
  act_vs_fcst_delta     : FLOAT64  — MFC only; NULL for all other sources
  is_mfc_pre_apportioned: BOOL     — TRUE for MFC BOUNDARY_WEEK rows; FALSE/NULL otherwise
  max_data_date         : DATE

DOWNSTREAM:
  10_vw_sdi_pulseTms_dim_date
  11_vw_sdi_pulseTms_reporting
================================================================================================= */

CREATE OR REPLACE VIEW `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseTms_gold_unified_long`
AS

WITH

-- -----------------------------------------------------------------------
-- PROFOUND: NON-BRAND AI Visibility
-- 12 metric rows per week (4 metrics × 3 assets)
-- data_source = 'PROFOUND', channel = 'AI SEARCH' from Silver
-- -----------------------------------------------------------------------
profound_long AS (
    SELECT
        week_sun_to_sat, data_source, channel, max_data_date,
        metric_name, metric_value, metric_value_wow, metric_value_ly, wow_pct, yoy_pct
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseTms_silver_profound_weekly`
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
-- data_source = 'GOFISH', channel = 'AI SEARCH' from Silver
-- -----------------------------------------------------------------------
gofish_long AS (
    SELECT
        week_sun_to_sat, data_source, channel, max_data_date,
        metric_name, metric_value, metric_value_wow, metric_value_ly, wow_pct, yoy_pct
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseTms_silver_profoundGofish_weekly`
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
-- data_source = 'SA360', channel = 'PAID SEARCH' from Silver
-- -----------------------------------------------------------------------
sa360_long AS (
    SELECT
        week_sun_to_sat, data_source, channel, max_data_date,
        metric_name, metric_value, metric_value_wow, metric_value_ly, wow_pct, yoy_pct
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseTms_silver_sa360_weekly`
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
-- GSC: Organic Search Performance
-- 4 metric rows per week (2 metrics × 2 brand types)
-- data_source = 'GSC', channel = 'ORGANIC SEARCH' from Silver
-- -----------------------------------------------------------------------
gsc_long AS (
    SELECT
        week_sun_to_sat, data_source, channel, max_data_date,
        metric_name, metric_value, metric_value_wow, metric_value_ly, wow_pct, yoy_pct
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseTms_silver_gsc_weekly`
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
-- data_source = 'TRENDS', channel = 'ORGANIC SEARCH' from Silver
-- -----------------------------------------------------------------------
trends_index_long AS (
    SELECT
        week_sun_to_sat, data_source, channel, max_data_date,
        metric_name, metric_value, metric_value_wow, metric_value_ly, wow_pct, yoy_pct
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseTms_silver_googleTrends_weekly`
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
-- Max 5 rows per week — trends_kw_interest only
-- keywords change week to week so wow_pct = NULL
-- dimension_name = KEYWORD_RANK_1..5, dimension_value = keyword text
-- -----------------------------------------------------------------------
trends_keywords_long AS (

    SELECT week_sun_to_sat, data_source, channel, max_data_date,
        'KEYWORD_RANK_1' AS dimension_name, trends_top_kw_1 AS dimension_value,
        'trends_kw_interest' AS metric_name, trends_kw1_interest AS metric_value,
        CAST(NULL AS FLOAT64) AS metric_value_wow, CAST(NULL AS FLOAT64) AS metric_value_ly,
        CAST(NULL AS FLOAT64) AS wow_pct,           CAST(NULL AS FLOAT64) AS yoy_pct
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseTms_silver_googleTrends_weekly`
    WHERE NULLIF(TRIM(trends_top_kw_1), '') IS NOT NULL

    UNION ALL SELECT week_sun_to_sat, data_source, channel, max_data_date,
        'KEYWORD_RANK_2', trends_top_kw_2, 'trends_kw_interest', trends_kw2_interest,
        CAST(NULL AS FLOAT64), CAST(NULL AS FLOAT64), CAST(NULL AS FLOAT64), CAST(NULL AS FLOAT64)
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseTms_silver_googleTrends_weekly`
    WHERE NULLIF(TRIM(trends_top_kw_2), '') IS NOT NULL

    UNION ALL SELECT week_sun_to_sat, data_source, channel, max_data_date,
        'KEYWORD_RANK_3', trends_top_kw_3, 'trends_kw_interest', trends_kw3_interest,
        CAST(NULL AS FLOAT64), CAST(NULL AS FLOAT64), CAST(NULL AS FLOAT64), CAST(NULL AS FLOAT64)
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseTms_silver_googleTrends_weekly`
    WHERE NULLIF(TRIM(trends_top_kw_3), '') IS NOT NULL

    UNION ALL SELECT week_sun_to_sat, data_source, channel, max_data_date,
        'KEYWORD_RANK_4', trends_top_kw_4, 'trends_kw_interest', trends_kw4_interest,
        CAST(NULL AS FLOAT64), CAST(NULL AS FLOAT64), CAST(NULL AS FLOAT64), CAST(NULL AS FLOAT64)
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseTms_silver_googleTrends_weekly`
    WHERE NULLIF(TRIM(trends_top_kw_4), '') IS NOT NULL

    UNION ALL SELECT week_sun_to_sat, data_source, channel, max_data_date,
        'KEYWORD_RANK_5', trends_top_kw_5, 'trends_kw_interest', trends_kw5_interest,
        CAST(NULL AS FLOAT64), CAST(NULL AS FLOAT64), CAST(NULL AS FLOAT64), CAST(NULL AS FLOAT64)
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseTms_silver_googleTrends_weekly`
    WHERE NULLIF(TRIM(trends_top_kw_5), '') IS NOT NULL
),

-- -----------------------------------------------------------------------
-- ADOBE: All product lines — multi-channel
-- 122 metric rows per week (BYOD + Postpaid + HSI + Totals × 7 channels)
-- data_source = 'ADOBE' from Silver
-- channel derived from metric_name suffix in combined CTE
-- -----------------------------------------------------------------------
adobe_long AS (
    SELECT
        week_sun_to_sat, data_source, max_data_date,
        metric_name, metric_value, metric_value_wow, metric_value_ly, wow_pct, yoy_pct
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseTms_silver_adobe_weekly`
    UNPIVOT (
        (metric_value, metric_value_wow, metric_value_ly, wow_pct, yoy_pct)
        FOR metric_name IN (
            -- ALL CHANNELS ONLY
            (adobe_uvnbTotalAdobe_allChannels,              adobe_uvnbTotalAdobe_allChannels_wow,              adobe_uvnbTotalAdobe_allChannels_ly,              adobe_uvnbTotalAdobe_allChannels_wow_pct,              adobe_uvnbTotalAdobe_allChannels_yoy_pct)              AS 'adobe_uvnbTotalAdobe_allChannels',
            (adobe_uvnbFlowTotal_allChannels,               adobe_uvnbFlowTotal_allChannels_wow,               adobe_uvnbFlowTotal_allChannels_ly,               adobe_uvnbFlowTotal_allChannels_wow_pct,               adobe_uvnbFlowTotal_allChannels_yoy_pct)               AS 'adobe_uvnbFlowTotal_allChannels',
            (adobe_uvnbByodPctOfUvnbFlow_allChannels,       adobe_uvnbByodPctOfUvnbFlow_allChannels_wow,       adobe_uvnbByodPctOfUvnbFlow_allChannels_ly,       adobe_uvnbByodPctOfUvnbFlow_allChannels_wow_pct,       adobe_uvnbByodPctOfUvnbFlow_allChannels_yoy_pct)       AS 'adobe_uvnbByodPctOfUvnbFlow_allChannels',
            -- ALL CHANNELS × PRODUCT LINES
            (adobe_uvnbPostpaid_allChannels,                adobe_uvnbPostpaid_allChannels_wow,                adobe_uvnbPostpaid_allChannels_ly,                adobe_uvnbPostpaid_allChannels_wow_pct,                adobe_uvnbPostpaid_allChannels_yoy_pct)                AS 'adobe_uvnbPostpaid_allChannels',
            (adobe_uvnbHsi_allChannels,                     adobe_uvnbHsi_allChannels_wow,                     adobe_uvnbHsi_allChannels_ly,                     adobe_uvnbHsi_allChannels_wow_pct,                     adobe_uvnbHsi_allChannels_yoy_pct)                     AS 'adobe_uvnbHsi_allChannels',
            (adobe_uvnbByod_allChannels,                    adobe_uvnbByod_allChannels_wow,                    adobe_uvnbByod_allChannels_ly,                    adobe_uvnbByod_allChannels_wow_pct,                    adobe_uvnbByod_allChannels_yoy_pct)                    AS 'adobe_uvnbByod_allChannels',
            (adobe_uvnbTrackedFlowSum_allChannels,           adobe_uvnbTrackedFlowSum_allChannels_wow,           adobe_uvnbTrackedFlowSum_allChannels_ly,           adobe_uvnbTrackedFlowSum_allChannels_wow_pct,           adobe_uvnbTrackedFlowSum_allChannels_yoy_pct)           AS 'adobe_uvnbTrackedFlowSum_allChannels',
            (adobe_cartStartTotal_allChannels,               adobe_cartStartTotal_allChannels_wow,               adobe_cartStartTotal_allChannels_ly,               adobe_cartStartTotal_allChannels_wow_pct,               adobe_cartStartTotal_allChannels_yoy_pct)               AS 'adobe_cartStartTotal_allChannels',
            (adobe_cartStartPostpaid_allChannels,            adobe_cartStartPostpaid_allChannels_wow,            adobe_cartStartPostpaid_allChannels_ly,            adobe_cartStartPostpaid_allChannels_wow_pct,            adobe_cartStartPostpaid_allChannels_yoy_pct)            AS 'adobe_cartStartPostpaid_allChannels',
            (adobe_cartStartHsi_allChannels,                 adobe_cartStartHsi_allChannels_wow,                 adobe_cartStartHsi_allChannels_ly,                 adobe_cartStartHsi_allChannels_wow_pct,                 adobe_cartStartHsi_allChannels_yoy_pct)                 AS 'adobe_cartStartHsi_allChannels',
            (adobe_cartStartByod_allChannels,                adobe_cartStartByod_allChannels_wow,                adobe_cartStartByod_allChannels_ly,                adobe_cartStartByod_allChannels_wow_pct,                adobe_cartStartByod_allChannels_yoy_pct)                AS 'adobe_cartStartByod_allChannels',
            (adobe_ordersTotal_allChannels,                  adobe_ordersTotal_allChannels_wow,                  adobe_ordersTotal_allChannels_ly,                  adobe_ordersTotal_allChannels_wow_pct,                  adobe_ordersTotal_allChannels_yoy_pct)                  AS 'adobe_ordersTotal_allChannels',
            (adobe_ordersUnassistedTotal_allChannels,        adobe_ordersUnassistedTotal_allChannels_wow,        adobe_ordersUnassistedTotal_allChannels_ly,        adobe_ordersUnassistedTotal_allChannels_wow_pct,        adobe_ordersUnassistedTotal_allChannels_yoy_pct)        AS 'adobe_ordersUnassistedTotal_allChannels',
            (adobe_ordersUnassistedPostpaid_allChannels,     adobe_ordersUnassistedPostpaid_allChannels_wow,     adobe_ordersUnassistedPostpaid_allChannels_ly,     adobe_ordersUnassistedPostpaid_allChannels_wow_pct,     adobe_ordersUnassistedPostpaid_allChannels_yoy_pct)     AS 'adobe_ordersUnassistedPostpaid_allChannels',
            (adobe_ordersUnassistedHsi_allChannels,          adobe_ordersUnassistedHsi_allChannels_wow,          adobe_ordersUnassistedHsi_allChannels_ly,          adobe_ordersUnassistedHsi_allChannels_wow_pct,          adobe_ordersUnassistedHsi_allChannels_yoy_pct)          AS 'adobe_ordersUnassistedHsi_allChannels',
            (adobe_ordersUnassistedByod_allChannels,         adobe_ordersUnassistedByod_allChannels_wow,         adobe_ordersUnassistedByod_allChannels_ly,         adobe_ordersUnassistedByod_allChannels_wow_pct,         adobe_ordersUnassistedByod_allChannels_yoy_pct)         AS 'adobe_ordersUnassistedByod_allChannels',
            (adobe_ordersAssistedTotal_allChannels,          adobe_ordersAssistedTotal_allChannels_wow,          adobe_ordersAssistedTotal_allChannels_ly,          adobe_ordersAssistedTotal_allChannels_wow_pct,          adobe_ordersAssistedTotal_allChannels_yoy_pct)          AS 'adobe_ordersAssistedTotal_allChannels',
            (adobe_ordersAssistedPostpaid_allChannels,       adobe_ordersAssistedPostpaid_allChannels_wow,       adobe_ordersAssistedPostpaid_allChannels_ly,       adobe_ordersAssistedPostpaid_allChannels_wow_pct,       adobe_ordersAssistedPostpaid_allChannels_yoy_pct)       AS 'adobe_ordersAssistedPostpaid_allChannels',
            (adobe_ordersAssistedHsi_allChannels,            adobe_ordersAssistedHsi_allChannels_wow,            adobe_ordersAssistedHsi_allChannels_ly,            adobe_ordersAssistedHsi_allChannels_wow_pct,            adobe_ordersAssistedHsi_allChannels_yoy_pct)            AS 'adobe_ordersAssistedHsi_allChannels',
            (adobe_ordersAssistedByod_allChannels,           adobe_ordersAssistedByod_allChannels_wow,           adobe_ordersAssistedByod_allChannels_ly,           adobe_ordersAssistedByod_allChannels_wow_pct,           adobe_ordersAssistedByod_allChannels_yoy_pct)           AS 'adobe_ordersAssistedByod_allChannels',
            -- PAID SEARCH
            (adobe_uvnbPostpaid_paidSearch,                  adobe_uvnbPostpaid_paidSearch_wow,                  adobe_uvnbPostpaid_paidSearch_ly,                  adobe_uvnbPostpaid_paidSearch_wow_pct,                  adobe_uvnbPostpaid_paidSearch_yoy_pct)                  AS 'adobe_uvnbPostpaid_paidSearch',
            (adobe_uvnbHsi_paidSearch,                       adobe_uvnbHsi_paidSearch_wow,                       adobe_uvnbHsi_paidSearch_ly,                       adobe_uvnbHsi_paidSearch_wow_pct,                       adobe_uvnbHsi_paidSearch_yoy_pct)                       AS 'adobe_uvnbHsi_paidSearch',
            (adobe_uvnbByod_paidSearch,                      adobe_uvnbByod_paidSearch_wow,                      adobe_uvnbByod_paidSearch_ly,                      adobe_uvnbByod_paidSearch_wow_pct,                      adobe_uvnbByod_paidSearch_yoy_pct)                      AS 'adobe_uvnbByod_paidSearch',
            (adobe_uvnbTrackedFlowSum_paidSearch,             adobe_uvnbTrackedFlowSum_paidSearch_wow,             adobe_uvnbTrackedFlowSum_paidSearch_ly,             adobe_uvnbTrackedFlowSum_paidSearch_wow_pct,             adobe_uvnbTrackedFlowSum_paidSearch_yoy_pct)             AS 'adobe_uvnbTrackedFlowSum_paidSearch',
            (adobe_cartStartTotal_paidSearch,                 adobe_cartStartTotal_paidSearch_wow,                 adobe_cartStartTotal_paidSearch_ly,                 adobe_cartStartTotal_paidSearch_wow_pct,                 adobe_cartStartTotal_paidSearch_yoy_pct)                 AS 'adobe_cartStartTotal_paidSearch',
            (adobe_cartStartPostpaid_paidSearch,              adobe_cartStartPostpaid_paidSearch_wow,              adobe_cartStartPostpaid_paidSearch_ly,              adobe_cartStartPostpaid_paidSearch_wow_pct,              adobe_cartStartPostpaid_paidSearch_yoy_pct)              AS 'adobe_cartStartPostpaid_paidSearch',
            (adobe_cartStartHsi_paidSearch,                   adobe_cartStartHsi_paidSearch_wow,                   adobe_cartStartHsi_paidSearch_ly,                   adobe_cartStartHsi_paidSearch_wow_pct,                   adobe_cartStartHsi_paidSearch_yoy_pct)                   AS 'adobe_cartStartHsi_paidSearch',
            (adobe_cartStartByod_paidSearch,                  adobe_cartStartByod_paidSearch_wow,                  adobe_cartStartByod_paidSearch_ly,                  adobe_cartStartByod_paidSearch_wow_pct,                  adobe_cartStartByod_paidSearch_yoy_pct)                  AS 'adobe_cartStartByod_paidSearch',
            (adobe_ordersTotal_paidSearch,                    adobe_ordersTotal_paidSearch_wow,                    adobe_ordersTotal_paidSearch_ly,                    adobe_ordersTotal_paidSearch_wow_pct,                    adobe_ordersTotal_paidSearch_yoy_pct)                    AS 'adobe_ordersTotal_paidSearch',
            (adobe_ordersUnassistedTotal_paidSearch,          adobe_ordersUnassistedTotal_paidSearch_wow,          adobe_ordersUnassistedTotal_paidSearch_ly,          adobe_ordersUnassistedTotal_paidSearch_wow_pct,          adobe_ordersUnassistedTotal_paidSearch_yoy_pct)          AS 'adobe_ordersUnassistedTotal_paidSearch',
            (adobe_ordersUnassistedPostpaid_paidSearch,       adobe_ordersUnassistedPostpaid_paidSearch_wow,       adobe_ordersUnassistedPostpaid_paidSearch_ly,       adobe_ordersUnassistedPostpaid_paidSearch_wow_pct,       adobe_ordersUnassistedPostpaid_paidSearch_yoy_pct)       AS 'adobe_ordersUnassistedPostpaid_paidSearch',
            (adobe_ordersUnassistedHsi_paidSearch,            adobe_ordersUnassistedHsi_paidSearch_wow,            adobe_ordersUnassistedHsi_paidSearch_ly,            adobe_ordersUnassistedHsi_paidSearch_wow_pct,            adobe_ordersUnassistedHsi_paidSearch_yoy_pct)            AS 'adobe_ordersUnassistedHsi_paidSearch',
            (adobe_ordersUnassistedByod_paidSearch,           adobe_ordersUnassistedByod_paidSearch_wow,           adobe_ordersUnassistedByod_paidSearch_ly,           adobe_ordersUnassistedByod_paidSearch_wow_pct,           adobe_ordersUnassistedByod_paidSearch_yoy_pct)           AS 'adobe_ordersUnassistedByod_paidSearch',
            (adobe_ordersAssistedTotal_paidSearch,            adobe_ordersAssistedTotal_paidSearch_wow,            adobe_ordersAssistedTotal_paidSearch_ly,            adobe_ordersAssistedTotal_paidSearch_wow_pct,            adobe_ordersAssistedTotal_paidSearch_yoy_pct)            AS 'adobe_ordersAssistedTotal_paidSearch',
            (adobe_ordersAssistedPostpaid_paidSearch,         adobe_ordersAssistedPostpaid_paidSearch_wow,         adobe_ordersAssistedPostpaid_paidSearch_ly,         adobe_ordersAssistedPostpaid_paidSearch_wow_pct,         adobe_ordersAssistedPostpaid_paidSearch_yoy_pct)         AS 'adobe_ordersAssistedPostpaid_paidSearch',
            (adobe_ordersAssistedHsi_paidSearch,              adobe_ordersAssistedHsi_paidSearch_wow,              adobe_ordersAssistedHsi_paidSearch_ly,              adobe_ordersAssistedHsi_paidSearch_wow_pct,              adobe_ordersAssistedHsi_paidSearch_yoy_pct)              AS 'adobe_ordersAssistedHsi_paidSearch',
            (adobe_ordersAssistedByod_paidSearch,             adobe_ordersAssistedByod_paidSearch_wow,             adobe_ordersAssistedByod_paidSearch_ly,             adobe_ordersAssistedByod_paidSearch_wow_pct,             adobe_ordersAssistedByod_paidSearch_yoy_pct)             AS 'adobe_ordersAssistedByod_paidSearch',
            -- ORGANIC SEARCH
            (adobe_uvnbPostpaid_organicSearch,               adobe_uvnbPostpaid_organicSearch_wow,               adobe_uvnbPostpaid_organicSearch_ly,               adobe_uvnbPostpaid_organicSearch_wow_pct,               adobe_uvnbPostpaid_organicSearch_yoy_pct)               AS 'adobe_uvnbPostpaid_organicSearch',
            (adobe_uvnbHsi_organicSearch,                    adobe_uvnbHsi_organicSearch_wow,                    adobe_uvnbHsi_organicSearch_ly,                    adobe_uvnbHsi_organicSearch_wow_pct,                    adobe_uvnbHsi_organicSearch_yoy_pct)                    AS 'adobe_uvnbHsi_organicSearch',
            (adobe_uvnbByod_organicSearch,                   adobe_uvnbByod_organicSearch_wow,                   adobe_uvnbByod_organicSearch_ly,                   adobe_uvnbByod_organicSearch_wow_pct,                   adobe_uvnbByod_organicSearch_yoy_pct)                   AS 'adobe_uvnbByod_organicSearch',
            (adobe_uvnbTrackedFlowSum_organicSearch,          adobe_uvnbTrackedFlowSum_organicSearch_wow,          adobe_uvnbTrackedFlowSum_organicSearch_ly,          adobe_uvnbTrackedFlowSum_organicSearch_wow_pct,          adobe_uvnbTrackedFlowSum_organicSearch_yoy_pct)          AS 'adobe_uvnbTrackedFlowSum_organicSearch',
            (adobe_cartStartTotal_organicSearch,              adobe_cartStartTotal_organicSearch_wow,              adobe_cartStartTotal_organicSearch_ly,              adobe_cartStartTotal_organicSearch_wow_pct,              adobe_cartStartTotal_organicSearch_yoy_pct)              AS 'adobe_cartStartTotal_organicSearch',
            (adobe_cartStartPostpaid_organicSearch,           adobe_cartStartPostpaid_organicSearch_wow,           adobe_cartStartPostpaid_organicSearch_ly,           adobe_cartStartPostpaid_organicSearch_wow_pct,           adobe_cartStartPostpaid_organicSearch_yoy_pct)           AS 'adobe_cartStartPostpaid_organicSearch',
            (adobe_cartStartHsi_organicSearch,                adobe_cartStartHsi_organicSearch_wow,                adobe_cartStartHsi_organicSearch_ly,                adobe_cartStartHsi_organicSearch_wow_pct,                adobe_cartStartHsi_organicSearch_yoy_pct)                AS 'adobe_cartStartHsi_organicSearch',
            (adobe_cartStartByod_organicSearch,               adobe_cartStartByod_organicSearch_wow,               adobe_cartStartByod_organicSearch_ly,               adobe_cartStartByod_organicSearch_wow_pct,               adobe_cartStartByod_organicSearch_yoy_pct)               AS 'adobe_cartStartByod_organicSearch',
            (adobe_ordersTotal_organicSearch,                 adobe_ordersTotal_organicSearch_wow,                 adobe_ordersTotal_organicSearch_ly,                 adobe_ordersTotal_organicSearch_wow_pct,                 adobe_ordersTotal_organicSearch_yoy_pct)                 AS 'adobe_ordersTotal_organicSearch',
            (adobe_ordersUnassistedTotal_organicSearch,       adobe_ordersUnassistedTotal_organicSearch_wow,       adobe_ordersUnassistedTotal_organicSearch_ly,       adobe_ordersUnassistedTotal_organicSearch_wow_pct,       adobe_ordersUnassistedTotal_organicSearch_yoy_pct)       AS 'adobe_ordersUnassistedTotal_organicSearch',
            (adobe_ordersUnassistedPostpaid_organicSearch,    adobe_ordersUnassistedPostpaid_organicSearch_wow,    adobe_ordersUnassistedPostpaid_organicSearch_ly,    adobe_ordersUnassistedPostpaid_organicSearch_wow_pct,    adobe_ordersUnassistedPostpaid_organicSearch_yoy_pct)    AS 'adobe_ordersUnassistedPostpaid_organicSearch',
            (adobe_ordersUnassistedHsi_organicSearch,         adobe_ordersUnassistedHsi_organicSearch_wow,         adobe_ordersUnassistedHsi_organicSearch_ly,         adobe_ordersUnassistedHsi_organicSearch_wow_pct,         adobe_ordersUnassistedHsi_organicSearch_yoy_pct)         AS 'adobe_ordersUnassistedHsi_organicSearch',
            (adobe_ordersUnassistedByod_organicSearch,        adobe_ordersUnassistedByod_organicSearch_wow,        adobe_ordersUnassistedByod_organicSearch_ly,        adobe_ordersUnassistedByod_organicSearch_wow_pct,        adobe_ordersUnassistedByod_organicSearch_yoy_pct)        AS 'adobe_ordersUnassistedByod_organicSearch',
            (adobe_ordersAssistedTotal_organicSearch,         adobe_ordersAssistedTotal_organicSearch_wow,         adobe_ordersAssistedTotal_organicSearch_ly,         adobe_ordersAssistedTotal_organicSearch_wow_pct,         adobe_ordersAssistedTotal_organicSearch_yoy_pct)         AS 'adobe_ordersAssistedTotal_organicSearch',
            (adobe_ordersAssistedPostpaid_organicSearch,      adobe_ordersAssistedPostpaid_organicSearch_wow,      adobe_ordersAssistedPostpaid_organicSearch_ly,      adobe_ordersAssistedPostpaid_organicSearch_wow_pct,      adobe_ordersAssistedPostpaid_organicSearch_yoy_pct)      AS 'adobe_ordersAssistedPostpaid_organicSearch',
            (adobe_ordersAssistedHsi_organicSearch,           adobe_ordersAssistedHsi_organicSearch_wow,           adobe_ordersAssistedHsi_organicSearch_ly,           adobe_ordersAssistedHsi_organicSearch_wow_pct,           adobe_ordersAssistedHsi_organicSearch_yoy_pct)           AS 'adobe_ordersAssistedHsi_organicSearch',
            (adobe_ordersAssistedByod_organicSearch,          adobe_ordersAssistedByod_organicSearch_wow,          adobe_ordersAssistedByod_organicSearch_ly,          adobe_ordersAssistedByod_organicSearch_wow_pct,          adobe_ordersAssistedByod_organicSearch_yoy_pct)          AS 'adobe_ordersAssistedByod_organicSearch',
            -- DIRECT
            (adobe_uvnbPostpaid_direct,                       adobe_uvnbPostpaid_direct_wow,                       adobe_uvnbPostpaid_direct_ly,                       adobe_uvnbPostpaid_direct_wow_pct,                       adobe_uvnbPostpaid_direct_yoy_pct)                       AS 'adobe_uvnbPostpaid_direct',
            (adobe_uvnbHsi_direct,                            adobe_uvnbHsi_direct_wow,                            adobe_uvnbHsi_direct_ly,                            adobe_uvnbHsi_direct_wow_pct,                            adobe_uvnbHsi_direct_yoy_pct)                            AS 'adobe_uvnbHsi_direct',
            (adobe_uvnbByod_direct,                           adobe_uvnbByod_direct_wow,                           adobe_uvnbByod_direct_ly,                           adobe_uvnbByod_direct_wow_pct,                           adobe_uvnbByod_direct_yoy_pct)                           AS 'adobe_uvnbByod_direct',
            (adobe_uvnbTrackedFlowSum_direct,                  adobe_uvnbTrackedFlowSum_direct_wow,                  adobe_uvnbTrackedFlowSum_direct_ly,                  adobe_uvnbTrackedFlowSum_direct_wow_pct,                  adobe_uvnbTrackedFlowSum_direct_yoy_pct)                  AS 'adobe_uvnbTrackedFlowSum_direct',
            (adobe_cartStartTotal_direct,                      adobe_cartStartTotal_direct_wow,                      adobe_cartStartTotal_direct_ly,                      adobe_cartStartTotal_direct_wow_pct,                      adobe_cartStartTotal_direct_yoy_pct)                      AS 'adobe_cartStartTotal_direct',
            (adobe_cartStartPostpaid_direct,                   adobe_cartStartPostpaid_direct_wow,                   adobe_cartStartPostpaid_direct_ly,                   adobe_cartStartPostpaid_direct_wow_pct,                   adobe_cartStartPostpaid_direct_yoy_pct)                   AS 'adobe_cartStartPostpaid_direct',
            (adobe_cartStartHsi_direct,                        adobe_cartStartHsi_direct_wow,                        adobe_cartStartHsi_direct_ly,                        adobe_cartStartHsi_direct_wow_pct,                        adobe_cartStartHsi_direct_yoy_pct)                        AS 'adobe_cartStartHsi_direct',
            (adobe_cartStartByod_direct,                       adobe_cartStartByod_direct_wow,                       adobe_cartStartByod_direct_ly,                       adobe_cartStartByod_direct_wow_pct,                       adobe_cartStartByod_direct_yoy_pct)                       AS 'adobe_cartStartByod_direct',
            (adobe_ordersTotal_direct,                         adobe_ordersTotal_direct_wow,                         adobe_ordersTotal_direct_ly,                         adobe_ordersTotal_direct_wow_pct,                         adobe_ordersTotal_direct_yoy_pct)                         AS 'adobe_ordersTotal_direct',
            (adobe_ordersUnassistedTotal_direct,               adobe_ordersUnassistedTotal_direct_wow,               adobe_ordersUnassistedTotal_direct_ly,               adobe_ordersUnassistedTotal_direct_wow_pct,               adobe_ordersUnassistedTotal_direct_yoy_pct)               AS 'adobe_ordersUnassistedTotal_direct',
            (adobe_ordersUnassistedPostpaid_direct,            adobe_ordersUnassistedPostpaid_direct_wow,            adobe_ordersUnassistedPostpaid_direct_ly,            adobe_ordersUnassistedPostpaid_direct_wow_pct,            adobe_ordersUnassistedPostpaid_direct_yoy_pct)            AS 'adobe_ordersUnassistedPostpaid_direct',
            (adobe_ordersUnassistedHsi_direct,                 adobe_ordersUnassistedHsi_direct_wow,                 adobe_ordersUnassistedHsi_direct_ly,                 adobe_ordersUnassistedHsi_direct_wow_pct,                 adobe_ordersUnassistedHsi_direct_yoy_pct)                 AS 'adobe_ordersUnassistedHsi_direct',
            (adobe_ordersUnassistedByod_direct,                adobe_ordersUnassistedByod_direct_wow,                adobe_ordersUnassistedByod_direct_ly,                adobe_ordersUnassistedByod_direct_wow_pct,                adobe_ordersUnassistedByod_direct_yoy_pct)                AS 'adobe_ordersUnassistedByod_direct',
            (adobe_ordersAssistedTotal_direct,                 adobe_ordersAssistedTotal_direct_wow,                 adobe_ordersAssistedTotal_direct_ly,                 adobe_ordersAssistedTotal_direct_wow_pct,                 adobe_ordersAssistedTotal_direct_yoy_pct)                 AS 'adobe_ordersAssistedTotal_direct',
            (adobe_ordersAssistedPostpaid_direct,              adobe_ordersAssistedPostpaid_direct_wow,              adobe_ordersAssistedPostpaid_direct_ly,              adobe_ordersAssistedPostpaid_direct_wow_pct,              adobe_ordersAssistedPostpaid_direct_yoy_pct)              AS 'adobe_ordersAssistedPostpaid_direct',
            (adobe_ordersAssistedHsi_direct,                   adobe_ordersAssistedHsi_direct_wow,                   adobe_ordersAssistedHsi_direct_ly,                   adobe_ordersAssistedHsi_direct_wow_pct,                   adobe_ordersAssistedHsi_direct_yoy_pct)                   AS 'adobe_ordersAssistedHsi_direct',
            (adobe_ordersAssistedByod_direct,                  adobe_ordersAssistedByod_direct_wow,                  adobe_ordersAssistedByod_direct_ly,                  adobe_ordersAssistedByod_direct_wow_pct,                  adobe_ordersAssistedByod_direct_yoy_pct)                  AS 'adobe_ordersAssistedByod_direct',
            -- SOCIAL
            (adobe_uvnbPostpaid_social,                        adobe_uvnbPostpaid_social_wow,                        adobe_uvnbPostpaid_social_ly,                        adobe_uvnbPostpaid_social_wow_pct,                        adobe_uvnbPostpaid_social_yoy_pct)                        AS 'adobe_uvnbPostpaid_social',
            (adobe_uvnbHsi_social,                             adobe_uvnbHsi_social_wow,                             adobe_uvnbHsi_social_ly,                             adobe_uvnbHsi_social_wow_pct,                             adobe_uvnbHsi_social_yoy_pct)                             AS 'adobe_uvnbHsi_social',
            (adobe_uvnbByod_social,                            adobe_uvnbByod_social_wow,                            adobe_uvnbByod_social_ly,                            adobe_uvnbByod_social_wow_pct,                            adobe_uvnbByod_social_yoy_pct)                            AS 'adobe_uvnbByod_social',
            (adobe_uvnbTrackedFlowSum_social,                   adobe_uvnbTrackedFlowSum_social_wow,                   adobe_uvnbTrackedFlowSum_social_ly,                   adobe_uvnbTrackedFlowSum_social_wow_pct,                   adobe_uvnbTrackedFlowSum_social_yoy_pct)                   AS 'adobe_uvnbTrackedFlowSum_social',
            (adobe_cartStartTotal_social,                       adobe_cartStartTotal_social_wow,                       adobe_cartStartTotal_social_ly,                       adobe_cartStartTotal_social_wow_pct,                       adobe_cartStartTotal_social_yoy_pct)                       AS 'adobe_cartStartTotal_social',
            (adobe_cartStartPostpaid_social,                    adobe_cartStartPostpaid_social_wow,                    adobe_cartStartPostpaid_social_ly,                    adobe_cartStartPostpaid_social_wow_pct,                    adobe_cartStartPostpaid_social_yoy_pct)                    AS 'adobe_cartStartPostpaid_social',
            (adobe_cartStartHsi_social,                         adobe_cartStartHsi_social_wow,                         adobe_cartStartHsi_social_ly,                         adobe_cartStartHsi_social_wow_pct,                         adobe_cartStartHsi_social_yoy_pct)                         AS 'adobe_cartStartHsi_social',
            (adobe_cartStartByod_social,                        adobe_cartStartByod_social_wow,                        adobe_cartStartByod_social_ly,                        adobe_cartStartByod_social_wow_pct,                        adobe_cartStartByod_social_yoy_pct)                        AS 'adobe_cartStartByod_social',
            (adobe_ordersTotal_social,                          adobe_ordersTotal_social_wow,                          adobe_ordersTotal_social_ly,                          adobe_ordersTotal_social_wow_pct,                          adobe_ordersTotal_social_yoy_pct)                          AS 'adobe_ordersTotal_social',
            (adobe_ordersUnassistedTotal_social,                adobe_ordersUnassistedTotal_social_wow,                adobe_ordersUnassistedTotal_social_ly,                adobe_ordersUnassistedTotal_social_wow_pct,                adobe_ordersUnassistedTotal_social_yoy_pct)                AS 'adobe_ordersUnassistedTotal_social',
            (adobe_ordersUnassistedPostpaid_social,             adobe_ordersUnassistedPostpaid_social_wow,             adobe_ordersUnassistedPostpaid_social_ly,             adobe_ordersUnassistedPostpaid_social_wow_pct,             adobe_ordersUnassistedPostpaid_social_yoy_pct)             AS 'adobe_ordersUnassistedPostpaid_social',
            (adobe_ordersUnassistedHsi_social,                  adobe_ordersUnassistedHsi_social_wow,                  adobe_ordersUnassistedHsi_social_ly,                  adobe_ordersUnassistedHsi_social_wow_pct,                  adobe_ordersUnassistedHsi_social_yoy_pct)                  AS 'adobe_ordersUnassistedHsi_social',
            (adobe_ordersUnassistedByod_social,                 adobe_ordersUnassistedByod_social_wow,                 adobe_ordersUnassistedByod_social_ly,                 adobe_ordersUnassistedByod_social_wow_pct,                 adobe_ordersUnassistedByod_social_yoy_pct)                 AS 'adobe_ordersUnassistedByod_social',
            (adobe_ordersAssistedTotal_social,                  adobe_ordersAssistedTotal_social_wow,                  adobe_ordersAssistedTotal_social_ly,                  adobe_ordersAssistedTotal_social_wow_pct,                  adobe_ordersAssistedTotal_social_yoy_pct)                  AS 'adobe_ordersAssistedTotal_social',
            (adobe_ordersAssistedPostpaid_social,               adobe_ordersAssistedPostpaid_social_wow,               adobe_ordersAssistedPostpaid_social_ly,               adobe_ordersAssistedPostpaid_social_wow_pct,               adobe_ordersAssistedPostpaid_social_yoy_pct)               AS 'adobe_ordersAssistedPostpaid_social',
            (adobe_ordersAssistedHsi_social,                    adobe_ordersAssistedHsi_social_wow,                    adobe_ordersAssistedHsi_social_ly,                    adobe_ordersAssistedHsi_social_wow_pct,                    adobe_ordersAssistedHsi_social_yoy_pct)                    AS 'adobe_ordersAssistedHsi_social',
            (adobe_ordersAssistedByod_social,                   adobe_ordersAssistedByod_social_wow,                   adobe_ordersAssistedByod_social_ly,                   adobe_ordersAssistedByod_social_wow_pct,                   adobe_ordersAssistedByod_social_yoy_pct)                   AS 'adobe_ordersAssistedByod_social',
            -- PROGRAMMATIC
            (adobe_uvnbPostpaid_programmatic,                  adobe_uvnbPostpaid_programmatic_wow,                  adobe_uvnbPostpaid_programmatic_ly,                  adobe_uvnbPostpaid_programmatic_wow_pct,                  adobe_uvnbPostpaid_programmatic_yoy_pct)                  AS 'adobe_uvnbPostpaid_programmatic',
            (adobe_uvnbHsi_programmatic,                       adobe_uvnbHsi_programmatic_wow,                       adobe_uvnbHsi_programmatic_ly,                       adobe_uvnbHsi_programmatic_wow_pct,                       adobe_uvnbHsi_programmatic_yoy_pct)                       AS 'adobe_uvnbHsi_programmatic',
            (adobe_uvnbByod_programmatic,                      adobe_uvnbByod_programmatic_wow,                      adobe_uvnbByod_programmatic_ly,                      adobe_uvnbByod_programmatic_wow_pct,                      adobe_uvnbByod_programmatic_yoy_pct)                      AS 'adobe_uvnbByod_programmatic',
            (adobe_uvnbTrackedFlowSum_programmatic,             adobe_uvnbTrackedFlowSum_programmatic_wow,             adobe_uvnbTrackedFlowSum_programmatic_ly,             adobe_uvnbTrackedFlowSum_programmatic_wow_pct,             adobe_uvnbTrackedFlowSum_programmatic_yoy_pct)             AS 'adobe_uvnbTrackedFlowSum_programmatic',
            (adobe_cartStartTotal_programmatic,                 adobe_cartStartTotal_programmatic_wow,                 adobe_cartStartTotal_programmatic_ly,                 adobe_cartStartTotal_programmatic_wow_pct,                 adobe_cartStartTotal_programmatic_yoy_pct)                 AS 'adobe_cartStartTotal_programmatic',
            (adobe_cartStartPostpaid_programmatic,              adobe_cartStartPostpaid_programmatic_wow,              adobe_cartStartPostpaid_programmatic_ly,              adobe_cartStartPostpaid_programmatic_wow_pct,              adobe_cartStartPostpaid_programmatic_yoy_pct)              AS 'adobe_cartStartPostpaid_programmatic',
            (adobe_cartStartHsi_programmatic,                   adobe_cartStartHsi_programmatic_wow,                   adobe_cartStartHsi_programmatic_ly,                   adobe_cartStartHsi_programmatic_wow_pct,                   adobe_cartStartHsi_programmatic_yoy_pct)                   AS 'adobe_cartStartHsi_programmatic',
            (adobe_cartStartByod_programmatic,                  adobe_cartStartByod_programmatic_wow,                  adobe_cartStartByod_programmatic_ly,                  adobe_cartStartByod_programmatic_wow_pct,                  adobe_cartStartByod_programmatic_yoy_pct)                  AS 'adobe_cartStartByod_programmatic',
            (adobe_ordersTotal_programmatic,                    adobe_ordersTotal_programmatic_wow,                    adobe_ordersTotal_programmatic_ly,                    adobe_ordersTotal_programmatic_wow_pct,                    adobe_ordersTotal_programmatic_yoy_pct)                    AS 'adobe_ordersTotal_programmatic',
            (adobe_ordersUnassistedTotal_programmatic,          adobe_ordersUnassistedTotal_programmatic_wow,          adobe_ordersUnassistedTotal_programmatic_ly,          adobe_ordersUnassistedTotal_programmatic_wow_pct,          adobe_ordersUnassistedTotal_programmatic_yoy_pct)          AS 'adobe_ordersUnassistedTotal_programmatic',
            (adobe_ordersUnassistedPostpaid_programmatic,       adobe_ordersUnassistedPostpaid_programmatic_wow,       adobe_ordersUnassistedPostpaid_programmatic_ly,       adobe_ordersUnassistedPostpaid_programmatic_wow_pct,       adobe_ordersUnassistedPostpaid_programmatic_yoy_pct)       AS 'adobe_ordersUnassistedPostpaid_programmatic',
            (adobe_ordersUnassistedHsi_programmatic,            adobe_ordersUnassistedHsi_programmatic_wow,            adobe_ordersUnassistedHsi_programmatic_ly,            adobe_ordersUnassistedHsi_programmatic_wow_pct,            adobe_ordersUnassistedHsi_programmatic_yoy_pct)            AS 'adobe_ordersUnassistedHsi_programmatic',
            (adobe_ordersUnassistedByod_programmatic,           adobe_ordersUnassistedByod_programmatic_wow,           adobe_ordersUnassistedByod_programmatic_ly,           adobe_ordersUnassistedByod_programmatic_wow_pct,           adobe_ordersUnassistedByod_programmatic_yoy_pct)           AS 'adobe_ordersUnassistedByod_programmatic',
            (adobe_ordersAssistedTotal_programmatic,            adobe_ordersAssistedTotal_programmatic_wow,            adobe_ordersAssistedTotal_programmatic_ly,            adobe_ordersAssistedTotal_programmatic_wow_pct,            adobe_ordersAssistedTotal_programmatic_yoy_pct)            AS 'adobe_ordersAssistedTotal_programmatic',
            (adobe_ordersAssistedPostpaid_programmatic,         adobe_ordersAssistedPostpaid_programmatic_wow,         adobe_ordersAssistedPostpaid_programmatic_ly,         adobe_ordersAssistedPostpaid_programmatic_wow_pct,         adobe_ordersAssistedPostpaid_programmatic_yoy_pct)         AS 'adobe_ordersAssistedPostpaid_programmatic',
            (adobe_ordersAssistedHsi_programmatic,              adobe_ordersAssistedHsi_programmatic_wow,              adobe_ordersAssistedHsi_programmatic_ly,              adobe_ordersAssistedHsi_programmatic_wow_pct,              adobe_ordersAssistedHsi_programmatic_yoy_pct)              AS 'adobe_ordersAssistedHsi_programmatic',
            (adobe_ordersAssistedByod_programmatic,             adobe_ordersAssistedByod_programmatic_wow,             adobe_ordersAssistedByod_programmatic_ly,             adobe_ordersAssistedByod_programmatic_wow_pct,             adobe_ordersAssistedByod_programmatic_yoy_pct)             AS 'adobe_ordersAssistedByod_programmatic',
            -- OTHER
            (adobe_uvnbPostpaid_other,                         adobe_uvnbPostpaid_other_wow,                         adobe_uvnbPostpaid_other_ly,                         adobe_uvnbPostpaid_other_wow_pct,                         adobe_uvnbPostpaid_other_yoy_pct)                         AS 'adobe_uvnbPostpaid_other',
            (adobe_uvnbHsi_other,                              adobe_uvnbHsi_other_wow,                              adobe_uvnbHsi_other_ly,                              adobe_uvnbHsi_other_wow_pct,                              adobe_uvnbHsi_other_yoy_pct)                              AS 'adobe_uvnbHsi_other',
            (adobe_uvnbByod_other,                             adobe_uvnbByod_other_wow,                             adobe_uvnbByod_other_ly,                             adobe_uvnbByod_other_wow_pct,                             adobe_uvnbByod_other_yoy_pct)                             AS 'adobe_uvnbByod_other',
            (adobe_uvnbTrackedFlowSum_other,                    adobe_uvnbTrackedFlowSum_other_wow,                    adobe_uvnbTrackedFlowSum_other_ly,                    adobe_uvnbTrackedFlowSum_other_wow_pct,                    adobe_uvnbTrackedFlowSum_other_yoy_pct)                    AS 'adobe_uvnbTrackedFlowSum_other',
            (adobe_cartStartTotal_other,                        adobe_cartStartTotal_other_wow,                        adobe_cartStartTotal_other_ly,                        adobe_cartStartTotal_other_wow_pct,                        adobe_cartStartTotal_other_yoy_pct)                        AS 'adobe_cartStartTotal_other',
            (adobe_cartStartPostpaid_other,                     adobe_cartStartPostpaid_other_wow,                     adobe_cartStartPostpaid_other_ly,                     adobe_cartStartPostpaid_other_wow_pct,                     adobe_cartStartPostpaid_other_yoy_pct)                     AS 'adobe_cartStartPostpaid_other',
            (adobe_cartStartHsi_other,                          adobe_cartStartHsi_other_wow,                          adobe_cartStartHsi_other_ly,                          adobe_cartStartHsi_other_wow_pct,                          adobe_cartStartHsi_other_yoy_pct)                          AS 'adobe_cartStartHsi_other',
            (adobe_cartStartByod_other,                         adobe_cartStartByod_other_wow,                         adobe_cartStartByod_other_ly,                         adobe_cartStartByod_other_wow_pct,                         adobe_cartStartByod_other_yoy_pct)                         AS 'adobe_cartStartByod_other',
            (adobe_ordersTotal_other,                           adobe_ordersTotal_other_wow,                           adobe_ordersTotal_other_ly,                           adobe_ordersTotal_other_wow_pct,                           adobe_ordersTotal_other_yoy_pct)                           AS 'adobe_ordersTotal_other',
            (adobe_ordersUnassistedTotal_other,                 adobe_ordersUnassistedTotal_other_wow,                 adobe_ordersUnassistedTotal_other_ly,                 adobe_ordersUnassistedTotal_other_wow_pct,                 adobe_ordersUnassistedTotal_other_yoy_pct)                 AS 'adobe_ordersUnassistedTotal_other',
            (adobe_ordersUnassistedPostpaid_other,              adobe_ordersUnassistedPostpaid_other_wow,              adobe_ordersUnassistedPostpaid_other_ly,              adobe_ordersUnassistedPostpaid_other_wow_pct,              adobe_ordersUnassistedPostpaid_other_yoy_pct)              AS 'adobe_ordersUnassistedPostpaid_other',
            (adobe_ordersUnassistedHsi_other,                   adobe_ordersUnassistedHsi_other_wow,                   adobe_ordersUnassistedHsi_other_ly,                   adobe_ordersUnassistedHsi_other_wow_pct,                   adobe_ordersUnassistedHsi_other_yoy_pct)                   AS 'adobe_ordersUnassistedHsi_other',
            (adobe_ordersUnassistedByod_other,                  adobe_ordersUnassistedByod_other_wow,                  adobe_ordersUnassistedByod_other_ly,                  adobe_ordersUnassistedByod_other_wow_pct,                  adobe_ordersUnassistedByod_other_yoy_pct)                  AS 'adobe_ordersUnassistedByod_other',
            (adobe_ordersAssistedTotal_other,                   adobe_ordersAssistedTotal_other_wow,                   adobe_ordersAssistedTotal_other_ly,                   adobe_ordersAssistedTotal_other_wow_pct,                   adobe_ordersAssistedTotal_other_yoy_pct)                   AS 'adobe_ordersAssistedTotal_other',
            (adobe_ordersAssistedPostpaid_other,                adobe_ordersAssistedPostpaid_other_wow,                adobe_ordersAssistedPostpaid_other_ly,                adobe_ordersAssistedPostpaid_other_wow_pct,                adobe_ordersAssistedPostpaid_other_yoy_pct)                AS 'adobe_ordersAssistedPostpaid_other',
            (adobe_ordersAssistedHsi_other,                     adobe_ordersAssistedHsi_other_wow,                     adobe_ordersAssistedHsi_other_ly,                     adobe_ordersAssistedHsi_other_wow_pct,                     adobe_ordersAssistedHsi_other_yoy_pct)                     AS 'adobe_ordersAssistedHsi_other',
            (adobe_ordersAssistedByod_other,                    adobe_ordersAssistedByod_other_wow,                    adobe_ordersAssistedByod_other_ly,                    adobe_ordersAssistedByod_other_wow_pct,                    adobe_ordersAssistedByod_other_yoy_pct)                    AS 'adobe_ordersAssistedByod_other'
        )
    )
),

-- -----------------------------------------------------------------------
-- MFC: Media spend — all LOBs, channels, tactics, message types, agencies
-- Three metric rows per grain row:
--   mfc_spend_actual   (NULL for unexecuted line items — expected)
--   mfc_spend_forecast (always populated)
--   mfc_spend_display  (actual when available, forecast otherwise — PRIMARY)
-- BOUNDARY_WEEK rows: spend values already apportioned at source
--   is_mfc_pre_apportioned = TRUE → reporting MUST NOT apply partial_weight
--   BOUNDARY_WEEK where week_sun_to_sat = quarter_end_date: wow_pct = NULL
--   BOUNDARY_WEEK where week_sun_to_sat = actual Saturday:  wow_pct populated
-- dimension_name   = 'TACTIC', dimension_value   = tactic value
-- dimension_name_2 = 'AGENCY', dimension_value_2 = agency value
-- lob_supported populated for MFC; NULL for all other sources
-- -----------------------------------------------------------------------
mfc_actual_long AS (
    SELECT
        week_sun_to_sat, data_source, channel, max_data_date,
        lob_supported,
        'TACTIC'                            AS dimension_name,
        tactic                              AS dimension_value,
        'AGENCY'                            AS dimension_name_2,
        agency                              AS dimension_value_2,
        'mfc_spend_actual'                  AS metric_name,
        spend_actual                        AS metric_value,
        spend_actual_wow                    AS metric_value_wow,
        spend_actual_ly                     AS metric_value_ly,
        spend_actual_wow_pct                AS wow_pct,
        spend_actual_yoy_pct                AS yoy_pct,
        act_vs_fcst_pct,
        act_vs_fcst_delta,
        week_type = 'BOUNDARY_WEEK'         AS is_mfc_pre_apportioned
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseTms_silver_mfc_weekly`
),

mfc_forecast_long AS (
    SELECT
        week_sun_to_sat, data_source, channel, max_data_date,
        lob_supported,
        'TACTIC'                            AS dimension_name,
        tactic                              AS dimension_value,
        'AGENCY'                            AS dimension_name_2,
        agency                              AS dimension_value_2,
        'mfc_spend_forecast'                AS metric_name,
        spend_forecast                      AS metric_value,
        spend_forecast_wow                  AS metric_value_wow,
        spend_forecast_ly                   AS metric_value_ly,
        spend_forecast_wow_pct              AS wow_pct,
        spend_forecast_yoy_pct              AS yoy_pct,
        act_vs_fcst_pct,
        act_vs_fcst_delta,
        week_type = 'BOUNDARY_WEEK'         AS is_mfc_pre_apportioned
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseTms_silver_mfc_weekly`
),

mfc_display_long AS (
    SELECT
        week_sun_to_sat, data_source, channel, max_data_date,
        lob_supported,
        'TACTIC'                            AS dimension_name,
        tactic                              AS dimension_value,
        'AGENCY'                            AS dimension_name_2,
        agency                              AS dimension_value_2,
        'mfc_spend_display'                 AS metric_name,
        spend_display                       AS metric_value,
        spend_display_wow                   AS metric_value_wow,
        spend_display_ly                    AS metric_value_ly,
        spend_display_wow_pct               AS wow_pct,
        spend_display_yoy_pct               AS yoy_pct,
        act_vs_fcst_pct,
        act_vs_fcst_delta,
        week_type = 'BOUNDARY_WEEK'         AS is_mfc_pre_apportioned
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseTms_silver_mfc_weekly`
),

-- -----------------------------------------------------------------------
-- COMBINE: All sources — two sections
--
-- section_name is NOT stored in Gold Long — it is assigned in the
-- reporting view based on dim_date join type
--
-- All columns present for all sources:
--   Non-MFC sources: lob_supported, dimension_name_2, dimension_value_2,
--                    act_vs_fcst_pct, act_vs_fcst_delta = NULL
--                    is_mfc_pre_apportioned = FALSE
--   MFC sources:     all columns populated
-- -----------------------------------------------------------------------
combined AS (

    -- PROFOUND
    
    SELECT
        week_sun_to_sat, data_source,
        channel,
        max_data_date,
        CAST(NULL AS STRING)    AS lob_supported,
        CAST(NULL AS STRING) AS dimension_name, CAST(NULL AS STRING) AS dimension_value,
        CAST(NULL AS STRING)    AS dimension_name_2,
        CAST(NULL AS STRING)    AS dimension_value_2,
        metric_name, metric_value, metric_value_wow, metric_value_ly, wow_pct, yoy_pct,
        CAST(NULL AS FLOAT64)   AS act_vs_fcst_pct,
        CAST(NULL AS FLOAT64)   AS act_vs_fcst_delta,
        FALSE                   AS is_mfc_pre_apportioned
    FROM profound_long
    UNION ALL
    -- GOFISH
    
    SELECT
        week_sun_to_sat, data_source,
        channel,
        max_data_date,
        CAST(NULL AS STRING)    AS lob_supported,
        CAST(NULL AS STRING) AS dimension_name, CAST(NULL AS STRING) AS dimension_value,
        CAST(NULL AS STRING)    AS dimension_name_2,
        CAST(NULL AS STRING)    AS dimension_value_2,
        metric_name, metric_value, metric_value_wow, metric_value_ly, wow_pct, yoy_pct,
        CAST(NULL AS FLOAT64)   AS act_vs_fcst_pct,
        CAST(NULL AS FLOAT64)   AS act_vs_fcst_delta,
        FALSE                   AS is_mfc_pre_apportioned
    FROM gofish_long
    UNION ALL
    -- SA360
    
    SELECT
        week_sun_to_sat, data_source,
        channel,
        max_data_date,
        CAST(NULL AS STRING)    AS lob_supported,
        CAST(NULL AS STRING) AS dimension_name, CAST(NULL AS STRING) AS dimension_value,
        CAST(NULL AS STRING)    AS dimension_name_2,
        CAST(NULL AS STRING)    AS dimension_value_2,
        metric_name, metric_value, metric_value_wow, metric_value_ly, wow_pct, yoy_pct,
        CAST(NULL AS FLOAT64)   AS act_vs_fcst_pct,
        CAST(NULL AS FLOAT64)   AS act_vs_fcst_delta,
        FALSE                   AS is_mfc_pre_apportioned
    FROM sa360_long
    UNION ALL
    -- GSC
    
    SELECT
        week_sun_to_sat, data_source,
        channel,
        max_data_date,
        CAST(NULL AS STRING)    AS lob_supported,
        CAST(NULL AS STRING) AS dimension_name, CAST(NULL AS STRING) AS dimension_value,
        CAST(NULL AS STRING)    AS dimension_name_2,
        CAST(NULL AS STRING)    AS dimension_value_2,
        metric_name, metric_value, metric_value_wow, metric_value_ly, wow_pct, yoy_pct,
        CAST(NULL AS FLOAT64)   AS act_vs_fcst_pct,
        CAST(NULL AS FLOAT64)   AS act_vs_fcst_delta,
        FALSE                   AS is_mfc_pre_apportioned
    FROM gsc_long
    UNION ALL
    -- TRENDS: byod_index
    
    SELECT
        week_sun_to_sat, data_source,
        channel,
        max_data_date,
        CAST(NULL AS STRING)    AS lob_supported,
        CAST(NULL AS STRING) AS dimension_name, CAST(NULL AS STRING) AS dimension_value,
        CAST(NULL AS STRING)    AS dimension_name_2,
        CAST(NULL AS STRING)    AS dimension_value_2,
        metric_name, metric_value, metric_value_wow, metric_value_ly, wow_pct, yoy_pct,
        CAST(NULL AS FLOAT64)   AS act_vs_fcst_pct,
        CAST(NULL AS FLOAT64)   AS act_vs_fcst_delta,
        FALSE                   AS is_mfc_pre_apportioned
    FROM trends_index_long
    UNION ALL
    -- TRENDS: keywords
    
    SELECT
        week_sun_to_sat, data_source,
        channel,
        max_data_date,
        CAST(NULL AS STRING)    AS lob_supported,
        dimension_name, dimension_value,
        CAST(NULL AS STRING)    AS dimension_name_2,
        CAST(NULL AS STRING)    AS dimension_value_2,
        metric_name, metric_value, metric_value_wow, metric_value_ly, wow_pct, yoy_pct,
        CAST(NULL AS FLOAT64)   AS act_vs_fcst_pct,
        CAST(NULL AS FLOAT64)   AS act_vs_fcst_delta,
        FALSE                   AS is_mfc_pre_apportioned
    FROM trends_keywords_long
    UNION ALL
    -- ADOBE: channel derived from metric_name suffix
    
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
        END                                         AS channel,
        max_data_date,
        CAST(NULL AS STRING)    AS lob_supported,
        CAST(NULL AS STRING) AS dimension_name, CAST(NULL AS STRING) AS dimension_value,
        CAST(NULL AS STRING)    AS dimension_name_2,
        CAST(NULL AS STRING)    AS dimension_value_2,
        metric_name, metric_value, metric_value_wow, metric_value_ly, wow_pct, yoy_pct,
        CAST(NULL AS FLOAT64)   AS act_vs_fcst_pct,
        CAST(NULL AS FLOAT64)   AS act_vs_fcst_delta,
        FALSE                   AS is_mfc_pre_apportioned
    FROM adobe_long
    UNION ALL

    -- MFC: spend_actual
    SELECT
        week_sun_to_sat, data_source, channel, max_data_date,
        lob_supported,
        dimension_name, dimension_value,
        dimension_name_2, dimension_value_2,
        metric_name, metric_value, metric_value_wow, metric_value_ly, wow_pct, yoy_pct,
        act_vs_fcst_pct, act_vs_fcst_delta,
        is_mfc_pre_apportioned
    FROM mfc_actual_long

    UNION ALL

    -- MFC: spend_forecast
    SELECT
        week_sun_to_sat, data_source, channel, max_data_date,
        lob_supported,
        dimension_name, dimension_value,
        dimension_name_2, dimension_value_2,
        metric_name, metric_value, metric_value_wow, metric_value_ly, wow_pct, yoy_pct,
        act_vs_fcst_pct, act_vs_fcst_delta,
        is_mfc_pre_apportioned
    FROM mfc_forecast_long

    UNION ALL

    -- MFC: spend_display (primary metric)
    SELECT
        week_sun_to_sat, data_source, channel, max_data_date,
        lob_supported,
        dimension_name, dimension_value,
        dimension_name_2, dimension_value_2,
        metric_name, metric_value, metric_value_wow, metric_value_ly, wow_pct, yoy_pct,
        act_vs_fcst_pct, act_vs_fcst_delta,
        is_mfc_pre_apportioned
    FROM mfc_display_long
)

-- -----------------------------------------------------------------------
-- FINAL OUTPUT
-- -----------------------------------------------------------------------
SELECT
    week_sun_to_sat,
    'WEEKLY'                                                            AS time_granularity,
    data_source,
    channel,
    lob_supported,
    dimension_name,
    dimension_value,
    dimension_name_2,
    dimension_value_2,
    metric_name,
    metric_value,
    metric_value_wow,
    metric_value_ly,
    wow_pct,
    yoy_pct,
    act_vs_fcst_pct,
    act_vs_fcst_delta,
    is_mfc_pre_apportioned,
    MAX(max_data_date) OVER (
        PARTITION BY data_source
    )                                                                   AS max_data_date
FROM combined
ORDER BY
    week_sun_to_sat         ASC,
    data_source             ASC,
    channel                 ASC,
    metric_name             ASC,
    dimension_value         ASC,
    dimension_value_2       ASC
;