/* =================================================================================================
FILE:         01_vw_sdi_pulseByod_gold_unified_wide.sql
LAYER:        Gold View — Wide
DATASET:      prj-dbi-prd-1.ds_dbi_digitalmedia_automation
VIEW NAME:    vw_sdi_pulseByod_gold_unified_wide

PURPOSE:
  Gold Wide view. One row per week, all metrics from all sources as columns.
  Used for ad-hoc analysis and Excel exports.
  Gold Long (used by dashboard) reads Silver directly.
================================================================================================= */

CREATE OR REPLACE VIEW `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_gold_unified_wide`
AS

SELECT
    COALESCE(sa.week_sun_to_sat, gsc.week_sun_to_sat, p.week_sun_to_sat, g.week_sun_to_sat, t.week_sun_to_sat, ab.week_sun_to_sat) AS week_sun_to_sat,
    'WEEKLY' AS time_granularity,

    -- ================================================================ PROFOUND
    p.max_data_date AS profound_max_data_date,
    p.profound_tmo_nonbrand_visibility_score, p.profound_tmo_nonbrand_visibility_score_wow, p.profound_tmo_nonbrand_visibility_score_ly, p.profound_tmo_nonbrand_visibility_score_wow_pct, p.profound_tmo_nonbrand_visibility_score_yoy_pct,
    p.profound_tmo_nonbrand_executions, p.profound_tmo_nonbrand_executions_wow, p.profound_tmo_nonbrand_executions_ly, p.profound_tmo_nonbrand_executions_wow_pct, p.profound_tmo_nonbrand_executions_yoy_pct,
    p.profound_tmo_nonbrand_mentions_count, p.profound_tmo_nonbrand_mentions_count_wow, p.profound_tmo_nonbrand_mentions_count_ly, p.profound_tmo_nonbrand_mentions_count_wow_pct, p.profound_tmo_nonbrand_mentions_count_yoy_pct,
    p.profound_tmo_nonbrand_share_of_voice, p.profound_tmo_nonbrand_share_of_voice_wow, p.profound_tmo_nonbrand_share_of_voice_ly, p.profound_tmo_nonbrand_share_of_voice_wow_pct, p.profound_tmo_nonbrand_share_of_voice_yoy_pct,
    p.profound_verizon_nonbrand_visibility_score, p.profound_verizon_nonbrand_visibility_score_wow, p.profound_verizon_nonbrand_visibility_score_ly, p.profound_verizon_nonbrand_visibility_score_wow_pct, p.profound_verizon_nonbrand_visibility_score_yoy_pct,
    p.profound_verizon_nonbrand_executions, p.profound_verizon_nonbrand_executions_wow, p.profound_verizon_nonbrand_executions_ly, p.profound_verizon_nonbrand_executions_wow_pct, p.profound_verizon_nonbrand_executions_yoy_pct,
    p.profound_verizon_nonbrand_mentions_count, p.profound_verizon_nonbrand_mentions_count_wow, p.profound_verizon_nonbrand_mentions_count_ly, p.profound_verizon_nonbrand_mentions_count_wow_pct, p.profound_verizon_nonbrand_mentions_count_yoy_pct,
    p.profound_verizon_nonbrand_share_of_voice, p.profound_verizon_nonbrand_share_of_voice_wow, p.profound_verizon_nonbrand_share_of_voice_ly, p.profound_verizon_nonbrand_share_of_voice_wow_pct, p.profound_verizon_nonbrand_share_of_voice_yoy_pct,
    p.profound_att_nonbrand_visibility_score, p.profound_att_nonbrand_visibility_score_wow, p.profound_att_nonbrand_visibility_score_ly, p.profound_att_nonbrand_visibility_score_wow_pct, p.profound_att_nonbrand_visibility_score_yoy_pct,
    p.profound_att_nonbrand_executions, p.profound_att_nonbrand_executions_wow, p.profound_att_nonbrand_executions_ly, p.profound_att_nonbrand_executions_wow_pct, p.profound_att_nonbrand_executions_yoy_pct,
    p.profound_att_nonbrand_mentions_count, p.profound_att_nonbrand_mentions_count_wow, p.profound_att_nonbrand_mentions_count_ly, p.profound_att_nonbrand_mentions_count_wow_pct, p.profound_att_nonbrand_mentions_count_yoy_pct,
    p.profound_att_nonbrand_share_of_voice, p.profound_att_nonbrand_share_of_voice_wow, p.profound_att_nonbrand_share_of_voice_ly, p.profound_att_nonbrand_share_of_voice_wow_pct, p.profound_att_nonbrand_share_of_voice_yoy_pct,

    -- ================================================================ GOFISH
    g.max_data_date AS gofish_max_data_date,
    g.gofish_tmo_brand_visibility_score, g.gofish_tmo_brand_visibility_score_wow, g.gofish_tmo_brand_visibility_score_ly, g.gofish_tmo_brand_visibility_score_wow_pct, g.gofish_tmo_brand_visibility_score_yoy_pct,
    g.gofish_tmo_brand_executions, g.gofish_tmo_brand_executions_wow, g.gofish_tmo_brand_executions_ly, g.gofish_tmo_brand_executions_wow_pct, g.gofish_tmo_brand_executions_yoy_pct,
    g.gofish_tmo_brand_mentions_count, g.gofish_tmo_brand_mentions_count_wow, g.gofish_tmo_brand_mentions_count_ly, g.gofish_tmo_brand_mentions_count_wow_pct, g.gofish_tmo_brand_mentions_count_yoy_pct,
    g.gofish_tmo_brand_share_of_voice, g.gofish_tmo_brand_share_of_voice_wow, g.gofish_tmo_brand_share_of_voice_ly, g.gofish_tmo_brand_share_of_voice_wow_pct, g.gofish_tmo_brand_share_of_voice_yoy_pct,
    g.gofish_verizon_brand_visibility_score, g.gofish_verizon_brand_visibility_score_wow, g.gofish_verizon_brand_visibility_score_ly, g.gofish_verizon_brand_visibility_score_wow_pct, g.gofish_verizon_brand_visibility_score_yoy_pct,
    g.gofish_verizon_brand_executions, g.gofish_verizon_brand_executions_wow, g.gofish_verizon_brand_executions_ly, g.gofish_verizon_brand_executions_wow_pct, g.gofish_verizon_brand_executions_yoy_pct,
    g.gofish_verizon_brand_mentions_count, g.gofish_verizon_brand_mentions_count_wow, g.gofish_verizon_brand_mentions_count_ly, g.gofish_verizon_brand_mentions_count_wow_pct, g.gofish_verizon_brand_mentions_count_yoy_pct,
    g.gofish_verizon_brand_share_of_voice, g.gofish_verizon_brand_share_of_voice_wow, g.gofish_verizon_brand_share_of_voice_ly, g.gofish_verizon_brand_share_of_voice_wow_pct, g.gofish_verizon_brand_share_of_voice_yoy_pct,
    g.gofish_att_brand_visibility_score, g.gofish_att_brand_visibility_score_wow, g.gofish_att_brand_visibility_score_ly, g.gofish_att_brand_visibility_score_wow_pct, g.gofish_att_brand_visibility_score_yoy_pct,
    g.gofish_att_brand_executions, g.gofish_att_brand_executions_wow, g.gofish_att_brand_executions_ly, g.gofish_att_brand_executions_wow_pct, g.gofish_att_brand_executions_yoy_pct,
    g.gofish_att_brand_mentions_count, g.gofish_att_brand_mentions_count_wow, g.gofish_att_brand_mentions_count_ly, g.gofish_att_brand_mentions_count_wow_pct, g.gofish_att_brand_mentions_count_yoy_pct,
    g.gofish_att_brand_share_of_voice, g.gofish_att_brand_share_of_voice_wow, g.gofish_att_brand_share_of_voice_ly, g.gofish_att_brand_share_of_voice_wow_pct, g.gofish_att_brand_share_of_voice_yoy_pct,

    -- ================================================================ SA360
    sa.max_data_date AS sa360_max_data_date,
    sa.sa360_tmo_brand_impressions, sa.sa360_tmo_brand_impressions_wow, sa.sa360_tmo_brand_impressions_ly, sa.sa360_tmo_brand_impressions_wow_pct, sa.sa360_tmo_brand_impressions_yoy_pct,
    sa.sa360_tmo_brand_clicks, sa.sa360_tmo_brand_clicks_wow, sa.sa360_tmo_brand_clicks_ly, sa.sa360_tmo_brand_clicks_wow_pct, sa.sa360_tmo_brand_clicks_yoy_pct,
    sa.sa360_tmo_brand_cost, sa.sa360_tmo_brand_cost_wow, sa.sa360_tmo_brand_cost_ly, sa.sa360_tmo_brand_cost_wow_pct, sa.sa360_tmo_brand_cost_yoy_pct,
    sa.sa360_tmo_brand_orders, sa.sa360_tmo_brand_orders_wow, sa.sa360_tmo_brand_orders_ly, sa.sa360_tmo_brand_orders_wow_pct, sa.sa360_tmo_brand_orders_yoy_pct,
    sa.sa360_tmo_brand_cart_start, sa.sa360_tmo_brand_cart_start_wow, sa.sa360_tmo_brand_cart_start_ly, sa.sa360_tmo_brand_cart_start_wow_pct, sa.sa360_tmo_brand_cart_start_yoy_pct,
    sa.sa360_tmo_brand_postpaid_pspv, sa.sa360_tmo_brand_postpaid_pspv_wow, sa.sa360_tmo_brand_postpaid_pspv_ly, sa.sa360_tmo_brand_postpaid_pspv_wow_pct, sa.sa360_tmo_brand_postpaid_pspv_yoy_pct,
    sa.sa360_tmo_nonbrand_impressions, sa.sa360_tmo_nonbrand_impressions_wow, sa.sa360_tmo_nonbrand_impressions_ly, sa.sa360_tmo_nonbrand_impressions_wow_pct, sa.sa360_tmo_nonbrand_impressions_yoy_pct,
    sa.sa360_tmo_nonbrand_clicks, sa.sa360_tmo_nonbrand_clicks_wow, sa.sa360_tmo_nonbrand_clicks_ly, sa.sa360_tmo_nonbrand_clicks_wow_pct, sa.sa360_tmo_nonbrand_clicks_yoy_pct,
    sa.sa360_tmo_nonbrand_cost, sa.sa360_tmo_nonbrand_cost_wow, sa.sa360_tmo_nonbrand_cost_ly, sa.sa360_tmo_nonbrand_cost_wow_pct, sa.sa360_tmo_nonbrand_cost_yoy_pct,
    sa.sa360_tmo_nonbrand_orders, sa.sa360_tmo_nonbrand_orders_wow, sa.sa360_tmo_nonbrand_orders_ly, sa.sa360_tmo_nonbrand_orders_wow_pct, sa.sa360_tmo_nonbrand_orders_yoy_pct,
    sa.sa360_tmo_nonbrand_cart_start, sa.sa360_tmo_nonbrand_cart_start_wow, sa.sa360_tmo_nonbrand_cart_start_ly, sa.sa360_tmo_nonbrand_cart_start_wow_pct, sa.sa360_tmo_nonbrand_cart_start_yoy_pct,
    sa.sa360_tmo_nonbrand_postpaid_pspv, sa.sa360_tmo_nonbrand_postpaid_pspv_wow, sa.sa360_tmo_nonbrand_postpaid_pspv_ly, sa.sa360_tmo_nonbrand_postpaid_pspv_wow_pct, sa.sa360_tmo_nonbrand_postpaid_pspv_yoy_pct,

    -- ================================================================ GSC
    gsc.max_data_date AS gsc_max_data_date,
    gsc.gsc_tmo_brand_impressions, gsc.gsc_tmo_brand_impressions_wow, gsc.gsc_tmo_brand_impressions_ly, gsc.gsc_tmo_brand_impressions_wow_pct, gsc.gsc_tmo_brand_impressions_yoy_pct,
    gsc.gsc_tmo_brand_clicks, gsc.gsc_tmo_brand_clicks_wow, gsc.gsc_tmo_brand_clicks_ly, gsc.gsc_tmo_brand_clicks_wow_pct, gsc.gsc_tmo_brand_clicks_yoy_pct,
    gsc.gsc_tmo_nonbrand_impressions, gsc.gsc_tmo_nonbrand_impressions_wow, gsc.gsc_tmo_nonbrand_impressions_ly, gsc.gsc_tmo_nonbrand_impressions_wow_pct, gsc.gsc_tmo_nonbrand_impressions_yoy_pct,
    gsc.gsc_tmo_nonbrand_clicks, gsc.gsc_tmo_nonbrand_clicks_wow, gsc.gsc_tmo_nonbrand_clicks_ly, gsc.gsc_tmo_nonbrand_clicks_wow_pct, gsc.gsc_tmo_nonbrand_clicks_yoy_pct,

    -- ================================================================ TRENDS
    t.max_data_date AS trends_max_data_date,
    t.trends_byod_index, t.trends_byod_index_wow, t.trends_byod_index_ly, t.trends_byod_index_wow_pct, t.trends_byod_index_yoy_pct,
    t.trends_top_kw_1, t.trends_kw1_interest, t.trends_kw1_change,
    t.trends_top_kw_2, t.trends_kw2_interest, t.trends_kw2_change,
    t.trends_top_kw_3, t.trends_kw3_interest, t.trends_kw3_change,
    t.trends_top_kw_4, t.trends_kw4_interest, t.trends_kw4_change,
    t.trends_top_kw_5, t.trends_kw5_interest, t.trends_kw5_change,

    -- ================================================================ ADOBE — ALL CHANNELS
    ab.max_data_date AS adobe_max_data_date,
    ab.adobe_uvnbByod_allChannels, ab.adobe_uvnbByod_allChannels_wow, ab.adobe_uvnbByod_allChannels_ly, ab.adobe_uvnbByod_allChannels_wow_pct, ab.adobe_uvnbByod_allChannels_yoy_pct,
    ab.adobe_uvnbTotal_allChannels, ab.adobe_uvnbTotal_allChannels_wow, ab.adobe_uvnbTotal_allChannels_ly, ab.adobe_uvnbTotal_allChannels_wow_pct, ab.adobe_uvnbTotal_allChannels_yoy_pct,
    ab.adobe_uvnbFlowTotal_allChannels, ab.adobe_uvnbFlowTotal_allChannels_wow, ab.adobe_uvnbFlowTotal_allChannels_ly, ab.adobe_uvnbFlowTotal_allChannels_wow_pct, ab.adobe_uvnbFlowTotal_allChannels_yoy_pct,
    ab.adobe_pctUvnbByodOfUvnbFlow_allChannels, ab.adobe_pctUvnbByodOfUvnbFlow_allChannels_wow, ab.adobe_pctUvnbByodOfUvnbFlow_allChannels_ly, ab.adobe_pctUvnbByodOfUvnbFlow_allChannels_wow_pct, ab.adobe_pctUvnbByodOfUvnbFlow_allChannels_yoy_pct,
    ab.adobe_cartStartByod_allChannels, ab.adobe_cartStartByod_allChannels_wow, ab.adobe_cartStartByod_allChannels_ly, ab.adobe_cartStartByod_allChannels_wow_pct, ab.adobe_cartStartByod_allChannels_yoy_pct,
    ab.adobe_ordersUnassistedByod_allChannels, ab.adobe_ordersUnassistedByod_allChannels_wow, ab.adobe_ordersUnassistedByod_allChannels_ly, ab.adobe_ordersUnassistedByod_allChannels_wow_pct, ab.adobe_ordersUnassistedByod_allChannels_yoy_pct,
    ab.adobe_ordersAssistedByod_allChannels, ab.adobe_ordersAssistedByod_allChannels_wow, ab.adobe_ordersAssistedByod_allChannels_ly, ab.adobe_ordersAssistedByod_allChannels_wow_pct, ab.adobe_ordersAssistedByod_allChannels_yoy_pct,
    ab.adobe_ordersTotalByod_allChannels, ab.adobe_ordersTotalByod_allChannels_wow, ab.adobe_ordersTotalByod_allChannels_ly, ab.adobe_ordersTotalByod_allChannels_wow_pct, ab.adobe_ordersTotalByod_allChannels_yoy_pct,
    ab.adobe_ordersTotal_allChannels, ab.adobe_ordersTotal_allChannels_wow, ab.adobe_ordersTotal_allChannels_ly, ab.adobe_ordersTotal_allChannels_wow_pct, ab.adobe_ordersTotal_allChannels_yoy_pct,
    ab.adobe_pctOrdersByodOfOrdersTotal_allChannels, ab.adobe_pctOrdersByodOfOrdersTotal_allChannels_wow, ab.adobe_pctOrdersByodOfOrdersTotal_allChannels_ly, ab.adobe_pctOrdersByodOfOrdersTotal_allChannels_wow_pct, ab.adobe_pctOrdersByodOfOrdersTotal_allChannels_yoy_pct,
    ab.adobe_cvrByod_allChannels, ab.adobe_cvrByod_allChannels_wow, ab.adobe_cvrByod_allChannels_ly, ab.adobe_cvrByod_allChannels_wow_pct, ab.adobe_cvrByod_allChannels_yoy_pct,
    ab.adobe_cvrSite_allChannels, ab.adobe_cvrSite_allChannels_wow, ab.adobe_cvrSite_allChannels_ly, ab.adobe_cvrSite_allChannels_wow_pct, ab.adobe_cvrSite_allChannels_yoy_pct,

    -- ================================================================ ADOBE — PAID SEARCH
    ab.adobe_uvnbByod_paidSearch, ab.adobe_uvnbByod_paidSearch_wow, ab.adobe_uvnbByod_paidSearch_ly, ab.adobe_uvnbByod_paidSearch_wow_pct, ab.adobe_uvnbByod_paidSearch_yoy_pct,
    ab.adobe_pctUvnbByodOfTotal_paidSearch, ab.adobe_pctUvnbByodOfTotal_paidSearch_wow, ab.adobe_pctUvnbByodOfTotal_paidSearch_ly, ab.adobe_pctUvnbByodOfTotal_paidSearch_wow_pct, ab.adobe_pctUvnbByodOfTotal_paidSearch_yoy_pct,
    ab.adobe_cartStartByod_paidSearch, ab.adobe_cartStartByod_paidSearch_wow, ab.adobe_cartStartByod_paidSearch_ly, ab.adobe_cartStartByod_paidSearch_wow_pct, ab.adobe_cartStartByod_paidSearch_yoy_pct,
    ab.adobe_ordersUnassistedByod_paidSearch, ab.adobe_ordersUnassistedByod_paidSearch_wow, ab.adobe_ordersUnassistedByod_paidSearch_ly, ab.adobe_ordersUnassistedByod_paidSearch_wow_pct, ab.adobe_ordersUnassistedByod_paidSearch_yoy_pct,
    ab.adobe_ordersAssistedByod_paidSearch, ab.adobe_ordersAssistedByod_paidSearch_wow, ab.adobe_ordersAssistedByod_paidSearch_ly, ab.adobe_ordersAssistedByod_paidSearch_wow_pct, ab.adobe_ordersAssistedByod_paidSearch_yoy_pct,
    ab.adobe_ordersTotalByod_paidSearch, ab.adobe_ordersTotalByod_paidSearch_wow, ab.adobe_ordersTotalByod_paidSearch_ly, ab.adobe_ordersTotalByod_paidSearch_wow_pct, ab.adobe_ordersTotalByod_paidSearch_yoy_pct,

    -- ================================================================ ADOBE — ORGANIC SEARCH
    ab.adobe_uvnbByod_organicSearch, ab.adobe_uvnbByod_organicSearch_wow, ab.adobe_uvnbByod_organicSearch_ly, ab.adobe_uvnbByod_organicSearch_wow_pct, ab.adobe_uvnbByod_organicSearch_yoy_pct,
    ab.adobe_pctUvnbByodOfTotal_organicSearch, ab.adobe_pctUvnbByodOfTotal_organicSearch_wow, ab.adobe_pctUvnbByodOfTotal_organicSearch_ly, ab.adobe_pctUvnbByodOfTotal_organicSearch_wow_pct, ab.adobe_pctUvnbByodOfTotal_organicSearch_yoy_pct,
    ab.adobe_cartStartByod_organicSearch, ab.adobe_cartStartByod_organicSearch_wow, ab.adobe_cartStartByod_organicSearch_ly, ab.adobe_cartStartByod_organicSearch_wow_pct, ab.adobe_cartStartByod_organicSearch_yoy_pct,
    ab.adobe_ordersUnassistedByod_organicSearch, ab.adobe_ordersUnassistedByod_organicSearch_wow, ab.adobe_ordersUnassistedByod_organicSearch_ly, ab.adobe_ordersUnassistedByod_organicSearch_wow_pct, ab.adobe_ordersUnassistedByod_organicSearch_yoy_pct,
    ab.adobe_ordersAssistedByod_organicSearch, ab.adobe_ordersAssistedByod_organicSearch_wow, ab.adobe_ordersAssistedByod_organicSearch_ly, ab.adobe_ordersAssistedByod_organicSearch_wow_pct, ab.adobe_ordersAssistedByod_organicSearch_yoy_pct,
    ab.adobe_ordersTotalByod_organicSearch, ab.adobe_ordersTotalByod_organicSearch_wow, ab.adobe_ordersTotalByod_organicSearch_ly, ab.adobe_ordersTotalByod_organicSearch_wow_pct, ab.adobe_ordersTotalByod_organicSearch_yoy_pct,

    -- ================================================================ ADOBE — DIRECT
    ab.adobe_uvnbByod_direct, ab.adobe_uvnbByod_direct_wow, ab.adobe_uvnbByod_direct_ly, ab.adobe_uvnbByod_direct_wow_pct, ab.adobe_uvnbByod_direct_yoy_pct,
    ab.adobe_pctUvnbByodOfTotal_direct, ab.adobe_pctUvnbByodOfTotal_direct_wow, ab.adobe_pctUvnbByodOfTotal_direct_ly, ab.adobe_pctUvnbByodOfTotal_direct_wow_pct, ab.adobe_pctUvnbByodOfTotal_direct_yoy_pct,
    ab.adobe_cartStartByod_direct, ab.adobe_cartStartByod_direct_wow, ab.adobe_cartStartByod_direct_ly, ab.adobe_cartStartByod_direct_wow_pct, ab.adobe_cartStartByod_direct_yoy_pct,
    ab.adobe_ordersUnassistedByod_direct, ab.adobe_ordersUnassistedByod_direct_wow, ab.adobe_ordersUnassistedByod_direct_ly, ab.adobe_ordersUnassistedByod_direct_wow_pct, ab.adobe_ordersUnassistedByod_direct_yoy_pct,
    ab.adobe_ordersAssistedByod_direct, ab.adobe_ordersAssistedByod_direct_wow, ab.adobe_ordersAssistedByod_direct_ly, ab.adobe_ordersAssistedByod_direct_wow_pct, ab.adobe_ordersAssistedByod_direct_yoy_pct,
    ab.adobe_ordersTotalByod_direct, ab.adobe_ordersTotalByod_direct_wow, ab.adobe_ordersTotalByod_direct_ly, ab.adobe_ordersTotalByod_direct_wow_pct, ab.adobe_ordersTotalByod_direct_yoy_pct,

    -- ================================================================ ADOBE — SOCIAL
    ab.adobe_uvnbByod_social, ab.adobe_uvnbByod_social_wow, ab.adobe_uvnbByod_social_ly, ab.adobe_uvnbByod_social_wow_pct, ab.adobe_uvnbByod_social_yoy_pct,
    ab.adobe_pctUvnbByodOfTotal_social, ab.adobe_pctUvnbByodOfTotal_social_wow, ab.adobe_pctUvnbByodOfTotal_social_ly, ab.adobe_pctUvnbByodOfTotal_social_wow_pct, ab.adobe_pctUvnbByodOfTotal_social_yoy_pct,
    ab.adobe_cartStartByod_social, ab.adobe_cartStartByod_social_wow, ab.adobe_cartStartByod_social_ly, ab.adobe_cartStartByod_social_wow_pct, ab.adobe_cartStartByod_social_yoy_pct,
    ab.adobe_ordersUnassistedByod_social, ab.adobe_ordersUnassistedByod_social_wow, ab.adobe_ordersUnassistedByod_social_ly, ab.adobe_ordersUnassistedByod_social_wow_pct, ab.adobe_ordersUnassistedByod_social_yoy_pct,
    ab.adobe_ordersAssistedByod_social, ab.adobe_ordersAssistedByod_social_wow, ab.adobe_ordersAssistedByod_social_ly, ab.adobe_ordersAssistedByod_social_wow_pct, ab.adobe_ordersAssistedByod_social_yoy_pct,
    ab.adobe_ordersTotalByod_social, ab.adobe_ordersTotalByod_social_wow, ab.adobe_ordersTotalByod_social_ly, ab.adobe_ordersTotalByod_social_wow_pct, ab.adobe_ordersTotalByod_social_yoy_pct,

    -- ================================================================ ADOBE — PROGRAMMATIC
    ab.adobe_uvnbByod_programmatic, ab.adobe_uvnbByod_programmatic_wow, ab.adobe_uvnbByod_programmatic_ly, ab.adobe_uvnbByod_programmatic_wow_pct, ab.adobe_uvnbByod_programmatic_yoy_pct,
    ab.adobe_pctUvnbByodOfTotal_programmatic, ab.adobe_pctUvnbByodOfTotal_programmatic_wow, ab.adobe_pctUvnbByodOfTotal_programmatic_ly, ab.adobe_pctUvnbByodOfTotal_programmatic_wow_pct, ab.adobe_pctUvnbByodOfTotal_programmatic_yoy_pct,
    ab.adobe_cartStartByod_programmatic, ab.adobe_cartStartByod_programmatic_wow, ab.adobe_cartStartByod_programmatic_ly, ab.adobe_cartStartByod_programmatic_wow_pct, ab.adobe_cartStartByod_programmatic_yoy_pct,
    ab.adobe_ordersUnassistedByod_programmatic, ab.adobe_ordersUnassistedByod_programmatic_wow, ab.adobe_ordersUnassistedByod_programmatic_ly, ab.adobe_ordersUnassistedByod_programmatic_wow_pct, ab.adobe_ordersUnassistedByod_programmatic_yoy_pct,
    ab.adobe_ordersAssistedByod_programmatic, ab.adobe_ordersAssistedByod_programmatic_wow, ab.adobe_ordersAssistedByod_programmatic_ly, ab.adobe_ordersAssistedByod_programmatic_wow_pct, ab.adobe_ordersAssistedByod_programmatic_yoy_pct,
    ab.adobe_ordersTotalByod_programmatic, ab.adobe_ordersTotalByod_programmatic_wow, ab.adobe_ordersTotalByod_programmatic_ly, ab.adobe_ordersTotalByod_programmatic_wow_pct, ab.adobe_ordersTotalByod_programmatic_yoy_pct,

    -- ================================================================ ADOBE — OTHER
    ab.adobe_uvnbByod_other, ab.adobe_uvnbByod_other_wow, ab.adobe_uvnbByod_other_ly, ab.adobe_uvnbByod_other_wow_pct, ab.adobe_uvnbByod_other_yoy_pct,
    ab.adobe_pctUvnbByodOfTotal_other, ab.adobe_pctUvnbByodOfTotal_other_wow, ab.adobe_pctUvnbByodOfTotal_other_ly, ab.adobe_pctUvnbByodOfTotal_other_wow_pct, ab.adobe_pctUvnbByodOfTotal_other_yoy_pct,
    ab.adobe_cartStartByod_other, ab.adobe_cartStartByod_other_wow, ab.adobe_cartStartByod_other_ly, ab.adobe_cartStartByod_other_wow_pct, ab.adobe_cartStartByod_other_yoy_pct,
    ab.adobe_ordersUnassistedByod_other, ab.adobe_ordersUnassistedByod_other_wow, ab.adobe_ordersUnassistedByod_other_ly, ab.adobe_ordersUnassistedByod_other_wow_pct, ab.adobe_ordersUnassistedByod_other_yoy_pct,
    ab.adobe_ordersAssistedByod_other, ab.adobe_ordersAssistedByod_other_wow, ab.adobe_ordersAssistedByod_other_ly, ab.adobe_ordersAssistedByod_other_wow_pct, ab.adobe_ordersAssistedByod_other_yoy_pct,
    ab.adobe_ordersTotalByod_other, ab.adobe_ordersTotalByod_other_wow, ab.adobe_ordersTotalByod_other_ly, ab.adobe_ordersTotalByod_other_wow_pct, ab.adobe_ordersTotalByod_other_yoy_pct

FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_silver_sa360_weekly`             sa
FULL OUTER JOIN `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_silver_gsc_weekly`            gsc ON sa.week_sun_to_sat = gsc.week_sun_to_sat
FULL OUTER JOIN `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_silver_profound_weekly`       p   ON COALESCE(sa.week_sun_to_sat, gsc.week_sun_to_sat) = p.week_sun_to_sat
FULL OUTER JOIN `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_silver_profoundGofish_weekly` g   ON COALESCE(sa.week_sun_to_sat, gsc.week_sun_to_sat, p.week_sun_to_sat) = g.week_sun_to_sat
FULL OUTER JOIN `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_silver_googleTrends_weekly`   t   ON COALESCE(sa.week_sun_to_sat, gsc.week_sun_to_sat, p.week_sun_to_sat, g.week_sun_to_sat) = t.week_sun_to_sat
FULL OUTER JOIN `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_silver_adobe_weekly`          ab  ON COALESCE(sa.week_sun_to_sat, gsc.week_sun_to_sat, p.week_sun_to_sat, g.week_sun_to_sat, t.week_sun_to_sat) = ab.week_sun_to_sat

ORDER BY week_sun_to_sat ASC
;